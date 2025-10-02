//
// Created by poyehchen on 9/26/25.
//
#include "connection.h"
#include "list.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/epoll.h>
#include <unistd.h>


bool conn_eq(HNode *le, HNode *re) {
    const Conn *lc = container_of(le, Conn, pool_node);
    const Conn *rc = container_of(re, Conn, pool_node);
    return lc->fd == rc->fd;
}

void conn_init(Conn *conn, const int fd) {
    if (!conn)
        return;

    conn->fd = fd;
    rb_init(&conn->income, INIT_BUFFER_SIZE);
    rb_init(&conn->outgo, INIT_BUFFER_SIZE);
    conn->flags = 0;
    conn->last_active = get_clock_ms();
    conn->pool_node.hcode = int_hash_rapid((uint64_t) fd);
    dlist_init(&conn->list_node);
}

void conn_clear(Conn *c) {
    if (!c)
        return;

    dlist_detach(&c->list_node);
    close(c->fd);
    rb_destroy(&c->income);
    rb_destroy(&c->outgo);
    free(c);
}

ConnState handle_read(Conn *c, DList *idle, ConnState (*try_one_req)(Conn *)) {
    uint8_t buf[INIT_BUFFER_SIZE];
    errno = 0;
    const ssize_t ret = read(c->fd, buf, INIT_BUFFER_SIZE);
    if (ret < 0) {
        switch (errno) {
            case EAGAIN:
                return AGAIN;
            case EINTR:
                return handle_read(c, idle, try_one_req);
            default:
                perror("read()");
                return CLOSE;
        }
    } else if (!ret) {
        if (rb_empty(&c->income)) {
            fprintf(stderr, "Client disconnected\n");
        } else {
            fprintf(stderr, "Unexpected EOF\n");
        }
        return CLOSE;
    }
    c->last_active = get_clock_ms();
    dlist_detach(&c->list_node);
    dlist_insert_before(idle, &c->list_node);

    const size_t sz = rb_size(&c->income);
    if ((size_t) ret > c->income.cap - 1 - sz) {
        rb_resize(&c->income, next_pow2(ret + sz));
    }
    rb_write(&c->income, buf, ret);

    ConnState s;
    while ((s = try_one_req(c)) == OK)
        ;

    if (s == CLOSE)
        return CLOSE;
    if (rb_size(&c->outgo) > 0)
        return handle_write(c, idle);

    return OK;
}

ConnState handle_write(Conn *c, DList *idle) {
    if (rb_empty(&c->outgo))
        return WAIT;

    uint8_t buf[4096];
    const size_t to_write = rb_size(&c->outgo);
    size_t consumed = 0;
    while (consumed < to_write) {
        const size_t peeked = rb_peek(&c->outgo, buf, 4096, consumed);
        errno = 0;
        const ssize_t ret = write(c->fd, buf, peeked);
        if (ret == -1) {
            switch (errno) {
                case EAGAIN:
                    rb_consume(&c->outgo, consumed);
                    return AGAIN;
                case EINTR:
                    continue;
                default:
                    perror("write()");
                    rb_consume(&c->outgo, consumed);
                    return CLOSE;
            }
        }
        consumed += ret;
    }
    c->last_active = get_clock_ms();
    dlist_detach(&c->list_node);
    dlist_insert_before(idle, &c->list_node);
    rb_consume(&c->outgo, consumed);
    return OK;
}

void cm_init(ConnManager *cm) {
    bzero(&cm->pool, sizeof(HMap));
    dlist_init(&cm->idle);
    dlist_init(&cm->closing);
}

void cm_destroy(ConnManager *cm) {
    HTable *tables[] = {&cm->pool.newer, &cm->pool.older};
    for (int i = 0; i < 2; i++) {
        if (tables[i]->tab) {
            for (size_t j = 0; j <= tables[i]->mask; j++) {
                HNode *node = tables[i]->tab[j];
                while (node) {
                    HNode *next = node->next; // Save next before delete
                    Conn *c = container_of(node, Conn, pool_node);
                    conn_clear(c);
                    node = next;
                }
            }
        }
    }

    hm_clear(&cm->pool);
}

ConnState handle_accept(ConnManager *cm, const int epfd, const int srv_fd) {
    struct sockaddr_in caddr;
    socklen_t addrlen = sizeof(struct sockaddr_in);
    errno = 0;
    const int cfd = accept(srv_fd, (struct sockaddr *) &caddr, &addrlen);
    if (cfd < 0) {
        switch (errno) {
            case EAGAIN:
                return AGAIN;
            case EINTR:
                return handle_accept(cm, epfd, srv_fd);
            default:
                perror("accept()");
                return CLOSE;
        }
    } else {
        const uint32_t ip = caddr.sin_addr.s_addr;
        fprintf(stderr, "new client from %u.%u.%u.%u:%u\n", ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
                ntohs(caddr.sin_port));
        const int flags = fcntl(cfd, F_GETFL);
        (void) fcntl(cfd, F_SETFL, flags | O_NONBLOCK);
        struct epoll_event ev;
        ev.data.fd = cfd;
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        for (;;) {
            errno = 0;
            const int ret = epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &ev);
            if (ret < 0) {
                if (errno == EAGAIN || errno == EINTR)
                    continue;
                perror("handle_accept(): epoll_ctl()");
                return CLOSE;
            }
            break;
        }
        Conn *conn = calloc(1, sizeof(*conn));
        conn_init(conn, cfd);
        hm_insert(&cm->pool, &conn->pool_node);
        dlist_insert_before(&cm->idle, &conn->list_node);
        return OK;
    }
}

Conn *cm_get_conn(ConnManager *cm, const int fd) {
    Conn key;
    key.fd = fd;
    key.pool_node.hcode = int_hash_rapid((uint64_t) fd);

    HNode *entry = hm_lookup(&cm->pool, &key.pool_node, conn_eq);
    return entry ? container_of(entry, Conn, pool_node) : NULL;
}

void cm_mark_closing(ConnManager *cm, Conn *conn) {
    conn->closing = true;
    dlist_detach(&conn->list_node);
    dlist_insert_before(&cm->closing, &conn->list_node);
}

void cm_clean_closing(ConnManager *cm) {
    while (!dlist_empty(&cm->closing)) {
        Conn *c = container_of(cm->closing.next, Conn, list_node);
        hm_delete(&cm->pool, &c->pool_node, conn_eq);
        conn_clear(c);
    }
}
