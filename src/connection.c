//
// Created by poyehchen on 9/26/25.
//
#include "connection.h"

#include <assert.h>
#include <errno.h>
#include <ev.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#include "list.h"
#include "ringbuf.h"
#include "utils.h"

// idles will be at tail, use idles.next to access oldest.
static DList idles = {&idles, &idles};

static ConnState handle_read(Conn *c);
static ConnState handle_write(Conn *c);
static ConnState handle_accept(SrvConn *c);

static void conn_cb(EV_P_ ev_io *w, const int revents) {
    Conn *c = w->data;
    int events;

    // Try at most 128 read/writes to prevent hogging
    if (w->events & EV_READ) {
        for (int i = 0; i < 128; i++) {
            ConnState s = handle_read(c); // Will execute handle_write2 if there is data.
            switch (s) {
                case OK: // read/write OK
                case WAIT: // nop for read, might from write but don't care here
                    break;
                case AGAIN: // read/write return EAGAIN
                    goto WRITE_PHASE;
                case CLOSE:
                    goto CLOSE;
            }
        }
    }

WRITE_PHASE:
    if (w->events & EV_WRITE) {
        for (int i = 0; i < 128; i++) {
            ConnState s = handle_write(c);
            switch (s) {
                case OK: // write OK
                    break;
                case WAIT: // need more data
                case AGAIN: // write returns EAGAIN
                    goto EXIT;
                case CLOSE: // connection need close
                    goto CLOSE;
            }
        }
    }

EXIT:
    events = EV_READ | (rb_size(&c->outgo) > 0 ? EV_WRITE : 0);
    if (w->events != events) {
        ev_io_stop(EV_A_ w);
        ev_io_set(w, c->fd, events);
        ev_io_start(EV_A_ w);
    }
    return;

CLOSE:
    conn_clear(c);
}

static void accept_cb(EV_P_ ev_io *w, const int revents) {
    SrvConn *c = w->data;

    if (w->events & EV_READ) {
        // Accept at most 128 connections to prevent hogging.
        for (int i = 0; i < 128; i++) {
            ConnState s = handle_accept(c);
            switch (s) {
                case OK: // continue accept next connection.
                case WAIT: // nop for accept
                    break;
                case AGAIN: // wait next notif
                    return;
                case CLOSE: // conn close
                    ev_io_stop(EV_A_ w);
                    close(c->fd);
                    return;
            }
        }
    }
}

static void idle_timer_cb(EV_P_ ev_timer *w, const int revents) {
    uint64_t now = get_clock_ms(), next = 0;
    while (!dlist_empty(&idles)) {
        Conn *c = container_of(idles.next, Conn, node);
        next = c->last_active + TIMEOUT;
        if (next >= now) {
            ev_timer_stop(EV_A_ w);
            ev_timer_set(w, (double) (next - now) / 1000., TIMEOUT_S);
            ev_timer_start(EV_A_ w);
        }
        fprintf(stderr, "Connection %d timed out, closing...\n", c->fd);
        conn_clear(c);
    }
}

void srv_init(SrvConn *c, int fd, const struct sockaddr *addr, socklen_t len) {
    // No alloc as SrvConn should be in bss or main.
    if (!c)
        return;
    set_reuseaddr(fd);
    if (bind(fd, addr, len) < 0) {
        die("bind()");
    }
    set_nonblock(fd);
    if (listen(fd, SOMAXCONN) < 0) {
        die("listen()");
    }

    c->fd = fd;
    struct ev_loop *loop = ev_default_loop(0);
    ev_io_init(&c->iow, accept_cb, fd, EV_READ);
    c->iow.data = c;
    ev_io_start(loop, &c->iow);
    ev_timer_init(&c->idlew, idle_timer_cb, TIMEOUT_S, TIMEOUT_S);
    c->idlew.data = c;
    ev_timer_start(loop, &c->idlew);
}

void srv_clear(SrvConn *c) {
    if (!c)
        return;

    struct ev_loop *loop = ev_default_loop(0);
    ev_io_stop(loop, &c->iow);
    ev_timer_stop(loop, &c->idlew);

    while (!dlist_empty(&idles)) {
        Conn *c = container_of(idles.next, Conn, node);
        fprintf(stderr, "Closing connection %d\n", c->fd);
        conn_clear(c);
    }

    close(c->fd);
}

Conn *conn_init(Conn *c, const int fd) {
    if (!c) {
        c = calloc(1, sizeof(Conn));
        assert(c);
        c->is_alloc = true;
    } else {
        c->is_alloc = false;
    }

    c->fd = fd;
    c->last_active = get_clock_ms();
    rb_init(&c->income, INIT_BUFFER_SIZE);
    rb_init(&c->outgo, INIT_BUFFER_SIZE);
    dlist_init(&c->node);
    dlist_insert_before(&idles, &c->node);

    struct ev_loop *loop = ev_default_loop(0);
    ev_io_init(&c->iow, conn_cb, fd, EV_READ);
    c->iow.data = c;
    ev_io_start(loop, &c->iow);

    return c;
}

void conn_clear(Conn *c) {
    if (!c)
        return;

    dlist_detach(&c->node);
    struct ev_loop *loop = ev_default_loop(0);
    ev_io_stop(loop, &c->iow);
    close(c->fd);
    rb_destroy(&c->income);
    rb_destroy(&c->outgo);
    if (c->is_alloc)
        free(c);
}

static ConnState handle_read(Conn *c) {
    uint8_t buf[INIT_BUFFER_SIZE];
    errno = 0;
    const ssize_t ret = read(c->fd, buf, INIT_BUFFER_SIZE);
    if (ret < 0) {
        switch (errno) {
            case EAGAIN:
                return AGAIN;
            case EINTR:
                return handle_read(c);
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
    dlist_detach(&c->node);
    dlist_insert_before(&idles, &c->node);

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
        return handle_write(c);

    return OK;
}

static ConnState handle_write(Conn *c) {
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
    dlist_detach(&c->node);
    dlist_insert_before(&idles, &c->node);
    rb_consume(&c->outgo, consumed);
    return OK;
}


static ConnState handle_accept(SrvConn *c) {
    struct sockaddr_in caddr;
    socklen_t addrlen = sizeof(struct sockaddr_in);
    errno = 0;
    const int cfd = accept(c->fd, (struct sockaddr *) &caddr, &addrlen);
    if (cfd < 0) {
        switch (errno) {
            case EAGAIN:
                return AGAIN;
            case EINTR:
                return handle_accept(c);
            default:
                perror("accept()");
                return CLOSE;
        }
    } else {
        const uint32_t ip = caddr.sin_addr.s_addr;
        fprintf(stderr, "new client from %u.%u.%u.%u:%u\n", ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
                ntohs(caddr.sin_port));
        set_nonblock(cfd);
        conn_init(NULL, cfd);
        return OK;
    }
}
