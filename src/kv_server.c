#include "connection.h"
#include "hashtable.h"
#include "kvstore.h"
#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_MSG 32 << 20
#define MAX_EVENTS 128

int epfd = -1, srv_fd = -1;
KVStore g_data;

// ConnState try_one_req(Conn *conn) {
//     if (rb_size(&conn->income) < 4)
//         return WAIT;
//     uint32_t len = 0;
//     rb_peek0(&conn->income, (uint8_t *) &len, 4);
//     if (len > MAX_MSG) {
//         msg("Message too long");
//         return CLOSE;
//     }
//
//     if (4 + len > rb_size(&conn->income))
//         return WAIT;
//     rb_consume(&conn->income, 4);
//
//     simple_req cmd;
//     if (parse_simple_req(&conn->income, len, &cmd) == -1) {
//         msg("Bad Request");
//         return CLOSE;
//     }
//     RingBuf buf;
//     rb_init(&buf, 4096);
//
//     do_req(&g_data, &cmd, &buf);
//     size_t resp_size = rb_size(&buf);
//     if (resp_size > MAX_MSG) {
//         rb_clear(&buf);
//         out_err(&buf, ERR_TOO_BIG, "message too long");
//         resp_size = rb_size(&buf);
//     }
//     write_u32(&conn->outgo, (uint32_t) resp_size);
//     out_buf(&conn->outgo, &buf);
//     rb_destroy(&buf);
//
//     for (int i = 0; i < cmd.argc; i++) {
//         vstr_destroy(cmd.argv[i]);
//     }
//     free(cmd.argv);
//
//     return OK;
// }

static void handle_signal(const int signum) {
    fprintf(stderr, "Received sig %d. Performing graceful shutdown...\n", signum);
    if (epfd != -1)
        close(epfd);
    if (srv_fd != -1)
        close(srv_fd);
    kv_clear(&g_data);
    exit(EXIT_SUCCESS); // Exit the program
}

// bool conn_handler(HNode *node, void *arg) {
//     Conn *c = container_of(node, Conn, pool_node);
//     int *cnt = arg;
//     if (!c->closing && c->flags & EPOLLIN) {
//         switch (handle_read(c, &g_data.manager.idle, try_one_req)) {
//             case OK:
//                 *cnt += 1;
//             case WAIT:
//                 break;
//             case AGAIN:
//                 c->flags &= ~EPOLLIN;
//                 break;
//             case CLOSE:
//                 cm_mark_closing(&g_data.manager, c);
//         }
//     }
//     if (!c->closing && c->flags & EPOLLOUT) {
//         switch (handle_write(c, &g_data.manager.idle)) {
//             case OK:
//                 *cnt += 1;
//             case WAIT:
//                 break;
//             case AGAIN:
//                 c->flags &= ~EPOLLOUT;
//                 break;
//             case CLOSE:
//                 cm_mark_closing(&g_data.manager, c);
//         }
//     }
//
//     return true;
// }

int main() {
    //     signal(SIGTERM, handle_signal);
    //     signal(SIGINT, handle_signal);
    //
    //     struct epoll_event ev, evs[MAX_EVENTS];
    //     bool can_accept = false;
    //
    //     kv_init(&g_data);
    //
    //     srv_fd = socket(AF_INET, SOCK_STREAM, 0);
    //     if (srv_fd == -1) {
    //         die("socket()");
    //     }
    //     set_reuseaddr(srv_fd);
    //
    //     struct sockaddr_in addr;
    //     addr.sin_family = AF_INET;
    //     addr.sin_addr.s_addr = htonl(0);
    //     addr.sin_port = htons(1234);
    //
    //     int ret = bind(srv_fd, (const struct sockaddr *) &addr, sizeof(struct sockaddr_in));
    //     if (ret == -1) {
    //         perror("bind()");
    //         goto L_ERR0;
    //     }
    //
    //     set_nonblock(srv_fd);
    //     if (listen(srv_fd, SOMAXCONN) < 0) {
    //         perror("listen()");
    //         goto L_ERR0;
    //     }
    //
    //     epfd = epoll_create1(EPOLL_CLOEXEC);
    //     if (epfd == -1) {
    //         perror("epoll_create1()");
    //         goto L_ERR0;
    //     }
    //
    //     ev.events = EPOLLIN | EPOLLET;
    //     ev.data.fd = srv_fd;
    //     for (;;) {
    //         errno = 0;
    //         ret = epoll_ctl(epfd, EPOLL_CTL_ADD, srv_fd, &ev);
    //         if (ret == -1) {
    //             if (errno == EINTR)
    //                 continue;
    //             perror("main(): epoll_ctl()");
    //             goto L_ERR1;
    //         }
    //         break;
    //     }
    //
    //     for (;;) {
    //         while (can_accept) {
    //             const ConnState rstate = handle_accept(&g_data.manager, epfd, srv_fd);
    //             switch (rstate) {
    //                 case OK:
    //                 case WAIT:
    //                     break;
    //                 case AGAIN:
    //                     can_accept = false;
    //                     break;
    //                 case CLOSE:
    //                     goto L_ERR1;
    //             }
    //         }
    //
    //         int cnt = 0;
    //         hm_foreach(&g_data.manager.pool, conn_handler, &cnt);
    //         cm_clean_closing(&g_data.manager);
    //         process_timer(&g_data);
    //         if (cnt > 0)
    //             continue;
    //
    //         const int32_t timeout = next_timer_ms(&g_data);
    //         int nfds;
    //         for (;;) {
    //             errno = 0;
    //             nfds = epoll_wait(epfd, evs, MAX_EVENTS, timeout);
    //             if (nfds == -1) {
    //                 if (errno == EINTR)
    //                     continue;
    //                 perror("epoll_wait()");
    //                 goto L_ERR1;
    //             }
    //             break;
    //         }
    //
    //         for (int i = 0; i < nfds; i++) {
    //             ev = evs[i];
    //             const int efd = ev.data.fd;
    //             Conn *c;
    //             if (efd == srv_fd) {
    //                 can_accept = true;
    //             } else if ((c = cm_get_conn(&g_data.manager, efd))) {
    //                 c->flags = ev.events;
    //                 c->last_active = get_clock_ms();
    //                 dlist_detach(&c->list_node);
    //                 dlist_insert_before(&g_data.manager.idle, &c->list_node);
    //             }
    //         }
    //     }
    //
    // L_ERR1:
    //     close(epfd);
    // L_ERR0:
    //     close(srv_fd);
    //     kv_clear(&g_data);
    //     return EXIT_FAILURE;
}
