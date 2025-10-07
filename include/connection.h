//
// Created by poyehchen on 9/26/25.
//

#ifndef CONNECTION_H
#define CONNECTION_H

#ifdef __cplusplus
extern "C" {
#endif

#include <ev.h>
#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>

#include "hashtable.h"
#include "list.h"
#include "ringbuf.h"

#define INIT_BUFFER_SIZE 65536
#define TIMEOUT 5000
#define TIMEOUT_S 5.0

enum ConnState {
    OK,
    AGAIN,
    WAIT,
    CLOSE,
};
typedef enum ConnState ConnState;

struct Conn2 {
    DList node;

    int fd;
    bool is_alloc;
    ev_io iow;
    uint64_t last_active;
    RingBuf income, outgo;
};

struct SrvConn {
    int fd;
    ev_io iow;
    ev_timer idlew;
};

ConnState try_one_req2(struct Conn2 *); // Blanket, rely external impl
void srv_conn_init(struct SrvConn *c, struct sockaddr_in *addr, socklen_t len);
struct Conn2 *conn2_init(struct Conn2 *c, int fd);
void conn2_clear(struct Conn2 *c);

// Obsolete, to be cleaned
// -----------------------------
struct Conn {
    HNode pool_node;
    DList list_node;

    int fd;
    bool closing;
    uint32_t flags;
    uint64_t last_active;
    RingBuf income, outgo;
};
typedef struct Conn Conn;

struct ConnManager {
    HMap pool;
    DList idle;
    DList closing;
};
typedef struct ConnManager ConnManager;

void conn_init(Conn *conn, int fd);
void conn_clear(Conn *c);
bool conn_eq(HNode *le, HNode *re);

void cm_init(ConnManager *cm);
void cm_destroy(ConnManager *cm);
Conn *cm_get_conn(ConnManager *cm, int fd);
void cm_mark_closing(ConnManager *cm, Conn *conn);
void cm_clean_closing(ConnManager *cm);

ConnState handle_read(Conn *c, DList *idle, ConnState (*try_one_req)(struct Conn *));
ConnState handle_write(Conn *c, DList *idle);
ConnState handle_accept(ConnManager *cm, int epfd, int srv_fd);
// -----------------------------

#ifdef __cplusplus
}
#endif
#endif
