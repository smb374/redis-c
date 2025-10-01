//
// Created by poyehchen on 9/26/25.
//

#ifndef CONNECTION_H
#define CONNECTION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "hashtable.h"
#include "list.h"
#include "ringbuf.h"

#define INIT_BUFFER_SIZE 65536
#define TIMEOUT 5000

enum ConnState {
    OK,
    AGAIN,
    WAIT,
    CLOSE,
};
typedef enum ConnState ConnState;

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

#ifdef __cplusplus
}
#endif
#endif
