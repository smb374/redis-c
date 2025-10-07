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

struct Conn {
    DList node;

    int fd;
    bool is_alloc;
    ev_io iow;
    uint64_t last_active;
    RingBuf income, outgo;
};
typedef struct Conn Conn;

struct SrvConn {
    int fd;
    ev_io iow;
    ev_timer idlew;
};
typedef struct SrvConn SrvConn;

ConnState try_one_req(Conn *); // Blanket, rely external impl
void srv_init(SrvConn *c, int fd, const struct sockaddr *addr, socklen_t len);
void srv_clear(SrvConn *c);
Conn *conn_init(Conn *c, int fd);
void conn_clear(Conn *c);

#ifdef __cplusplus
}
#endif
#endif
