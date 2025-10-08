#include <ev.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

#include "connection.h"
#include "kvstore.h"
#include "serialize.h"
#include "utils.h"

#define MAX_MSG 32 << 20
#define MAX_EVENTS 128

SrvConn srv;
KVStore g_data;
ev_timer kv_timer;

ConnState try_one_req(Conn *conn) {
    if (rb_size(&conn->income) < 4)
        return WAIT;
    uint32_t len = 0;
    rb_peek0(&conn->income, (uint8_t *) &len, 4);
    if (len > MAX_MSG) {
        msg("Message too long");
        return CLOSE;
    }

    if (4 + len > rb_size(&conn->income))
        return WAIT;
    rb_consume(&conn->income, 4);

    simple_req cmd;
    if (parse_simple_req(&conn->income, len, &cmd) == -1) {
        msg("Bad Request");
        return CLOSE;
    }
    RingBuf buf;
    rb_init(&buf, 4096);

    do_req(&g_data, &cmd, &buf);
    size_t resp_size = rb_size(&buf);
    if (resp_size > MAX_MSG) {
        rb_clear(&buf);
        out_err(&buf, ERR_TOO_BIG, "message too long");
        resp_size = rb_size(&buf);
    }
    write_u32(&conn->outgo, (uint32_t) resp_size);
    out_buf(&conn->outgo, &buf);
    rb_destroy(&buf);

    for (int i = 0; i < cmd.argc; i++) {
        vstr_destroy(cmd.argv[i]);
    }
    free(cmd.argv);

    return OK;
}

static void exit_cb(EV_P_ ev_signal *w, const int revents) {
    msg("Received terminating signal. Performing graceful shutdown...");
    ev_timer_stop(EV_A, &kv_timer);
    srv_clear(&srv);
    ev_break(EV_A_ EVBREAK_ALL);
}

// static void kv_timer_cb(EV_P_ ev_timer *w, const int revents) {
//     process_timer(&g_data);
//     int32_t next = next_timer_ms(&g_data);
//     if (next > 0) {
//         ev_timer_stop(EV_A_ w);
//         ev_timer_set(w, (double) next / 1000.0, 1.);
//         ev_timer_start(EV_A_ w);
//     }
// }

int main() {
    // Init KVStore.
    kv_new(&g_data);
    struct ev_loop *loop = ev_default_loop(0);
    // Signal Handling
    ev_signal sigint, sigterm;
    ev_signal_init(&sigint, exit_cb, SIGINT);
    ev_signal_init(&sigterm, exit_cb, SIGTERM);
    ev_signal_start(loop, &sigint);
    ev_signal_start(loop, &sigterm);
    // KVStore timer
    // ev_timer_init(&kv_timer, kv_timer_cb, 1., 1.);
    // ev_timer_start(loop, &kv_timer);
    // Setup server
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(0);
    addr.sin_port = htons(1234);
    srv_init(&srv, fd, (const struct sockaddr *) &addr, sizeof(struct sockaddr_in));
    // Start loop
    ev_run(loop, 0);
    // Epilogue
    msg("Exit main loop");
    kv_clear(&g_data);
}
