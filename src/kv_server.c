#include <ev.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#include "connection.h"
#include "kvstore.h"
#include "parse.h"
#include "qsbr.h"
#include "utils.h"

#define MAX_MSG 32 << 20
#define MAX_EVENTS 128

SrvConn srv;
KVStore g_data;

ConnState try_one_req(Conn *c) {
    if (rb_size(&c->income) < 4)
        return WAIT;
    uint32_t len = 0;
    rb_peek0(&c->income, (uint8_t *) &len, 4);
    if (len > MAX_MSG) {
        logger(stderr, "WARN", "[conn %d] Message too long\n", c->fd);
        return CLOSE;
    }

    if (4 + len > rb_size(&c->income))
        return WAIT;
    rb_consume(&c->income, 4);

    OwnedRequest *oreq = new_owned_req(NULL, &c->income, len);
    if (!oreq) {
        logger(stderr, "WARN", "[conn %d] Invalid request in input buffer\n", c->fd);
        return CLOSE;
    }

    kv_dispatch(&g_data, c, oreq);

    return OK;
}

static void exit_cb(EV_P_ ev_signal *w, const int revents) {
    logger(stderr, "INFO", "[signal] Got singal %d, Perform graceful shutdown...\n", w->signum);
    kv_stop(&g_data);
    srv_clear(&srv);
}

int main() {
    // Init KVStore.
    qsbr_init(65536);
    qsbr_reg();
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
    // Start thread pool
    kv_start(&g_data);
    // Start loop
    ev_run(loop, 0);
    // Epilogue
    kv_clear(&g_data);
    ev_default_destroy();
    qsbr_quiescent();
    qsbr_unreg();
    qsbr_destroy();
    logger(stderr, "INFO", "[main] Exit main loop\n");
}
