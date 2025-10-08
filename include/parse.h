#ifndef PARSE_H
#define PARSE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "ringbuf.h"
#include "utils.h"

#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>

#ifndef MAX_ARGS
#define MAX_ARGS 256
#endif

struct simple_req {
    uint32_t argc;
    vstr **argv;
};
typedef struct simple_req simple_req;

enum cmd_type {
    CMD_GET,
    CMD_SET,
    CMD_DEL,
    CMD_KEYS,
    CMD_ZADD,
    CMD_ZREM,
    CMD_ZSCORE,
    CMD_ZQUERY,
    CMD_PTTL,
    CMD_PEXPIRE,
    // Errors
    CMD_BAD,
    CMD_UNKNOWN,
};

struct Request {
    enum cmd_type type;
    vstr *key;
    union {
        vstr *val;
        struct {
            double score;
            vstr *name;
        } zadd_arg;
        struct {
            double score;
            vstr *name;
            int64_t offset, limit;
        } zquery_arg;
        int64_t ttl;
        char *err;
    } args;
};
typedef struct Request Request;

struct OwnedRequest {
    Request req;
    const simple_req *base;
};
typedef struct OwnedRequest OwnedRequest;

bool str2dbl(const vstr *str, double *out);
bool str2int(const vstr *str, int64_t *out);
ssize_t parse_simple_req(RingBuf *rb, size_t sz, simple_req *out);
void simple2req(const simple_req *sreq, Request *req);
void simple2oreq(const simple_req *sreq, OwnedRequest *req);

#ifdef __cplusplus
}
#endif
#endif
// vim: set ft=c
