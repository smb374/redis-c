#include "parse.h"
#include "ringbuf.h"

#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>


bool str2dbl(const vstr *str, double *out) {
    char *endptr = NULL;
    *out = strtod(str->dat, &endptr);
    return !isnan(*out);
}

bool str2int(const vstr *str, int64_t *out) {
    char *endptr = NULL;
    *out = strtoll(str->dat, &endptr, 10);
    return true;
}

ssize_t parse_simple_req(RingBuf *rb, const size_t sz, simple_req *out) {
    const size_t end = (rb->head + sz) % rb->cap;
    uint32_t nstr = 0;
    if (rb_read(rb, (uint8_t *) &nstr, 4) != 4 || nstr > MAX_ARGS)
        return -1;
    if (!nstr) {
        if (rb->head != end)
            return -1;
        out->argc = 0;
        out->argv = NULL;
        return 0;
    }

    vstr **vec = calloc(nstr, sizeof(vstr *));
    out->argv = vec;
    for (size_t i = 0; i < nstr; i++) {
        uint32_t len = 0;
        if (rb_read(rb, (uint8_t *) &len, 4) != 4)
            return -1;
        vec[i] = calloc(1, sizeof(vstr) + len + 1);
        vec[i]->len = len;
        const size_t nread = rb_read(rb, (uint8_t *) vec[i]->dat, vec[i]->len);
        if (nread != vec[i]->len) {
            vstr_destroy(vec[i]);
            vec[i] = NULL;
            return -1;
        }
        out->argc = i + 1;
    }
    if (rb->head != end)
        return -1;

    return 0;
}

void simple2req(const simple_req *sreq, Request *req) {
    bzero(req, sizeof(Request));
    if (sreq->argc == 2 && !strncmp("get", sreq->argv[0]->dat, 3)) {
        // get key
        req->type = CMD_GET;
        req->key = sreq->argv[1];
    } else if (sreq->argc == 3 && !strncmp("set", sreq->argv[0]->dat, 3)) {
        // set key name
        req->type = CMD_SET;
        req->key = sreq->argv[1];
        req->args.val = sreq->argv[2];
    } else if (sreq->argc == 2 && !strncmp("del", sreq->argv[0]->dat, 3)) {
        // del key
        req->type = CMD_DEL;
        req->key = sreq->argv[1];
    } else if (sreq->argc == 1 && !strncmp("keys", sreq->argv[0]->dat, 4)) {
        // keys
        req->type = CMD_KEYS;
    } else if (sreq->argc == 4 && !strncmp("zadd", sreq->argv[0]->dat, 4)) {
        // zadd key score name
        double score;
        if (!str2dbl(sreq->argv[2], &score)) {
            req->type = CMD_BAD;
            req->args.err = "expect a float";
            return;
        }
        req->type = CMD_ZADD;
        req->key = sreq->argv[1];
        req->args.zadd_arg.name = sreq->argv[3];
        req->args.zadd_arg.score = score;
    } else if (sreq->argc == 3 && !strncmp("zrem", sreq->argv[0]->dat, 4)) {
        // zrem key name
        req->type = CMD_ZREM;
        req->key = sreq->argv[1];
        req->args.val = sreq->argv[2];
    } else if (sreq->argc == 3 && !strncmp("zscore", sreq->argv[0]->dat, 6)) {
        // zscore key name
        req->type = CMD_ZSCORE;
        req->key = sreq->argv[1];
        req->args.val = sreq->argv[2];
    } else if (sreq->argc == 6 && !strncmp("zquery", sreq->argv[0]->dat, 6)) {
        // zquery key score name offset limit
        double score;
        int64_t offset, limit;
        if (!str2dbl(sreq->argv[2], &score)) {
            req->type = CMD_BAD;
            req->args.err = "expect fp number";
            return;
        }
        if (!str2int(sreq->argv[4], &offset) || !str2int(sreq->argv[5], &limit)) {
            req->type = CMD_BAD;
            req->args.err = "expect int";
            return;
        }
        req->type = CMD_ZQUERY;
        req->key = sreq->argv[1];
        req->args.zquery_arg.score = score;
        req->args.zquery_arg.name = sreq->argv[3];
        req->args.zquery_arg.offset = offset;
        req->args.zquery_arg.limit = limit;
    } else if (sreq->argc == 2 && !strncmp("pttl", sreq->argv[0]->dat, 4)) {
        // pttl key
        req->type = CMD_PTTL;
        req->key = sreq->argv[1];
    } else if (sreq->argc == 3 && !strncmp("pexpire", sreq->argv[0]->dat, 7)) {
        // pexpire key ttl
        int64_t ttl;
        if (!str2int(sreq->argv[2], &ttl)) {
            req->type = CMD_BAD;
            req->args.err = "expect i64";
            return;
        }
        req->type = CMD_PEXPIRE;
        req->key = sreq->argv[1];
        req->args.ttl = ttl;
    } else {
        req->type = CMD_UNKNOWN;
    }
}

void simple2oreq(const simple_req *sreq, OwnedRequest *req) {
    simple2req(sreq, &req->req);
    req->base = sreq;
}
