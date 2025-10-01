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

bool str2dbl(const vstr *str, double *out);
bool str2int(const vstr *str, int64_t *out);
ssize_t parse_simple_req(RingBuf *rb, size_t sz, simple_req *out);

#ifdef __cplusplus
}
#endif

#endif
