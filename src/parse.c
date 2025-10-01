#include "parse.h"
#include "ringbuf.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>


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