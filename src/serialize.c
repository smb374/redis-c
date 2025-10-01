//
// Created by poyehchen on 9/29/25.
//

#include "serialize.h"
#include "utils.h"

#include <stdlib.h>
#include <string.h>

void write_u8(RingBuf *rb, const uint8_t val) { rb_write(rb, &val, 1); }
void write_u32(RingBuf *rb, const uint32_t val) { rb_write(rb, (uint8_t *) &val, 4); }
void write_i64(RingBuf *rb, const int64_t val) { rb_write(rb, (uint8_t *) &val, 8); }
void write_dbl(RingBuf *rb, const double val) { rb_write(rb, (uint8_t *) &val, 8); }

// | tag |
void out_nil(RingBuf *rb) {
    const size_t sz = rb_size(rb), wsize = 1;
    if (wsize > rb->cap - 1 - sz) {
        rb_resize(rb, next_pow2(sz + wsize));
    }
    write_u8(rb, TAG_NIL);
}
// | tag | len | str |
void out_vstr(RingBuf *rb, const vstr *val) {
    const size_t sz = rb_size(rb), wsize = 1 + 4 + val->len;
    if (wsize > rb->cap - 1 - sz) {
        rb_resize(rb, next_pow2(sz + wsize));
    }
    write_u8(rb, TAG_STR);
    write_u32(rb, val->len);
    rb_write(rb, (uint8_t *) val->dat, val->len);
}
// | tag | len | str |
void out_str(RingBuf *rb, const char *s, const size_t len) {
    const size_t sz = rb_size(rb), wsize = 1 + 4 + len;
    if (wsize > rb->cap - 1 - sz) {
        rb_resize(rb, next_pow2(sz + wsize));
    }
    write_u8(rb, TAG_STR);
    write_u32(rb, len);
    rb_write(rb, (uint8_t *) s, len);
}
// | tag | i64 val |
void out_int(RingBuf *rb, const int64_t val) {
    const size_t sz = rb_size(rb), wsize = 1 + 8;
    if (wsize > rb->cap - 1 - sz) {
        rb_resize(rb, next_pow2(sz + wsize));
    }
    write_u8(rb, TAG_INT);
    write_i64(rb, val);
}
// | tag | double val |
void out_dbl(RingBuf *rb, const double val) {
    const size_t sz = rb_size(rb), wsize = 1 + 8;
    if (wsize > rb->cap - 1 - sz) {
        rb_resize(rb, next_pow2(sz + wsize));
    }
    write_u8(rb, TAG_DBL);
    write_dbl(rb, val);
}
// | tag | err code | msg len | msg |
void out_err(RingBuf *rb, const uint32_t err, const char *msg) {
    const uint32_t len = strnlen(msg, 65536);
    const size_t sz = rb_size(rb), wsize = 1 + 4 + 4 + len;
    if (wsize > rb->cap - 1 - sz) {
        rb_resize(rb, next_pow2(sz + wsize));
    }
    write_u8(rb, TAG_ERR);
    write_u32(rb, err);
    write_u32(rb, len);
    rb_write(rb, (uint8_t *) msg, len);
}
// | tag | n | item 1 | ... | item n |
void out_arr(RingBuf *rb, const uint32_t n) {
    const size_t sz = rb_size(rb), wsize = 1 + 4;
    if (wsize > rb->cap - 1 - sz) {
        rb_resize(rb, next_pow2(sz + wsize));
    }
    write_u8(rb, TAG_ARR);
    write_u32(rb, n);
}
// Copies content from buf to rb, useful for dynamic sized arr
void out_buf(RingBuf *rb, RingBuf *buf) {
    const size_t sz = rb_size(rb), wsize = rb_size(buf);
    if (wsize > rb->cap - 1 - sz) {
        rb_resize(rb, next_pow2(sz + wsize));
    }
    uint8_t *dat = calloc(wsize, 1);
    rb_read(buf, dat, wsize);
    rb_write(rb, dat, wsize);
    free(dat);
}
