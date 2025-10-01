//
// Created by poyehchen on 9/29/25.
//

#ifndef SERIALIZE_H
#define SERIALIZE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "ringbuf.h"
#include "utils.h"

enum tlv_tag {
    TAG_NIL = 0, // nil
    TAG_ERR = 1, // error code + msg
    TAG_STR = 2, // string
    TAG_INT = 3, // int64
    TAG_DBL = 4, // double
    TAG_ARR = 5, // array
};


void write_u8(RingBuf *rb, uint8_t val);
void write_u32(RingBuf *rb, uint32_t val);
void write_i64(RingBuf *rb, int64_t val);
void write_dbl(RingBuf *rb, double val);

void out_nil(RingBuf *rb);
void out_vstr(RingBuf *rb, const vstr *val);
void out_str(RingBuf *rb, const char *s, size_t len);
void out_int(RingBuf *rb, int64_t val);
void out_dbl(RingBuf *rb, double val);
void out_err(RingBuf *rb, uint32_t err, const char *msg);
void out_arr(RingBuf *rb, uint32_t n);
void out_buf(RingBuf *rb, RingBuf *buf);

#ifdef __cplusplus
}
#endif

#endif
