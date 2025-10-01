#ifndef RINGBUF_H
#define RINGBUF_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct RingBuf {
    uint8_t *data;
    size_t head, tail, cap;
};
typedef struct RingBuf RingBuf;

void rb_init(RingBuf *rb, size_t cap);
void rb_destroy(RingBuf *rb);
size_t rb_size(RingBuf *rb);
bool rb_empty(RingBuf *rb);
bool rb_full(RingBuf *rb);
size_t rb_read(RingBuf *rb, uint8_t *buf, size_t len);
size_t rb_peek(RingBuf *rb, uint8_t *buf, size_t len,
               size_t offset);
size_t rb_peek0(RingBuf *rb, uint8_t *buf, size_t len);
size_t rb_write(RingBuf *rb, const uint8_t *buf, size_t len);
void rb_consume(RingBuf *rb, size_t len);
void rb_clear(RingBuf *rb);
void rb_resize(RingBuf *rb, size_t new_cap);

#ifdef __cplusplus
}
#endif
#endif
