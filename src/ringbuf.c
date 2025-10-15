#include "ringbuf.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

void rb_init(RingBuf *rb, size_t cap) {
    if (!rb)
        return;

    rb->cap = cap;
    rb->head = rb->tail = 0;
    rb->data = calloc(cap, sizeof(uint8_t));
}

void rb_destroy(RingBuf *rb) {
    if (!rb)
        return;

    free(rb->data);
    rb->data = NULL;
    rb->cap = rb->head = rb->tail = 0;
}

bool rb_empty(RingBuf *rb) {
    if (!rb)
        return true;

    return rb->head == rb->tail;
}

bool rb_full(RingBuf *rb) {
    if (!rb)
        return false;

    return ((rb->tail + 1) % rb->cap) == rb->head;
}

size_t rb_size(RingBuf *rb) {
    if (!rb)
        return 0;

    if (rb->tail >= rb->head) {
        return rb->tail - rb->head;
    } else {
        return rb->cap - rb->head + rb->tail;
    }
}

size_t rb_write(RingBuf *rb, const uint8_t *buf, const size_t len) {
    if (!rb || !buf || !len)
        return 0;

    const size_t to_write = MIN(len, rb->cap - 1 - rb_size(rb));
    size_t nwrite = 0;

    while (nwrite < to_write) {
        const size_t part = MIN(to_write - nwrite, (rb->tail < rb->head) ? rb->head - rb->tail - 1
                                                                         : rb->cap - rb->tail - (!rb->head ? 1 : 0));
        if (!part)
            break;

        memcpy(&rb->data[rb->tail], buf + nwrite, part);
        nwrite += part;
        rb->tail = (rb->tail + part) % rb->cap;
    }

    return nwrite;
}

size_t rb_read(RingBuf *rb, uint8_t *buf, const size_t len) {
    if (!rb || !buf || !len)
        return 0;

    const size_t to_read = MIN(len, rb_size(rb));
    size_t nread = 0;

    while (nread < to_read) {
        const size_t part = MIN(to_read - nread, (rb->head < rb->tail) ? rb->tail - rb->head : rb->cap - rb->head);
        if (!part)
            break;

        memcpy(buf + nread, &rb->data[rb->head], part);
        nread += part;
        rb->head = (rb->head + part) % rb->cap;
    }

    return nread;
}

size_t rb_peek0(RingBuf *rb, uint8_t *buf, const size_t len) { return rb_peek(rb, buf, len, 0); }

size_t rb_peek(RingBuf *rb, uint8_t *buf, const size_t len, const size_t offset) {
    if (!rb || !buf || !len)
        return 0;

    const size_t sz = rb_size(rb);
    if (offset >= sz)
        return 0;
    const size_t to_peek = MIN(len, sz - offset);
    size_t npeek = 0, phead = (rb->head + offset) % rb->cap;

    while (npeek < to_peek) {
        const size_t part = MIN(to_peek - npeek, (phead < rb->tail) ? rb->tail - phead : rb->cap - phead);
        if (!part)
            break;

        memcpy(buf + npeek, &rb->data[phead], part);
        npeek += part;
        phead = (phead + part) % rb->cap;
    }

    return npeek;
}

void rb_consume(RingBuf *rb, const size_t len) {
    if (!rb || !len)
        return;
    const size_t consume_len = MIN(len, rb_size(rb));

    rb->head = (rb->head + consume_len) % rb->cap;
}

void rb_clear(RingBuf *rb) {
    if (!rb)
        return;

    rb->head = rb->tail = 0;
}

void rb_resize(RingBuf *rb, const size_t new_cap) {
    if (!rb || new_cap < rb_size(rb) + 1)
        return;

    if (!rb->head) {
        uint8_t *ndata = realloc(rb->data, sizeof(uint8_t) * new_cap);
        if (!ndata)
            return;
        rb->data = ndata;
        rb->cap = new_cap;
    } else {
        uint8_t *ndata = calloc(new_cap, sizeof(uint8_t));
        if (!ndata)
            return;
        const size_t len = rb_size(rb);
        if (!rb_empty(rb)) {
            rb_read(rb, ndata, len);
        }
        uint8_t *odata = rb->data;
        rb->data = ndata;
        rb->head = 0;
        rb->tail = len;
        rb->cap = new_cap;
        free(odata);
    }
}
