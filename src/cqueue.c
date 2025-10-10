#include "cqueue.h"

#include <assert.h>
#include <sched.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "utils.h"

cqueue *cq_init(cqueue *q, size_t cap) {
    if (!q) {
        q = calloc(1, sizeof(cqueue));
        q->is_alloc = true;
    } else {
        q->is_alloc = false;
    }
    atomic_init(&q->head, 0);
    atomic_init(&q->count, 0);
    q->tail = 0;
    q->cap = cap;
    q->buf = calloc(cap, sizeof(cnode *));
    atomic_thread_fence(memory_order_release);
    return q;
}
void cq_destroy(cqueue *q) {
    free(q->buf);
    if (q->is_alloc)
        free(q);
}
bool cq_put(cqueue *q, cnode *node) {
    size_t count = FAA(&q->count, 1, memory_order_acquire);
    if (count >= q->cap) {
        // queue is full
        FAS(&q->count, 1, memory_order_release);
        return false;
    }

    size_t head = LOAD(&q->head, memory_order_acquire), nhead;
    for (;;) {
        nhead = (head + 1) % q->cap;
        if (CMPXCHG(&q->head, &head, nhead, memory_order_acq_rel, memory_order_acquire)) {
            // CMPEXG success, q->head is nhead now.
            break;
        }
        // Acquires new head on fail
    }
    // Since slot is acquired after CMPEXG success for head, we can just store the slot.
    cnode *old = XCHG(&q->buf[head], node, memory_order_release);
    assert(old == NULL); // Sanity check
    return true;
}
cnode *cq_pop(cqueue *q) {
    cnode *ret = XCHG(&q->buf[q->tail], NULL, memory_order_acquire);
    if (!ret)
        /* a thread is adding to the queue, but hasn't done the write yet
         * to actually put the item in. Act as if nothing is in the queue.
         * Worst case, other producers write content to tail + 1..n and finish, but
         * the producer that writes to tail doesn't do it in time, and we get here.
         * But that's okay, because once it DOES finish, we can get at all the data
         * that has been filled in. */
        return NULL;

    q->tail = (q->tail + 1) % q->cap;
    size_t r = FAS(&q->count, 1, memory_order_release);
    assert(r > 0);
    return ret;
}
size_t cq_size(cqueue *q) { return LOAD(&q->count, memory_order_relaxed); }
size_t cq_cap(cqueue *q) { return q->cap; }
