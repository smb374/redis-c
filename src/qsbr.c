#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "cqueue.h"
#include "qsbr.h"
#include "utils.h"

qsbr *qsbr_init(qsbr *gc, size_t back_logs) {
    if (!gc) {
        gc = calloc(1, sizeof(qsbr));
        if (!gc) {
            perror("calloc");
            exit(EXIT_FAILURE);
        }
        gc->is_alloc = true;
    } else {
        gc->is_alloc = false;
    }

    atomic_init(&gc->quiescent, 0);
    atomic_init(&gc->active, 0);
    atomic_init(&gc->threads, 0);
    gc->curr = cq_init(NULL, back_logs);
    gc->prev = cq_init(NULL, back_logs);
    pthread_mutex_init(&gc->lock, NULL);
    return gc;
}
void qsbr_destroy(qsbr *gc) {
    cq_destroy(gc->curr);
    cq_destroy(gc->prev);
    if (gc->is_alloc) {
        free(gc);
    }
}
qsbr_tid qsbr_reg(qsbr *gc) {
    qsbr_tid tid = atomic_fetch_add_explicit(&gc->threads, 1, memory_order_acq_rel);
    if (tid >= 64) {
        return -1;
    }
    atomic_fetch_or_explicit(&gc->active, 1ULL << tid, memory_order_acq_rel);
    return tid;
}
void qsbr_add_cb(qsbr *gc, qsbr_cb *cb) { cq_put(gc->curr, &cb->qnode); }
void qsbr_alloc_cb(qsbr *gc, void (*f)(void *), void *arg) {
    qsbr_cb *cb = calloc(1, sizeof(qsbr_cb));
    cb->f = f;
    cb->arg = arg;
    cb->internal = true;
    qsbr_add_cb(gc, cb);
}
void qsbr_quiescent(qsbr *gc, qsbr_tid tid) {
    uint64_t loc = 1ULL << tid;
    uint64_t q = atomic_fetch_or_explicit(&gc->quiescent, loc, memory_order_acq_rel);
    uint64_t active = atomic_load_explicit(&gc->active, memory_order_acquire);
    if ((q | loc) == active) {
        if (!pthread_mutex_trylock(&gc->lock)) {
            q = atomic_load_explicit(&gc->quiescent, memory_order_acquire);
            active = atomic_load_explicit(&gc->active, memory_order_acquire);
            if (q == active) {
                cnode *node = NULL;
                // Drain prev
                while ((node = cq_pop(gc->prev))) {
                    qsbr_cb *cb = container_of(node, qsbr_cb, qnode);
                    cb->f(cb->arg);
                    if (cb->internal) {
                        free(cb);
                    }
                }
                // Safe, as threads putting to curr will use the same pointer,
                // but we treat unfinished put as previous interval.
                cqueue *oprev = gc->prev;
                gc->prev = gc->curr;
                gc->curr = oprev;
                atomic_store_explicit(&gc->quiescent, 0, memory_order_release);
            }
            pthread_mutex_unlock(&gc->lock);
        }
    }
}
