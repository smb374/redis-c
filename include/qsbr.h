#ifndef QSBR_H
#define QSBR_H

#include <stdalign.h>
#ifdef __cplusplus
extern "C" {
#endif /* ifndef __cplusplus */

#include <pthread.h>
#include <stdint.h>
#include "cqueue.h"

struct qsbr_cb {
    void (*f)(void *);
    void *arg;
    bool internal;
    cnode qnode;
};
typedef struct qsbr_cb qsbr_cb;

typedef int qsbr_tid;

struct qsbr;
typedef struct qsbr qsbr;

#ifndef __cplusplus
#include <stdatomic.h>
typedef _Atomic(uint64_t) atomic_u64;
struct qsbr {
    alignas(64) atomic_u64 quiescent, active;
    alignas(64) atomic_uchar threads;
    alignas(64) pthread_mutex_t lock;
    cqueue *curr, *prev;
    bool is_alloc;
};
#endif

qsbr *qsbr_init(qsbr *gc, size_t back_logs);
void qsbr_destroy(qsbr *gc);
qsbr_tid qsbr_reg(qsbr *gc);
void qsbr_add_cb(qsbr *gc, qsbr_cb *cb);
void qsbr_alloc_cb(qsbr *gc, void (*f)(void *), void *arg);
void qsbr_quiescent(qsbr *gc, qsbr_tid tid);

#ifdef __cplusplus
}
#endif /* ifndef __cplusplus */

#endif /* ifndef QSBR_H */
