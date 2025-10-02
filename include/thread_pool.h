//
// Created by poyehchen on 10/1/25.
//

#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#ifdef __cplusplus
extern "C" {

#endif

#include "cqueue.h"
#include <pthread.h>

#define WORKERS 8
#define QUEUESIZE 4096

struct ThreadPool {
    // Worker chan
    pthread_t workers[WORKERS];
    cqueue *worker_qs[WORKERS];
    int worker_evs[WORKERS];
    // Result chan
    cqueue *result_q;
    int res_ev;
    // RR idx
    size_t idx;
};

typedef struct ThreadPool ThreadPool;

void pool_init(ThreadPool *pool);
void pool_start(ThreadPool *pool, cnode * (*f)(cnode *));
bool pool_post(ThreadPool *pool, cnode *work);
void pool_destroy(ThreadPool *pool);
void pool_stop(ThreadPool *pool);

#ifdef __cplusplus
}
#endif
#endif
