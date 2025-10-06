//
// Created by poyehchen on 10/1/25.
//

#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#ifdef __cplusplus
extern "C" {
#endif

#include <ev.h>
#include <pthread.h>

#include "cqueue.h"

#define WORKERS 8
#define QUEUESIZE 4096
#define STOP_MAGIC 0xDEADBEEFCAFEBEEF

struct wctx {
    // worker id
    int id;
    pthread_t thread;
    // self loop
    struct ev_loop *loop, *master;
    // async watchers
    struct ev_async *rev, wev;
    // in & out queues
    cqueue *q, *rq;
    // process f
    cnode *(*f)(cnode *);
};
typedef struct wctx wctx;

struct ThreadPool {
    size_t rr_idx;
    bool (*res_cb)(cnode *);
    struct ev_loop *loop;
    struct ev_async rev;
    cqueue *result_q;
    wctx *workers[WORKERS];
};
typedef struct ThreadPool ThreadPool;

void pool_init(ThreadPool *pool, bool (*res_cb)(cnode *));
void pool_start(ThreadPool *pool, cnode *(*f)(cnode *) );
void pool_post(ThreadPool *pool, cnode *work);
void pool_destroy(ThreadPool *pool);
void pool_stop(ThreadPool *pool);

#ifdef __cplusplus
}
#endif
#endif
