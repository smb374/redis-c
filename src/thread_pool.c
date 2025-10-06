//
// Created by poyehchen on 10/1/25.
//
#include "thread_pool.h"

#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#include "cqueue.h"
#include "ev.h"

static pthread_barrier_t barrier;

void pool_cb(EV_P_ ev_async *w, const int revents) {
    ThreadPool *pool = w->data;
    cnode *p;
    bool res = false;
    while ((p = cq_pop(pool->result_q)) && !res) {
        res = pool->res_cb(p);
    }
    if (res) {
        ev_async_stop(loop, &pool->rev);
        ev_break(loop, EVBREAK_ALL);
        pool_stop(pool);
    }
}

void worker_cb(EV_P_ ev_async *w, const int revents) {
    wctx *ctx = w->data;
    cnode *p, *res;
    while ((p = cq_pop(ctx->q))) {
        if (p == (cnode *) STOP_MAGIC) {
            ev_async_stop(loop, w);
            ev_break(loop, EVBREAK_ALL);
            return;
        }
        res = ctx->f(p);
        cq_put(ctx->rq, res);
        ev_async_send(ctx->master, ctx->rev);
    }
}

void *worker_f(void *arg) {
    wctx *ctx = (wctx *) arg;

    ctx->loop = ev_loop_new(0);
    ev_async_init(&ctx->wev, worker_cb);
    ctx->wev.data = ctx;
    ev_async_start(ctx->loop, &ctx->wev);

    pthread_barrier_wait(&barrier);

    ev_run(ctx->loop, 0);
    return NULL;
}

void pool_init(ThreadPool *pool, bool (*res_cb)(cnode *)) {
    if (!pool)
        return;
    pthread_barrier_init(&barrier, NULL, WORKERS + 1);
    pool->rr_idx = 0;
    pool->res_cb = res_cb;
    // get default Loop
    // NOTE: it should be main() calling ev_run on the default loop.
    pool->loop = ev_default_loop(0);
    // setup rev
    ev_async_init(&pool->rev, pool_cb);
    pool->rev.data = pool;
    ev_async_start(pool->loop, &pool->rev);
    // setup result queue
    pool->result_q = cq_init(NULL, QUEUESIZE * WORKERS);
}
void pool_start(ThreadPool *pool, cnode *(*f)(cnode *) ) {
    for (int i = 0; i < WORKERS; i++) {
        wctx *w = calloc(1, sizeof(wctx));

        w->id = i;
        w->rev = &pool->rev;
        w->q = cq_init(NULL, QUEUESIZE);
        w->rq = pool->result_q;
        w->f = f;
        w->master = pool->loop;
        pthread_create(&w->thread, NULL, worker_f, w);

        pool->workers[i] = w;
    }
    pthread_barrier_wait(&barrier);
}
void pool_post(ThreadPool *pool, cnode *work) {
    wctx *w = pool->workers[pool->rr_idx];
    pool->rr_idx = (pool->rr_idx + 1) % WORKERS;
    cq_put(w->q, work);
    ev_async_send(w->loop, &w->wev);
}
void pool_stop(ThreadPool *pool) {
    for (int i = 0; i < WORKERS; i++) {
        wctx *w = pool->workers[i];
        cq_put(w->q, (cnode *) STOP_MAGIC);
        ev_async_send(w->loop, &w->wev);
    }

    for (int i = 0; i < WORKERS; i++) {
        wctx *w = pool->workers[i];
        pool->workers[i] = NULL;

        pthread_join(w->thread, NULL);
        ev_loop_destroy(w->loop);
        cq_destroy(w->q);
        free(w);
    }
}
void pool_destroy(ThreadPool *pool) {
    ev_async_stop(pool->loop, &pool->rev);
    cq_destroy(pool->result_q);
}
