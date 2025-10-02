//
// Created by poyehchen on 10/1/25.
//
#include "thread_pool.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/param.h>

void pool_init(ThreadPool *pool) {
    pool->res_ev = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (pool->res_ev == -1) {
        perror("eventfd");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < WORKERS; i++) {
        pool->worker_evs[i] = eventfd(0, EFD_CLOEXEC);
        if (pool->worker_evs[i] == -1) {
            perror("eventfd");
            exit(EXIT_FAILURE);
        }
    }
    for (int i = 0; i < WORKERS; i++) {
        pool->worker_qs[i] = cq_init(NULL, QUEUESIZE);
    }
    pool->result_q = cq_init(NULL, QUEUESIZE * WORKERS);
    pool->idx = 0;
}

struct worker_arg {
    cqueue *q, *rq;
    int ev, rev, id;
    cnode * (*f)(cnode *);
};

void *worker_f(void *arg) {
    const struct worker_arg *w = (struct worker_arg *) arg;
    uint64_t works = 0;
    ssize_t ret;
    int err;

    for (;;) {
        // Get number of works need to be collected from q
        for (;;) {
            errno = 0;
            ret = read(w->ev, &works, sizeof(uint64_t));
            err = errno;
            if (ret < 0) {
                if (err == EINTR)
                    continue;
                perror("read()");
                goto EPILOGUE;
            }
            break;
        }
        if (works == 0xDEADBEEFCAFEBEEF) {
            goto EPILOGUE;
        }

        uint64_t processed = 0;
        cnode *node;
        while ((node = cq_pop(w->q))) {
            cnode *rnode = w->f(node);
            // Put finished work to rq
            cq_put(w->rq, rnode);
            processed++;
        }

        for (;;) {
            errno = 0;
            ret = write(w->rev, &processed, sizeof(uint64_t));
            err = errno;
            if (ret < 0) {
                if (err == EINTR)
                    continue;
                perror("write()");
                goto EPILOGUE;
            }
            break;
        }
    }

EPILOGUE:
    free((void *) w);
    return NULL;
}

void pool_start(ThreadPool *pool, cnode * (*f)(cnode *)) {
    for (int i = 0; i < WORKERS; i++) {
        struct worker_arg *w = calloc(1, sizeof(struct worker_arg));
        w->id = i;
        w->q = pool->worker_qs[i];
        w->rq = pool->result_q;
        w->ev = pool->worker_evs[i];
        w->rev = pool->res_ev;
        w->f = f;

        pthread_create(&pool->workers[i], NULL, worker_f, w);
    }
}

bool pool_post(ThreadPool *pool, cnode *work) {
    cq_put(pool->worker_qs[pool->idx], work);
    const uint64_t val = 1;
    for (;;) {
        errno = 0;
        const ssize_t ret = write(pool->worker_evs[pool->idx], &val, sizeof(uint64_t));
        const int err = errno;
        if (ret < 0) {
            if (err == EINTR)
                continue;
            perror("write");
            return false;
        }
        break;
    }
    pool->idx = (pool->idx + 1) % WORKERS;
    return true;
}

void pool_stop(ThreadPool *pool) {
    const uint64_t val = 0xDEADBEEFCAFEBEEF;
    for (int i = 0; i < WORKERS; i++) {
        // Closing the fd will cause the blocking read() in the worker to fail.
        write(pool->worker_evs[i], &val, sizeof(uint64_t));
    }

    for (int i = 0; i < WORKERS; i++) {
        pthread_join(pool->workers[i], NULL);
    }
}

void pool_destroy(ThreadPool *pool) {
    for (int i = 0; i < WORKERS; i++) {
        close(pool->worker_evs[i]);
        pool->worker_evs[i] = -1;
        cq_destroy(pool->worker_qs[i]);
        pool->worker_qs[i] = NULL;
    }
    close(pool->res_ev);
    pool->res_ev = -1;
    cq_destroy(pool->result_q);
    pool->result_q = NULL;
}
