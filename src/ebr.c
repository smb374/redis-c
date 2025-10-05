#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include "ebr.h"
#include "strings.h"
#include "utils.h"

#define MAX_THREADS 64
#define N_EPOCHS 3
#define MAX_LOCAL_GARBAGES 64
typedef _Atomic(uint64_t) atomic_u64;

struct ebr_ptr {
    struct ebr_ptr *next;
    bool mark;
    uint8_t ptr[];
};
typedef struct ebr_ptr ebr_ptr;

struct ebr_tstate {
    atomic_bool active;
    atomic_u64 local_epoch;
    ebr_ptr *garbages[MAX_LOCAL_GARBAGES];
    size_t gsize;
    int idx;
};
typedef struct ebr_tstate ebr_tstate;

struct ebr_manager {
    atomic_u64 epoch;
    pthread_mutex_t lock, garbage_lock[N_EPOCHS];
    ebr_ptr *garbages[N_EPOCHS];
    ebr_tstate *tstates[MAX_THREADS];
};
typedef struct ebr_manager ebr_manager;

static ebr_manager em;
static __thread ebr_tstate tstate = {
        .active = false,
        .local_epoch = 0ULL,
        .garbages = {NULL},
        .gsize = 0,
        .idx = -1,
};
static pthread_once_t ebr_init_done = PTHREAD_ONCE_INIT;

void ebr_init() {
    pthread_mutex_init(&em.lock, NULL);
    for (int i = 0; i < N_EPOCHS; i++) {
        pthread_mutex_init(&em.garbage_lock[i], NULL);
        em.garbages[i] = NULL;
    }
    atomic_init(&em.epoch, 0);
    bzero(em.garbages, sizeof(ebr_ptr *) * N_EPOCHS);
    bzero(em.tstates, sizeof(ebr_tstate *) * MAX_THREADS);
}

void ebr_clear() {
    pthread_mutex_lock(&em.lock);
    for (int i = 0; i < N_EPOCHS; i++) {
        ebr_ptr *p = em.garbages[i], *next;
        while (p) {
            next = p->next;
            free(p);
            p = next;
        }
        em.garbages[i] = NULL;
    }
    atomic_init(&em.epoch, 0);
    bzero(em.tstates, sizeof(ebr_tstate *) * MAX_THREADS);
    pthread_mutex_unlock(&em.lock);
}
void *ebr_calloc(size_t nmemb, size_t size) {
    ebr_ptr *p = calloc(1, sizeof(ebr_ptr) + (nmemb * size));
    return p ? p->ptr : NULL;
}
void *ebr_realloc(void *ptr, size_t size) {
    if (!ptr) {
        ebr_ptr *p = calloc(1, sizeof(ebr_ptr) + size);
        return p ? p->ptr : NULL;
    } else {
        ebr_ptr *p = container_of(ptr, ebr_ptr, ptr);
        if (p->mark)
            return NULL;
        ebr_ptr *np = realloc(p, sizeof(ebr_ptr) + size);
        np->mark = false;
        return np ? np->ptr : p->ptr;
    }
}
void ebr_free(void *ptr) {
    if (!ptr)
        return;
    ebr_ptr *p = container_of(ptr, ebr_ptr, ptr);
    if (!p || p->mark)
        return;

    p->mark = true;
    tstate.garbages[tstate.gsize++] = p;
    if (tstate.gsize >= MAX_LOCAL_GARBAGES) {
        uint64_t epoch = atomic_load_explicit(&em.epoch, memory_order_relaxed);
        uint64_t idx = epoch % N_EPOCHS;
        ebr_ptr *node;
        pthread_mutex_lock(&em.garbage_lock[idx]);
        for (int i = 0; i < MAX_LOCAL_GARBAGES; i++) {
            node = tstate.garbages[i];
            node->next = em.garbages[idx];
            em.garbages[idx] = node;
        }
        pthread_mutex_unlock(&em.garbage_lock[idx]);
        tstate.gsize = 0;
    }

    ebr_try_reclaim();
}
bool ebr_reg() {
    pthread_once(&ebr_init_done, ebr_init);
    bool result = false;
    ebr_tstate *my_tstate = &tstate;
    pthread_mutex_lock(&em.lock);
    for (int i = 0; i < MAX_THREADS; ++i) {
        if (em.tstates[i] == NULL) {
            em.tstates[i] = my_tstate;
            tstate.idx = i;
            result = true;
            break;
        }
    }
    pthread_mutex_unlock(&em.lock);
    return result;
}
void ebr_unreg() {
    if (tstate.gsize > 0) {
        uint64_t epoch = atomic_load_explicit(&em.epoch, memory_order_relaxed);
        uint64_t idx = epoch % N_EPOCHS;
        ebr_ptr *node;
        pthread_mutex_lock(&em.garbage_lock[idx]);
        for (int i = 0; i < tstate.gsize; i++) {
            node = tstate.garbages[i];
            node->next = em.garbages[idx];
            em.garbages[idx] = node;
        }
        pthread_mutex_unlock(&em.garbage_lock[idx]);
        tstate.gsize = 0;
    }

    pthread_mutex_lock(&em.lock);
    em.tstates[tstate.idx] = NULL;
    tstate.idx = -1;
    pthread_mutex_unlock(&em.lock);
}
void ebr_try_reclaim() {
    uint64_t epoch = atomic_load_explicit(&em.epoch, memory_order_relaxed);
    bool can_advance = true;

    pthread_mutex_lock(&em.lock);
    for (int i = 0; i < MAX_THREADS; i++) {
        ebr_tstate *state = em.tstates[i];
        if (!state)
            continue;
        if (atomic_load_explicit(&state->active, memory_order_acquire) &&
            atomic_load_explicit(&state->local_epoch, memory_order_relaxed) < epoch) {
            can_advance = false;
            break;
        }
    }
    pthread_mutex_unlock(&em.lock);

    if (!can_advance)
        return;

    atomic_fetch_add_explicit(&em.epoch, 1, memory_order_acq_rel);
    uint64_t nepoch = epoch + 1;
    uint64_t idx = (nepoch - 2) % N_EPOCHS;
    ebr_ptr *ptr, *next;
    pthread_mutex_lock(&em.garbage_lock[idx]);
    ptr = em.garbages[idx];
    em.garbages[idx] = NULL;
    pthread_mutex_unlock(&em.garbage_lock[idx]);
    while (ptr) {
        next = ptr->next;
        free(ptr);
        ptr = next;
    }
}
bool ebr_enter() {
    pthread_once(&ebr_init_done, ebr_init);
    if (tstate.idx == -1)
        return false;
    atomic_store_explicit(&tstate.local_epoch, atomic_load_explicit(&em.epoch, memory_order_acquire),
                          memory_order_release);
    atomic_store_explicit(&tstate.active, true, memory_order_release);
    return true;
}
void ebr_leave() { atomic_store_explicit(&tstate.active, false, memory_order_release); }
