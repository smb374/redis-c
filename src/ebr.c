#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

#include "ebr.h"
#include "strings.h"
#include "utils.h"

static __thread ebr_tstate tstate = {
        .active = false,
        .local_epoch = 0ULL,
        .garbages = {NULL},
        .gsize = 0,
        .idx = -1,
};

ebr_manager *ebr_new() {
    ebr_manager *m = calloc(1, sizeof(ebr_manager));
    ebr_init(m);
    return m;
}
void ebr_init(ebr_manager *m) {
    pthread_mutex_init(&m->lock, NULL);
    for (int i = 0; i < N_EPOCHS; i++) {
        pthread_mutex_init(&m->garbage_lock[i], NULL);
        m->garbages[i] = NULL;
    }
    atomic_init(&m->epoch, 0);
    bzero(m->garbages, sizeof(ebr_ptr *) * N_EPOCHS);
    bzero(m->tstates, sizeof(ebr_tstate *) * MAX_THREADS);
}
void ebr_destroy(ebr_manager *m) {
    pthread_mutex_destroy(&m->lock);
    for (int i = 0; i < N_EPOCHS; i++) {
        pthread_mutex_destroy(&m->garbage_lock[i]);
        ebr_ptr *p = m->garbages[i], *next;
        while (p) {
            next = p->next;
            free(p);
            p = next;
        }
        m->garbages[i] = NULL;
    }
    atomic_init(&m->epoch, 0);
    bzero(m->tstates, sizeof(ebr_tstate *) * MAX_THREADS);
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
void ebr_free(ebr_manager *m, void *ptr) {
    if (!ptr)
        return;
    ebr_ptr *p = container_of(ptr, ebr_ptr, ptr);
    if (!p || p->mark)
        return;

    p->mark = true;
    tstate.garbages[tstate.gsize++] = p;
    if (tstate.gsize >= MAX_LOCAL_GARBAGES) {
        uint64_t epoch = atomic_load_explicit(&m->epoch, memory_order_relaxed);
        uint64_t idx = epoch % N_EPOCHS;
        ebr_ptr *node;
        pthread_mutex_lock(&m->garbage_lock[idx]);
        for (int i = 0; i < MAX_LOCAL_GARBAGES; i++) {
            node = tstate.garbages[i];
            node->next = m->garbages[idx];
            m->garbages[idx] = node;
        }
        pthread_mutex_unlock(&m->garbage_lock[idx]);
        tstate.gsize = 0;
    }

    ebr_try_reclaim(m);
}
bool ebr_reg(ebr_manager *m) {
    bool result = false;
    ebr_tstate *my_tstate = &tstate;
    pthread_mutex_lock(&m->lock);
    for (int i = 0; i < MAX_THREADS; ++i) {
        if (m->tstates[i] == NULL) {
            m->tstates[i] = my_tstate;
            tstate.idx = i;
            result = true;
            break;
        }
    }
    pthread_mutex_unlock(&m->lock);
    return result;
}
void ebr_unreg(ebr_manager *m) {
    if (tstate.gsize > 0) {
        uint64_t epoch = atomic_load_explicit(&m->epoch, memory_order_relaxed);
        uint64_t idx = epoch % N_EPOCHS;
        ebr_ptr *node;
        pthread_mutex_lock(&m->garbage_lock[idx]);
        for (int i = 0; i < tstate.gsize; i++) {
            node = tstate.garbages[i];
            node->next = m->garbages[idx];
            m->garbages[idx] = node;
        }
        pthread_mutex_unlock(&m->garbage_lock[idx]);
        tstate.gsize = 0;
    }

    pthread_mutex_lock(&m->lock);
    m->tstates[tstate.idx] = NULL;
    tstate.idx = -1;
    pthread_mutex_unlock(&m->lock);
}
void ebr_try_reclaim(ebr_manager *m) {
    uint64_t epoch = atomic_load_explicit(&m->epoch, memory_order_relaxed);
    bool can_advance = true;

    pthread_mutex_lock(&m->lock);
    for (int i = 0; i < MAX_THREADS; i++) {
        ebr_tstate *state = m->tstates[i];
        if (!state)
            continue;
        if (atomic_load_explicit(&state->active, memory_order_acquire) &&
            atomic_load_explicit(&state->local_epoch, memory_order_relaxed) < epoch) {
            can_advance = false;
            break;
        }
    }
    pthread_mutex_unlock(&m->lock);

    if (!can_advance)
        return;

    atomic_fetch_add_explicit(&m->epoch, 1, memory_order_acq_rel);
    uint64_t nepoch = epoch + 1;
    uint64_t idx = (nepoch - 2) % N_EPOCHS;
    ebr_ptr *ptr, *next;
    pthread_mutex_lock(&m->garbage_lock[idx]);
    ptr = m->garbages[idx];
    m->garbages[idx] = NULL;
    pthread_mutex_unlock(&m->garbage_lock[idx]);
    while (ptr) {
        next = ptr->next;
        free(ptr);
        ptr = next;
    }
}
bool ebr_enter(ebr_manager *m) {
    if (tstate.idx == -1)
        return false;
    atomic_store_explicit(&tstate.local_epoch, atomic_load_explicit(&m->epoch, memory_order_acquire),
                          memory_order_release);
    atomic_store_explicit(&tstate.active, true, memory_order_release);
    return true;
}
void ebr_leave(ebr_manager *m) { atomic_store_explicit(&tstate.active, false, memory_order_release); }
