#include "cskiplist.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <strings.h>

#include "qsbr.h"

// Should work on 8-byte aligned & above pointers on 64-bit machines
#define TAG_MASK 0x7
#define PTR_MASK ~TAG_MASK

// CSKIPLIST_MAX_LEVELS = 64
static int32_t grand() {
    // Mask is to handle when random() generates 0xFFFFFFFFFFFFFFFF
    // s.t. the ffs can report 64 on 0x8000000000000000
    return __builtin_ffsll(~(random() & 0x7FFFFFFFFFFFFFFF));
}

static bool is_marked(void *ptr) { return ((uintptr_t) ptr & TAG_MASK) != 0; }
static void *tag_ptr(void *ptr, int tag) { return (void *) (((uintptr_t) ptr & PTR_MASK) | (tag & TAG_MASK)); }
static void *untag_ptr(void *ptr) { return (void *) ((uintptr_t) ptr & PTR_MASK); }

static void mark_node_ptrs(CSNode *x) {
    for (int i = x->level - 1; i >= 0; i--) {
        CSNode *x_next;
        for (;;) {
            x_next = atomic_load_explicit(&x->next[i], memory_order_acquire);
            if (is_marked(x_next))
                break;

            bool snip = atomic_compare_exchange_strong_explicit(&x->next[i], &x_next, tag_ptr(x_next, 1),
                                                                memory_order_acq_rel, memory_order_relaxed);
            if (snip)
                break;
        }
    }
}

static void csl_search(CSList *l, uint64_t key, CSNode *preds[], CSNode *succs[]) {
    CSNode *pred, *succ, *pnext, *snext;
RETRY:
    pred = &l->head;
    for (int i = CSKIPLIST_MAX_LEVELS - 1; i >= 0; i--) {
        pnext = atomic_load_explicit(&pred->next[i], memory_order_acquire);
        if (is_marked(pnext))
            goto RETRY;
        for (succ = pnext;; succ = snext) {
            for (;;) {
                snext = atomic_load_explicit(&succ->next[i], memory_order_acquire);
                if (!is_marked(snext))
                    break;
                succ = untag_ptr(snext);
            }
            if (succ->key >= key)
                break;
            pred = succ;
            pnext = snext;
        }

        if (pnext != succ && !atomic_compare_exchange_strong_explicit(&pred->next[i], &pnext, succ,
                                                                      memory_order_acq_rel, memory_order_relaxed)) {
            goto RETRY;
        }
        if (pnext != succ && i == 0) {
            CSNode *curr = pnext;
            while (curr != succ) {
                CSNode *next = untag_ptr(atomic_load_explicit(&curr->next[i], memory_order_acquire));
                qsbr_alloc_cb(g_qsbr_gc, free, curr);
                curr = next;
            }
        }
        preds[i] = pred;
        succs[i] = succ;
    }
}

CSList *csl_new(CSList *l) {
    if (!l) {
        l = calloc(1, sizeof(CSList));
        assert(l);
        l->is_alloc = true;
    } else {
        l->is_alloc = false;
    }

    l->head.key = 0;
    l->head.level = CSKIPLIST_MAX_LEVELS;
    l->head.ptr = NULL;
    l->tail.key = UINT64_MAX;
    l->tail.level = CSKIPLIST_MAX_LEVELS;
    l->tail.ptr = NULL;

    bzero(l->tail.next, sizeof(CSNode *) * CSKIPLIST_MAX_LEVELS);
    for (int i = 0; i < CSKIPLIST_MAX_LEVELS; i++) {
        l->head.next[i] = &l->tail;
    }

    return l;
}

void csl_destroy(CSList *l) {
    CSNode *curr = l->head.next[0];

    while (curr != &l->tail) {
        CSNode *next = curr->next[0];
        free(curr);
        curr = next;
    }

    if (l->is_alloc) {
        free(l);
    }
}

void *csl_lookup(CSList *l, uint64_t key) {
    CSNode *preds[CSKIPLIST_MAX_LEVELS], *succs[CSKIPLIST_MAX_LEVELS];
    csl_search(l, key, preds, succs);

    return (succs[0]->key == key) ? atomic_load_explicit(&succs[0]->ptr, memory_order_acquire) : NULL;
}

void *csl_remove(CSList *l, uint64_t key) {
    CSNode *preds[CSKIPLIST_MAX_LEVELS], *succs[CSKIPLIST_MAX_LEVELS];
    void *val;
    csl_search(l, key, preds, succs);

    if (succs[0]->key != key)
        return NULL;

    for (;;) {
        val = atomic_load_explicit(&succs[0]->ptr, memory_order_acquire);
        if (!val)
            return NULL;
        bool snip = atomic_compare_exchange_strong_explicit(&succs[0]->ptr, &val, NULL, memory_order_acq_rel,
                                                            memory_order_relaxed);
        if (snip)
            break;
    }

    mark_node_ptrs(succs[0]);
    // Force remove
    csl_search(l, key, preds, succs);
    return val;
}

void *csl_update(CSList *l, uint64_t key, void *val) {
    bool snip;
    CSNode *preds[CSKIPLIST_MAX_LEVELS], *succs[CSKIPLIST_MAX_LEVELS];
    CSNode *nnode = calloc(1, sizeof(CSNode)), *pred, *succ, *nnext;
    nnode->level = grand();
    nnode->key = key;
    atomic_init(&nnode->ptr, val);

RETRY:
    csl_search(l, key, preds, succs);
    if (succs[0]->key == key) {
        for (;;) {
            void *oval = atomic_load_explicit(&succs[0]->ptr, memory_order_acquire);
            if (!oval) {
                mark_node_ptrs(succs[0]);
                goto RETRY;
            }
            snip = atomic_compare_exchange_strong_explicit(&succs[0]->ptr, &oval, val, memory_order_acq_rel,
                                                           memory_order_relaxed);
            if (snip) {
                // Never inserted, OK to free directly
                free(nnode);
                return oval;
            }
        }
    }

    for (int i = 0; i < nnode->level; i++) {
        atomic_init(&nnode->next[i], succs[i]);
    }
    snip = atomic_compare_exchange_strong_explicit(&preds[0]->next[0], &succs[0], nnode, memory_order_acq_rel,
                                                   memory_order_relaxed);
    if (!snip)
        goto RETRY;

    for (int i = 1; i < nnode->level; i++) {
        for (;;) {
            pred = preds[i];
            succ = succs[i];
            nnext = untag_ptr(nnode->next[i]);

            if (nnext != succ && !atomic_compare_exchange_strong_explicit(&nnode->next[i], &nnext, succ,
                                                                          memory_order_acq_rel, memory_order_relaxed)) {
                break;
            }

            if (succ->key == key) {
                succ = untag_ptr(succ->next[i]);
            }
            snip = atomic_compare_exchange_strong_explicit(&pred->next[i], &succ, nnode, memory_order_acq_rel,
                                                           memory_order_relaxed);
            if (snip)
                break;

            csl_search(l, key, preds, succs);
        }
    }

    return NULL;
}
