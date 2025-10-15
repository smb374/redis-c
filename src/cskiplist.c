#include "cskiplist.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <strings.h>
#include <time.h>

#include "crystalline.h"
#include "utils.h"

// Should work on 8-byte aligned & above pointers on 64-bit machines
#define TAG_MASK 0x7
#define PTR_MASK ~TAG_MASK

static bool is_marked(void *ptr) { return ((uintptr_t) ptr & TAG_MASK) != 0; }
static void *tag_ptr(void *ptr, int tag) { return (void *) (((uintptr_t) ptr & PTR_MASK) | (tag & TAG_MASK)); }
static void *untag_ptr(void *ptr) { return (void *) ((uintptr_t) ptr & PTR_MASK); }

static bool seeded = false;
// CSKIPLIST_MAX_LEVELS = 64
static int32_t grand() {
    // Mask is to handle when random() generates 0xFFFFFFFFFFFFFFFF
    // s.t. the ffs can report 64 on 0x8000000000000000
    return ffsll(~(random() & 0x7FFFFFFFFFFFFFFF));
}

int cskey_cmp(CSKey l, CSKey r) {
    return (l.key ^ r.key) ? (l.key > r.key) - (l.key < r.key) : (l.nonce > r.nonce) - (l.nonce < r.nonce);
}

static void mark_node_ptrs(CSNode *x) {
    for (int i = x->level - 1; i >= 0; i--) {
        CSNode *x_next;
        for (;;) {
            x_next = LOAD(&x->next[i], memory_order_acquire);
            if (is_marked(x_next))
                break;

            bool snip = CMPXCHG(&x->next[i], &x_next, tag_ptr(x_next, 1), memory_order_acq_rel, memory_order_relaxed);
            if (snip)
                break;
        }
    }
}

static void csl_search(CSList *l, CSKey key, CSNode *preds[], CSNode *succs[]) {
    CSNode *pred, *succ, *pnext, *snext;
RETRY:
    pred = &l->head;
    for (int i = CSKIPLIST_MAX_LEVELS - 1; i >= 0; i--) {
        pnext = LOAD(&pred->next[i], memory_order_acquire);
        if (is_marked(pnext))
            goto RETRY;
        pnext = gc_protect((void **) &pnext, 0);
        for (succ = pnext;; succ = snext) {
            for (;;) {
                snext = LOAD(&succ->next[i], memory_order_acquire);
                if (!is_marked(snext))
                    break;
                succ = untag_ptr(snext);
                succ = gc_protect((void **) &succ, 0);
            }
            if (cskey_cmp(succ->key, key) >= 0)
                break;
            pred = succ;
            pnext = snext;
        }

        if (pnext != succ && !CMPXCHG(&pred->next[i], &pnext, succ, memory_order_acq_rel, memory_order_relaxed)) {
            goto RETRY;
        }
        if (pnext != succ && i == 0) {
            CSNode *curr = pnext;
            while (curr != succ) {
                CSNode *next = untag_ptr(LOAD(&curr->next[i], memory_order_acquire));
                gc_retire(curr);
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

    if (!seeded) {
        srand(time(NULL));
        seeded = true;
    }

    l->head.key = (CSKey) {0, 0};
    l->head.level = CSKIPLIST_MAX_LEVELS;
    l->head.ptr = NULL;
    l->tail.key = (CSKey) {UINT64_MAX, UINT64_MAX};
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
        gc_retire(curr);
        curr = next;
    }

    if (l->is_alloc) {
        free(l);
    }
}

void *csl_lookup(CSList *l, CSKey key) {
    CSNode *preds[CSKIPLIST_MAX_LEVELS], *succs[CSKIPLIST_MAX_LEVELS];
    csl_search(l, key, preds, succs);

    return (!cskey_cmp(succs[0]->key, key)) ? LOAD(&succs[0]->ptr, memory_order_acquire) : NULL;
}

void *csl_remove(CSList *l, CSKey key) {
    CSNode *preds[CSKIPLIST_MAX_LEVELS], *succs[CSKIPLIST_MAX_LEVELS];
    void *val;
    csl_search(l, key, preds, succs);

    if (cskey_cmp(succs[0]->key, key))
        return NULL;

    for (;;) {
        val = LOAD(&succs[0]->ptr, memory_order_acquire);
        if (!val)
            return NULL;
        bool snip = CMPXCHG(&succs[0]->ptr, &val, NULL, memory_order_acq_rel, memory_order_relaxed);
        if (snip)
            break;
    }

    mark_node_ptrs(succs[0]);
    // Force remove
    csl_search(l, key, preds, succs);
    return val;
}

CSKey csl_find_min_key(CSList *l) {
    CSNode *node, *succ;
    node = &l->head;
    for (;;) {
        succ = LOAD(&node->next[0], memory_order_acquire);
        if (!is_marked(succ))
            break;
        node = untag_ptr(succ);
    }
    node = succ;

    return node->key;
}

void *csl_pop_min(CSList *l) {
    CSNode *node, *succ, *preds[CSKIPLIST_MAX_LEVELS], *succs[CSKIPLIST_MAX_LEVELS];
    void *val;

RETRY:
    node = &l->head;
    for (;;) {
        succ = LOAD(&node->next[0], memory_order_acquire);
        if (!is_marked(succ))
            break;
        node = untag_ptr(succ);
    }
    node = succ;

    if (!cskey_cmp(node->key, l->tail.key))
        return NULL;

    val = LOAD(&node->ptr, memory_order_acquire);
    if (!val || !CMPXCHG(&node->ptr, &val, NULL, memory_order_acq_rel, memory_order_relaxed)) {
        goto RETRY;
    }

    mark_node_ptrs(node);
    csl_search(l, node->key, preds, succs);

    return val;
}

void *csl_update(CSList *l, CSKey key, void *val) {
    bool snip;
    CSNode *preds[CSKIPLIST_MAX_LEVELS], *succs[CSKIPLIST_MAX_LEVELS];
    CSNode *nnode = gc_calloc(1, sizeof(CSNode)), *pred, *succ, *nnext;
    nnode->level = grand();
    nnode->key = key;
    atomic_init(&nnode->ptr, val);

RETRY:
    csl_search(l, key, preds, succs);
    if (!cskey_cmp(succs[0]->key, key)) {
        for (;;) {
            void *oval = LOAD(&succs[0]->ptr, memory_order_acquire);
            if (!oval) {
                mark_node_ptrs(succs[0]);
                goto RETRY;
            }
            snip = CMPXCHG(&succs[0]->ptr, &oval, val, memory_order_acq_rel, memory_order_relaxed);
            if (snip) {
                gc_retire(nnode);
                return oval;
            }
        }
    }

    for (int i = 0; i < nnode->level; i++) {
        atomic_init(&nnode->next[i], succs[i]);
    }
    snip = CMPXCHG(&preds[0]->next[0], &succs[0], nnode, memory_order_acq_rel, memory_order_relaxed);
    if (!snip)
        goto RETRY;

    for (int i = 1; i < nnode->level; i++) {
        for (;;) {
            pred = preds[i];
            succ = succs[i];
            nnext = untag_ptr(nnode->next[i]);

            if (nnext != succ && !CMPXCHG(&nnode->next[i], &nnext, succ, memory_order_acq_rel, memory_order_relaxed)) {
                break;
            }

            if (!cskey_cmp(succ->key, key)) {
                succ = untag_ptr(succ->next[i]);
            }
            snip = CMPXCHG(&pred->next[i], &succ, nnode, ACQ_REL, RELAXED);
            if (snip)
                break;

            csl_search(l, key, preds, succs);
        }
    }

    return NULL;
}
