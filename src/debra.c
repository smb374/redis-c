#include "debra.h"

#include <assert.h>
#include <stdalign.h>
#include <stdlib.h>
#include <strings.h>

#include "utils.h"

struct Node {
    atomic_bool retired;
    struct Node *next;
    void (*on_free)(void *);
};

typedef struct Node Node;

#define MAX_THREADS 64
#define QBIT 0x1ULL

static atomic_u64 epoch = 0;
static atomic_u64 announce[MAX_THREADS] = {};
static atomic_u64 active = 0;
static Node *bags[MAX_THREADS][3] = {};

static __thread int TID = -1;
static __thread int idx = 0;
static __thread u64 check_next = 0;
static __thread u64 ops = 0;

static void reclam(int tid, int bag_idx);

static inline Node *ptr_to_node(void *ptr) {
    if (!ptr)
        return NULL;
    return (Node *) ((char *) ptr - sizeof(Node));
}

static inline void *node_to_ptr(Node *node) {
    if (!node)
        return NULL;
    return (void *) ((char *) node + sizeof(Node));
}

// Call this for zeroing global state.
void gc_init() {
    epoch = 0;
    active = 0;
    bzero(bags, sizeof(Node *) * MAX_THREADS * 3);
    bzero(announce, sizeof(atomic_u64) * MAX_THREADS);
}

void gc_reg(void) {
    if (TID == -1) {
        u64 lactive = LOAD(&active, ACQUIRE);
        int slot;
        do {
            slot = ffsll(~(i64) lactive) - 1;
            assert(slot != -1 && "Too many threads");
        } while (!CMPXCHG(&active, &lactive, lactive | (1ULL << slot), RELEASE, ACQUIRE));
        TID = slot;
        idx = 0;
        check_next = 0;
        ops = 0;
    }
}

void gc_unreg(void) {
    if (TID != -1) {
        FAAND(&active, ~(1ULL << TID), ACQ_REL);
        TID = -1;
    }
}

void gc_clear() {
    for (int i = 0; i < MAX_THREADS; i++) {
        reclam(i, 0);
        reclam(i, 1);
        reclam(i, 2);
    }
    gc_init();
}

void *gc_alloc(size_t size) { return gc_calloc(1, size); }

void *gc_calloc(size_t nmemb, size_t size) {
    assert(TID != -1 && "Thread not registered");
    Node *node = calloc(1, sizeof(Node) + nmemb * size);
    if (!node)
        return NULL;

    STORE(&node->retired, false, RELAXED);
    node->next = NULL;
    return node_to_ptr(node);
}

void gc_retire_custom(void *ptr, void (*on_free)(void *)) {
    assert(TID != -1 && "Thread not registered");
    if (!ptr)
        return;

    Node *node = ptr_to_node(ptr);

    // Atomically check and set the retired flag.
    // If it was already true, just return to prevent a double-free.
    if (XCHG(&node->retired, true, ACQ_REL)) {
        return;
    }

    node->on_free = on_free;

    node->next = bags[TID][idx];
    bags[TID][idx] = node;
}

void gc_retire(void *ptr) { return gc_retire_custom(ptr, NULL); }

// leaveQState
bool gc_enter() {
    bool res = false;
    u64 curr_epoch = LOAD(&epoch, ACQUIRE);

    if (curr_epoch != (LOAD(&announce[TID], ACQUIRE) & ~QBIT)) {
        ops = check_next = 0;
        idx = (idx + 1) % 3;
        reclam(TID, idx);
        res = true;
    }

    if (++ops >= CHECK_THRES) {
        ops = 0;
        u64 otid = check_next % MAX_THREADS;
        u64 other = LOAD(&announce[otid], ACQUIRE);
        if (curr_epoch == (other & ~QBIT) || (other & QBIT)) {
            u64 c = ++check_next;
            if (c >= MAX_THREADS && c >= INCR_THRES) {
                u64 expect = curr_epoch;
                CMPXCHG(&epoch, &expect, curr_epoch + 2, ACQ_REL, RELAXED);
            }
        }
    }
    STORE(&announce[TID], curr_epoch, RELEASE);
    return res;
}

// enterQState
void gc_leave() { FAOR(&announce[TID], QBIT, ACQ_REL); }

static void reclam(int tid, int bag_idx) {
    Node *next = bags[tid][bag_idx];
    bags[tid][bag_idx] = NULL;
    while (next) {
        Node *curr = next;
        next = next->next;
        if (curr->on_free) {
            curr->on_free(node_to_ptr(curr));
        }
        // Diagnostic: poison the pointer to catch use-after-free immediately.
        curr->next = (void *) 0xDEADBEEF;
        free(curr);
    }
}
