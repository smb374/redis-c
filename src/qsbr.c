#include "qsbr.h"

#include <assert.h>
#include <pthread.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>

#include "cqueue.h"
#include "utils.h"

struct QSBR {
    alignas(64) atomic_u64 quiescent;
    alignas(64) atomic_u64 active;
    alignas(64) pthread_mutex_t lock;
    cqueue *curr, *prev;
};
typedef struct QSBR QSBR;

struct Node {
    void (*cb)(void *arg); // callback run before free
    alignas(8) atomic_bool retired; // Double retire guard
    alignas(16) cnode qnode; // MPSC queue node
};
typedef struct Node Node;

static QSBR gc;
static __thread int TID = -1;

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

static void process_queue(cqueue *q) {
    if (!q)
        return;
    cnode *qnode = NULL;
    while ((qnode = cq_pop(q))) {
        Node *node = container_of(qnode, Node, qnode);
        if (node->cb) {
            node->cb(node_to_ptr(node));
        }
        free(node);
    }
}

void qsbr_init(size_t back_logs) {
    gc.quiescent = 0;
    gc.active = 0;
    pthread_mutex_init(&gc.lock, NULL);
    gc.curr = cq_init(NULL, back_logs);
    gc.prev = cq_init(NULL, back_logs);
    atomic_thread_fence(RELEASE);
}

void qsbr_reg() {
    if (TID == -1) {
        u64 lactive = LOAD(&gc.active, ACQUIRE);
        int slot;
        do {
            slot = ffsll(~(i64) lactive) - 1;
            assert(slot != -1 && "Too many threads");
        } while (!CMPXCHG(&gc.active, &lactive, lactive | (1ULL << slot), RELEASE, ACQUIRE));
        TID = slot;
    }
}

void qsbr_unreg() {
    if (TID != -1) {
        FAAND(&gc.active, ~(1ULL << TID), ACQ_REL);
        TID = -1;
    }
}

void *qsbr_calloc(size_t nmemb, size_t size) {
    assert(TID != -1 && "Thread not registered");
    Node *node = calloc(1, sizeof(Node) + nmemb * size);
    if (!node)
        return NULL;

    STORE(&node->retired, false, RELAXED);
    return node_to_ptr(node);
}

void qsbr_retire(void *ptr, void (*cb)(void *)) {
    assert(TID != -1 && "Thread not registered");
    if (!ptr)
        return;

    Node *node = ptr_to_node(ptr);

    bool expected = false;
    if (!CMPXCHG(&node->retired, &expected, true, ACQ_REL, RELAXED)) {
        return;
    }

    node->cb = cb;
    cq_put(gc.curr, &node->qnode);
}

void qsbr_quiescent() {
    uint64_t loc = 1ULL << TID;
    uint64_t q = FAOR(&gc.quiescent, loc, ACQ_REL);
    uint64_t active = LOAD(&gc.active, ACQUIRE);
    if ((q | loc) == active) {
        if (!pthread_mutex_trylock(&gc.lock)) {
            q = LOAD(&gc.quiescent, ACQUIRE);
            active = LOAD(&gc.active, ACQUIRE);
            if (q == active) {
                // Drain prev
                process_queue(gc.prev);
                // Safe, as threads putting to curr will use the same pointer,
                // but we treat unfinished put as previous interval.
                cqueue *oprev = gc.prev;
                gc.prev = gc.curr;
                gc.curr = oprev;
                STORE(&gc.quiescent, 0, RELEASE);
            }
            pthread_mutex_unlock(&gc.lock);
        }
    }
}
// NOTE: Assumes exclusive access on destroy
void qsbr_destroy() {
    process_queue(gc.prev);
    process_queue(gc.curr);
    cq_destroy(gc.prev);
    cq_destroy(gc.curr);
    gc.prev = NULL;
    gc.curr = NULL;
    pthread_mutex_destroy(&gc.lock);
    atomic_init(&gc.quiescent, 0);
    atomic_init(&gc.active, 0);
}
