#include "crystalline.h"

#include <assert.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

#include "utils.h"

#define RETIRE_FREQ 120
#define ALLOC_FREQ 1
const uint64_t REFC_PROTECT = 1UL << 63;
void *const INVPTR = (void *) 1;

struct Reservation {
    _Atomic(Node *) list;
    atomic_u64 era;
};
struct Batch {
    Node *first;
    Node *refs;
    int counter;
};
typedef struct Reservation Reservation;
typedef struct Batch Batch;

static Reservation rsrv[MAX_THREADS][MAX_IDX];
static atomic_u64 global_era = 1;
static atomic_int next_tid = 0;

static _Thread_local int TID = -1;
static _Thread_local Batch batch = {.first = NULL, .refs = NULL, .counter = 0};
static _Thread_local int alloc_cnt = 0;

static void free_batch(Node *refs);
static void traverse(Node *next);
static void try_retire(void);
static u64 update_era(u64 curr_era, int index);
static Node *ptr_to_node(void *ptr);
static void *node_to_ptr(Node *node);

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

void gc_init(void) {
    for (int i = 0; i < MAX_THREADS; ++i) {
        for (int j = 0; j < MAX_IDX; ++j) {
            STORE(&rsrv[i][j].list, INVPTR, RELAXED);
            STORE(&rsrv[i][j].era, 0, RELAXED);
        }
    }
}

void gc_reg(void) {
    if (TID == -1) {
        TID = FAA(&next_tid, 1, RELAXED);
        assert(TID < MAX_THREADS && "Exceeded MAX_THREADS");
    }
}

void gc_unreg(void) {
    gc_clear();
    TID = -1;
}

void *gc_alloc(size_t size) {
    assert(TID != -1 && "Thread not registered");
    if (!(alloc_cnt++ % ALLOC_FREQ)) {
        FAA(&global_era, 1, RELAXED);
    }
    Node *node = calloc(1, sizeof(Node) + size);
    if (!node)
        return NULL;

    node->birth = LOAD(&global_era, RELAXED);
    return node_to_ptr(node);
}

void *gc_calloc(size_t nmemb, size_t size) {
    assert(TID != -1 && "Thread not registered");
    if ((alloc_cnt++ % ALLOC_FREQ) == 0) {
        FAA(&global_era, 1, RELAXED);
    }
    Node *node = calloc(1, sizeof(Node) + nmemb * size);
    if (!node)
        return NULL;

    node->birth = LOAD(&global_era, RELAXED);
    return node_to_ptr(node);
}

static void free_batch(Node *refs) {
    Node *n = refs->blink; // First SLOT node
    do {
        Node *obj = n;
        n = n->bnext;
        free(obj);
    } while (n);
}

static void traverse(Node *next) {
    while (next && next != INVPTR) {
        Node *curr = next;
        next = LOAD(&curr->next, ACQUIRE);
        Node *refs = curr->blink;
        if (FAS(&refs->refc, 1, ACQ_REL) == 1) {
            free_batch(refs);
        }
    }
}

static void try_retire(void) {
    u64 min_birth = batch.refs->birth;
    Node *last = batch.first;

    // Phase 1: Check reservations and see if we have enough nodes in the batch.
    for (int i = 0; i < MAX_THREADS; ++i) {
        for (int j = 0; j < MAX_IDX; ++j) {
            if (LOAD(&rsrv[i][j].list, ACQUIRE) == INVPTR)
                continue;
            if (LOAD(&rsrv[i][j].era, ACQUIRE) < min_birth)
                continue;
            if (last == batch.refs)
                return; // Ran out of nodes, must abort and wait for more.
            last->slot = &rsrv[i][j];
            last = last->bnext;
        }
    }

    // Phase 2: Attach nodes to the collected reservation lists.
    Node *curr = batch.first;
    i64 cnt = -REFC_PROTECT;

    for (; curr != last; curr = curr->bnext) {
        Reservation *slot = curr->slot;
        for (;;) {
            Node *prev = LOAD(&slot->list, ACQUIRE);
            if (prev == INVPTR)
                break; // Inactive, do not attach.
            STORE(&curr->next, prev, RELAXED);
            if (WCMPXCHG(&slot->list, &prev, curr, RELEASE, ACQUIRE)) {
                cnt++;
                break;
            }
        }
    }
    if (FAA(&batch.refs->refc, cnt, ACQ_REL) + cnt == 0) {
        free_batch(batch.refs);
    }
    batch.first = NULL;
    batch.counter = 0;
}

void gc_retire(void *ptr) {
    assert(TID != -1 && "Thread not registered");
    Node *node = ptr_to_node(ptr);

    if (!batch.refs) { // This is the first node, it becomes the REFS node.
        batch.refs = node;
        STORE(&node->refc, REFC_PROTECT, RELAXED);
    } else { // This is a SLOT node.
        if (batch.refs->birth > node->birth) {
            batch.refs->birth = node->birth; // Update batch's minimum birth era.
        }
        node->blink = batch.refs;
        node->bnext = batch.first;
    }
    batch.first = node;

    if ((batch.counter++ % RETIRE_FREQ) == 0) {
        batch.refs->blink = batch.first; // The REFS node points to the latest SLOT node.
        try_retire();
    }
}

static u64 update_era(u64 curr_era, int index) {
    if (LOAD(&rsrv[TID][index].list, ACQUIRE)) {
        Node *list = XCHG(&rsrv[TID][index].list, NULL, ACQ_REL);
        if (list != INVPTR) {
            traverse(list);
        }
        curr_era = LOAD(&global_era, ACQUIRE);
    }
    // Set the new era for this protection slot.
    STORE(&rsrv[TID][index].era, curr_era, RELEASE);
    return curr_era;
}

void *gc_protect(void **obj, int index) {
    assert(TID != -1 && "Thread not registered");
    assert(index < MAX_IDX && "Protection index out of bounds");

    u64 prev_era = LOAD(&rsrv[TID][index].era, ACQUIRE);
    for (;;) {
        void *ptr = *obj;
        u64 curr_era = LOAD(&global_era, ACQUIRE);
        if (prev_era == curr_era) {
            return ptr; // Fast path: Era hasn't changed, protection is valid.
        }
        prev_era = update_era(curr_era, index); // Slow path: Update era and retry.
    }
}

void gc_clear(void) {
    assert(TID != -1 && "Thread not registered");
    for (int i = 0; i < MAX_IDX; ++i) {
        Node *p = XCHG(&rsrv[TID][i].list, INVPTR, ACQ_REL);
        if (p != INVPTR) {
            traverse(p);
        }
    }
}
