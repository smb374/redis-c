#include "crystalline.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <unistd.h>

#include "utils.h"

const u64 REFC_PROTECT = (1ULL << 63);
void *const INVPTR = ((void *) -1LL);

struct Node;
typedef struct Node Node;
struct Reservation;
typedef struct Reservation Reservation;
struct Batch;
typedef struct Batch Batch;

struct Node {
    union {
        atomic_u64 refc;
        Node *bnext;
    };
    union {
        atomic_u64 birth;
        struct Reservation *slot;
        _Atomic(Node *) next;
    };
    Node *blink;
    void (*on_free)(void *arg);
};
struct Reservation {
    _Atomic(Node *) list;
    atomic_u64 epoch;
};
struct Batch {
    Node *first, *refs;
    int counter;
};

static Reservation rsrv[MAX_THREADS][MAX_IDX] = {};
static atomic_u64 global_epoch = 1;
static atomic_u64 active = 0;
static atomic_u64 allocates = 0;
static atomic_u64 frees = 0;

static __thread int TID = -1;
static __thread Batch batch = {.first = NULL, .refs = NULL, .counter = 0};
static __thread int alloc_cnt = 0;

static void free_batch(Node *refs);
static void traverse(Node *next);
static void try_retire(void);
static u64 update_epoch(u64 curr_epoch, int index);
static Node *ptr_to_node(void *ptr);
static void *node_to_ptr(Node *node);
static void force_retire(void);

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
    for (int i = 0; i < MAX_THREADS; i++) {
        for (int j = 0; j < MAX_IDX; j++) {
            STORE(&rsrv[i][j].list, INVPTR, RELAXED);
            STORE(&rsrv[i][j].epoch, 0, RELAXED);
        }
    }
    allocates = 0;
    frees = 0;
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
    }
}

void gc_unreg(void) {
    if (TID != -1) {
        // Retire local batch
        u64 prev_active = FAAND(&active, ~(1ULL << TID), ACQ_REL);
        if (batch.first) {
            batch.refs->blink = batch.first;
            if (prev_active == (1ULL << TID)) {
                logger(stderr, "INFO", "%d: Big gun!\n", TID);
                force_retire();
            } else {
                try_retire();
            }
        }
        gc_clear();
        TID = -1;
        logger(stderr, "INFO", "[crystalline] allocates = %lu, frees = %lu\n", LOAD(&allocates, RELAXED),
               LOAD(&frees, RELAXED));
    }
}

void *gc_alloc(size_t size) {
    assert(TID != -1 && "Thread not registered");
    if ((alloc_cnt++ % ALLOC_FREQ) == 0) {
        FAA(&global_epoch, 1, ACQ_REL);
    }
    Node *node = calloc(1, sizeof(Node) + size);
    if (!node)
        return NULL;

    node->birth = LOAD(&global_epoch, ACQUIRE);
    node->blink = NULL;

    FAA(&allocates, 1, RELAXED);
    return node_to_ptr(node);
}

void *gc_calloc(size_t nmemb, size_t size) {
    assert(TID != -1 && "Thread not registered");
    if (!(alloc_cnt++ % ALLOC_FREQ)) {
        FAA(&global_epoch, 1, ACQ_REL);
    }
    Node *node = calloc(1, sizeof(Node) + nmemb * size);
    if (!node)
        return NULL;

    node->birth = LOAD(&global_epoch, ACQUIRE);
    node->blink = NULL;

    FAA(&allocates, 1, RELAXED);
    return node_to_ptr(node);
}

static void free_batch(Node *refs) {
    Node *n = refs->blink;
    while (n) {
        Node *obj = n;
        n = n->bnext;
        if (obj->on_free) {
            obj->on_free(node_to_ptr(obj));
        }
        FAA(&frees, 1, RELAXED);
        free(obj);
    }
}

static void traverse(Node *next) {
    while (next != NULL && next != INVPTR) {
        Node *curr = next;
        next = XCHG(&curr->next, INVPTR, ACQ_REL);
        Node *refs = curr->blink;
        if (FAA(&refs->refc, -1, ACQ_REL) == 1) {
            free_batch(refs);
        }
    }
}

// Force evict nodes in a partially filled batch.
static void force_retire(void) {
    if (!batch.first)
        return;

    u64 min_birth = batch.refs->birth;
    Node *curr = batch.first;
    i64 cnt = (i64) -REFC_PROTECT;

    // Attach to as many slots as we have nodes for
    for (int i = 0; i < MAX_THREADS && curr != batch.refs; i++) {
        for (int j = 0; j < MAX_IDX && curr != batch.refs; j++) {
            if (LOAD(&rsrv[i][j].list, ACQUIRE) == INVPTR)
                continue;
            if (LOAD(&rsrv[i][j].epoch, ACQUIRE) < min_birth)
                continue;

            Reservation *slot = &rsrv[i][j];
            if (LOAD(&slot->list, ACQUIRE) == INVPTR)
                continue;

            STORE(&curr->next, NULL, RELEASE);
            Node *prev = XCHG(&slot->list, curr, ACQ_REL);

            if (prev != NULL) {
                if (prev == INVPTR) {
                    Node *expect = curr;
                    if (CMPXCHG(&slot->list, &expect, INVPTR, ACQ_REL, RELAXED))
                        continue;
                } else {
                    Node *expect = NULL;
                    if (!CMPXCHG(&curr->next, &expect, prev, ACQ_REL, RELAXED))
                        traverse(prev);
                }
            }
            cnt++;
            if ((uintptr_t) curr->bnext & REFC_PROTECT) {
                goto EPILOGUE;
            }
            curr = curr->bnext;
        }
    }
EPILOGUE:

    if (FAA(&batch.refs->refc, cnt, ACQ_REL) == -cnt) {
        free_batch(batch.refs);
    }
    batch.first = NULL;
    batch.counter = 0;
}

static void try_retire(void) {
    u64 min_birth = batch.refs->birth;
    Node *last = batch.first;

    for (int i = 0; i < MAX_THREADS; i++) {
        for (int j = 0; j < MAX_IDX; j++) {
            if (LOAD(&rsrv[i][j].list, ACQUIRE) == INVPTR)
                continue;
            if (LOAD(&rsrv[i][j].epoch, ACQUIRE) < min_birth)
                continue;
            if (last == batch.refs) {
                return;
            }
            last->slot = &rsrv[i][j];
            last = last->bnext;
        }
    }

    Node *curr = batch.first;
    i64 cnt = (i64) -REFC_PROTECT;

    for (; curr != last; curr = curr->bnext) {
        Reservation *slot = curr->slot;
        if (LOAD(&slot->list, ACQUIRE) == INVPTR)
            continue;
        STORE(&curr->next, NULL, RELEASE);
        Node *prev = XCHG(&slot->list, curr, ACQ_REL);
        if (prev != NULL) {
            if (prev == INVPTR) {
                Node *expect = curr;
                if (CMPXCHG(&slot->list, &expect, INVPTR, ACQ_REL, RELAXED))
                    continue;
            } else {
                Node *expect = NULL;
                if (!CMPXCHG(&curr->next, &expect, prev, ACQ_REL, RELAXED))
                    traverse(prev);
            }
        }
        cnt++;
    }
    if (FAA(&batch.refs->refc, cnt, ACQ_REL) == -cnt) {
        free_batch(batch.refs);
    }
    batch.first = NULL;
    batch.counter = 0;
}

void gc_retire_custom(void *ptr, void (*on_free)(void *)) {
    assert(TID != -1 && "Thread not registered");
    if (!ptr)
        return;

    Node *node = ptr_to_node(ptr);
    node->on_free = on_free;

    if (!batch.first) {
        batch.refs = node;
        STORE(&node->refc, REFC_PROTECT, RELEASE);
    } else {
        if (batch.refs->birth > node->birth) {
            batch.refs->birth = node->birth;
        }
        node->blink = batch.refs;
        node->bnext = batch.first;
    }
    batch.first = node;

    if ((batch.counter++ % RETIRE_FREQ) == 0) {
        batch.refs->blink = batch.first;
        try_retire();
    }
}

void gc_retire(void *ptr) { gc_retire_custom(ptr, NULL); }

static u64 update_epoch(u64 curr_epoch, int index) {
    Node *list = LOAD(&rsrv[TID][index].list, ACQUIRE);
    if (list != NULL) {
        list = XCHG(&rsrv[TID][index].list, NULL, ACQ_REL);

        if (list != INVPTR) {
            traverse(list);
        }

        curr_epoch = LOAD(&global_epoch, ACQUIRE);
    }
    STORE(&rsrv[TID][index].epoch, curr_epoch, RELEASE);
    return curr_epoch;
}

void *gc_protect(void **obj, int index) {
    assert(TID != -1 && "Thread not registered");
    assert(index < MAX_IDX && "Protection index out of bounds");

    u64 prev_epoch = LOAD(&rsrv[TID][index].epoch, ACQUIRE);
    for (;;) {
        void *ptr = *obj;
        u64 curr_epoch = LOAD(&global_epoch, ACQUIRE);
        if (prev_epoch == curr_epoch)
            return ptr;
        prev_epoch = update_epoch(curr_epoch, index);
    }
}

void gc_clear(void) {
    assert(TID != -1 && "Thread not registered");
    for (int i = 0; i < MAX_IDX; i++) {
        Node *p = XCHG(&rsrv[TID][i].list, INVPTR, ACQ_REL);
        if (p != INVPTR) {
            traverse(p);
        }
    }
}

void gc_force_cleanup(void) {
    assert(TID != -1 && "Thread not registered");

    // Force retire current batch
    if (batch.first) {
        batch.refs->blink = batch.first;
        force_retire();
    }

    // Process all reservation lists
    for (int i = 0; i < MAX_IDX; i++) {
        u64 curr_epoch = LOAD(&global_epoch, ACQUIRE);
        if (LOAD(&rsrv[TID][i].epoch, ACQUIRE) < curr_epoch) {
            update_epoch(curr_epoch, i);
        }
    }
}
