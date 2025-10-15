#include "hpmap.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <unistd.h>

#include "crystalline.h"
#include "utils.h"

struct Segment {
    atomic_u64 ts;
    pthread_mutex_t lock;
};

struct Bucket {
    atomic_u64 hop;
    atomic_bool in_use;
    _Atomic(struct BNode *) node;
};

struct HPTable {
    _Atomic(struct HPTable *) next;
    struct Segment *segments;
    struct Bucket *buckets;
    u64 mask, nsegs;
    atomic_u64 size;
    char data[];
};

struct HPMap {
    _Atomic(struct HPTable *) active; // GC
    atomic_u64 migrate_pos, mthreads, size, epoch;
    atomic_bool migrate_started;
    bool is_alloc;
};

static bool find_closer_free_bucket(struct HPTable *t, u64 free_segment, u64 *free_bucket_idx, u64 *free_distance);

#define PTR_TAG 0x8000000000000000

struct HPTable *hpt_new(size_t size) {
    u64 cap = next_pow2(size);
    u64 buckets = cap + INSERT_RANGE, nsegs = buckets / SEGMENT_SIZE + (buckets % SEGMENT_SIZE != 0);
    struct HPTable *t =
            gc_calloc(1, sizeof(struct HPTable) + sizeof(struct Segment) * nsegs + sizeof(struct Bucket) * buckets);
    assert(t);
    t->segments = (struct Segment *) t->data;
    t->buckets = (struct Bucket *) (t->data + sizeof(struct Segment) * nsegs);
    t->mask = cap - 1;
    t->nsegs = nsegs;
    t->next = NULL;

    for (u64 i = 0; i < t->nsegs; i++) {
        STORE(&t->segments[i].ts, 0, RELAXED);
        pthread_mutex_init(&t->segments[i].lock, NULL);
    }

    for (u64 i = 0; i <= (t->mask + INSERT_RANGE); i++) {
        STORE(&t->buckets[i].hop, 0, RELAXED);
        STORE(&t->buckets[i].in_use, false, RELAXED);
        STORE(&t->buckets[i].node, NULL, RELAXED);
    }

    STORE(&t->size, 0, RELAXED);

    return t;
}

// NOTE: don't destroy mutex or can cause possible deadlock
void hpt_destroy(struct HPTable *t) { gc_retire(t); }

struct BNode *hpt_lookup(struct HPTable *t, struct BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 o_seg = o_buc / SEGMENT_SIZE;

    u64 ts_before = LOAD(&t->segments[o_seg].ts, ACQUIRE);
    for (;;) {
        u64 hop = LOAD(&t->buckets[o_buc].hop, RELAXED);
        while (hop > 0) {
            u64 lowest_set = ffsll((i64) hop) - 1;
            u64 curr_idx = (o_buc + lowest_set) & t->mask;
            struct BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
            if (eq(curr_node, k)) {
                return curr_node;
            }
            hop &= ~(1ULL << lowest_set);
        }
        u64 ts_after = LOAD(&t->segments[o_seg].ts, ACQUIRE);
        if (ts_before != ts_after) {
            ts_before = ts_after;
            continue;
        }
        return NULL;
    }
}

struct BNode *hpt_remove(struct HPTable *t, struct BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 o_seg = o_buc / SEGMENT_SIZE;

    pthread_mutex_lock(&t->segments[o_seg].lock);

    u64 hop = LOAD(&t->buckets[o_buc].hop, RELAXED);
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = (o_buc + lowest_set) & t->mask;
        struct BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
        if (eq(curr_node, k)) {
            STORE(&t->buckets[curr_idx].node, NULL, RELAXED);
            STORE(&t->buckets[curr_idx].in_use, false, RELEASE);
            FAAND(&t->buckets[o_buc].hop, ~(1ULL << lowest_set), RELAXED);
            FAS(&t->size, 1, ACQ_REL);
            pthread_mutex_unlock(&t->segments[o_seg].lock);
            return curr_node;
        }
        hop &= ~(1ULL << lowest_set);
    }

    pthread_mutex_unlock(&t->segments[o_seg].lock);
    return NULL;
}

struct BNode *hpt_upsert(struct HPTable *t, struct BNode *n, node_eq eq) {
    u64 hash = n->hcode;
    u64 o_buc = hash & t->mask;
    u64 o_seg = o_buc / SEGMENT_SIZE;

    pthread_mutex_lock(&t->segments[o_seg].lock);

    if (LOAD(&t->next, ACQUIRE) != NULL) {
        pthread_mutex_unlock(&t->segments[o_seg].lock);
        return NULL; // Indicate retry is needed
    }

    u64 hop = LOAD(&t->buckets[o_buc].hop, RELAXED);
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = (o_buc + lowest_set) & t->mask;
        struct BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
        if (eq(curr_node, n)) {
            pthread_mutex_unlock(&t->segments[o_seg].lock);
            return (struct BNode *) ((uintptr_t) curr_node | PTR_TAG); // Key already exists, return existing node
        }
        hop &= ~(1ULL << lowest_set);
    }

    u64 offset = 0;
    u64 res_buc = o_buc;
    for (; offset < INSERT_RANGE; res_buc++, offset++) {
        if (!LOAD(&t->buckets[res_buc].in_use, RELAXED) && !XCHG(&t->buckets[res_buc].in_use, true, RELAXED)) {
            break;
        }
    }

    if (offset < INSERT_RANGE) {
        while (offset >= MASK_RANGE) {
            if (!find_closer_free_bucket(t, o_seg, &res_buc, &offset)) {
                STORE(&t->buckets[res_buc].node, NULL, RELAXED);
                STORE(&t->buckets[res_buc].in_use, false, RELEASE);
                pthread_mutex_unlock(&t->segments[o_seg].lock);
                return NULL; // Resize needed
            }
        }
        STORE(&t->buckets[res_buc].node, n, RELAXED);
        FAOR(&t->buckets[o_buc].hop, (1ULL << offset), RELAXED);
        FAA(&t->size, 1, ACQ_REL);
        pthread_mutex_unlock(&t->segments[o_seg].lock);
        return n; // Insertion successful, return new node
    } else {
        pthread_mutex_unlock(&t->segments[o_seg].lock);
        return NULL; // Resize needed
    }
}

bool hpt_foreach(struct HPTable *t, bool (*f)(struct BNode *, void *), void *arg) {
    for (u64 seg = 0; seg < t->nsegs; seg++) {
        u64 start = seg * SEGMENT_SIZE;
        u64 ts_before = LOAD(&t->segments[seg].ts, ACQUIRE);
        for (u64 i = 0; i < SEGMENT_SIZE; i++) {
            for (;;) {
                struct BNode *node = LOAD(&t->buckets[start + i].node, ACQUIRE);
                if (node) {
                    if (!f(node, arg)) {
                        return false;
                    }
                    break;
                }
                u64 ts_after = LOAD(&t->segments[seg].ts, ACQUIRE);
                if (ts_before != ts_after) {
                    ts_before = ts_after;
                    continue;
                }
                break;
            }
        }
    }
    return true;
}

u64 hpt_size(struct HPTable *t) { return LOAD(&t->size, RELAXED); }

// Internal helper to move an entry to a closer free bucket
static bool find_closer_free_bucket(struct HPTable *t, const u64 free_seg, u64 *free_buc, u64 *free_dist) {
    u64 dist;

BEGIN:
    dist = MASK_RANGE - 1;
    for (u64 curr_buc = *free_buc - dist; curr_buc < *free_buc; curr_buc++, dist--) {
        u64 hop = LOAD(&t->buckets[curr_buc].hop, RELAXED);
        while (hop > 0) {
            const u64 moved_offset = ffsll((i64) hop) - 1;
            const u64 index = curr_buc + moved_offset;
            if (index >= *free_buc) {
                break; // This entry is not in the range we can move from
            }

            const u64 curr_seg = index / SEGMENT_SIZE;
            // Lock the other segment if necessary
            if (free_seg != curr_seg) {
                pthread_mutex_lock(&t->segments[curr_seg].lock);
            }

            // Re-check hop bitmask after acquiring lock
            const u64 hop_after = LOAD(&t->buckets[curr_buc].hop, RELAXED);
            if (hop_after != hop) {
                if (free_seg != curr_seg) {
                    pthread_mutex_unlock(&t->segments[curr_seg].lock);
                }
                goto BEGIN;
            }
            // Perform the swap
            struct BNode *node = LOAD(&t->buckets[index].node, RELAXED);
            STORE(&t->buckets[*free_buc].node, node, RELAXED);

            // Update bitmaps
            FAOR(&t->buckets[curr_buc].hop, (1ULL << dist), RELAXED);
            FAAND(&t->buckets[curr_buc].hop, ~(1ULL << moved_offset), RELAXED);
            // Update timestamp for the moved entry's segment
            FAA(&t->segments[curr_seg].ts, 1, RELAXED);
            // Update free bucket pointers
            *free_dist -= (*free_buc - index);
            *free_buc = index;
            if (free_seg != curr_seg) {
                pthread_mutex_unlock(&t->segments[curr_seg].lock);
            }
            return true;
        }
    }
    return false;
}

static void migrate_seg(struct HPMap *m, struct HPTable *t, struct HPTable *nxt, u64 seg, node_eq eq) {
    pthread_mutex_lock(&t->segments[seg].lock);
    u64 start = seg * SEGMENT_SIZE;
    for (u64 i = 0; i < SEGMENT_SIZE; i++) {
        struct BNode *node = LOAD(&t->buckets[start + i].node, RELAXED);
        if (node) {
            hpt_upsert(nxt, node, eq);
        }
    }
    pthread_mutex_unlock(&t->segments[seg].lock);
}

static void migrate_helper(struct HPMap *m, struct HPTable *t, struct HPTable *nxt, node_eq eq) {
    if (nxt) {
        FAA(&m->mthreads, 1, ACQ_REL);
        for (;;) {
            u64 seg = FAA(&m->migrate_pos, 1, ACQ_REL);
            if (seg >= t->nsegs) {
                break;
            }
            migrate_seg(m, t, nxt, seg, eq);
        }

        if (FAS(&m->mthreads, 1, ACQ_REL) == 1) {
            struct HPTable *old_t = XCHG(&m->active, nxt, ACQ_REL);
            STORE(&m->migrate_started, false, RELEASE);
            hpt_destroy(old_t);
            return;
        }
    }
    u64 epoch = LOAD(&m->epoch, ACQUIRE);
    int spin = 0;
    while (LOAD(&m->migrate_started, ACQUIRE) && epoch == LOAD(&m->epoch, ACQUIRE)) {
        if (spin < 5) {
#if defined(__i386__) || defined(__x86_64__)
            __asm__ __volatile__("pause");
#elif defined(__aarch64__) || defined(__arm__)
            __asm__ __volatile__("yield");
#endif
        } else {
            usleep(1 << MIN(spin - 5, 9));
        }
        spin++;
    }
}

struct HPMap *hpm_new(struct HPMap *m, size_t size) {
    if (!m) {
        m = calloc(1, sizeof(struct HPMap));
        assert(m);
        m->is_alloc = true;
    } else {
        m->is_alloc = false;
    }

    m->active = hpt_new(size);
    m->migrate_pos = 0;
    m->mthreads = 0;
    m->epoch = 0;
    return m;
}
void hpm_destroy(struct HPMap *m) {
    struct HPTable *t = LOAD(&m->active, ACQUIRE);
    struct HPTable *nxt = LOAD(&t->next, ACQUIRE);
    if (t) {
        hpt_destroy(t);
    }
    if (nxt) {
        hpt_destroy(nxt);
    }

    m->migrate_pos = 0;
    m->mthreads = 0;
    m->epoch = 0;

    if (m->is_alloc) {
        free(m);
    }
}

bool hpm_contains(struct HPMap *m, struct BNode *k, node_eq eq) { return hpm_lookup(m, k, eq) != NULL; }

struct BNode *hpm_lookup(struct HPMap *m, struct BNode *k, node_eq eq) {
    struct HPTable *t = NULL;
    u64 e_before = LOAD(&m->epoch, ACQUIRE);
    for (;;) {
        t = LOAD(&m->active, ACQUIRE);
        t = gc_protect((void **) &t, 0);
        struct BNode *res = hpt_lookup(t, k, eq);
        if (res) {
            return res;
        }
        u64 e_after = LOAD(&m->epoch, ACQUIRE);
        if (e_after != e_before) {
            e_before = e_after;
            continue;
        }
        return NULL;
    }
}

bool hpm_add(struct HPMap *m, struct BNode *n, node_eq eq) {
    struct HPTable *t = NULL, *nxt = NULL;
RETRY:
    for (;;) {
        t = LOAD(&m->active, ACQUIRE);
        t = gc_protect((void **) &t, 0);
        nxt = LOAD(&t->next, ACQUIRE);
        if (nxt) {
            nxt = gc_protect((void **) &nxt, 1);
            migrate_helper(m, t, nxt, eq);
            continue;
        }
        break;
    }

    struct BNode *res = hpt_upsert(t, n, eq);
    if (!res)
        goto RETRY;

    if (res == n) { // Node was newly inserted
        u64 sz = hpt_size(t), cap = t->mask + 1;
        if (res == n && (cap - sz <= (cap >> 2) + (cap >> 3))) { // Trigger resize only on successful insert
            struct HPTable *nt = hpt_new(cap << 1);
            struct HPTable *expect = NULL;
            STORE(&m->migrate_pos, 0, RELEASE);
            STORE(&m->migrate_started, true, RELEASE);
            if (!CMPXCHG(&t->next, &expect, nt, ACQ_REL, RELAXED)) {
                hpt_destroy(nt);
            }
        }
        FAA(&m->size, 1, RELAXED);
        FAA(&m->epoch, 1, RELEASE);
        return true;
    }
    return false;
}

struct BNode *hpm_remove(struct HPMap *m, struct BNode *k, node_eq eq) {
    struct HPTable *t = NULL, *nxt = NULL;
    for (;;) {
        t = LOAD(&m->active, ACQUIRE);
        t = gc_protect((void **) &t, 0);
        nxt = LOAD(&t->next, ACQUIRE);
        if (nxt) {
            nxt = gc_protect((void **) &nxt, 1);
            migrate_helper(m, t, nxt, eq);
            continue;
        }
        break;
    }
    struct BNode *res = hpt_remove(t, k, eq);
    if (res) {
        FAS(&m->size, 1, RELAXED);
        FAA(&m->epoch, 1, RELEASE);
    }
    return res;
}

u64 hpm_size(struct HPMap *m) { return LOAD(&m->size, RELAXED); }

struct BNode *hpm_upsert(struct HPMap *m, struct BNode *n, node_eq eq) {
    struct HPTable *t = NULL, *nxt = NULL;
RETRY:
    for (;;) {
        t = LOAD(&m->active, ACQUIRE);
        t = gc_protect((void **) &t, 0);
        nxt = LOAD(&t->next, ACQUIRE);
        if (nxt) {
            nxt = gc_protect((void **) &nxt, 1);
            migrate_helper(m, t, nxt, eq);
            continue;
        }
        break;
    }

    struct BNode *res = hpt_upsert(t, n, eq);
    if (!res)
        goto RETRY;

    if (res == n) { // Node was newly inserted
        u64 sz = hpt_size(t), cap = t->mask + 1;
        if (res == n && (cap - sz <= (cap >> 2) + (cap >> 3))) { // Trigger resize only on successful insert
            struct HPTable *nt = hpt_new(cap << 1);
            struct HPTable *expect = NULL;
            STORE(&m->migrate_pos, 0, RELEASE);
            STORE(&m->migrate_started, true, RELEASE);
            if (!CMPXCHG(&t->next, &expect, nt, ACQ_REL, RELAXED)) {
                hpt_destroy(nt);
            }
        }
        FAA(&m->size, 1, RELAXED);
        FAA(&m->epoch, 1, RELEASE);
    }
    res = (struct BNode *) ((uintptr_t) res & ~PTR_TAG);

    return res;
}

bool hpm_foreach(struct HPMap *m, bool (*f)(struct BNode *, void *), void *arg, node_eq eq) {
    struct HPTable *t = LOAD(&m->active, ACQUIRE);
    struct HPTable *nxt = LOAD(&t->next, ACQUIRE);
    if (nxt) {
        migrate_helper(m, t, nxt, eq);
        t = LOAD(&m->active, ACQUIRE);
    }
    return hpt_foreach(t, f, arg);
}
