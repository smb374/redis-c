#include "hpmap.h"

#include <assert.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>

#include "utils.h"

// Forward declaration for the internal helper function
static bool find_closer_free_bucket(struct HPTable *t, u64 free_segment, u64 *free_bucket_idx, u64 *free_distance);

#define PTR_TAG 0x8000000000000000

struct HPTable *hpt_new(struct HPTable *t, size_t size) {
    if (!t) {
        t = calloc(1, sizeof(struct HPTable));
        assert(t);
        t->is_alloc = true;
    } else {
        t->is_alloc = false;
    }

    u64 cap = next_pow2(size);
    t->mask = cap - 1;
    t->buckets = calloc(cap + INSERT_RANGE, sizeof(Bucket));
    t->nsegs = (cap + INSERT_RANGE) / SEGMENT_SIZE + ((cap + INSERT_RANGE) % SEGMENT_SIZE != 0);
    assert(t->buckets);

    t->segments = calloc(t->nsegs, sizeof(Segment));
    assert(t->segments);
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

void hpt_destroy(struct HPTable *t) {
    for (size_t i = 0; i < t->nsegs; i++) {
        pthread_mutex_destroy(&t->segments[i].lock);
    }
    free(t->segments);
    free(t->buckets);
    if (t->is_alloc) {
        free(t);
    }
}

bool hpt_contains(struct HPTable *t, BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 o_seg = o_buc / SEGMENT_SIZE;

    u64 ts_before = LOAD(&t->segments[o_seg].ts, ACQUIRE);
    for (;;) {
        u64 hop = LOAD(&t->buckets[o_buc].hop, RELAXED);
        while (hop > 0) {
            u64 lowest_set = ffsll((i64) hop) - 1;
            u64 curr_idx = (o_buc + lowest_set) & t->mask;
            BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
            if (eq(curr_node, k)) {
                return true;
            }
            hop &= ~(1ULL << lowest_set);
        }
        u64 ts_after = LOAD(&t->segments[o_seg].ts, ACQUIRE);
        if (ts_before != ts_after) {
            ts_before = ts_after;
            continue;
        }
        return false;
    }
}

BNode *hpt_lookup(struct HPTable *t, BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 o_seg = o_buc / SEGMENT_SIZE;

    u64 ts_before = LOAD(&t->segments[o_seg].ts, ACQUIRE);
    for (;;) {
        u64 hop = LOAD(&t->buckets[o_buc].hop, RELAXED);
        while (hop > 0) {
            u64 lowest_set = ffsll((i64) hop) - 1;
            u64 curr_idx = (o_buc + lowest_set) & t->mask;
            BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
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

bool hpt_add(struct HPTable *t, BNode *n, node_eq eq) {
    u64 hash = n->hcode;
    u64 o_buc = hash & t->mask;
    u64 o_seg = o_buc / SEGMENT_SIZE;

    pthread_mutex_lock(&t->segments[o_seg].lock);
    if (LOAD(&t->next, ACQUIRE) != NULL) {
        pthread_mutex_unlock(&t->segments[o_seg].lock);
        return false; // Indicate failure, let caller retry on the new table.
    }

    u64 hop = LOAD(&t->buckets[o_buc].hop, RELAXED);
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = (o_buc + lowest_set) & t->mask;
        BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
        if (eq(curr_node, n)) {
            pthread_mutex_unlock(&t->segments[o_seg].lock);
            return false; // Key already exists
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
                return false; // Resize needed
            }
        }
        STORE(&t->buckets[res_buc].node, n, RELAXED);
        FAOR(&t->buckets[o_buc].hop, (1ULL << offset), RELAXED);
        FAA(&t->size, 1, ACQ_REL);
        pthread_mutex_unlock(&t->segments[o_seg].lock);
        return true;
    } else {
        pthread_mutex_unlock(&t->segments[o_seg].lock);
        return false; // Resize needed
    }
}

BNode *hpt_remove(struct HPTable *t, BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 o_seg = o_buc / SEGMENT_SIZE;

    pthread_mutex_lock(&t->segments[o_seg].lock);

    u64 hop = LOAD(&t->buckets[o_buc].hop, RELAXED);
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = (o_buc + lowest_set) & t->mask;
        BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
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

BNode *hpt_upsert(struct HPTable *t, BNode *n, node_eq eq) {
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
        BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
        if (eq(curr_node, n)) {
            pthread_mutex_unlock(&t->segments[o_seg].lock);
            return (BNode *) ((uintptr_t) curr_node | PTR_TAG); // Key already exists, return existing node
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

bool hpt_foreach(struct HPTable *t, bool (*f)(BNode *, void *), void *arg) {
    for (u64 seg = 0; seg <= t->nsegs; seg++) {
        u64 start = seg * SEGMENT_SIZE;
        for (u64 i = 0; i < SEGMENT_SIZE; i++) {
            BNode *node = LOAD(&t->buckets[start + i].node, ACQUIRE);
            if (node) {
                if (!f(node, arg)) {
                    return false;
                }
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
            BNode *node = LOAD(&t->buckets[index].node, RELAXED);
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
    u64 start = seg * SEGMENT_SIZE, end = (seg + 1) * SEGMENT_SIZE;
    for (u64 i = start; i < end; i++) {
        BNode *node = LOAD(&t->buckets[i].node, RELAXED);
        if (node) {
            hpt_add(nxt, node, eq);
        }
    }
    pthread_mutex_unlock(&t->segments[seg].lock);
}

static void migrate_helper(struct HPMap *m, node_eq eq) {
    struct HPTable *t = LOAD(&m->active, ACQUIRE);
    struct HPTable *nxt = LOAD(&t->next, ACQUIRE);
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
            pthread_mutex_lock(&m->mlock);
            XCHG(&m->active, nxt, RELEASE);
            STORE(&m->migration_started, false, RELEASE);
            pthread_cond_broadcast(&m->mcond);
            pthread_mutex_unlock(&m->mlock);
        }
    }
    pthread_mutex_lock(&m->mlock);
    while (LOAD(&m->migration_started, ACQUIRE)) {
        pthread_cond_wait(&m->mcond, &m->mlock);
    }
    pthread_mutex_unlock(&m->mlock);
}

struct HPMap *hpm_new(struct HPMap *m, size_t size) {
    if (!m) {
        m = calloc(1, sizeof(struct HPMap));
        assert(m);
        m->is_alloc = true;
    } else {
        m->is_alloc = false;
    }

    m->active = hpt_new(NULL, size);
    m->migrate_pos = 0;
    m->mthreads = 0;
    pthread_mutex_init(&m->mlock, NULL);
    pthread_cond_init(&m->mcond, NULL);
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
    pthread_mutex_destroy(&m->mlock);
    pthread_cond_destroy(&m->mcond);

    if (m->is_alloc) {
        free(m);
    }
}

bool hpm_contains(struct HPMap *m, BNode *k, node_eq eq) {
    struct HPTable *t = LOAD(&m->active, ACQUIRE);
    struct HPTable *nxt = LOAD(&t->next, ACQUIRE);
    if (nxt) {
        migrate_helper(m, eq);
        t = LOAD(&m->active, ACQUIRE);
    }
    return hpt_contains(t, k, eq);
}

BNode *hpm_lookup(struct HPMap *m, BNode *k, node_eq eq) {
    struct HPTable *t = LOAD(&m->active, ACQUIRE);
    struct HPTable *nxt = LOAD(&t->next, ACQUIRE);
    if (nxt) {
        migrate_helper(m, eq);
        t = LOAD(&m->active, ACQUIRE);
    }
    return hpt_lookup(t, k, eq);
}

bool hpm_add(struct HPMap *m, BNode *n, node_eq eq) {
    struct HPTable *t = LOAD(&m->active, ACQUIRE);
    struct HPTable *nxt = LOAD(&t->next, ACQUIRE);
    if (nxt) {
        migrate_helper(m, eq);
        t = LOAD(&m->active, ACQUIRE);
    }

    bool res = hpt_add(t, n, eq);
    if (!res && LOAD(&t->next, ACQUIRE) != NULL) {
        // The add failed specifically because a resize started.
        // We must now help migrate and retry on the new table.
        migrate_helper(m, eq);
        t = LOAD(&m->active, ACQUIRE);
        res = hpt_add(t, n, eq); // Retry the add
    }
    u64 sz = hpt_size(t), cap = t->mask + 1;
    if (cap - sz <= (cap >> 2)) {
        struct HPTable *nt = hpt_new(NULL, cap << 1);
        struct HPTable *expect = NULL;
        STORE(&m->migrate_pos, 0, RELEASE);
        STORE(&m->migration_started, true, RELEASE);
        if (!CMPXCHG(&t->next, &expect, nt, ACQ_REL, RELAXED)) {
            hpt_destroy(nt);
        }
    }

    if (res) {
        FAA(&m->size, 1, RELAXED);
    }

    return res;
}

BNode *hpm_remove(struct HPMap *m, BNode *k, node_eq eq) {
    struct HPTable *t = LOAD(&m->active, ACQUIRE);
    struct HPTable *nxt = LOAD(&t->next, ACQUIRE);
    if (nxt) {
        migrate_helper(m, eq);
        t = LOAD(&m->active, ACQUIRE);
    }
    BNode *res = hpt_remove(t, k, eq);
    if (res) {
        FAS(&m->size, 1, RELAXED);
    }
    return res;
}

u64 hpm_size(struct HPMap *m) { return LOAD(&m->size, RELAXED); }

BNode *hpm_upsert(struct HPMap *m, BNode *n, node_eq eq) {
    struct HPTable *t = LOAD(&m->active, ACQUIRE);
    if (LOAD(&t->next, ACQUIRE)) {
        migrate_helper(m, eq);
        t = LOAD(&m->active, ACQUIRE);
    }

    BNode *res_node = hpt_upsert(t, n, eq);
    if (!res_node) {
        // Upsert failed, likely needs resize or a race with resize start
        migrate_helper(m, eq);
        t = LOAD(&m->active, ACQUIRE);
        res_node = hpt_upsert(t, n, eq);
    }

    u64 sz = hpt_size(t), cap = t->mask + 1;
    if (res_node == n && (cap - sz <= (cap >> 2))) { // Trigger resize only on successful insert
        struct HPTable *nt = hpt_new(NULL, cap << 1);
        struct HPTable *expect = NULL;
        STORE(&m->migrate_pos, 0, RELEASE);
        STORE(&m->migration_started, true, RELEASE);
        if (!CMPXCHG(&t->next, &expect, nt, ACQ_REL, RELAXED)) {
            hpt_destroy(nt);
        }
    }

    if (res_node == n) { // Node was newly inserted
        FAA(&m->size, 1, RELAXED);
    }
    res_node = (BNode *) ((uintptr_t) res_node & ~PTR_TAG);

    return res_node;
}

bool hpm_foreach(struct HPMap *m, bool (*f)(BNode *, void *), void *arg, node_eq eq) {
    struct HPTable *t = LOAD(&m->active, ACQUIRE);
    struct HPTable *nxt = LOAD(&t->next, ACQUIRE);
    if (nxt) {
        migrate_helper(m, eq);
        t = LOAD(&m->active, ACQUIRE);
    }
    return hpt_foreach(t, f, arg);
}
