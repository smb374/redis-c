#include "hpmap.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <unistd.h>

#include "qsbr.h"
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

struct CHPTable {
    _Atomic(struct CHPTable *) next;
    struct Segment *segments;
    struct Bucket *buckets;
    u64 mask, nsegs;
    atomic_u64 size;
    char data[];
};

static bool find_closer_free_bucket(struct CHPTable *t, u64 free_segment, u64 *free_bucket_idx, u64 *free_distance);

static inline void spin_wait(struct CHPMap *m) {
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
            int sleep_duration = spin - 5 < 9 ? spin - 5 : 9;
            usleep(1 << sleep_duration);
        }
        spin++;
    }
}

static struct CHPTable *hpt_new(size_t size) {
    u64 cap = next_pow2(size);
    u64 buckets = cap + INSERT_RANGE, nsegs = buckets / SEGMENT_SIZE + (buckets % SEGMENT_SIZE != 0);
    struct CHPTable *t =
            qsbr_calloc(1, sizeof(struct CHPTable) + sizeof(struct Segment) * nsegs + sizeof(struct Bucket) * buckets);
    assert(t);
    t->segments = (struct Segment *) t->data;
    t->buckets = (struct Bucket *) (t->data + sizeof(struct Segment) * nsegs);
    t->mask = cap - 1;
    t->nsegs = nsegs;

    for (u64 i = 0; i < t->nsegs; i++) {
        pthread_mutex_init(&t->segments[i].lock, NULL);
    }

    return t;
}

static void hpt_destroy(struct CHPTable *t) { qsbr_retire(t, NULL); }

static struct BNode *hpt_lookup(struct CHPTable *t, struct BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 o_seg = o_buc / SEGMENT_SIZE;

    u64 ts_before = LOAD(&t->segments[o_seg].ts, ACQUIRE);
    for (;;) {
        u64 hop = LOAD(&t->buckets[o_buc].hop, RELAXED);
        while (hop > 0) {
            u64 lowest_set = ffsll((i64) hop) - 1;
            u64 curr_idx = o_buc + lowest_set;
            struct BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
            if (curr_node && eq(curr_node, k)) {
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

static struct BNode *hpt_remove(struct CHPTable *t, struct BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 o_seg = o_buc / SEGMENT_SIZE;

    pthread_mutex_lock(&t->segments[o_seg].lock);

    u64 hop = LOAD(&t->buckets[o_buc].hop, RELAXED);
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = o_buc + lowest_set;
        struct BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
        if (curr_node && eq(curr_node, k)) {
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

static struct BNode *hpt_upsert(struct CHPTable *t, struct BNode *n, node_eq eq) {
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
        u64 curr_idx = o_buc + lowest_set;
        struct BNode *curr_node = LOAD(&t->buckets[curr_idx].node, RELAXED);
        if (curr_node && eq(curr_node, n)) {
            pthread_mutex_unlock(&t->segments[o_seg].lock);
            // return (struct BNode *) ((uintptr_t) curr_node | PTR_TAG); // Key already exists, return existing node
            return tag_ptr(curr_node, 1); // Key already exists, return existing node
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

static bool hpt_foreach(struct CHPTable *t, bool (*f)(struct BNode *, void *), void *arg) {
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

static u64 hpt_size(struct CHPTable *t) { return LOAD(&t->size, RELAXED); }

static bool find_closer_free_bucket(struct CHPTable *t, const u64 free_seg, u64 *free_buc, u64 *free_dist) {
    u64 dist, start;

BEGIN:
    dist = MASK_RANGE - 1;
    for (u64 curr_buc = *free_buc - dist; curr_buc < *free_buc; curr_buc++, dist--) {
        u64 hop = LOAD(&t->buckets[curr_buc].hop, RELAXED);
        if (hop > 0) {
            const u64 moved_offset = ffsll((i64) hop) - 1;
            const u64 index = curr_buc + moved_offset;
            if (index >= *free_buc) {
                continue;
            }

            const u64 curr_seg = index / SEGMENT_SIZE;
            if (free_seg != curr_seg) {
                pthread_mutex_lock(&t->segments[curr_seg].lock);
            }

            const u64 hop_after = LOAD(&t->buckets[curr_buc].hop, RELAXED);
            if (hop_after != hop) {
                if (free_seg != curr_seg) {
                    pthread_mutex_unlock(&t->segments[curr_seg].lock);
                }
                goto BEGIN;
            }
            struct BNode *node = LOAD(&t->buckets[index].node, RELAXED);
            STORE(&t->buckets[*free_buc].node, node, RELAXED);

            FAOR(&t->buckets[curr_buc].hop, (1ULL << dist), RELAXED);
            FAAND(&t->buckets[curr_buc].hop, ~(1ULL << moved_offset), RELAXED);
            FAA(&t->segments[curr_seg].ts, 1, RELAXED);
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

static void migrate_seg(struct CHPTable *t, struct CHPTable *nxt, u64 seg, node_eq eq) {
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

static void migrate_helper(struct CHPMap *m, struct CHPTable *t, struct CHPTable *nxt, node_eq eq) {
    if (nxt) {
        FAA(&m->mthreads, 1, ACQ_REL);
        for (;;) {
            u64 seg = FAA(&m->migrate_pos, 1, ACQ_REL);
            if (seg >= t->nsegs) {
                break;
            }
            migrate_seg(t, nxt, seg, eq);
        }

        if (FAS(&m->mthreads, 1, ACQ_REL) == 1) {
            struct CHPTable *old_t = XCHG(&m->active, nxt, ACQ_REL);
            STORE(&m->migrate_started, false, RELEASE);
            hpt_destroy(old_t);
            return;
        }
    }
    spin_wait(m);
}

struct CHPMap *chpm_new(struct CHPMap *m, size_t size) {
    if (!m) {
        m = calloc(1, sizeof(struct CHPMap));
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
void chpm_destroy(struct CHPMap *m) {

    struct CHPTable *t = LOAD(&m->active, ACQUIRE);
    if (t) {
        hpt_destroy(t);
    }

    m->migrate_pos = 0;
    m->mthreads = 0;
    m->epoch = 0;

    if (m->is_alloc) {
        free(m);
    }
}

bool chpm_contains(struct CHPMap *m, struct BNode *k, node_eq eq) { return chpm_lookup(m, k, eq) != NULL; }

struct BNode *chpm_lookup(struct CHPMap *m, struct BNode *k, node_eq eq) {
    struct BNode *res;
    u64 e_before = LOAD(&m->epoch, ACQUIRE);
    for (;;) {
        struct CHPTable *t = LOAD(&m->active, ACQUIRE);
        res = hpt_lookup(t, k, eq);
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

bool chpm_add(struct CHPMap *m, struct BNode *n, node_eq eq) {
    struct CHPTable *t = NULL, *nxt = NULL;
    struct BNode *res;
RETRY:
    for (;;) {
        t = LOAD(&m->active, ACQUIRE);
        nxt = LOAD(&t->next, ACQUIRE);
        if (nxt) {
            migrate_helper(m, t, nxt, eq);
            qsbr_quiescent();
            continue;
        }
        break;
    }

    res = hpt_upsert(t, n, eq);
    if (!res)
        goto RETRY;

    if (res == n) { // Node was newly inserted
        u64 sz = hpt_size(t), cap = t->mask + 1;
        if (cap - sz <= (cap >> 2) + (cap >> 3) || sz >= cap) {
            struct CHPTable *nt = hpt_new(cap << 1);
            struct CHPTable *expect = NULL;
            STORE(&m->migrate_pos, 0, RELEASE);
            STORE(&m->migrate_started, true, RELEASE);
            if (!CMPXCHG(&t->next, &expect, nt, ACQ_REL, RELAXED)) {
                hpt_destroy(nt);
            }
        }
        FAA(&m->size, 1, RELAXED);
        FAA(&m->epoch, 1, RELEASE);
        qsbr_quiescent();
        return true;
    }

    return false;
}

struct BNode *chpm_remove(struct CHPMap *m, struct BNode *k, node_eq eq) {
    struct BNode *result = NULL;
    struct CHPTable *t = NULL, *nxt = NULL;
    for (;;) {
        t = LOAD(&m->active, ACQUIRE);
        nxt = LOAD(&t->next, ACQUIRE);
        if (nxt) {
            migrate_helper(m, t, nxt, eq);
            qsbr_quiescent();
            continue;
        }
        break;
    }
    result = hpt_remove(t, k, eq);
    if (result) {
        FAS(&m->size, 1, RELAXED);
        FAA(&m->epoch, 1, RELEASE);
        qsbr_quiescent();
    }
    return result;
}

u64 chpm_size(struct CHPMap *m) { return LOAD(&m->size, RELAXED); }

struct BNode *chpm_upsert(struct CHPMap *m, struct BNode *n, node_eq eq) {
    struct BNode *result;
    struct CHPTable *t = NULL, *nxt = NULL;
RETRY:
    for (;;) {
        t = LOAD(&m->active, ACQUIRE);
        nxt = LOAD(&t->next, ACQUIRE);
        if (nxt) {
            migrate_helper(m, t, nxt, eq);
            qsbr_quiescent();
            continue;
        }
        break;
    }

    result = hpt_upsert(t, n, eq);
    if (!result)
        goto RETRY;

    if (result == n) { // Node was newly inserted
        u64 sz = hpt_size(t), cap = t->mask + 1;
        if (cap - sz <= (cap >> 2) + (cap >> 3) || sz >= cap) {
            struct CHPTable *nt = hpt_new(cap << 1);
            struct CHPTable *expect = NULL;
            STORE(&m->migrate_pos, 0, RELEASE);
            STORE(&m->migrate_started, true, RELEASE);
            if (!CMPXCHG(&t->next, &expect, nt, ACQ_REL, RELAXED)) {
                hpt_destroy(nt);
            }
        }
        FAA(&m->size, 1, RELAXED);
        FAA(&m->epoch, 1, RELEASE);
        qsbr_quiescent();
    }
    result = untag_ptr(result);
    return result;
}

bool chpm_foreach(struct CHPMap *m, bool (*f)(struct BNode *, void *), void *arg, node_eq eq) {
    struct CHPTable *t = LOAD(&m->active, ACQUIRE);
    struct CHPTable *nxt = LOAD(&t->next, ACQUIRE);
    if (nxt) {
        migrate_helper(m, t, nxt, eq);
        t = LOAD(&m->active, ACQUIRE);
    }
    return hpt_foreach(t, f, arg);
}
