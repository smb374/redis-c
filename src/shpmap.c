#include "hpmap.h"

#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#include "strings.h"
#include "utils.h"

struct Bucket {
    u64 hop;
    struct BNode *node;
};

struct SHPTable {
    struct SHPTable *next;
    struct Bucket *buckets;
    u64 mask, size;
    char data[];
};

static bool find_closer_free_bucket(struct SHPTable *t, u64 *free_bucket_idx, u64 *free_distance);

static struct SHPTable *hpt_new(size_t size) {
    u64 cap = next_pow2(size);
    u64 buckets = cap + INSERT_RANGE;
    struct SHPTable *t = calloc(1, sizeof(struct SHPTable) + sizeof(struct Bucket) * buckets);

    t->buckets = (struct Bucket *) t->data;
    t->mask = cap - 1;
    t->size = 0;
    t->next = NULL;

    for (u64 i = 0; i < buckets; i++) {
        t->buckets[i].hop = 0;
        t->buckets[i].node = NULL;
    }

    return t;
}

static void hpt_destroy(struct SHPTable *t) { free(t); }

static struct BNode *hpt_lookup(struct SHPTable *t, struct BNode *k, node_eq eq) {
    if (!t)
        return NULL;
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 hop = t->buckets[o_buc].hop;
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = o_buc + lowest_set;
        struct BNode *curr_node = t->buckets[curr_idx].node;
        if (eq(curr_node, k)) {
            return curr_node;
        }
        hop &= ~(1ULL << lowest_set);
    }
    return NULL;
}

static struct BNode *hpt_remove(struct SHPTable *t, struct BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 hop = t->buckets[o_buc].hop;
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = o_buc + lowest_set;
        struct BNode *curr_node = t->buckets[curr_idx].node;
        if (eq(curr_node, k)) {
            t->buckets[curr_idx].node = NULL;
            t->buckets[o_buc].hop &= ~(1ULL << lowest_set);
            t->size--;
            return curr_node;
        }
        hop &= ~(1ULL << lowest_set);
    }
    return NULL;
}

static struct BNode *hpt_upsert(struct SHPTable *t, struct BNode *n, node_eq eq) {
    u64 hash = n->hcode;
    u64 o_buc = hash & t->mask;
    u64 hop = t->buckets[o_buc].hop;
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = o_buc + lowest_set;
        struct BNode *curr_node = t->buckets[curr_idx].node;
        if (curr_node && eq(curr_node, n)) {
            return (struct BNode *) ((uintptr_t) curr_node | PTR_TAG); // Key already exists, return existing node
        }
        hop &= ~(1ULL << lowest_set);
    }

    u64 offset = 0;
    u64 res_buc = o_buc;
    for (; offset < INSERT_RANGE; res_buc++, offset++) {
        if (!t->buckets[res_buc].node) {
            break;
        }
    }

    if (offset < INSERT_RANGE) {
        while (offset >= MASK_RANGE) {
            if (!find_closer_free_bucket(t, &res_buc, &offset)) {
                t->buckets[res_buc].node = NULL;
                return NULL;
            }
        }
        t->buckets[res_buc].node = n;
        t->buckets[o_buc].hop |= 1ULL << offset;
        t->size++;
        return n;
    } else {
        return NULL;
    }
}

static bool hpt_foreach(struct SHPTable *t, bool (*f)(struct BNode *, void *), void *arg) {
    if (!t)
        return true;
    u64 num_buckets = t->mask + 1 + INSERT_RANGE;
    for (u64 i = 0; i < num_buckets; i++) {
        struct BNode *node = t->buckets[i].node;
        if (node) {
            if (!f(node, arg)) {
                return false;
            }
        }
    }
    return true;
}

static u64 hpt_size(struct SHPTable *t) { return t->size; }

static bool find_closer_free_bucket(struct SHPTable *t, u64 *free_buc, u64 *free_dist) {
    u64 dist = MASK_RANGE - 1;
    for (u64 curr_buc = *free_buc - dist; curr_buc < *free_buc; curr_buc++, dist--) {
        u64 hop = t->buckets[curr_buc].hop;
        if (hop > 0) {
            const u64 moved_offset = ffsll((i64) hop) - 1;
            const u64 index = curr_buc + moved_offset;
            if (index >= *free_buc) {
                continue; // This entry is not in the range we can move from
            }

            // Perform the swap
            struct BNode *node = t->buckets[index].node;
            t->buckets[*free_buc].node = node;

            // Update bitmaps
            t->buckets[curr_buc].hop |= 1ULL << dist;
            t->buckets[curr_buc].hop &= ~(1ULL << moved_offset);
            *free_dist -= (*free_buc - index);
            *free_buc = index;
            return true;
        }
    }
    return false;
}

static void migrate_chunk(struct SHPTable *t, struct SHPTable *nxt, u64 start, node_eq eq) {
    u64 buckets = t->mask + 1 + INSERT_RANGE;
    for (u64 i = 0; i < SEGMENT_SIZE && start + i < buckets; i++) {
        struct BNode *node = t->buckets[start + i].node;
        if (node) {
            u64 h_buc = node->hcode & t->mask;
            u64 dist = start + i - h_buc;
            struct BNode *result = hpt_upsert(nxt, node, eq);
            if (result == node) {
                t->buckets[start + i].node = NULL;
                t->buckets[h_buc].hop &= ~(1ULL << dist);
                t->size--;
                assert(hpt_lookup(nxt, node, eq) == node);
            }
        }
    }
}

static void migrate_helper(struct SHPMap *m, node_eq eq) {
    struct SHPTable *t = m->active;
    struct SHPTable *nxt = t->next;
    if (!nxt)
        return;
    u64 buckets = t->mask + 1 + INSERT_RANGE;
    u64 start = m->migrate_pos;
    if (start >= buckets) {
        m->active = nxt;
        hpt_destroy(t);
        return;
    }
    m->migrate_pos += SEGMENT_SIZE;
    migrate_chunk(t, nxt, start, eq);
}

struct SHPMap *shpm_new(struct SHPMap *m, size_t size) {
    if (!m) {
        m = calloc(1, sizeof(struct CHPMap));
        assert(m);
        m->is_alloc = true;
    } else {
        m->is_alloc = false;
    }

    m->active = hpt_new(size);
    m->migrate_pos = 0;
    m->size = 0;
    return m;
}

void shpm_destroy(struct SHPMap *m) {
    struct SHPTable *nxt = m->active->next;
    hpt_destroy(m->active);
    if (nxt) {
        hpt_destroy(nxt);
    }
    if (m->is_alloc) {
        free(m);
    }
}

struct BNode *shpm_lookup(struct SHPMap *m, struct BNode *k, node_eq eq) {
    struct BNode *res = NULL;
    struct SHPTable *t = m->active;
    struct SHPTable *nxt = t->next;

    res = hpt_lookup(nxt, k, eq);
    if (!res) {
        res = hpt_lookup(t, k, eq);
    }
    return res;
}

struct BNode *shpm_remove(struct SHPMap *m, struct BNode *k, node_eq eq) {
    migrate_helper(m, eq);

    struct BNode *res;
    struct SHPTable *t = m->active;
    struct SHPTable *nxt = t->next;

    res = NULL;
    if (nxt) {
        res = hpt_remove(nxt, k, eq);
    }
    if (!res) {
        res = hpt_remove(t, k, eq);
    }
    if (res) {
        m->size--;
    }
    return res;
}

struct BNode *shpm_upsert(struct SHPMap *m, struct BNode *n, node_eq eq) {
    migrate_helper(m, eq);

    struct BNode *res;
    struct SHPTable *t = m->active;
    struct SHPTable *nxt = t->next;

    if (nxt) {
        // During migration.
        res = hpt_upsert(nxt, n, eq);
        if (res == n) {
            m->size++;
        }
    } else {
        // No migration.
        res = hpt_upsert(t, n, eq);
        if (res == n) {
            m->size++;
            u64 sz = t->size, cap = t->mask + 1;
            if (cap - sz <= (cap >> 2) + (cap >> 3) || sz >= cap) {
                // Start migration.
                struct SHPTable *nxt = hpt_new(cap << 1);
                m->migrate_pos = 0;
                t->next = nxt;
            }
        }
    }


    res = (struct BNode *) ((uintptr_t) res & ~PTR_TAG);
    return res;
}

bool shpm_foreach(struct SHPMap *m, bool (*f)(struct BNode *, void *), void *arg) {
    struct SHPTable *t = m->active;
    struct SHPTable *nxt = t->next;

    return hpt_foreach(nxt, f, arg) && hpt_foreach(t, f, arg);
}
