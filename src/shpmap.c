#include "hpmap.h"

#include <stddef.h>
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

struct SHPTable *hpt_new(size_t size) {
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

void hpt_destroy(struct SHPTable *t) { free(t); }

struct BNode *hpt_lookup(struct SHPTable *t, struct BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 hop = t->buckets[o_buc].hop;
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = (o_buc + lowest_set) & t->mask;
        struct BNode *curr_node = t->buckets[curr_idx].node;
        if (eq(curr_node, k)) {
            return curr_node;
        }
        hop &= ~(1ULL << lowest_set);
    }
    return NULL;
}

struct BNode *hpt_remove(struct SHPTable *t, struct BNode *k, node_eq eq) {
    u64 hash = k->hcode;
    u64 o_buc = hash & t->mask;
    u64 hop = t->buckets[o_buc].hop;
    while (hop > 0) {
        u64 lowest_set = ffsll((i64) hop) - 1;
        u64 curr_idx = (o_buc + lowest_set) & t->mask;
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


static bool find_closer_free_bucket(struct SHPTable *t, u64 *free_buc, u64 *free_dist) {
    u64 dist = MASK_RANGE - 1;
    for (u64 curr_buc = *free_buc - dist; curr_buc < *free_buc; curr_buc++, dist--) {
        u64 hop = t->buckets[curr_buc].hop;
        while (hop > 0) {
            const u64 moved_offset = ffsll((i64) hop) - 1;
            const u64 index = curr_buc + moved_offset;
            if (index >= *free_buc) {
                break; // This entry is not in the range we can move from
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
