#include "leapfrog.h"

#include <assert.h>
#include <stddef.h>
#include <stdlib.h>

#include "utils.h"

#define INITIAL_SIZE 8

struct LFCell {
    u64 hcode;
    struct LFNode *node;
};

struct LFCellGroup {
    u8 deltas[8];
    struct LFCell cells[4];
};

struct LFTable {
    struct LFCellGroup *groups;
    u64 mask;
};

static struct LFTable *lft_new(size_t size) {
    size_t cap = next_pow2(size);
    struct LFTable *t = calloc(1, sizeof(struct LFTable));

    t->groups = calloc(cap >> 2, sizeof(struct LFCellGroup));
    t->mask = cap - 1;

    return t;
}

static void lft_destroy(struct LFTable *t) {
    if (!t)
        return;
    free(t->groups);
    free(t);
}

static struct LFNode *lft_upsert(struct LFTable *t, struct LFNode *n, u64 *overflow_idx, lfn_eq eq) {
    u64 idx = n->hcode;
    struct LFCellGroup *grp = &t->groups[(idx & t->mask) >> 2];
    struct LFCell *cell = &grp->cells[(idx & 3)];
    // Check home bucket first
    if (!cell->hcode) {
        // Vacant, insert
        cell->node = n;
        cell->hcode = n->hcode;
        return n;
    } else if (cell->hcode == n->hcode && eq(cell->node, n)) {
        // Has node with same hash and equals our node, return found.
        return tag_ptr(cell->node, 1);
    }
    // Max index limit is idx + mask.
    u64 max_idx = idx + t->mask;
    // First jump uses 1st delta
    u8 *prev_link = &grp->deltas[(idx & 3)], delta = *prev_link;
    while (delta) {
        idx += delta;
        grp = &t->groups[(idx & t->mask) >> 2];
        cell = &grp->cells[(idx & 3)];
        if (cell->hcode == n->hcode && eq(cell->node, n)) {
            return tag_ptr(cell->node, 1);
        }
        // Subsequent jumps uses 2nd delta.
        prev_link = &grp->deltas[(idx & 3) + 4];
        delta = *prev_link;
    }
    // Jump finished & found nothing, start linear probing...
    u64 prev_link_idx = idx;
    u64 remain = MIN(max_idx - idx, LINEAR_SEARCH_LIMIT);
    while (remain--) {
        idx++;
        grp = &t->groups[(idx & t->mask) >> 2];
        cell = &grp->cells[(idx & 3)];
        if (!cell->hcode) {
            cell->hcode = n->hcode;
            cell->node = n;
            *prev_link = idx - prev_link_idx;
            return n;
        }
    }

    *overflow_idx = idx + 1;
    return NULL;
}

static struct LFNode *lft_lookup(struct LFTable *t, struct LFNode *k, lfn_eq eq) {
    u64 idx = k->hcode;
    struct LFCellGroup *grp = &t->groups[(idx & t->mask) >> 2];
    struct LFCell *cell = &grp->cells[(idx & 3)];
    if (cell->hcode == k->hcode && eq(cell->node, k)) {
        return cell->node;
    }
    // First jump uses 1st delta
    u8 delta = grp->deltas[(idx & 3)];
    while (delta) {
        idx += delta;
        grp = &t->groups[(idx & t->mask) >> 2];
        cell = &grp->cells[(idx & 3)];
        if (cell->hcode == k->hcode && eq(cell->node, k)) {
            return cell->node;
        }
        // Subsequent jumps uses 2nd delta.
        delta = grp->deltas[(idx & 3) + 4];
    }

    return NULL;
}

static struct LFNode *lft_remove(struct LFTable *t, struct LFNode *k, lfn_eq eq) {
    u64 idx = k->hcode;
    struct LFCellGroup *grp = &t->groups[(idx & t->mask) >> 2];
    struct LFCell *cell = &grp->cells[(idx & 3)];
    if (cell->hcode == k->hcode && eq(cell->node, k)) {
        struct LFNode *res = cell->node;
        cell->node = NULL;
        return res;
    }
    // First jump uses 1st delta
    u8 delta = grp->deltas[(idx & 3)];
    while (delta) {
        idx += delta;
        grp = &t->groups[(idx & t->mask) >> 2];
        cell = &grp->cells[(idx & 3)];
        if (cell->hcode == k->hcode && eq(cell->node, k)) {
            struct LFNode *res = cell->node;
            cell->node = NULL;
            return res;
        }
        // Subsequent jumps uses 2nd delta.
        delta = grp->deltas[(idx & 3) + 4];
    }

    return NULL;
}

static bool lfm_try_migrate(struct LFMap *m, u64 size, lfn_eq eq) {
    struct LFTable *t = m->active;
    struct LFTable *nt = lft_new(size);
    struct LFCellGroup *groups = t->groups;
    u64 s_size = t->mask + 1;

    for (u64 i = 0; i < s_size; i++) {
        struct LFCellGroup *grp = &groups[i >> 2];
        struct LFCell *cell = &grp->cells[i & 3];
        if (cell->node) {
            u64 overflow_idx;
            if (!lft_upsert(nt, cell->node, &overflow_idx, eq)) {
                lft_destroy(nt);
                return false;
            }
        }
    }
    m->active = nt;
    lft_destroy(t);
    return true;
}

static void lfm_migrate(struct LFMap *m, u64 overflow_idx, lfn_eq eq) {
    struct LFTable *t = m->active;
    u64 idx = overflow_idx - LINEAR_SEARCH_LIMIT;
    u64 in_use = 0;
    for (u64 remain = LINEAR_SEARCH_LIMIT; remain > 0; remain--) {
        struct LFCellGroup *grp = &t->groups[(idx & t->mask) >> 2];
        struct LFCell *cell = &grp->cells[(idx & 3)];
        if (cell->node) {
            in_use++;
        }
        idx++;
    }

    float ratio = (float) in_use / LINEAR_SEARCH_LIMIT;
    float estimate = (float) (t->mask + 1) * ratio;
    u64 next_size = MAX(INITIAL_SIZE, next_pow2((u64) estimate * 2ULL));
    for (;;) {
        if (lfm_try_migrate(m, next_size, eq)) {
            break;
        }
        next_size <<= 1;
    }
}

struct LFMap *lfm_new(struct LFMap *m, size_t size) {
    if (!m) {
        m = calloc(1, sizeof(struct LFMap));
        m->is_alloc = true;
    } else {
        m->is_alloc = false;
    }

    m->active = lft_new(size);
    m->size = 0;
    return m;
}

void lfm_destroy(struct LFMap *m) {
    if (!m)
        return;

    lft_destroy(m->active);
    if (m->is_alloc) {
        free(m);
    }
}

struct LFNode *lfm_upsert(struct LFMap *m, struct LFNode *node, lfn_eq eq) {
    if (!m || !m->active)
        return NULL;

    for (;;) {
        u64 overflow_idx = 0;
        struct LFNode *res = lft_upsert(m->active, node, &overflow_idx, eq);

        if (res) {
            if (!is_marked(res))
                m->size++;
            return untag_ptr(res);
        }

        lfm_migrate(m, overflow_idx, eq);
    }
}

struct LFNode *lfm_lookup(struct LFMap *m, struct LFNode *key, lfn_eq eq) {
    if (!m || !m->active)
        return NULL;

    return lft_lookup(m->active, key, eq);
}

struct LFNode *lfm_remove(struct LFMap *m, struct LFNode *key, lfn_eq eq) {
    if (!m || !m->active)
        return NULL;

    struct LFNode *res = lft_remove(m->active, key, eq);
    if (res) {
        m->size--;
    }
    return res;
}

size_t lfm_size(struct LFMap *m) { return m->size; }
