#include "phmap.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/param.h>

#include "utils.h"

#define BPACK(bound, scanning) (((bound) << 1) | ((scanning) & 1))
#define VPACK(version, state) (((version) << 3) | ((state) & STATE_MASK))
#define GET_BOUND(b) ((b) & BOUND_MASK) >> 1
#define GET_VER(vs) ((vs) & VERSION_MASK) >> 3

static Bucket *bucket(PHTable *t, uint64_t h, uint64_t idx) {
    uint64_t seq = (h + idx * (idx + 1) / 2) & t->mask;
    return &t->buckets[seq];
}

static void init_probe_bound(PHTable *t, uint64_t h) { STORE(&t->bounds[h], BPACK(0, false), RELAXED); }
static uint64_t get_probe_bound(PHTable *t, uint64_t h) {
    uint64_t b = LOAD(&t->bounds[h], ACQUIRE);
    return GET_BOUND(b);
}
static void cond_raise_bound(PHTable *t, uint64_t h, uint64_t idx) {
    uint64_t obound, nb, b;
    bool scanning;
    do {
        b = LOAD(&t->bounds[h], ACQUIRE);
        obound = GET_BOUND(b);
        scanning = b & 1;
        nb = BPACK(MAX(obound, idx), false);
    } while (!CMPXCHG(&t->bounds[h], &b, nb, ACQ_REL, RELAXED));
}
static bool has_collision(PHTable *t, uint64_t h, uint64_t idx) {
    Bucket *buc = bucket(t, h, idx);
    uint64_t vs1 = LOAD(&buc->vs, ACQUIRE), vs2;
    uint64_t version1 = GET_VER(vs1), version2;
    uint32_t state1 = vs1 & STATE_MASK, state2;

    if (state1 >= B_VISIBLE && buc->hcode == h) {
        vs2 = LOAD(&buc->vs, ACQUIRE);
        version2 = GET_VER(vs2);
        state2 = vs2 & STATE_MASK;
        if (state2 >= B_VISIBLE && version1 == version2) {
            return true;
        }
    }
    return false;
}
static void cond_lower_bound(PHTable *t, uint64_t h, uint64_t idx) {
    uint64_t b = LOAD(&t->bounds[h], ACQUIRE);
    uint64_t bound = GET_BOUND(b);
    bool scanning = b & 1;
    uint64_t expect;

    if (scanning) {
        expect = BPACK(bound, true);
        CMPXCHG(&t->bounds[h], &expect, BPACK(bound, false), ACQ_REL, RELAXED);
    }
    if (idx) {
        expect = BPACK(idx, false);
        if (CMPXCHG(&t->bounds[h], &expect, BPACK(idx, true), ACQ_REL, RELAXED)) {
            uint64_t i = idx - 1;
            while (i > 0 && !has_collision(t, h, i)) {
                i--;
            }
            expect = BPACK(idx, true);
            CMPXCHG(&t->bounds[h], &expect, BPACK(i, false), ACQ_REL, RELAXED);
        }
    }
}
static bool assist(PHTable *t, BNode *k, uint64_t h, uint64_t i, uint64_t ver_i, bool (*eq)(BNode *, BNode *)) {
    Bucket *buc_i = bucket(t, h, i);
    uint64_t max = get_probe_bound(t, h);
    for (int j = 0; j <= max; j++) {
        if (i != j) {
            Bucket *buc_j = bucket(t, h, j);
            uint64_t vsj = LOAD(&buc_j->vs, ACQUIRE);
            uint64_t ver_j = GET_VER(vsj);
            uint32_t state_j = vsj & STATE_MASK;
            if (state_j == B_INSERTING && eq(LOAD(&buc_j->node, ACQUIRE), k)) {
                if (j < i) {
                    if (LOAD(&buc_j->vs, ACQUIRE) == VPACK(ver_j, B_INSERTING)) {
                        uint64_t expect = VPACK(ver_i, B_INSERTING);
                        CMPXCHG(&buc_i->vs, &expect, VPACK(ver_i, B_COLLIDED), ACQ_REL, RELAXED);
                        return assist(t, k, h, j, ver_j, eq);
                    }
                } else {
                    if (LOAD(&buc_i->vs, ACQUIRE) == VPACK(ver_i, B_INSERTING)) {
                        uint64_t expect = VPACK(ver_j, B_INSERTING);
                        CMPXCHG(&buc_j->vs, &expect, VPACK(ver_j, B_COLLIDED), ACQ_REL, RELAXED);
                    }
                }
            }
            vsj = LOAD(&buc_j->vs, ACQUIRE);
            ver_j = GET_VER(vsj);
            state_j = vsj & STATE_MASK;
            if (state_j == B_MEMBER && eq(LOAD(&buc_j->node, ACQUIRE), k)) {
                uint64_t expect = VPACK(ver_i, B_INSERTING);
                CMPXCHG(&buc_i->vs, &expect, VPACK(ver_i, B_COLLIDED), ACQ_REL, RELAXED);
                return false;
            }
        }
    }
    uint64_t expect = VPACK(ver_i, B_INSERTING);
    CMPXCHG(&buc_i->vs, &expect, VPACK(ver_i, B_MEMBER), ACQ_REL, RELAXED);
    return true;
}

void phm_init(PHTable *t) {
    for (int i = 0; i <= t->mask; i++) {
        init_probe_bound(t, i);
        STORE(&t->buckets[i].vs, VPACK(0, B_EMPTY), RELAXED);
    }
}

PHTable *pht_new(PHTable *t, size_t len) {
    assert(IS_POW_2(len));
    if (!t) {
        t = calloc(1, sizeof(PHTable));
        assert(t);
        t->is_alloc = true;
    } else {
        t->is_alloc = false;
    }

    t->mask = len - 1;
    t->buckets = calloc(len, sizeof(Bucket));
    t->bounds = calloc(len, sizeof(atomic_u64));
    assert(t->buckets && t->bounds);
    phm_init(t);
    return t;
}

void pht_destroy(PHTable *t) {
    free(t->buckets);
    free(t->bounds);
    if (t->is_alloc) {
        free(t);
    }
}

BNode *pht_lookup(PHTable *t, BNode *k, bool (*eq)(BNode *, BNode *)) {
    uint64_t h = k->hcode & t->mask, max = get_probe_bound(t, h);
    for (int i = 0; i <= max; i++) {
        Bucket *buc = bucket(t, h, i);
        uint64_t vs = LOAD(&buc->vs, ACQUIRE);
        uint64_t version = GET_VER(vs);
        uint32_t state = vs & STATE_MASK;
        BNode *node = LOAD(&buc->node, ACQUIRE);
        if (state == B_MEMBER && eq(node, k)) {
            uint64_t vs2 = LOAD(&buc->vs, ACQUIRE);
            if (vs2 == VPACK(version, B_MEMBER)) {
                return node;
            }
        }
    }

    return NULL;
}

bool pht_insert(PHTable *t, BNode *n, bool (*eq)(BNode *, BNode *)) {
    uint64_t h = n->hcode & t->mask, i = 0;
    uint64_t version;
    for (i = 0; i <= t->mask; i++) {
        uint64_t vs = LOAD(&bucket(t, h, i)->vs, ACQUIRE);
        version = GET_VER(vs);
        uint64_t expect = VPACK(version, B_EMPTY);
        if (CMPXCHG(&bucket(t, h, i)->vs, &expect, VPACK(version, B_BUSY), ACQ_REL, RELAXED)) {
            break;
        }
    }
    if (i > t->mask) {
        // Table Full
        return NULL;
    }
    Bucket *buc = bucket(t, h, i);
    buc->hcode = n->hcode;
    STORE(&buc->node, n, RELEASE);
    for (;;) {
        STORE(&buc->vs, VPACK(version, B_VISIBLE), RELEASE);
        cond_raise_bound(t, h, i);
        STORE(&buc->vs, VPACK(version, B_INSERTING), RELEASE);
        bool r = assist(t, n, h, i, version, eq);
        if (LOAD(&buc->vs, ACQUIRE) != VPACK(version, B_COLLIDED)) {
            FAA(&t->size, 1, RELAXED);
            return true;
        }
        if (!r) {
            cond_lower_bound(t, h, i);
            STORE(&buc->vs, VPACK(version + 1, B_EMPTY), RELEASE);
            STORE(&buc->node, NULL, RELEASE);
            return false;
        }
        version++;
    }
}

BNode *pht_erase(PHTable *t, BNode *k, bool (*eq)(BNode *, BNode *)) {
    uint64_t h = k->hcode & t->mask, max = get_probe_bound(t, h);
    for (uint64_t i = 0; i <= max; i++) {
        Bucket *buc = bucket(t, h, i);
        uint64_t vs = LOAD(&buc->vs, ACQUIRE);
        uint64_t version = GET_VER(vs);
        uint32_t state = vs & STATE_MASK;
        BNode *node = LOAD(&buc->node, ACQUIRE);
        if (state == B_MEMBER && eq(node, k)) {
            uint64_t expect = VPACK(version, B_MEMBER);
            if (CMPXCHG(&buc->vs, &expect, VPACK(version, B_BUSY), ACQ_REL, RELAXED)) {
                cond_lower_bound(t, h, i);
                STORE(&buc->vs, VPACK(version + 1, B_EMPTY), RELEASE);
                STORE(&buc->node, NULL, RELEASE);
                FAS(&t->size, 1, RELAXED);
                return node;
            }
        }
    }
    return NULL;
}
