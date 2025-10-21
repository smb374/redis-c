#include "leapfrog.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>

#include "qsbr.h"
#include "utils.h"

#define MIGRATION_UNIT 32

struct Cell {
    atomic_u64 hcode;
    _Atomic(struct LFNode *) node;
};

struct CellGroup {
    atomic_u64 epoch;
    atomic_uchar deltas[8];
    struct Cell cells[4];
};

struct CLFTable {
    struct CellGroup *groups;
    u64 mask;
};

typedef _Atomic(i64) atomic_i64;

struct Migration {
    struct CLFTable *src, *dst;
    atomic_u64 migrate_pos, threads;
    atomic_bool overflowed;
};

static struct CLFTable *clft_new(size_t size) {
    size_t cap = next_pow2(size);
    struct CLFTable *t = qsbr_calloc(1, sizeof(struct CLFTable) + sizeof(struct CellGroup) * (cap >> 2));

    t->groups = (struct CellGroup *) ((char *) t + sizeof(struct CLFTable));
    t->mask = cap - 1;

    return t;
}

static void clft_destroy(struct CLFTable *t) {
    if (!t)
        return;
    qsbr_retire(t, NULL);
}

// Cells have static hash value once an insert committed
//
// As the table doesn't move a cell's location even on remove, we only need to check
// if the home CellGroup's epoch changed to see if we have any modification to the probe chain.
// For both lookup and remove, we use the epoch to check if there is a new insert that extends
// the tail of the probe chain.
//
// Since it only use one epoch to guard 4 chains, it should be fast with high quality hash
// and big enough size.
static struct Cell *clft_find_cell(struct CLFTable *t, struct LFNode *k, lfn_eq eq) {
    const u64 mask = t->mask;
    u64 home_idx = k->hcode & mask;
    struct CellGroup *home_grp = &t->groups[home_idx >> 2];
    u64 epoch = LOAD(&home_grp->epoch, ACQUIRE), epoch_after;
    for (;;) {
        // (Re)Load home bucket.
        u64 idx = home_idx;
        struct CellGroup *grp = &t->groups[idx >> 2];
        struct Cell *cell = &grp->cells[(idx & 3)];
        u64 phash = LOAD(&cell->hcode, RELAXED);
        struct LFNode *node = LOAD(&cell->node, ACQUIRE);
        if (phash == k->hcode && eq(node, k)) {
            return cell;
        } else if (!phash) {
            // home cell is never inserted, check immediately
            goto CHECK;
        }
        // We don't need to check !phash in the probing seq since
        // only written cells exists in the chain and it'll stop when delta=0
        u8 delta = LOAD(&grp->deltas[(idx & 3)], RELAXED);
        while (delta) {
            idx = (idx + delta) & mask;
            grp = &t->groups[idx >> 2];
            cell = &grp->cells[(idx & 3)];
            phash = LOAD(&cell->hcode, RELAXED);
            node = LOAD(&cell->node, ACQUIRE);
            if (phash == k->hcode && eq(node, k)) {
                return cell;
            }
            delta = LOAD(&grp->deltas[(idx & 3) + 4], RELAXED);
        }
    CHECK:
        epoch_after = LOAD(&home_grp->epoch, ACQUIRE);
        if (epoch != epoch_after) {
            epoch = epoch_after;
            continue;
        }
        return NULL;
    }
}

static struct LFNode *clft_lookup(struct CLFTable *t, struct LFNode *k, lfn_eq eq) {
    struct Cell *cell = clft_find_cell(t, k, eq);
    if (cell) {
        return LOAD(&cell->node, ACQUIRE);
    } else {
        return NULL;
    }
}

static struct LFNode *clft_remove(struct CLFTable *t, struct LFNode *k, lfn_eq eq) {
    struct Cell *cell = clft_find_cell(t, k, eq);
    if (!cell) {
        return NULL;
    }
    struct LFNode *expect = LOAD(&cell->node, ACQUIRE);
    // Since an insert cannot reuse written cell with static hash, it's safe to do
    // a plain CAS loop.
    for (;;) {
        if (!expect) {
            // Removed by others
            return NULL;
        }
        if (CMPXCHG(&cell->node, &expect, NULL, ACQ_REL, ACQUIRE)) {
            // Success
            return expect;
        }
    }
}

static struct LFNode *clft_upsert(struct CLFTable *t, struct LFNode *n, u64 *overflow_idx, lfn_eq eq) {
    const u64 mask = t->mask, hash = n->hcode;
    u64 home_idx = hash & mask, idx = home_idx;
    struct CellGroup *home_grp = &t->groups[idx >> 2], *grp = home_grp;
    struct Cell *cell = &grp->cells[(idx & 3)];
    u64 phash = LOAD(&cell->hcode, RELAXED);
    if (!phash) {
        // Vacant slot, try to reserve
        if (CMPXCHG(&cell->hcode, &phash, hash, ACQ_REL, ACQUIRE)) {
            // Reserve success, write node & increase epoch
            STORE(&cell->node, n, RELEASE);
            FAA(&home_grp->epoch, 1, RELEASE);
            return n;
        }
    }
    struct LFNode *node = LOAD(&cell->node, ACQUIRE);
    if (phash == hash && eq(node, n)) {
        // Match, return tagged node
        return tag_ptr(node, 1);
    }

    u64 max_idx = idx + mask, link_level = 0;
    atomic_uchar *prev_link = NULL;
    for (;;) {
    FOLLOW:
        prev_link = &grp->deltas[(idx & 3) + link_level];
        link_level = 4;
        u8 delta = LOAD(prev_link, RELAXED);
        if (delta) {
            idx += delta;
            grp = &t->groups[idx >> 2];
            cell = &grp->cells[(idx & 3)];
            phash = LOAD(&cell->hcode, RELAXED);
            if (!phash) {
                // Wait phash to be seen
                do {
                    phash = LOAD(&cell->hcode, ACQUIRE);
                } while (!phash);
            }
            node = LOAD(&cell->node, ACQUIRE);
            if (phash == hash && eq(node, n)) {
                // Match, return tagged node
                return tag_ptr(node, 1);
            }
        } else {
            u64 prev_idx = idx;
            u64 remain = MIN(max_idx - idx, LINEAR_SEARCH_LIMIT);
            while (remain--) {
                idx++;
                grp = &t->groups[idx >> 2];
                cell = &grp->cells[(idx & 3)];
                phash = LOAD(&cell->hcode, RELAXED);
                if (!phash) {
                    // Vacant slot, try to reserve
                    if (CMPXCHG(&cell->hcode, &phash, hash, ACQ_REL, ACQUIRE)) {
                        // Reserve success, write node & increase epoch
                        STORE(&cell->node, n, RELEASE);
                        FAA(&home_grp->epoch, 1, RELEASE);
                        // Link delta
                        u8 delta = idx - prev_idx;
                        STORE(prev_link, delta, RELEASE);
                        return n;
                    }
                }
                u64 x = phash ^ hash;
                if (!x && eq(node, n)) {
                    // Match, return tagged node
                    u8 delta = idx - prev_idx;
                    STORE(prev_link, delta, RELEASE);
                    return tag_ptr(node, 1);
                } else if (!(x & mask)) {
                    // Missed link, link for the sleeper & restart linear probe
                    u8 delta = idx - prev_idx;
                    STORE(prev_link, delta, RELEASE);
                    goto FOLLOW;
                }
            }
            // Too crowded, time for migration
            *overflow_idx = idx + 1;
            return NULL;
        }
    }
}

// Use DCLI to have only one thread start the migration
static void clfm_begin_migrate(struct CLFMap *m, struct CLFTable *src, size_t next_size) {
    struct Migration *job = LOAD(&m->job, RELAXED);
    if (job) {
        return;
    } else {
        pthread_mutex_lock(&m->mlock);
        job = LOAD(&m->job, RELAXED);
        if (!job) {
            job = qsbr_calloc(1, sizeof(struct Migration));
            job->src = src;
            job->dst = clft_new(next_size);
            job->migrate_pos = 0;
            STORE(&m->job, job, RELEASE);
        }
        pthread_mutex_unlock(&m->mlock);
    }
}

// Estimates the rough size of the next table.
static u64 clfm_estimate_next_size(struct CLFMap *m, struct CLFTable *src, const u64 overflow_idx) {
    const u64 mask = src->mask;
    u64 idx = overflow_idx - LINEAR_SEARCH_LIMIT;
    u64 in_use = 0;
    for (u64 remain = LINEAR_SEARCH_LIMIT; remain > 0; remain--) {
        struct CellGroup *grp = &src->groups[idx >> 2];
        struct Cell *cell = &grp->cells[(idx & 3)];
        struct LFNode *node = LOAD(&cell->node, RELAXED);
        if (node) {
            in_use++;
        }
        idx++;
    }
    float ratio = ((float) in_use) / LINEAR_SEARCH_LIMIT;
    float estimate = ((float) (mask + 1)) * ratio;
    // MIN_SIZE = 8
    u64 next_size = MAX(MIN_SIZE, (u64) (estimate * 2));
    return next_size;
}

// Spin wait until one of m->job or m->epoch changes
static inline void spin_wait(struct CLFMap *m, struct Migration *job) {
    u64 epoch = LOAD(&m->epoch, ACQUIRE);
    int spin = 0;
    // Spin loop to wait:
    // - m->job changes (whether it ends or another started because of an overflow)
    // - m->epoch changes (migration successes and a write commits)
    while (job == LOAD(&m->job, ACQUIRE) && epoch == LOAD(&m->epoch, ACQUIRE)) {
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

static bool clfm_migrate_range(struct Migration *job, const u64 start, lfn_eq eq) {
    struct CLFTable *src = job->src, *dst = job->dst;
    struct CellGroup *grp;
    struct Cell *cell;
    const u64 mask = job->src->mask;
    for (u64 i = 0; i < MIGRATION_UNIT && start + i <= mask; i++) {
        u64 idx = start + i;
        grp = &src->groups[idx >> 2];
        cell = &grp->cells[(idx & 3)];
        struct LFNode *node = LOAD(&cell->node, ACQUIRE);
        if (node) {
            // We don't actually use overflow since we're already in a migration
            // Just double the size of dst and restart.
            u64 overflow;
            if (!clft_upsert(dst, node, &overflow, eq)) {
                return false;
            }
        }
    }
    return true;
}

// If the migration failed then the helper don't set m->active, the caller needs to check this.
static void clfm_migrate_helper(struct CLFMap *m, struct Migration *job, lfn_eq eq) {
    struct CLFTable *src = job->src, *dst = job->dst;
    FAA(&job->threads, 1, ACQ_REL);
    do {
        // MIGRATION_UNIT is small so we can tolerate some excess writes to the new table if
        // an overflow is reported in other migration unit.
        u64 start = FAA(&job->migrate_pos, MIGRATION_UNIT, ACQ_REL);
        if (start > src->mask) {
            break;
        }
        if (!clfm_migrate_range(job, start, eq)) {
            STORE(&job->overflowed, true, RELEASE);
            break;
        }
    } while (!LOAD(&job->overflowed, ACQUIRE));

    if (FAA(&job->threads, -1, ACQ_REL) == 1) {
        if (LOAD(&job->overflowed, ACQUIRE)) {
            STORE(&m->job, NULL, RELEASE);
            qsbr_retire(job, NULL);
            return;
        }
        STORE(&m->active, dst, RELEASE);
        qsbr_retire(src, NULL);
        // Retire job at last since the spin wait loop needs to wait at least
        // m->active changes if the migration is succeessed.
        STORE(&m->job, NULL, RELEASE);
        qsbr_retire(job, NULL);
        return;
    }

    spin_wait(m, job);
}
