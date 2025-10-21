#ifndef LEAPFROG_H
#define LEAPFROG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>

#include "utils.h"

#define LINEAR_SEARCH_LIMIT 128
#define MIN_SIZE 8

struct LFNode {
    u64 hcode;
};

// This should handle if the node it NULL
// Since the hash will stay on the cell.
typedef bool (*lfn_eq)(struct LFNode *, struct LFNode *);

struct LFTable;
struct CLFTable;

struct LFMap {
    struct LFTable *active;
    size_t size;
    bool is_alloc;
};

struct CLFMap;
struct Migration;
#ifndef __cplusplus
#include <pthread.h>
#include <stdatomic.h>

struct CLFMap {
    _Atomic(struct CLFTable *) active;
    _Atomic(struct Migration *) job;
    pthread_mutex_t mlock;
    atomic_u64 epoch;
    atomic_size_t size;
    bool is_alloc;
};
#endif

struct LFMap *lfm_new(struct LFMap *m, size_t size);
void lfm_destroy(struct LFMap *m);
struct LFNode *lfm_upsert(struct LFMap *m, struct LFNode *node, lfn_eq eq);
struct LFNode *lfm_lookup(struct LFMap *m, struct LFNode *key, lfn_eq eq);
struct LFNode *lfm_remove(struct LFMap *m, struct LFNode *key, lfn_eq eq);
size_t lfm_size(struct LFMap *m);

struct LFMap *clfm_new(struct CLFMap *m, size_t size);
void clfm_destroy(struct CLFMap *m);
struct LFNode *clfm_upsert(struct CLFMap *m, struct LFNode *node, lfn_eq eq);
struct LFNode *clfm_lookup(struct CLFMap *m, struct LFNode *key, lfn_eq eq);
struct LFNode *clfm_remove(struct CLFMap *m, struct LFNode *key, lfn_eq eq);
size_t clfm_size(struct CLFMap *m);

#ifdef __cplusplus
}
#endif
#endif /* ifndef LEAPFROG_H */
