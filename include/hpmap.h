#ifndef HPMAP_H
#define HPMAP_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>

#include "utils.h"

#define MASK_RANGE 64
#define INSERT_RANGE (1024 << 2)
#define SEGMENT_SIZE 128

struct BNode {
    u64 hcode;
};
typedef struct BNode BNode;

typedef bool (*node_eq)(BNode *, BNode *);

struct Segment;
typedef struct Segment Segment;

struct Bucket;
typedef struct Bucket Bucket;

struct HPTable;
typedef struct HPTable HPTable;

struct HPMap;
typedef struct HPMap HPMap;

#ifndef __cplusplus
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
    bool is_alloc;
};

struct HPMap {
    _Atomic(struct HPTable *) active;
    atomic_u64 migrate_pos, mthreads, size;
    atomic_bool migration_started;
    pthread_mutex_t mlock;
    pthread_cond_t mcond;
    bool is_alloc;
};
#endif

struct HPTable *hpt_new(struct HPTable *t, size_t size);
void hpt_destroy(struct HPTable *t);
bool hpt_contains(struct HPTable *t, BNode *k, node_eq eq);
bool hpt_add(struct HPTable *t, BNode *n, node_eq eq);
BNode *hpt_remove(struct HPTable *t, BNode *k, node_eq eq);
u64 hpt_size(struct HPTable *t);

struct HPMap *hpm_new(struct HPMap *m, size_t size);
void hpm_destroy(struct HPMap *m);
bool hpm_contains(struct HPMap *m, BNode *k, node_eq eq);
BNode *hpm_lookup(struct HPMap *m, BNode *k, node_eq eq);
bool hpm_add(struct HPMap *m, BNode *n, node_eq eq);
BNode *hpm_remove(struct HPMap *m, BNode *k, node_eq eq);
u64 hpm_size(struct HPMap *m);
BNode *hpm_upsert(struct HPMap *m, BNode *n, node_eq eq);
bool hpm_foreach(struct HPMap *m, bool (*f)(BNode *, void *), void *arg, node_eq eq);

#ifdef __cplusplus
}
#endif
#endif /* ifndef HPMAP_H */
