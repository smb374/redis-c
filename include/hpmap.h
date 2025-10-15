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

typedef bool (*node_eq)(struct BNode *, struct BNode *);

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
    char data[];
};

struct HPMap {
    _Atomic(struct HPTable *) active; // GC
    atomic_u64 migrate_pos, mthreads, size, epoch;
    atomic_bool migration_started;
    pthread_mutex_t mlock;
    pthread_cond_t mcond;
    bool is_alloc;
};
#endif

struct HPMap *hpm_new(struct HPMap *m, size_t size);
void hpm_destroy(struct HPMap *m);
bool hpm_contains(struct HPMap *m, struct BNode *k, node_eq eq);
struct BNode *hpm_lookup(struct HPMap *m, struct BNode *k, node_eq eq);
bool hpm_add(struct HPMap *m, struct BNode *n, node_eq eq);
struct BNode *hpm_remove(struct HPMap *m, struct BNode *k, node_eq eq);
u64 hpm_size(struct HPMap *m);
struct BNode *hpm_upsert(struct HPMap *m, struct BNode *n, node_eq eq);
bool hpm_foreach(struct HPMap *m, bool (*f)(struct BNode *, void *), void *arg, node_eq eq);

#ifdef __cplusplus
}
#endif
#endif /* ifndef HPMAP_H */
