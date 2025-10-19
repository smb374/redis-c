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

struct SHPTable;
typedef struct SHPTable SHPTable;

struct SHPMap {
    struct SHPTable *active;
    u64 migrate_pos, size;
    bool is_alloc;
};
typedef struct SHPMap SHPMap;

struct CHPMap;
typedef struct CHPMap CHPMap;

#ifndef __cplusplus
struct CHPTable;

struct CHPMap {
    _Atomic(struct CHPTable *) active; // GC
    atomic_u64 migrate_pos, mthreads, size, epoch;
    atomic_bool migrate_started;
    bool is_alloc;
};
#endif

struct SHPMap *shpm_new(struct SHPMap *m, size_t size);
void shpm_destroy(struct SHPMap *m);
struct BNode *shpm_lookup(struct SHPMap *m, struct BNode *k, node_eq eq);
struct BNode *shpm_remove(struct SHPMap *m, struct BNode *k, node_eq eq);
struct BNode *shpm_upsert(struct SHPMap *m, struct BNode *n, node_eq eq);
bool shpm_foreach(struct SHPMap *m, bool (*f)(struct BNode *, void *), void *arg);

struct CHPMap *chpm_new(struct CHPMap *m, size_t size);
void chpm_destroy(struct CHPMap *m);
bool chpm_contains(struct CHPMap *m, struct BNode *k, node_eq eq);
struct BNode *chpm_lookup(struct CHPMap *m, struct BNode *k, node_eq eq);
bool chpm_add(struct CHPMap *m, struct BNode *n, node_eq eq);
struct BNode *chpm_remove(struct CHPMap *m, struct BNode *k, node_eq eq);
u64 chpm_size(struct CHPMap *m);
struct BNode *chpm_upsert(struct CHPMap *m, struct BNode *n, node_eq eq);
bool chpm_foreach(struct CHPMap *m, bool (*f)(struct BNode *, void *), void *arg, node_eq eq);

#ifdef __cplusplus
}
#endif
#endif /* ifndef HPMAP_H */
