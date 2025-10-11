#ifndef PHMAP_H
#define PHMAP_H

#include <stddef.h>
#include <stdint.h>

#include "utils.h"

#define BOUND_MASK 0xfffffffffffffffe
#define VERSION_MASK 0xfffffffffffffff8
#define STATE_MASK 0x7
#define INIT_PHT_SIZE 256

enum BucketState {
    B_EMPTY = 0b000,
    B_BUSY = 0b001,
    B_COLLIDED = 0b010,
    B_VISIBLE = 0b011,
    B_INSERTING = 0b100,
    B_MEMBER = 0b101,
    B_MOVED = 0b111,
};

struct BNode {
    uint64_t hcode;
};
typedef struct BNode BNode;
struct Bucket;
typedef struct Bucket Bucket;
struct PHTable;
typedef struct PHTable PHTable;
struct PHMap;
typedef struct PHMap PHMap;

#ifndef __cplusplus
#include <stdatomic.h>

struct Bucket {
    // [61-bit version|3-bit state]
    atomic_u64 vs;
    uint64_t hcode;
    _Atomic(BNode *) node;
};

struct PHTable {
    // [63-bit bound|1-bit scanniong]
    atomic_u64 *bounds;
    Bucket *buckets;
    size_t mask;
    atomic_size_t size;
    bool is_alloc;
};

struct PHMap {
    _Atomic(PHTable *) older, newer;
    bool is_alloc;
};
#endif

PHTable *pht_new(PHTable *t, size_t len);
void pht_destroy(PHTable *t);
BNode *pht_lookup(PHTable *t, BNode *k, bool (*eq)(BNode *, BNode *));
bool pht_insert(PHTable *t, BNode *n, bool (*eq)(BNode *, BNode *));
BNode *pht_erase(PHTable *t, BNode *k, bool (*eq)(BNode *, BNode *));

#endif /* ifndef PHMAP_H */
