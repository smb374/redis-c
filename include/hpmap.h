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

struct HPMap;
typedef struct HPMap HPMap;

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
