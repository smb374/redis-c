#ifndef CSKIPLIST_H
#define CSKIPLIST_H

#ifdef __cplusplus
extern "C" {
#endif /* ifdef __cplusplus */

#include <stdbool.h>
#include <stdint.h>

#define CSKIPLIST_MAX_LEVELS 64

struct CSKey {
    uint64_t key, nonce;
};
typedef struct CSKey CSKey;
struct CSNode;
typedef struct CSNode CSNode;
// `CSList`, unlike the serial `SList` in `skiplist.h`,
// does not use intrusive nodes to keep it lock-free.
struct CSList;
typedef struct CSList CSList;

#ifndef __cplusplus
typedef _Atomic(void *) atomic_ptr;

struct CSNode {
    int32_t level;
    CSKey key;
    _Atomic(CSNode *) next[CSKIPLIST_MAX_LEVELS];
    atomic_ptr ptr;
};
struct CSList {
    CSNode head, tail;
    bool is_alloc;
};
#endif

int cskey_cmp(CSKey l, CSKey r);
CSList *csl_new(CSList *l);
// NOTE: Should be run in single thread.
void csl_destroy(CSList *l);
void *csl_lookup(CSList *l, CSKey key);
void *csl_remove(CSList *l, CSKey key);
void *csl_update(CSList *l, CSKey key, void *val);
CSKey csl_find_min_key(CSList *l);
void *csl_pop_min(CSList *l);

#ifdef __cplusplus
}
#endif /* ifdef __cplusplus */
#endif /* ifndef CSKIPLIST_H */
