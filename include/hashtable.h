#ifndef HASHTABLE_H
#define HASHTABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "qsbr.h"
#include "utils.h"

#define REHASH_WORK 64
#define MAX_LOAD 8
#define DEFAULT_TABLE_SIZE 128
#define BUCKET_LOCKS 128

struct HNode {
    struct HNode *next;
    uint64_t hcode;
};
typedef struct HNode HNode;

struct HTable {
    HNode **tab;
    size_t mask;
    size_t size;
};
typedef struct HTable HTable;

struct HMap {
    HTable newer, older; // progressive rehashing
    size_t migrate_pos;
};
typedef struct HMap HMap;

struct CHTable;
struct CHMap;
typedef struct CHTable CHTable;
typedef struct CHMap CHMap;

#ifndef __cplusplus
#include <stdatomic.h>
struct CHTable {
    HNode **tab;
    atomic_size_t mask;
    alignas(64) atomic_size_t size; // Use atomic so we don't need a lock for it.
    bool is_alloc;
};

typedef _Atomic(CHTable *) ACHTable;
struct CHMap {
    ACHTable newer, older;
    bool is_alloc;
    qsbr gc;
    alignas(64) atomic_size_t migrate_pos;
    alignas(64) atomic_size_t size; // Use atomic so we don't need a lock for it.
    alignas(64) pthread_mutex_t nb_lock[BUCKET_LOCKS];
    alignas(64) pthread_mutex_t ob_lock[BUCKET_LOCKS];
};
#endif

HNode *hm_lookup(HMap *hm, HNode *key, bool (*eq)(HNode *, HNode *));
HNode *hm_insert(HMap *hm, HNode *node, bool (*eq)(HNode *, HNode *));
void hm_insert_unchecked(HMap *hm, HNode *node);
HNode *hm_delete(HMap *hm, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_clear(HMap *hm);
size_t hm_size(const HMap *hm);
void hm_foreach(const HMap *hm, bool (*f)(HNode *, void *), void *arg);

CHMap *chm_new(CHMap *hm);
void chm_register(CHMap *hm);
HNode *chm_lookup(CHMap *hm, HNode *key, bool (*eq)(HNode *, HNode *));
bool chm_insert(CHMap *hm, HNode *node, bool (*eq)(HNode *, HNode *));
void chm_insert_unchecked(CHMap *hm, HNode *node);
HNode *chm_delete(CHMap *hm, HNode *key, bool (*eq)(HNode *, HNode *));
void chm_clear(CHMap *hm);
size_t chm_size(CHMap *hm);
void chm_foreach(CHMap *hm, bool (*f)(HNode *, void *), void *arg);

#ifdef __cplusplus
}
#endif
#endif
