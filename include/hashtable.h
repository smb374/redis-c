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

HNode *hm_lookup(HMap *hm, HNode *key, bool (*eq)(HNode *, HNode *));
HNode *hm_insert(HMap *hm, HNode *node, bool (*eq)(HNode *, HNode *));
void hm_insert_unchecked(HMap *hm, HNode *node);
HNode *hm_delete(HMap *hm, HNode *key, bool (*eq)(HNode *, HNode *));
void hm_clear(HMap *hm);
size_t hm_size(const HMap *hm);
void hm_foreach(const HMap *hm, bool (*f)(HNode *, void *), void *arg);

#ifdef __cplusplus
}
#endif
#endif
