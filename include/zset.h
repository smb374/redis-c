//
// Created by poyehchen on 9/28/25.
//

#ifndef ZSET_H
#define ZSET_H

#include "hpmap.h"
#include "skiplist.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ZSet {
    SkipList sl;
    // HMap hm;
    SHPMap hm;
};
typedef struct ZSet ZSet;

struct ZNode {
    SLNode tnode;
    BNode hnode;

    double score;
    size_t len;
    char name[];
};
typedef struct ZNode ZNode;

struct ZHKey {
    BNode node;
    const char *name;
    size_t len;
};
typedef struct ZHKey ZHKey;

bool zhkey_cmp(BNode *node, BNode *key);
bool zhcmp(BNode *ln, BNode *rn);
int zcmp(SLNode *ln, SLNode *rn);

ZNode *znode_new(const char *name, size_t len, double score);

void zset_init(ZSet *zset);
void zset_destroy(ZSet *zset);
bool zset_insert(ZSet *zset, const char *name, size_t len, double score);
ZNode *zset_lookup(ZSet *zset, const char *name, size_t len);
void zset_delete(ZSet *zset, ZNode *node);
void zset_update(ZSet *zset, ZNode *node, double score);
ZNode *zset_seekge(ZSet *zset, double score, const char *name, size_t len);
ZNode *znode_offset(ZSet *zset, ZNode *node, int64_t offset);

#ifdef __cplusplus
}
#endif
#endif

