//
// Created by poyehchen on 9/28/25.
//

#ifndef SKIPLIST_H
#define SKIPLIST_H

#ifdef __cplusplus
extern "C" {
#endif
#include <stdint.h>

#define SKIPLIST_MAX_LEVELS 64

struct SLNode {
    uint32_t level;
    struct SLNode *next[SKIPLIST_MAX_LEVELS];
    uint32_t span[SKIPLIST_MAX_LEVELS];
};
typedef struct SLNode SLNode;

struct SkipList {
    SLNode *head;
};
typedef struct SkipList SkipList;

void sl_init(SkipList *sl);
SLNode *sl_search(SkipList *sl, SLNode *key, int (*cmp)(SLNode *, SLNode *));
SLNode *sl_insert(SkipList *sl, SLNode *node, int (*cmp)(SLNode *, SLNode *));
SLNode *sl_delete(SkipList *sl, SLNode *key, int (*cmp)(SLNode *, SLNode *));
SLNode *sl_lookup_by_rank(SkipList *sl, uint32_t rank);
uint32_t sl_get_rank(SkipList *sl, SLNode *key, int (*cmp)(SLNode *, SLNode *));

#ifdef __cplusplus
}
#endif
#endif

