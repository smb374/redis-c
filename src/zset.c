//
// Created by poyehchen on 9/28/25.
//
#include "zset.h"
#include "utils.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>


ZNode *znode_new(const char *name, const size_t len, const double score) {
    ZNode *node = calloc(1, sizeof(ZNode) + len);
    node->tnode.level = 1;
    node->hnode.next = NULL;
    node->hnode.hcode = bytes_hash_rapid((const uint8_t *) name, len);
    node->score = score;
    node->len = len;
    memcpy(node->name, name, len);

    return node;
}

bool zhkey_cmp(HNode *node, HNode *key) {
    const ZNode *znode = container_of(node, ZNode, hnode);
    const ZHKey *hkey = container_of(key, ZHKey, node);

    if (znode->len != hkey->len) {
        return false;
    }

    return !memcmp(znode->name, hkey->name, znode->len);
}

bool zhcmp(HNode *ln, HNode *rn) {
    const ZNode *lz = container_of(ln, ZNode, hnode);
    const ZNode *rz = container_of(rn, ZNode, hnode);

    if (lz->len != rz->len) {
        return false;
    }

    return !memcmp(lz->name, rz->name, lz->len);
}

int zcmp(SLNode *ln, SLNode *rn) {
    const ZNode *lz = container_of(ln, ZNode, tnode);
    const ZNode *rz = container_of(rn, ZNode, tnode);
    if (lz->score != rz->score) {
        if (lz->score > rz->score) {
            return 1;
        } else if (lz->score < rz->score) {
            return -1;
        } else {
            return 0;
        }
    }

    const int rv = memcmp(lz->name, rz->name, MIN(lz->len, rz->len));
    return !rv ? (int) lz->len - (int) rz->len : rv;
}

void zset_init(ZSet *zset) {
    if (!zset)
        return;

    sl_init(&zset->sl);
    bzero(&zset->hm, sizeof(HMap));
}

void zset_update(ZSet *zset, ZNode *node, const double score) {
    SLNode *tnode = sl_delete(&zset->sl, &node->tnode, zcmp);
    if (!tnode)
        return;
    ZNode *znode = container_of(tnode, ZNode, tnode);
    if (znode != node) {
        // Sanity check
        sl_insert(&zset->sl, &znode->tnode, zcmp);
        return;
    }

    node->score = score;
    sl_insert(&zset->sl, &node->tnode, zcmp);
}

bool zset_insert(ZSet *zset, const char *name, const size_t len, const double score) {
    ZNode *node = zset_lookup(zset, name, len);
    if (node) {
        zset_update(zset, node, score);
        return false;
    }

    node = znode_new(name, len, score);
    hm_insert(&zset->hm, &node->hnode);
    sl_insert(&zset->sl, &node->tnode, zcmp);
    return true;
}

void zset_delete(ZSet *zset, ZNode *node) {
    if (!node)
        return;
    ZHKey zkey = {
            .len = node->len,
            .name = node->name,
            .node.hcode = bytes_hash_rapid((const uint8_t *) node->name, node->len),
    };

    const HNode *found = hm_lookup(&zset->hm, &zkey.node, zhkey_cmp);
    assert(found);
    sl_delete(&zset->sl, &node->tnode, zcmp);
    hm_delete(&zset->hm, &node->hnode, zhcmp);
    free(node);
}

ZNode *zset_lookup(ZSet *zset, const char *name, const size_t len) {
    if (!zset->sl.head) {
        return NULL;
    }
    ZHKey zkey = {
            .len = len,
            .name = name,
            .node.hcode = bytes_hash_rapid((const uint8_t *) name, len),
    };
    HNode *found = hm_lookup(&zset->hm, &zkey.node, zhkey_cmp);
    return found ? container_of(found, ZNode, hnode) : NULL;
}

bool zless(SLNode *node, const double score, const char *name, const size_t len) {
    const ZNode *znode = container_of(node, ZNode, tnode);
    if (znode->score != score) {
        return znode->score < score;
    }

    const int rv = memcmp(znode->name, name, MIN(znode->len, len));
    return !rv ? znode->len < len : rv < 0;
}

ZNode *zset_seekge(ZSet *zset, const double score, const char *name, const size_t len) {
    assert(zset && zset->sl.head);
    const SLNode *curr = zset->sl.head;
    SLNode *match = NULL;

    for (int32_t i = zset->sl.head->level - 1; i >= 0; i--) {
        while (curr && curr->next[i] && zless(curr->next[i], score, name, len)) {
            curr = curr->next[i];
        }
    }

    if (curr)
        match = curr->next[0];

    return match ? container_of(match, ZNode, tnode) : NULL;
}

ZNode *znode_offset(ZSet *zset, ZNode *node, const int64_t offset) {
    const int64_t rank = sl_get_rank(&zset->sl, &node->tnode, zcmp);
    const int64_t target = rank + offset;
    if (target < 1 || target > UINT32_MAX)
        return NULL;
    SLNode *found = sl_lookup_by_rank(&zset->sl, target);
    return found ? container_of(found, ZNode, tnode) : NULL;
}

void zset_destroy(ZSet *zset) {
    if (zset->sl.head) {
        SLNode *curr = zset->sl.head->next[0];
        while (curr) {
            SLNode *next = curr->next[0];
            ZNode *znode = container_of(curr, ZNode, tnode);
            free(znode);
            curr = next;
        }
    }

    free(zset->sl.head);
    hm_clear(&zset->hm);
}
