//
// Created by poyehchen on 9/30/25.
//
#include "kvstore.h"

#include <assert.h>
#include <ev.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "connection.h"
#include "cqueue.h"
#include "hashtable.h"
#include "parse.h"
#include "ringbuf.h"
#include "serialize.h"
#include "utils.h"
#include "zset.h"

struct Work {
    cnode node;
    OwnedRequest req;
    RingBuf buf;
    const Conn *c;
};

Entry *entry_new(const vstr *key, const uint32_t type) {
    Entry *e = calloc(1, sizeof(Entry));
    spin_rw_init(&e->lock);
    e->type = type;
    if (key) {
        vstr_cpy(&e->key, key);
        e->node.hcode = vstr_hash_rapid(e->key);
    }
    return e;
}

static HNode *upsert_create(HNode *key) {
    assert(key);
    Entry *ent = calloc(1, sizeof(Entry));
    Entry *kent = container_of(key, Entry, node);
    spin_rw_init(&ent->lock);
    vstr_cpy(&ent->key, kent->key);
    ent->node.hcode = kent->node.hcode;

    return &ent->node;
}

bool entry_eq(HNode *ln, HNode *rn) {
    const Entry *le = container_of(ln, Entry, node);
    const Entry *re = container_of(rn, Entry, node);

    return le->key->len == re->key->len && !strncmp(le->key->dat, re->key->dat, le->key->len);
}

KVStore *kv_new(KVStore *kv) {
    if (!kv) {
        kv = calloc(1, sizeof(KVStore));
        kv->is_alloc = true;
    } else {
        kv->is_alloc = false;
    }
    chm_new(&kv->store);
    return kv;
}
void kv_clear(KVStore *kv) {
    // TODO: Purge all nodes & Send to GC
    chm_clear(&kv->store);
    if (kv->is_alloc) {
        free(kv);
    }
}

// get key
void do_get(KVStore *kv, RingBuf *out, vstr *kstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };

    HNode *node = chm_lookup(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_nil(out);
        return;
    }

    Entry *ent = container_of(node, Entry, node);
    spin_rw_rlock(&ent->lock);
    if (ent->type != ENT_STR) {
        out_err(out, ERR_BAD_TYP, "not a string");
    } else {
        out_vstr(out, ent->val.s);
    }
    spin_rw_runlock(&ent->lock);
}

// set key val_str
void do_set(KVStore *kv, RingBuf *out, vstr *kstr, vstr *vstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };

    HNode *node = chm_upsert(&kv->store, &key.node, upsert_create, entry_eq);
    if (!node) {
        out_err(out, ERR_UNKNOWN, "store not initialized");
    } else {
        Entry *ent = container_of(node, Entry, node);
        spin_rw_wlock(&ent->lock);
        switch (ent->type) {
            case ENT_INIT:
                ent->type = ENT_STR;
            case ENT_STR:
                vstr_cpy(&ent->val.s, vstr);
                break;
            case ENT_ZSET:
                out_err(out, ERR_BAD_TYP, "non string entry");
                break;
        }
        spin_rw_wunlock(&ent->lock);
        out_nil(out);
    }
}

// del key
void do_del(KVStore *kv, RingBuf *out, vstr *kstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = chm_delete(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_int(out, 0);
    } else {
        Entry *ent = container_of(node, Entry, node);
        // TODO: Send to GC
        out_int(out, 1);
    }
}

bool keys_cb(HNode *node, void *arg) {
    RingBuf *out = (RingBuf *) arg;
    Entry *ent = container_of(node, Entry, node);
    vstr *key;
    spin_rw_rlock(&ent->lock);
    key = container_of(node, Entry, node)->key;
    spin_rw_runlock(&ent->lock);
    out_vstr(out, key);
    return true;
}

// keys
void do_keys(KVStore *kv, RingBuf *out) {
    out_arr(out, (uint32_t) chm_size(&kv->store));
    chm_foreach(&kv->store, keys_cb, out);
}

// zadd key score name
void do_zadd(KVStore *kv, RingBuf *out, vstr *kstr, const double score, vstr *name) {
    bool added = false;
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };

    HNode *node = chm_upsert(&kv->store, &key.node, upsert_create, entry_eq);
    if (!node) {
        out_err(out, ERR_UNKNOWN, "store not initialized");
        return;
    } else {
        Entry *ent = container_of(node, Entry, node);
        spin_rw_wlock(&ent->lock);
        switch (ent->type) {
            case ENT_INIT:
                ent->type = ENT_ZSET;
                zset_init(&ent->val.zs);
            case ENT_ZSET:
                added = zset_insert(&ent->val.zs, name->dat, name->len, score);
                break;
            case ENT_STR:
                out_err(out, ERR_BAD_TYP, "non zset entry");
                return;
        }
        spin_rw_wunlock(&ent->lock);
    }

    out_int(out, (int64_t) added);
}

// zrem key name
void do_zrem(KVStore *kv, RingBuf *out, vstr *kstr, vstr *name) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = chm_lookup(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_int(out, 0);
    } else {
        Entry *ent = container_of(node, Entry, node);
        spin_rw_wlock(&ent->lock);
        if (ent->type != ENT_ZSET) {
            spin_rw_wunlock(&ent->lock);
            out_err(out, ERR_BAD_TYP, "not a zset");
            return;
        }
        ZNode *znode = zset_lookup(&ent->val.zs, name->dat, name->len);
        if (znode) {
            zset_delete(&ent->val.zs, znode);
        }
        spin_rw_wunlock(&ent->lock);
        out_int(out, znode ? 1 : 0);
    }
}

// zscore key name
void do_zscore(KVStore *kv, RingBuf *out, vstr *kstr, vstr *name) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = chm_lookup(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_nil(out);
    } else {
        Entry *ent = container_of(node, Entry, node);
        spin_rw_rlock(&ent->lock);
        if (ent->type != ENT_ZSET) {
            spin_rw_runlock(&ent->lock);
            out_err(out, ERR_BAD_TYP, "not a zset");
            return;
        }
        const ZNode *znode = zset_lookup(&ent->val.zs, name->dat, name->len);
        if (znode) {
            out_dbl(out, znode->score);
        } else {
            out_nil(out);
        }
        spin_rw_runlock(&ent->lock);
    }
}

// zquery key score name offset limit
void do_zquery(KVStore *kv, RingBuf *out, vstr *kstr, const double score, vstr *name, const int64_t offset,
               const int64_t limit) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = chm_lookup(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_arr(out, 0);
        return;
    }

    Entry *ent = container_of(node, Entry, node);
    spin_rw_rlock(&ent->lock);
    if (ent->type != ENT_ZSET) {
        spin_rw_runlock(&ent->lock);
        out_err(out, ERR_BAD_TYP, "not a zset");
        return;
    }

    if (limit <= 0) {
        spin_rw_runlock(&ent->lock);
        out_arr(out, 0);
        return;
    }
    ZNode *znode = zset_seekge(&ent->val.zs, score, name->dat, name->len);
    znode = znode_offset(&ent->val.zs, znode, offset);

    RingBuf buf;
    rb_init(&buf, 4096);
    int64_t n = 0;
    size_t sz = rb_size(&buf);
    while (znode && n < limit) {
        out_str(&buf, znode->name, znode->len);
        out_dbl(&buf, znode->score);
        SLNode *next = znode->tnode.next[0];
        znode = next ? container_of(next, ZNode, tnode) : NULL;
        n += 2;
    }
    spin_rw_runlock(&ent->lock);
    out_arr(out, n);
    out_buf(out, &buf);
    rb_destroy(&buf);
}

void do_req(KVStore *kv, const simple_req *sreq, RingBuf *out) {
    Request req;
    simple2req(sreq, &req);
    switch (req.type) {
        case CMD_GET:
            return do_get(kv, out, req.key);
        case CMD_SET:
            return do_set(kv, out, req.key, req.args.val);
        case CMD_DEL:
            return do_del(kv, out, req.key);
        case CMD_KEYS:
            return do_keys(kv, out);
        case CMD_ZADD:
            return do_zadd(kv, out, req.key, req.args.zadd_arg.score, req.args.zadd_arg.name);
        case CMD_ZREM:
            return do_zrem(kv, out, req.key, req.args.val);
        case CMD_ZSCORE:
            return do_zscore(kv, out, req.key, req.args.val);
        case CMD_ZQUERY:
            return do_zquery(kv, out, req.key, req.args.zquery_arg.score, req.args.zquery_arg.name,
                             req.args.zquery_arg.offset, req.args.zquery_arg.limit);
        case CMD_PTTL:
        case CMD_PEXPIRE:
            return out_err(out, ERR_UNKNOWN, "command not implemented");
        case CMD_BAD:
            return out_err(out, ERR_BAD_ARG, req.args.err);
        case CMD_UNKNOWN:
            return out_err(out, ERR_UNKNOWN, "unknown command");
    }
}
