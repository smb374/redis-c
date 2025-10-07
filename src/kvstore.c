//
// Created by poyehchen on 9/30/25.
//
#include "kvstore.h"
#include "parse.h"
#include "serialize.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

Entry *entry_new(const vstr *key, const uint32_t type) {
    Entry *e = calloc(1, sizeof(Entry));
    e->heap_idx = -1;
    e->type = type;
    if (key) {
        vstr_cpy(&e->key, key);
        e->node.hcode = vstr_hash_rapid(e->key);
    }
    return e;
}
bool entry_eq(HNode *ln, HNode *rn) {
    const Entry *le = container_of(ln, Entry, node);
    const Entry *re = container_of(rn, Entry, node);

    return le->key->len == re->key->len && !strncmp(le->key->dat, re->key->dat, le->key->len);
}

void kv_clear_entry(KVStore *kv, Entry *e) {
    kv_set_ttl(kv, e, -1);
    hm_delete(&kv->store, &e->node, entry_eq);
    vstr_destroy(e->key);
    switch (e->type) {
        case ENT_STR:
            vstr_destroy(e->val.s);
            break;
        case ENT_ZSET:
            zset_destroy(&e->val.zs);
            break;
        default:
            break;
    }
    free(e);
}
void kv_set_ttl(KVStore *kv, Entry *e, int64_t ttl) {
    if (ttl < 0 && e->heap_idx != (size_t) -1) {
        heap_delete(&kv->expire, e->heap_idx);
        e->heap_idx = (size_t) -1;
    } else {
        const uint64_t expire_at = get_clock_ms() + (uint64_t) ttl;
        const HeapNode node = {expire_at, &e->heap_idx};
        heap_upsert(&kv->expire, e->heap_idx, node);
    }
}
int32_t next_timer_ms(KVStore *kv) {
    const uint64_t now = get_clock_ms();
    uint64_t next = (uint64_t) -1;
    if (kv->expire.len && kv->expire.nodes[0].val < next) {
        next = kv->expire.nodes[0].val;
    }

    if (next == (uint64_t) -1) {
        return -1;
    }
    if (next <= now) {
        return 0;
    }
    return (int32_t) (next - now);
}
void process_timer(KVStore *kv) {
    const uint64_t now = get_clock_ms();
    while (kv->expire.len && kv->expire.nodes[0].val < now) {
        Entry *e = container_of(kv->expire.nodes[0].ref, Entry, heap_idx);
        hm_delete(&kv->store, &e->node, entry_eq);
        kv_clear_entry(kv, e);
    }
}
void kv_init(KVStore *kv) {
    bzero(&kv->store, sizeof(Heap));
    heap_init(&kv->expire, 4096);
}
void kv_clear(KVStore *kv) {
    HTable *tables[] = {&kv->store.newer, &kv->store.older};
    for (int i = 0; i < 2; i++) {
        if (tables[i]->tab) {
            for (size_t j = 0; j <= tables[i]->mask; j++) {
                HNode *node = tables[i]->tab[j];
                while (node) {
                    HNode *next = node->next; // Save next before delete
                    Entry *e = container_of(node, Entry, node);
                    kv_clear_entry(kv, e);
                    node = next;
                }
            }
        }
    }

    hm_clear(&kv->store);
    heap_free(&kv->expire);
}

// get key
void do_get(KVStore *kv, RingBuf *out, vstr *kstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };

    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_nil(out);
        return;
    }

    const Entry *ent = container_of(node, Entry, node);
    if (ent->type != ENT_STR) {
        out_err(out, ERR_BAD_TYP, "not a string");
    } else {
        out_vstr(out, ent->val.s);
    }
}

// set key val_str
void do_set(KVStore *kv, RingBuf *out, vstr *kstr, vstr *vstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };

    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != ENT_STR) {
            out_err(out, ERR_BAD_TYP, "non string entry");
        } else {
            // Modify to return old data in response.
            vstr_cpy(&ent->val.s, vstr);
        }
    } else {
        Entry *ent = calloc(1, sizeof(Entry));
        ent->type = ENT_STR;
        vstr_cpy(&ent->key, kstr);
        vstr_cpy(&ent->val.s, vstr);
        ent->node.hcode = key.node.hcode;
        ent->heap_idx = (size_t) -1;
        hm_insert_unchecked(&kv->store, &ent->node);
    }
    out_nil(out);
}

// del key
void do_del(KVStore *kv, RingBuf *out, vstr *kstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = hm_delete(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_int(out, 0);
    } else {
        Entry *ent = container_of(node, Entry, node);
        kv_clear_entry(kv, ent);
        out_int(out, 1);
    }
}

bool keys_cb(HNode *node, void *arg) {
    RingBuf *out = (RingBuf *) arg;
    vstr *key = container_of(node, Entry, node)->key;
    out_vstr(out, key);
    return true;
}

// keys
void do_keys(KVStore *kv, RingBuf *out) {
    out_arr(out, (uint32_t) hm_size(&kv->store));
    hm_foreach(&kv->store, keys_cb, out);
}

// zadd key score name
void do_zadd(KVStore *kv, RingBuf *out, vstr *kstr, const double score, vstr *name) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);

    Entry *ent = NULL;
    if (!node) {
        ent = calloc(1, sizeof(Entry));
        ent->type = ENT_ZSET;
        vstr_cpy(&ent->key, kstr);
        ent->node.hcode = key.node.hcode;
        ent->heap_idx = (size_t) -1;
        zset_init(&ent->val.zs);
        hm_insert_unchecked(&kv->store, &ent->node);
    } else {
        ent = container_of(node, Entry, node);
        if (ent->type != ENT_ZSET) {
            out_err(out, ERR_BAD_TYP, "not a zset");
            return;
        }
    }

    const bool added = zset_insert(&ent->val.zs, name->dat, name->len, score);
    return out_int(out, (int64_t) added);
}

// zrem key name
void do_zrem(KVStore *kv, RingBuf *out, vstr *kstr, vstr *name) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_int(out, 0);
    } else {
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != ENT_ZSET) {
            out_err(out, ERR_BAD_TYP, "not a zset");
            return;
        }
        ZNode *znode = zset_lookup(&ent->val.zs, name->dat, name->len);
        if (znode) {
            zset_delete(&ent->val.zs, znode);
        }
        out_int(out, znode ? 1 : 0);
    }
}

// zscore key name
void do_zscore(KVStore *kv, RingBuf *out, vstr *kstr, vstr *name) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_nil(out);
    } else {
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != ENT_ZSET) {
            out_err(out, ERR_BAD_TYP, "not a zset");
            return;
        }
        const ZNode *znode = zset_lookup(&ent->val.zs, name->dat, name->len);
        if (znode) {
            out_dbl(out, znode->score);
        } else {
            out_nil(out);
        }
    }
}

// zquery key score name offset limit
void do_zquery(KVStore *kv, RingBuf *out, vstr *kstr, const double score, vstr *name, const int64_t offset,
               const int64_t limit) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_arr(out, 0);
        return;
    }

    Entry *ent = container_of(node, Entry, node);
    if (ent->type != ENT_ZSET) {
        out_err(out, ERR_BAD_TYP, "not a zset");
        return;
    }

    if (limit <= 0) {
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
    out_arr(out, n);
    out_buf(out, &buf);
    rb_destroy(&buf);
}

// pttl key
void do_pttl(KVStore *kv, RingBuf *out, vstr *kstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);
    if (!node) {
        out_int(out, -2);
        return;
    }

    const Entry *ent = container_of(node, Entry, node);
    if (ent->heap_idx == (size_t) -1) {
        out_int(out, -1);
        return;
    }

    const uint64_t expire_at = kv->expire.nodes[ent->heap_idx].val;
    const uint64_t now = get_clock_ms();
    return out_int(out, expire_at > now ? (int64_t) (expire_at - now) : 0);
}

// pexpire key ttl
void do_pexpire(KVStore *kv, RingBuf *out, vstr *kstr, int64_t ttl) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        kv_set_ttl(kv, ent, ttl);
    }
    out_int(out, node ? 1 : 0);
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
            return do_pttl(kv, out, req.key);
        case CMD_PEXPIRE:
            return do_pexpire(kv, out, req.key, req.args.ttl);
        case CMD_BAD:
            return out_err(out, ERR_BAD_ARG, req.args.err);
        case CMD_UNKNOWN:
            return out_err(out, ERR_UNKNOWN, "unknown command");
    }
}
