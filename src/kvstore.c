//
// Created by poyehchen on 9/30/25.
//
#include "kvstore.h"
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
        e->node.hcode = vstr_hash(e->key);
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
    if (!dlist_empty(&kv->manager.idle)) {
        const Conn *c = container_of(kv->manager.idle.next, Conn, list_node);
        next = c->last_active + TIMEOUT;
    }
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
    while (!dlist_empty(&kv->manager.idle)) {
        Conn *c = container_of(kv->manager.idle.next, Conn, list_node);
        const uint64_t next = c->last_active + TIMEOUT;
        if (next >= now)
            break;
        fprintf(stderr, "closing idle connection: %d\n", c->fd);
        hm_delete(&kv->manager.pool, &c->pool_node, conn_eq);
        conn_clear(c);
    }

    while (kv->expire.len && kv->expire.nodes[0].val < now) {
        Entry *e = container_of(kv->expire.nodes[0].ref, Entry, heap_idx);
        hm_delete(&kv->store, &e->node, entry_eq);
        kv_clear_entry(kv, e);
    }
}
void kv_init(KVStore *kv) {
    bzero(&kv->store, sizeof(Heap));
    cm_init(&kv->manager);
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
    cm_destroy(&kv->manager);
    heap_free(&kv->expire);
}

// get key
void do_get(KVStore *kv, const simple_req *req, RingBuf *out) {
    Entry key = {
            .key = req->argv[1],
            .node.hcode = vstr_hash(req->argv[1]),
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
void do_set(KVStore *kv, const simple_req *req, RingBuf *out) {
    Entry key = {
            .key = req->argv[1],
            .node.hcode = vstr_hash(req->argv[1]),
    };

    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != ENT_STR) {
            out_err(out, ERR_BAD_TYP, "non string entry");
        } else {
            // Modify to return old data in response.
            vstr_cpy(&ent->val.s, req->argv[2]);
        }
    } else {
        Entry *ent = calloc(1, sizeof(Entry));
        ent->type = ENT_STR;
        vstr_cpy(&ent->key, req->argv[1]);
        vstr_cpy(&ent->val.s, req->argv[2]);
        ent->node.hcode = key.node.hcode;
        ent->heap_idx = (size_t) -1;
        hm_insert(&kv->store, &ent->node);
    }
    out_nil(out);
}

// del key
void do_del(KVStore *kv, const simple_req *req, RingBuf *out) {
    Entry key = {
            .key = req->argv[1],
            .node.hcode = vstr_hash(req->argv[1]),
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
    const vstr *key = container_of(node, Entry, node)->key;
    out_vstr(out, key);
    return true;
}

// keys
void do_keys(KVStore *kv, const simple_req *_req, RingBuf *out) {
    out_arr(out, (uint32_t) hm_size(&kv->store));
    hm_foreach(&kv->store, keys_cb, out);
}

// zadd key score member
void do_zadd(KVStore *kv, const simple_req *req, RingBuf *out) {
    double score;
    if (!str2dbl(req->argv[2], &score)) {
        out_err(out, ERR_BAD_ARG, "expect a float");
        return;
    }
    Entry key = {
            .key = req->argv[1],
            .node.hcode = vstr_hash(req->argv[1]),
    };
    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);

    Entry *ent = NULL;
    if (!node) {
        ent = calloc(1, sizeof(Entry));
        ent->type = ENT_ZSET;
        vstr_cpy(&ent->key, req->argv[1]);
        ent->node.hcode = key.node.hcode;
        ent->heap_idx = (size_t) -1;
        zset_init(&ent->val.zs);
        hm_insert(&kv->store, &ent->node);
    } else {
        ent = container_of(node, Entry, node);
        if (ent->type != ENT_ZSET) {
            out_err(out, ERR_BAD_TYP, "not a zset");
            return;
        }
    }

    const vstr *name = req->argv[3];
    const bool added = zset_insert(&ent->val.zs, name->dat, name->len, score);
    return out_int(out, (int64_t) added);
}

// zrem key name
void do_zrem(KVStore *kv, const simple_req *req, RingBuf *out) {
    Entry key = {
            .key = req->argv[1],
            .node.hcode = vstr_hash(req->argv[1]),
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
        ZNode *znode = zset_lookup(&ent->val.zs, req->argv[2]->dat, req->argv[2]->len);
        if (znode) {
            zset_delete(&ent->val.zs, znode);
        }
        out_int(out, znode ? 1 : 0);
    }
}

// zscore key name
void do_zscore(KVStore *kv, const simple_req *req, RingBuf *out) {
    Entry key = {
            .key = req->argv[1],
            .node.hcode = vstr_hash(req->argv[1]),
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
        const ZNode *znode = zset_lookup(&ent->val.zs, req->argv[2]->dat, req->argv[2]->len);
        if (znode) {
            out_dbl(out, znode->score);
        } else {
            out_nil(out);
        }
    }
}

// zquery key score name offset limit
void do_zquery(KVStore *kv, const simple_req *req, RingBuf *out) {
    double score;
    if (!str2dbl(req->argv[2], &score)) {
        out_err(out, ERR_BAD_ARG, "expect fp number");
        return;
    }

    int64_t offset = 0, limit = 0;
    if (!str2int(req->argv[4], &offset) || !str2int(req->argv[5], &limit)) {
        out_err(out, ERR_BAD_ARG, "expect int");
        return;
    }

    Entry key = {
            .key = req->argv[1],
            .node.hcode = vstr_hash(req->argv[1]),
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
    ZNode *znode = zset_seekge(&ent->val.zs, score, req->argv[3]->dat, req->argv[3]->len);
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
void do_ttl(KVStore *kv, const simple_req *req, RingBuf *out) {
    Entry key = {
            .key = req->argv[1],
            .node.hcode = vstr_hash(req->argv[1]),
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
void do_expire(KVStore *kv, const simple_req *req, RingBuf *out) {
    int64_t ttl = 0;
    if (!str2int(req->argv[2], &ttl)) {
        out_err(out, ERR_BAD_ARG, "expect i64");
        return;
    }

    Entry key = {
            .key = req->argv[1],
            .node.hcode = vstr_hash(req->argv[1]),
    };
    HNode *node = hm_lookup(&kv->store, &key.node, entry_eq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        kv_set_ttl(kv, ent, ttl);
    }
    out_int(out, node ? 1 : 0);
}

void do_req(KVStore *kv, const simple_req *req, RingBuf *out) {
    if (req->argc == 2 && !strncmp("get", req->argv[0]->dat, 3)) {
        do_get(kv, req, out);
    } else if (req->argc == 3 && !strncmp("set", req->argv[0]->dat, 3)) {
        do_set(kv, req, out);
    } else if (req->argc == 2 && !strncmp("del", req->argv[0]->dat, 3)) {
        do_del(kv, req, out);
    } else if (req->argc == 1 && !strncmp("keys", req->argv[0]->dat, 4)) {
        do_keys(kv, req, out);
    } else if (req->argc == 4 && !strncmp("zadd", req->argv[0]->dat, 4)) {
        do_zadd(kv, req, out);
    } else if (req->argc == 3 && !strncmp("zrem", req->argv[0]->dat, 4)) {
        do_zrem(kv, req, out);
    } else if (req->argc == 3 && !strncmp("zscore", req->argv[0]->dat, 6)) {
        do_zscore(kv, req, out);
    } else if (req->argc == 6 && !strncmp("zquery", req->argv[0]->dat, 6)) {
        do_zquery(kv, req, out);
    } else if (req->argc == 2 && !strncmp("pttl", req->argv[0]->dat, 4)) {
        do_ttl(kv, req, out);
    } else if (req->argc == 3 && !strncmp("pexpire", req->argv[0]->dat, 7)) {
        do_expire(kv, req, out);
    } else {
        out_err(out, ERR_UNKNOWN, "unknown command");
    }
}
