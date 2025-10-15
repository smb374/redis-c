//
// Created by poyehchen on 9/30/25.
//
#include "kvstore.h"

#include <assert.h>
#include <ev.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "connection.h"
#include "cqueue.h"
#include "cskiplist.h"
#include "hpmap.h"
#include "parse.h"
#include "qsbr.h"
#include "ringbuf.h"
#include "serialize.h"
#include "thread_pool.h"
#include "utils.h"
#include "zset.h"

#define MAX_MSG (32 << 20)
#define TIMEOUT 5000
#define TIMEOUT_S 5.0

static atomic_u64 g_nonce_cnt = 0;

struct Work {
    cnode node;
    OwnedRequest *req;
    RingBuf *buf;
    KVStore *kv;
    Conn *c;
};
typedef struct Work Work;

struct Result {
    cnode node;
    RingBuf *buf;
    Conn *c;
};
typedef struct Result Result;

// Callbacks for thread pool
static bool kv_res_cb(cnode *rn) {
    if ((uint64_t) rn == STOP_MAGIC) {
        return true;
    }
    struct ev_loop *loop = ev_default_loop(0);
    Result *r = container_of(rn, Result, node);
    Conn *c = r->c;
    // Write buf to outgo
    size_t resp_size = rb_size(r->buf);
    if (resp_size > MAX_MSG) {
        rb_clear(r->buf);
        out_err(r->buf, ERR_TOO_BIG, "message too long");
        resp_size = rb_size(r->buf);
    }
    write_u32(&c->outgo, (uint32_t) resp_size);
    out_buf(&c->outgo, r->buf);
    // Add EV_WRITE for conn
    if (c->fd) {
        ev_io_stop(loop, &c->iow);
        ev_io_set(&c->iow, c->fd, EV_READ | EV_WRITE);
        ev_io_start(loop, &c->iow);
    }
    // Cleanup
    rb_destroy(r->buf);
    free(r->buf);
    free(r);
    return false;
}

static cnode *kv_wrk_cb(cnode *wn) {
    // Setup
    Work *w = container_of(wn, Work, node);
    KVStore *kv = w->kv;
    Result *r = calloc(1, sizeof(Result));
    // Do req
    do_owned_req(kv, w->req, w->buf);
    // Setup result
    r->buf = w->buf;
    r->c = w->c;
    // Clean work.
    w->buf = NULL;
    owned_req_destroy(w->req);
    free(w);
    return &r->node;
}

static void kv_expire_cb(EV_P_ ev_timer *w, const int revents) {
    KVStore *kv = w->data;
    uint64_t next_expire = kv_clean_expired(kv);
    ev_timer_set(w, next_expire >= TIMEOUT ? TIMEOUT_S : (double) next_expire / 1000.0, 0.);
    ev_timer_start(EV_A_ w);
}

static Entry *create_empty_entry(vstr *key) {
    assert(key);
    Entry *ent = calloc(1, sizeof(Entry));
    spin_rw_init(&ent->lock);
    vstr_cpy(&ent->key, key);
    ent->type = ENT_INIT;
    ent->node.hcode = vstr_hash_rapid(key);
    ent->expire_ms = NOEXPIRE;

    return ent;
}

static void entry_destroy(void *p) {
    if (!p)
        return;
    Entry *ent = p;
    ent->expire_ms = NOEXPIRE;
    vstr_destroy(ent->key);
    switch (ent->type) {
        case ENT_STR:
            vstr_destroy(ent->val.s);
            break;
        case ENT_ZSET:
            zset_destroy(&ent->val.zs);
            break;
    }

    free(ent);
}

bool entry_eq(BNode *ln, BNode *rn) {
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
    kv->store = hpm_new(NULL, 4096);
    pool_init(&kv->pool, kv_res_cb);
    csl_new(&kv->expire);
    return kv;
}
void kv_clear(KVStore *kv) {
    // TODO: Purge all nodes & Send to GC
    pool_destroy(&kv->pool);
    hpm_destroy(kv->store);
    csl_destroy(&kv->expire);
    if (kv->is_alloc) {
        free(kv);
    }
}
void kv_dispatch(KVStore *kv, Conn *c, OwnedRequest *req) {
    ThreadPool *pool = &kv->pool;

    Work *w = calloc(1, sizeof(Work));
    assert(w);
    w->buf = calloc(1, sizeof(RingBuf));
    rb_init(w->buf, 4096);
    w->c = c;
    w->kv = kv;
    w->req = req;

    pool_post(pool, &w->node);
}

void kv_start(KVStore *kv) {
    struct ev_loop *loop = ev_default_loop(0);
    pool_start(&kv->pool, kv_wrk_cb);
    ev_timer_init(&kv->expire_w, kv_expire_cb, TIMEOUT_S, 0.);
    kv->expire_w.data = kv;
    ev_timer_start(loop, &kv->expire_w);
}
void kv_stop(KVStore *kv) {
    logger(stderr, "INFO", "[master] Send stop signal...\n");
    struct ev_loop *loop = ev_default_loop(0);
    ev_timer_stop(loop, &kv->expire_w);
    cq_put(kv->pool.result_q, (cnode *) STOP_MAGIC);
    ev_async_send(EV_DEFAULT, &kv->pool.rev);
}

void kv_set_ttl(KVStore *kv, Entry *ent, int64_t ttl) {
    spin_rw_wlock(&ent->lock);
    if (cskey_cmp(ent->expire_ms, NOEXPIRE)) {
        csl_remove(&kv->expire, ent->expire_ms);
    }
    if (ttl < 0) {
        ent->expire_ms = NOEXPIRE;
    } else {
        ent->expire_ms.key = get_clock_ms() + ttl;
        ent->expire_ms.nonce = atomic_fetch_add_explicit(&g_nonce_cnt, 1, memory_order_relaxed);
        csl_update(&kv->expire, ent->expire_ms, ent);
    }
    spin_rw_wunlock(&ent->lock);
}

// Returns the min not-expired key
uint64_t kv_clean_expired(KVStore *kv) {
    // No need to lock as we don't read the content of ent
    CSKey now = {get_clock_ms(), UINT64_MAX};
    CSKey expire_ms = csl_find_min_key(&kv->expire);
    // now >= expire_ms
    while (cskey_cmp(now, expire_ms) >= 0) {
        Entry *ent = csl_pop_min(&kv->expire);
        if (ent) {
            spin_rw_rlock(&ent->lock);
            // if (ent->expire_ms > now) {
            if (cskey_cmp(ent->expire_ms, now) > 0) {
                csl_update(&kv->expire, ent->expire_ms, ent);
                expire_ms = ent->expire_ms;
            } else {
                BNode *res = hpm_remove(kv->store, &ent->node, entry_eq);
                if (res) {
                    qsbr_alloc_cb(g_qsbr_gc, entry_destroy, ent);
                }
                expire_ms = csl_find_min_key(&kv->expire);
            }
            spin_rw_runlock(&ent->lock);
        } else {
            expire_ms = csl_find_min_key(&kv->expire);
        }
        now.key = get_clock_ms();
    }

    return expire_ms.key - now.key;
}

// get key
void do_get(KVStore *kv, RingBuf *out, vstr *kstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };

    BNode *node = hpm_lookup(kv->store, &key.node, entry_eq);
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
    Entry *e = create_empty_entry(kstr);

    BNode *node = hpm_upsert(kv->store, &e->node, entry_eq);
    if (!node) {
        out_err(out, ERR_UNKNOWN, "store not initialized");
        free(e);
    } else {
        Entry *found = container_of(node, Entry, node);
        spin_rw_wlock(&found->lock);
        switch (found->type) {
            case ENT_INIT:
                found->type = ENT_STR;
            case ENT_STR:
                vstr_cpy(&found->val.s, vstr);
                break;
            case ENT_ZSET:
                spin_rw_wunlock(&found->lock);
                free(e);
                out_err(out, ERR_BAD_TYP, "non string entry");
                return;
        }
        spin_rw_wunlock(&found->lock);
        if (found != e) {
            free(e);
        }
        out_nil(out);
    }
}

// del key
void do_del(KVStore *kv, RingBuf *out, vstr *kstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    BNode *node = hpm_remove(kv->store, &key.node, entry_eq);
    if (!node) {
        out_int(out, 0);
    } else {
        Entry *ent = container_of(node, Entry, node);
        // TODO: Send to GC
        out_int(out, 1);
    }
}

bool keys_cb(BNode *node, void *arg) {
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
    out_arr(out, (uint32_t) hpm_size(kv->store));
    hpm_foreach(kv->store, keys_cb, out, entry_eq);
}

// zadd key score name
void do_zadd(KVStore *kv, RingBuf *out, vstr *kstr, const double score, vstr *name) {
    bool added = false;
    Entry *e = create_empty_entry(kstr);

    BNode *node = hpm_upsert(kv->store, &e->node, entry_eq);
    if (!node) {
        out_err(out, ERR_UNKNOWN, "store not initialized");
        free(e);
        return;
    } else {
        Entry *found = container_of(node, Entry, node);
        spin_rw_wlock(&found->lock);
        switch (found->type) {
            case ENT_INIT:
                found->type = ENT_ZSET;
                zset_init(&found->val.zs);
            case ENT_ZSET:
                added = zset_insert(&found->val.zs, name->dat, name->len, score);
                break;
            case ENT_STR:
                spin_rw_wunlock(&found->lock);
                out_err(out, ERR_BAD_TYP, "non zset entry");
                return;
        }
        spin_rw_wunlock(&found->lock);
        if (found != e) {
            free(e);
        }
    }

    out_int(out, (int64_t) added);
}

// zrem key name
void do_zrem(KVStore *kv, RingBuf *out, vstr *kstr, vstr *name) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    BNode *node = hpm_lookup(kv->store, &key.node, entry_eq);
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
    BNode *node = hpm_lookup(kv->store, &key.node, entry_eq);
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
    BNode *node = hpm_lookup(kv->store, &key.node, entry_eq);
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

void do_pttl(KVStore *kv, RingBuf *out, vstr *kstr) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    BNode *node = hpm_lookup(kv->store, &key.node, entry_eq);
    if (!node) {
        out_int(out, -2);
        return;
    }

    const Entry *ent = container_of(node, Entry, node);
    if (!cskey_cmp(ent->expire_ms, NOEXPIRE)) {
        out_int(out, -1);
        return;
    }

    const uint64_t expire_at = ent->expire_ms.key;
    const uint64_t now = get_clock_ms();
    return out_int(out, expire_at > now ? (int64_t) (expire_at - now) : 0);
}

// pexpire key ttl
void do_pexpire(KVStore *kv, RingBuf *out, vstr *kstr, int64_t ttl) {
    Entry key = {
            .key = kstr,
            .node.hcode = vstr_hash_rapid(kstr),
    };
    BNode *node = hpm_lookup(kv->store, &key.node, entry_eq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        kv_set_ttl(kv, ent, ttl);
    }
    out_int(out, node ? 1 : 0);
}

void do_owned_req(KVStore *kv, OwnedRequest *oreq, RingBuf *out) {
    switch (oreq->req.type) {
        case CMD_GET:
            return do_get(kv, out, oreq->req.key);
        case CMD_SET:
            return do_set(kv, out, oreq->req.key, oreq->req.args.val);
        case CMD_DEL:
            return do_del(kv, out, oreq->req.key);
        case CMD_KEYS:
            return do_keys(kv, out);
        case CMD_ZADD:
            return do_zadd(kv, out, oreq->req.key, oreq->req.args.zadd_arg.score, oreq->req.args.zadd_arg.name);
        case CMD_ZREM:
            return do_zrem(kv, out, oreq->req.key, oreq->req.args.val);
        case CMD_ZSCORE:
            return do_zscore(kv, out, oreq->req.key, oreq->req.args.val);
        case CMD_ZQUERY:
            return do_zquery(kv, out, oreq->req.key, oreq->req.args.zquery_arg.score, oreq->req.args.zquery_arg.name,
                             oreq->req.args.zquery_arg.offset, oreq->req.args.zquery_arg.limit);
        case CMD_PTTL:
            return do_pttl(kv, out, oreq->req.key);
        case CMD_PEXPIRE:
            return do_pexpire(kv, out, oreq->req.key, oreq->req.args.ttl);
        case CMD_BAD:
            return out_err(out, ERR_BAD_ARG, oreq->req.args.err);
        case CMD_UNKNOWN:
            return out_err(out, ERR_UNKNOWN, "unknown command");
    }
}
