//
// Created by poyehchen on 9/30/25.
//

#ifndef KVSTORE_H
#define KVSTORE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <ev.h>
#include <stddef.h>
#include <stdint.h>

#include "connection.h"
#include "cskiplist.h"
#include "hpmap.h"
#include "parse.h"
#include "thread_pool.h"
#include "utils.h"
#include "zset.h"

enum EntType {
    ENT_INIT = 0,
    ENT_STR = 1,
    ENT_ZSET = 2,
};

enum ErrType {
    ERR_UNKNOWN = 1,
    ERR_TOO_BIG = 2,
    ERR_BAD_TYP = 3,
    ERR_BAD_ARG = 4,
};

#define NOEXPIRE ((CSKey) {-1, 0})

struct KVStore;
typedef struct KVStore KVStore;
struct Entry;
typedef struct Entry Entry;

#ifndef __cplusplus
struct Entry {
    BNode node;
    spin_rwlock lock;

    CSKey expire_ms;
    uint32_t type;
    vstr *key;
    union {
        vstr *s;
        ZSet zs;
    } val;
};
struct KVStore {
    HPMap *store;
    CSList expire;
    ThreadPool pool;
    ev_timer expire_w;
    bool is_alloc;
};
#endif

bool entry_eq(BNode *ln, BNode *rn);

KVStore *kv_new(KVStore *kv);
void kv_clear(KVStore *kv);
void do_owned_req(KVStore *kv, OwnedRequest *oreq, RingBuf *out);
// Called by `try_one_req` to dispatch to thread pool
void kv_dispatch(KVStore *kv, Conn *c, OwnedRequest *req);
// Start thread pool
//
// NOTE: Doesn't start main loop
void kv_start(KVStore *kv);
// Stop thread pool.
//
// Currently by pushing a STOP_MAGIC to result queue
void kv_stop(KVStore *kv);

void kv_set_ttl(KVStore *kv, Entry *ent, int64_t ttl);
uint64_t kv_clean_expired(KVStore *kv);
// void kv_clear_entry(KVStore *kv, Entry *e);
// void kv_set_ttl(KVStore *kv, Entry *e, int64_t ttl);
// int32_t next_timer_ms(KVStore *kv);
// void process_timer(KVStore *kv);

#ifdef __cplusplus
}
#endif
#endif // KVSTORE_H
