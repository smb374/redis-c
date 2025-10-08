//
// Created by poyehchen on 9/30/25.
//

#ifndef KVSTORE_H
#define KVSTORE_H

#ifdef __cplusplus
extern "C" {
#endif
#include "hashtable.h"
#include "parse.h"
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

struct KVStore;
typedef struct KVStore KVStore;
struct Entry;
typedef struct Entry Entry;

#ifndef __cplusplus
struct Entry {
    HNode node;
    spin_rwlock lock;

    uint32_t type;
    vstr *key;
    union {
        vstr *s;
        ZSet zs;
    } val;
};
struct KVStore {
    CHMap store;
    bool is_alloc;
};
#endif

Entry *entry_new(const vstr *key, uint32_t type);
bool entry_eq(HNode *ln, HNode *rn);

KVStore *kv_new(KVStore *kv);
void kv_clear(KVStore *kv);
void do_req(KVStore *kv, const simple_req *req, RingBuf *out);

// void kv_clear_entry(KVStore *kv, Entry *e);
// void kv_set_ttl(KVStore *kv, Entry *e, int64_t ttl);
// int32_t next_timer_ms(KVStore *kv);
// void process_timer(KVStore *kv);

#ifdef __cplusplus
}
#endif
#endif // KVSTORE_H
