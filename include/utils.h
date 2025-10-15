#ifndef UTILS_H
#define UTILS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

#define container_of(ptr, T, member) ((T *) ((char *) (ptr) - offsetof(T, member)))

#define IS_POW_2(n) (((n) > 0) && (((n) & ((n) - 1)) == 0))
#define MIN(x, y) ((y) ^ (((x) ^ (y)) & -((x) < (y))))
#define MAX(x, y) ((x) ^ (((x) ^ (y)) & -((x) < (y))))

struct vstr {
    uint32_t len;
    char dat[];
};
typedef struct vstr vstr;

struct spin_rwlock;
typedef struct spin_rwlock spin_rwlock;

#ifndef __cplusplus
#include <stdalign.h>
#include <stdatomic.h>

typedef _Atomic(uint64_t) atomic_u64;

enum {
    RELAXED = memory_order_relaxed,
    CONSUME = memory_order_consume,
    ACQUIRE = memory_order_acquire,
    RELEASE = memory_order_release,
    ACQ_REL = memory_order_acq_rel,
    SEQ_CST = memory_order_seq_cst,
};

#define LOAD(t, o) atomic_load_explicit((t), (o))
#define STORE(t, v, o) atomic_store_explicit((t), (v), (o))
#define CMPXCHG(t, e, v, os, of) atomic_compare_exchange_strong_explicit((t), (e), (v), (os), (of))
#define WCMPXCHG(t, e, v, os, of) atomic_compare_exchange_weak_explicit((t), (e), (v), (os), (of))
#define XCHG(t, v, o) atomic_exchange_explicit((t), (v), (o))
#define FAA(t, v, o) atomic_fetch_add_explicit((t), (v), (o))
#define FAS(t, v, o) atomic_fetch_sub_explicit((t), (v), (o))
#define FAAND(t, v, o) atomic_fetch_and_explicit((t), (v), (o))
#define FAOR(t, v, o) atomic_fetch_or_explicit((t), (v), (o))
#define FAXOR(t, v, o) atomic_fetch_xor_explicit((t), (v), (o))

struct spin_rwlock {
    alignas(64) atomic_int ticket;
};
#endif

u64 next_pow2(u64 x);
uint64_t int_hash_fnv(uint64_t val);
uint64_t bytes_hash_fnv(const uint8_t *bytes, size_t len);
uint64_t vstr_hash_fnv(const vstr *v);
uint64_t int_hash_rapid(uint64_t val);
uint64_t bytes_hash_rapid(const uint8_t *bytes, size_t len);
uint64_t vstr_hash_rapid(const vstr *v);
void logger(FILE *f, const char *tag, const char *format, ...);
void msg(const char *msg);
void die(const char *source);
void set_nonblock(int fd);
void set_reuseaddr(int fd);
uint64_t get_clock_ms();

vstr *vstr_new(const char *s, uint32_t len);
vstr *vstr_new_s(const char *s);
void vstr_cpy(vstr **dst, const vstr *src);
void vstr_scpy(vstr *dst, const char *s);
void vstr_destroy(vstr *s);

void spin_rw_init(spin_rwlock *l);
void spin_rw_rlock(spin_rwlock *l);
void spin_rw_runlock(spin_rwlock *l);
void spin_rw_wlock(spin_rwlock *l);
void spin_rw_wunlock(spin_rwlock *l);

#ifdef __cplusplus
}
#endif

#endif
