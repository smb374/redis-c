#include "utils.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <rapidhash.h>
#include <sched.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <threads.h>
#include <time.h>

uint32_t next_pow2m1(uint32_t x) {
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    return x;
}
uint32_t next_pow2(const uint32_t x) { return next_pow2m1(x - 1) + 1; }

// FNV-hash 64-bit
uint64_t int_hash_fnv(const uint64_t val) {
    uint64_t h = 0xcbf29ce484222325;
    for (size_t i = 0; i < sizeof(val); i++) {
        h ^= (val >> (i * 8)) & 0xff;
        h *= 0x100000001b3;
    }
    return h;
}
uint64_t int_hash_rapid(const uint64_t val) { return rapidhash(&val, sizeof(uint64_t)); }

// FNV-hash 32-bit, as individuals are 8-bit only.
uint64_t bytes_hash_fnv(const uint8_t *bytes, const size_t len) {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++) {
        h = (h + bytes[i]) * 0x01000193;
    }
    return h;
}
uint64_t bytes_hash_rapid(const uint8_t *bytes, const size_t len) { return rapidhash(bytes, len); }

// Same as bytes_hash_fnv but for vstr
uint64_t vstr_hash_fnv(const vstr *v) {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < v->len; i++) {
        h = (h + v->dat[i]) * 0x01000193;
    }
    return h;
}
uint64_t vstr_hash_rapid(const vstr *v) { return rapidhash(v->dat, v->len); }

void logger(FILE *f, const char *tag, const char *format, ...) {
#ifdef LOGGING
    char buf[200];
    time_t t = time(NULL);
    strftime(buf, 200, "%Y-%m-%dT%H:%M:%SZ", gmtime(&(time_t) {time(NULL)}));
    fprintf(f, "%s [%s] ", buf, tag);
    va_list args;
    va_start(args, format);
    vfprintf(f, format, args);
    va_end(args);
#endif
}
void msg(const char *msg) { fprintf(stderr, "%s\n", msg); }
void die(const char *source) {
    logger(stderr, "ERROR", "Fatal error from %s: %s\n", source, strerror(errno));
    exit(EXIT_FAILURE);
}

void set_reuseaddr(const int fd) {
    const int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
}

void set_nonblock(const int fd) {
    errno = 0;
    const int flags = fcntl(fd, F_GETFL);
    if (errno) {
        die("fcntl()");
    }

    errno = 0;
    (void) fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    if (errno) {
        die("fcntl()");
    }
}

vstr *vstr_new(const char *s, const uint32_t len) {
    vstr *v = calloc(sizeof(vstr) + len + 1, 1);
    v->len = len;
    memcpy(v->dat, s, len);
    return v;
}

vstr *vstr_new_s(const char *s) {
    const uint32_t len = strnlen(s, 65536);
    vstr *v = calloc(sizeof(vstr) + len + 1, 1);
    v->len = len;
    memcpy(v->dat, s, len);
    return v;
}

void vstr_cpy(vstr **dst, const vstr *src) {
    if (!*dst) {
        *dst = calloc(sizeof(vstr) + src->len + 1, 1);
    } else if ((*dst)->len < src->len) {
        vstr *ndst = calloc(sizeof(vstr) + src->len + 1, 1);
        if (!ndst) {
            perror("calloc()");
            exit(EXIT_FAILURE);
        }
        free(*dst);
        *dst = ndst;
    }
    (*dst)->len = src->len;
    memcpy((*dst)->dat, src->dat, src->len);
}

void vstr_scpy(vstr *dst, const char *s) { strncpy(dst->dat, s, dst->len); }

void vstr_destroy(vstr *s) { free(s); }

uint64_t get_clock_ms() {
    struct timespec ts = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000 + (uint64_t) ts.tv_nsec / 1000000;
}

void spin_rw_init(spin_rwlock *l) { l->ticket = ATOMIC_VAR_INIT(0); }
void spin_rw_rlock(spin_rwlock *l) {
    int v = atomic_load_explicit(&l->ticket, memory_order_acquire);
    while (v < 0 ||
           !atomic_compare_exchange_weak_explicit(&l->ticket, &v, v + 1, memory_order_acq_rel, memory_order_relaxed)) {
        __builtin_ia32_pause();
        v = atomic_load_explicit(&l->ticket, memory_order_acquire);
    }
}
void spin_rw_runlock(spin_rwlock *l) { atomic_fetch_sub_explicit(&l->ticket, 1, memory_order_release); }
void spin_rw_wlock(spin_rwlock *l) {
    int v = 0;
    while (!atomic_compare_exchange_weak_explicit(&l->ticket, &v, -1, memory_order_acq_rel, memory_order_relaxed)) {
        __builtin_ia32_pause();
        v = 0;
    }
}
void spin_rw_wunlock(spin_rwlock *l) { atomic_store_explicit(&l->ticket, 0, memory_order_release); }
