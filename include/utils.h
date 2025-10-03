#ifndef UTILS_H
#define UTILS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#define container_of(ptr, T, member) ((T *) ((char *) (ptr) - offsetof(T, member)))

struct vstr {
    uint32_t len;
    char dat[];
};
typedef struct vstr vstr;

uint32_t next_pow2(uint32_t x);
uint64_t int_hash_fnv(uint64_t val);
uint64_t bytes_hash_fnv(const uint8_t *bytes, size_t len);
uint64_t vstr_hash_fnv(const vstr *v);
uint64_t int_hash_rapid(uint64_t val);
uint64_t bytes_hash_rapid(const uint8_t *bytes, size_t len);
uint64_t vstr_hash_rapid(const vstr *v);
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

#ifdef __cplusplus
}
#endif

#endif
