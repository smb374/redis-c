#ifndef EBR_H
#define EBR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define MAX_THREADS 64
#define N_EPOCHS 3
#define MAX_LOCAL_GARBAGES 64

struct ebr_ptr {
    struct ebr_ptr *next;
    bool mark;
    uint8_t ptr[];
};
typedef struct ebr_ptr ebr_ptr;

struct ebr_tstate;
typedef struct ebr_tstate ebr_tstate;
struct ebr_manager;
typedef struct ebr_manager ebr_manager;

#ifndef __cplusplus
#include <stdatomic.h>
typedef _Atomic(uint64_t) atomic_u64;

struct ebr_tstate {
    atomic_bool active;
    atomic_u64 local_epoch;
    ebr_ptr *garbages[MAX_LOCAL_GARBAGES];
    size_t gsize;
    int idx;
};

struct ebr_manager {
    atomic_u64 epoch;
    pthread_mutex_t lock, garbage_lock[N_EPOCHS];
    ebr_ptr *garbages[N_EPOCHS];
    ebr_tstate *tstates[MAX_THREADS];
};
#endif

void ebr_init(ebr_manager *m);
void ebr_destroy(ebr_manager *m);
void *ebr_calloc(size_t nmemb, size_t size);
void *ebr_realloc(void *ptr, size_t size);
void ebr_free(ebr_manager *m, void *ptr);
bool ebr_reg(ebr_manager *m);
void ebr_unreg(ebr_manager *m);
void ebr_try_reclaim(ebr_manager *m);
bool ebr_enter(ebr_manager *m);
void ebr_leave(ebr_manager *m);

#ifdef __cplusplus
}
#endif
#endif /* ifndef EBR_H */
