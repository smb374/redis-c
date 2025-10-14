#ifndef CRYSTALLINE_H
#define CRYSTALLINE_H

#include "utils.h"

#ifndef MAX_THREADS
#define MAX_THREADS 16
#endif

#ifndef MAX_IDX
#define MAX_IDX 8
#endif

struct Reservation;

struct Node {
    union {
        atomic_u64 refc;
        struct Node *bnext;
    };
    union {
        u64 birth;
        struct Reservation *slot;
        _Atomic(struct Node *) next;
    };
    struct Node *blink;
};
typedef struct Node Node;

void gc_init(void);
void gc_reg(void);
void gc_unreg(void);
void *gc_alloc(size_t size);
void *gc_calloc(size_t nmemb, size_t size);
void gc_retire(void *ptr);
void *gc_protect(void **obj, int index);
void gc_clear(void);

#endif /* ifndef CRYSTALLINE_H */
