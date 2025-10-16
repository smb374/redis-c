#ifndef CQUEUE_H
#define CQUEUE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>

// Forward declare for both C and C++
struct cnode {
    char _pad[8];
};
typedef struct cnode cnode;
struct cqueue;
typedef struct cqueue cqueue;

// C gets the full implementation details
#ifndef __cplusplus
#include <stdalign.h>
#include <stdatomic.h>

struct cqueue {
    alignas(64) atomic_size_t head;
    alignas(64) atomic_size_t count;
    alignas(64) size_t tail;
    size_t cap;
    cnode *_Atomic *buf;
    bool is_alloc;
};
#endif

// Public API is visible to both
cqueue *cq_init(cqueue *q, size_t cap);
void cq_destroy(cqueue *q);
bool cq_put(cqueue *q, cnode *n);
cnode *cq_pop(cqueue *q);
size_t cq_size(cqueue *q);
size_t cq_cap(cqueue *q);

#ifdef __cplusplus
}
#endif

#endif /* CQUEUE_H */
// vim: set ft=c
