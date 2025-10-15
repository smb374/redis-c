#ifndef CRYSTALLINE_H
#define CRYSTALLINE_H


#ifdef __cplusplus
extern "C" {
#endif /* ifndef __cplusplus */

#include <stddef.h>

#ifndef MAX_THREADS
#define MAX_THREADS 64
#endif

#ifndef MAX_IDX
#define MAX_IDX 12
#endif

#define RETIRE_FREQ 128
#define ALLOC_FREQ 128
#define REFC_PROTECT (1UL << 63)
#define INVPTR ((void *) -1LL)

void gc_init(void);
void gc_reg(void);
void gc_unreg(void);
void *gc_alloc(size_t size);
void *gc_calloc(size_t nmemb, size_t size);
void gc_retire_custom(void *ptr, void (*on_free)(void *));
void gc_retire(void *ptr);
void *gc_protect(void **obj, int index);
void gc_clear(void);

#ifdef __cplusplus
}
#endif /* ifndef __cplusplus */
#endif /* ifndef CRYSTALLINE_H */
