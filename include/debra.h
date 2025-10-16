#ifndef DEBRA_H
#define DEBRA_H

#ifdef __cplusplus
extern "C" {
#endif /* ifdef __cplusplus */

#include <stdbool.h>
#include <stddef.h>


#ifndef INCR_THRES
#define INCR_THRES 32
#endif

#ifndef CHECK_THRES
#define CHECK_THRES 32
#endif

void gc_init();
void gc_reg();
void gc_unreg();
void *gc_alloc(size_t size);
void *gc_calloc(size_t nmemb, size_t size);
void gc_retire_custom(void *ptr, void (*on_free)(void *));
void gc_retire(void *ptr);
bool gc_enter();
void gc_leave();
bool gc_entered();
void gc_clear();

#ifdef __cplusplus
}
#endif /* ifdef __cplusplus */

#endif /* ifndef DEBRA_H */
