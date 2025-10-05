#ifndef EBR_H
#define EBR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>

void ebr_clear();
void *ebr_calloc(size_t nmemb, size_t size);
void *ebr_realloc(void *ptr, size_t size);
void ebr_free(void *ptr);
bool ebr_reg();
void ebr_unreg();
void ebr_try_reclaim();
bool ebr_enter();
void ebr_leave();

#ifdef __cplusplus
}
#endif
#endif /* ifndef EBR_H */
