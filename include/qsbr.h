#ifndef QSBR_H
#define QSBR_H

#ifdef __cplusplus
extern "C" {
#endif /* ifndef __cplusplus */

#include <stddef.h>

void qsbr_init(size_t back_logs);
void qsbr_destroy();
void qsbr_reg();
void qsbr_unreg();
void *qsbr_calloc(size_t nmemb, size_t size);
void qsbr_retire(void *ptr, void (*cb)(void *));
void qsbr_quiescent();

#ifdef __cplusplus
}
#endif /* ifndef __cplusplus */

#endif /* ifndef QSBR_H */
