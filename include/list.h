//
// Created by poyehchen on 9/29/25.
//

#ifndef LIST_H
#define LIST_H

#ifdef __cplusplus
extern "C" {
#endif

#include "stdbool.h"

struct DList {
    struct DList *prev, *next;
};
typedef struct DList DList;

static void dlist_init(DList *node) {
    node->prev = node->next = node;
}

static bool dlist_empty(DList *node) {
    return node->next == node;
}

static void dlist_detach(DList *node) {
    DList *prev = node->prev;
    DList *next = node->next;
    prev->next = next;
    next->prev = prev;
}

static void dlist_insert_before(DList *target, DList *rookie) {
    DList *prev = target->prev;
    prev->next = rookie;
    rookie->prev = prev;
    rookie->next = target;
    target->prev = rookie;
}

#ifdef __cplusplus
}
#endif

#endif // LIST_H
