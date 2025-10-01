//
// Created by poyehchen on 9/29/25.
//

#ifndef HEAP_H
#define HEAP_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

struct HeapNode {
    uint64_t val;
    size_t *ref;
};
typedef struct HeapNode HeapNode;

struct Heap {
    size_t len, cap;
    HeapNode *nodes;
};
typedef struct Heap Heap;

void heap_init(Heap *heap, size_t cap);
void heap_free(Heap *heap);
void heap_up(Heap *heap, size_t pos);
void heap_down(Heap *heap, size_t pos);
void heap_update(Heap *heap, size_t pos);
void heap_upsert(Heap *heap, size_t pos, HeapNode node);
void heap_delete(Heap *heap, size_t pos);

#ifdef __cplusplus
}
#endif

#endif
