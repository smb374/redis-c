//
// Created by poyehchen on 9/29/25.
//
#include "heap.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

static size_t heap_parent(const size_t i) { return (i + 1) / 2 - 1; }
static size_t heap_left(const size_t i) { return 2 * i + 1; }
static size_t heap_right(const size_t i) { return 2 * i + 2; }

void heap_init(Heap *heap, const size_t cap) {
    heap->cap = cap;
    heap->len = 0;
    heap->nodes = calloc(cap, sizeof(HeapNode));
}

void heap_free(Heap *heap) {
    free(heap->nodes);
    heap->nodes = NULL;
}

void heap_up(Heap *heap, size_t pos) {
    const HeapNode t = heap->nodes[pos];
    while (pos > 0 && heap->nodes[heap_parent(pos)].val > t.val) {
        heap->nodes[pos] = heap->nodes[heap_parent(pos)];
        *heap->nodes[pos].ref = pos;
        pos = heap_parent(pos);
    }
    heap->nodes[pos] = t;
    *heap->nodes[pos].ref = pos;
}

void heap_down(Heap *heap, size_t pos) {
    HeapNode t = heap->nodes[pos];
    for (;;) {
        size_t l = heap_left(pos);
        size_t r = heap_right(pos);
        size_t min_pos = pos;
        uint64_t min_val = t.val;
        if (l < heap->len && heap->nodes[l].val < min_val) {
            min_pos = l;
            min_val = heap->nodes[l].val;
        }
        if (r < heap->len && heap->nodes[r].val < min_val) {
            min_pos = r;
        }
        if (min_pos == pos) {
            break;
        }
        heap->nodes[pos] = heap->nodes[min_pos];
        *heap->nodes[pos].ref = pos;
        pos = min_pos;
    }
    heap->nodes[pos] = t;
    *heap->nodes[pos].ref = pos;
}

void heap_update(Heap *heap, const size_t pos) {
    if (pos > 0 && heap->nodes[heap_parent(pos)].val > heap->nodes[pos].val) {
        heap_up(heap, pos);
    } else {
        heap_down(heap, pos);
    }
}

void heap_upsert(Heap *heap, size_t pos, HeapNode node) {
    if (pos < heap->len) {
        heap->nodes[pos] = node;
    } else if (pos == (size_t) -1 && heap->len < heap->cap) {
        pos = heap->len;
        heap->nodes[pos] = node;
        heap->len++;
    } else {
        heap->cap <<= 1;
        HeapNode *nnodes = realloc(heap->nodes, heap->cap * sizeof(HeapNode));
        if (!nnodes) {
            perror("realloc");
            exit(EXIT_FAILURE);
        }
        heap->nodes = nnodes;
        pos = heap->len;
        heap->nodes[pos] = node;
        heap->len++;
    }

    heap_update(heap, pos);
}

void heap_delete(Heap *heap, const size_t pos) {
    if (pos < heap->len) {
        heap->nodes[pos] = heap->nodes[heap->len - 1];
        heap->len--;
        if (pos < heap->len) {
            heap_update(heap, pos);
        }
    }
}

