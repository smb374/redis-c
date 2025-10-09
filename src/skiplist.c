//
// Created by poyehchen on 9/28/25.
//
#include "skiplist.h"

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

static bool seeded = false;

// SKIPLIST_MAX_LEVELS = 64
static uint32_t grand() {
    // Mask is to handle when random() generates 0xFFFFFFFFFFFFFFFF
    // s.t. the ffs can report 64 on 0x8000000000000000
    return __builtin_ffsll(~(random() & 0x7FFFFFFFFFFFFFFF));
}

void sl_init(SkipList *sl) {
    assert(sl != NULL);
    if (!seeded) {
        srand((unsigned int) time(NULL));
        seeded = true;
    }
    SLNode *head = calloc(1, sizeof(SLNode));
    head->level = 1;
    sl->head = head;
}

SLNode *sl_search(SkipList *sl, SLNode *key, int (*cmp)(SLNode *, SLNode *)) {
    assert(sl != NULL && sl->head != NULL);
    const SLNode *curr = sl->head;
    SLNode *match = NULL;

    for (int i = sl->head->level - 1; i >= 0; i--) {
        while (curr && curr->next[i] && cmp(curr->next[i], key) < 0) {
            curr = curr->next[i];
        }
    }

    if (curr)
        match = curr->next[0];

    return (match && !cmp(match, key)) ? match : NULL;
}

SLNode *sl_insert(SkipList *sl, SLNode *node, int (*cmp)(SLNode *, SLNode *)) {
    assert(sl != NULL && sl->head != NULL);
    SLNode *update[SKIPLIST_MAX_LEVELS];
    uint32_t rank[SKIPLIST_MAX_LEVELS];
    SLNode *curr = sl->head;
    SLNode *match = NULL;

    for (int i = sl->head->level - 1; i >= 0; i--) {
        rank[i] = (i == sl->head->level - 1) ? 0 : rank[i + 1];
        while (curr && curr->next[i] && cmp(curr->next[i], node) < 0) {
            rank[i] += curr->span[i];
            curr = curr->next[i];
        }
        update[i] = curr;
    }

    if (curr)
        match = curr->next[0];

    if (match && !cmp(match, node)) {
        node->level = match->level;

        for (int i = 0; i < match->level; i++) {
            if (update[i]->next[i] == match) {
                node->span[i] = match->span[i];
                node->next[i] = match->next[i];
                update[i]->next[i] = node;
            }
        }
        return match;
    } else {
        node->level = grand(SKIPLIST_MAX_LEVELS);

        if (node->level > sl->head->level) {
            for (int i = sl->head->level; i < node->level; i++) {
                rank[i] = 0;
                update[i] = sl->head;
                update[i]->span[i] = 0;
            }
            sl->head->level = node->level;
        }

        for (int i = 0; i < node->level; i++) {
            node->next[i] = update[i]->next[i];
            update[i]->next[i] = node;

            const uint32_t old_span = update[i]->span[i];
            update[i]->span[i] = rank[0] + 1 - rank[i];
            node->span[i] = old_span - update[i]->span[i] + 1;
        }

        for (uint32_t i = node->level; i < sl->head->level; i++) {
            update[i]->span[i]++;
        }
        return NULL;
    }
}

SLNode *sl_delete(SkipList *sl, SLNode *key, int (*cmp)(SLNode *, SLNode *)) {
    assert(sl != NULL && sl->head != NULL);
    SLNode *update[SKIPLIST_MAX_LEVELS];
    SLNode *curr = sl->head;
    SLNode *match = NULL;

    for (int i = sl->head->level - 1; i >= 0; i--) {
        while (curr && curr->next[i] && cmp(curr->next[i], key) < 0) {
            curr = curr->next[i];
        }
        update[i] = curr;
    }

    if (curr)
        match = curr->next[0];

    if (match && !cmp(match, key)) {
        for (int i = 0; i < sl->head->level; i++) {
            if (update[i]->next[i] == match) {
                update[i]->span[i] += match->span[i] - 1;
                update[i]->next[i] = match->next[i];
            } else {
                update[i]->span[i]--;
            }
        }

        while (sl->head->level > 1 && !sl->head->next[sl->head->level - 1]) {
            sl->head->level--;
        }

        return match;
    } else {
        return NULL;
    }
}

SLNode *sl_lookup_by_rank(SkipList *sl, const uint32_t rank) {
    SLNode *curr = sl->head;
    uint32_t traversed = 0; // Rank of `curr` is `traversed`

    // Start from the highest level
    for (int i = sl->head->level - 1; i >= 0; i--) {
        // Move forward while the next node is still within our rank
        while (curr->next[i] && (traversed + curr->span[i]) <= rank) {
            traversed += curr->span[i];
            curr = curr->next[i];
        }
    }

    if (traversed == rank) {
        return curr;
    }
    return NULL; // Rank is out of bounds
}

uint32_t sl_get_rank(SkipList *sl, SLNode *key, int (*cmp)(SLNode *, SLNode *)) {
    SLNode *curr = sl->head;
    uint32_t rank = 0;

    for (int i = sl->head->level - 1; i >= 0; i--) {
        while (curr->next[i] && cmp(curr->next[i], key) < 0) {
            rank += curr->span[i]; // Add the span of the link we are traversing
            curr = curr->next[i];
        }
    }

    // After the main loop, curr is the predecessor. Move one more step.
    if (curr->next[0] && cmp(curr->next[0], key) == 0) {
        return rank + 1; // Add 1 for the final step
    }

    return 0; // Not found
}
