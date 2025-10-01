#include "hashtable.h"
#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

// Internal Functions
void ht_init(HTable* ht, size_t n) {
    assert(n > 0 && ((n - 1) & n) == 0);
    ht->tab = calloc(n, sizeof(HNode*));
    assert(ht->tab != NULL);
    ht->mask = n - 1;
    ht->size = 0;
}

void ht_insert(HTable* ht, HNode* node) {
    const size_t pos = node->hcode & ht->mask;
    HNode* next = ht->tab[pos];
    node->next = next;
    ht->tab[pos] = node;
    ht->size++;
}

HNode** ht_lookup(const HTable* ht, HNode* key, bool (*eq)(HNode*, HNode*)) {
    if (!ht->tab)
        return NULL;

    const size_t pos = key->hcode & ht->mask;
    HNode** from = &ht->tab[pos]; // incoming pointer to the target
    for (HNode* cur; (cur = *from) != NULL; from = &cur->next) {
        if (cur->hcode == key->hcode && eq(cur, key))
            return from; // may be a node, may be a slot
    }
    return NULL;
}

HNode* ht_detach(HTable* ht, HNode** from) {
    HNode* node = *from; // the target node
    *from = node->next;  // update the incoming pointer to the target
    ht->size--;
    return node;
}

void hm_help_rehashing(HMap* hm) {
    size_t nwork = 0;
    while (nwork < REHASH_WORK && hm->older.size > 0) {
        // find a non-empty slot
        HNode** from = &hm->older.tab[hm->migrate_pos];
        if (!*from) {
            hm->migrate_pos++;
            continue; // empty slot
        }
        // move the first list item to the newer table
        ht_insert(&hm->newer, ht_detach(&hm->older, from));
        nwork++;
    }
    // discard the old table if done
    if (hm->older.size == 0 && hm->older.tab) {
        free(hm->older.tab);
        bzero(&hm->older, sizeof(HTable));
    }
}

void hm_trigger_rehashing(HMap* hm) {
    assert(hm->older.tab == NULL);
    // (newer, older) <- (new_table, newer)
    hm->older = hm->newer;
    ht_init(&hm->newer, (hm->newer.mask + 1) * 2);
    hm->migrate_pos = 0;
}

// NOTE: Intend to be used in a loop without modifying the hashtable.
HNode* ht_visit_next(HTVisitor* vis) {
    if (!vis->ht || !vis->ht->mask || vis->slot_pos > vis->ht->mask)
        return NULL;
    if (vis->chain_pos) {
        vis->chain_pos = vis->chain_pos->next;
        if (vis->chain_pos) {
            return vis->chain_pos;
        }
        vis->slot_pos++;
    }
    while (vis->slot_pos <= vis->ht->mask) {
        vis->chain_pos = vis->ht->tab[vis->slot_pos];
        if (vis->chain_pos) {
            return vis->chain_pos;
        }
        vis->slot_pos++;
    }

    return NULL;
}

bool ht_foreach(const HTable* ht, bool (*f)(HNode*, void*), void* arg) {
    for (size_t i = 0; ht->mask && i <= ht->mask; i++) {
        for (HNode* node = ht->tab[i]; node; node = node->next) {
            if (!f(node, arg)) {
                return false;
            }
        }
    }
    return true;
}

// Public Interface
HNode* hm_lookup(HMap* hm, HNode* key, bool (*eq)(HNode*, HNode*)) {
    hm_help_rehashing(hm);
    HNode** from = ht_lookup(&hm->newer, key, eq);
    if (!from) {
        from = ht_lookup(&hm->older, key, eq);
    }
    return from ? *from : NULL;
}

void hm_insert(HMap* hm, HNode* node) {
    if (!hm->newer.tab) {
        ht_init(&hm->newer, 16); // initialize it if empty
    }
    ht_insert(&hm->newer, node); // always insert to the newer table

    if (!hm->older.tab) {
        // check whether we need to rehash
        const size_t threshold = (hm->newer.mask + 1) * MAX_LOAD;
        if (hm->newer.size >= threshold) {
            hm_trigger_rehashing(hm);
        }
    }
    hm_help_rehashing(hm); // migrate some keys
}

HNode* hm_delete(HMap* hm, HNode* key, bool (*eq)(HNode*, HNode*)) {
    hm_help_rehashing(hm);
    HNode** from;
    ht_lookup(&hm->newer, key, eq);
    if ((from = ht_lookup(&hm->newer, key, eq))) {
        return ht_detach(&hm->newer, from);
    }
    if ((from = ht_lookup(&hm->older, key, eq))) {
        return ht_detach(&hm->older, from);
    }
    return NULL;
}

void hm_clear(HMap* hm) {
    free(hm->newer.tab);
    free(hm->older.tab);
    bzero(hm, sizeof(HMap));
}

size_t hm_size(const HMap* hm) { return hm->newer.size + hm->older.size; }

void hm_create_visitor(const HMap* hm, HTVisitor* vis) {
    if (!vis)
        return;

    vis->ht = &hm->newer;
    vis->chain_pos = NULL;
    vis->slot_pos = 0;
}

HNode* hm_visit_next(const HMap* hm, HTVisitor* vis) {
    HNode* res = ht_visit_next(vis);
    if (!res) {
        vis->ht = vis->ht == &hm->newer ? &hm->older : NULL;
        vis->chain_pos = NULL;
        vis->slot_pos = 0;
        res = ht_visit_next(vis);
    }
    return res;
}

void hm_foreach(const HMap* hm, bool (*f)(HNode*, void*), void* arg) {
    ht_foreach(&hm->newer, f, arg) && ht_foreach(&hm->older, f, arg);
}
