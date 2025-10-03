#include "hashtable.h"
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

// Internal Functions
void ht_init(HTable *ht, size_t n) {
    assert(n > 0 && ((n - 1) & n) == 0);
    ht->tab = calloc(n, sizeof(HNode *));
    assert(ht->tab != NULL);
    ht->mask = n - 1;
    ht->size = 0;
}

void ht_insert(HTable *ht, HNode *node) {
    const size_t pos = node->hcode & ht->mask;
    HNode *next = ht->tab[pos];
    node->next = next;
    ht->tab[pos] = node;
    ht->size++;
}

HNode **ht_lookup(const HTable *ht, HNode *key, bool (*eq)(HNode *, HNode *)) {
    if (!ht->tab)
        return NULL;

    const size_t pos = key->hcode & ht->mask;
    HNode **from = &ht->tab[pos]; // incoming pointer to the target
    for (HNode *cur; (cur = *from) != NULL; from = &cur->next) {
        if (cur->hcode == key->hcode && eq(cur, key))
            return from; // may be a node, may be a slot
    }
    return NULL;
}

HNode *ht_detach(HTable *ht, HNode **from) {
    HNode *node = *from; // the target node
    *from = node->next; // update the incoming pointer to the target
    ht->size--;
    return node;
}

void hm_help_rehashing(HMap *hm) {
    size_t nwork = 0;
    while (nwork < REHASH_WORK && hm->older.size > 0) {
        // find a non-empty slot
        HNode **from = &hm->older.tab[hm->migrate_pos];
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

void hm_trigger_rehashing(HMap *hm) {
    assert(hm->older.tab == NULL);
    // (newer, older) <- (new_table, newer)
    hm->older = hm->newer;
    ht_init(&hm->newer, (hm->newer.mask + 1) * 2);
    hm->migrate_pos = 0;
}

bool ht_foreach(const HTable *ht, bool (*f)(HNode *, void *), void *arg) {
    for (size_t i = 0; ht->mask && i <= ht->mask; i++) {
        for (HNode *node = ht->tab[i]; node; node = node->next) {
            if (!f(node, arg)) {
                return false;
            }
        }
    }
    return true;
}

// Public Interface
HNode *hm_lookup(HMap *hm, HNode *key, bool (*eq)(HNode *, HNode *)) {
    hm_help_rehashing(hm);
    HNode **from = ht_lookup(&hm->newer, key, eq);
    if (!from) {
        from = ht_lookup(&hm->older, key, eq);
    }
    return from ? *from : NULL;
}

void hm_insert(HMap *hm, HNode *node) {
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

HNode *hm_delete(HMap *hm, HNode *key, bool (*eq)(HNode *, HNode *)) {
    hm_help_rehashing(hm);
    HNode **from;
    if ((from = ht_lookup(&hm->newer, key, eq))) {
        return ht_detach(&hm->newer, from);
    }
    if ((from = ht_lookup(&hm->older, key, eq))) {
        return ht_detach(&hm->older, from);
    }
    return NULL;
}

void hm_clear(HMap *hm) {
    free(hm->newer.tab);
    free(hm->older.tab);
    bzero(hm, sizeof(HMap));
}

size_t hm_size(const HMap *hm) { return hm->newer.size + hm->older.size; }

void hm_foreach(const HMap *hm, bool (*f)(HNode *, void *), void *arg) {
    ht_foreach(&hm->newer, f, arg) && ht_foreach(&hm->older, f, arg);
}

// Concurrent Internal Functions
void cht_init(CHTable *ht, size_t n) {
    assert(n > 0 && ((n - 1) & n) == 0);
    ht->tab = calloc(n, sizeof(HNode *));
    assert(ht->tab != NULL);
    ht->mask = n - 1;
    ht->size = ATOMIC_VAR_INIT(0);
}

void cht_insert(CHTable *ht, HNode *node, atomic_size_t *gsize) {
    if (!node)
        return;
    const size_t pos = node->hcode & ht->mask;
    HNode *next = ht->tab[pos];
    node->next = next;
    ht->tab[pos] = node;
    atomic_fetch_add_explicit(&ht->size, 1, memory_order_acq_rel);
    atomic_fetch_add_explicit(gsize, 1, memory_order_acq_rel);
}

HNode **cht_lookup(const CHTable *ht, HNode *key, bool (*eq)(HNode *, HNode *)) {
    if (!ht->tab)
        return NULL;

    const size_t pos = key->hcode & ht->mask;
    HNode **from = &ht->tab[pos]; // incoming pointer to the target
    for (HNode *cur; (cur = *from) != NULL; from = &cur->next) {
        if (cur->hcode == key->hcode && eq(cur, key))
            return from; // may be a node, may be a slot
    }
    return NULL;
}

HNode *cht_detach(CHTable *ht, HNode **from, atomic_size_t *gsize) {
    HNode *node = *from; // the target node
    *from = node->next; // update the incoming pointer to the target
    atomic_fetch_sub_explicit(&ht->size, 1, memory_order_acq_rel);
    atomic_fetch_sub_explicit(gsize, 1, memory_order_acq_rel);
    return node;
}

void chm_help_rehashing(CHMap *hm) {
    pthread_mutex_lock(&hm->st_lock);
    // Create lists to distribute the current chain to BUCKET_LOCKS chain
    // such that we can insert the same chain in the list with 1 mutex lock
    // in current iteration.
    HNode hlist[BUCKET_LOCKS], *clist[BUCKET_LOCKS], *cur;
    for (int i = 0; i < BUCKET_LOCKS; i++) {
        clist[i] = &hlist[i];
    }
    size_t nwork = 0;
    size_t older_size = atomic_load_explicit(&hm->older.size, memory_order_acquire);

    while (nwork < REHASH_WORK && older_size > 0) {
        size_t bidx = hm->migrate_pos;

        if (bidx > hm->older.mask) {
            break;
        }
        const size_t olidx = bidx % BUCKET_LOCKS;
        pthread_mutex_lock(&hm->ob_lock[olidx]);
        cur = hm->older.tab[bidx];
        hm->older.tab[bidx] = NULL;
        pthread_mutex_unlock(&hm->ob_lock[olidx]);
        if (!cur) {
            hm->migrate_pos++;
            older_size = atomic_load_explicit(&hm->older.size, memory_order_acquire);
            continue;
        }
        size_t cnt = 0;
        while (cur) {
            HNode *next = cur->next;
            const size_t nlidx = cur->hcode % BUCKET_LOCKS;
            cur->next = clist[nlidx];
            clist[nlidx] = cur;
            cur = next;

            cnt++;
            nwork++;
        }
        atomic_fetch_sub_explicit(&hm->older.size, cnt, memory_order_acq_rel);
        cnt = 0;
        for (int i = 0; i < BUCKET_LOCKS; i++) {
            if (clist[i] == &hlist[i])
                continue;
            pthread_mutex_lock(&hm->nb_lock[i]);
            while (clist[i] != &hlist[i]) {
                const size_t pos = clist[i]->hcode & hm->newer.mask;
                HNode *cnext = clist[i]->next;
                HNode *next = hm->newer.tab[pos];
                clist[i]->next = next;
                hm->newer.tab[pos] = clist[i];
                clist[i] = cnext;
                cnt++;
            }
            pthread_mutex_unlock(&hm->nb_lock[i]);
        }
        atomic_fetch_add_explicit(&hm->newer.size, cnt, memory_order_acq_rel);
        hm->migrate_pos++;
        older_size = atomic_load_explicit(&hm->older.size, memory_order_acquire);
    }

    if (older_size == 0 && hm->older.tab) {
        if (hm->older.tab) {
            free(hm->older.tab);
            bzero(&hm->older, sizeof(CHTable));
        }
    }
    pthread_mutex_unlock(&hm->st_lock);
}

void chm_trigger_rehashing(CHMap *hm) {
    pthread_mutex_lock(&hm->st_lock);
    if (hm->older.tab) {
        pthread_mutex_unlock(&hm->st_lock);
        return;
    }

    hm->older = hm->newer;
    cht_init(&hm->newer, (hm->newer.mask + 1) << 1);
    hm->migrate_pos = 0;
    pthread_mutex_unlock(&hm->st_lock);
}

bool cht_foreach(CHTable *ht, pthread_mutex_t *locks, bool (*f)(HNode *, void *), void *arg) {
    if (!ht->tab)
        return false;

    for (int i = 0; i <= ht->mask; i++) {
        pthread_mutex_lock(&locks[i % BUCKET_LOCKS]);
        for (HNode *node = ht->tab[i]; node; node = node->next) {
            if (!f(node, arg)) {
                pthread_mutex_unlock(&locks[i % BUCKET_LOCKS]);
                return false;
            }
        }
        pthread_mutex_unlock(&locks[i % BUCKET_LOCKS]);
    }
    return true;
}

// Concurrent Public Interface
// Public Interface
HNode *chm_lookup(CHMap *hm, HNode *key, bool (*eq)(HNode *, HNode *)) {
    chm_help_rehashing(hm);

    const size_t nlidx = key->hcode % BUCKET_LOCKS;
    const size_t olidx = key->hcode % BUCKET_LOCKS;
    HNode *result = NULL;
    pthread_mutex_lock(&hm->ob_lock[olidx]);
    pthread_mutex_lock(&hm->nb_lock[nlidx]);

    HNode **from = cht_lookup(&hm->newer, key, eq);
    if (from) {
        result = *from;
    } else if (hm->older.tab) {
        from = cht_lookup(&hm->older, key, eq);
        result = from ? *from : NULL;
    }

    pthread_mutex_unlock(&hm->nb_lock[nlidx]);
    pthread_mutex_unlock(&hm->ob_lock[olidx]);

    return result;
}

void chm_insert(CHMap *hm, HNode *node) {
    pthread_mutex_lock(&hm->st_lock);
    if (!hm->newer.tab) {
        cht_init(&hm->newer, 16); // initialize it if empty
    }

    bool to_rehash = false;
    if (!hm->older.tab) {
        // check whether we need to rehash
        size_t new_size = atomic_load_explicit(&hm->newer.size, memory_order_acquire);
        const size_t threshold = (hm->newer.mask + 1) * MAX_LOAD;
        if (new_size >= threshold) {
            to_rehash = true;
        }
    }
    pthread_mutex_unlock(&hm->st_lock);

    const size_t nlidx = node->hcode % BUCKET_LOCKS;
    pthread_mutex_lock(&hm->nb_lock[nlidx]);
    cht_insert(&hm->newer, node, &hm->size); // always insert to the newer table
    pthread_mutex_unlock(&hm->nb_lock[nlidx]);

    if (to_rehash)
        chm_trigger_rehashing(hm);

    chm_help_rehashing(hm); // migrate some keys
}

HNode *chm_delete(CHMap *hm, HNode *key, bool (*eq)(HNode *, HNode *)) {
    chm_help_rehashing(hm);

    const size_t nlidx = key->hcode % BUCKET_LOCKS;
    const size_t olidx = key->hcode % BUCKET_LOCKS;
    HNode *result = NULL;
    pthread_mutex_lock(&hm->ob_lock[olidx]);
    pthread_mutex_lock(&hm->nb_lock[nlidx]);

    HNode **from = cht_lookup(&hm->newer, key, eq);
    if (from) {
        result = cht_detach(&hm->newer, from, &hm->size);
    } else if (hm->older.tab) {
        from = cht_lookup(&hm->older, key, eq);
        if (from) {
            result = cht_detach(&hm->older, from, &hm->size);
        }
    }

    pthread_mutex_unlock(&hm->nb_lock[nlidx]);
    pthread_mutex_unlock(&hm->ob_lock[olidx]);

    return result;
}

CHMap *chm_new(CHMap *hm) {
    if (!hm) {
        hm = calloc(1, sizeof(CHMap));
        hm->is_alloc = true;
    } else {
        hm->is_alloc = false;
    }

    hm->size = ATOMIC_VAR_INIT(0);

    pthread_mutex_init(&hm->st_lock, NULL);
    for (int i = 0; i < BUCKET_LOCKS; i++) {
        pthread_mutex_init(&hm->nb_lock[i], NULL);
        pthread_mutex_init(&hm->ob_lock[i], NULL);
    }

    return hm;
}
void chm_clear(CHMap *hm) {
    free(hm->newer.tab);
    free(hm->older.tab);
    pthread_mutex_destroy(&hm->st_lock);
    for (int i = 0; i < BUCKET_LOCKS; i++) {
        pthread_mutex_destroy(&hm->nb_lock[i]);
        pthread_mutex_destroy(&hm->ob_lock[i]);
    }
    bzero(hm, sizeof(HMap));
}

size_t chm_size(CHMap *hm) { return atomic_load_explicit(&hm->size, memory_order_acquire); }

void chm_foreach(CHMap *hm, bool (*f)(HNode *, void *), void *arg) {
    cht_foreach(&hm->newer, hm->nb_lock, f, arg) && cht_foreach(&hm->older, hm->ob_lock, f, arg);
}
