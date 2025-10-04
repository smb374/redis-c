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

HNode *hm_insert(HMap *hm, HNode *node, bool (*eq)(HNode *, HNode *)) {
    if (!hm->newer.tab) {
        ht_init(&hm->newer, DEFAULT_TABLE_SIZE); // initialize it if empty
    }
    HNode *result = NULL, **from;
    if ((from = ht_lookup(&hm->newer, node, eq))) {
        result = ht_detach(&hm->newer, from);
        if (result) {
            ht_insert(&hm->newer, node);
        }
    }
    if ((from = ht_lookup(&hm->older, node, eq))) {
        result = ht_detach(&hm->older, from);
        if (result) {
            ht_insert(&hm->older, node);
        }
    }
    if (!result) {
        ht_insert(&hm->newer, node); // always insert to the newer table
    }

    if (!hm->older.tab) {
        // check whether we need to rehash
        const size_t threshold = (hm->newer.mask + 1) * MAX_LOAD;
        if (hm->newer.size >= threshold) {
            hm_trigger_rehashing(hm);
        }
    }
    hm_help_rehashing(hm); // migrate some keys
    return result;
}

void hm_insert_unchecked(HMap *hm, HNode *node) {
    if (!hm->newer.tab) {
        ht_init(&hm->newer, DEFAULT_TABLE_SIZE); // initialize it if empty
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
    atomic_fetch_add_explicit(gsize, 1, memory_order_relaxed);
}

HNode **cht_lookup(const CHTable *ht, HNode *key, bool (*eq)(HNode *, HNode *)) {
    if (!ht->tab || !ht->mask || !ht->size)
        return NULL;

    const size_t pos = key->hcode & ht->mask;
    HNode **from = &ht->tab[pos]; // incoming pointer to the target
    if (!from)
        return NULL;
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
    atomic_fetch_sub_explicit(gsize, 1, memory_order_relaxed);
    return node;
}

void chm_help_rehashing(CHMap *hm) {
    // We don't need to manipulate hm->size here as the migration itself doesn't
    // change the number of entries in the table.

    // Create lists to distribute the current chain to BUCKET_LOCKS chain
    // such that we can insert the same chain in the list with 1 mutex lock
    // in current iteration.
    HNode *clist[BUCKET_LOCKS], *cur;
    bzero(clist, sizeof(HNode *) * BUCKET_LOCKS);
    size_t nwork = 0, cnt = 0, older_size;
    bool clean_old = false;

    while (nwork < REHASH_WORK) {
        // Read phase
        pthread_rwlock_rdlock(&hm->st_lock);
        if (!hm->older.tab) {
            pthread_rwlock_unlock(&hm->st_lock);
            return;
        }
        size_t bidx = atomic_fetch_add_explicit(&hm->migrate_pos, 1, memory_order_acq_rel);
        if (bidx > hm->older.mask) {
            pthread_rwlock_unlock(&hm->st_lock);
            break;
        }
        pthread_rwlock_unlock(&hm->st_lock);
        // Bucket Migrate
        const size_t olidx = bidx % BUCKET_LOCKS;
        pthread_mutex_lock(&hm->ob_lock[olidx]);
        cur = hm->older.tab[bidx];
        hm->older.tab[bidx] = NULL;
        pthread_mutex_unlock(&hm->ob_lock[olidx]);
        if (!cur) {
            continue;
        }
        cnt = 0;
        while (cur) {
            HNode *next = cur->next;
            const size_t nlidx = cur->hcode % BUCKET_LOCKS;
            cur->next = clist[nlidx];
            clist[nlidx] = cur;
            cur = next;

            cnt++;
        }
        nwork += cnt;
        // Insert to newer
        cnt = 0;
        for (int i = 0; i < BUCKET_LOCKS && nwork > 0; i++) {
            if (!clist[i])
                continue;
            pthread_mutex_lock(&hm->nb_lock[i]);
            while (clist[i]) {
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
        atomic_fetch_sub_explicit(&hm->older.size, cnt, memory_order_acq_rel);
        atomic_fetch_add_explicit(&hm->newer.size, cnt, memory_order_acq_rel);
    }

    older_size = atomic_load_explicit(&hm->older.size, memory_order_acquire);
    if (!older_size) {
        pthread_rwlock_wrlock(&hm->st_lock);
        older_size = atomic_load_explicit(&hm->older.size, memory_order_acquire);
        if (!older_size && hm->older.tab) {
            free(hm->older.tab);
            hm->older.tab = NULL;
            hm->older.mask = 0;
        }
        pthread_rwlock_unlock(&hm->st_lock);
    }
}

void chm_trigger_rehashing(CHMap *hm) {
    hm->older = hm->newer;
    cht_init(&hm->newer, (hm->newer.mask + 1) << 1);
    atomic_store_explicit(&hm->migrate_pos, 0, memory_order_release);
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
    const size_t nlidx = key->hcode % BUCKET_LOCKS;
    const size_t olidx = key->hcode % BUCKET_LOCKS;
    HNode *result = NULL, **from;
    pthread_rwlock_rdlock(&hm->st_lock);
    if (!hm->newer.tab && !hm->older.tab) {
        pthread_rwlock_unlock(&hm->st_lock);
        return NULL;
    }

    if (hm->newer.tab) {
        pthread_mutex_lock(&hm->nb_lock[nlidx]);
        from = cht_lookup(&hm->newer, key, eq);
        result = from ? *from : NULL;
        pthread_mutex_unlock(&hm->nb_lock[nlidx]);
    }

    if (!result && hm->older.tab) {
        pthread_mutex_lock(&hm->ob_lock[olidx]);
        from = cht_lookup(&hm->older, key, eq);
        result = from ? *from : NULL;
        pthread_mutex_unlock(&hm->ob_lock[olidx]);
    }
    pthread_rwlock_unlock(&hm->st_lock);

    chm_help_rehashing(hm);
    return result;
}

bool chm_insert(CHMap *hm, HNode *node, bool (*eq)(HNode *, HNode *)) {
    const size_t nlidx = node->hcode % BUCKET_LOCKS;
    const size_t olidx = node->hcode % BUCKET_LOCKS;
    HNode **from;
    bool need_rehash = false, found = false, result = false;

    pthread_rwlock_rdlock(&hm->st_lock);
    if (!hm->newer.tab && !hm->older.tab) {
        pthread_rwlock_unlock(&hm->st_lock);
        return NULL;
    }

    if (hm->newer.tab) {
        pthread_mutex_lock(&hm->nb_lock[nlidx]);
        from = cht_lookup(&hm->newer, node, eq);
        found = from ? *from : false;
        pthread_mutex_unlock(&hm->nb_lock[nlidx]);
    }

    if (!found && hm->older.tab) {
        pthread_mutex_lock(&hm->ob_lock[olidx]);
        from = cht_lookup(&hm->older, node, eq);
        found = from ? *from : false;
        pthread_mutex_unlock(&hm->ob_lock[olidx]);
    }

    if (!hm->older.tab) {
        // check whether we need to rehash again
        size_t new_size = atomic_load_explicit(&hm->newer.size, memory_order_acquire);
        const size_t threshold = (hm->newer.mask + 1) * MAX_LOAD;
        if (new_size >= threshold) {
            need_rehash = true;
        }
    }
    pthread_rwlock_unlock(&hm->st_lock);

    if (need_rehash) {
        pthread_rwlock_wrlock(&hm->st_lock);
        if (!hm->older.tab) {
            // check whether we need to rehash again
            size_t new_size = atomic_load_explicit(&hm->newer.size, memory_order_acquire);
            const size_t threshold = (hm->newer.mask + 1) * MAX_LOAD;
            if (new_size >= threshold) {
                chm_trigger_rehashing(hm);
            }
        }
        pthread_rwlock_unlock(&hm->st_lock);
    }

    if (!found) {
        pthread_mutex_lock(&hm->nb_lock[nlidx]);
        cht_insert(&hm->newer, node, &hm->size);
        pthread_mutex_unlock(&hm->nb_lock[nlidx]);
        result = true;
    } else {
        result = false;
    }

    chm_help_rehashing(hm); // migrate some keys
    return result;
}

void chm_insert_unchecked(CHMap *hm, HNode *node) {
    const size_t nlidx = node->hcode % BUCKET_LOCKS;
    bool need_rehash = false;
    pthread_rwlock_rdlock(&hm->st_lock);
    if (!hm->newer.tab) {
        pthread_rwlock_unlock(&hm->st_lock);
        return;
    }
    pthread_mutex_lock(&hm->nb_lock[nlidx]);
    cht_insert(&hm->newer, node, &hm->size);
    pthread_mutex_unlock(&hm->nb_lock[nlidx]);

    if (!hm->older.tab) {
        // check whether we need to rehash
        size_t new_size = atomic_load_explicit(&hm->newer.size, memory_order_acquire);
        const size_t threshold = (hm->newer.mask + 1) * MAX_LOAD;
        if (new_size >= threshold) {
            need_rehash = true;
        }
    }
    pthread_rwlock_unlock(&hm->st_lock);

    if (need_rehash) {
        pthread_rwlock_wrlock(&hm->st_lock);
        if (!hm->older.tab) {
            // check whether we need to rehash
            size_t new_size = atomic_load_explicit(&hm->newer.size, memory_order_acquire);
            const size_t threshold = (hm->newer.mask + 1) * MAX_LOAD;
            if (new_size >= threshold) {
                chm_trigger_rehashing(hm);
            }
        }
        pthread_rwlock_unlock(&hm->st_lock);
    }

    chm_help_rehashing(hm); // migrate some keys
}

HNode *chm_delete(CHMap *hm, HNode *key, bool (*eq)(HNode *, HNode *)) {
    const size_t nlidx = key->hcode % BUCKET_LOCKS;
    const size_t olidx = key->hcode % BUCKET_LOCKS;
    HNode *result = NULL, **from;
    pthread_rwlock_rdlock(&hm->st_lock);
    if (!hm->newer.tab && !hm->older.tab) {
        pthread_rwlock_unlock(&hm->st_lock);
        return NULL;
    }

    if (hm->newer.tab) {
        pthread_mutex_lock(&hm->nb_lock[nlidx]);
        from = cht_lookup(&hm->newer, key, eq);
        result = from ? cht_detach(&hm->newer, from, &hm->size) : NULL;
        pthread_mutex_unlock(&hm->nb_lock[nlidx]);
    }

    if (!result && hm->older.tab) {
        pthread_mutex_lock(&hm->ob_lock[olidx]);
        from = cht_lookup(&hm->older, key, eq);
        result = from ? cht_detach(&hm->older, from, &hm->size) : NULL;
        pthread_mutex_unlock(&hm->ob_lock[olidx]);
    }
    pthread_rwlock_unlock(&hm->st_lock);

    chm_help_rehashing(hm);
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
    cht_init(&hm->newer, DEFAULT_TABLE_SIZE);

    pthread_rwlock_init(&hm->st_lock, NULL);
    for (int i = 0; i < BUCKET_LOCKS; i++) {
        pthread_mutex_init(&hm->nb_lock[i], NULL);
        pthread_mutex_init(&hm->ob_lock[i], NULL);
    }

    return hm;
}
void chm_clear(CHMap *hm) {
    free(hm->newer.tab);
    free(hm->older.tab);
    pthread_rwlock_destroy(&hm->st_lock);
    for (int i = 0; i < BUCKET_LOCKS; i++) {
        pthread_mutex_destroy(&hm->nb_lock[i]);
        pthread_mutex_destroy(&hm->ob_lock[i]);
    }
    bzero(hm, sizeof(HMap));
}

size_t chm_size(CHMap *hm) { return atomic_load_explicit(&hm->size, memory_order_relaxed); }

void chm_foreach(CHMap *hm, bool (*f)(HNode *, void *), void *arg) {
    pthread_rwlock_rdlock(&hm->st_lock);
    cht_foreach(&hm->newer, hm->nb_lock, f, arg) && cht_foreach(&hm->older, hm->ob_lock, f, arg);
    pthread_rwlock_unlock(&hm->st_lock);
}
