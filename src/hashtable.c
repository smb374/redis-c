#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "hashtable.h"
#include "qsbr.h"
#include "utils.h"

static __thread qsbr_tid hm_tid = -1;
static __thread uint32_t write_counter = 0;

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
CHTable *cht_init(CHTable *ht, size_t n) {
    assert(n > 0 && ((n - 1) & n) == 0);
    if (!ht) {
        ht = calloc(1, sizeof(CHTable));
        if (!ht) {
            perror("calloc()");
            exit(EXIT_FAILURE);
        }
        ht->is_alloc = true;
    } else {
        ht->is_alloc = false;
    }
    ht->tab = calloc(n, sizeof(HNode *));
    assert(ht->tab != NULL);
    atomic_store_explicit(&ht->size, 0, memory_order_release);
    atomic_store_explicit(&ht->mask, n - 1, memory_order_release);
    return ht;
}

void cht_destroy(CHTable *ht) {
    if (ht->tab)
        free(ht->tab);
    atomic_store_explicit(&ht->size, 0, memory_order_release);
    atomic_store_explicit(&ht->mask, 0, memory_order_release);
    if (ht->is_alloc) {
        free(ht);
    }
}

void cht_destroy_cb(void *arg) {
    CHTable *ht = arg;
    if (!ht)
        return;
    cht_destroy(ht);
}

bool cht_insert(CHTable *ht, HNode *node, atomic_size_t *gsize) {
    if (!node)
        return false;
    size_t mask = atomic_load_explicit(&ht->mask, memory_order_acquire);
    if (!mask)
        return false;
    const size_t pos = node->hcode & mask;
    HNode *next = ht->tab[pos];
    node->next = next;
    ht->tab[pos] = node;
    atomic_fetch_add_explicit(&ht->size, 1, memory_order_acq_rel);
    atomic_fetch_add_explicit(gsize, 1, memory_order_relaxed);
    return true;
}

HNode **cht_lookup(const CHTable *ht, HNode *key, bool (*eq)(HNode *, HNode *)) {
    if (!ht)
        return NULL;

    size_t mask = atomic_load_explicit(&ht->mask, memory_order_acquire);
    size_t size = atomic_load_explicit(&ht->size, memory_order_acquire);
    if (!mask || !size)
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

bool chm_help_rehashing(CHMap *hm) {
    // We don't need to manipulate hm->size here as the migration itself doesn't
    // change the number of entries in the table.

    // Create lists to distribute the current chain to BUCKET_LOCKS chain
    // such that we can insert the same chain in the list with 1 mutex lock
    // in current iteration.
    HNode *clist[BUCKET_LOCKS], *cur;
    bzero(clist, sizeof(HNode *) * BUCKET_LOCKS);
    size_t nwork = 0, cnt = 0, osize, omask, nmask;
    bool clean_old = false;
    CHTable *lnew = atomic_load_explicit(&hm->newer, memory_order_acquire);
    CHTable *lold;

    while (nwork < REHASH_WORK) {
        lold = atomic_load_explicit(&hm->older, memory_order_acquire);
        if (!lold)
            return false;
        omask = atomic_load_explicit(&lold->mask, memory_order_acquire);
        if (!omask)
            return false;
        // Read phase
        size_t bidx = atomic_fetch_add_explicit(&hm->migrate_pos, 1, memory_order_acq_rel);
        if (bidx > omask) {
            break;
        }
        // Bucket Migrate
        const size_t olidx = bidx % BUCKET_LOCKS;
        pthread_mutex_lock(&hm->ob_lock[olidx]);
        cur = lold->tab[bidx];
        lold->tab[bidx] = NULL;
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
        lnew = atomic_load_explicit(&hm->newer, memory_order_acquire);
        nmask = atomic_load_explicit(&lnew->mask, memory_order_acquire);
        cnt = 0;
        for (int i = 0; i < BUCKET_LOCKS && nwork > 0; i++) {
            if (!clist[i])
                continue;
            pthread_mutex_lock(&hm->nb_lock[i]);
            while (clist[i]) {
                const size_t pos = clist[i]->hcode & nmask;
                HNode *cnext = clist[i]->next;
                HNode *next = lnew->tab[pos];
                clist[i]->next = next;
                lnew->tab[pos] = clist[i];
                clist[i] = cnext;
                cnt++;
            }
            pthread_mutex_unlock(&hm->nb_lock[i]);
        }
        atomic_fetch_sub_explicit(&lold->size, cnt, memory_order_acq_rel);
        atomic_fetch_add_explicit(&lnew->size, cnt, memory_order_acq_rel);
    }

    bool marked = false;

    if (lold) {
        osize = atomic_load_explicit(&lold->size, memory_order_acquire);
        if (!osize) {
            CHTable *cht = atomic_exchange_explicit(&hm->older, NULL, memory_order_acq_rel);
            if (cht) {
                marked = true;
                qsbr_quiescent(&hm->gc, hm_tid);
                qsbr_alloc_cb(&hm->gc, cht_destroy_cb, cht);
            }
        }
    }

    return marked;
}

void chm_trigger_rehashing(CHMap *hm) {
    CHTable *pnew = atomic_load_explicit(&hm->newer, memory_order_acquire);
    size_t nmask = atomic_load_explicit(&pnew->mask, memory_order_acquire);
    CHTable *pold = atomic_load_explicit(&hm->older, memory_order_acquire);
    if (pold) {
        return;
    }
    while (!atomic_compare_exchange_strong_explicit(&hm->older, &pold, pnew, memory_order_acq_rel,
                                                    memory_order_acquire)) {
        if (pold) {
            return;
        }
    }
    CHTable *nnew = cht_init(NULL, (nmask + 1) << 1);
    atomic_store_explicit(&hm->newer, nnew, memory_order_release);
    atomic_store_explicit(&hm->migrate_pos, 0, memory_order_release);
}

bool cht_foreach(CHTable *ht, pthread_mutex_t *locks, bool (*f)(HNode *, void *), void *arg) {
    if (!ht)
        return false;

    size_t mask = atomic_load_explicit(&ht->mask, memory_order_acquire);
    for (int i = 0; i <= mask; i++) {
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
CHMap *chm_new(CHMap *hm) {
    if (!hm) {
        hm = calloc(1, sizeof(CHMap));
        hm->is_alloc = true;
    } else {
        hm->is_alloc = false;
    }

    CHTable *newer = cht_init(NULL, DEFAULT_TABLE_SIZE);

    atomic_init(&hm->size, 0);
    atomic_init(&hm->older, NULL);
    atomic_init(&hm->newer, newer);
    qsbr_init(&hm->gc, 4096);

    for (int i = 0; i < BUCKET_LOCKS; i++) {
        pthread_mutex_init(&hm->nb_lock[i], NULL);
        pthread_mutex_init(&hm->ob_lock[i], NULL);
    }

    return hm;
}

void chm_register(CHMap *hm) {
    if (hm_tid == -1) {
        hm_tid = qsbr_reg(&hm->gc);
        if (hm_tid == -1) {
            msg("Too many threads");
            exit(EXIT_FAILURE);
        }
    }
}

void chm_clear(CHMap *hm) {
    qsbr_destroy(&hm->gc);
    CHTable *lnew = atomic_exchange_explicit(&hm->newer, NULL, memory_order_acquire);
    CHTable *lold = atomic_exchange_explicit(&hm->older, NULL, memory_order_acquire);
    if (lnew) {
        cht_destroy(lnew);
    }
    if (lold) {
        cht_destroy(lold);
    }
    for (int i = 0; i < BUCKET_LOCKS; i++) {
        pthread_mutex_destroy(&hm->nb_lock[i]);
        pthread_mutex_destroy(&hm->ob_lock[i]);
    }
    bzero(hm, sizeof(HMap));
    if (hm->is_alloc) {
        free(hm);
    }
    hm_tid = -1;
}

HNode *chm_lookup(CHMap *hm, HNode *key, bool (*eq)(HNode *, HNode *)) {
    const size_t nlidx = key->hcode % BUCKET_LOCKS;
    const size_t olidx = key->hcode % BUCKET_LOCKS;
    CHTable *lnew = atomic_load_explicit(&hm->newer, memory_order_acquire);
    CHTable *lold = atomic_load_explicit(&hm->older, memory_order_acquire);
    HNode *result = NULL, **from;

    if (!lnew && !lold)
        return NULL;

    if (lnew) {
        pthread_mutex_lock(&hm->nb_lock[nlidx]);
        from = cht_lookup(lnew, key, eq);
        result = from ? *from : NULL;
        pthread_mutex_unlock(&hm->nb_lock[nlidx]);
    }

    if (!result && lold) {
        pthread_mutex_lock(&hm->ob_lock[olidx]);
        from = cht_lookup(lold, key, eq);
        result = from ? *from : NULL;
        pthread_mutex_unlock(&hm->ob_lock[olidx]);
    }

    chm_help_rehashing(hm);
    return result;
}

bool chm_insert(CHMap *hm, HNode *node, bool (*eq)(HNode *, HNode *)) {
    const size_t nlidx = node->hcode % BUCKET_LOCKS;
    const size_t olidx = node->hcode % BUCKET_LOCKS;
    CHTable *lnew = atomic_load_explicit(&hm->newer, memory_order_acquire);
    CHTable *lold = atomic_load_explicit(&hm->older, memory_order_acquire);
    HNode **from;
    bool found = false, result = false;

    if (!lnew && !lold)
        return false;

    if (lnew) {
        pthread_mutex_lock(&hm->nb_lock[nlidx]);
        from = cht_lookup(lnew, node, eq);
        found = from ? *from != NULL : false;
        pthread_mutex_unlock(&hm->nb_lock[nlidx]);
        if (!lold) {
            // check whether we need to rehash
            size_t nsize = atomic_load_explicit(&lnew->size, memory_order_acquire);
            size_t nmask = atomic_load_explicit(&lnew->mask, memory_order_acquire);
            const size_t threshold = (nmask + 1) * MAX_LOAD;
            if (nsize >= threshold) {
                chm_trigger_rehashing(hm);
            }
        } else if (!found) {
            pthread_mutex_lock(&hm->ob_lock[olidx]);
            from = cht_lookup(lold, node, eq);
            found = from ? *from != NULL : false;
            pthread_mutex_unlock(&hm->ob_lock[olidx]);
        }
    }

    // Attempt insert if we don't found matching key.
    if (!found) {
        // Reload newer as trigger rehash may replace newer.
        lnew = atomic_load_explicit(&hm->newer, memory_order_acquire);
        pthread_mutex_lock(&hm->nb_lock[nlidx]);
        cht_insert(lnew, node, &hm->size);
        pthread_mutex_unlock(&hm->nb_lock[nlidx]);
        result = true;
    }

    bool marked = chm_help_rehashing(hm); // migrate some keys
    write_counter = (write_counter + 1) % 1000;
    if (!marked && !write_counter)
        qsbr_quiescent(&hm->gc, hm_tid);
    return result;
}

void chm_insert_unchecked(CHMap *hm, HNode *node) {
    const size_t nlidx = node->hcode % BUCKET_LOCKS;
    CHTable *lnew = atomic_load_explicit(&hm->newer, memory_order_acquire);
    CHTable *lold = atomic_load_explicit(&hm->older, memory_order_acquire);
    if (!lnew && !lold)
        return;

    if (lnew) {
        pthread_mutex_lock(&hm->nb_lock[nlidx]);
        cht_insert(lnew, node, &hm->size);
        pthread_mutex_unlock(&hm->nb_lock[nlidx]);
        if (!lold) {
            // check whether we need to rehash
            size_t nsize = atomic_load_explicit(&lnew->size, memory_order_acquire);
            size_t nmask = atomic_load_explicit(&lnew->mask, memory_order_acquire);
            const size_t threshold = (nmask + 1) * MAX_LOAD;
            if (nsize >= threshold) {
                chm_trigger_rehashing(hm);
            }
        }
    }

    bool marked = chm_help_rehashing(hm); // migrate some keys
    write_counter = (write_counter + 1) % 1000;
    if (!marked && !write_counter)
        qsbr_quiescent(&hm->gc, hm_tid);
}

HNode *chm_delete(CHMap *hm, HNode *key, bool (*eq)(HNode *, HNode *)) {
    const size_t nlidx = key->hcode % BUCKET_LOCKS;
    const size_t olidx = key->hcode % BUCKET_LOCKS;
    CHTable *lnew = atomic_load_explicit(&hm->newer, memory_order_acquire);
    CHTable *lold = atomic_load_explicit(&hm->older, memory_order_acquire);
    HNode *result = NULL, **from;

    if (!lnew && !lold)
        return NULL;

    if (lnew) {
        pthread_mutex_lock(&hm->nb_lock[nlidx]);
        from = cht_lookup(lnew, key, eq);
        result = from ? cht_detach(lnew, from, &hm->size) : NULL;
        pthread_mutex_unlock(&hm->nb_lock[nlidx]);
    }

    if (!result && lold) {
        pthread_mutex_lock(&hm->ob_lock[olidx]);
        from = cht_lookup(lold, key, eq);
        result = from ? cht_detach(lold, from, &hm->size) : NULL;
        pthread_mutex_unlock(&hm->ob_lock[olidx]);
    }

    bool marked = chm_help_rehashing(hm); // migrate some keys
    write_counter = (write_counter + 1) % 1000;
    if (!marked && !write_counter)
        qsbr_quiescent(&hm->gc, hm_tid);
    return result;
}

size_t chm_size(CHMap *hm) { return atomic_load_explicit(&hm->size, memory_order_relaxed); }

void chm_foreach(CHMap *hm, bool (*f)(HNode *, void *), void *arg) {
    CHTable *lnew = atomic_load_explicit(&hm->newer, memory_order_acquire);
    if (cht_foreach(lnew, hm->nb_lock, f, arg)) {
        CHTable *lold = atomic_load_explicit(&hm->older, memory_order_acquire);
        cht_foreach(lold, hm->ob_lock, f, arg);
    }
}
