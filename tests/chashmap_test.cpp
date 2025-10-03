// tests/chashmap_test.cpp
#include <atomic>
#include <cstdio>
#include <gtest/gtest.h>
#include <iostream>
#include <thread>
#include <vector>

extern "C" {
#include "hashtable.h"
#include "utils.h"
}

// --- Test Setup ---

struct CTestEntry {
    HNode node;
    uint64_t key;
    uint64_t value;
    // In a real scenario, this would also contain the pthread_rwlock_t
    // and the atomic is_deleted flag.
};

static bool test_entry_eq(HNode *lhs, HNode *rhs) {
    const CTestEntry *le = container_of(lhs, CTestEntry, node);
    const CTestEntry *re = container_of(rhs, CTestEntry, node);
    return le->key == re->key;
}

// --- Test Fixture ---
static std::vector<CTestEntry *> entries;

bool catcher(HNode *node, void *arg) {
    entries.push_back(container_of(node, CTestEntry, node));
    return true;
}

class CHashMapTest : public ::testing::Test {
protected:
    CHMap *chm;

    void SetUp() override {
        // The CHMap is not zero-initialized. It needs cht_init.
        // We will do this inside the tests themselves, as some tests
        // might want different initial sizes.
        chm = chm_new(NULL);
        entries.clear();
    }


    void TearDown() override {
        // In a real concurrent scenario with deferred reclamation,
        // this cleanup would be more complex (e.g., processing a garbage list).
        // For this test, we assume all threads have finished and we can safely clean up.
        chm_foreach(chm, catcher, nullptr);
        for (auto entry: entries)
            delete entry;
        chm_clear(chm);
        free(chm);
    }
};

// --- Test Cases ---

TEST_F(CHashMapTest, SingleThreadedInsertAndLookup) {
    auto *entry1 = new CTestEntry();
    entry1->key = 100;
    entry1->value = 1000;
    entry1->node.hcode = int_hash_rapid(100);
    chm_insert(chm, &entry1->node);

    auto *entry2 = new CTestEntry();
    entry2->key = 200;
    entry2->value = 2000;
    entry2->node.hcode = int_hash_rapid(200);
    chm_insert(chm, &entry2->node);

    ASSERT_EQ(chm_size(chm), 2);

    CTestEntry key_entry = {};
    key_entry.key = 100;
    key_entry.node.hcode = int_hash_rapid(100);

    HNode *found = chm_lookup(chm, &key_entry.node, &test_entry_eq);
    ASSERT_NE(found, nullptr);

    const CTestEntry *found_entry = container_of(found, CTestEntry, node);
    EXPECT_EQ(found_entry->key, 100);
    EXPECT_EQ(found_entry->value, 1000);
}

TEST_F(CHashMapTest, SingleThreadedDelete) {
    auto *entry1 = new CTestEntry();
    entry1->key = 100;
    entry1->node.hcode = int_hash_rapid(100);
    chm_insert(chm, &entry1->node);

    ASSERT_EQ(chm_size(chm), 1);

    CTestEntry key_entry = {};
    key_entry.key = 100;
    key_entry.node.hcode = int_hash_rapid(100);

    HNode *deleted_node = chm_delete(chm, &key_entry.node, &test_entry_eq);
    ASSERT_NE(deleted_node, nullptr);
    ASSERT_EQ(chm_size(chm), 0);

    delete container_of(deleted_node, CTestEntry, node);

    HNode *found = chm_lookup(chm, &key_entry.node, &test_entry_eq);
    ASSERT_EQ(found, nullptr);
}

TEST_F(CHashMapTest, MultiThreadedConcurrentInsert) {
    const int num_threads = 8;
    const int items_per_thread = 1000;
    std::vector<std::thread> threads;

    // Using a vector of vectors to store nodes each thread creates
    // This avoids data races on a shared vector and simplifies cleanup.
    std::vector<std::vector<CTestEntry *>> thread_nodes(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, i, items_per_thread, &thread_nodes]() {
            for (int j = 0; j < items_per_thread; ++j) {
                uint64_t key = i * items_per_thread + j;
                auto *entry = new CTestEntry();
                entry->key = key;
                entry->value = key * 10;
                entry->node.hcode = int_hash_rapid(key);
                chm_insert(chm, &entry->node);
                thread_nodes[i].push_back(entry);
            }
        });
    }

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    const int total_items = num_threads * items_per_thread;
    ASSERT_EQ(chm_size(chm), total_items);

    // Verify all items are findable
    for (int i = 0; i < total_items; ++i) {
        // CTestEntry key_entry = {{int_hash_rapid((uint64_t) i)}, (uint64_t) i, 0};
        CTestEntry key_entry = {};
        key_entry.key = i;
        key_entry.node.hcode = int_hash_rapid(i);
        HNode *found = chm_lookup(chm, &key_entry.node, &test_entry_eq);
        ASSERT_NE(found, nullptr) << "Failed to find key: " << i;
        EXPECT_EQ(container_of(found, CTestEntry, node)->value, (uint64_t) i * 10);
    }
}

TEST_F(CHashMapTest, MultiThreadedConcurrentInsertDelete) {
    const int num_threads = 8;
    const int items_per_thread = 20000;
    std::vector<std::thread> threads;
    std::atomic<int> success_deletes(0);

    // 1. Pre-fill the map
    std::vector<CTestEntry *> all_nodes;
    for (int i = 0; i < num_threads * items_per_thread; ++i) {
        auto *entry = new CTestEntry();
        entry->key = i;
        entry->value = i;
        entry->node.hcode = int_hash_rapid(i);
        chm_insert(chm, &entry->node);
        all_nodes.push_back(entry);
    }

    // 2. Start threads to concurrently delete and insert
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, i, items_per_thread, &success_deletes]() {
            // Each thread operates on its own slice of keys
            for (int j = 0; j < items_per_thread; ++j) {
                uint64_t key = i * items_per_thread + j;
                CTestEntry key_entry = {};
                key_entry.key = key;
                key_entry.node.hcode = int_hash_rapid(key);

                // Half the threads delete, half insert new items
                if (i % 2 == 0) { // Deleter threads
                    HNode *deleted = chm_delete(chm, &key_entry.node, &test_entry_eq);
                    if (deleted) {
                        CTestEntry *ent = container_of(deleted, CTestEntry, node);
                        ASSERT_EQ(ent->key, ent->value);
                        success_deletes++;
                        // In a real system, we'd put this in a garbage list.
                        // For the test, we just leak it and let TearDown handle it.
                        delete container_of(deleted, CTestEntry, node);
                    }
                } else { // Inserter threads
                    uint64_t new_key = key + (num_threads * items_per_thread);
                    auto *entry = new CTestEntry();
                    entry->key = new_key;
                    entry->value = new_key;
                    entry->node.hcode = int_hash_rapid(new_key);
                    chm_insert(chm, &entry->node);
                }
            }
        });
    }

    for (auto &t: threads) {
        t.join();
    }

    // 3. Verify final state
    int expected_deletes = (num_threads / 2) * items_per_thread;
    int expected_inserts = (num_threads - (num_threads / 2)) * items_per_thread;

    ASSERT_EQ(success_deletes.load(), expected_deletes);
    ASSERT_EQ(chm_size(chm), (num_threads * items_per_thread) - expected_deletes + expected_inserts);
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
