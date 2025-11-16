#include <gtest/gtest.h>
#include <random>
#include <thread>
#include <vector>

#include "leapfrog.h"
#include "qsbr.h"
#include "utils.h"


// --- Test Entry Structure ---
struct TestEntry {
    LFNode node;
    uint64_t key;
    uint64_t value;
};

CLFMap *cmap = nullptr;
// Equality function for the test entries
static bool test_entry_eq(LFNode *lhs, LFNode *rhs) {
    if (!lhs || !rhs)
        return lhs == rhs;
    const auto *le = container_of(lhs, TestEntry, node);
    const auto *re = container_of(rhs, TestEntry, node);
    return le->key == re->key;
}

// --- Test Fixture ---
class CLFMapTest : public ::testing::Test {
protected:
    // Start with a small table to ensure migration is triggered and tested.
    static const size_t MAP_SIZE = 1024;
    static const size_t NUM_THREADS = 8;

    void SetUp() override {
        qsbr_init(65536);
        qsbr_reg();
        cmap = clfm_new(nullptr, MAP_SIZE);
        ASSERT_NE(cmap, nullptr);
    }

    void TearDown() override {
        clfm_destroy(cmap);
        qsbr_unreg();
        qsbr_destroy();
    }
};

TEST_F(CLFMapTest, SingleThreadUpsertRemove) {
    auto *entry1 = new TestEntry{{int_hash_rapid(100)}, 100, 1000};
    auto *entry2 = new TestEntry{{int_hash_rapid(200)}, 200, 2000};
    TestEntry query1{{int_hash_rapid(100)}, 100, 0};

    // Upsert new keys
    ASSERT_EQ(clfm_upsert(cmap, &entry1->node, test_entry_eq), &entry1->node);
    ASSERT_EQ(clfm_upsert(cmap, &entry2->node, test_entry_eq), &entry2->node);
    ASSERT_EQ(clfm_size(cmap), 2);

    // Upsert duplicate
    ASSERT_EQ(clfm_upsert(cmap, &entry1->node, test_entry_eq), &entry1->node);
    ASSERT_EQ(clfm_size(cmap), 2);

    // Lookup
    ASSERT_NE(clfm_lookup(cmap, &query1.node, test_entry_eq), nullptr);

    // Remove
    ASSERT_EQ(clfm_remove(cmap, &query1.node, test_entry_eq), &entry1->node);
    ASSERT_EQ(clfm_size(cmap), 1);
    ASSERT_EQ(clfm_lookup(cmap, &query1.node, test_entry_eq), nullptr);

    // Remove non-existent
    ASSERT_EQ(clfm_remove(cmap, &query1.node, test_entry_eq), nullptr);

    delete entry1;
    delete entry2;
    qsbr_quiescent();
}

TEST_F(CLFMapTest, MultiThreadAllNodesPresent) {
    const int keys_per_thread = 10000;
    std::vector<std::thread> threads;
    std::vector<TestEntry *> all_entries(NUM_THREADS * keys_per_thread);

    for (size_t i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&, thread_id = i]() {
            qsbr_reg();
            uint64_t start_key = thread_id * keys_per_thread;
            uint64_t end_key = start_key + keys_per_thread;

            for (uint64_t k = start_key; k < end_key; ++k) {
                // Keys are unique across all threads
                auto *entry = new TestEntry{{int_hash_rapid(k)}, k, k};
                all_entries[k] = entry;
                ASSERT_EQ(clfm_upsert(cmap, &entry->node, test_entry_eq), &entry->node);
            }
            qsbr_quiescent();
            qsbr_unreg();
        });
    }

    for (auto &t: threads) {
        t.join();
    }

    // Verify all nodes are present
    ASSERT_EQ(clfm_size(cmap), NUM_THREADS * keys_per_thread);
    for (uint64_t k = 0; k < NUM_THREADS * keys_per_thread; ++k) {
        TestEntry query{{int_hash_rapid(k)}, k, 0};
        ASSERT_NE(clfm_lookup(cmap, &query.node, test_entry_eq), nullptr) << "Key " << k << " was not found.";
    }
    qsbr_quiescent();

    // Cleanup
    for (auto *entry: all_entries) {
        delete entry;
    }
}

TEST_F(CLFMapTest, MultiThreadMixedReadWrite) {
    const int ops_per_thread = 50000;
    const int key_space = 10000;
    std::vector<std::thread> threads;
    std::vector<TestEntry *> initial_entries(key_space);

    // Pre-fill with some data
    for (int i = 0; i < key_space; ++i) {
        auto *entry = new TestEntry{{int_hash_rapid((uint64_t) i)}, (uint64_t) i, (uint64_t) i};
        initial_entries[i] = entry;
        clfm_upsert(cmap, &entry->node, test_entry_eq);
    }

    for (size_t i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&]() {
            qsbr_reg();
            std::mt19937 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::uniform_int_distribution<uint64_t> key_dist(0, key_space - 1);
            std::uniform_int_distribution<int> op_dist(0, 99);

            for (int j = 0; j < ops_per_thread; ++j) {
                uint64_t key = key_dist(rng);
                TestEntry query{{int_hash_rapid(key)}, key, 0};

                int op = op_dist(rng);
                if (op < 80) { // 80% Lookup
                    clfm_lookup(cmap, &query.node, test_entry_eq);
                } else if (op < 90) { // 10% Upsert (most will be duplicates)
                    auto *entry = new TestEntry{{int_hash_rapid(key)}, key, key + 1};
                    LFNode *result = clfm_upsert(cmap, &entry->node, test_entry_eq);
                    if (result != &entry->node) {
                        delete entry; // Upsert found existing, so new entry is not used
                    }
                } else { // 10% Remove
                    LFNode *removed = clfm_remove(cmap, &query.node, test_entry_eq);
                    // In a real GC, we would retire the 'removed' node. Here we just acknowledge it.
                    (void) removed;
                }
            }
            qsbr_quiescent();
            qsbr_unreg();
        });
    }

    for (auto &t: threads) {
        t.join();
    }
    qsbr_quiescent();

    // Cleanup initial entries
    for (auto *entry: initial_entries) {
        // This is simplified; a real GC would be needed to know if it's safe to delete
    }
}

TEST_F(CLFMapTest, SingleThreadUpsertReturnValue) {
    auto *entry1 = new TestEntry{{int_hash_rapid(100)}, 100, 1000};
    auto *entry2_new = new TestEntry{{int_hash_rapid(100)}, 100, 2000}; // Same key, new value

    // 1. Upsert a new key
    LFNode *result_node = clfm_upsert(cmap, &entry1->node, test_entry_eq);
    ASSERT_EQ(result_node, &entry1->node); // Should return the new node
    ASSERT_EQ(clfm_size(cmap), 1);

    // Verify the content
    LFNode *found_node = clfm_lookup(cmap, &entry1->node, test_entry_eq);
    ASSERT_NE(found_node, nullptr);
    auto *found_entry = container_of(found_node, TestEntry, node);
    ASSERT_EQ(found_entry->value, 1000);

    // 2. Upsert the same key again
    result_node = clfm_upsert(cmap, &entry2_new->node, test_entry_eq);
    ASSERT_EQ(result_node, &entry1->node); // Should return the ORIGINAL node
    ASSERT_EQ(clfm_size(cmap), 1); // Size should not change

    // 3. Verify the value has not changed (upsert does not replace)
    found_node = clfm_lookup(cmap, &entry1->node, test_entry_eq);
    ASSERT_NE(found_node, nullptr);
    found_entry = container_of(found_node, TestEntry, node);
    ASSERT_EQ(found_entry->value, 1000); // Value should still be the original

    delete entry1;
    delete entry2_new;
    qsbr_quiescent();
}
