#include <atomic>
#include <gtest/gtest.h>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

extern "C" {
#include "hashtable.h"
#include "utils.h"
}

// --- Test Entry Structure ---
struct TestEntry {
    HNode node;
    uint64_t key;
    uint64_t value;
};

static bool test_entry_eq(HNode *lhs, HNode *rhs) {
    const auto *le = container_of(lhs, TestEntry, node);
    const auto *re = container_of(rhs, TestEntry, node);
    return le->key == re->key;
}

// --- Global Test State ---
class CHMapTest : public ::testing::Test {
protected:
    static CHMap *g_chmap;
    static std::atomic<bool> g_initialized;
    // Purgatory for nodes that have been deleted but are pending safe reclamation.
    static std::vector<TestEntry *> g_purgatory;
    static std::mutex g_purgatory_mutex;

    static void SetUpTestSuite() {
        bool expected = false;
        if (g_initialized.compare_exchange_strong(expected, true)) {
            g_chmap = chm_new(nullptr);
        }
    }

    static void TearDownTestSuite() {
        bool expected = true;
        if (g_initialized.compare_exchange_strong(expected, false)) {
            chm_clear(g_chmap); // Use the map's own cleanup function
            g_chmap = nullptr;

            // Now it's safe to free the nodes from purgatory
            for (auto *entry: g_purgatory) {
                delete entry;
            }
            g_purgatory.clear();
        }
    }

    void SetUp() override {}
};

CHMap *CHMapTest::g_chmap = nullptr;
std::atomic<bool> CHMapTest::g_initialized{false};
std::vector<TestEntry *> CHMapTest::g_purgatory;
std::mutex CHMapTest::g_purgatory_mutex;


// --- Test Cases ---

TEST_F(CHMapTest, SingleThreadInsertAndLookup) {
    chm_register(g_chmap);
    uint64_t key = 1001;
    auto *entry = new TestEntry{{}, key, key * 2};
    entry->node.hcode = int_hash_rapid(key);

    bool success = chm_insert(g_chmap, &entry->node, test_entry_eq);
    ASSERT_TRUE(success);

    TestEntry query{{}, key};
    query.node.hcode = int_hash_rapid(key);
    HNode *result = chm_lookup(g_chmap, &query.node, test_entry_eq);

    ASSERT_NE(result, nullptr);
    auto *found = container_of(result, TestEntry, node);
    EXPECT_EQ(found->value, key * 2);
}

TEST_F(CHMapTest, SingleThreadSafeDelete) {
    chm_register(g_chmap);
    uint64_t key = 2002;
    auto *entry = new TestEntry{{}, key, key * 2};
    entry->node.hcode = int_hash_rapid(key);

    chm_insert(g_chmap, &entry->node, test_entry_eq);

    TestEntry query{{}, key};
    query.node.hcode = int_hash_rapid(key);
    HNode *deleted_node = chm_delete(g_chmap, &query.node, test_entry_eq);

    ASSERT_NE(deleted_node, nullptr);
    EXPECT_EQ(&entry->node, deleted_node);

    // Instead of freeing immediately, add to purgatory for deferred cleanup.
    // This is the correct, safe memory handling pattern.
    {
        std::lock_guard<std::mutex> lock(g_purgatory_mutex);
        g_purgatory.push_back(entry);
    }

    // Verify it's gone
    HNode *result = chm_lookup(g_chmap, &query.node, test_entry_eq);
    EXPECT_EQ(result, nullptr);
}

TEST_F(CHMapTest, MultiThreadContendedInsert) {
    const int num_threads = 8;
    const int inserts_per_thread = 10000;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            chm_register(g_chmap);
            for (int j = 0; j < inserts_per_thread; ++j) {
                // All threads contend on the same small key space
                uint64_t key = j % 100;
                auto *entry = new TestEntry{{}, key, (uint64_t) j};
                entry->node.hcode = int_hash_rapid(key);
                // Insert will either add a new node or replace an existing one.
                // The returned old node (if any) must be reclaimed safely.
                bool success = chm_insert(g_chmap, &entry->node, test_entry_eq);
                if (!success) { // Insert failed, so we can delete the new entry
                    delete entry;
                }
            }
        });
    }

    for (auto &t: threads) {
        t.join();
    }
}

TEST_F(CHMapTest, MultiThreadMixedReadWriteDelete) {
    const int num_threads = 8;
    const int ops_per_thread = 100000;
    std::vector<std::thread> threads;

    // Pre-fill with some data
    for (int i = 0; i < 1000; ++i) {
        auto *entry = new TestEntry{{}, (uint64_t) i, (uint64_t) i};
        entry->node.hcode = int_hash_rapid(i);
        chm_insert(g_chmap, &entry->node, test_entry_eq);
    }

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            chm_register(g_chmap);
            std::mt19937 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::uniform_int_distribution<uint64_t> key_dist(0, 999);
            std::uniform_int_distribution<int> op_dist(0, 99);

            for (int j = 0; j < ops_per_thread; ++j) {
                uint64_t key = key_dist(rng);
                TestEntry query{{}, key};
                query.node.hcode = int_hash_rapid(key);

                int op = op_dist(rng);
                if (op < 80) { // 80% Lookup
                    chm_lookup(g_chmap, &query.node, test_entry_eq);
                } else if (op < 90) { // 10% Insert
                    auto *entry = new TestEntry{{}, key, key};
                    entry->node.hcode = int_hash_rapid(key);
                    if (!chm_insert(g_chmap, &entry->node, test_entry_eq)) {
                        delete entry; // Insert failed, key exists
                    }
                } else { // 10% Delete
                    HNode *deleted = chm_delete(g_chmap, &query.node, test_entry_eq);
                    if (deleted) {
                        // Correctly defer freeing the node
                        std::lock_guard<std::mutex> lock(g_purgatory_mutex);
                        g_purgatory.push_back(container_of(deleted, TestEntry, node));
                    }
                }
            }
        });
    }

    for (auto &t: threads) {
        t.join();
    }
}
