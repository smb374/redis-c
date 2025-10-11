#include <cstdio>
#include <gtest/gtest.h>
#include <random>
#include <thread>
#include <unistd.h>
#include <vector>

extern "C" {
#include "phmap.h"
#include "qsbr.h"
#include "utils.h"
}

// Define the global QSBR instance for the test executable
qsbr *g_qsbr_gc = nullptr;

// --- Test Entry Structure ---
struct TestEntry {
    BNode node;
    uint64_t key;
    uint64_t value;
};

static bool test_entry_eq(BNode *lhs, BNode *rhs) {
    if (!lhs || !rhs)
        return false;
    const auto *le = container_of(lhs, TestEntry, node);
    const auto *re = container_of(rhs, TestEntry, node);
    return le->key == re->key;
}

// --- Test Fixture ---
class PHMapTest : public ::testing::Test {
protected:
    PHTable *map = nullptr;
    // static const size_t MAP_SIZE = 65536; // Must be a power of 2
    static const size_t MAP_SIZE = 1048576; // Must be a power of 2

    void SetUp() override {
        g_qsbr_gc = qsbr_init(nullptr, 65536);
        ASSERT_NE(g_qsbr_gc, nullptr);
        map = pht_new(nullptr, MAP_SIZE);
        ASSERT_NE(map, nullptr);
    }

    void TearDown() override {
        // In a real scenario, we'd need to iterate and free nodes before destroying the map.
        // For these tests, we rely on QSBR callbacks for nodes that were erased.
        pht_destroy(map);
        qsbr_destroy(g_qsbr_gc);
        g_qsbr_gc = nullptr;
    }
};

TEST_F(PHMapTest, SingleThreadInsertLookupErase) {
    qsbr_tid tid = qsbr_reg(g_qsbr_gc);
    auto *entry = new TestEntry{{int_hash_rapid(100)}, 100, 1000};

    // Insert
    ASSERT_TRUE(pht_insert(map, &entry->node, test_entry_eq));

    // Lookup
    TestEntry query{{int_hash_rapid(100)}, 100, 0};
    BNode *found_node = pht_lookup(map, &query.node, test_entry_eq);
    ASSERT_NE(found_node, nullptr);
    ASSERT_EQ(container_of(found_node, TestEntry, node)->value, 1000);

    // Erase
    BNode *erased_node = pht_erase(map, &query.node, test_entry_eq);
    ASSERT_EQ(erased_node, &entry->node);

    // Schedule for reclamation
    qsbr_alloc_cb(g_qsbr_gc, [](void *p) { delete container_of((BNode *) p, TestEntry, node); }, erased_node);
    qsbr_quiescent(g_qsbr_gc, tid);

    // Verify it's gone
    ASSERT_EQ(pht_lookup(map, &query.node, test_entry_eq), nullptr);
}

TEST_F(PHMapTest, MultiThreadAllNodesPresent) {
    const int num_threads = 8;
    const int keys_per_thread = 10000;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, thread_id = i]() {
            qsbr_tid tid = qsbr_reg(g_qsbr_gc);
            uint64_t start_key = thread_id * keys_per_thread;
            uint64_t end_key = start_key + keys_per_thread;

            for (uint64_t k = start_key; k < end_key; ++k) {
                auto *entry = new TestEntry{{int_hash_rapid(k)}, k, k};
                bool inserted = pht_insert(map, &entry->node, test_entry_eq);
                ASSERT_EQ(inserted, true);
            }
            qsbr_quiescent(g_qsbr_gc, tid);
        });
    }

    for (auto &t: threads) {
        t.join();
    }

    // Verify all nodes are present
    for (uint64_t k = 0; k < num_threads * keys_per_thread; ++k) {
        TestEntry query{{int_hash_rapid(k)}, k, 0};
        BNode *result = pht_lookup(map, &query.node, test_entry_eq);
        ASSERT_NE(result, nullptr) << "Key " << k << " was not found.";
        ASSERT_EQ(container_of(result, TestEntry, node)->key, k);
    }
}

TEST_F(PHMapTest, MultiThreadMixedReadWriteDelete) {
    const int num_threads = 8;
    const int ops_per_thread = 20000;
    const int key_space = 500;
    std::vector<std::thread> threads;

    // Pre-fill with some data
    for (int i = 0; i < key_space; ++i) {
        auto *entry = new TestEntry{{int_hash_rapid((uint64_t) i)}, (uint64_t) i, (uint64_t) i};
        pht_insert(map, &entry->node, test_entry_eq);
    }

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            qsbr_tid tid = qsbr_reg(g_qsbr_gc);
            std::mt19937 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::uniform_int_distribution<uint64_t> key_dist(0, key_space - 1);
            std::uniform_int_distribution<int> op_dist(0, 99);

            for (int j = 0; j < ops_per_thread; ++j) {
                uint64_t key = key_dist(rng);
                TestEntry query{{int_hash_rapid(key)}, key, 0};

                int op = op_dist(rng);
                if (op < 80) { // 80% Lookup
                    pht_lookup(map, &query.node, test_entry_eq);
                } else if (op < 90) { // 10% Insert (or update)
                    auto *entry = new TestEntry{{int_hash_rapid(key)}, key, key + 1};
                    if (!pht_insert(map, &entry->node, test_entry_eq)) {
                        // In this model, insert fails if key exists. We don't update.
                        delete entry;
                    }
                } else { // 10% Delete
                    BNode *deleted = pht_erase(map, &query.node, test_entry_eq);
                    if (deleted) {
                        qsbr_alloc_cb(
                                g_qsbr_gc, [](void *p) { delete container_of((BNode *) p, TestEntry, node); }, deleted);
                    }
                }
            }
            qsbr_quiescent(g_qsbr_gc, tid);
        });
    }

    for (auto &t: threads) {
        t.join();
    }
}

TEST_F(PHMapTest, MultiThreadInsertThenErase) {
    const int num_threads = 8;
    const int keys_per_thread = 10000;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, thread_id = i]() {
            qsbr_tid tid = qsbr_reg(g_qsbr_gc);
            uint64_t start_key = thread_id * keys_per_thread;
            uint64_t end_key = start_key + keys_per_thread;

            // Phase 1: Insert
            std::vector<TestEntry *> inserted_nodes;
            for (uint64_t k = start_key; k < end_key; ++k) {
                auto *entry = new TestEntry{{int_hash_rapid(k)}, k, k};
                bool inserted = pht_insert(map, &entry->node, test_entry_eq);
                ASSERT_EQ(inserted, true);
                inserted_nodes.push_back(entry);
            }

            qsbr_quiescent(g_qsbr_gc, tid);

            // Phase 2: Erase
            for (auto *entry: inserted_nodes) {
                TestEntry query{{entry->node.hcode}, entry->key, 0};
                BNode *deleted = pht_erase(map, &query.node, test_entry_eq);
                EXPECT_EQ(deleted, &entry->node);
                if (deleted) {
                    qsbr_alloc_cb(
                            g_qsbr_gc, [](void *p) { delete container_of((BNode *) p, TestEntry, node); }, deleted);
                }
            }
            qsbr_quiescent(g_qsbr_gc, tid);
        });
    }

    for (auto &t: threads) {
        t.join();
    }

    // Verify all nodes are gone
    for (uint64_t k = 0; k < num_threads * keys_per_thread; ++k) {
        TestEntry query{{int_hash_rapid(k)}, k, 0};
        ASSERT_EQ(pht_lookup(map, &query.node, test_entry_eq), nullptr) << "Key " << k << " was not erased.";
    }
}
