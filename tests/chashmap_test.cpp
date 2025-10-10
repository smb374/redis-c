#include <gtest/gtest.h>
#include <random>
#include <thread>
#include <vector>

extern "C" {
#include "hashtable.h"
#include "qsbr.h"
#include "utils.h"
}

qsbr *g_qsbr_gc = nullptr;
static __thread qsbr_tid tid;

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
    CHMap *chmap = nullptr;

    void SetUp() override {
        g_qsbr_gc = qsbr_init(nullptr, 65536);
        ASSERT_NE(g_qsbr_gc, nullptr);
        chmap = chm_new(nullptr);
        ASSERT_NE(chmap, nullptr);
    }

    void TearDown() override {
        chm_clear(chmap);
        qsbr_destroy(g_qsbr_gc);
        g_qsbr_gc = nullptr;
    }
};


// --- Test Cases ---

static HNode *upsert_create(HNode *key) {
    auto *query = container_of(key, TestEntry, node);
    auto *new_entry = new TestEntry{{}, query->key, query->value};
    new_entry->node.hcode = query->node.hcode;
    return &new_entry->node;
}

TEST_F(CHMapTest, SingleThreadInsertAndLookup) {
    uint64_t key = 1001;
    auto *entry = new TestEntry{{}, key, key * 2};
    entry->node.hcode = int_hash_rapid(key);

    HNode *result_node = chm_upsert(chmap, &entry->node, upsert_create, test_entry_eq);
    ASSERT_NE(result_node, nullptr);
    // In this case, it should be a new insertion, so the returned node is the one we passed.
    ASSERT_EQ(container_of(result_node, TestEntry, node)->value, key * 2);
    delete entry; // The entry is copied in upsert_create, so we can delete the stack one.

    TestEntry query{{}, key};
    query.node.hcode = int_hash_rapid(key);
    HNode *result = chm_lookup(chmap, &query.node, test_entry_eq);

    ASSERT_NE(result, nullptr);
    auto *found = container_of(result, TestEntry, node);
    EXPECT_EQ(found->value, key * 2);
}

TEST_F(CHMapTest, SingleThreadSafeDelete) {
    qsbr_tid tid = qsbr_reg(g_qsbr_gc);
    uint64_t key = 2002;
    auto *entry = new TestEntry{{}, key, key * 2};
    entry->node.hcode = int_hash_rapid(key);

    chm_upsert(chmap, &entry->node, upsert_create, test_entry_eq);
    delete entry;

    TestEntry query{{}, key};
    query.node.hcode = int_hash_rapid(key);
    HNode *deleted_node = chm_delete(chmap, &query.node, test_entry_eq);

    ASSERT_NE(deleted_node, nullptr);

    // Use QSBR to reclaim the memory
    qsbr_alloc_cb(g_qsbr_gc, [](void *p) { delete container_of((HNode *) p, TestEntry, node); }, deleted_node);
    qsbr_quiescent(g_qsbr_gc, tid);

    // Verify it's gone
    HNode *result = chm_lookup(chmap, &query.node, test_entry_eq);
    EXPECT_EQ(result, nullptr);
}

TEST_F(CHMapTest, MultiThreadMixedReadWriteDelete) {
    const int num_threads = 8;
    const int ops_per_thread = 100000;
    std::vector<std::thread> threads;

    // Pre-fill with some data
    for (int i = 0; i < 1000; ++i) {
        auto *entry = new TestEntry{{}, (uint64_t) i, (uint64_t) i};
        entry->node.hcode = int_hash_rapid(i);
        chm_upsert(chmap, &entry->node, upsert_create, test_entry_eq);
        delete entry;
    }

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            qsbr_tid tid = qsbr_reg(g_qsbr_gc);
            std::mt19937 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::uniform_int_distribution<uint64_t> key_dist(0, 999);
            std::uniform_int_distribution<int> op_dist(0, 99);

            for (int j = 0; j < ops_per_thread; ++j) {
                uint64_t key = key_dist(rng);
                int op = op_dist(rng);

                if (op < 80) { // 80% Lookup
                    TestEntry query{{}, key};
                    query.node.hcode = int_hash_rapid(key);
                    chm_lookup(chmap, &query.node, test_entry_eq);
                } else if (op < 90) { // 10% Upsert
                    auto *entry = new TestEntry{{}, key, key + 1}; // New value
                    entry->node.hcode = int_hash_rapid(key);
                    chm_upsert(chmap, &entry->node, upsert_create, test_entry_eq);
                    delete entry;
                } else { // 10% Delete
                    TestEntry query{{}, key};
                    query.node.hcode = int_hash_rapid(key);
                    HNode *deleted = chm_delete(chmap, &query.node, test_entry_eq);
                    if (deleted) {
                        qsbr_alloc_cb(
                                g_qsbr_gc, [](void *p) { delete container_of((HNode *) p, TestEntry, node); }, deleted);
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

TEST_F(CHMapTest, MultiThreadAllNodesPresent) {
    const int num_threads = 8;
    const int keys_per_thread = 1000;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, thread_id = i]() {
            qsbr_tid tid = qsbr_reg(g_qsbr_gc);
            uint64_t start_key = thread_id * keys_per_thread;
            uint64_t end_key = start_key + keys_per_thread;

            for (uint64_t k = start_key; k < end_key; ++k) {
                auto *entry = new TestEntry{{}, k, k};
                entry->node.hcode = int_hash_rapid(k);
                chm_upsert(chmap, &entry->node, upsert_create, test_entry_eq);
                delete entry;
            }
            qsbr_quiescent(g_qsbr_gc, tid);
        });
    }

    for (auto &t: threads) {
        t.join();
    }

    // Verify all nodes are present
    for (uint64_t k = 0; k < num_threads * keys_per_thread; ++k) {
        TestEntry query{{}, k};
        query.node.hcode = int_hash_rapid(k);
        HNode *result = chm_lookup(chmap, &query.node, test_entry_eq);
        ASSERT_NE(result, nullptr) << "Key " << k << " was not found.";
        auto *found = container_of(result, TestEntry, node);
        ASSERT_EQ(found->key, k);
    }

    ASSERT_EQ(chm_size(chmap), num_threads * keys_per_thread);
}
