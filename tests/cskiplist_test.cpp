#include <gtest/gtest.h>
#include <numeric>
#include <thread>
#include <unistd.h>
#include <vector>

extern "C" {
#include "cskiplist.h"
#include "qsbr.h"
}

// Define the global QSBR instance for the test executable
qsbr *g_qsbr_gc = nullptr;

class CSkipListTest : public ::testing::Test {
protected:
    CSList *list = nullptr;

    void SetUp() override {
        // Initialize the global QSBR for each test
        g_qsbr_gc = qsbr_init(nullptr, 65536);
        ASSERT_TRUE(g_qsbr_gc != nullptr);

        // Initialize the skip list
        list = csl_new(nullptr);
    }

    void TearDown() override {
        // Ensure all memory is reclaimed before the next test
        qsbr_destroy(g_qsbr_gc);
        g_qsbr_gc = nullptr;
        csl_destroy(list);
        list = nullptr;
    }
};

TEST_F(CSkipListTest, InsertLookupRemove) {
    qsbr_tid main_tid = qsbr_reg(g_qsbr_gc);
    long val1 = 100;
    long val2 = 200;

    // Insert
    ASSERT_EQ(csl_update(list, 10, &val1), nullptr);
    ASSERT_EQ(csl_update(list, 20, &val2), nullptr);

    // Lookup
    ASSERT_EQ(csl_lookup(list, 10), &val1);
    ASSERT_EQ(csl_lookup(list, 20), &val2);
    ASSERT_EQ(csl_lookup(list, 30), nullptr);

    // Update
    long val3 = 300;
    ASSERT_EQ(csl_update(list, 10, &val3), &val1);
    ASSERT_EQ(csl_lookup(list, 10), &val3);

    // Remove
    ASSERT_EQ(csl_remove(list, 10), &val3);
    qsbr_quiescent(g_qsbr_gc, main_tid); // Allow GC to proceed
    ASSERT_EQ(csl_lookup(list, 10), nullptr);

    // Remove non-existent
    ASSERT_EQ(csl_remove(list, 10), nullptr);
    qsbr_quiescent(g_qsbr_gc, main_tid);
}

TEST_F(CSkipListTest, ConcurrentInsertAndRemove) {
    const int num_threads = 4;
    const int keys_per_thread = 1000;
    std::vector<std::thread> threads;

    auto worker = [&](int thread_id) {
        qsbr_tid tid = qsbr_reg(g_qsbr_gc);
        uint64_t start_key = thread_id * keys_per_thread;
        uint64_t end_key = start_key + keys_per_thread;

        std::vector<long> values(keys_per_thread);
        std::iota(values.begin(), values.end(), start_key);

        // Insert all keys
        for (uint64_t i = start_key; i < end_key; ++i) {
            csl_update(list, i, &values[i - start_key]);
        }

        // Mark quiescent state
        qsbr_quiescent(g_qsbr_gc, tid);

        // Verify all inserted keys can be found
        for (uint64_t i = start_key; i < end_key; ++i) {
            ASSERT_EQ(csl_lookup(list, i), &values[i - start_key]);
        }

        // Mark quiescent state
        qsbr_quiescent(g_qsbr_gc, tid);

        // Remove all keys
        for (uint64_t i = start_key; i < end_key; ++i) {
            ASSERT_EQ(csl_remove(list, i), &values[i - start_key]);
        }
        // Make sure that all callbacks are visible & ran.
        for (int i = 0; i < 4; i++) {
            qsbr_quiescent(g_qsbr_gc, tid);
            usleep(500);
        }
    };

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto &t: threads) {
        t.join();
    }

    for (int i = 0; i < num_threads * keys_per_thread; ++i) {
        ASSERT_EQ(csl_lookup(list, i), nullptr);
    }
}
