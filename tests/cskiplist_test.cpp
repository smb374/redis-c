#include <gtest/gtest.h>
#include <numeric>
#include <thread>
#include <unistd.h>
#include <vector>

#include "cskiplist.h"
#include "debra.h"

// Define the global QSBR instance for the test executable

class CSkipListTest : public ::testing::Test {
protected:
    CSList *list = nullptr;

    void SetUp() override {
        gc_init();
        gc_reg();
        // Initialize the skip list
        list = csl_new(nullptr);
    }

    void TearDown() override {
        // Ensure all memory is reclaimed before the next test
        csl_destroy(list);
        list = nullptr;
        gc_unreg();
    }
};

TEST_F(CSkipListTest, InsertLookupRemove) {
    long val1 = 100;
    long val2 = 200;

    // Insert
    ASSERT_EQ(csl_update(list, {10, 0}, &val1), nullptr);
    ASSERT_EQ(csl_update(list, {20, 0}, &val2), nullptr);

    // Lookup
    ASSERT_EQ(csl_lookup(list, {10, 0}), &val1);
    ASSERT_EQ(csl_lookup(list, {20, 0}), &val2);
    ASSERT_EQ(csl_lookup(list, {30, 0}), nullptr);

    // Update
    long val3 = 300;
    ASSERT_EQ(csl_update(list, {10, 0}, &val3), &val1);
    ASSERT_EQ(csl_lookup(list, {10, 0}), &val3);

    // Remove
    ASSERT_EQ(csl_remove(list, {10, 0}), &val3);
    ASSERT_EQ(csl_lookup(list, {10, 0}), nullptr);

    // Remove non-existent
    ASSERT_EQ(csl_remove(list, {10, 0}), nullptr);
}

TEST_F(CSkipListTest, ConcurrentInsertAndRemove) {
    const int num_threads = 4;
    const int keys_per_thread = 1000;
    std::vector<std::thread> threads;

    auto worker = [&](int thread_id) {
        gc_reg();
        uint64_t start_key = thread_id * keys_per_thread;
        uint64_t end_key = start_key + keys_per_thread;

        std::vector<long> values(keys_per_thread);
        std::iota(values.begin(), values.end(), start_key);

        // Insert all keys
        for (uint64_t i = start_key; i < end_key; ++i) {
            csl_update(list, {i, 0}, &values[i - start_key]);
        }

        // Mark quiescent state

        // Verify all inserted keys can be found
        for (uint64_t i = start_key; i < end_key; ++i) {
            ASSERT_EQ(csl_lookup(list, {i, 0}), &values[i - start_key]);
        }

        // Mark quiescent state

        // Remove all keys
        for (uint64_t i = start_key; i < end_key; ++i) {
            ASSERT_EQ(csl_remove(list, {i, 0}), &values[i - start_key]);
        }
        // Make sure that all callbacks are visible & ran.
        for (int i = 0; i < 4; i++) {
            usleep(500);
        }
        gc_unreg();
    };

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto &t: threads) {
        t.join();
    }

    for (uint64_t i = 0; i < num_threads * keys_per_thread; ++i) {
        ASSERT_EQ(csl_lookup(list, {i, 0}), nullptr);
    }
}

TEST_F(CSkipListTest, PopMinSingleThreaded) {
    long val1 = 10, val2 = 20, val3 = 30;

    // Pop from empty list
    ASSERT_EQ(csl_pop_min(list), nullptr);

    // Insert items
    csl_update(list, {20, 0}, &val2);
    csl_update(list, {10, 0}, &val1);
    csl_update(list, {30, 0}, &val3);

    // Pop items and check order
    ASSERT_EQ(csl_pop_min(list), &val1);
    ASSERT_EQ(csl_pop_min(list), &val2);
    ASSERT_EQ(csl_pop_min(list), &val3);

    // List should be empty now
    ASSERT_EQ(csl_pop_min(list), nullptr);
    ASSERT_EQ(csl_lookup(list, {10, 0}), nullptr);
}

TEST_F(CSkipListTest, PopMinConcurrent) {
    const int num_keys = 4000;
    const int num_threads = 4;
    std::vector<long> values(num_keys);
    std::vector<std::thread> threads;
    std::atomic<int> pop_count(0);

    // Pre-populate the list
    for (uint64_t i = 0; i < num_keys; ++i) {
        values[i] = i;
        csl_update(list, {i, 0}, &values[i]);
    }

    auto pop_worker = [&]() {
        gc_reg();
        while (true) {
            void *val = csl_pop_min(list);
            if (val) {
                pop_count++;
            } else if (csl_find_min_key(list).key == UINT64_MAX) {
                break;
            }
        }
        gc_unreg();
    };

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(pop_worker);
    }

    for (auto &t: threads) {
        t.join();
    }

    // Check that all keys were popped exactly once
    ASSERT_EQ(pop_count.load(), num_keys);
    ASSERT_EQ(csl_find_min_key(list).key, UINT64_MAX); // List should be empty
}
