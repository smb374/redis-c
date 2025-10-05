#include <atomic>
#include <cstdlib>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

extern "C" {
#include "ebr.h"
}

class EBRTest : public ::testing::Test {
protected:
    ebr_manager *m;

    void SetUp() override { m = ebr_new(); }

    void TearDown() override {
        ebr_destroy(m);
        free(m);
    }
};

TEST_F(EBRTest, SingleThreadRegistration) {
    ASSERT_TRUE(ebr_reg(m));
    // In this model, the thread state is thread-local, so we can't inspect it directly.
    // We just verify that registration and unregistration don't crash.
    ebr_unreg(m);
}

TEST_F(EBRTest, SingleThreadAllocFree) {
    ASSERT_TRUE(ebr_reg(m));

    // Enter a critical section
    ASSERT_TRUE(ebr_enter(m));

    // Allocate and immediately free memory
    void *ptr = ebr_calloc(1, 64);
    ASSERT_NE(ptr, nullptr);
    ebr_free(m, ptr);

    // Leave the critical section
    ebr_leave(m);

    // Try to reclaim any pending garbage
    ebr_try_reclaim(m);
    ebr_try_reclaim(m);
    ebr_try_reclaim(m);

    ebr_unreg(m);
    // The test passes if it doesn't crash, implying the free was handled correctly.
}

TEST_F(EBRTest, MultiThreadStress) {
    const int num_threads = 8;
    const int ops_per_thread = 10000;
    std::vector<std::thread> threads;
    std::atomic<bool> all_threads_ready(false);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, ops_per_thread, &all_threads_ready]() {
            // Each thread registers itself
            ASSERT_TRUE(ebr_reg(m));

            // Wait for a signal to start all threads at roughly the same time
            while (!all_threads_ready.load()) {
            }

            for (int j = 0; j < ops_per_thread; ++j) {
                // Enter critical section
                ASSERT_TRUE(ebr_enter(m));

                // Simulate work: allocate an object, then immediately free it.
                void *ptr = ebr_calloc(1, 128);
                ASSERT_NE(ptr, nullptr);
                ebr_free(m, ptr);

                // Leave critical section
                ebr_leave(m);

                // Periodically, one thread might try to advance the epoch
                if (j % 100 == 0) {
                    ebr_try_reclaim(m);
                }
            }

            // Deregister when done
            ebr_unreg(m);
        });
    }

    all_threads_ready.store(true);

    for (auto &t: threads) {
        t.join();
    }

    // Final cleanup cycles
    ebr_try_reclaim(m);
    ebr_try_reclaim(m);
    ebr_try_reclaim(m);

    // The test passes if it completes without crashing from memory errors.
}
