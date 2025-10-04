#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

extern "C" {
#include "qsbr.h"
}

// Simple callback function for testing
void test_callback(void *arg) {
    if (arg) {
        auto *counter = static_cast<std::atomic<int> *>(arg);
        counter->fetch_add(1);
    }
}

class QSBRTest : public ::testing::Test {
protected:
    qsbr *gc;
    std::atomic<int> reclamation_counter;

    void SetUp() override {
        gc = qsbr_init(nullptr, 100000); // Large enough for the stress test
        reclamation_counter.store(0);
    }

    void TearDown() override {
        // The number of threads isn't known here, so the test that needs flushing
        // must do it itself.
        qsbr_destroy(gc);
    }

    // Helper to force reclamation cycles
    void force_reclamation(uint8_t num_threads) {
        if (num_threads == 0)
            return;

        // Quiescent states might be needed multiple times to clear both prev and curr queues
        for (int i = 0; i < 3; ++i) {
            for (uint8_t j = 0; j < num_threads; ++j) {
                qsbr_quiescent(gc, j);
            }
        }
    }
};

TEST_F(QSBRTest, BasicRegistrationAndError) {
    ASSERT_NE(gc, nullptr);

    // Check sequential registration
    for (int i = 0; i < 64; ++i) {
        qsbr_tid tid = qsbr_reg(gc);
        EXPECT_EQ(tid, i);
    }

    // Check error case when more than 64 threads are registered
    qsbr_tid error_tid = qsbr_reg(gc);
    EXPECT_EQ(error_tid, -1);
}

TEST_F(QSBRTest, SingleThreadReclamation) {
    qsbr_tid tid = qsbr_reg(gc);
    ASSERT_EQ(tid, 0);

    qsbr_alloc_cb(gc, test_callback, &reclamation_counter);

    // After one quiescent state, callback should be in prev queue, not executed
    qsbr_quiescent(gc, tid);
    EXPECT_EQ(reclamation_counter.load(), 0);

    // After a second quiescent state, epoch advances, and callback is executed
    qsbr_quiescent(gc, tid);
    EXPECT_EQ(reclamation_counter.load(), 1);
    // No delete cb, memory is managed by QSBR system
}

TEST_F(QSBRTest, MultiThreadReclamation) {
    const int num_threads = 8;
    const int callbacks_per_thread = 10000;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, callbacks_per_thread]() {
            qsbr_tid tid = qsbr_reg(gc);
            ASSERT_NE(tid, -1);

            for (int j = 0; j < callbacks_per_thread; ++j) {
                qsbr_alloc_cb(gc, test_callback, &reclamation_counter);
                qsbr_quiescent(gc, tid);
            }
        });
    }

    for (auto &t: threads) {
        t.join();
    }

    // At this point, many callbacks are pending. Force reclamation.
    force_reclamation(num_threads);

    EXPECT_EQ(reclamation_counter.load(), num_threads * callbacks_per_thread);
    // No cleanup loop needed, memory is managed by QSBR system
}
