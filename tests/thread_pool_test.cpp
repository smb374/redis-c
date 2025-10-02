// tests/thread_pool_test.cpp
#include <gtest/gtest.h>
#include <vector>
#include <numeric>
#include <unistd.h>
#include <sys/eventfd.h>

// The cqueue header is now C++-safe, but we still need the pool header
extern "C" {
#include "thread_pool.h"
// We need the cqueue API for the test to pop from the result queue
#include "cqueue.h"
}

// Define simple node structures for the test
struct WorkNode : public cnode {
    int value;
};

struct ResultNode : public cnode {
    int value;
};

// A simple function to be executed by the worker threads
// It doubles the input value and cleans up the work node.
cnode* double_value_work(cnode* work) {
    if (!work) return nullptr;
    WorkNode* w_node = static_cast<WorkNode*>(work);
    
    ResultNode* r_node = new ResultNode();
    r_node->value = w_node->value * 2;
    
    delete w_node; // The worker is responsible for cleaning up the work item
    return r_node;
}

class ThreadPoolTest : public ::testing::Test {
protected:
    ThreadPool pool;

    void SetUp() override {
        pool_init(&pool);
    }

    void TearDown() override {
        // Stop the threads first. This will block until they have all exited.
        pool_stop(&pool);

        // Clean up any remaining nodes in the result queue in case a test fails.
        cnode* node;
        if (pool.result_q) {
            while ((node = cq_pop(pool.result_q))) {
                delete static_cast<ResultNode*>(node);
            }
        }
        
        // Destroy cleans up queues and file descriptors
        pool_destroy(&pool);
    }
};

TEST_F(ThreadPoolTest, PostAndReceiveWork) {
    const int num_items = 8000; // Enough to exercise all workers
    pool_start(&pool, double_value_work);

    // Post work items
    for (int i = 0; i < num_items; ++i) {
        WorkNode* work = new WorkNode();
        work->value = i;
        ASSERT_TRUE(pool_post(&pool, work));
    }

    int items_received = 0;
    std::vector<bool> received_check(num_items, false);

    while (items_received < num_items) {
        uint64_t finished_count;
        // Block and wait for the result eventfd to be signaled
        ssize_t ret = read(pool.res_ev, &finished_count, sizeof(finished_count));
        ASSERT_GT(ret, 0);

        // We were woken up, now pop from the result queue
        for (uint64_t i = 0; i < finished_count; ++i) {
            // The result_q is now a pointer, so we pass it directly to cq_pop
            cnode* node = cq_pop(pool.result_q);
            ASSERT_NE(node, nullptr);
            
            ResultNode* r_node = static_cast<ResultNode*>(node);
            
            // Verify the work was done correctly
            int original_value = r_node->value / 2;
            EXPECT_EQ(r_node->value % 2, 0);
            ASSERT_GE(original_value, 0);
            ASSERT_LT(original_value, num_items);

            // Check for duplicates
            ASSERT_FALSE(received_check[original_value]);
            received_check[original_value] = true;

            items_received++;
            delete r_node;
        }
    }

    EXPECT_EQ(items_received, num_items);
    
    // Final check to ensure all values were received
    for(int i = 0; i < num_items; ++i) {
        ASSERT_TRUE(received_check[i]);
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}