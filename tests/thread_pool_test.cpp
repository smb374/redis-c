#include <cstdio>
#include <gtest/gtest.h>
#include <numeric>
#include <vector>

#include "ev.h"
#include "qsbr.h"
#include "thread_pool.h"

// --- Test Data Structures ---
struct WorkNode : public cnode {
    int value;
};

struct ResultNode : public cnode {
    int value;
};

// --- Test State (Global for Callback Access) ---
static int g_items_received = 0;
static int g_num_items = 0;
static std::vector<bool> g_received_check;
static struct ev_loop *g_main_loop = nullptr;

// --- Worker Function ---
// Doubles the input value and cleans up the work node.
cnode *double_value_work(cnode *work) {
    if (!work)
        return nullptr;
    auto *w_node = static_cast<WorkNode *>(work);
    auto *r_node = new ResultNode();
    r_node->value = w_node->value * 2;

    delete w_node; // Worker cleans up the work item
    qsbr_quiescent();
    return r_node;
}

// --- Result Callback for the Main Thread ---
// This function is called by the thread pool when a result is ready.
bool test_res_cb(cnode *node) {
    EXPECT_NE(node, nullptr);
    auto *r_node = static_cast<ResultNode *>(node);

    // Verify the work was done correctly
    int original_value = r_node->value / 2;
    EXPECT_EQ(r_node->value % 2, 0);
    EXPECT_GE(original_value, 0);
    EXPECT_LT(original_value, g_num_items);

    // Check for duplicates
    EXPECT_FALSE(g_received_check[original_value]);
    g_received_check[original_value] = true;

    g_items_received++;
    delete r_node;

    // If all items have been received, stop the main event loop.
    qsbr_quiescent();
    return g_items_received == g_num_items;
}

// --- Test Fixture ---
class ThreadPoolTest : public ::testing::Test {
protected:
    ThreadPool pool;

    void SetUp() override {
        // Initialize the thread pool with our callback
        qsbr_init(65536);
        qsbr_reg();
        pool_init(&pool, test_res_cb);
        g_main_loop = pool.loop;

        // Reset global state for each test
        g_items_received = 0;
        g_num_items = 0;
        g_received_check.clear();
    }

    void TearDown() override {
        pool_destroy(&pool);
        g_main_loop = nullptr;
        qsbr_unreg();
        qsbr_destroy();
    }
};

// --- Test Case ---
TEST_F(ThreadPoolTest, PostAndReceiveWork) {
    g_num_items = 8000; // Set the total number of items for this test
    g_received_check.assign(g_num_items, false);

    pool_start(&pool, double_value_work);

    // Post all work items to the pool
    for (int i = 0; i < g_num_items; ++i) {
        auto *work = new WorkNode();
        work->value = i;
        pool_post(&pool, work);
    }

    // Run the event loop. This will block until the callback calls ev_break.
    ev_run(g_main_loop, 0);

    // The loop has exited, verify that all items were processed.
    EXPECT_EQ(g_items_received, g_num_items);

    // Final check to ensure all values were received
    for (int i = 0; i < g_num_items; ++i) {
        ASSERT_TRUE(g_received_check[i]);
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
