// tests/cqueue_test.cpp

#include "cqueue.h"
#include "utils.h"

#include <gtest/gtest.h>
#include <thread>
#include <vector>

// The header is C, so we need extern "C" in C++

// A simple struct that embeds cnode for testing
struct TestNode {
    cnode n;
    int producer_id;
    int value;
};

class CQueueTest : public ::testing::Test {
protected:
    cqueue *q = nullptr;
    static constexpr size_t QUEUE_CAPACITY = 128;

    void SetUp() override { q = cq_init(q, QUEUE_CAPACITY); }

    void TearDown() override {
        // Clean up any remaining nodes in the queue to prevent memory leaks
        cnode *node;
        while ((node = cq_pop(q))) {
            delete container_of(node, TestNode, n);
        }
        cq_destroy(q);
    }
};

TEST_F(CQueueTest, Initialization) {
    EXPECT_EQ(cq_size(q), 0);
    EXPECT_EQ(cq_cap(q), QUEUE_CAPACITY);
}

TEST_F(CQueueTest, SingleProducerSingleConsumer) {
    // Put 10 items
    for (int i = 0; i < 10; ++i) {
        TestNode *node = new TestNode{{}, 0, i};
        ASSERT_TRUE(cq_put(q, &node->n));
    }

    EXPECT_EQ(cq_size(q), 10);

    // Pop 10 items
    for (int i = 0; i < 10; ++i) {
        cnode *c_node = cq_pop(q);
        ASSERT_NE(c_node, nullptr);
        TestNode *t_node = container_of(c_node, TestNode, n);
        EXPECT_EQ(t_node->value, i);
        delete t_node;
    }

    EXPECT_EQ(cq_size(q), 0);
    EXPECT_EQ(cq_pop(q), nullptr);
}

TEST_F(CQueueTest, FullAndEmpty) {
    // Fill the queue to capacity
    for (size_t i = 0; i < QUEUE_CAPACITY; ++i) {
        TestNode *n = new TestNode{{}, 0, (int) i};
        ASSERT_TRUE(cq_put(q, &n->n));
    }

    EXPECT_EQ(cq_size(q), QUEUE_CAPACITY);

    // Try to put one more item, should fail
    TestNode *extra_node = new TestNode{{}, 0, 999};
    ASSERT_FALSE(cq_put(q, &extra_node->n));
    delete extra_node; // Clean up the node that failed to be put

    // Pop everything
    for (size_t i = 0; i < QUEUE_CAPACITY; ++i) {
        cnode *node = cq_pop(q);
        ASSERT_NE(node, nullptr);
        delete container_of(node, TestNode, n);
    }

    EXPECT_EQ(cq_size(q), 0);

    // Try to pop from empty queue
    ASSERT_EQ(cq_pop(q), nullptr);
}

TEST_F(CQueueTest, MultiProducerSingleConsumer) {
    const int num_producers = 8;
    const int items_per_producer = 10000;
    const int total_items = num_producers * items_per_producer;
    std::vector<std::thread> producers;

    for (int i = 0; i < num_producers; ++i) {
        producers.emplace_back([this, i, items_per_producer]() {
            for (int j = 0; j < items_per_producer; ++j) {
                TestNode *node = new TestNode{{}, i, j};
                // Spin until put succeeds
                while (!cq_put(q, &node->n)) {
                    std::this_thread::yield();
                }
            }
        });
    }

    std::vector<int> items_from_producer(num_producers, 0);
    int consumed_count = 0;

    while (consumed_count < total_items) {
        cnode *c_node = cq_pop(q);
        if (c_node) {
            consumed_count++;
            TestNode *t_node = container_of(c_node, TestNode, n);
            items_from_producer[t_node->producer_id]++;
            delete t_node;
        } else {
            // Yield to give producers time to produce
            std::this_thread::yield();
        }
    }

    for (auto &t: producers) {
        t.join();
    }

    EXPECT_EQ(consumed_count, total_items);
    EXPECT_EQ(cq_size(q), 0);

    for (int i = 0; i < num_producers; ++i) {
        EXPECT_EQ(items_from_producer[i], items_per_producer);
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
