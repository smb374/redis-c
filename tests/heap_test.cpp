//
// Created by poyehchen on 9/29/25.
//
#include "heap.h"

#include <gtest/gtest.h>
#include <vector>
#include <numeric>
#include <algorithm>

static size_t heap_parent(const size_t i) {
    return (i + 1) / 2 - 1;
}
// --- Helper function to verify the min-heap property ---
// Returns true if the array satisfies the min-heap property, false otherwise.
bool is_min_heap(const Heap* heap) {
    for (size_t i = 1; i < heap->len; ++i) {
        if (heap->nodes[i].val < heap->nodes[heap_parent(i)].val) {
            // Found a child smaller than its parent.
            return false;
        }
    }
    return true;
}

// --- Test Fixture ---

class HeapTest : public ::testing::Test {
protected:
    Heap heap;
    // This vector simulates an external data structure that stores the positions
    // of its items within the heap. The `ref` pointers will point into this vector.
    std::vector<size_t> external_indices;

    void SetUp() override {
        // Start with a small capacity to ensure resizing logic is tested.
        heap_init(&heap, 4);
    }

    void TearDown() override {
        heap_free(&heap);
    }
};

// --- Test Cases ---

TEST_F(HeapTest, Initialization) {
    ASSERT_NE(heap.nodes, nullptr);
    EXPECT_EQ(heap.len, 0);
    EXPECT_EQ(heap.cap, 4);
}

TEST_F(HeapTest, UpsertAsInsert) {
    const int num_items = 5;
    external_indices.resize(num_items);

    // Insert items in a non-trivial order
    std::vector<uint64_t> values = {50, 20, 80, 10, 40};

    for (int i = 0; i < num_items; ++i) {
        HeapNode node = {values[i], &external_indices[i]};
        // Use heap.len as the position to indicate insertion.
        heap_upsert(&heap, heap.len, node);
    }

    ASSERT_EQ(heap.len, num_items);
    EXPECT_TRUE(is_min_heap(&heap));

    // The smallest element should be at the top.
    EXPECT_EQ(heap.nodes[0].val, 10);
}

TEST_F(HeapTest, IndexingIsCorrectAfterInserts) {
    const int num_items = 5;
    external_indices.resize(num_items);
    std::vector<uint64_t> values = {50, 20, 80, 10, 40};

    for (int i = 0; i < num_items; ++i) {
        HeapNode node = {values[i], &external_indices[i]};
        heap_upsert(&heap, heap.len, node);
    }

    // After all insertions, check if the external indices are correct.
    // For each original item `i`, its index `external_indices[i]` should point
    // to a node in the heap that has the correct value and ref pointer.
    for (int i = 0; i < num_items; ++i) {
        size_t pos_in_heap = external_indices[i];
        ASSERT_LT(pos_in_heap, heap.len); // Index must be valid.

        HeapNode* node_in_heap = &heap.nodes[pos_in_heap];
        EXPECT_EQ(node_in_heap->val, values[i]);
        EXPECT_EQ(node_in_heap->ref, &external_indices[i]);
    }
}

TEST_F(HeapTest, UpsertAsUpdate) {
    const int num_items = 5;
    external_indices.resize(num_items);
    std::vector<uint64_t> values = {50, 20, 80, 10, 40};

    for (int i = 0; i < num_items; ++i) {
        HeapNode node = {values[i], &external_indices[i]};
        heap_upsert(&heap, heap.len, node);
    }
    ASSERT_TRUE(is_min_heap(&heap));

    // --- Test an update that causes a heap_up (decrease key) ---
    // Find the item with value 80 (original index 2).
    size_t pos_of_80 = external_indices[2];
    HeapNode updated_node_up = {5, &external_indices[2]}; // New value is 5
    heap_upsert(&heap, pos_of_80, updated_node_up);
    values[2] = 5; // Update our local values for later check.

    ASSERT_TRUE(is_min_heap(&heap));
    EXPECT_EQ(heap.nodes[0].val, 5); // The new minimum should be at the top.

    // --- Test an update that causes a heap_down (increase key) ---
    // Find the item with value 20 (original index 1).
    size_t pos_of_20 = external_indices[1];
    HeapNode updated_node_down = {99, &external_indices[1]}; // New value is 99
    heap_upsert(&heap, pos_of_20, updated_node_down);
    values[1] = 99;

    ASSERT_TRUE(is_min_heap(&heap));

    // After all updates, re-verify all indices.
    for (int i = 0; i < num_items; ++i) {
        size_t pos_in_heap = external_indices[i];
        ASSERT_LT(pos_in_heap, heap.len);
        EXPECT_EQ(heap.nodes[pos_in_heap].val, values[i]);
    }
}

TEST_F(HeapTest, ReallocationOnInsert) {
    // Initial capacity is 4. This test will insert 5 items.
    // Note: The provided heap_upsert reallocs on every insert, not just when full.
    // This test verifies it works despite that inefficiency.
    size_t initial_cap = heap.cap;
    const int num_items = 5;
    external_indices.resize(num_items);
    std::vector<uint64_t> values = {50, 20, 80, 10, 40};

    for (int i = 0; i < num_items; ++i) {
        HeapNode node = {values[i], &external_indices[i]};
        heap_upsert(&heap, heap.len, node);
    }

    ASSERT_EQ(heap.len, num_items);
    // Capacity should have increased from the initial value.
    EXPECT_GT(heap.cap, initial_cap);
    EXPECT_TRUE(is_min_heap(&heap));
}

// The main function that runs all of the tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}