//
// Created by poyehchen on 9/28/25.
//
#include "skiplist.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

// The container_of macro is essential for intrusive data structures.
// It gets the pointer to the parent struct from a pointer to one of its members.
#define container_of(ptr, T, member) ((T *) ((char *) (ptr) - offsetof(T, member)))

// --- Test Setup ---

// A simple struct that embeds the SLNode for testing purposes.
struct TestEntry {
    SLNode node;
    int key;
};

// Comparison function required by the skiplist API.
static int test_entry_cmp(SLNode *a, SLNode *b) {
    TestEntry *entry_a = container_of(a, TestEntry, node);
    TestEntry *entry_b = container_of(b, TestEntry, node);
    if (entry_a->key < entry_b->key)
        return -1;
    if (entry_a->key > entry_b->key)
        return 1;
    return 0;
}

// --- Test Fixture ---

class SkipListTest : public ::testing::Test {
protected:
    SkipList sl;
    // We must track all allocated nodes to prevent memory leaks.
    std::vector<TestEntry *> allocated_nodes;

    void SetUp() override { sl_init(&sl); }

    void TearDown() override {
        // Free the head node created by sl_init.
        free(sl.head);
        // Free all TestEntry nodes allocated during the tests.
        for (auto *entry: allocated_nodes) {
            delete entry;
        }
    }

    // Helper to create, track, and insert a new entry.
    void insert_new_entry(int key) {
        TestEntry *entry = new TestEntry();
        entry->key = key;
        allocated_nodes.push_back(entry);
        sl_insert(&sl, &entry->node, &test_entry_cmp);
    }
};

// --- Test Cases ---

TEST_F(SkipListTest, Initialization) {
    ASSERT_NE(sl.head, nullptr);
    EXPECT_EQ(sl.head->level, 1);
    EXPECT_EQ(sl.head->next[0], nullptr);
}

TEST_F(SkipListTest, InsertAndSearch) {
    insert_new_entry(100);

    // Create a dummy key for lookup
    TestEntry key_entry;
    key_entry.key = 100;

    SLNode *found = sl_search(&sl, &key_entry.node, &test_entry_cmp);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(container_of(found, TestEntry, node)->key, 100);

    // Search for a non-existent key
    key_entry.key = 999;
    found = sl_search(&sl, &key_entry.node, &test_entry_cmp);
    ASSERT_EQ(found, nullptr);
}

TEST_F(SkipListTest, InsertOutOfOrderAndVerifySorted) {
    insert_new_entry(50);
    insert_new_entry(20);
    insert_new_entry(80);
    insert_new_entry(10);
    insert_new_entry(90);

    // Traverse the base level (level 0) to check for sorted order.
    std::vector<int> found_keys;
    SLNode *curr = sl.head->next[0];
    while (curr) {
        found_keys.push_back(container_of(curr, TestEntry, node)->key);
        curr = curr->next[0];
    }

    std::vector<int> expected_keys = {10, 20, 50, 80, 90};
    EXPECT_EQ(found_keys, expected_keys);
}

TEST_F(SkipListTest, ReplaceNode) {
    TestEntry *old_entry = new TestEntry({{}, 100});
    allocated_nodes.push_back(old_entry);
    sl_insert(&sl, &old_entry->node, &test_entry_cmp);

    TestEntry *new_entry = new TestEntry({{}, 100});
    allocated_nodes.push_back(new_entry);

    SLNode *replaced_node = sl_insert(&sl, &new_entry->node, &test_entry_cmp);
    ASSERT_EQ(replaced_node, &old_entry->node);

    SLNode *found = sl_search(&sl, &new_entry->node, &test_entry_cmp);
    ASSERT_EQ(found, &new_entry->node);

    auto it = std::find(allocated_nodes.begin(), allocated_nodes.end(), old_entry);
    if (it != allocated_nodes.end()) {
        allocated_nodes.erase(it);
    }
    delete old_entry;
}

TEST_F(SkipListTest, DeleteNode) {
    insert_new_entry(10);
    insert_new_entry(20);
    insert_new_entry(30);

    TestEntry key_to_delete;
    key_to_delete.key = 20;
    SLNode *deleted_node = sl_delete(&sl, &key_to_delete.node, &test_entry_cmp);

    ASSERT_NE(deleted_node, nullptr);
    TestEntry *deleted_entry = container_of(deleted_node, TestEntry, node);
    EXPECT_EQ(deleted_entry->key, 20);

    SLNode *found = sl_search(&sl, &key_to_delete.node, &test_entry_cmp);
    ASSERT_EQ(found, nullptr);

    auto it = std::find(allocated_nodes.begin(), allocated_nodes.end(), deleted_entry);
    if (it != allocated_nodes.end()) {
        allocated_nodes.erase(it);
    }
    delete deleted_entry;
}

// --- NEW/UPDATED TEST CASES FOR RANK OPERATIONS ---

TEST_F(SkipListTest, RankOperations) {
    // Insert 5 nodes with known keys
    insert_new_entry(10); // rank 1
    insert_new_entry(20); // rank 2
    insert_new_entry(30); // rank 3
    insert_new_entry(40); // rank 4
    insert_new_entry(50); // rank 5

    // --- Test sl_get_rank ---
    TestEntry key_entry;
    key_entry.key = 10;
    EXPECT_EQ(sl_get_rank(&sl, &key_entry.node, &test_entry_cmp), 1);
    key_entry.key = 30;
    EXPECT_EQ(sl_get_rank(&sl, &key_entry.node, &test_entry_cmp), 3);
    key_entry.key = 50;
    EXPECT_EQ(sl_get_rank(&sl, &key_entry.node, &test_entry_cmp), 5);
    key_entry.key = 99; // Non-existent
    EXPECT_EQ(sl_get_rank(&sl, &key_entry.node, &test_entry_cmp), 0);

    // --- Test sl_lookup_by_rank ---
    SLNode *node = sl_lookup_by_rank(&sl, 1);
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(container_of(node, TestEntry, node)->key, 10);

    node = sl_lookup_by_rank(&sl, 4);
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(container_of(node, TestEntry, node)->key, 40);

    node = sl_lookup_by_rank(&sl, 6); // Out of bounds
    ASSERT_EQ(node, nullptr);
    node = sl_lookup_by_rank(&sl, 0); // Rank 0 is head
    ASSERT_EQ(node, sl.head);

    // --- Test rank updates after deletion ---
    key_entry.key = 20; // Delete node at rank 2
    SLNode *deleted_node = sl_delete(&sl, &key_entry.node, &test_entry_cmp);
    ASSERT_NE(deleted_node, nullptr);
    // Clean up memory for deleted node
    TestEntry *deleted_entry = container_of(deleted_node, TestEntry, node);
    auto it = std::find(allocated_nodes.begin(), allocated_nodes.end(), deleted_entry);
    if (it != allocated_nodes.end()) {
        allocated_nodes.erase(it);
    }
    delete deleted_entry;

    // Key 30 should now be at rank 2
    key_entry.key = 30;
    EXPECT_EQ(sl_get_rank(&sl, &key_entry.node, &test_entry_cmp), 2);

    // Looking up rank 2 should now return the node with key 30
    node = sl_lookup_by_rank(&sl, 2);
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(container_of(node, TestEntry, node)->key, 30);

    // Key 50 should now be at rank 4
    key_entry.key = 50;
    EXPECT_EQ(sl_get_rank(&sl, &key_entry.node, &test_entry_cmp), 4);
}


// The main function that runs all of the tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
