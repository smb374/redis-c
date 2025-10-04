//
// Created by poyehchen on 9/26/25.
//

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>

#include "hashtable.h"
#include "utils.h"

#define PARTIAL_REHASH_TRIGGER 65536

// --- Test Setup ---

// A simple struct that embeds the hnode_t for testing purposes.
struct TestEntry {
    HNode node;
    uint64_t key;
    uint64_t value;
};

// Equality comparison function required by the hash map API.
static bool test_entry_eq(HNode *lhs, HNode *rhs) {
    const TestEntry *le = container_of(lhs, TestEntry, node);
    const TestEntry *re = container_of(rhs, TestEntry, node);
    return le->key == re->key;
}

// Helper to create and insert a new entry.
static void insert_entry(HMap *hm, const uint64_t key, const uint64_t value) {
    auto *entry = new TestEntry();
    entry->key = key;
    entry->value = value;
    entry->node.hcode = int_hash_rapid(key);
    hm_insert(hm, &entry->node, test_entry_eq);
}

// --- Test Fixture ---

class HashMapTest : public ::testing::Test {
protected:
    HMap hm = {};

    // SetUp() is called before each test case.
    void SetUp() override {
        // Initialize the hashmap to a zeroed state.
        bzero(&hm, sizeof(hm));
    }

    // TearDown() is called after each test case.
    void TearDown() override {
        // IMPORTANT: Because this is an intrusive hash map, we are responsible
        // for freeing the memory of every TestEntry we allocated.
        // We must iterate through both tables and delete all nodes.
        HTable *tables[] = {&hm.newer, &hm.older};
        for (HTable *table: tables) {
            if (table->tab) {
                for (size_t i = 0; i <= table->mask; ++i) {
                    HNode *node = table->tab[i];
                    while (node) {
                        HNode *next = node->next; // Save next before delete
                        delete container_of(node, TestEntry, node);
                        node = next;
                    }
                }
            }
        }
        // Now it's safe to free the hash map's internal tables.
        hm_clear(&hm);
    }
};

// --- Test Cases ---

// Basic Operations

TEST_F(HashMapTest, InsertAndLookup) {
    ASSERT_EQ(hm_size(&hm), 0);

    insert_entry(&hm, 100, 1000);
    insert_entry(&hm, 200, 2000);

    ASSERT_EQ(hm_size(&hm), 2);

    // Create a dummy key for lookup
    TestEntry key_entry = {};
    key_entry.key = 100;
    key_entry.node.hcode = int_hash_rapid(100);

    HNode *found = hm_lookup(&hm, &key_entry.node, &test_entry_eq);
    ASSERT_NE(found, nullptr);

    const TestEntry *found_entry = container_of(found, TestEntry, node);
    EXPECT_EQ(found_entry->key, 100);
    EXPECT_EQ(found_entry->value, 1000);
}

TEST_F(HashMapTest, LookupNonExistent) {
    insert_entry(&hm, 100, 1000);

    TestEntry key_entry = {};
    key_entry.key = 999; // Key that doesn't exist
    key_entry.node.hcode = int_hash_rapid(999);

    HNode *found = hm_lookup(&hm, &key_entry.node, &test_entry_eq);
    ASSERT_EQ(found, nullptr);
}

TEST_F(HashMapTest, Delete) {
    insert_entry(&hm, 100, 1000);
    insert_entry(&hm, 200, 2000);
    ASSERT_EQ(hm_size(&hm), 2);

    TestEntry key_entry = {};
    key_entry.key = 100;
    key_entry.node.hcode = int_hash_rapid(100);

    // Delete the node
    HNode *deleted_node = hm_delete(&hm, &key_entry.node, &test_entry_eq);
    ASSERT_NE(deleted_node, nullptr);
    ASSERT_EQ(hm_size(&hm), 1);

    // Free the memory for the deleted entry
    delete container_of(deleted_node, TestEntry, node);

    // Verify it's gone
    HNode *found = hm_lookup(&hm, &key_entry.node, &test_entry_eq);
    ASSERT_EQ(found, nullptr);

    // Verify the other node is still there
    key_entry.key = 200;
    key_entry.node.hcode = int_hash_rapid(200);
    found = hm_lookup(&hm, &key_entry.node, &test_entry_eq);
    ASSERT_NE(found, nullptr);
}

TEST_F(HashMapTest, DeleteNonExistent) {
    insert_entry(&hm, 100, 1000);
    ASSERT_EQ(hm_size(&hm), 1);

    TestEntry key_entry = {};
    key_entry.key = 999; // Key that doesn't exist
    key_entry.node.hcode = int_hash_rapid(999);

    HNode *deleted_node = hm_delete(&hm, &key_entry.node, &test_entry_eq);
    ASSERT_EQ(deleted_node, nullptr);
    ASSERT_EQ(hm_size(&hm), 1); // Size should be unchanged
}

// Progressive Rehashing

TEST_F(HashMapTest, TriggersAndCompletesInstantRehash) {
    // Initial table capacity is 4. Rehash triggers at size 4 * 8 = 32.
    // Since 32 < REHASH_WORK (128), this rehash should finish instantly.
    constexpr size_t trigger_count = 4 * MAX_LOAD;

    for (uint64_t i = 0; i < trigger_count; ++i) {
        insert_entry(&hm, i, i * 10);
    }

    ASSERT_EQ(hm_size(&hm), trigger_count);
    // The key checks: verify the rehash happened and completed.
    ASSERT_EQ(hm.older.tab, nullptr); // Should be null because it finished.
    ASSERT_GT(hm.newer.mask, 3); // The new table should be larger.

    // Verify all data is still accessible.
    for (uint64_t i = 0; i < trigger_count; ++i) {
        TestEntry key_entry = {{nullptr, int_hash_rapid(i)}, i, 0};
        HNode *found = hm_lookup(&hm, &key_entry.node, &test_entry_eq);
        ASSERT_NE(found, nullptr) << "Failed to find key: " << i;
        EXPECT_EQ(container_of(found, TestEntry, node)->value, i * 10);
    }
}

TEST_F(HashMapTest, TriggersRehashing) {
    // Rehashing should trigger when size >= 4 * MAX_LOAD.
    constexpr size_t trigger_count = PARTIAL_REHASH_TRIGGER;

    for (uint64_t i = 0; i < trigger_count; ++i) {
        insert_entry(&hm, i, i * 10);
    }
    ASSERT_EQ(hm_size(&hm), trigger_count);

    // A rehash should have been triggered. `older` table must now exist.
    ASSERT_NE(hm.older.tab, nullptr);
    ASSERT_NE(hm.newer.tab, nullptr);
    EXPECT_GT(hm.newer.mask, hm.older.mask); // Newer table is larger
    EXPECT_GT(hm.older.size, 0); // Some items are still in the old table

    // Verify we can still find all the items
    for (uint64_t i = 0; i < trigger_count; ++i) {
        TestEntry key_entry = {};
        key_entry.key = i;
        key_entry.node.hcode = int_hash_rapid(i);
        HNode *found = hm_lookup(&hm, &key_entry.node, &test_entry_eq);
        ASSERT_NE(found, nullptr) << "Failed to find key: " << i;
        EXPECT_EQ(container_of(found, TestEntry, node)->value, i * 10);
    }
}

TEST_F(HashMapTest, CompletesRehashing) {
    constexpr size_t trigger_count = PARTIAL_REHASH_TRIGGER;
    for (uint64_t i = 0; i < trigger_count; ++i) {
        insert_entry(&hm, i, i * 10);
    }
    ASSERT_NE(hm.older.tab, nullptr);

    // Perform enough lookups to force the rehashing to complete.
    // Each lookup performs REHASH_WORK migrations.
    // Total nodes / REHASH_WORK should be enough iterations.
    constexpr size_t iterations_needed = (trigger_count / REHASH_WORK) + 5;
    for (size_t i = 0; i < iterations_needed; ++i) {
        TestEntry key_entry = {};
        key_entry.key = 0; // Just look up the same key repeatedly
        key_entry.node.hcode = int_hash_rapid(0);
        hm_lookup(&hm, &key_entry.node, &test_entry_eq);
    }

    // The older table should now be gone.
    ASSERT_EQ(hm.older.tab, nullptr);
    ASSERT_EQ(hm.older.size, 0);
    ASSERT_EQ(hm_size(&hm), trigger_count);

    // Verify we can still find all the items in the new, larger table
    for (uint64_t i = 0; i < trigger_count; ++i) {
        TestEntry key_entry = {};
        key_entry.key = i;
        key_entry.node.hcode = int_hash_rapid(i);
        HNode *found = hm_lookup(&hm, &key_entry.node, &test_entry_eq);
        ASSERT_NE(found, nullptr);
    }
}

TEST_F(HashMapTest, DeleteDuringRehashing) {
    constexpr size_t trigger_count = PARTIAL_REHASH_TRIGGER;
    for (uint64_t i = 0; i < trigger_count; ++i) {
        insert_entry(&hm, i, i * 10);
    }
    // Rehashing is now in progress
    ASSERT_NE(hm.older.tab, nullptr);
    const size_t initial_size = hm_size(&hm);

    // Delete half of the keys
    for (uint64_t i = 0; i < trigger_count; i += 2) {
        TestEntry key_entry = {};
        key_entry.key = i;
        key_entry.node.hcode = int_hash_rapid(i);
        HNode *deleted = hm_delete(&hm, &key_entry.node, &test_entry_eq);
        ASSERT_NE(deleted, nullptr);
        delete container_of(deleted, TestEntry, node);
    }

    ASSERT_EQ(hm_size(&hm), initial_size / 2);

    // Verify that the deleted keys are gone and the others remain
    for (uint64_t i = 0; i < trigger_count; ++i) {
        TestEntry key_entry = {};
        key_entry.key = i;
        key_entry.node.hcode = int_hash_rapid(i);
        HNode *found = hm_lookup(&hm, &key_entry.node, &test_entry_eq);
        if (i % 2 == 0) {
            ASSERT_EQ(found, nullptr) << "Key " << i << " should have been deleted.";
        } else {
            ASSERT_NE(found, nullptr) << "Key " << i << " should still exist.";
        }
    }
}

// The main function that runs all of the tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
