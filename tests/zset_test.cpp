//
// Created by poyehchen on 9/28/25.
//
#include "zset.h"
#include "utils.h"

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <algorithm>

// --- Test Fixture ---

class ZSetTest : public ::testing::Test {
protected:
    ZSet zset;

    void SetUp() override {
        zset_init(&zset);
    }

    void TearDown() override {
        // This is critical for preventing memory leaks. We must manually free
        // every ZNode, as well as the internal structures of the zset.

        zset_destroy(&zset);
    }

    // Helper to verify a node exists and has the correct score.
    void verify_node(const char *name, double expected_score) {
        ZNode *node = zset_lookup(&zset, name, strlen(name));
        ASSERT_NE(node, nullptr) << "Node '" << name << "' should exist.";
        EXPECT_EQ(node->score, expected_score);
        EXPECT_EQ(node->len, strlen(name));
        EXPECT_STREQ(node->name, name);
    }
};

// --- Test Cases ---

TEST_F(ZSetTest, InsertAndLookup) {
    bool is_new = zset_insert(&zset, "apple", 5, 100.0);
    ASSERT_TRUE(is_new);
    verify_node("apple", 100.0);

    // Look up a non-existent key
    ZNode *not_found = zset_lookup(&zset, "banana", 6);
    ASSERT_EQ(not_found, nullptr);
}

TEST_F(ZSetTest, InsertAndUpdate) {
    zset_insert(&zset, "apple", 5, 100.0);
    verify_node("apple", 100.0);

    // "Insert" the same key, which should trigger an update.
    bool is_new = zset_insert(&zset, "apple", 5, 200.0);
    ASSERT_FALSE(is_new); // Should return false for an update.

    // Verify the score was updated.
    verify_node("apple", 200.0);
}

TEST_F(ZSetTest, Delete) {
    zset_insert(&zset, "apple", 5, 100.0);
    zset_insert(&zset, "banana", 6, 200.0);

    // Find and delete "apple"
    ZNode *to_delete = zset_lookup(&zset, "apple", 5);
    ASSERT_NE(to_delete, nullptr);
    zset_delete(&zset, to_delete);

    // Verify it's gone
    ASSERT_EQ(zset_lookup(&zset, "apple", 5), nullptr);
    // Verify other nodes remain
    verify_node("banana", 200.0);
}

TEST_F(ZSetTest, ScoreAndLexicographicalOrder) {
    zset_insert(&zset, "c", 1, 20.0);
    zset_insert(&zset, "a", 1, 10.0);
    zset_insert(&zset, "b", 1, 20.0);
    zset_insert(&zset, "d", 1, 30.0);

    std::vector<std::string> ordered_names;
    SLNode *curr = zset.sl.head->next[0];
    while (curr) {
        ordered_names.push_back(container_of(curr, ZNode, tnode)->name);
        curr = curr->next[0];
    }

    // Expected order: score ascending, then name lexicographically ascending.
    std::vector<std::string> expected = {"a", "b", "c", "d"};
    ASSERT_EQ(ordered_names, expected);
}

TEST_F(ZSetTest, SeekGreaterOrEqual) {
    zset_insert(&zset, "a", 1, 10.0);
    zset_insert(&zset, "b", 1, 20.0);
    zset_insert(&zset, "c", 1, 20.0);
    zset_insert(&zset, "d", 1, 30.0);

    ZNode *found;

    // Seek to first element >= (15.0, "a") -> should be (20.0, "b")
    found = zset_seekge(&zset, 15.0, "a", 1);
    ASSERT_NE(found, nullptr);
    EXPECT_STREQ(found->name, "b");

    // Seek to first element >= (20.0, "b") -> should be (20.0, "b")
    found = zset_seekge(&zset, 20.0, "b", 1);
    ASSERT_NE(found, nullptr);
    EXPECT_STREQ(found->name, "b");

    // Seek to first element >= (20.0, "bb") -> should be (20.0, "c")
    found = zset_seekge(&zset, 20.0, "bb", 2);
    ASSERT_NE(found, nullptr);
    EXPECT_STREQ(found->name, "c");

    // Seek past the end
    found = zset_seekge(&zset, 40.0, "a", 1);
    ASSERT_EQ(found, nullptr);
}

TEST_F(ZSetTest, NodeOffset) {
    zset_insert(&zset, "a", 1, 10.0); // rank 1
    zset_insert(&zset, "b", 1, 20.0); // rank 2
    zset_insert(&zset, "c", 1, 30.0); // rank 3
    zset_insert(&zset, "d", 1, 40.0); // rank 4
    zset_insert(&zset, "e", 1, 50.0); // rank 5

    // Get a reference node ("c", rank 3)
    ZNode *ref_node = zset_lookup(&zset, "c", 1);
    ASSERT_NE(ref_node, nullptr);

    ZNode *target;

    // Positive offset
    target = znode_offset(&zset, ref_node, 2);
    ASSERT_NE(target, nullptr);
    EXPECT_STREQ(target->name, "e"); // rank 3 + 2 = 5

    // Negative offset
    target = znode_offset(&zset, ref_node, -1);
    ASSERT_NE(target, nullptr);
    EXPECT_STREQ(target->name, "b"); // rank 3 - 1 = 2

    // Zero offset
    target = znode_offset(&zset, ref_node, 0);
    ASSERT_NE(target, nullptr);
    EXPECT_EQ(target, ref_node);

    // Out of bounds (negative)
    target = znode_offset(&zset, ref_node, -3); // rank 3 - 3 = 0, invalid
    ASSERT_EQ(target, nullptr);

    // Out of bounds (positive)
    target = znode_offset(&zset, ref_node, 3); // rank 3 + 3 = 6, invalid
    ASSERT_EQ(target, nullptr);
}

// The main function that runs all of the tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
