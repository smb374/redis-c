#include <gtest/gtest.h>
#include <vector>

extern "C" {
#include "leapfrog.h"
#include "utils.h"
}

struct TestNode {
    LFNode lf_node;
    int key;
    int value;
};

static bool test_node_eq(LFNode *a, LFNode *b) {
    if (!a || !b) {
        return a == b;
    }
    TestNode *ta = (TestNode *) ((char *) a - offsetof(TestNode, lf_node));
    TestNode *tb = (TestNode *) ((char *) b - offsetof(TestNode, lf_node));
    return ta->key == tb->key;
}

class LeapfrogTest : public ::testing::Test {
protected:
    void SetUp() override {
        lfm = lfm_new(nullptr, 16);
        ASSERT_NE(lfm, nullptr);
    }

    void TearDown() override { lfm_destroy(lfm); }

    LFMap *lfm;
};

TEST_F(LeapfrogTest, NewAndDestroy) {
    // Setup and teardown do the work.
    SUCCEED();
}

TEST_F(LeapfrogTest, UpsertAndLookup) {
    TestNode n1 = {.lf_node = {.hcode = int_hash_rapid(1) + 1}, .key = 1, .value = 100};
    TestNode n2 = {.lf_node = {.hcode = int_hash_rapid(2) + 1}, .key = 2, .value = 200};

    // Insert
    LFNode *res = lfm_upsert(lfm, &n1.lf_node, test_node_eq);
    ASSERT_EQ(res, &n1.lf_node);
    ASSERT_EQ(lfm_size(lfm), 1);

    res = lfm_upsert(lfm, &n2.lf_node, test_node_eq);
    ASSERT_EQ(res, &n2.lf_node);
    ASSERT_EQ(lfm_size(lfm), 2);

    // Lookup
    TestNode k1 = {.lf_node = {.hcode = int_hash_rapid(1) + 1}, .key = 1};
    res = lfm_lookup(lfm, &k1.lf_node, test_node_eq);
    ASSERT_NE(res, nullptr);
    TestNode *found1 = (TestNode *) ((char *) res - offsetof(TestNode, lf_node));
    EXPECT_EQ(found1->key, 1);
    EXPECT_EQ(found1->value, 100);

    TestNode k2 = {.lf_node = {.hcode = int_hash_rapid(2) + 1}, .key = 2};
    res = lfm_lookup(lfm, &k2.lf_node, test_node_eq);
    ASSERT_NE(res, nullptr);
    TestNode *found2 = (TestNode *) ((char *) res - offsetof(TestNode, lf_node));
    EXPECT_EQ(found2->key, 2);
    EXPECT_EQ(found2->value, 200);

    // Lookup non-existent
    TestNode k3 = {.lf_node = {.hcode = int_hash_rapid(3) + 1}, .key = 3};
    res = lfm_lookup(lfm, &k3.lf_node, test_node_eq);
    EXPECT_EQ(res, nullptr);
}

TEST_F(LeapfrogTest, UpsertExisting) {
    TestNode n1 = {.lf_node = {.hcode = int_hash_rapid(1) + 1}, .key = 1, .value = 100};
    lfm_upsert(lfm, &n1.lf_node, test_node_eq);
    ASSERT_EQ(lfm_size(lfm), 1);

    TestNode n1_update = {.lf_node = {.hcode = int_hash_rapid(1) + 1}, .key = 1, .value = 101};
    LFNode *res = lfm_upsert(lfm, &n1_update.lf_node, test_node_eq);

    ASSERT_EQ(res, &n1.lf_node);
    ASSERT_EQ(lfm_size(lfm), 1);

    // Verify the original value is unchanged, as upsert doesn't replace
    TestNode k1 = {.lf_node = {.hcode = int_hash_rapid(1) + 1}, .key = 1};
    LFNode *lookup_res = lfm_lookup(lfm, &k1.lf_node, test_node_eq);
    TestNode *found1 = (TestNode *) ((char *) lookup_res - offsetof(TestNode, lf_node));
    EXPECT_EQ(found1->value, 100);
}

TEST_F(LeapfrogTest, Remove) {
    TestNode n1 = {.lf_node = {.hcode = int_hash_rapid(1) + 1}, .key = 1, .value = 100};
    lfm_upsert(lfm, &n1.lf_node, test_node_eq);
    ASSERT_EQ(lfm_size(lfm), 1);

    // Remove
    TestNode k1 = {.lf_node = {.hcode = int_hash_rapid(1) + 1}, .key = 1};
    LFNode *removed = lfm_remove(lfm, &k1.lf_node, test_node_eq);
    ASSERT_EQ(removed, &n1.lf_node);
    ASSERT_EQ(lfm_size(lfm), 0);

    // Lookup should fail
    LFNode *res = lfm_lookup(lfm, &k1.lf_node, test_node_eq);
    EXPECT_EQ(res, nullptr);

    // Removing again should fail
    removed = lfm_remove(lfm, &k1.lf_node, test_node_eq);
    EXPECT_EQ(removed, nullptr);
}

TEST_F(LeapfrogTest, FillTable) {
    const int capacity = 65536; // Initial size
    std::vector<TestNode> nodes;
    nodes.reserve(capacity * 2);

    // Insert up to capacity. This should work.
    for (int i = 0; i < capacity; ++i) {
        nodes.emplace_back(TestNode{.lf_node = {.hcode = int_hash_rapid(i) + 1}, .key = i, .value = i * 10});
        LFNode *res = lfm_upsert(lfm, &nodes.back().lf_node, test_node_eq);
        ASSERT_EQ(res, &nodes.back().lf_node) << "Failed at index " << i;
        ASSERT_EQ(lfm_size(lfm), i + 1);
    }

    // Verify all nodes are there
    for (int i = 0; i < capacity; ++i) {
        TestNode key = {.lf_node = {.hcode = int_hash_rapid(i) + 1}, .key = i};
        LFNode *res = lfm_lookup(lfm, &key.lf_node, test_node_eq);
        ASSERT_NE(res, nullptr) << "Failed to find key " << i;
        TestNode *found = (TestNode *) ((char *) res - offsetof(TestNode, lf_node));
        EXPECT_EQ(found->value, i * 10);
    }

    nodes.emplace_back(
            TestNode{.lf_node = {.hcode = int_hash_rapid(capacity) + 1}, .key = capacity, .value = capacity * 10});
    LFNode *res = lfm_upsert(lfm, &nodes.back().lf_node, test_node_eq);
    EXPECT_NE(res, nullptr);
    EXPECT_EQ(lfm_size(lfm), capacity + 1);
}
