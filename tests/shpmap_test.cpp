#include <cstdio>
#include <gtest/gtest.h>
#include <set>

extern "C" {
#include "hpmap.h"
#include "utils.h"
}

// --- Globals and Test Entry ---

// Global pointer to the map for easy access in a debugger.
SHPMap *g_shp_map = nullptr;

struct SHPMapTestEntry {
    BNode node;
    uint64_t key;
    uint64_t value;
};

static bool test_entry_eq(BNode *lhs, BNode *rhs) {
    if (!lhs || !rhs)
        return lhs == rhs;
    const auto *le = container_of(lhs, SHPMapTestEntry, node);
    const auto *re = container_of(rhs, SHPMapTestEntry, node);
    return le->key == re->key;
}

// --- Test Fixture ---

class SHPMapTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use the global map pointer, start with a small size to test migration.
        g_shp_map = shpm_new(nullptr, 16);
    }

    void TearDown() override {
        // To properly clean up, we need to iterate and free the nodes,
        // as the map itself doesn't own them.
        shpm_foreach(
                g_shp_map,
                [](BNode *node, void *) {
                    free(container_of(node, SHPMapTestEntry, node));
                    return true;
                },
                nullptr);
        shpm_destroy(g_shp_map);
        g_shp_map = nullptr;
    }
};

// --- Tests ---

TEST_F(SHPMapTest, InitAndDestroy) {
    ASSERT_NE(g_shp_map, nullptr);
    ASSERT_TRUE(g_shp_map->is_alloc);
}

TEST_F(SHPMapTest, UpsertAndLookup) {
    auto *entry1 = new SHPMapTestEntry{{int_hash_rapid(10)}, 10, 100};
    auto *entry2 = new SHPMapTestEntry{{int_hash_rapid(20)}, 20, 200};

    // Insert new nodes
    BNode *res1 = shpm_upsert(g_shp_map, &entry1->node, test_entry_eq);
    BNode *res2 = shpm_upsert(g_shp_map, &entry2->node, test_entry_eq);

    ASSERT_EQ(res1, &entry1->node);
    ASSERT_EQ(res2, &entry2->node);

    // Lookup existing nodes
    SHPMapTestEntry query1{{int_hash_rapid(10)}, 10, 0};
    BNode *found1 = shpm_lookup(g_shp_map, &query1.node, test_entry_eq);
    ASSERT_NE(found1, nullptr);
    EXPECT_EQ(container_of(found1, SHPMapTestEntry, node)->value, 100);

    // Lookup non-existent node
    SHPMapTestEntry query3{{int_hash_rapid(30)}, 30, 0};
    BNode *found3 = shpm_lookup(g_shp_map, &query3.node, test_entry_eq);
    ASSERT_EQ(found3, nullptr);
}

TEST_F(SHPMapTest, UpsertFindsExisting) {
    auto *entry1 = new SHPMapTestEntry{{int_hash_rapid(10)}, 10, 100};
    shpm_upsert(g_shp_map, &entry1->node, test_entry_eq);

    auto *entry2_duplicate = new SHPMapTestEntry{{int_hash_rapid(10)}, 10, 999}; // Same key, different value
    BNode *res = shpm_upsert(g_shp_map, &entry2_duplicate->node, test_entry_eq);

    // Upsert on an existing key should return the EXISTING node.
    ASSERT_EQ(res, &entry1->node);

    // The new node was not inserted, so we are responsible for freeing it.
    delete entry2_duplicate;

    // Lookup should still find the original node with its original value.
    BNode *found = shpm_lookup(g_shp_map, &entry1->node, test_entry_eq);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(container_of(found, SHPMapTestEntry, node)->value, 100);
}

TEST_F(SHPMapTest, Remove) {
    auto *entry1 = new SHPMapTestEntry{{int_hash_rapid(10)}, 10, 100};
    shpm_upsert(g_shp_map, &entry1->node, test_entry_eq);

    // Remove existing
    SHPMapTestEntry query1{{int_hash_rapid(10)}, 10, 0};
    BNode *removed = shpm_remove(g_shp_map, &query1.node, test_entry_eq);
    ASSERT_EQ(removed, &entry1->node);
    free(container_of(removed, SHPMapTestEntry, node)); // Clean up removed node

    // Verify it's gone
    BNode *found = shpm_lookup(g_shp_map, &query1.node, test_entry_eq);
    ASSERT_EQ(found, nullptr);

    // Remove non-existent
    SHPMapTestEntry query2{{int_hash_rapid(20)}, 20, 0};
    removed = shpm_remove(g_shp_map, &query2.node, test_entry_eq);
    ASSERT_EQ(removed, nullptr);
}

TEST_F(SHPMapTest, Migration) {
    // A small table of size 16 will migrate after ~10 inserts.
    const int count = 256;
    for (int i = 0; i < count; ++i) {
        auto *entry = new SHPMapTestEntry{.node = {int_hash_rapid(i)}, .key = (uint64_t) i, .value = (uint64_t) i * 10};
        BNode *res = shpm_upsert(g_shp_map, &entry->node, test_entry_eq);
    }

    // After migration, check that all keys are still present.
    for (int i = 0; i < count; ++i) {
        SHPMapTestEntry query{.node = {int_hash_rapid(i)}, .key = (uint64_t) i, .value = 0};
        BNode *found = shpm_lookup(g_shp_map, &query.node, test_entry_eq);
        ASSERT_NE(found, nullptr) << "Failed to find key " << i << " after migration.";
        EXPECT_EQ(container_of(found, SHPMapTestEntry, node)->value, (uint64_t) i * 10);
    }
}

TEST_F(SHPMapTest, Foreach) {
    std::set<uint64_t> expected_keys;
    const int count = 256;

    for (int i = 0; i < count; ++i) {
        expected_keys.insert(i);
        auto *entry = new SHPMapTestEntry{{int_hash_rapid(i)}, (uint64_t) i, (uint64_t) i * 10};
        shpm_upsert(g_shp_map, &entry->node, test_entry_eq);
    }

    std::set<uint64_t> found_keys;
    bool result = shpm_foreach(
            g_shp_map,
            [](BNode *node, void *arg) {
                auto *found_keys_ptr = static_cast<std::set<uint64_t> *>(arg);
                found_keys_ptr->insert(container_of(node, SHPMapTestEntry, node)->key);
                return true;
            },
            &found_keys);

    ASSERT_TRUE(result);
    ASSERT_EQ(found_keys, expected_keys);

    // Test stopping early
    int iter_count = 0;
    result = shpm_foreach(
            g_shp_map,
            [](BNode *node, void *arg) {
                (*static_cast<int *>(arg))++;
                return false; // Stop after first element
            },
            &iter_count);

    ASSERT_FALSE(result);
    ASSERT_EQ(iter_count, 1);
}
