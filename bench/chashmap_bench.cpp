#include <benchmark/benchmark.h>
#include <mutex>
#include <thread>
#include <vector>

extern "C" {
#include "hashtable.h"
#include "utils.h"
}

// --- Test Setup ---

struct CTestEntry {
    HNode node;
    uint64_t key;
    uint64_t value;
};

static bool test_entry_eq(HNode *lhs, HNode *rhs) {
    const CTestEntry *le = container_of(lhs, CTestEntry, node);
    const CTestEntry *re = container_of(rhs, CTestEntry, node);
    return le->key == re->key;
}

static std::vector<CTestEntry *> entries;

bool catcher(HNode *node, void *arg) {
    entries.push_back(container_of(node, CTestEntry, node));
    return true;
}

// --- Benchmark Fixture ---

class CHashMapFixture : public benchmark::Fixture {
public:
    static CHMap *chm;
    static std::once_flag init_flag;

    void SetUp(const ::benchmark::State &state) override {
        std::call_once(init_flag, []() {
            chm = chm_new(nullptr);
            // Pre-fill the map to ensure there's data to delete
            const int num_initial_items = 200000;
            for (int i = 0; i < num_initial_items; ++i) {
                auto *entry = new CTestEntry();
                entry->key = i;
                entry->value = i;
                entry->node.hcode = int_hash_rapid(i);
                chm_insert(chm, &entry->node);
            }
        });
    }

    void TearDown(const ::benchmark::State &state) override {
        // In a real application, we would clean up. For a benchmark,
        // we can skip this to not pollute the results. The OS will reclaim memory.
        std::call_once(init_flag, []() {
            chm_foreach(chm, catcher, nullptr);
            for (auto entry: entries)
                delete entry;
            chm_clear(chm);
            free(chm);
        });
    }
};

CHMap *CHashMapFixture::chm = nullptr;
std::once_flag CHashMapFixture::init_flag;


BENCHMARK_F(CHashMapFixture, BM_CHMap_InsertDelete)(benchmark::State &state) {
    // Each thread will work on its own range of keys to ensure contention
    // is on the data structure, not on the test logic itself.
    const int items_per_thread = 20000;
    const int thread_id = state.thread_index();
    const uint64_t key_range_start = items_per_thread * thread_id;

    // The inserter threads will add keys starting from a high number
    // to avoid colliding with the pre-filled data or deleter threads.
    const uint64_t insert_key_offset = 10000000;

    for (auto _: state) {
        uint64_t key = key_range_start + (state.iterations() % items_per_thread);

        if (thread_id % 2 == 0) { // Deleter threads
            CTestEntry key_entry = {};
            key_entry.key = key;
            key_entry.node.hcode = int_hash_rapid(key);
            HNode *deleted = chm_delete(chm, &key_entry.node, &test_entry_eq);
            // In a real system, deleted nodes would be put on a garbage list.
            // For the benchmark, we can just delete it if found.
            if (deleted) {
                delete container_of(deleted, CTestEntry, node);
            }
        } else { // Inserter threads
            uint64_t new_key = key + insert_key_offset;
            auto *entry = new CTestEntry();
            entry->key = new_key;
            entry->value = new_key;
            entry->node.hcode = int_hash_rapid(new_key);
            chm_insert(chm, &entry->node);
        }
    }
}

// Run the benchmark with 8 threads
BENCHMARK_REGISTER_F(CHashMapFixture, BM_CHMap_InsertDelete)->Threads(8)->UseRealTime();


// A simpler benchmark for just insertions
BENCHMARK_F(CHashMapFixture, BM_CHMap_Insert)(benchmark::State &state) {
    const int thread_id = state.thread_index();
    const uint64_t key_offset = 20000000 + (thread_id * 100000);

    for (auto _: state) {
        uint64_t key = key_offset + state.iterations();
        auto *entry = new CTestEntry();
        entry->key = key;
        entry->value = key;
        entry->node.hcode = int_hash_rapid(key);
        chm_insert(chm, &entry->node);
    }
}
BENCHMARK_REGISTER_F(CHashMapFixture, BM_CHMap_Insert)->Threads(8)->UseRealTime();


BENCHMARK_MAIN();
