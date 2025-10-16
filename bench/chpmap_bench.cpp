#include <atomic>
#include <benchmark/benchmark.h>
#include <cstdio>
#include <random>
#include <thread>

#include "hpmap.h"
#include "qsbr.h"
#include "utils.h"

// --- Test Entry ---

struct TestEntry {
    BNode node;
    uint64_t key;
    uint64_t value;
};

static bool test_entry_eq(BNode *lhs, BNode *rhs) {
    if (!lhs || !rhs)
        return lhs == rhs;
    const TestEntry *le = container_of(lhs, TestEntry, node);
    const TestEntry *re = container_of(rhs, TestEntry, node);
    return le->key == re->key;
}

// --- Benchmark Fixture ---

class CHPMapFixture : public benchmark::Fixture {
public:
    static CHPMap *g_hpmap;
    static std::atomic<bool> g_initialized;
    static std::atomic<int> g_threads_finished;

    void SetUp(const ::benchmark::State &state) override {
        if (state.thread_index() == 0) {
            // First thread initializes the map
            g_threads_finished.store(0, std::memory_order_relaxed);
            qsbr_init(65536);
            qsbr_reg();
            g_hpmap = chpm_new(nullptr, 1 << 20); // Start with a 1M element capacity

            // Pre-populate with 500k entries for lookup/delete tests
            const uint64_t prefill_count = 500000;
            for (uint64_t i = 0; i < prefill_count; i++) {
                auto *entry = new TestEntry();
                entry->key = i;
                entry->value = i * 2;
                entry->node.hcode = int_hash_rapid(i);
                chpm_add(g_hpmap, &entry->node, test_entry_eq);
            }
            g_initialized.store(true, std::memory_order_release);
        } else {
            // Other threads wait for initialization to complete
            while (!g_initialized.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            qsbr_reg();
        }
        qsbr_quiescent();
    }

    void TearDown(const ::benchmark::State &state) override {
        qsbr_quiescent();
        qsbr_unreg();

        if (g_threads_finished.fetch_add(1, std::memory_order_acq_rel) + 1 == state.threads()) {
            // Last thread out cleans up all resources
            chpm_destroy(g_hpmap);
            g_hpmap = nullptr;
            g_initialized.store(false, std::memory_order_release);
            qsbr_destroy();
        }
    }
};

// Static member initialization
CHPMap *CHPMapFixture::g_hpmap = nullptr;
std::atomic<bool> CHPMapFixture::g_initialized{false};
std::atomic<int> CHPMapFixture::g_threads_finished{0};

// --- Pure Insert Benchmark ---

BENCHMARK_DEFINE_F(CHPMapFixture, BM_Insert)(benchmark::State &state) {
    const int thread_id = state.thread_index();
    const uint64_t keys_per_thread = 1000000;
    const uint64_t base_key = 1000000 + thread_id * keys_per_thread;
    uint64_t local_key = 0;

    for (auto _: state) {
        uint64_t key = base_key + local_key++;
        auto *entry = new TestEntry{{int_hash_rapid(key)}, key, key * 2};
        BNode *result = chpm_upsert(g_hpmap, &entry->node, test_entry_eq);
        if (result != &entry->node) {
            delete entry; // Node was not inserted
        }
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_REGISTER_F(CHPMapFixture, BM_Insert)->ThreadRange(1, 8)->UseRealTime();

// --- Pure Lookup Benchmark ---

BENCHMARK_DEFINE_F(CHPMapFixture, BM_Lookup)(benchmark::State &state) {
    std::mt19937 rng(state.thread_index());
    std::uniform_int_distribution<uint64_t> dist(0, 499999);

    for (auto _: state) {
        uint64_t key = dist(rng);
        TestEntry query{{int_hash_rapid(key)}, key, 0};
        BNode *found = chpm_lookup(g_hpmap, &query.node, test_entry_eq);
        benchmark::DoNotOptimize(found);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_REGISTER_F(CHPMapFixture, BM_Lookup)->ThreadRange(1, 8)->UseRealTime();

// --- Upsert Benchmark ---

BENCHMARK_DEFINE_F(CHPMapFixture, BM_Upsert)(benchmark::State &state) {
    std::mt19937 rng(state.thread_index());
    std::uniform_int_distribution<uint64_t> dist(0, 999999); // Larger key space

    for (auto _: state) {
        uint64_t key = dist(rng);
        auto *entry = new TestEntry{{int_hash_rapid(key)}, key, key * 2};
        BNode *result = chpm_upsert(g_hpmap, &entry->node, test_entry_eq);
        if (result != &entry->node) {
            delete entry; // Node was not inserted, delete it
        }
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_REGISTER_F(CHPMapFixture, BM_Upsert)->ThreadRange(1, 8)->UseRealTime();


// --- Mixed 80/20 (80% Lookup, 20% Insert) ---

BENCHMARK_DEFINE_F(CHPMapFixture, BM_Mixed_80Read_20Write)(benchmark::State &state) {
    const int thread_id = state.thread_index();
    std::mt19937 rng(thread_id);
    std::uniform_int_distribution<uint64_t> lookup_dist(0, 499999);
    std::uniform_int_distribution<int> op_dist(0, 99);

    const uint64_t keys_per_thread = 1000000;
    const uint64_t base_key = 10000000 + thread_id * keys_per_thread;
    uint64_t local_key = 0;

    for (auto _: state) {
        int op = op_dist(rng);
        if (op < 80) {
            uint64_t key = lookup_dist(rng);
            TestEntry query{{int_hash_rapid(key)}, key, 0};
            BNode *found = chpm_lookup(g_hpmap, &query.node, test_entry_eq);
            benchmark::DoNotOptimize(found);
        } else {
            uint64_t key = base_key + local_key++;
            auto *entry = new TestEntry{{int_hash_rapid(key)}, key, key * 2};
            BNode *result = chpm_upsert(g_hpmap, &entry->node, test_entry_eq);
            if (result != &entry->node) {
                delete entry; // Node was not inserted
            }
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_REGISTER_F(CHPMapFixture, BM_Mixed_80Read_20Write)->ThreadRange(1, 8)->UseRealTime();


// --- Mixed 80/10/10 (80% Lookup, 10% Insert, 10% Delete) ---

BENCHMARK_DEFINE_F(CHPMapFixture, BM_Mixed_CRUD)(benchmark::State &state) {
    const int thread_id = state.thread_index();
    std::mt19937 rng(thread_id);
    std::uniform_int_distribution<uint64_t> key_dist(0, 499999);
    std::uniform_int_distribution<int> op_dist(0, 99);

    const uint64_t keys_per_thread = 1000000;
    const uint64_t base_key = 30000000 + thread_id * keys_per_thread;
    uint64_t local_key = 0;

    for (auto _: state) {
        int op = op_dist(rng);
        uint64_t key = key_dist(rng);

        if (op < 80) {
            TestEntry query{{int_hash_rapid(key)}, key, 0};
            BNode *found = chpm_lookup(g_hpmap, &query.node, test_entry_eq);
            benchmark::DoNotOptimize(found);
        } else if (op < 90) {
            TestEntry query{{int_hash_rapid(key)}, key, 0};
            chpm_remove(g_hpmap, &query.node, test_entry_eq);
        } else {
            uint64_t insert_key = base_key + local_key++;
            auto *entry = new TestEntry{{int_hash_rapid(insert_key)}, insert_key, insert_key * 2};
            BNode *result = chpm_upsert(g_hpmap, &entry->node, test_entry_eq);
            if (result != &entry->node) {
                delete entry; // Node was not inserted
            }
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_REGISTER_F(CHPMapFixture, BM_Mixed_CRUD)->ThreadRange(1, 8)->UseRealTime();


BENCHMARK_MAIN();
