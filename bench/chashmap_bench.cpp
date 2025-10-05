#include <atomic>
#include <benchmark/benchmark.h>
#include <random>
#include <thread>

extern "C" {
#include "hashtable.h"
#include "utils.h"
}

// --- Test Entry ---

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

// --- Benchmark Fixture ---

class CHashMapFixture : public benchmark::Fixture {
public:
    static CHMap *g_chmap;
    static std::atomic<bool> g_initialized;
    static std::atomic<int> g_init_complete;
    static std::atomic<int> g_thread_count;
    static std::atomic<int> g_teardown_count;

    void SetUp(const ::benchmark::State &state) override {
        // First thread initializes the map
        bool expected = false;
        if (g_initialized.compare_exchange_strong(expected, true)) {
            g_chmap = chm_new(nullptr);

            // Pre-populate with 500k entries for lookup/delete tests
            const uint64_t prefill_count = 500000;
            for (uint64_t i = 0; i < prefill_count; i++) {
                auto *entry = new CTestEntry();
                entry->key = i;
                entry->value = i * 2;
                entry->node.hcode = int_hash_rapid(i);
                chm_insert(g_chmap, &entry->node, test_entry_eq);
            }

            // Signal that initialization is complete
            g_init_complete.store(1, std::memory_order_release);
        } else {
            // Other threads wait for initialization to complete
            while (g_init_complete.load(std::memory_order_acquire) == 0) {
                std::this_thread::yield();
            }
        }

        // All threads register with the map for QSBR
        chm_register(g_chmap);

        // Track active threads
        g_thread_count.fetch_add(1, std::memory_order_relaxed);
    }

    void TearDown(const ::benchmark::State &state) override {
        // Last thread cleans up the map to prevent QSBR overflow
        int remaining = g_thread_count.fetch_sub(1, std::memory_order_acq_rel) - 1;

        if (remaining == 0) {
            // Reset for next benchmark
            g_teardown_count.fetch_add(1, std::memory_order_relaxed);

            // Clear the map to reset QSBR state
            chm_clear(g_chmap);
            g_chmap = nullptr;

            // Reset initialization flags for next benchmark
            g_initialized.store(false, std::memory_order_release);
            g_init_complete.store(0, std::memory_order_release);
        }
    }
};

// Static member initialization
CHMap *CHashMapFixture::g_chmap = nullptr;
std::atomic<bool> CHashMapFixture::g_initialized{false};
std::atomic<int> CHashMapFixture::g_init_complete{0};
std::atomic<int> CHashMapFixture::g_thread_count{0};
std::atomic<int> CHashMapFixture::g_teardown_count{0};

// --- Pure Insert Benchmark ---

BENCHMARK_DEFINE_F(CHashMapFixture, BM_Insert)(benchmark::State &state) {
    const int thread_id = state.thread_index();

    // Reserve key space for this thread to avoid conflicts
    const uint64_t keys_per_thread = 1000000;
    const uint64_t base_key = 1000000 + thread_id * keys_per_thread;
    uint64_t local_key = 0;

    for (auto _: state) {
        uint64_t key = base_key + local_key++;

        auto *entry = new CTestEntry();
        entry->key = key;
        entry->value = key * 2;
        entry->node.hcode = int_hash_rapid(key);

        bool success = chm_insert(g_chmap, &entry->node, test_entry_eq);
        benchmark::DoNotOptimize(success);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(CHashMapFixture, BM_Insert)->ThreadRange(1, 8)->UseRealTime();

// --- Pure Lookup Benchmark ---

BENCHMARK_DEFINE_F(CHashMapFixture, BM_Lookup)(benchmark::State &state) {
    std::mt19937 rng(state.thread_index());
    std::uniform_int_distribution<uint64_t> dist(0, 499999);

    for (auto _: state) {
        uint64_t key = dist(rng);

        CTestEntry query;
        query.key = key;
        query.node.hcode = int_hash_rapid(key);

        HNode *result = chm_lookup(g_chmap, &query.node, test_entry_eq);
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(CHashMapFixture, BM_Lookup)->ThreadRange(1, 8)->UseRealTime();

// --- Pure Delete Benchmark ---

BENCHMARK_DEFINE_F(CHashMapFixture, BM_Delete)(benchmark::State &state) {
    std::mt19937 rng(state.thread_index());
    std::uniform_int_distribution<uint64_t> dist(0, 499999);

    for (auto _: state) {
        uint64_t key = dist(rng);

        CTestEntry query;
        query.key = key;
        query.node.hcode = int_hash_rapid(key);

        HNode *deleted = chm_delete(g_chmap, &query.node, test_entry_eq);

        // Re-insert immediately to keep map populated
        if (deleted) {
            chm_insert(g_chmap, deleted, test_entry_eq);
        } else {
            auto *entry = new CTestEntry();
            entry->key = key;
            entry->value = key * 2;
            entry->node.hcode = int_hash_rapid(key);
            chm_insert(g_chmap, &entry->node, test_entry_eq);
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(CHashMapFixture, BM_Delete)->ThreadRange(1, 8)->UseRealTime();

// --- Mixed 80/20 (80% Lookup, 20% Insert) ---

BENCHMARK_DEFINE_F(CHashMapFixture, BM_Mixed_80Read_20Write)(benchmark::State &state) {
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
            // Lookup (80%)
            uint64_t key = lookup_dist(rng);
            CTestEntry query;
            query.key = key;
            query.node.hcode = int_hash_rapid(key);
            HNode *result = chm_lookup(g_chmap, &query.node, test_entry_eq);
            benchmark::DoNotOptimize(result);
        } else {
            // Insert (20%)
            uint64_t key = base_key + local_key++;
            auto *entry = new CTestEntry();
            entry->key = key;
            entry->value = key * 2;
            entry->node.hcode = int_hash_rapid(key);
            bool success = chm_insert(g_chmap, &entry->node, test_entry_eq);
            benchmark::DoNotOptimize(success);
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(CHashMapFixture, BM_Mixed_80Read_20Write)->ThreadRange(1, 8)->UseRealTime();

// --- Mixed 50/50 (50% Lookup, 50% Insert) ---

BENCHMARK_DEFINE_F(CHashMapFixture, BM_Mixed_50Read_50Write)(benchmark::State &state) {
    const int thread_id = state.thread_index();
    std::mt19937 rng(thread_id);
    std::uniform_int_distribution<uint64_t> lookup_dist(0, 499999);
    std::uniform_int_distribution<int> op_dist(0, 99);

    const uint64_t keys_per_thread = 1000000;
    const uint64_t base_key = 20000000 + thread_id * keys_per_thread;
    uint64_t local_key = 0;

    for (auto _: state) {
        int op = op_dist(rng);

        if (op < 50) {
            // Lookup (50%)
            uint64_t key = lookup_dist(rng);
            CTestEntry query;
            query.key = key;
            query.node.hcode = int_hash_rapid(key);
            HNode *result = chm_lookup(g_chmap, &query.node, test_entry_eq);
            benchmark::DoNotOptimize(result);
        } else {
            // Insert (50%)
            uint64_t key = base_key + local_key++;
            auto *entry = new CTestEntry();
            entry->key = key;
            entry->value = key * 2;
            entry->node.hcode = int_hash_rapid(key);
            bool success = chm_insert(g_chmap, &entry->node, test_entry_eq);
            benchmark::DoNotOptimize(success);
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(CHashMapFixture, BM_Mixed_50Read_50Write)->ThreadRange(1, 8)->UseRealTime();

// --- Mixed 80/10/10 (80% Lookup, 10% Insert, 10% Delete) ---

BENCHMARK_DEFINE_F(CHashMapFixture, BM_Mixed_CRUD)(benchmark::State &state) {
    const int thread_id = state.thread_index();
    const int num_threads = state.threads();
    std::mt19937 rng(thread_id);
    std::uniform_int_distribution<uint64_t> key_dist(0, 499999);
    std::uniform_int_distribution<int> op_dist(0, 99);

    const uint64_t keys_per_thread = 1000000;
    const uint64_t base_key = 30000000 + thread_id * keys_per_thread;
    uint64_t local_key = 0;
    std::vector<std::vector<CTestEntry *>> dentries(num_threads);

    for (auto _: state) {
        int op = op_dist(rng);
        uint64_t key = key_dist(rng);

        if (op < 80) {
            // Lookup (80%)
            CTestEntry query;
            query.key = key;
            query.node.hcode = int_hash_rapid(key);
            HNode *result = chm_lookup(g_chmap, &query.node, test_entry_eq);
            benchmark::DoNotOptimize(result);
        } else if (op < 90) {
            // Delete (10%)
            CTestEntry query;
            query.key = key;
            query.node.hcode = int_hash_rapid(key);
            HNode *deleted = chm_delete(g_chmap, &query.node, test_entry_eq);

            // QSBR handles reclamation, but we track for manual cleanup
            state.PauseTiming();
            if (deleted) {
                dentries[thread_id].push_back(container_of(deleted, CTestEntry, node));
            }
            state.ResumeTiming();
        } else {
            // Insert (10%)
            uint64_t insert_key = base_key + local_key++;
            auto *entry = new CTestEntry();
            entry->key = insert_key;
            entry->value = insert_key * 2;
            entry->node.hcode = int_hash_rapid(insert_key);
            bool success = chm_insert(g_chmap, &entry->node, test_entry_eq);
            benchmark::DoNotOptimize(success);
        }
    }

    state.SetItemsProcessed(state.iterations());
    for (auto &v: dentries) {
        for (auto d: v) {
            delete d;
        }
    }
}

BENCHMARK_REGISTER_F(CHashMapFixture, BM_Mixed_CRUD)->ThreadRange(1, 8)->UseRealTime();

// --- Contended Workload ---

BENCHMARK_DEFINE_F(CHashMapFixture, BM_Contended_Insert)(benchmark::State &state) {
    std::mt19937 rng(state.thread_index());
    // Only 10k possible keys - high contention!
    std::uniform_int_distribution<uint64_t> dist(1000000, 1009999);

    for (auto _: state) {
        uint64_t key = dist(rng);

        auto *entry = new CTestEntry();
        entry->key = key;
        entry->value = key * 2;
        entry->node.hcode = int_hash_rapid(key);

        bool success = chm_insert(g_chmap, &entry->node, test_entry_eq);
        benchmark::DoNotOptimize(success);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(CHashMapFixture, BM_Contended_Insert)->ThreadRange(1, 8)->UseRealTime();

BENCHMARK_MAIN();
