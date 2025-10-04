#include <atomic>
#include <benchmark/benchmark.h>
#include <random>

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

// --- Global State ---
// We use a single global map that persists across benchmarks to avoid
// expensive setup/teardown between iterations

static CHMap *g_chmap = nullptr;
static std::atomic<bool> g_initialized{false};
static std::atomic<uint64_t> g_next_key{0};

// Initialize map once
static void InitializeMap() {
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

        g_next_key.store(prefill_count);
    }
}

// --- Pure Insert Benchmark ---
// Each thread inserts unique keys to minimize contention

static void BM_Insert(benchmark::State &state) {
    InitializeMap();

    const int thread_id = state.thread_index();
    const int num_threads = state.threads();

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
        // Should always be NULL (new insert) since keys are unique
        benchmark::DoNotOptimize(success);
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_Insert)->ThreadRange(1, 8)->UseRealTime();

// --- Pure Lookup Benchmark ---
// Look up keys from the pre-populated range

static void BM_Lookup(benchmark::State &state) {
    InitializeMap();

    // Use thread-local random to avoid contention on RNG
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

BENCHMARK(BM_Lookup)->ThreadRange(1, 8)->UseRealTime();

// --- Pure Delete Benchmark ---
// Delete and re-insert to maintain constant map size
// This avoids the "delete fails after first pass" problem

static void BM_Delete(benchmark::State &state) {
    InitializeMap();

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
            // Key wasn't present, insert a new one
            auto *entry = new CTestEntry();
            entry->key = key;
            entry->value = key * 2;
            entry->node.hcode = int_hash_rapid(key);
            chm_insert(g_chmap, &entry->node, test_entry_eq);
        }
    }

    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_Delete)->ThreadRange(1, 8)->UseRealTime();

// --- Mixed 80/20 (80% Lookup, 20% Insert) ---
// Read-heavy workload - typical for caches

static void BM_Mixed_80Read_20Write(benchmark::State &state) {
    InitializeMap();

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

BENCHMARK(BM_Mixed_80Read_20Write)->ThreadRange(1, 8)->UseRealTime();

// --- Mixed 50/50 (50% Lookup, 50% Insert) ---
// Balanced workload

static void BM_Mixed_50Read_50Write(benchmark::State &state) {
    InitializeMap();

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

BENCHMARK(BM_Mixed_50Read_50Write)->ThreadRange(1, 8)->UseRealTime();

// --- Mixed 80/10/10 (80% Lookup, 10% Insert, 10% Delete) ---
// Full CRUD workload with constant map size

static void BM_Mixed_CRUD(benchmark::State &state) {
    InitializeMap();

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

            // Re-insert to maintain map size
            state.PauseTiming();
            if (deleted) {
                chm_insert(g_chmap, deleted, test_entry_eq);
            } else {
                auto *entry = new CTestEntry();
                entry->key = key;
                entry->value = key * 2;
                entry->node.hcode = int_hash_rapid(key);
                chm_insert(g_chmap, &entry->node, test_entry_eq);
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
}

BENCHMARK(BM_Mixed_CRUD)->ThreadRange(1, 8)->UseRealTime();

// --- Contended Workload ---
// All threads fight over the same small key range

static void BM_Contended_Insert(benchmark::State &state) {
    InitializeMap();

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

BENCHMARK(BM_Contended_Insert)->ThreadRange(1, 8)->UseRealTime();

BENCHMARK_MAIN();
