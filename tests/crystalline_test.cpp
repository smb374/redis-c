#include <atomic>
#include <cstdio>
#include <gtest/gtest.h>
#include <pthread.h>
#include <random>
#include <vector>
#include "utils.h"

// The Crystalline API is pure C, so we wrap it
extern "C" {
#include "crystalline.h"
}

// Test configuration
#define NUM_THREADS 8
#define ITERATIONS_PER_THREAD 100000
#define PUSH_RATIO 0.5 // 50% pushes, 50% pops

// The node for our lock-free stack.
// IMPORTANT: The Crystalline Node header is allocated *before* this struct.
struct StackNode {
    int value;
    std::atomic<StackNode *> next;
};

// The global head of our lock-free stack
static std::atomic<StackNode *> g_stack_head;

// Pushes a value onto the stack
void push(int value) {
    // Allocate a new node using the Crystalline garbage collector
    StackNode *node = (StackNode *) gc_alloc(sizeof(StackNode));
    ASSERT_NE(node, nullptr);
    node->value = value;

    StackNode *old_head = g_stack_head.load(std::memory_order_relaxed);
    do {
        node->next.store(old_head, std::memory_order_relaxed);
    } while (!g_stack_head.compare_exchange_weak(old_head, node, std::memory_order_release, std::memory_order_acquire));
}

// Pops a value from the stack
int pop() {
    // The protection index for gc_protect. We only need one for this simple case.
    const int protect_idx = 0;
    StackNode *head;

    for (;;) {
        // CRITICAL: Protect the head pointer before we dereference it.
        // gc_protect ensures that even if another thread pops this node and retires it,
        // the memory will not be freed while we hold this protection.
        head = (StackNode *) gc_protect((void **) &g_stack_head, protect_idx);

        if (head == nullptr) {
            return -1; // Stack is empty
        }

        // Now it is safe to dereference the protected head pointer.
        StackNode *next = head->next.load(std::memory_order_acquire);

        // Try to swing the head pointer to the next node.
        if (g_stack_head.compare_exchange_weak(head, next, std::memory_order_release, std::memory_order_acquire)) {
            // Success! The 'head' node is now logically removed from the stack.
            // We can now retire it, telling the GC it's safe to free when no
            // other threads are protecting it.
            int value = head->value;
            gc_retire(head);
            return value;
        }
        // If CMPXCHG failed, another thread modified the stack. Loop and retry.
    }
}

// A struct to hold thread-specific results
struct ThreadResult {
    long pushes = 0;
    long pops = 0;
};

// Worker function for each thread
void *thread_func(void *arg) {
    auto *result = static_cast<ThreadResult *>(arg);

    // Register the thread with the Crystalline garbage collector
    gc_reg();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    for (int i = 0; i < ITERATIONS_PER_THREAD; ++i) {
        if (dis(gen) < PUSH_RATIO) {
            push(i);
            result->pushes++;
        } else {
            if (pop() != -1) {
                result->pops++;
            }
        }
    }

    // Unregister the thread before it exits
    gc_unreg();
    return nullptr;
}


class CrystallineTest : public ::testing::Test {
protected:
    void SetUp() override {
        gc_init();
        g_stack_head.store(nullptr, std::memory_order_relaxed);
    }
};

TEST_F(CrystallineTest, ConcurrentPushPop) {
    std::vector<pthread_t> threads(NUM_THREADS);
    std::vector<ThreadResult> results(NUM_THREADS);

    for (int i = 0; i < NUM_THREADS; ++i) {
        pthread_create(&threads[i], nullptr, thread_func, &results[i]);
    }

    long total_pushes = 0;
    long total_pops = 0;

    for (int i = 0; i < NUM_THREADS; ++i) {
        pthread_join(threads[i], nullptr);
        total_pushes += results[i].pushes;
        total_pops += results[i].pops;
    }

    // After all threads are done, drain the stack to verify the final count.
    // This part is single-threaded, so we don't need gc_protect.
    long final_pop_count = 0;
    for (;;) {
        StackNode *head = g_stack_head.load(std::memory_order_acquire);
        if (head == nullptr)
            break;
        StackNode *next = head->next.load(std::memory_order_relaxed);
        if (g_stack_head.compare_exchange_strong(head, next, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            final_pop_count++;
            // We don't free here because the test is over and the OS will reclaim.
        }
    }

    // Final verification: the number of items left on the stack should be pushes - pops.
    ASSERT_EQ(final_pop_count, total_pushes - total_pops);
}
