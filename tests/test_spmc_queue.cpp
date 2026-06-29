#include "spmc_queue.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

// ─── Test helpers ───────────────────────────────────────────────────────────

static std::atomic<int> tests_passed{0};
static std::atomic<int> tests_failed{0};

#define CHECK(cond, msg)                                                    \
    do {                                                                    \
        if (!(cond)) {                                                      \
            std::fprintf(stderr, "  FAIL: %s  (%s:%d)\n", msg, __FILE__,   \
                         __LINE__);                                         \
            tests_failed.fetch_add(1);                                      \
        } else {                                                            \
            tests_passed.fetch_add(1);                                      \
        }                                                                   \
    } while (0)

// ─── Tests ──────────────────────────────────────────────────────────────────

void test_single_consumer_basic() {
    std::printf("[test] single consumer basic push/pop ... ");
    SPMCQueue<int, 8> q;
    auto id = q.register_consumer();

    q.push(10);
    q.push(20);
    q.push(30);

    CHECK(q.pop(id) == 10, "first pop should be 10");
    CHECK(q.pop(id) == 20, "second pop should be 20");
    CHECK(q.pop(id) == 30, "third pop should be 30");
    std::printf("done\n");
}

void test_broadcast_to_multiple_consumers() {
    std::printf("[test] broadcast to 4 consumers ... ");
    SPMCQueue<int, 16, 4> q;

    auto c0 = q.register_consumer();
    auto c1 = q.register_consumer();
    auto c2 = q.register_consumer();
    auto c3 = q.register_consumer();

    constexpr int N = 10;
    for (int i = 0; i < N; ++i) {
        q.push(i);
    }

    // Every consumer should see every message, in order.
    bool all_correct = true;
    for (auto id : {c0, c1, c2, c3}) {
        for (int i = 0; i < N; ++i) {
            if (q.pop(id) != i) {
                all_correct = false;
            }
        }
    }
    CHECK(all_correct, "all 4 consumers should see 0..9 in order");
    std::printf("done\n");
}

void test_try_pop_when_empty() {
    std::printf("[test] try_pop when empty ... ");
    SPMCQueue<int, 4> q;
    auto id = q.register_consumer();

    auto result = q.try_pop(id);
    CHECK(!result.has_value(), "try_pop on empty queue should return nullopt");

    q.push(42);
    result = q.try_pop(id);
    CHECK(result.has_value() && *result == 42, "try_pop should return 42");
    std::printf("done\n");
}

void test_try_push_when_full() {
    std::printf("[test] try_push when full (slow consumer) ... ");
    SPMCQueue<int, 4> q;
    auto id = q.register_consumer();

    // Fill to capacity.
    const std::size_t cap = q.capacity();
    std::printf("(capacity=%zu) ", cap);
    for (std::size_t i = 0; i < cap; ++i) {
        CHECK(q.try_push(static_cast<int>(i)), "push within cap should work");
    }
    CHECK(!q.try_push(999), "push beyond capacity should fail");

    // Pop one to free a slot.
    q.pop(id);
    CHECK(q.try_push(888), "push after pop should succeed");
    std::printf("done\n");
}

void test_slow_consumer_blocks_producer() {
    std::printf("[test] slow consumer blocks producer ... ");
    SPMCQueue<int, 4, 2> q;

    auto fast = q.register_consumer();
    auto slow = q.register_consumer();

    const std::size_t cap = q.capacity();

    // Fill the queue.
    for (std::size_t i = 0; i < cap; ++i) {
        q.push(static_cast<int>(i));
    }

    // Fast consumer reads everything.
    for (std::size_t i = 0; i < cap; ++i) {
        q.pop(fast);
    }

    // Producer should STILL be blocked because slow hasn't read.
    CHECK(!q.try_push(999), "producer blocked by slow consumer");

    // Slow reads one — now producer can push one.
    q.pop(slow);
    CHECK(q.try_push(999), "producer unblocked after slow consumer pops");
    std::printf("done\n");
}

void test_consumer_registers_mid_stream() {
    std::printf("[test] consumer registers mid-stream ... ");
    SPMCQueue<int, 16> q;

    // Publish some messages before any consumer.
    q.push(100);
    q.push(200);
    q.push(300);

    // Now register — should NOT see the 3 old messages.
    auto id = q.register_consumer();
    auto result = q.try_pop(id);
    CHECK(!result.has_value(),
          "late consumer should not see pre-registration messages");

    // New messages should be visible.
    q.push(400);
    CHECK(q.pop(id) == 400, "late consumer should see post-registration msg");
    std::printf("done\n");
}

void test_no_consumers_producer_runs_free() {
    std::printf("[test] no consumers — producer never blocked ... ");
    SPMCQueue<int, 4> q;

    // Push more than capacity — should not block.
    for (int i = 0; i < 100; ++i) {
        CHECK(q.try_push(i), "producer should never be blocked with 0 consumers");
    }
    std::printf("done\n");
}

void test_threaded_spmc_broadcast() {
    std::printf("[test] threaded SPMC broadcast (4 consumers, 500K msgs) ... ");
    constexpr int N = 500'000;
    constexpr int num_consumers = 4;

    SPMCQueue<std::uint64_t, 1024, 8> q;

    // Register consumers before producer starts.
    std::vector<std::size_t> ids(num_consumers);
    for (int c = 0; c < num_consumers; ++c) {
        ids[c] = q.register_consumer();
    }

    // Producer
    std::thread producer([&] {
        for (int i = 0; i < N; ++i) {
            q.push(static_cast<std::uint64_t>(i));
        }
    });

    // Each consumer independently verifies FIFO order.
    std::vector<std::thread> consumers;
    std::atomic<int> order_failures{0};
    std::vector<std::uint64_t> sums(num_consumers, 0);

    for (int c = 0; c < num_consumers; ++c) {
        consumers.emplace_back([&, c] {
            for (int i = 0; i < N; ++i) {
                auto val = q.pop(ids[c]);
                sums[c] += val;
                if (val != static_cast<std::uint64_t>(i)) {
                    order_failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    producer.join();
    for (auto& t : consumers) t.join();

    CHECK(order_failures.load() == 0,
          "all consumers should see items in order");

    const std::uint64_t expected =
        static_cast<std::uint64_t>(N - 1) * N / 2;
    bool sums_ok = true;
    for (int c = 0; c < num_consumers; ++c) {
        if (sums[c] != expected) sums_ok = false;
    }
    CHECK(sums_ok, "each consumer's sum should match expected");
    std::printf("done\n");
}

// ─── Throughput benchmarks ──────────────────────────────────────────────────

void bench_spmc(int num_consumers) {
    constexpr int N = 10'000'000;

    std::printf("[bench] SPMC 1-producer / %d-consumer throughput ...\n",
                num_consumers);

    SPMCQueue<std::uint64_t, 1024, 8> q;

    std::vector<std::size_t> ids(num_consumers);
    for (int c = 0; c < num_consumers; ++c) {
        ids[c] = q.register_consumer();
    }

    auto start = std::chrono::high_resolution_clock::now();

    std::thread producer([&] {
        for (int i = 0; i < N; ++i) {
            q.push(static_cast<std::uint64_t>(i));
        }
    });

    std::vector<std::uint64_t> sums(num_consumers, 0);
    std::vector<std::thread> consumers;
    for (int c = 0; c < num_consumers; ++c) {
        consumers.emplace_back([&, c] {
            std::uint64_t local_sum = 0;
            for (int i = 0; i < N; ++i) {
                local_sum += q.pop(ids[c]);
            }
            sums[c] = local_sum;
        });
    }

    producer.join();
    for (auto& t : consumers) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    double ops_per_sec = N / (elapsed_ms / 1000.0);

    std::printf("  %d ops in %.2f ms  →  %.2f Mops/s\n", N, elapsed_ms,
                ops_per_sec / 1e6);

    const std::uint64_t expected =
        static_cast<std::uint64_t>(N - 1) * N / 2;
    bool ok = true;
    for (int c = 0; c < num_consumers; ++c) {
        if (sums[c] != expected) ok = false;
    }
    CHECK(ok, "all consumer sums should match");
}

// ─── Main ───────────────────────────────────────────────────────────────────

int main() {
    std::printf("═══ SPMCQueue Tests ═══\n\n");

    test_single_consumer_basic();
    test_broadcast_to_multiple_consumers();
    test_try_pop_when_empty();
    test_try_push_when_full();
    test_slow_consumer_blocks_producer();
    test_consumer_registers_mid_stream();
    test_no_consumers_producer_runs_free();
    test_threaded_spmc_broadcast();

    std::printf("\n");
    bench_spmc(1);
    bench_spmc(2);
    bench_spmc(4);
    bench_spmc(8);

    std::printf("\n═══ Results: %d passed, %d failed ═══\n",
                tests_passed.load(), tests_failed.load());

    return tests_failed.load() > 0 ? 1 : 0;
}
