/// Phase 4 — Benchmark harness.
///
/// Sweeps across:
///   - Queue type   : mutex (work-distribution), spsc, spmc (broadcast)
///   - Message size  : 8 / 64 / 256 bytes
///   - Consumer count: 1 / 2 / 4 / 8
///   - Queue depth   : 64 / 256 / 1024
///
/// Each message carries an rdtsc send-timestamp.  Consumers record
/// rdtsc() − msg.send_tsc to compute per-message latency.
/// Results are emitted as CSV for matplotlib plotting.

#include "mutex_queue.h"
#include "spsc_queue.h"
#include "spmc_queue.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

// ═══════════════════════════════════════════════════════════════════════════
// rdtsc — low-overhead timestamp counter
// ═══════════════════════════════════════════════════════════════════════════

#if defined(__x86_64__) || defined(_M_X64)
static inline std::uint64_t rdtsc_now() {
    std::uint32_t lo, hi;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return (static_cast<std::uint64_t>(hi) << 32) | lo;
}
#elif defined(__aarch64__)
static inline std::uint64_t rdtsc_now() {
    std::uint64_t val;
    asm volatile("mrs %0, cntvct_el0" : "=r"(val));
    return val;
}
#else
static inline std::uint64_t rdtsc_now() {
    return static_cast<std::uint64_t>(
        std::chrono::high_resolution_clock::now().time_since_epoch().count());
}
#endif

// ═══════════════════════════════════════════════════════════════════════════
// Message payloads (8 / 64 / 256 bytes)
// ═══════════════════════════════════════════════════════════════════════════

template <std::size_t Size> struct Message;

template <> struct Message<8> {
    std::uint64_t send_tsc{0};
};

template <> struct Message<64> {
    std::uint64_t send_tsc{0};
    char padding[56]{};
};

template <> struct Message<256> {
    std::uint64_t send_tsc{0};
    char padding[248]{};
};

static_assert(sizeof(Message<8>)   == 8);
static_assert(sizeof(Message<64>)  == 64);
static_assert(sizeof(Message<256>) == 256);

// ═══════════════════════════════════════════════════════════════════════════
// TSC calibration — convert ticks → nanoseconds
// ═══════════════════════════════════════════════════════════════════════════

static double g_tsc_to_ns = 1.0;

static void calibrate_tsc() {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::uint64_t tsc0 = rdtsc_now();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::uint64_t tsc1 = rdtsc_now();
    auto t1 = std::chrono::high_resolution_clock::now();

    double elapsed_ns =
        std::chrono::duration<double, std::nano>(t1 - t0).count();
    g_tsc_to_ns = elapsed_ns / static_cast<double>(tsc1 - tsc0);

    std::printf("TSC calibration: %.2f GHz  (%.4f ns/tick)\n",
                1.0 / g_tsc_to_ns, g_tsc_to_ns);
}

// ═══════════════════════════════════════════════════════════════════════════
// Latency statistics
// ═══════════════════════════════════════════════════════════════════════════

struct LatStats {
    double mean_ns  = 0;
    double p50_ns   = 0;
    double p99_ns   = 0;
    double min_ns   = 0;
    double max_ns   = 0;
};

static LatStats compute_stats(std::vector<std::uint64_t>& ticks) {
    if (ticks.empty()) return {};
    std::sort(ticks.begin(), ticks.end());

    double sum = 0;
    for (auto t : ticks) sum += static_cast<double>(t);

    auto to_ns = [](std::uint64_t t) { return static_cast<double>(t) * g_tsc_to_ns; };
    std::size_t n = ticks.size();

    return {
        (sum / static_cast<double>(n)) * g_tsc_to_ns,  // mean_ns
        to_ns(ticks[n / 2]),                             // p50_ns
        to_ns(ticks[n * 99 / 100]),                      // p99_ns
        to_ns(ticks.front()),                            // min_ns
        to_ns(ticks.back()),                             // max_ns
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// CSV helpers
// ═══════════════════════════════════════════════════════════════════════════

static void csv_header(FILE* f) {
    std::fprintf(f,
        "queue_type,msg_bytes,consumers,queue_depth,total_msgs,"
        "elapsed_ms,throughput_mops,"
        "lat_mean_ns,lat_p50_ns,lat_p99_ns,lat_min_ns,lat_max_ns\n");
}

static void csv_row(FILE* f, const char* qtype, int msg_bytes, int consumers,
                    int depth, int total_msgs, double elapsed_ms,
                    const LatStats& lat) {
    double throughput = static_cast<double>(total_msgs) / (elapsed_ms / 1000.0) / 1e6;

    std::fprintf(f,
        "%s,%d,%d,%d,%d,%.2f,%.4f,%.1f,%.1f,%.1f,%.1f,%.1f\n",
        qtype, msg_bytes, consumers, depth, total_msgs, elapsed_ms,
        throughput, lat.mean_ns, lat.p50_ns, lat.p99_ns,
        lat.min_ns, lat.max_ns);
    std::fflush(f);

    // Console progress
    std::printf("  %-8s  %3dB  %dc  depth=%-5d → %7.2f Mops/s  p50=%6.0fns\n",
                qtype, msg_bytes, consumers, depth, throughput, lat.p50_ns);
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: Mutex baseline (work-distribution among consumers)
// ═══════════════════════════════════════════════════════════════════════════

template <std::size_t MsgSize, std::size_t Depth>
void bench_mutex(int num_consumers, int num_msgs, FILE* csv) {
    using Msg = Message<MsgSize>;
    auto q = std::make_unique<MutexQueue<Msg, Depth>>();
    auto& queue = *q;

    std::vector<std::vector<std::uint64_t>> latencies(
        static_cast<std::size_t>(num_consumers));
    for (auto& v : latencies)
        v.reserve(static_cast<std::size_t>(num_msgs / num_consumers + 256));

    auto start = std::chrono::high_resolution_clock::now();

    // Producer
    std::thread producer([&] {
        for (int i = 0; i < num_msgs; ++i) {
            Msg msg{};
            msg.send_tsc = rdtsc_now();
            queue.push(std::move(msg));
        }
    });

    // Consumers — collectively consume all num_msgs messages
    std::atomic<int> consumed{0};
    std::vector<std::thread> consumers;
    for (int c = 0; c < num_consumers; ++c) {
        consumers.emplace_back([&, c] {
            while (consumed.load(std::memory_order_acquire) < num_msgs) {
                auto msg = queue.try_pop();
                if (msg.has_value()) {
                    latencies[static_cast<std::size_t>(c)].push_back(
                        rdtsc_now() - msg->send_tsc);
                    consumed.fetch_add(1, std::memory_order_release);
                }
            }
        });
    }

    producer.join();
    for (auto& t : consumers) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();

    // Merge latencies
    std::vector<std::uint64_t> all;
    for (auto& v : latencies) all.insert(all.end(), v.begin(), v.end());
    auto stats = compute_stats(all);

    csv_row(csv, "mutex", static_cast<int>(MsgSize), num_consumers,
            static_cast<int>(Depth), num_msgs, elapsed_ms, stats);
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: SPSC lock-free (1 producer, 1 consumer only)
// ═══════════════════════════════════════════════════════════════════════════

template <std::size_t MsgSize, std::size_t Depth>
void bench_spsc(int num_msgs, FILE* csv) {
    using Msg = Message<MsgSize>;
    auto q = std::make_unique<SPSCQueue<Msg, Depth>>();
    auto& queue = *q;

    std::vector<std::uint64_t> latencies;
    latencies.reserve(static_cast<std::size_t>(num_msgs));

    auto start = std::chrono::high_resolution_clock::now();

    std::thread producer([&] {
        for (int i = 0; i < num_msgs; ++i) {
            Msg msg{};
            msg.send_tsc = rdtsc_now();
            queue.push(std::move(msg));
        }
    });

    std::thread consumer([&] {
        for (int i = 0; i < num_msgs; ++i) {
            Msg msg = queue.pop();
            latencies.push_back(rdtsc_now() - msg.send_tsc);
        }
    });

    producer.join();
    consumer.join();

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();

    auto stats = compute_stats(latencies);
    csv_row(csv, "spsc", static_cast<int>(MsgSize), 1,
            static_cast<int>(Depth), num_msgs, elapsed_ms, stats);
}

// ═══════════════════════════════════════════════════════════════════════════
// Benchmark: SPMC broadcast (1 producer, N consumers — each sees every msg)
// ═══════════════════════════════════════════════════════════════════════════

template <std::size_t MsgSize, std::size_t Depth>
void bench_spmc(int num_consumers, int num_msgs, FILE* csv) {
    using Msg = Message<MsgSize>;
    auto q = std::make_unique<SPMCQueue<Msg, Depth, 8>>();
    auto& queue = *q;

    std::vector<std::size_t> ids(static_cast<std::size_t>(num_consumers));
    for (int c = 0; c < num_consumers; ++c) {
        ids[static_cast<std::size_t>(c)] = queue.register_consumer();
    }

    std::vector<std::vector<std::uint64_t>> latencies(
        static_cast<std::size_t>(num_consumers));
    for (auto& v : latencies)
        v.reserve(static_cast<std::size_t>(num_msgs));

    auto start = std::chrono::high_resolution_clock::now();

    std::thread producer([&] {
        for (int i = 0; i < num_msgs; ++i) {
            Msg msg{};
            msg.send_tsc = rdtsc_now();
            queue.push(msg);
        }
    });

    std::vector<std::thread> consumers;
    for (int c = 0; c < num_consumers; ++c) {
        consumers.emplace_back([&, c] {
            std::uint64_t recv_tsc;
            for (int i = 0; i < num_msgs; ++i) {
                Msg msg = queue.pop(ids[static_cast<std::size_t>(c)]);
                recv_tsc = rdtsc_now();
                latencies[static_cast<std::size_t>(c)].push_back(
                    recv_tsc - msg.send_tsc);
            }
        });
    }

    producer.join();
    for (auto& t : consumers) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();

    // Merge all consumer latencies
    std::vector<std::uint64_t> all;
    for (auto& v : latencies) all.insert(all.end(), v.begin(), v.end());
    auto stats = compute_stats(all);

    csv_row(csv, "spmc", static_cast<int>(MsgSize), num_consumers,
            static_cast<int>(Depth), num_msgs, elapsed_ms, stats);
}

// ═══════════════════════════════════════════════════════════════════════════
// Dispatch — instantiate all (MsgSize, Depth) combinations
// ═══════════════════════════════════════════════════════════════════════════

#define RUN_DEPTHS(fn, ms, ...)  \
    fn<ms,   64>(__VA_ARGS__);   \
    fn<ms,  256>(__VA_ARGS__);   \
    fn<ms, 1024>(__VA_ARGS__);

#define RUN_ALL_SIZES(fn, ...)      \
    RUN_DEPTHS(fn,   8, __VA_ARGS__) \
    RUN_DEPTHS(fn,  64, __VA_ARGS__) \
    RUN_DEPTHS(fn, 256, __VA_ARGS__)

static void run_all(int num_msgs, FILE* csv) {
    for (int nc : {1, 2, 4, 8}) {
        std::printf("\n══ %d consumer(s) ═══════════════════════════════════\n", nc);

        RUN_ALL_SIZES(bench_mutex, nc, num_msgs, csv)

        if (nc == 1) {
            RUN_ALL_SIZES(bench_spsc, num_msgs, csv)
        }

        RUN_ALL_SIZES(bench_spmc, nc, num_msgs, csv)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════

int main(int argc, char* argv[]) {
    int num_msgs = 1'000'000;
    const char* csv_path = "bench_results.csv";

    // Optional: override message count
    if (argc > 1) num_msgs = std::atoi(argv[1]);
    if (argc > 2) csv_path = argv[2];

    std::printf("Lock-Free Queue Benchmark\n");
    std::printf("  Messages per config : %d\n", num_msgs);
    std::printf("  Output              : %s\n\n", csv_path);

    calibrate_tsc();

    FILE* csv = std::fopen(csv_path, "w");
    if (!csv) {
        std::fprintf(stderr, "Cannot open %s for writing\n", csv_path);
        return 1;
    }
    csv_header(csv);

    run_all(num_msgs, csv);

    std::fclose(csv);
    std::printf("\n✓ Results written to %s\n", csv_path);
    return 0;
}
