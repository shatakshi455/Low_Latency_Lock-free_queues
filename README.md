# Low-Latency Lock-Free Queues

High-performance, lock-free queue implementations in C++17 for low-latency systems — compared head-to-head against a mutex baseline.

## Queue Implementations

### `SPSCQueue<T, Capacity>` — Lock-Free Single-Producer / Single-Consumer

A bounded ring buffer using two cache-line-separated atomics (`head_` for the producer, `tail_` for the consumer). The hot path uses **only acquire/release loads and stores** — no CAS, no locks, no syscalls.

- **Power-of-2 buffer size** — bitwise mask replaces modulo for index wrapping.
- **One slot reserved** to distinguish full from empty without a separate counter.
- **Cache-line padding** between `head_` and `tail_` eliminates false sharing.

### `SPMCQueue<T, Capacity, MaxConsumers>` — Lock-Free Broadcast Queue

A single-producer, multiple-consumer broadcast queue where **every registered consumer independently receives every message**. This is not a competing-consumer model — it's fan-out.

- **Per-consumer tail array** — each consumer has its own read position on a dedicated cache line.
- **Vyukov-style sequence stamps** — per-slot atomic sequence numbers let consumers confirm data is committed before reading.
- **Cached min-tail optimization** — the producer caches `min(all consumer tails)` and only rescans when the cache indicates the buffer is full. This avoids scanning N tails on every push.
- **Dynamic consumer registration** — consumers can register at any time and will only receive messages published after registration.

### `MutexQueue<T, Capacity>` — Baseline (Control Group)

A thread-safe bounded queue using `std::mutex` + `std::condition_variable`. Serves as the **control group** for benchmarking — every lock-free optimization is measured against this.

## Design Decisions

| Decision | Rationale |
|---|---|
| **Power-of-2 ring buffers** | `index & mask` is a single AND instruction vs. `index % size` which compiles to division. On the hot path this matters. |
| **Cache-line padding** (`alignas(64)`) | Producer and consumer atomics on separate cache lines prevent false sharing — the #1 killer of lock-free performance on multi-core. |
| **No CAS on the hot path** | Compare-and-swap generates a `LOCK CMPXCHG` which takes the cache line exclusive and retries on contention. Acquire/release loads and stores (`MOV` + fences) are strictly cheaper. |
| **Broadcast vs. competing-consumer** | The SPMC queue gives every consumer every message (market data fan-out pattern). Competing-consumer is a different problem that mutex queues already handle well. |
| **Cached min-tail** | Scanning all consumer tails on every `push()` is O(N) in consumers. The cached min-tail makes the common case O(1) — only rescanning when the cache says "full". |
| **Sequence stamps (Vyukov)** | Without per-slot sequence numbers, a consumer can't distinguish between "data is here" and "slot is stale from a previous lap". The sequence stamp resolves this race. |

## Build & Run

Requires **g++ with C++17 support** and **pthreads**.

```bash
# Compile and run all correctness tests
make run

# Compile and run the standalone benchmark (emits CSV)
make run-bench

# Generate throughput charts (requires matplotlib)
make plot

# Clean all build artifacts
make clean
```

## Benchmark Results

The standalone benchmark (`bench/benchmark.cpp`) compares all three queue types across:
- **Message sizes**: 8B, 64B, 256B
- **Consumer counts**: 1, 2, 4, 8
- **3 iterations per configuration**, reporting the median

Results are written to `bench_results.csv`. Run `make plot` to generate the charts below.

### Throughput vs Message Size (1 consumer)

![Throughput vs Message Size](throughput_vs_msgsize.png)

### Throughput vs Consumer Count (64B messages)

![Throughput vs Consumer Count](throughput_vs_consumers.png)

## Project Structure

```
├── include/
│   ├── spsc_queue.h          # Lock-free SPSC queue
│   ├── spmc_queue.h          # Lock-free SPMC broadcast queue
│   └── mutex_queue.h         # Mutex baseline queue
├── tests/
│   ├── test_spsc_queue.cpp   # SPSC correctness tests + inline bench
│   ├── test_spmc_queue.cpp   # SPMC correctness tests + inline bench
│   └── test_mutex_queue.cpp  # Mutex correctness tests + inline bench
├── bench/
│   ├── benchmark.cpp         # Standalone head-to-head benchmark
│   └── plot.py               # Chart generation script
├── Makefile                  # Build system
├── output.log                # Test output log
├── bench_results.csv         # Benchmark results (generated)
├── throughput_vs_msgsize.png # Chart (generated)
└── throughput_vs_consumers.png # Chart (generated)
```
