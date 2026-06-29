#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <optional>
#include <type_traits>

/// Lock-free single-producer / multiple-consumer **broadcast** queue.
///
/// Every registered consumer independently receives every message
/// published after its registration.  The design:
///
///   - Producer owns `head_` (no CAS — single writer).
///   - Each consumer has its own `tail_[id]` on a dedicated cache line.
///   - Producer tracks min(all tails) to avoid overwriting slots the
///     slowest consumer hasn't read yet (cached for perf).
///   - Vyukov-style per-slot sequence numbers let consumers confirm
///     that the data at their position is actually committed.
///
/// Template parameters:
///   T            — element type (must be copy-constructible because
///                  multiple consumers read the same slot)
///   Capacity     — usable ring-buffer depth (rounded to next power of 2)
///   MaxConsumers — hard cap on concurrent consumers
template <typename T, std::size_t Capacity = 1024, std::size_t MaxConsumers = 8>
class SPMCQueue {
    static_assert(Capacity > 0, "Capacity must be greater than zero");
    static_assert(MaxConsumers > 0, "MaxConsumers must be greater than zero");
    static_assert(std::is_copy_constructible_v<T>,
                  "T must be copy-constructible (multiple consumers read "
                  "the same slot)");

    // ── Constants ───────────────────────────────────────────────────────
    static constexpr std::size_t kCacheLine = 64;

    static constexpr std::size_t next_pow2(std::size_t v) {
        --v;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        return v + 1;
    }

    static constexpr std::size_t kBufSize = next_pow2(Capacity);
    static constexpr std::size_t kMask    = kBufSize - 1;

    // Sentinel value — no valid position will ever match this.
    static constexpr std::size_t kSentinel = static_cast<std::size_t>(-1);

    // ── Per-slot storage ────────────────────────────────────────────────
    // Data + a sequence stamp.  The producer stores `sequence = pos`
    // (release) after writing `data` at position `pos`.  Consumers
    // spin-check `sequence == their_pos` (acquire) to confirm readiness.
    struct Slot {
        T                         data{};
        std::atomic<std::size_t>  sequence{kSentinel};
    };

    // ── Per-consumer tail (cache-line-padded) ───────────────────────────
    struct alignas(kCacheLine) ConsumerTail {
        std::atomic<std::size_t> pos{0};
    };

public:
    SPMCQueue() = default;
    ~SPMCQueue() = default;

    // Non-copyable, non-movable.
    SPMCQueue(const SPMCQueue&)            = delete;
    SPMCQueue& operator=(const SPMCQueue&) = delete;
    SPMCQueue(SPMCQueue&&)                 = delete;
    SPMCQueue& operator=(SPMCQueue&&)      = delete;

    // ── Consumer registration ───────────────────────────────────────────

    /// Register a new consumer.  Returns an ID for use with try_pop/pop.
    /// The new consumer will only see messages published *after* this
    /// call — not historical data.
    ///
    /// Thread-safe (multiple consumers may register concurrently), but
    /// must not be called concurrently with push().
    std::size_t register_consumer() {
        const std::size_t id =
            num_consumers_.fetch_add(1, std::memory_order_acq_rel);
        assert(id < MaxConsumers && "Exceeded MaxConsumers");

        // Start from current head — future messages only.
        tails_[id].pos.store(head_.load(std::memory_order_acquire),
                             std::memory_order_release);
        return id;
    }

    /// Number of currently registered consumers.
    std::size_t consumer_count() const {
        return num_consumers_.load(std::memory_order_acquire);
    }

    // ── Producer API (call from exactly ONE thread) ─────────────────────

    /// Non-blocking push.  Returns false if the slowest consumer hasn't
    /// caught up and the ring buffer is full.
    bool try_push(const T& item) {
        const std::size_t pos = head_.load(std::memory_order_relaxed);

        // Guard against overwriting data the slowest consumer hasn't read.
        if (pos - cached_min_tail_ >= kBufSize) {
            cached_min_tail_ = scan_min_tail();
            if (pos - cached_min_tail_ >= kBufSize) {
                return false;  // queue full for slowest consumer
            }
        }

        auto& slot       = slots_[pos & kMask];
        slot.data         = item;
        // Publish: sequence == pos signals "data at position pos is ready".
        slot.sequence.store(pos, std::memory_order_release);
        head_.store(pos + 1, std::memory_order_release);
        return true;
    }

    /// Spinning push.  Blocks until space is available.
    void push(const T& item) {
        while (!try_push(item)) {
            // spin — wait for slowest consumer
        }
    }

    // ── Consumer API (each consumer uses its own ID) ────────────────────

    /// Non-blocking pop for consumer `consumer_id`.
    /// Returns std::nullopt if no new data is available.
    std::optional<T> try_pop(std::size_t consumer_id) {
        assert(consumer_id < num_consumers_.load(std::memory_order_relaxed));

        const std::size_t pos =
            tails_[consumer_id].pos.load(std::memory_order_relaxed);

        // Quick check: has the producer committed this position?
        if (pos >= head_.load(std::memory_order_acquire)) {
            return std::nullopt;  // nothing new
        }

        // Acquire the sequence to synchronize with the producer's data write.
        auto& slot = slots_[pos & kMask];
        if (slot.sequence.load(std::memory_order_acquire) != pos) {
            // Head advanced but data not visible yet — transient; retry later.
            return std::nullopt;
        }

        // COPY the data — multiple consumers read the same slot.
        T item = slot.data;
        tails_[consumer_id].pos.store(pos + 1, std::memory_order_release);
        return item;
    }

    /// Spinning pop.  Blocks until data is available for this consumer.
    T pop(std::size_t consumer_id) {
        while (true) {
            auto item = try_pop(consumer_id);
            if (item.has_value()) {
                return std::move(*item);
            }
            // spin
        }
    }

    // ── Observers ───────────────────────────────────────────────────────

    /// Usable capacity of the ring buffer.
    static constexpr std::size_t capacity() { return kBufSize; }

    /// Approximate items pending for a given consumer.
    std::size_t size_approx(std::size_t consumer_id) const {
        const std::size_t h = head_.load(std::memory_order_relaxed);
        const std::size_t t =
            tails_[consumer_id].pos.load(std::memory_order_relaxed);
        return h - t;
    }

private:
    /// Scan all consumer tails; return the minimum (slowest reader).
    /// Called only by the producer, so no locking needed.
    std::size_t scan_min_tail() const {
        const std::size_t n =
            num_consumers_.load(std::memory_order_acquire);
        if (n == 0) {
            // No consumers — producer is never blocked.
            return head_.load(std::memory_order_relaxed);
        }
        std::size_t min_t = tails_[0].pos.load(std::memory_order_acquire);
        for (std::size_t i = 1; i < n; ++i) {
            const std::size_t t =
                tails_[i].pos.load(std::memory_order_acquire);
            if (t < min_t) min_t = t;
        }
        return min_t;
    }

    // ── Data layout ─────────────────────────────────────────────────────
    //
    //  head_            — written by producer, read by all consumers
    //  cached_min_tail_ — producer-private (plain size_t, not atomic)
    //  num_consumers_   — written during registration, read by producer
    //  tails_[]         — each on its own cache line
    //  slots_[]         — shared ring buffer

    alignas(kCacheLine) std::atomic<std::size_t> head_{0};

    // Producer-private state on a separate cache line so consumer reads
    // of head_ don't bounce when cached_min_tail_ is updated.
    alignas(kCacheLine) std::size_t cached_min_tail_{0};
                        std::atomic<std::size_t> num_consumers_{0};

    alignas(kCacheLine) ConsumerTail tails_[MaxConsumers];
    alignas(kCacheLine) Slot         slots_[kBufSize];
};
