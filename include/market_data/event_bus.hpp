// Bounded, thread-safe queue between ingestion and everything downstream.
//
// Capacity
//   Set by Config::event_queue_capacity, default 262,144 events. At an
//   observed few-thousand-events-per-second feed that is roughly a minute of
//   buffer -- enough to ride out a slow fsync or a log rotation without ever
//   touching the network thread.
//
// What happens when consumers fall behind
//   Two policies, chosen by Config::block_when_queue_full:
//     Block (default) -- the producer waits. Data is never lost. Backpressure
//       propagates to the socket, TCP's receive window closes, and the venue
//       slows down or eventually drops us; we then reconnect and resync, which
//       is a recoverable and *detectable* outcome.
//     DropNewest -- the producer discards the event and increments a counter.
//       Latency is preserved at the cost of a gap.
//
// Whether events may ever be dropped
//   Under the default policy, no. push() only returns false once close() has
//   been called, which happens during shutdown after ingestion has stopped.
//
// How dropped events are detected
//   Every drop increments dropped_count(), which is printed in the status
//   line, included in the final shutdown summary, and written into the raw log
//   as part of the metrics record. A non-zero drop count means a recorded
//   session has a hole in it and must not be treated as complete.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

namespace market_data {

enum class OverflowPolicy : std::uint8_t {
    // Wait for room. Preserves every event; applies backpressure upstream.
    Block,
    // Discard the incoming event and count it.
    DropNewest,
};

template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity, OverflowPolicy policy = OverflowPolicy::Block)
        : capacity_(capacity == 0 ? 1 : capacity), policy_(policy) {}

    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    // Returns false only if the queue is closed, or if the event was dropped
    // under DropNewest.
    bool push(T value) {
        std::unique_lock lock(mutex_);
        if (closed_) {
            return false;
        }
        if (items_.size() >= capacity_) {
            if (policy_ == OverflowPolicy::DropNewest) {
                dropped_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            ++producers_blocked_;
            not_full_.wait(lock, [this] { return closed_ || items_.size() < capacity_; });
            --producers_blocked_;
            if (closed_) {
                return false;
            }
        }
        items_.push_back(std::move(value));
        const std::size_t depth = items_.size();
        if (depth > high_water_) {
            high_water_ = depth;
        }
        lock.unlock();
        not_empty_.notify_one();
        return true;
    }

    // Blocks until an item is available or the queue is closed and drained.
    std::optional<T> pop() {
        std::unique_lock lock(mutex_);
        not_empty_.wait(lock, [this] { return closed_ || !items_.empty(); });
        if (items_.empty()) {
            return std::nullopt;  // Closed and drained.
        }
        T value = std::move(items_.front());
        items_.pop_front();
        lock.unlock();
        not_full_.notify_one();
        return value;
    }

    // Drains up to `max` items in one lock acquisition. Batching keeps the
    // consumer from paying a mutex round trip per event at high message rates.
    std::size_t pop_batch(std::vector<T>& out, std::size_t max) {
        out.clear();
        std::unique_lock lock(mutex_);
        not_empty_.wait(lock, [this] { return closed_ || !items_.empty(); });
        while (!items_.empty() && out.size() < max) {
            out.push_back(std::move(items_.front()));
            items_.pop_front();
        }
        lock.unlock();
        if (!out.empty()) {
            not_full_.notify_all();
        }
        return out.size();
    }

    // Like pop_batch, but gives up after `timeout`.
    //
    // Returns nullopt once the queue is closed *and* drained; 0 when the wait
    // timed out with the queue still open. That distinction is what lets a
    // consumer run periodic work (a flush, a rotation) without needing an
    // event to arrive first.
    std::optional<std::size_t> pop_batch_for(std::vector<T>& out,
                                             std::size_t max,
                                             std::chrono::milliseconds timeout) {
        out.clear();
        std::unique_lock lock(mutex_);
        not_empty_.wait_for(lock, timeout, [this] { return closed_ || !items_.empty(); });
        if (items_.empty()) {
            return closed_ ? std::nullopt : std::optional<std::size_t>{0};
        }
        while (!items_.empty() && out.size() < max) {
            out.push_back(std::move(items_.front()));
            items_.pop_front();
        }
        lock.unlock();
        not_full_.notify_all();
        return out.size();
    }

    // Wakes every waiter. Consumers still drain what is already queued, so a
    // close during shutdown does not discard buffered events.
    void close() {
        {
            const std::lock_guard lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    [[nodiscard]] bool closed() const {
        const std::lock_guard lock(mutex_);
        return closed_;
    }

    [[nodiscard]] std::size_t size() const {
        const std::lock_guard lock(mutex_);
        return items_.size();
    }

    [[nodiscard]] std::size_t capacity() const { return capacity_; }

    [[nodiscard]] std::size_t high_water_mark() const {
        const std::lock_guard lock(mutex_);
        return high_water_;
    }

    [[nodiscard]] std::uint64_t dropped_count() const {
        return dropped_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t blocked_producers() const {
        const std::lock_guard lock(mutex_);
        return producers_blocked_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<T> items_;

    const std::size_t capacity_;
    const OverflowPolicy policy_;

    bool closed_ = false;
    std::size_t high_water_ = 0;
    std::size_t producers_blocked_ = 0;
    std::atomic<std::uint64_t> dropped_{0};
};

}  // namespace market_data
