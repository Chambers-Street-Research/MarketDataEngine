// Runtime metrics.
//
// Counters are plain atomics updated on the hot path with relaxed ordering:
// they are statistics, not synchronization, and a metric that perturbed the
// feed it measures would defeat the point. Rates are derived by differencing
// snapshots, so no per-event timestamp bookkeeping is needed.
#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "market_data/types.hpp"

namespace market_data {

// An immutable point-in-time copy, safe to format and diff.
struct MetricsSnapshot {
    std::uint64_t markets_discovered = 0;
    std::uint64_t tokens_subscribed = 0;

    std::uint64_t messages_received = 0;
    std::uint64_t book_snapshots = 0;
    std::uint64_t book_updates = 0;
    std::uint64_t trades = 0;
    std::uint64_t events_persisted = 0;
    std::uint64_t snapshots_written = 0;

    std::uint64_t queue_depth = 0;
    std::uint64_t queue_high_water = 0;
    std::uint64_t storage_queue_depth = 0;
    std::uint64_t storage_queue_high_water = 0;
    std::uint64_t events_dropped = 0;

    std::uint64_t reconnects = 0;
    std::uint64_t invalid_messages = 0;
    std::uint64_t crossed_books = 0;
    std::uint64_t resyncs_started = 0;
    std::uint64_t resyncs_completed = 0;

    std::uint64_t bytes_written = 0;
    std::uint64_t bytes_received = 0;

    std::uint64_t books_healthy = 0;
    std::uint64_t books_stale = 0;
    std::uint64_t books_invalid = 0;

    double uptime_seconds = 0.0;
};

// Rates between two snapshots, computed over their wall-clock gap.
struct MetricsRates {
    double messages_per_sec = 0.0;
    double book_updates_per_sec = 0.0;
    double trades_per_sec = 0.0;
    double events_persisted_per_sec = 0.0;
    double interval_seconds = 0.0;
};

MetricsRates compute_rates(const MetricsSnapshot& previous, const MetricsSnapshot& current);

class Metrics {
public:
    Metrics();

    void set_markets_discovered(std::uint64_t value);
    void set_tokens_subscribed(std::uint64_t value);
    void set_queue_depth(std::uint64_t depth, std::uint64_t high_water);
    void set_book_health_counts(std::uint64_t healthy, std::uint64_t stale, std::uint64_t invalid);

    void on_message_received();
    void on_book_snapshot();
    void on_book_update(std::uint64_t count = 1);
    void on_trade();
    // Persistence is performed by the storage layer on its own threads, so
    // the engine mirrors its counters rather than incrementing per event.
    void set_persistence_counters(std::uint64_t events_persisted,
                                  std::uint64_t bytes_written,
                                  std::uint64_t events_dropped);
    void set_storage_queue_depth(std::uint64_t depth, std::uint64_t high_water);
    void on_snapshot_written();
    void on_events_dropped(std::uint64_t count);
    void on_invalid_message(std::uint64_t count = 1);
    void on_crossed_book();
    void on_resync_started();
    void on_resync_completed();
    // Transport counters live in the adapter; the engine mirrors them here
    // rather than double counting on the hot path.
    void set_transport_counters(std::uint64_t bytes_received,
                                std::uint64_t malformed_messages,
                                std::uint64_t reconnects);

    [[nodiscard]] MetricsSnapshot snapshot() const;

    // Human-readable multi-line block for the periodic status print.
    [[nodiscard]] static std::string format_status(const MetricsSnapshot& snapshot,
                                                   const MetricsRates& rates);

private:
    ReceiveTime started_at_;

    std::atomic<std::uint64_t> markets_discovered_{0};
    std::atomic<std::uint64_t> tokens_subscribed_{0};
    std::atomic<std::uint64_t> messages_received_{0};
    std::atomic<std::uint64_t> book_snapshots_{0};
    std::atomic<std::uint64_t> book_updates_{0};
    std::atomic<std::uint64_t> trades_{0};
    std::atomic<std::uint64_t> events_persisted_{0};
    std::atomic<std::uint64_t> snapshots_written_{0};
    std::atomic<std::uint64_t> queue_depth_{0};
    std::atomic<std::uint64_t> queue_high_water_{0};
    std::atomic<std::uint64_t> storage_queue_depth_{0};
    std::atomic<std::uint64_t> storage_queue_high_water_{0};
    std::atomic<std::uint64_t> events_dropped_{0};
    std::atomic<std::uint64_t> reconnects_{0};
    std::atomic<std::uint64_t> invalid_messages_{0};
    std::atomic<std::uint64_t> crossed_books_{0};
    std::atomic<std::uint64_t> resyncs_started_{0};
    std::atomic<std::uint64_t> resyncs_completed_{0};
    std::atomic<std::uint64_t> bytes_written_{0};
    std::atomic<std::uint64_t> bytes_received_{0};
    std::atomic<std::uint64_t> books_healthy_{0};
    std::atomic<std::uint64_t> books_stale_{0};
    std::atomic<std::uint64_t> books_invalid_{0};
};

}  // namespace market_data
