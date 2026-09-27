// Deterministic replay of a recorded raw event log.
//
// The replay engine feeds decoded events into the *same* OrderBookManager the
// live engine uses. Nothing about book construction, validation or health is
// reimplemented here, so a divergence between live and replay can only come
// from the recording itself -- which is the property that makes this the
// foundation for a backtesting simulator.
//
// Determinism: events are applied strictly in recorded `seq` order, which the
// live engine assigns on a single ingest thread. Replay performs no I/O of its
// own; resync requests are recorded as "would have resynced" rather than
// issued, so a replay never depends on the venue being reachable or on what it
// would say today.
#pragma once

#include <cstdint>
#include <functional>
#include <istream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "market_data/config.hpp"
#include "market_data/market_event.hpp"
#include "market_data/order_book_manager.hpp"

namespace market_data {

struct ReplayStats {
    std::uint64_t lines_read = 0;
    std::uint64_t events_decoded = 0;
    std::uint64_t events_applied = 0;
    std::uint64_t malformed_lines = 0;
    // Events whose sequence number did not follow the previous one. A gap
    // means the recording is incomplete; a repeat means it was concatenated.
    std::uint64_t sequence_gaps = 0;
    std::uint64_t out_of_order_events = 0;
    std::uint64_t duplicate_sequences = 0;
    // Books that ended in a state the engine would not trust.
    std::uint64_t books_invalid_at_end = 0;
    std::uint64_t crossed_incidents = 0;
    std::uint64_t resyncs_requested = 0;

    std::uint64_t first_sequence = 0;
    std::uint64_t last_sequence = 0;

    std::uint64_t snapshots_emitted = 0;
    std::uint64_t trades_emitted = 0;
};

class ReplayEngine {
public:
    explicit ReplayEngine(Config config);

    // Invoked after each applied event, for progress output or assertions.
    using EventObserver = std::function<void(const MarketEvent&, const ReplayStats&)>;
    void set_event_observer(EventObserver observer);

    // Emits normalized snapshots on the same cadence the live engine uses,
    // measured in *recorded* time rather than wall-clock. That is what makes a
    // rebuilt dataset comparable to the one the live session produced, and it
    // keeps replay deterministic: the same log always yields the same rows.
    using SnapshotObserver = std::function<void(const MarketSnapshot&)>;
    void set_snapshot_observer(SnapshotObserver observer);

    // Trades are emitted as they are decoded, so a rebuilt trade dataset is
    // identical to the live one.
    using TradeObserver = std::function<void(const Trade&)>;
    void set_trade_observer(TradeObserver observer);

    // Reads JSON Lines until EOF. A malformed line is counted and skipped, so
    // a truncated final record cannot abort the run.
    void replay_stream(std::istream& input);
    bool replay_file(const std::string& path, std::string& error);

    [[nodiscard]] const ReplayStats& stats() const { return stats_; }
    [[nodiscard]] const OrderBookManager& books() const { return book_manager_; }

    [[nodiscard]] std::vector<MarketSnapshot> final_snapshots() const;

    // A stable digest of every book's final state. Two runs over the same
    // input produce the same digest, and a replay of a live session's log
    // produces the same digest as the live session's final books.
    [[nodiscard]] std::string final_state_digest() const;

private:
    void apply(MarketEvent&& event);

    // Emits a snapshot for every tracked book if `now` has advanced past the
    // next scheduled snapshot instant.
    void maybe_emit_snapshots(ReceiveTime now, bool force);

    Config config_;
    OrderBookManager book_manager_;
    ReplayStats stats_;
    EventObserver observer_;
    SnapshotObserver snapshot_observer_;
    TradeObserver trade_observer_;
    std::uint64_t previous_sequence_ = 0;
    // Recorded-time clock driving the snapshot cadence.
    ReceiveTime next_snapshot_at_{};
    bool snapshot_clock_started_ = false;
};

// Digest of a set of books, used to compare a live session against its replay.
std::string snapshot_digest(const std::vector<MarketSnapshot>& snapshots);

}  // namespace market_data
