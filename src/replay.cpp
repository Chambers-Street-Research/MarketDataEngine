#include "market_data/replay.hpp"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "market_data/event_codec.hpp"
#include "market_data/logging.hpp"

namespace market_data {

ReplayEngine::ReplayEngine(Config config)
    : config_(std::move(config)), book_manager_(config_) {
    // Replay is offline and deterministic: a resync would mean asking the
    // venue what the book looks like *now*, which has nothing to do with the
    // recorded session. Record the request and carry on.
    book_manager_.set_resync_handler([this](const TokenId& token) {
        ++stats_.resyncs_requested;
        MDE_DEBUG("replay_resync_skipped", field("token", token));
    });
}

void ReplayEngine::set_event_observer(EventObserver observer) {
    observer_ = std::move(observer);
}

void ReplayEngine::set_snapshot_observer(SnapshotObserver observer) {
    snapshot_observer_ = std::move(observer);
}

void ReplayEngine::set_trade_observer(TradeObserver observer) {
    trade_observer_ = std::move(observer);
}

void ReplayEngine::maybe_emit_snapshots(ReceiveTime now, bool force) {
    if (!snapshot_observer_) {
        return;
    }
    const auto interval = std::chrono::milliseconds{config_.storage.parquet.snapshot_interval_ms};

    if (!snapshot_clock_started_) {
        snapshot_clock_started_ = true;
        next_snapshot_at_ = now + interval;
        // No snapshot at time zero: the first books have not been seeded yet.
        if (!force) {
            return;
        }
    }
    if (!force && now < next_snapshot_at_) {
        return;
    }

    for (const auto& snapshot : book_manager_.build_all_snapshots()) {
        snapshot_observer_(snapshot);
        ++stats_.snapshots_emitted;
    }

    if (!force) {
        // Advance to the next slot at or after `now`, so a gap in the recording
        // does not produce a burst of catch-up snapshots.
        do {
            next_snapshot_at_ += interval;
        } while (next_snapshot_at_ <= now);
    }
}

void ReplayEngine::apply(MarketEvent&& event) {
    const auto sequence = header_of(event).sequence;

    if (sequence != 0) {
        if (stats_.first_sequence == 0) {
            stats_.first_sequence = sequence;
        } else if (sequence == previous_sequence_) {
            ++stats_.duplicate_sequences;
        } else if (sequence < previous_sequence_) {
            ++stats_.out_of_order_events;
        } else if (sequence != previous_sequence_ + 1) {
            ++stats_.sequence_gaps;
        }
        previous_sequence_ = sequence;
        stats_.last_sequence = sequence;
    }

    // Tracking on first sight rather than from a subscription list: the log is
    // the only source of truth about which tokens the session actually saw.
    const auto& token = header_of(event).token_id;
    if (!token.empty() && !book_manager_.state(token).has_value()) {
        book_manager_.set_tracked_tokens([&] {
            auto tokens = std::vector<TokenId>{};
            for (const auto& state : book_manager_.all_states()) {
                tokens.push_back(state.token_id);
            }
            tokens.push_back(token);
            return tokens;
        }());
    }

    if (book_manager_.apply(event)) {
        ++stats_.events_applied;
    }
    ++stats_.events_decoded;

    if (const auto* trade = std::get_if<TradeEvent>(&event)) {
        if (trade_observer_) {
            trade_observer_(trade->trade);
            ++stats_.trades_emitted;
        }
    }

    // Recorded receive time drives the cadence; wall-clock time during the
    // replay is irrelevant and would destroy determinism.
    const auto recorded_now = header_of(event).receive_timestamp;
    if (recorded_now.time_since_epoch().count() != 0) {
        maybe_emit_snapshots(recorded_now, /*force=*/false);
    }

    if (observer_) {
        observer_(event, stats_);
    }
}

void ReplayEngine::replay_stream(std::istream& input) {
    std::string line;
    while (std::getline(input, line)) {
        ++stats_.lines_read;
        if (line.empty()) {
            continue;
        }
        std::string error;
        auto event = decode_event_line(line, &error);
        if (!event) {
            ++stats_.malformed_lines;
            // A torn last line is the normal result of killing a live engine;
            // it is counted, reported, and never fatal.
            MDE_DEBUG("replay_bad_line", field("line", stats_.lines_read),
                      field("error", error));
            continue;
        }
        apply(std::move(*event));
    }

    // A final snapshot so the rebuilt dataset ends on the same state the live
    // session's shutdown snapshot captured.
    if (snapshot_clock_started_) {
        maybe_emit_snapshots(next_snapshot_at_, /*force=*/true);
    }

    stats_.crossed_incidents = book_manager_.total_crossed_incidents();
    std::uint64_t healthy = 0;
    std::uint64_t stale = 0;
    std::uint64_t invalid = 0;
    book_manager_.health_counts(healthy, stale, invalid);
    stats_.books_invalid_at_end = invalid;
}

bool ReplayEngine::replay_file(const std::string& path, std::string& error) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        error = "cannot open input file: " + path;
        return false;
    }
    replay_stream(input);
    return true;
}

std::vector<MarketSnapshot> ReplayEngine::final_snapshots() const {
    return book_manager_.build_all_snapshots();
}

std::string snapshot_digest(const std::vector<MarketSnapshot>& snapshots) {
    // Deliberately a readable, order-independent digest rather than a hash:
    // when a replay diverges from a live run, a diff of two of these points
    // straight at the book that went wrong.
    std::vector<std::string> rows;
    rows.reserve(snapshots.size());
    for (const auto& snapshot : snapshots) {
        std::ostringstream row;
        row << snapshot.token_id << '|' << std::fixed << std::setprecision(4)
            << snapshot.best_bid << '|' << snapshot.best_ask << '|'
            << std::setprecision(6) << snapshot.best_bid_size << '|'
            << snapshot.best_ask_size << '|' << snapshot.bid_levels << '|'
            << snapshot.ask_levels;
        rows.push_back(row.str());
    }
    std::sort(rows.begin(), rows.end());

    std::ostringstream out;
    for (const auto& row : rows) {
        out << row << '\n';
    }
    return out.str();
}

std::string ReplayEngine::final_state_digest() const {
    return snapshot_digest(final_snapshots());
}

}  // namespace market_data
