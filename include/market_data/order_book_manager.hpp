// Owns one OrderBook per subscribed token, plus the health state machine that
// decides when a book can be trusted.
//
// Health lifecycle
//
//   Initializing --snapshot--> Healthy
//        ^                        |
//        |          no message for stale_after_ms
//        |                        v
//        |                     Stale --update--> Healthy
//        |                        |
//        |    crossed book, phantom deletes, tick re-grid, or disconnect
//        |                        v
//        +---- snapshot ----- Recovering <-- resync requested
//                                 |
//                       resync repeatedly failed
//                                 v
//                              Invalid
//
// The rule that matters: a book is never Healthy on the strength of
// incremental updates alone. It must have been seeded by a snapshot since the
// last invalidation, because an update stream applied to a book with an
// unknown starting state produces a plausible-looking but wrong book.
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "market_data/config.hpp"
#include "market_data/market_event.hpp"
#include "market_data/order_book.hpp"
#include "market_data/types.hpp"

namespace market_data {

enum class BookHealth : std::uint8_t {
    Initializing,  // Subscribed, no snapshot yet.
    Healthy,       // Seeded by a snapshot and receiving updates.
    Stale,         // Seeded, but nothing has arrived recently.
    Recovering,    // Invalidated; a fresh snapshot has been requested.
    Invalid,       // Invalidated and recovery has not succeeded.
};

const char* to_string(BookHealth health);

struct BookState {
    TokenId token_id;
    MarketId market_id;
    BookHealth health = BookHealth::Initializing;

    ExchangeTime last_message_exchange_time{};
    ReceiveTime last_message_time{};
    ReceiveTime last_snapshot_time{};
    ReceiveTime last_resync_request{};

    std::uint64_t updates_processed = 0;
    std::uint64_t invalid_updates = 0;
    std::uint64_t snapshots_applied = 0;
    std::uint64_t crossed_incidents = 0;
    std::uint64_t resync_requests = 0;
    std::uint64_t trades_seen = 0;
    // Sequence of the most recent event applied to this book; 0 if none.
    std::uint64_t last_sequence = 0;

    double last_trade_price = 0.0;
    double tick_size = 0.0;
};

// Called when a book needs a fresh snapshot. The manager never performs I/O
// itself; the engine routes this to the adapter.
using ResyncRequest = std::function<void(const TokenId&)>;

class OrderBookManager {
public:
    explicit OrderBookManager(Config config);

    void set_resync_handler(ResyncRequest handler);

    // Declares the tokens we expect data for. Tokens no longer in the set are
    // dropped; new ones start Initializing.
    void set_tracked_tokens(const std::vector<TokenId>& tokens);

    // Applies one normalized event. Returns true if the event mutated a book.
    bool apply(const MarketEvent& event);

    // Marks every book as needing a fresh snapshot. Called on disconnect: a
    // local book cannot be assumed valid across a gap in the feed.
    void invalidate_all(const std::string& reason);

    // Moves books with no recent traffic to Stale and re-requests snapshots
    // for anything stuck in Recovering past the cooldown. Call periodically.
    void run_maintenance();

    [[nodiscard]] std::optional<MarketSnapshot> build_snapshot(const TokenId& token) const;
    [[nodiscard]] std::vector<MarketSnapshot> build_all_snapshots() const;

    [[nodiscard]] std::optional<BookState> state(const TokenId& token) const;
    [[nodiscard]] std::vector<BookState> all_states() const;

    [[nodiscard]] std::size_t book_count() const;
    void health_counts(std::uint64_t& healthy, std::uint64_t& stale, std::uint64_t& invalid) const;

    // Total crossed-book incidents across every book, for metrics.
    [[nodiscard]] std::uint64_t total_crossed_incidents() const;

private:
    struct Entry {
        OrderBook book;
        BookState state;
        // Set when the book was seeded by a snapshot and not invalidated since.
        bool seeded = false;
        // Venue hash of the last snapshot applied, used to skip re-applying a
        // book the venue resent unchanged.
        std::string last_hash;
    };

    // All helpers below assume mutex_ is held.
    Entry& entry_for(const TokenId& token, const MarketId& market_id);
    void invalidate_locked(Entry& entry, const std::string& reason);
    void maybe_request_resync_locked(Entry& entry);
    [[nodiscard]] MarketSnapshot build_snapshot_locked(const Entry& entry) const;

    bool apply_snapshot_event(const BookSnapshotEvent& event);
    bool apply_update_event(const BookUpdateEvent& event);
    bool apply_trade_event(const TradeEvent& event);
    bool apply_metadata_event(const MarketMetadataEvent& event);
    bool apply_connection_event(const ConnectionEvent& event);

    Config config_;
    mutable std::mutex mutex_;
    // Ordered so status output and snapshot files list tokens in a stable
    // order run to run, which matters when diffing a replay against a session.
    std::map<TokenId, Entry> books_;
    ResyncRequest resync_handler_;
    // Collected under the lock, invoked after releasing it: the handler does
    // network I/O and must not run with the book mutex held.
    mutable std::vector<TokenId> pending_resyncs_;
};

}  // namespace market_data
