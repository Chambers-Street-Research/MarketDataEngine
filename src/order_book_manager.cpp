#include "market_data/order_book_manager.hpp"

#include <algorithm>
#include <set>

#include "market_data/logging.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

constexpr std::size_t kDepthL5 = 5;
constexpr std::size_t kDepthL10 = 10;

}  // namespace

const char* to_string(BookHealth health) {
    switch (health) {
        case BookHealth::Initializing: return "initializing";
        case BookHealth::Healthy:      return "healthy";
        case BookHealth::Stale:        return "stale";
        case BookHealth::Recovering:   return "recovering";
        case BookHealth::Invalid:      return "invalid";
    }
    return "unknown";
}

OrderBookManager::OrderBookManager(Config config) : config_(std::move(config)) {}

void OrderBookManager::set_resync_handler(ResyncRequest handler) {
    const std::lock_guard lock(mutex_);
    resync_handler_ = std::move(handler);
}

void OrderBookManager::set_tracked_tokens(const std::vector<TokenId>& tokens) {
    const std::lock_guard lock(mutex_);
    const std::set<TokenId> wanted(tokens.begin(), tokens.end());

    for (auto it = books_.begin(); it != books_.end();) {
        it = wanted.count(it->first) == 0 ? books_.erase(it) : std::next(it);
    }
    for (const auto& token : tokens) {
        if (books_.find(token) == books_.end()) {
            Entry entry;
            entry.state.token_id = token;
            entry.state.health = BookHealth::Initializing;
            books_.emplace(token, std::move(entry));
        }
    }
}

OrderBookManager::Entry& OrderBookManager::entry_for(const TokenId& token,
                                                     const MarketId& market_id) {
    auto it = books_.find(token);
    if (it == books_.end()) {
        // Data for a token we did not explicitly track: still worth keeping,
        // because the venue can push an outcome we did not ask for and
        // dropping it would silently lose part of a market.
        Entry entry;
        entry.state.token_id = token;
        it = books_.emplace(token, std::move(entry)).first;
    }
    if (!market_id.empty()) {
        it->second.state.market_id = market_id;
    }
    return it->second;
}

void OrderBookManager::invalidate_locked(Entry& entry, const std::string& reason) {
    entry.book.clear();
    entry.seeded = false;
    entry.state.health = BookHealth::Recovering;
    MDE_INFO("book_invalidated", field("token", entry.state.token_id),
             field("reason", reason));
    maybe_request_resync_locked(entry);
}

void OrderBookManager::maybe_request_resync_locked(Entry& entry) {
    const auto now = now_receive();
    const auto since = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now - entry.state.last_resync_request)
                           .count();
    // The cooldown keeps a persistently bad feed from turning into a REST hot
    // loop against the venue.
    if (entry.state.resync_requests > 0 &&
        since < static_cast<std::int64_t>(config_.health.resync_cooldown_ms)) {
        return;
    }
    entry.state.last_resync_request = now;
    ++entry.state.resync_requests;
    pending_resyncs_.push_back(entry.state.token_id);
}

// ---------------------------------------------------------------------------
// Event application
// ---------------------------------------------------------------------------

bool OrderBookManager::apply(const MarketEvent& event) {
    const bool mutated = std::visit(
        [this]<typename T>(const T& typed) {
            using E = std::decay_t<T>;
            if constexpr (std::is_same_v<E, BookSnapshotEvent>) return apply_snapshot_event(typed);
            else if constexpr (std::is_same_v<E, BookUpdateEvent>) return apply_update_event(typed);
            else if constexpr (std::is_same_v<E, TradeEvent>) return apply_trade_event(typed);
            else if constexpr (std::is_same_v<E, MarketMetadataEvent>) return apply_metadata_event(typed);
            else return apply_connection_event(typed);
        },
        event);

    // Drain resync requests outside the lock: the handler performs an HTTP
    // request and must never run while holding the book mutex.
    std::vector<TokenId> to_resync;
    ResyncRequest handler;
    {
        const std::lock_guard lock(mutex_);
        to_resync.swap(pending_resyncs_);
        handler = resync_handler_;
    }
    if (handler) {
        for (const auto& token : to_resync) {
            handler(token);
        }
    }
    return mutated;
}

bool OrderBookManager::apply_snapshot_event(const BookSnapshotEvent& event) {
    const std::lock_guard lock(mutex_);
    Entry& entry = entry_for(event.header.token_id, event.header.market_id);

    entry.state.last_message_time = event.header.receive_timestamp;
    entry.state.last_message_exchange_time = event.header.exchange_timestamp;
    entry.state.last_sequence = event.header.sequence;
    if (event.tick_size > 0.0) {
        entry.state.tick_size = event.tick_size;
    }

    // A snapshot the venue resent unchanged (same book hash) still proves the
    // feed is alive, but rebuilding both ladders from it is wasted work.
    if (entry.seeded && !event.header.venue_hash.empty() &&
        event.header.venue_hash == entry.last_hash) {
        if (entry.state.health == BookHealth::Stale) {
            entry.state.health = BookHealth::Healthy;
        }
        return false;
    }

    entry.book.apply_snapshot(event.bids, event.asks);
    entry.seeded = true;
    entry.last_hash = event.header.venue_hash;
    entry.state.last_snapshot_time = event.header.receive_timestamp;
    entry.state.snapshots_applied = entry.book.stats().snapshots_applied;

    if (entry.book.crossed()) {
        // A snapshot that is itself crossed means the venue sent us garbage;
        // there is nothing better to fall back to, so record and retry.
        entry.state.crossed_incidents = entry.book.stats().crossed_incidents;
        MDE_WARN("snapshot_crossed", field("token", entry.state.token_id));
        entry.state.health = BookHealth::Invalid;
        maybe_request_resync_locked(entry);
    } else {
        const bool recovering = entry.state.health == BookHealth::Recovering ||
                                entry.state.health == BookHealth::Invalid;
        entry.state.health = BookHealth::Healthy;
        if (recovering) {
            MDE_INFO("resync_completed", field("token", entry.state.token_id),
                     field("bids", event.bids.size()), field("asks", event.asks.size()));
        } else {
            MDE_DEBUG("snapshot_applied", field("token", entry.state.token_id),
                      field("bids", event.bids.size()), field("asks", event.asks.size()));
        }
    }
    return true;
}

bool OrderBookManager::apply_update_event(const BookUpdateEvent& event) {
    const std::lock_guard lock(mutex_);
    Entry& entry = entry_for(event.header.token_id, event.header.market_id);

    entry.state.last_message_time = event.header.receive_timestamp;
    entry.state.last_message_exchange_time = event.header.exchange_timestamp;
    entry.state.last_sequence = event.header.sequence;

    if (!entry.seeded) {
        // Applying increments to a book with an unknown base state would build
        // a book that looks fine and is wrong. Wait for the snapshot instead.
        if (entry.state.health != BookHealth::Recovering) {
            entry.state.health = BookHealth::Recovering;
            maybe_request_resync_locked(entry);
        }
        return false;
    }

    const auto crossed_before = entry.book.stats().crossed_incidents;
    const auto phantom_before = entry.book.stats().phantom_deletes;

    for (const auto& update : event.updates) {
        if (!entry.book.apply_update(update.side, update.price, update.quantity)) {
            ++entry.state.invalid_updates;
        }
        ++entry.state.updates_processed;
    }

    const auto& stats = entry.book.stats();
    entry.state.crossed_incidents = stats.crossed_incidents;

    if (stats.phantom_deletes > phantom_before) {
        // We were told to delete a level we never had: our view has drifted
        // from the venue's, which only a snapshot can fix.
        invalidate_locked(entry, "phantom delete");
        return true;
    }
    if (stats.crossed_incidents > crossed_before) {
        MDE_WARN("crossed_book", field("token", entry.state.token_id));
        invalidate_locked(entry, "crossed book");
        return true;
    }

    if (entry.state.health != BookHealth::Healthy) {
        entry.state.health = BookHealth::Healthy;
    }
    return true;
}

bool OrderBookManager::apply_trade_event(const TradeEvent& event) {
    const std::lock_guard lock(mutex_);
    Entry& entry = entry_for(event.header.token_id, event.header.market_id);
    entry.state.last_message_time = event.header.receive_timestamp;
    entry.state.last_message_exchange_time = event.header.exchange_timestamp;
    entry.state.last_sequence = event.header.sequence;
    entry.state.last_trade_price = event.trade.price;
    ++entry.state.trades_seen;
    return true;
}

bool OrderBookManager::apply_metadata_event(const MarketMetadataEvent& event) {
    if (event.header.token_id.empty()) {
        return false;
    }
    const std::lock_guard lock(mutex_);
    Entry& entry = entry_for(event.header.token_id, event.header.market_id);

    if (event.tick_size > 0.0 && event.tick_size != entry.state.tick_size) {
        const double previous = entry.state.tick_size;
        entry.state.tick_size = event.tick_size;
        if (previous > 0.0) {
            // The venue re-gridded the book. Levels quoted on the old grid may
            // no longer exist, so the local copy is not trustworthy.
            MDE_INFO("tick_size_changed", field("token", entry.state.token_id),
                     field("old", previous), field("new", event.tick_size));
            invalidate_locked(entry, "tick size change");
            return true;
        }
    }
    return false;
}

bool OrderBookManager::apply_connection_event(const ConnectionEvent& event) {
    switch (event.state) {
        case ConnectionState::Disconnected:
        case ConnectionState::Reconnecting:
        case ConnectionState::Failed:
            // Any gap in the feed means missed updates. Every book goes back to
            // Recovering; none of them is trusted again until re-snapshotted.
            invalidate_all("connection lost");
            return true;
        case ConnectionState::Connecting:
        case ConnectionState::Connected:
        case ConnectionState::SubscriptionSent:
            return false;
    }
    return false;
}

void OrderBookManager::invalidate_all(const std::string& reason) {
    {
        const std::lock_guard lock(mutex_);
        for (auto& [token, entry] : books_) {
            static_cast<void>(token);
            entry.book.clear();
            entry.seeded = false;
            entry.state.health = BookHealth::Recovering;
        }
        // Deliberately no resync requests here: the websocket will resubscribe
        // on reconnect and the venue replies with a fresh book for every token,
        // which is cheaper and more correct than N REST calls.
        pending_resyncs_.clear();
    }
    MDE_INFO("books_invalidated", field("reason", reason));
}

void OrderBookManager::run_maintenance() {
    std::vector<TokenId> to_resync;
    ResyncRequest handler;
    {
        const std::lock_guard lock(mutex_);
        const auto now = now_receive();
        const auto stale_after = std::chrono::milliseconds{config_.health.stale_after_ms};

        for (auto& [token, entry] : books_) {
            static_cast<void>(token);
            const auto quiet = now - entry.state.last_message_time;

            if (entry.state.health == BookHealth::Healthy &&
                entry.state.last_message_time.time_since_epoch().count() != 0 &&
                quiet > stale_after) {
                entry.state.health = BookHealth::Stale;
                MDE_DEBUG("book_stale", field("token", entry.state.token_id));
            }

            if (entry.state.health == BookHealth::Recovering ||
                entry.state.health == BookHealth::Invalid) {
                maybe_request_resync_locked(entry);
            }
        }
        to_resync.swap(pending_resyncs_);
        handler = resync_handler_;
    }

    if (handler) {
        for (const auto& token : to_resync) {
            handler(token);
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

MarketSnapshot OrderBookManager::build_snapshot_locked(const Entry& entry) const {
    MarketSnapshot snapshot;
    snapshot.token_id = entry.state.token_id;
    snapshot.market_id = entry.state.market_id;
    snapshot.timestamp = now_receive();
    snapshot.exchange_timestamp = entry.state.last_message_exchange_time;

    snapshot.book_health = to_string(entry.state.health);
    snapshot.sequence_number = entry.state.last_sequence;

    if (const auto bid = entry.book.best_bid_ticks()) {
        snapshot.best_bid_ticks = bid->price;
        snapshot.best_bid = ticks_to_price(bid->price);
        snapshot.best_bid_size = units_to_qty(bid->quantity);
    }
    if (const auto ask = entry.book.best_ask_ticks()) {
        snapshot.best_ask_ticks = ask->price;
        snapshot.best_ask = ticks_to_price(ask->price);
        snapshot.best_ask_size = units_to_qty(ask->quantity);
    }

    snapshot.midpoint = entry.book.midpoint();
    snapshot.spread = entry.book.spread();
    snapshot.bid_depth_l1 = entry.book.depth(Side::Bid, 1);
    snapshot.ask_depth_l1 = entry.book.depth(Side::Ask, 1);
    snapshot.bid_depth_l5 = entry.book.depth(Side::Bid, kDepthL5);
    snapshot.ask_depth_l5 = entry.book.depth(Side::Ask, kDepthL5);
    snapshot.bid_depth_l10 = entry.book.depth(Side::Bid, kDepthL10);
    snapshot.ask_depth_l10 = entry.book.depth(Side::Ask, kDepthL10);
    snapshot.imbalance_l1 = entry.book.imbalance(1);
    snapshot.imbalance_l5 = entry.book.imbalance(kDepthL5);
    snapshot.imbalance_l10 = entry.book.imbalance(kDepthL10);
    snapshot.bid_levels = entry.book.bid_level_count();
    snapshot.ask_levels = entry.book.ask_level_count();
    snapshot.bids = entry.book.top_bids(config_.order_book_depth);
    snapshot.asks = entry.book.top_asks(config_.order_book_depth);
    return snapshot;
}

std::optional<MarketSnapshot> OrderBookManager::build_snapshot(const TokenId& token) const {
    const std::lock_guard lock(mutex_);
    const auto it = books_.find(token);
    if (it == books_.end()) {
        return std::nullopt;
    }
    return build_snapshot_locked(it->second);
}

std::vector<MarketSnapshot> OrderBookManager::build_all_snapshots() const {
    const std::lock_guard lock(mutex_);
    std::vector<MarketSnapshot> out;
    out.reserve(books_.size());
    for (const auto& [token, entry] : books_) {
        static_cast<void>(token);
        // Only books we actually trust are published downstream; an unseeded
        // book would export a midpoint computed from nothing.
        if (entry.state.health == BookHealth::Healthy ||
            entry.state.health == BookHealth::Stale) {
            out.push_back(build_snapshot_locked(entry));
        }
    }
    return out;
}

std::optional<BookState> OrderBookManager::state(const TokenId& token) const {
    const std::lock_guard lock(mutex_);
    const auto it = books_.find(token);
    return it == books_.end() ? std::nullopt : std::optional<BookState>{it->second.state};
}

std::vector<BookState> OrderBookManager::all_states() const {
    const std::lock_guard lock(mutex_);
    std::vector<BookState> out;
    out.reserve(books_.size());
    for (const auto& [token, entry] : books_) {
        static_cast<void>(token);
        out.push_back(entry.state);
    }
    return out;
}

std::size_t OrderBookManager::book_count() const {
    const std::lock_guard lock(mutex_);
    return books_.size();
}

void OrderBookManager::health_counts(std::uint64_t& healthy, std::uint64_t& stale,
                                     std::uint64_t& invalid) const {
    healthy = stale = invalid = 0;
    const std::lock_guard lock(mutex_);
    for (const auto& [token, entry] : books_) {
        static_cast<void>(token);
        switch (entry.state.health) {
            case BookHealth::Healthy: ++healthy; break;
            case BookHealth::Stale: ++stale; break;
            case BookHealth::Invalid:
            case BookHealth::Recovering:
            case BookHealth::Initializing: ++invalid; break;
        }
    }
}

std::uint64_t OrderBookManager::total_crossed_incidents() const {
    const std::lock_guard lock(mutex_);
    std::uint64_t total = 0;
    for (const auto& [token, entry] : books_) {
        static_cast<void>(token);
        total += entry.book.stats().crossed_incidents;
    }
    return total;
}

}  // namespace market_data
