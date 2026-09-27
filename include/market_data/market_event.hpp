// The normalized event model. Everything downstream of an exchange adapter
// speaks these types and only these types -- no venue JSON escapes the adapter.
#pragma once

#include <cstdint>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "market_data/types.hpp"

namespace market_data {

// Fields every normalized event carries. Both clocks are preserved: the
// exchange stamp for cross-venue ordering and latency analysis, the local
// steady stamp for staleness checks and for replay, which must not be
// perturbed by an NTP step.
struct EventHeader {
    std::string exchange;       // "polymarket"
    MarketId market_id;
    TokenId token_id;
    ExchangeTime exchange_timestamp{};
    ReceiveTime receive_timestamp{};
    // Engine-assigned total order. Stamped once, on the single ingest thread,
    // which is what makes a recorded session deterministically replayable.
    std::uint64_t sequence = 0;
    // Venue-supplied book fingerprint when available; empty otherwise. Used to
    // detect duplicate snapshots cheaply.
    std::string venue_hash;
};

// A full replacement of the book for one token.
struct BookSnapshotEvent {
    EventHeader header;
    std::vector<PriceLevel> bids;
    std::vector<PriceLevel> asks;
    // Venue minimum tick at the time of the snapshot, 0 if unreported.
    double tick_size = 0.0;
};

// One or more absolute level replacements for one token. Polymarket batches
// several level changes into a single price_change frame; keeping them
// together preserves the venue's own atomicity.
struct BookUpdateEvent {
    EventHeader header;
    std::vector<BookUpdate> updates;
};

struct TradeEvent {
    EventHeader header;
    Trade trade;
};

struct MarketMetadataEvent {
    EventHeader header;
    std::string question;
    std::string category;
    std::vector<TokenId> token_ids;
    std::vector<std::string> outcomes;
    bool active = false;
    bool closed = false;
    double tick_size = 0.0;
};

enum class ConnectionState : std::uint8_t {
    Connecting,
    Connected,
    SubscriptionSent,
    Disconnected,
    Reconnecting,
    Failed,
};

constexpr const char* to_string(ConnectionState s) noexcept {
    switch (s) {
        case ConnectionState::Connecting: return "connecting";
        case ConnectionState::Connected: return "connected";
        case ConnectionState::SubscriptionSent: return "subscription_sent";
        case ConnectionState::Disconnected: return "disconnected";
        case ConnectionState::Reconnecting: return "reconnecting";
        case ConnectionState::Failed: return "failed";
    }
    return "unknown";
}

// Connection lifecycle, recorded inline with market data so a replay can see
// exactly where a gap in the feed begins and ends.
struct ConnectionEvent {
    EventHeader header;  // token_id/market_id are empty: this is connection-wide.
    ConnectionState state = ConnectionState::Disconnected;
    std::string detail;
    std::uint32_t attempt = 0;
    std::size_t subscribed_tokens = 0;
};

using MarketEvent = std::variant<BookSnapshotEvent,
                                 BookUpdateEvent,
                                 TradeEvent,
                                 MarketMetadataEvent,
                                 ConnectionEvent>;

// Header access without the caller writing a visitor every time.
inline const EventHeader& header_of(const MarketEvent& event) {
    return std::visit([](const auto& e) -> const EventHeader& { return e.header; }, event);
}

inline EventHeader& header_of(MarketEvent& event) {
    return std::visit([](auto& e) -> EventHeader& { return e.header; }, event);
}

inline const char* event_type_name(const MarketEvent& event) {
    return std::visit(
        []<typename T>(const T&) -> const char* {
            using E = std::decay_t<T>;
            if constexpr (std::is_same_v<E, BookSnapshotEvent>) return "book_snapshot";
            else if constexpr (std::is_same_v<E, BookUpdateEvent>) return "book_update";
            else if constexpr (std::is_same_v<E, TradeEvent>) return "trade";
            else if constexpr (std::is_same_v<E, MarketMetadataEvent>) return "market_metadata";
            else return "connection";
        },
        event);
}

}  // namespace market_data
