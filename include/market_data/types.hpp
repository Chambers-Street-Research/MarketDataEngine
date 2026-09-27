// Core value types shared by every component of the engine.
//
// Design note on numeric representation
// -------------------------------------
// Prices are held internally as integer *ticks* (PriceTicks) and quantities as
// integer *micro-units* (QtyUnits). Prediction-market prices live on a coarse,
// exactly-representable grid (Polymarket quotes on 0.01 or 0.001 boundaries),
// and sizes are USDC-denominated with at most six decimals. Mapping both onto
// integers means "did this level go to zero?" is an exact `== 0` test rather
// than an epsilon comparison, and two prices are equal iff their ticks are
// equal. Doubles are used only at the API boundary, where the outside world
// expects decimal prices.
#pragma once

#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace market_data {

using MarketId = std::string;
using TokenId = std::string;

// Wall-clock time reported by the exchange. Comparable across processes and
// across a restart, but subject to clock skew and NTP steps.
using ExchangeTime = std::chrono::system_clock::time_point;

// Local monotonic receipt time. Never jumps; used for latency and staleness.
using ReceiveTime = std::chrono::steady_clock::time_point;

enum class Side : std::uint8_t { Bid, Ask };

constexpr const char* to_string(Side s) noexcept {
    return s == Side::Bid ? "bid" : "ask";
}

// Trade-side label for persisted datasets: "buy", "sell" or "unknown".
inline const char* trade_side_label(std::optional<Side> side) noexcept {
    if (!side) {
        return "unknown";
    }
    return *side == Side::Bid ? "buy" : "sell";
}

constexpr Side opposite(Side s) noexcept {
    return s == Side::Bid ? Side::Ask : Side::Bid;
}

// ---------------------------------------------------------------------------
// Fixed-point scales
// ---------------------------------------------------------------------------

// Integer price on a 1/1000 grid. Polymarket's finest tick is 0.001, so every
// quotable price is representable exactly; a 0.01 tick is simply 10 ticks.
using PriceTicks = std::int32_t;

// Integer quantity in millionths of a unit (USDC has six decimals).
using QtyUnits = std::int64_t;

inline constexpr PriceTicks kPriceTicksPerUnit = 1000;
inline constexpr QtyUnits kQtyUnitsPerUnit = 1'000'000;

// Valid price range for a prediction-market outcome token: [0.0, 1.0].
inline constexpr PriceTicks kMinPriceTicks = 0;
inline constexpr PriceTicks kMaxPriceTicks = kPriceTicksPerUnit;

inline PriceTicks price_to_ticks(double price) noexcept {
    return static_cast<PriceTicks>(std::llround(price * kPriceTicksPerUnit));
}

constexpr double ticks_to_price(PriceTicks ticks) noexcept {
    return static_cast<double>(ticks) / kPriceTicksPerUnit;
}

inline QtyUnits qty_to_units(double qty) noexcept {
    return static_cast<QtyUnits>(std::llround(qty * kQtyUnitsPerUnit));
}

constexpr double units_to_qty(QtyUnits units) noexcept {
    return static_cast<double>(units) / kQtyUnitsPerUnit;
}

// True if `ticks` names a price a prediction market could actually quote.
constexpr bool is_valid_price(PriceTicks ticks) noexcept {
    return ticks >= kMinPriceTicks && ticks <= kMaxPriceTicks;
}

// ---------------------------------------------------------------------------
// Book and trade primitives
// ---------------------------------------------------------------------------

// A decimal view of one book level, used at the boundary of the engine
// (normalized output, snapshots, test fixtures).
struct PriceLevel {
    double price = 0.0;
    double quantity = 0.0;
};

// The exact integer form of a level, used inside the order book.
struct TickLevel {
    PriceTicks price = 0;
    QtyUnits quantity = 0;
};

inline PriceLevel to_price_level(TickLevel l) noexcept {
    return PriceLevel{ticks_to_price(l.price), units_to_qty(l.quantity)};
}

inline TickLevel to_tick_level(PriceLevel l) noexcept {
    return TickLevel{price_to_ticks(l.price), qty_to_units(l.quantity)};
}

struct Trade {
    TokenId token_id;
    MarketId market_id;
    // Venue outcome label; filled in by the engine from the registry.
    std::string outcome;

    double price = 0.0;
    double quantity = 0.0;

    // Taker direction. Absent when the venue did not report it -- a trade
    // whose side is unknown is recorded as unknown rather than guessed,
    // because a fabricated side silently corrupts trade-flow research.
    std::optional<Side> aggressor_side;

    // Venue trade identifier; empty when unreported.
    std::string trade_id;
    // Engine sequence of the event that carried this trade; 0 if unknown.
    std::uint64_t sequence_number = 0;

    ExchangeTime exchange_timestamp{};
    ReceiveTime receive_timestamp{};
};

struct BookUpdate {
    TokenId token_id;
    Side side = Side::Bid;
    double price = 0.0;
    // Absolute resting quantity at `price` after the update, not a delta.
    // Zero means the level was removed.
    double quantity = 0.0;
    ExchangeTime exchange_timestamp{};
    ReceiveTime receive_timestamp{};
};

struct MarketSnapshot {
    TokenId token_id;
    MarketId market_id;
    // Venue outcome label ("Yes"/"No"). Filled in by the engine from the
    // registry; empty when the token is not in the registry.
    std::string outcome;

    // Absent when that side of the book is empty.
    //
    // These optionals, not the doubles below, are the authoritative
    // "is there a quote?" signal: zero is a *valid* price on a prediction
    // market, so encoding absence as 0.0 would make "no bid" and "bid at
    // 0.000" indistinguishable to a researcher.
    std::optional<PriceTicks> best_bid_ticks;
    std::optional<PriceTicks> best_ask_ticks;

    // NaN when the corresponding side is empty.
    double best_bid = std::numeric_limits<double>::quiet_NaN();
    double best_ask = std::numeric_limits<double>::quiet_NaN();
    double best_bid_size = 0.0;
    double best_ask_size = 0.0;

    // NaN when either side of the book is empty.
    double midpoint = std::numeric_limits<double>::quiet_NaN();
    double spread = std::numeric_limits<double>::quiet_NaN();

    double bid_depth_l1 = 0.0;
    double ask_depth_l1 = 0.0;
    double bid_depth_l5 = 0.0;
    double ask_depth_l5 = 0.0;
    double bid_depth_l10 = 0.0;
    double ask_depth_l10 = 0.0;

    // (bid - ask) / (bid + ask) over the top N levels; NaN when both sides
    // are empty, so a guarded division never yields a fake zero.
    double imbalance_l1 = std::numeric_limits<double>::quiet_NaN();
    double imbalance_l5 = std::numeric_limits<double>::quiet_NaN();
    double imbalance_l10 = std::numeric_limits<double>::quiet_NaN();

    // True resting level counts, independent of the display depth below.
    std::size_t bid_levels = 0;
    std::size_t ask_levels = 0;

    // Health of the book this snapshot was taken from, so a researcher can
    // filter out anything the engine did not fully trust.
    std::string book_health;
    // Sequence of the last event applied to the book; 0 when unknown.
    std::uint64_t sequence_number = 0;

    // Truncated to the configured display depth.
    std::vector<PriceLevel> bids;
    std::vector<PriceLevel> asks;

    ExchangeTime exchange_timestamp{};
    ReceiveTime timestamp{};
};

}  // namespace market_data
