// L2 order book for a single outcome token.
//
// Container choice
// ----------------
// Each side is a std::map keyed by integer price ticks, with the bid side
// ordered descending via std::greater. That gives:
//
//   * best-of-book in O(1)            -- it is always begin()
//   * top-N levels in O(n)            -- ordered iteration, no sorting
//   * level insert/update/erase in O(log n)
//   * stable iterators and no rehashing
//
// The alternative, an unordered_map plus cached best prices, makes the common
// case faster but turns "the best level was just deleted" into a full scan, and
// top-N into a sort. Prediction-market books are shallow (tens of levels) and
// update at modest rates, so the ordered map wins on correctness-per-line.
//
// The optimization path, once measurements justify it: prices are bounded to
// 0..1000 ticks, so the whole side fits in a fixed 1001-slot array ladder with
// O(1) update and a bitset to find best-of-book. See docs/architecture.md.
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <vector>

#include "market_data/types.hpp"

namespace market_data {

// Counters describing how a book has behaved since construction (or the last
// reset). Consumed by the health tracker and the metrics registry.
struct OrderBookStats {
    std::uint64_t snapshots_applied = 0;
    std::uint64_t updates_applied = 0;
    // Updates rejected outright: out-of-range price, negative quantity.
    std::uint64_t invalid_updates = 0;
    // Updates that deleted a level which was not present. Harmless in
    // isolation, but a run of them means we missed a snapshot.
    std::uint64_t phantom_deletes = 0;
    // Transitions from an uncrossed to a crossed state.
    std::uint64_t crossed_incidents = 0;
};

class OrderBook {
public:
    using BidLadder = std::map<PriceTicks, QtyUnits, std::greater<PriceTicks>>;
    using AskLadder = std::map<PriceTicks, QtyUnits, std::less<PriceTicks>>;

    OrderBook() = default;

    // Replace the entire book. Levels with non-positive quantity or an
    // out-of-range price are skipped and counted as invalid.
    void apply_snapshot(const std::vector<PriceLevel>& bids,
                        const std::vector<PriceLevel>& asks);
    void apply_snapshot(const std::vector<TickLevel>& bids,
                        const std::vector<TickLevel>& asks);

    // Set the resting quantity at `price` to `quantity` (an absolute value, not
    // a delta). A quantity of zero removes the level. Returns false if the
    // update was rejected as invalid.
    //
    // An update that crosses the book is still applied: the feed is reporting
    // real state, and hiding it would mask the need to resync. Callers should
    // consult crossed() and drive the book to a recovering state themselves.
    bool apply_update(Side side, PriceTicks price, QtyUnits quantity);
    bool apply_update(Side side, double price, double quantity);

    [[nodiscard]] std::optional<TickLevel> best_bid_ticks() const;
    [[nodiscard]] std::optional<TickLevel> best_ask_ticks() const;
    [[nodiscard]] std::optional<PriceLevel> best_bid() const;
    [[nodiscard]] std::optional<PriceLevel> best_ask() const;

    // NaN when either side is empty, so an undefined midpoint can never be
    // mistaken for a real price of zero.
    [[nodiscard]] double midpoint() const;
    [[nodiscard]] double spread() const;

    [[nodiscard]] std::vector<PriceLevel> top_bids(std::size_t n) const;
    [[nodiscard]] std::vector<PriceLevel> top_asks(std::size_t n) const;

    // Summed quantity of the best `n` levels on one side.
    [[nodiscard]] double depth(Side side, std::size_t n) const;

    // (bid_depth - ask_depth) / (bid_depth + ask_depth) over the top `n`
    // levels. NaN when both sides are empty, so no division by zero.
    [[nodiscard]] double imbalance(std::size_t n) const;

    // True when the best bid is at or above the best ask. A locked book
    // (bid == ask) counts: it is not quotable state on this venue.
    [[nodiscard]] bool crossed() const;

    [[nodiscard]] bool empty() const { return bids_.empty() && asks_.empty(); }
    [[nodiscard]] std::size_t bid_level_count() const { return bids_.size(); }
    [[nodiscard]] std::size_t ask_level_count() const { return asks_.size(); }

    [[nodiscard]] const BidLadder& bids() const { return bids_; }
    [[nodiscard]] const AskLadder& asks() const { return asks_; }

    [[nodiscard]] const OrderBookStats& stats() const { return stats_; }

    // Drop all levels but keep the statistics; used when a book is invalidated
    // and awaits a fresh snapshot.
    void clear();

private:
    template <typename Ladder>
    bool apply_to_ladder(Ladder& ladder, PriceTicks price, QtyUnits quantity);

    template <typename Ladder>
    static std::vector<PriceLevel> take_top(const Ladder& ladder, std::size_t n);

    template <typename Ladder>
    static QtyUnits sum_top(const Ladder& ladder, std::size_t n);

    // Recompute crossed state and count a fresh incident if we just crossed.
    void note_cross_transition(bool was_crossed);

    BidLadder bids_;
    AskLadder asks_;
    OrderBookStats stats_;
};

}  // namespace market_data
