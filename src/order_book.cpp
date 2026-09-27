#include "market_data/order_book.hpp"

#include <algorithm>
#include <limits>

namespace market_data {
namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

}  // namespace

// ---------------------------------------------------------------------------
// Mutation
// ---------------------------------------------------------------------------

template <typename Ladder>
bool OrderBook::apply_to_ladder(Ladder& ladder, PriceTicks price, QtyUnits quantity) {
    if (!is_valid_price(price) || quantity < 0) {
        ++stats_.invalid_updates;
        return false;
    }

    if (quantity == 0) {
        // Exact integer comparison: no epsilon, no level lingering at 1e-18.
        if (ladder.erase(price) == 0) {
            ++stats_.phantom_deletes;
        }
    } else {
        ladder.insert_or_assign(price, quantity);
    }
    return true;
}

void OrderBook::apply_snapshot(const std::vector<TickLevel>& bids,
                               const std::vector<TickLevel>& asks) {
    bids_.clear();
    asks_.clear();

    for (const auto& level : bids) {
        if (!is_valid_price(level.price) || level.quantity < 0) {
            ++stats_.invalid_updates;
            continue;
        }
        if (level.quantity > 0) {
            bids_.insert_or_assign(level.price, level.quantity);
        }
    }
    for (const auto& level : asks) {
        if (!is_valid_price(level.price) || level.quantity < 0) {
            ++stats_.invalid_updates;
            continue;
        }
        if (level.quantity > 0) {
            asks_.insert_or_assign(level.price, level.quantity);
        }
    }

    ++stats_.snapshots_applied;
    // A snapshot replaces state wholesale, so any cross it contains is a new
    // incident regardless of what the previous book looked like.
    note_cross_transition(false);
}

void OrderBook::apply_snapshot(const std::vector<PriceLevel>& bids,
                               const std::vector<PriceLevel>& asks) {
    std::vector<TickLevel> tick_bids;
    std::vector<TickLevel> tick_asks;
    tick_bids.reserve(bids.size());
    tick_asks.reserve(asks.size());
    std::transform(bids.begin(), bids.end(), std::back_inserter(tick_bids), to_tick_level);
    std::transform(asks.begin(), asks.end(), std::back_inserter(tick_asks), to_tick_level);
    apply_snapshot(tick_bids, tick_asks);
}

bool OrderBook::apply_update(Side side, PriceTicks price, QtyUnits quantity) {
    const bool was_crossed = crossed();

    const bool ok = (side == Side::Bid) ? apply_to_ladder(bids_, price, quantity)
                                        : apply_to_ladder(asks_, price, quantity);
    if (!ok) {
        return false;
    }

    ++stats_.updates_applied;
    note_cross_transition(was_crossed);
    return true;
}

bool OrderBook::apply_update(Side side, double price, double quantity) {
    return apply_update(side, price_to_ticks(price), qty_to_units(quantity));
}

void OrderBook::note_cross_transition(bool was_crossed) {
    if (crossed() && !was_crossed) {
        ++stats_.crossed_incidents;
    }
}

void OrderBook::clear() {
    bids_.clear();
    asks_.clear();
}

// ---------------------------------------------------------------------------
// Inspection
// ---------------------------------------------------------------------------

std::optional<TickLevel> OrderBook::best_bid_ticks() const {
    if (bids_.empty()) {
        return std::nullopt;
    }
    const auto& [price, qty] = *bids_.begin();
    return TickLevel{price, qty};
}

std::optional<TickLevel> OrderBook::best_ask_ticks() const {
    if (asks_.empty()) {
        return std::nullopt;
    }
    const auto& [price, qty] = *asks_.begin();
    return TickLevel{price, qty};
}

std::optional<PriceLevel> OrderBook::best_bid() const {
    const auto level = best_bid_ticks();
    return level ? std::optional<PriceLevel>{to_price_level(*level)} : std::nullopt;
}

std::optional<PriceLevel> OrderBook::best_ask() const {
    const auto level = best_ask_ticks();
    return level ? std::optional<PriceLevel>{to_price_level(*level)} : std::nullopt;
}

double OrderBook::midpoint() const {
    const auto bid = best_bid_ticks();
    const auto ask = best_ask_ticks();
    if (!bid || !ask) {
        return kNaN;
    }
    // Averaged in tick space, then converted once, so a half-tick midpoint is
    // exact rather than the sum of two rounding errors.
    return static_cast<double>(bid->price + ask->price) / (2.0 * kPriceTicksPerUnit);
}

double OrderBook::spread() const {
    const auto bid = best_bid_ticks();
    const auto ask = best_ask_ticks();
    if (!bid || !ask) {
        return kNaN;
    }
    return ticks_to_price(ask->price - bid->price);
}

template <typename Ladder>
std::vector<PriceLevel> OrderBook::take_top(const Ladder& ladder, std::size_t n) {
    std::vector<PriceLevel> out;
    out.reserve(std::min(n, ladder.size()));
    for (const auto& [price, qty] : ladder) {
        if (out.size() >= n) {
            break;
        }
        out.push_back(to_price_level(TickLevel{price, qty}));
    }
    return out;
}

template <typename Ladder>
QtyUnits OrderBook::sum_top(const Ladder& ladder, std::size_t n) {
    QtyUnits total = 0;
    std::size_t seen = 0;
    for (const auto& [price, qty] : ladder) {
        if (seen >= n) {
            break;
        }
        static_cast<void>(price);
        total += qty;
        ++seen;
    }
    return total;
}

std::vector<PriceLevel> OrderBook::top_bids(std::size_t n) const {
    return take_top(bids_, n);
}

std::vector<PriceLevel> OrderBook::top_asks(std::size_t n) const {
    return take_top(asks_, n);
}

double OrderBook::depth(Side side, std::size_t n) const {
    return units_to_qty(side == Side::Bid ? sum_top(bids_, n) : sum_top(asks_, n));
}

double OrderBook::imbalance(std::size_t n) const {
    const QtyUnits bid = sum_top(bids_, n);
    const QtyUnits ask = sum_top(asks_, n);
    const QtyUnits total = bid + ask;
    if (total == 0) {
        // Both sides empty (or zero-sized): imbalance is undefined, not zero.
        return kNaN;
    }
    return static_cast<double>(bid - ask) / static_cast<double>(total);
}

bool OrderBook::crossed() const {
    if (bids_.empty() || asks_.empty()) {
        return false;
    }
    return bids_.begin()->first >= asks_.begin()->first;
}

}  // namespace market_data
