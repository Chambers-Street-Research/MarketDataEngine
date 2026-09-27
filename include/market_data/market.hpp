// Venue-agnostic market metadata.
#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "market_data/types.hpp"

namespace market_data {

// One tradeable market and its outcome tokens. A binary Polymarket market has
// two tokens ("Yes"/"No"); token_ids[i] is the token for outcomes[i], and the
// two vectors are always the same length by construction.
struct Market {
    MarketId market_id;      // Venue condition id.
    std::string question;
    std::string category;    // Derived from the venue's grouping; may be empty.
    std::string slug;

    std::vector<TokenId> token_ids;
    std::vector<std::string> outcomes;

    bool active = false;
    bool closed = false;
    // Whether the venue is currently matching orders on this market. A market
    // can be active-but-not-accepting during deployment or resolution.
    bool accepting_orders = false;

    std::optional<ExchangeTime> resolution_time;

    double reported_volume = 0.0;
    double reported_liquidity = 0.0;
    double reported_volume_24h = 0.0;

    // Venue-declared minimum price increment, in decimal form (0.01 or 0.001
    // on Polymarket). Zero when the venue did not report one.
    double min_tick_size = 0.0;

    ExchangeTime fetched_at{};

    [[nodiscard]] bool is_subscribable() const {
        return active && !closed && !token_ids.empty();
    }

    // Outcome label paired with `token`, or empty if the token is not ours.
    [[nodiscard]] std::string outcome_for(const TokenId& token) const {
        for (std::size_t i = 0; i < token_ids.size(); ++i) {
            if (token_ids[i] == token) {
                return i < outcomes.size() ? outcomes[i] : std::string{};
            }
        }
        return {};
    }
};

}  // namespace market_data
