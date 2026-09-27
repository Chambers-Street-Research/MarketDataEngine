// Thread-safe index of market metadata and the token -> market mapping.
#pragma once

#include <cstddef>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "market_data/market.hpp"
#include "market_data/types.hpp"

namespace market_data {

// Owns every discovered Market and the derived lookups the rest of the engine
// needs. Discovery runs on the network thread while the book manager, snapshot
// writer and status printer read concurrently, so every method takes a mutex.
// Reads copy rather than return references: metadata is small, refreshed on the
// order of minutes, and a dangling reference across a refresh would be a
// genuinely nasty bug to chase.
class MarketRegistry {
public:
    // Inserts or replaces `market`, reindexing its tokens. Returns false if the
    // market is malformed (no id, or mismatched token/outcome counts).
    bool upsert(Market market);

    // Bulk form. Returns the number of markets accepted.
    std::size_t upsert_all(std::vector<Market> markets);

    [[nodiscard]] std::optional<Market> market_by_id(const MarketId& id) const;
    [[nodiscard]] std::optional<Market> market_for_token(const TokenId& token) const;

    [[nodiscard]] std::vector<TokenId> tokens_for_market(const MarketId& id) const;
    [[nodiscard]] std::optional<std::string> outcome_for_token(const TokenId& token) const;
    [[nodiscard]] std::optional<std::string> category_for_market(const MarketId& id) const;
    [[nodiscard]] std::optional<ExchangeTime> resolution_time_for_market(const MarketId& id) const;

    [[nodiscard]] bool has_token(const TokenId& token) const;

    [[nodiscard]] std::size_t market_count() const;
    [[nodiscard]] std::size_t token_count() const;

    [[nodiscard]] std::vector<Market> all_markets() const;

    // Markets that can actually be subscribed to, most liquid first. `limit` of
    // zero means no limit.
    [[nodiscard]] std::vector<Market> top_markets_by_volume(std::size_t limit) const;

    // Token ids of the top `limit` markets by volume, in market order. This is
    // what the engine hands to the adapter's subscribe().
    [[nodiscard]] std::vector<TokenId> top_tokens_by_volume(std::size_t market_limit) const;

    // Append a JSON Lines metadata snapshot to `path`, creating parent
    // directories as needed. Returns the number of markets written, or
    // std::nullopt on I/O failure.
    [[nodiscard]] std::optional<std::size_t> write_snapshot(const std::string& path) const;

    void clear();

private:
    // Called with mutex_ held.
    [[nodiscard]] std::vector<Market> snapshot_locked() const;

    mutable std::mutex mutex_;
    // Ordered by id so iteration (and therefore persisted snapshots and
    // volume-tie ordering) is deterministic across runs -- replay depends on it.
    std::map<MarketId, Market> markets_;
    std::unordered_map<TokenId, MarketId> token_to_market_;
};

}  // namespace market_data
