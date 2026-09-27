#include "market_data/market_registry.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <system_error>

#include <nlohmann/json.hpp>

#include "market_data/time_util.hpp"

namespace market_data {
namespace {

nlohmann::json market_to_json(const Market& m) {
    nlohmann::json j{
        {"market_id", m.market_id},
        {"question", m.question},
        {"category", m.category},
        {"slug", m.slug},
        {"token_ids", m.token_ids},
        {"outcomes", m.outcomes},
        {"active", m.active},
        {"closed", m.closed},
        {"accepting_orders", m.accepting_orders},
        {"reported_volume", m.reported_volume},
        {"reported_liquidity", m.reported_liquidity},
        {"reported_volume_24h", m.reported_volume_24h},
        {"min_tick_size", m.min_tick_size},
        {"fetched_at_ms", to_epoch_millis(m.fetched_at)},
    };
    if (m.resolution_time) {
        j["resolution_time_ms"] = to_epoch_millis(*m.resolution_time);
    } else {
        j["resolution_time_ms"] = nullptr;
    }
    return j;
}

}  // namespace

bool MarketRegistry::upsert(Market market) {
    if (market.market_id.empty() || market.token_ids.empty()) {
        return false;
    }
    if (!market.outcomes.empty() && market.outcomes.size() != market.token_ids.size()) {
        // A token/outcome mismatch would silently mislabel every later lookup.
        return false;
    }

    const std::lock_guard lock(mutex_);

    // Drop stale token bindings first: a market can be re-deployed with a
    // different token set, and a leftover entry would route data to the wrong
    // outcome.
    if (const auto existing = markets_.find(market.market_id); existing != markets_.end()) {
        for (const auto& token : existing->second.token_ids) {
            if (const auto it = token_to_market_.find(token);
                it != token_to_market_.end() && it->second == market.market_id) {
                token_to_market_.erase(it);
            }
        }
    }

    for (const auto& token : market.token_ids) {
        if (!token.empty()) {
            token_to_market_[token] = market.market_id;
        }
    }
    markets_.insert_or_assign(market.market_id, std::move(market));
    return true;
}

std::size_t MarketRegistry::upsert_all(std::vector<Market> markets) {
    std::size_t accepted = 0;
    for (auto& market : markets) {
        if (upsert(std::move(market))) {
            ++accepted;
        }
    }
    return accepted;
}

std::optional<Market> MarketRegistry::market_by_id(const MarketId& id) const {
    const std::lock_guard lock(mutex_);
    const auto it = markets_.find(id);
    return it == markets_.end() ? std::nullopt : std::optional<Market>{it->second};
}

std::optional<Market> MarketRegistry::market_for_token(const TokenId& token) const {
    const std::lock_guard lock(mutex_);
    const auto mapped = token_to_market_.find(token);
    if (mapped == token_to_market_.end()) {
        return std::nullopt;
    }
    const auto it = markets_.find(mapped->second);
    return it == markets_.end() ? std::nullopt : std::optional<Market>{it->second};
}

std::vector<TokenId> MarketRegistry::tokens_for_market(const MarketId& id) const {
    const std::lock_guard lock(mutex_);
    const auto it = markets_.find(id);
    return it == markets_.end() ? std::vector<TokenId>{} : it->second.token_ids;
}

std::optional<std::string> MarketRegistry::outcome_for_token(const TokenId& token) const {
    const std::lock_guard lock(mutex_);
    const auto mapped = token_to_market_.find(token);
    if (mapped == token_to_market_.end()) {
        return std::nullopt;
    }
    const auto it = markets_.find(mapped->second);
    if (it == markets_.end()) {
        return std::nullopt;
    }
    auto outcome = it->second.outcome_for(token);
    if (outcome.empty()) {
        return std::nullopt;
    }
    return outcome;
}

std::optional<std::string> MarketRegistry::category_for_market(const MarketId& id) const {
    const std::lock_guard lock(mutex_);
    const auto it = markets_.find(id);
    return it == markets_.end() ? std::nullopt : std::optional<std::string>{it->second.category};
}

std::optional<ExchangeTime> MarketRegistry::resolution_time_for_market(const MarketId& id) const {
    const std::lock_guard lock(mutex_);
    const auto it = markets_.find(id);
    return it == markets_.end() ? std::nullopt : it->second.resolution_time;
}

bool MarketRegistry::has_token(const TokenId& token) const {
    const std::lock_guard lock(mutex_);
    return token_to_market_.count(token) > 0;
}

std::size_t MarketRegistry::market_count() const {
    const std::lock_guard lock(mutex_);
    return markets_.size();
}

std::size_t MarketRegistry::token_count() const {
    const std::lock_guard lock(mutex_);
    return token_to_market_.size();
}

std::vector<Market> MarketRegistry::snapshot_locked() const {
    std::vector<Market> out;
    out.reserve(markets_.size());
    for (const auto& [id, market] : markets_) {
        static_cast<void>(id);
        out.push_back(market);
    }
    return out;
}

std::vector<Market> MarketRegistry::all_markets() const {
    const std::lock_guard lock(mutex_);
    return snapshot_locked();
}

std::vector<Market> MarketRegistry::top_markets_by_volume(std::size_t limit) const {
    std::vector<Market> candidates;
    {
        const std::lock_guard lock(mutex_);
        for (const auto& [id, market] : markets_) {
            static_cast<void>(id);
            if (market.is_subscribable()) {
                candidates.push_back(market);
            }
        }
    }

    // Stable sort over an id-ordered input, with market_id breaking volume
    // ties, so the selected token set is identical across runs given identical
    // input. Determinism here is what makes a replay comparable to the live run.
    std::stable_sort(candidates.begin(), candidates.end(),
                     [](const Market& a, const Market& b) {
                         if (a.reported_volume != b.reported_volume) {
                             return a.reported_volume > b.reported_volume;
                         }
                         return a.market_id < b.market_id;
                     });

    if (limit > 0 && candidates.size() > limit) {
        candidates.resize(limit);
    }
    return candidates;
}

std::vector<TokenId> MarketRegistry::top_tokens_by_volume(std::size_t market_limit) const {
    std::vector<TokenId> tokens;
    for (const auto& market : top_markets_by_volume(market_limit)) {
        for (const auto& token : market.token_ids) {
            if (!token.empty()) {
                tokens.push_back(token);
            }
        }
    }
    return tokens;
}

std::optional<std::size_t> MarketRegistry::write_snapshot(const std::string& path) const {
    const auto markets = all_markets();

    std::error_code ec;
    const std::filesystem::path file{path};
    if (file.has_parent_path()) {
        std::filesystem::create_directories(file.parent_path(), ec);
        if (ec) {
            return std::nullopt;
        }
    }

    // Write to a sibling temp file and rename, so a crash mid-write leaves the
    // previous snapshot intact rather than a truncated one.
    const std::filesystem::path tmp = file.string() + ".partial";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) {
            return std::nullopt;
        }
        for (const auto& market : markets) {
            out << market_to_json(market).dump() << '\n';
        }
        out.flush();
        if (!out) {
            return std::nullopt;
        }
    }

    std::filesystem::rename(tmp, file, ec);
    if (ec) {
        std::filesystem::remove(tmp, ec);
        return std::nullopt;
    }
    return markets.size();
}

void MarketRegistry::clear() {
    const std::lock_guard lock(mutex_);
    markets_.clear();
    token_to_market_.clear();
}

}  // namespace market_data
