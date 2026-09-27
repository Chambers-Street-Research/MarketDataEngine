#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

#include <nlohmann/json.hpp>

#include "market_data/market_registry.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

Market make_market(MarketId id, std::vector<TokenId> tokens, double volume) {
    Market m;
    m.market_id = std::move(id);
    m.question = "Will it rain?";
    m.category = "weather";
    m.token_ids = std::move(tokens);
    m.outcomes.assign(m.token_ids.size(), "Yes");
    if (m.outcomes.size() == 2) {
        m.outcomes = {"Yes", "No"};
    }
    m.active = true;
    m.closed = false;
    m.accepting_orders = true;
    m.reported_volume = volume;
    m.resolution_time = from_epoch_millis(1'800'000'000'000);
    return m;
}

TEST(MarketRegistryTest, UpsertIndexesEveryTokenBackToItsMarket) {
    MarketRegistry registry;
    ASSERT_TRUE(registry.upsert(make_market("0xaa", {"t1", "t2"}, 100.0)));

    EXPECT_EQ(registry.market_count(), 1u);
    EXPECT_EQ(registry.token_count(), 2u);
    EXPECT_TRUE(registry.has_token("t1"));
    EXPECT_FALSE(registry.has_token("nope"));

    const auto market = registry.market_for_token("t2");
    ASSERT_TRUE(market.has_value());
    EXPECT_EQ(market->market_id, "0xaa");
}

TEST(MarketRegistryTest, RejectsMalformedMarkets) {
    MarketRegistry registry;
    EXPECT_FALSE(registry.upsert(make_market("", {"t1"}, 1.0)));
    EXPECT_FALSE(registry.upsert(make_market("0xaa", {}, 1.0)));

    Market mismatched = make_market("0xbb", {"t1", "t2"}, 1.0);
    mismatched.outcomes = {"Yes"};
    EXPECT_FALSE(registry.upsert(std::move(mismatched)));

    EXPECT_EQ(registry.market_count(), 0u);
}

TEST(MarketRegistryTest, AllLookupsResolve) {
    MarketRegistry registry;
    ASSERT_TRUE(registry.upsert(make_market("0xaa", {"t1", "t2"}, 100.0)));

    EXPECT_EQ(registry.tokens_for_market("0xaa"), (std::vector<TokenId>{"t1", "t2"}));
    EXPECT_EQ(registry.outcome_for_token("t1").value_or(""), "Yes");
    EXPECT_EQ(registry.outcome_for_token("t2").value_or(""), "No");
    EXPECT_EQ(registry.category_for_market("0xaa").value_or(""), "weather");

    const auto resolution = registry.resolution_time_for_market("0xaa");
    ASSERT_TRUE(resolution.has_value());
    EXPECT_EQ(to_epoch_millis(*resolution), 1'800'000'000'000LL);

    EXPECT_FALSE(registry.market_by_id("missing").has_value());
    EXPECT_FALSE(registry.outcome_for_token("missing").has_value());
    EXPECT_TRUE(registry.tokens_for_market("missing").empty());
}

TEST(MarketRegistryTest, ReupsertWithNewTokensRetiresStaleBindings) {
    MarketRegistry registry;
    ASSERT_TRUE(registry.upsert(make_market("0xaa", {"old1", "old2"}, 100.0)));
    ASSERT_TRUE(registry.upsert(make_market("0xaa", {"new1", "new2"}, 150.0)));

    // A leftover binding would route live data for a retired token into the
    // wrong outcome, so it must be gone entirely.
    EXPECT_FALSE(registry.has_token("old1"));
    EXPECT_TRUE(registry.has_token("new1"));
    EXPECT_EQ(registry.token_count(), 2u);
    EXPECT_EQ(registry.market_count(), 1u);
    EXPECT_DOUBLE_EQ(registry.market_by_id("0xaa")->reported_volume, 150.0);
}

TEST(MarketRegistryTest, TopMarketsRankByVolumeAndExcludeUnsubscribable) {
    MarketRegistry registry;
    registry.upsert(make_market("0xlow", {"a"}, 10.0));
    registry.upsert(make_market("0xhigh", {"b"}, 900.0));
    registry.upsert(make_market("0xmid", {"c"}, 500.0));

    Market closed = make_market("0xclosed", {"d"}, 10'000.0);
    closed.closed = true;
    registry.upsert(std::move(closed));

    Market inactive = make_market("0xinactive", {"e"}, 10'000.0);
    inactive.active = false;
    registry.upsert(std::move(inactive));

    const auto top = registry.top_markets_by_volume(0);
    ASSERT_EQ(top.size(), 3u);
    EXPECT_EQ(top[0].market_id, "0xhigh");
    EXPECT_EQ(top[1].market_id, "0xmid");
    EXPECT_EQ(top[2].market_id, "0xlow");

    EXPECT_EQ(registry.top_markets_by_volume(2).size(), 2u);
}

TEST(MarketRegistryTest, VolumeTiesBreakDeterministicallyById) {
    MarketRegistry registry;
    registry.upsert(make_market("0xccc", {"c"}, 42.0));
    registry.upsert(make_market("0xaaa", {"a"}, 42.0));
    registry.upsert(make_market("0xbbb", {"b"}, 42.0));

    // Replay is only comparable to a live run if the subscription set is
    // reproducible, which means ties cannot depend on insertion order.
    const auto first = registry.top_markets_by_volume(0);
    const auto second = registry.top_markets_by_volume(0);
    ASSERT_EQ(first.size(), 3u);
    EXPECT_EQ(first[0].market_id, "0xaaa");
    EXPECT_EQ(first[1].market_id, "0xbbb");
    EXPECT_EQ(first[2].market_id, "0xccc");
    for (std::size_t i = 0; i < first.size(); ++i) {
        EXPECT_EQ(first[i].market_id, second[i].market_id);
    }
}

TEST(MarketRegistryTest, TopTokensFlattensInMarketOrder) {
    MarketRegistry registry;
    registry.upsert(make_market("0xhigh", {"h1", "h2"}, 900.0));
    registry.upsert(make_market("0xlow", {"l1", "l2"}, 10.0));

    EXPECT_EQ(registry.top_tokens_by_volume(0),
              (std::vector<TokenId>{"h1", "h2", "l1", "l2"}));
    EXPECT_EQ(registry.top_tokens_by_volume(1), (std::vector<TokenId>{"h1", "h2"}));
}

TEST(MarketRegistryTest, SnapshotRoundTripsThroughJsonLines) {
    MarketRegistry registry;
    registry.upsert(make_market("0xaa", {"t1", "t2"}, 100.0));
    registry.upsert(make_market("0xbb", {"t3"}, 7.5));

    const auto dir = std::filesystem::temp_directory_path() / "mde_registry_test";
    std::filesystem::remove_all(dir);
    const auto path = dir / "nested" / "markets.jsonl";

    const auto written = registry.write_snapshot(path.string());
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, 2u);
    ASSERT_TRUE(std::filesystem::exists(path));
    // The temp file used for the atomic rename must not be left behind.
    EXPECT_FALSE(std::filesystem::exists(path.string() + ".partial"));

    std::ifstream in(path);
    std::string line;
    std::size_t lines = 0;
    while (std::getline(in, line)) {
        const auto parsed = nlohmann::json::parse(line, nullptr, false);
        ASSERT_FALSE(parsed.is_discarded());
        EXPECT_TRUE(parsed.contains("market_id"));
        EXPECT_TRUE(parsed.contains("token_ids"));
        EXPECT_TRUE(parsed.contains("resolution_time_ms"));
        ++lines;
    }
    EXPECT_EQ(lines, 2u);

    std::filesystem::remove_all(dir);
}

TEST(MarketRegistryTest, ClearEmptiesBothIndexes) {
    MarketRegistry registry;
    registry.upsert(make_market("0xaa", {"t1", "t2"}, 100.0));
    registry.clear();
    EXPECT_EQ(registry.market_count(), 0u);
    EXPECT_EQ(registry.token_count(), 0u);
    EXPECT_FALSE(registry.has_token("t1"));
}

}  // namespace
}  // namespace market_data
