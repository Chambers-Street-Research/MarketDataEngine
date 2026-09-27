// Health state machine tests. The invariant under test: a book is never
// treated as Healthy on the strength of incremental updates alone.
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "market_data/order_book_manager.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

Config test_config(std::uint32_t stale_after_ms = 60'000) {
    Config config;
    config.order_book_depth = 20;
    config.health.stale_after_ms = stale_after_ms;
    config.health.resync_cooldown_ms = 0;
    return config;
}

EventHeader header(const TokenId& token) {
    EventHeader h;
    h.exchange = "polymarket";
    h.market_id = "0xmarket";
    h.token_id = token;
    h.exchange_timestamp = now_exchange();
    h.receive_timestamp = now_receive();
    return h;
}

MarketEvent snapshot(const TokenId& token,
                     std::vector<PriceLevel> bids,
                     std::vector<PriceLevel> asks) {
    BookSnapshotEvent event;
    event.header = header(token);
    event.bids = std::move(bids);
    event.asks = std::move(asks);
    return MarketEvent{std::move(event)};
}

MarketEvent update(const TokenId& token, Side side, double price, double quantity) {
    BookUpdateEvent event;
    event.header = header(token);
    event.updates.push_back(BookUpdate{token, side, price, quantity, {}, {}});
    return MarketEvent{std::move(event)};
}

MarketEvent connection(ConnectionState state) {
    ConnectionEvent event;
    event.header = header("");
    event.state = state;
    return MarketEvent{std::move(event)};
}

TEST(OrderBookManagerTest, TrackedTokensStartInitializing) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a", "b"});

    EXPECT_EQ(manager.book_count(), 2u);
    ASSERT_TRUE(manager.state("a").has_value());
    EXPECT_EQ(manager.state("a")->health, BookHealth::Initializing);
    // An unseeded book publishes nothing.
    EXPECT_TRUE(manager.build_all_snapshots().empty());
}

TEST(OrderBookManagerTest, SnapshotMakesABookHealthy) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));

    EXPECT_EQ(manager.state("a")->health, BookHealth::Healthy);

    const auto snapshots = manager.build_all_snapshots();
    ASSERT_EQ(snapshots.size(), 1u);
    EXPECT_DOUBLE_EQ(snapshots[0].best_bid, 0.49);
    EXPECT_DOUBLE_EQ(snapshots[0].midpoint, 0.50);
    EXPECT_EQ(snapshots[0].market_id, "0xmarket");
}

TEST(OrderBookManagerTest, UpdatesBeforeASnapshotAreRefusedAndTriggerResync) {
    std::vector<TokenId> resyncs;
    OrderBookManager manager(test_config());
    manager.set_resync_handler([&resyncs](const TokenId& t) { resyncs.push_back(t); });
    manager.set_tracked_tokens({"a"});

    EXPECT_FALSE(manager.apply(update("a", Side::Bid, 0.49, 100.0)));

    EXPECT_EQ(manager.state("a")->health, BookHealth::Recovering);
    EXPECT_TRUE(manager.build_all_snapshots().empty());
    ASSERT_EQ(resyncs.size(), 1u);
    EXPECT_EQ(resyncs[0], "a");
}

TEST(OrderBookManagerTest, SnapshotAfterUpdatesRecoversTheBook) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a"});
    manager.apply(update("a", Side::Bid, 0.49, 100.0));
    ASSERT_EQ(manager.state("a")->health, BookHealth::Recovering);

    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));
    EXPECT_EQ(manager.state("a")->health, BookHealth::Healthy);
}

TEST(OrderBookManagerTest, CrossedUpdateInvalidatesAndRequestsAResync) {
    std::vector<TokenId> resyncs;
    OrderBookManager manager(test_config());
    manager.set_resync_handler([&resyncs](const TokenId& t) { resyncs.push_back(t); });
    manager.set_tracked_tokens({"a"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));

    manager.apply(update("a", Side::Bid, 0.55, 10.0));

    EXPECT_EQ(manager.state("a")->health, BookHealth::Recovering);
    EXPECT_EQ(manager.total_crossed_incidents(), 1u);
    // The stale local book must not keep being published after it went wrong.
    EXPECT_TRUE(manager.build_all_snapshots().empty());
    EXPECT_FALSE(resyncs.empty());
}

TEST(OrderBookManagerTest, PhantomDeleteInvalidatesTheBook) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));

    // Told to remove a level we never had: our view has drifted from the venue's.
    manager.apply(update("a", Side::Bid, 0.30, 0.0));

    EXPECT_EQ(manager.state("a")->health, BookHealth::Recovering);
}

TEST(OrderBookManagerTest, DisconnectInvalidatesEveryBook) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a", "b"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));
    manager.apply(snapshot("b", {{0.20, 10.0}}, {{0.22, 10.0}}));
    ASSERT_EQ(manager.build_all_snapshots().size(), 2u);

    manager.apply(connection(ConnectionState::Disconnected));

    EXPECT_EQ(manager.state("a")->health, BookHealth::Recovering);
    EXPECT_EQ(manager.state("b")->health, BookHealth::Recovering);
    EXPECT_TRUE(manager.build_all_snapshots().empty());
}

TEST(OrderBookManagerTest, ConnectDoesNotInvalidate) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));

    manager.apply(connection(ConnectionState::Connected));
    manager.apply(connection(ConnectionState::SubscriptionSent));

    EXPECT_EQ(manager.state("a")->health, BookHealth::Healthy);
}

TEST(OrderBookManagerTest, QuietBooksGoStaleButStayUsable) {
    OrderBookManager manager(test_config(/*stale_after_ms=*/1));
    manager.set_tracked_tokens({"a"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));

    std::this_thread::sleep_for(std::chrono::milliseconds{15});
    manager.run_maintenance();

    EXPECT_EQ(manager.state("a")->health, BookHealth::Stale);
    // Stale means "no recent traffic", not "wrong": the book is still exported.
    EXPECT_EQ(manager.build_all_snapshots().size(), 1u);
}

TEST(OrderBookManagerTest, AnUpdateRevivesAStaleBook) {
    OrderBookManager manager(test_config(/*stale_after_ms=*/1));
    manager.set_tracked_tokens({"a"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));
    std::this_thread::sleep_for(std::chrono::milliseconds{15});
    manager.run_maintenance();
    ASSERT_EQ(manager.state("a")->health, BookHealth::Stale);

    manager.apply(update("a", Side::Bid, 0.48, 50.0));
    EXPECT_EQ(manager.state("a")->health, BookHealth::Healthy);
}

TEST(OrderBookManagerTest, TickSizeChangeInvalidatesTheBook) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a"});

    BookSnapshotEvent seed;
    seed.header = header("a");
    seed.bids = {{0.49, 100.0}};
    seed.asks = {{0.51, 150.0}};
    seed.tick_size = 0.01;
    manager.apply(MarketEvent{seed});
    ASSERT_EQ(manager.state("a")->health, BookHealth::Healthy);

    MarketMetadataEvent regrid;
    regrid.header = header("a");
    regrid.tick_size = 0.001;
    manager.apply(MarketEvent{regrid});

    // Levels quoted on the old grid may no longer exist.
    EXPECT_EQ(manager.state("a")->health, BookHealth::Recovering);
    EXPECT_DOUBLE_EQ(manager.state("a")->tick_size, 0.001);
}

TEST(OrderBookManagerTest, DuplicateSnapshotHashIsNotReapplied) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a"});

    BookSnapshotEvent first;
    first.header = header("a");
    first.header.venue_hash = "same";
    first.bids = {{0.49, 100.0}};
    first.asks = {{0.51, 150.0}};
    ASSERT_TRUE(manager.apply(MarketEvent{first}));

    BookSnapshotEvent repeat = first;
    repeat.header.receive_timestamp = now_receive();
    EXPECT_FALSE(manager.apply(MarketEvent{repeat}));
    EXPECT_EQ(manager.state("a")->health, BookHealth::Healthy);
}

TEST(OrderBookManagerTest, UntrackedTokenDataIsStillCaptured) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a"});

    // The venue can push an outcome we did not explicitly ask for; dropping it
    // would silently lose half a market.
    manager.apply(snapshot("surprise", {{0.30, 10.0}}, {{0.31, 10.0}}));

    EXPECT_EQ(manager.book_count(), 2u);
    ASSERT_TRUE(manager.state("surprise").has_value());
    EXPECT_EQ(manager.state("surprise")->health, BookHealth::Healthy);
}

TEST(OrderBookManagerTest, RetrackingDropsTokensNoLongerWanted) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a", "b"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));

    manager.set_tracked_tokens({"b", "c"});

    EXPECT_EQ(manager.book_count(), 2u);
    EXPECT_FALSE(manager.state("a").has_value());
    EXPECT_TRUE(manager.state("c").has_value());
}

TEST(OrderBookManagerTest, HealthCountsAddUpToTheBookCount) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a", "b", "c"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));

    std::uint64_t healthy = 0;
    std::uint64_t stale = 0;
    std::uint64_t unhealthy = 0;
    manager.health_counts(healthy, stale, unhealthy);

    EXPECT_EQ(healthy, 1u);
    EXPECT_EQ(stale, 0u);
    EXPECT_EQ(unhealthy, 2u);
    EXPECT_EQ(healthy + stale + unhealthy, manager.book_count());
}

TEST(OrderBookManagerTest, TradesUpdateStatisticsWithoutTouchingTheBook) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a"});
    manager.apply(snapshot("a", {{0.49, 100.0}}, {{0.51, 150.0}}));

    TradeEvent trade;
    trade.header = header("a");
    trade.trade.token_id = "a";
    trade.trade.price = 0.51;
    trade.trade.quantity = 25.0;
    manager.apply(MarketEvent{trade});

    EXPECT_EQ(manager.state("a")->trades_seen, 1u);
    EXPECT_DOUBLE_EQ(manager.state("a")->last_trade_price, 0.51);
    EXPECT_DOUBLE_EQ(manager.build_all_snapshots()[0].best_ask, 0.51);
}

TEST(OrderBookManagerTest, SnapshotCarriesDerivedResearchMetrics) {
    OrderBookManager manager(test_config());
    manager.set_tracked_tokens({"a"});
    manager.apply(snapshot("a", {{0.49, 100.0}, {0.48, 200.0}},
                           {{0.51, 150.0}, {0.52, 300.0}}));

    const auto snapshots = manager.build_all_snapshots();
    ASSERT_EQ(snapshots.size(), 1u);
    const auto& s = snapshots[0];

    EXPECT_DOUBLE_EQ(s.spread, 0.02);
    EXPECT_DOUBLE_EQ(s.midpoint, 0.50);
    EXPECT_DOUBLE_EQ(s.bid_depth_l5, 300.0);
    EXPECT_DOUBLE_EQ(s.ask_depth_l5, 450.0);
    EXPECT_DOUBLE_EQ(s.imbalance_l1, -0.2);
    EXPECT_DOUBLE_EQ(s.imbalance_l5, -0.2);
    EXPECT_EQ(s.bid_levels, 2u);
    EXPECT_EQ(s.ask_levels, 2u);
}

}  // namespace
}  // namespace market_data
