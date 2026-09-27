#include <gtest/gtest.h>

#include <cmath>

#include "market_data/order_book.hpp"

namespace market_data {
namespace {

// The fixture from the specification:
//
//   BIDS            ASKS
//   0.49  100       0.51  150
//   0.48  200       0.52  300
OrderBook make_spec_book() {
    OrderBook book;
    book.apply_snapshot(
        std::vector<PriceLevel>{{0.49, 100.0}, {0.48, 200.0}},
        std::vector<PriceLevel>{{0.51, 150.0}, {0.52, 300.0}});
    return book;
}

TEST(OrderBookTest, EmptyBookHasNoBestQuotesAndUndefinedStatistics) {
    const OrderBook book;
    EXPECT_TRUE(book.empty());
    EXPECT_FALSE(book.best_bid().has_value());
    EXPECT_FALSE(book.best_ask().has_value());
    EXPECT_TRUE(std::isnan(book.midpoint()));
    EXPECT_TRUE(std::isnan(book.spread()));
    EXPECT_TRUE(std::isnan(book.imbalance(1)));
    EXPECT_FALSE(book.crossed());
}

TEST(OrderBookTest, SnapshotConstructsExpectedTopOfBook) {
    const OrderBook book = make_spec_book();

    ASSERT_TRUE(book.best_bid().has_value());
    ASSERT_TRUE(book.best_ask().has_value());
    EXPECT_DOUBLE_EQ(book.best_bid()->price, 0.49);
    EXPECT_DOUBLE_EQ(book.best_bid()->quantity, 100.0);
    EXPECT_DOUBLE_EQ(book.best_ask()->price, 0.51);
    EXPECT_DOUBLE_EQ(book.best_ask()->quantity, 150.0);

    EXPECT_DOUBLE_EQ(book.spread(), 0.02);
    EXPECT_DOUBLE_EQ(book.midpoint(), 0.50);

    EXPECT_EQ(book.bid_level_count(), 2u);
    EXPECT_EQ(book.ask_level_count(), 2u);
    EXPECT_EQ(book.stats().snapshots_applied, 1u);
}

TEST(OrderBookTest, SnapshotReplacesRatherThanMergesPriorState) {
    OrderBook book = make_spec_book();
    book.apply_snapshot(std::vector<PriceLevel>{{0.10, 5.0}},
                        std::vector<PriceLevel>{{0.90, 7.0}});

    EXPECT_EQ(book.bid_level_count(), 1u);
    EXPECT_EQ(book.ask_level_count(), 1u);
    EXPECT_DOUBLE_EQ(book.best_bid()->price, 0.10);
    EXPECT_DOUBLE_EQ(book.best_ask()->price, 0.90);
}

TEST(OrderBookTest, SnapshotSkipsZeroQuantityLevels) {
    OrderBook book;
    book.apply_snapshot(std::vector<PriceLevel>{{0.49, 100.0}, {0.48, 0.0}},
                        std::vector<PriceLevel>{{0.51, 150.0}});
    EXPECT_EQ(book.bid_level_count(), 1u);
    EXPECT_DOUBLE_EQ(book.best_bid()->price, 0.49);
}

TEST(OrderBookTest, InsertNewBestBidLevel) {
    OrderBook book = make_spec_book();

    EXPECT_TRUE(book.apply_update(Side::Bid, 0.50, 400.0));

    ASSERT_TRUE(book.best_bid().has_value());
    EXPECT_DOUBLE_EQ(book.best_bid()->price, 0.50);
    EXPECT_DOUBLE_EQ(book.best_bid()->quantity, 400.0);
    EXPECT_DOUBLE_EQ(book.spread(), 0.01);
    EXPECT_EQ(book.bid_level_count(), 3u);
}

TEST(OrderBookTest, DeleteZeroQuantityLevelRestoresPreviousBest) {
    OrderBook book = make_spec_book();

    ASSERT_TRUE(book.apply_update(Side::Bid, 0.50, 400.0));
    ASSERT_DOUBLE_EQ(book.best_bid()->price, 0.50);

    ASSERT_TRUE(book.apply_update(Side::Bid, 0.50, 0.0));

    ASSERT_TRUE(book.best_bid().has_value());
    EXPECT_DOUBLE_EQ(book.best_bid()->price, 0.49);
    EXPECT_DOUBLE_EQ(book.best_bid()->quantity, 100.0);
    EXPECT_EQ(book.bid_level_count(), 2u);
    EXPECT_EQ(book.stats().phantom_deletes, 0u);
}

TEST(OrderBookTest, ModifyExistingLevelReplacesQuantityRatherThanAccumulating) {
    OrderBook book = make_spec_book();

    ASSERT_TRUE(book.apply_update(Side::Ask, 0.51, 25.0));

    EXPECT_EQ(book.ask_level_count(), 2u);
    EXPECT_DOUBLE_EQ(book.best_ask()->price, 0.51);
    EXPECT_DOUBLE_EQ(book.best_ask()->quantity, 25.0);
}

TEST(OrderBookTest, DeletingTheLastLevelEmptiesThatSideOnly) {
    OrderBook book;
    book.apply_snapshot(std::vector<PriceLevel>{{0.49, 100.0}},
                        std::vector<PriceLevel>{{0.51, 150.0}});

    ASSERT_TRUE(book.apply_update(Side::Bid, 0.49, 0.0));

    EXPECT_FALSE(book.best_bid().has_value());
    EXPECT_TRUE(book.best_ask().has_value());
    EXPECT_TRUE(std::isnan(book.midpoint()));
    EXPECT_TRUE(std::isnan(book.spread()));
    EXPECT_FALSE(book.empty());
}

TEST(OrderBookTest, DeletingAnAbsentLevelIsCountedAsPhantom) {
    OrderBook book = make_spec_book();

    EXPECT_TRUE(book.apply_update(Side::Bid, 0.33, 0.0));

    EXPECT_EQ(book.stats().phantom_deletes, 1u);
    EXPECT_EQ(book.bid_level_count(), 2u);
}

TEST(OrderBookTest, TopNLevelsAreOrderedBestFirstAndClampToAvailableDepth) {
    const OrderBook book = make_spec_book();

    const auto bids = book.top_bids(5);
    ASSERT_EQ(bids.size(), 2u);
    EXPECT_DOUBLE_EQ(bids[0].price, 0.49);
    EXPECT_DOUBLE_EQ(bids[1].price, 0.48);

    const auto asks = book.top_asks(5);
    ASSERT_EQ(asks.size(), 2u);
    EXPECT_DOUBLE_EQ(asks[0].price, 0.51);
    EXPECT_DOUBLE_EQ(asks[1].price, 0.52);

    EXPECT_EQ(book.top_bids(1).size(), 1u);
    EXPECT_DOUBLE_EQ(book.top_bids(1)[0].price, 0.49);
    EXPECT_TRUE(book.top_bids(0).empty());
}

TEST(OrderBookTest, DepthSumsRequestedLevels) {
    const OrderBook book = make_spec_book();

    EXPECT_DOUBLE_EQ(book.depth(Side::Bid, 1), 100.0);
    EXPECT_DOUBLE_EQ(book.depth(Side::Bid, 5), 300.0);
    EXPECT_DOUBLE_EQ(book.depth(Side::Ask, 1), 150.0);
    EXPECT_DOUBLE_EQ(book.depth(Side::Ask, 5), 450.0);
}

TEST(OrderBookTest, ImbalanceIsNormalizedAndGuardsAgainstZeroDenominator) {
    const OrderBook book = make_spec_book();

    // L1: (100 - 150) / 250
    EXPECT_DOUBLE_EQ(book.imbalance(1), -0.2);
    // L5: (300 - 450) / 750
    EXPECT_DOUBLE_EQ(book.imbalance(5), -0.2);

    OrderBook one_sided;
    one_sided.apply_snapshot(std::vector<PriceLevel>{{0.49, 100.0}},
                             std::vector<PriceLevel>{});
    EXPECT_DOUBLE_EQ(one_sided.imbalance(1), 1.0);

    const OrderBook empty_book;
    EXPECT_TRUE(std::isnan(empty_book.imbalance(1)));
}

TEST(OrderBookTest, CrossedBookIsDetectedAndCounted) {
    OrderBook book = make_spec_book();
    ASSERT_FALSE(book.crossed());

    // A bid above the best ask: the feed is reporting a crossed state.
    ASSERT_TRUE(book.apply_update(Side::Bid, 0.55, 10.0));

    EXPECT_TRUE(book.crossed());
    EXPECT_EQ(book.stats().crossed_incidents, 1u);

    // Still crossed after another update: the incident is not double counted.
    ASSERT_TRUE(book.apply_update(Side::Bid, 0.56, 10.0));
    EXPECT_EQ(book.stats().crossed_incidents, 1u);

    // Clearing the offending levels uncrosses the book.
    ASSERT_TRUE(book.apply_update(Side::Bid, 0.55, 0.0));
    ASSERT_TRUE(book.apply_update(Side::Bid, 0.56, 0.0));
    EXPECT_FALSE(book.crossed());
    EXPECT_EQ(book.stats().crossed_incidents, 1u);
}

TEST(OrderBookTest, LockedBookCountsAsCrossed) {
    OrderBook book;
    book.apply_snapshot(std::vector<PriceLevel>{{0.50, 10.0}},
                        std::vector<PriceLevel>{{0.50, 10.0}});
    EXPECT_TRUE(book.crossed());
    EXPECT_DOUBLE_EQ(book.spread(), 0.0);
    EXPECT_DOUBLE_EQ(book.midpoint(), 0.50);
}

TEST(OrderBookTest, RejectsOutOfRangePricesAndNegativeQuantities) {
    OrderBook book = make_spec_book();

    EXPECT_FALSE(book.apply_update(Side::Bid, 1.50, 10.0));
    EXPECT_FALSE(book.apply_update(Side::Bid, -0.10, 10.0));
    EXPECT_FALSE(book.apply_update(Side::Ask, 0.51, -5.0));

    EXPECT_EQ(book.stats().invalid_updates, 3u);
    // A rejected update leaves the book untouched.
    EXPECT_EQ(book.bid_level_count(), 2u);
    EXPECT_DOUBLE_EQ(book.best_ask()->quantity, 150.0);
}

TEST(OrderBookTest, BoundaryPricesZeroAndOneAreValid) {
    OrderBook book;
    EXPECT_TRUE(book.apply_update(Side::Bid, 0.0, 10.0));
    EXPECT_TRUE(book.apply_update(Side::Ask, 1.0, 10.0));

    EXPECT_DOUBLE_EQ(book.best_bid()->price, 0.0);
    EXPECT_DOUBLE_EQ(book.best_ask()->price, 1.0);
    EXPECT_DOUBLE_EQ(book.spread(), 1.0);
    EXPECT_EQ(book.stats().invalid_updates, 0u);
}

TEST(OrderBookTest, FractionalTickAndSizePrecisionSurvivesRoundTrip) {
    OrderBook book;
    // 0.001 is Polymarket's finest tick; sizes carry six decimals.
    ASSERT_TRUE(book.apply_update(Side::Bid, 0.001, 219.217767));
    ASSERT_TRUE(book.apply_update(Side::Ask, 0.999, 0.000001));

    EXPECT_DOUBLE_EQ(book.best_bid()->price, 0.001);
    EXPECT_DOUBLE_EQ(book.best_bid()->quantity, 219.217767);
    EXPECT_DOUBLE_EQ(book.best_ask()->quantity, 0.000001);

    // A level whose size rounds below one micro-unit is a deletion, not a
    // ghost level with an unrepresentably small size.
    ASSERT_TRUE(book.apply_update(Side::Ask, 0.999, 1e-9));
    EXPECT_FALSE(book.best_ask().has_value());
}

TEST(OrderBookTest, SnapshotAfterIncrementalUpdatesRebuildsCleanState) {
    OrderBook book = make_spec_book();
    ASSERT_TRUE(book.apply_update(Side::Bid, 0.50, 400.0));
    ASSERT_TRUE(book.apply_update(Side::Ask, 0.51, 0.0));

    book.apply_snapshot(std::vector<PriceLevel>{{0.49, 100.0}, {0.48, 200.0}},
                        std::vector<PriceLevel>{{0.51, 150.0}, {0.52, 300.0}});

    EXPECT_DOUBLE_EQ(book.best_bid()->price, 0.49);
    EXPECT_DOUBLE_EQ(book.best_ask()->price, 0.51);
    EXPECT_EQ(book.stats().snapshots_applied, 2u);
}

TEST(OrderBookTest, ClearDropsLevelsButPreservesStatistics) {
    OrderBook book = make_spec_book();
    ASSERT_TRUE(book.apply_update(Side::Bid, 0.50, 400.0));

    book.clear();

    EXPECT_TRUE(book.empty());
    EXPECT_EQ(book.stats().snapshots_applied, 1u);
    EXPECT_EQ(book.stats().updates_applied, 1u);
}

TEST(OrderBookTest, PriceConversionsRoundTripAcrossTheWholeGrid) {
    for (PriceTicks t = kMinPriceTicks; t <= kMaxPriceTicks; ++t) {
        EXPECT_EQ(price_to_ticks(ticks_to_price(t)), t) << "tick " << t;
    }
}

}  // namespace
}  // namespace market_data
