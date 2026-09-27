// Parser tests run against fixtures captured from the live venue on
// 2026-09-27, so they pin the wire format as it actually is rather than as the
// published schema describes it.
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>

#include "market_data/exchange/polymarket_parser.hpp"
#include "market_data/time_util.hpp"

namespace market_data::polymarket {
namespace {

std::string load_fixture(const std::string& name) {
    const std::filesystem::path path = std::filesystem::path{MDE_TEST_FIXTURE_DIR} / name;
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.good()) << "missing fixture: " << path;
    std::ostringstream buffer;
    buffer << in.rdbuf();
    return buffer.str();
}

// ---------------------------------------------------------------------------
// Gamma double-encoded arrays
// ---------------------------------------------------------------------------

TEST(PolymarketParserTest, DecodesEmbeddedJsonStringArray) {
    const auto values = parse_embedded_json_string_array(R"(["Yes", "No"])");
    ASSERT_EQ(values.size(), 2u);
    EXPECT_EQ(values[0], "Yes");
    EXPECT_EQ(values[1], "No");
}

TEST(PolymarketParserTest, EmbeddedArrayDecoderToleratesGarbage) {
    EXPECT_TRUE(parse_embedded_json_string_array("").empty());
    EXPECT_TRUE(parse_embedded_json_string_array("not json").empty());
    EXPECT_TRUE(parse_embedded_json_string_array(R"({"a":1})").empty());
    EXPECT_TRUE(parse_embedded_json_string_array("[1, 2, 3]").empty());
}

// ---------------------------------------------------------------------------
// Market discovery
// ---------------------------------------------------------------------------

TEST(PolymarketParserTest, ParsesLiveGammaMarketsFixture) {
    std::vector<std::string> errors;
    const auto markets = parse_gamma_markets(load_fixture("gamma_markets.json"),
                                             now_exchange(), &errors);

    ASSERT_FALSE(markets.empty());
    EXPECT_TRUE(errors.empty()) << (errors.empty() ? "" : errors.front());

    for (const auto& market : markets) {
        EXPECT_FALSE(market.market_id.empty());
        EXPECT_FALSE(market.question.empty());
        // The double-decode of clobTokenIds is the whole point of this test.
        EXPECT_FALSE(market.token_ids.empty());
        EXPECT_EQ(market.token_ids.size(), market.outcomes.size());
        EXPECT_GT(market.min_tick_size, 0.0);
        EXPECT_TRUE(market.resolution_time.has_value());
    }
}

TEST(PolymarketParserTest, SkipsMarketsMissingRequiredFieldsButKeepsTheRest) {
    const std::string body = R"([
      {"question":"no condition id","clobTokenIds":"[\"1\"]","outcomes":"[\"Yes\"]"},
      {"conditionId":"0xaa","question":"no tokens","clobTokenIds":"[]","outcomes":"[]"},
      {"conditionId":"0xbb","question":"mismatched","clobTokenIds":"[\"1\",\"2\"]","outcomes":"[\"Yes\"]"},
      {"conditionId":"0xcc","question":"good","clobTokenIds":"[\"7\",\"8\"]","outcomes":"[\"Yes\",\"No\"]",
       "active":true,"closed":false,"volumeNum":1234.5}
    ])";

    std::vector<std::string> errors;
    const auto markets = parse_gamma_markets(body, now_exchange(), &errors);

    ASSERT_EQ(markets.size(), 1u);
    EXPECT_EQ(markets[0].market_id, "0xcc");
    EXPECT_DOUBLE_EQ(markets[0].reported_volume, 1234.5);
    EXPECT_EQ(errors.size(), 3u);
}

TEST(PolymarketParserTest, AcceptsNumericOrStringNumbersFromGamma) {
    const std::string body = R"([
      {"conditionId":"0xcc","clobTokenIds":"[\"7\"]","outcomes":"[\"Yes\"]",
       "volumeNum":"9876.5","liquidityNum":42,"orderPriceMinTickSize":"0.001"}
    ])";
    const auto markets = parse_gamma_markets(body, now_exchange());
    ASSERT_EQ(markets.size(), 1u);
    EXPECT_DOUBLE_EQ(markets[0].reported_volume, 9876.5);
    EXPECT_DOUBLE_EQ(markets[0].reported_liquidity, 42.0);
    EXPECT_DOUBLE_EQ(markets[0].min_tick_size, 0.001);
}

TEST(PolymarketParserTest, DerivesCategoryFromParentEventTicker) {
    const std::string body = R"([
      {"conditionId":"0xcc","clobTokenIds":"[\"7\"]","outcomes":"[\"Yes\"]",
       "events":[{"ticker":"next-prime-minister-of-ethiopia","slug":"fallback"}]}
    ])";
    const auto markets = parse_gamma_markets(body, now_exchange());
    ASSERT_EQ(markets.size(), 1u);
    EXPECT_EQ(markets[0].category, "next-prime-minister-of-ethiopia");
}

TEST(PolymarketParserTest, MalformedGammaBodyYieldsErrorNotException) {
    std::vector<std::string> errors;
    EXPECT_TRUE(parse_gamma_markets("{ this is not json", now_exchange(), &errors).empty());
    EXPECT_FALSE(errors.empty());
}

// ---------------------------------------------------------------------------
// CLOB REST book (the resync path)
// ---------------------------------------------------------------------------

TEST(PolymarketParserTest, ParsesLiveClobBookFixture) {
    BookSnapshotEvent event;
    std::string error;
    ASSERT_TRUE(parse_clob_book(load_fixture("clob_book.json"), now_receive(), event, &error))
        << error;

    EXPECT_EQ(event.header.exchange, "polymarket");
    EXPECT_FALSE(event.header.token_id.empty());
    EXPECT_FALSE(event.header.market_id.empty());
    EXPECT_FALSE(event.header.venue_hash.empty());
    EXPECT_FALSE(event.asks.empty());
    // Prices and sizes arrive as strings and must come back as usable numbers.
    for (const auto& level : event.asks) {
        EXPECT_GE(level.price, 0.0);
        EXPECT_LE(level.price, 1.0);
        EXPECT_GT(level.quantity, 0.0);
    }
}

TEST(PolymarketParserTest, RejectsBookWithoutAssetId) {
    BookSnapshotEvent event;
    std::string error;
    EXPECT_FALSE(parse_clob_book(R"({"bids":[],"asks":[]})", now_receive(), event, &error));
    EXPECT_FALSE(error.empty());
}

// ---------------------------------------------------------------------------
// Websocket framing
// ---------------------------------------------------------------------------

TEST(PolymarketParserTest, TreatsHeartbeatRepliesAsControlFrames) {
    for (const auto* frame : {"PONG", "PING", "  PONG\n", ""}) {
        const auto outcome = parse_ws_frame(frame, now_receive());
        EXPECT_TRUE(outcome.was_control_frame) << frame;
        EXPECT_TRUE(outcome.events.empty());
        // A heartbeat must never be logged as a malformed message.
        EXPECT_FALSE(outcome.has_errors()) << frame;
    }
}

TEST(PolymarketParserTest, ParsesBatchedArrayOfBookSnapshots) {
    const auto outcome = parse_ws_frame(load_fixture("ws_book_array.json"), now_receive());

    EXPECT_FALSE(outcome.has_errors());
    ASSERT_FALSE(outcome.events.empty());
    for (const auto& event : outcome.events) {
        ASSERT_TRUE(std::holds_alternative<BookSnapshotEvent>(event));
        const auto& snapshot = std::get<BookSnapshotEvent>(event);
        EXPECT_FALSE(snapshot.header.token_id.empty());
        EXPECT_FALSE(snapshot.bids.empty());
        EXPECT_FALSE(snapshot.asks.empty());
        EXPECT_GT(snapshot.tick_size, 0.0);
    }
}

TEST(PolymarketParserTest, ParsesBareObjectFrameToo) {
    const std::string frame = R"({"event_type":"book","market":"0xm","asset_id":"tok",
      "timestamp":"1782753357257","hash":"0xabc",
      "bids":[{"price":"0.48","size":"100"}],"asks":[{"price":"0.51","size":"150"}]})";

    const auto outcome = parse_ws_frame(frame, now_receive());
    ASSERT_EQ(outcome.events.size(), 1u);

    const auto& snapshot = std::get<BookSnapshotEvent>(outcome.events.front());
    EXPECT_EQ(snapshot.header.token_id, "tok");
    EXPECT_EQ(snapshot.header.market_id, "0xm");
    EXPECT_EQ(to_epoch_millis(snapshot.header.exchange_timestamp), 1782753357257LL);
    ASSERT_EQ(snapshot.bids.size(), 1u);
    EXPECT_DOUBLE_EQ(snapshot.bids[0].price, 0.48);
    EXPECT_DOUBLE_EQ(snapshot.bids[0].quantity, 100.0);
}

TEST(PolymarketParserTest, SplitsPriceChangeFrameByTokenAndMapsSides) {
    const auto outcome = parse_ws_frame(load_fixture("ws_price_change.json"), now_receive());

    EXPECT_FALSE(outcome.has_errors());
    // The live fixture carries both legs of a binary market in one frame; each
    // must become its own event so the book manager can dispatch per token.
    ASSERT_EQ(outcome.events.size(), 2u);

    std::vector<Side> sides;
    for (const auto& event : outcome.events) {
        ASSERT_TRUE(std::holds_alternative<BookUpdateEvent>(event));
        const auto& update = std::get<BookUpdateEvent>(event);
        ASSERT_EQ(update.updates.size(), 1u);
        EXPECT_EQ(update.updates[0].token_id, update.header.token_id);
        sides.push_back(update.updates[0].side);
    }
    // BUY maps to the bid ladder, SELL to the ask ladder.
    EXPECT_EQ(sides[0], Side::Bid);
    EXPECT_EQ(sides[1], Side::Ask);
}

TEST(PolymarketParserTest, GroupsMultipleChangesForTheSameTokenIntoOneEvent) {
    const std::string frame = R"({"event_type":"price_change","market":"0xm",
      "timestamp":"1782753357257","price_changes":[
        {"asset_id":"tok","price":"0.48","size":"100","side":"BUY"},
        {"asset_id":"tok","price":"0.47","size":"0","side":"BUY"},
        {"asset_id":"tok","price":"0.52","size":"300","side":"SELL"}]})";

    const auto outcome = parse_ws_frame(frame, now_receive());
    ASSERT_EQ(outcome.events.size(), 1u);

    const auto& event = std::get<BookUpdateEvent>(outcome.events.front());
    ASSERT_EQ(event.updates.size(), 3u);
    EXPECT_EQ(event.updates[0].side, Side::Bid);
    EXPECT_DOUBLE_EQ(event.updates[1].quantity, 0.0);  // a deletion
    EXPECT_EQ(event.updates[2].side, Side::Ask);
}

TEST(PolymarketParserTest, DropsOnlyTheBadEntriesInAPriceChangeFrame) {
    const std::string frame = R"({"event_type":"price_change","market":"0xm",
      "timestamp":"1782753357257","price_changes":[
        {"asset_id":"tok","price":"0.48","size":"100","side":"BUY"},
        {"asset_id":"tok","price":"0.49","side":"BUY"},
        {"asset_id":"tok","price":"0.50","size":"10","side":"SIDEWAYS"}]})";

    const auto outcome = parse_ws_frame(frame, now_receive());
    ASSERT_EQ(outcome.events.size(), 1u);
    EXPECT_EQ(std::get<BookUpdateEvent>(outcome.events.front()).updates.size(), 1u);
    EXPECT_EQ(outcome.errors.size(), 2u);
}

TEST(PolymarketParserTest, ParsesTradeFrame) {
    const auto outcome = parse_ws_frame(load_fixture("ws_last_trade_price.json"), now_receive());
    ASSERT_EQ(outcome.events.size(), 1u);

    const auto& trade = std::get<TradeEvent>(outcome.events.front()).trade;
    EXPECT_DOUBLE_EQ(trade.price, 0.08);
    EXPECT_DOUBLE_EQ(trade.quantity, 219.217767);
    EXPECT_EQ(trade.aggressor_side, Side::Ask);
}

TEST(PolymarketParserTest, TickSizeChangeBecomesMetadataEvent) {
    const auto outcome = parse_ws_frame(load_fixture("ws_tick_size_change.json"), now_receive());
    ASSERT_EQ(outcome.events.size(), 1u);

    const auto& meta = std::get<MarketMetadataEvent>(outcome.events.front());
    EXPECT_DOUBLE_EQ(meta.tick_size, 0.001);
    EXPECT_FALSE(meta.header.token_id.empty());
}

TEST(PolymarketParserTest, MalformedFramesProduceErrorsAndNoEvents) {
    for (const auto* frame : {"{ not json", R"({"no_event_type":1})",
                              R"({"event_type":"book","asset_id":"t"})",
                              R"([1, 2, 3])"}) {
        const auto outcome = parse_ws_frame(frame, now_receive());
        EXPECT_TRUE(outcome.events.empty()) << frame;
        EXPECT_TRUE(outcome.has_errors()) << frame;
    }
}

TEST(PolymarketParserTest, UnknownEventTypeIsReportedButNotFatal) {
    const auto outcome = parse_ws_frame(R"({"event_type":"brand_new_thing"})", now_receive());
    EXPECT_TRUE(outcome.events.empty());
    ASSERT_EQ(outcome.errors.size(), 1u);
    EXPECT_NE(outcome.errors[0].find("brand_new_thing"), std::string::npos);
}

}  // namespace
}  // namespace market_data::polymarket
