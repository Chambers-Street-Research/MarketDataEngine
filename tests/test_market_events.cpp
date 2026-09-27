// Codec round-trip tests. If a field does not survive this, replay cannot
// reconstruct it, so every event type is covered end to end.
#include <gtest/gtest.h>

#include <algorithm>

#include "market_data/event_codec.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

EventHeader make_header(TokenId token, std::uint64_t seq) {
    EventHeader header;
    header.exchange = "polymarket";
    header.market_id = "0xmarket";
    header.token_id = std::move(token);
    header.exchange_timestamp = from_epoch_millis(1'790'540'018'860);
    header.receive_timestamp = from_steady_nanos(1'276'100'591'259'583);
    header.sequence = seq;
    header.venue_hash = "abc123";
    return header;
}

void expect_header_round_trip(const EventHeader& in, const EventHeader& out) {
    EXPECT_EQ(out.exchange, in.exchange);
    EXPECT_EQ(out.market_id, in.market_id);
    EXPECT_EQ(out.token_id, in.token_id);
    EXPECT_EQ(out.sequence, in.sequence);
    // Both clocks must survive: research needs the exchange stamp for latency
    // and the steady stamp for ordering.
    EXPECT_EQ(to_epoch_millis(out.exchange_timestamp), to_epoch_millis(in.exchange_timestamp));
    EXPECT_EQ(to_steady_nanos(out.receive_timestamp), to_steady_nanos(in.receive_timestamp));
}

TEST(MarketEventCodecTest, BookSnapshotRoundTrips) {
    BookSnapshotEvent original;
    original.header = make_header("tok", 7);
    original.bids = {{0.49, 100.0}, {0.48, 200.5}};
    original.asks = {{0.51, 150.0}};
    original.tick_size = 0.001;

    const auto decoded = decode_event_line(encode_event_line(MarketEvent{original}));
    ASSERT_TRUE(decoded.has_value());
    ASSERT_TRUE(std::holds_alternative<BookSnapshotEvent>(*decoded));

    const auto& out = std::get<BookSnapshotEvent>(*decoded);
    expect_header_round_trip(original.header, out.header);
    EXPECT_EQ(out.header.venue_hash, "abc123");
    ASSERT_EQ(out.bids.size(), 2u);
    EXPECT_DOUBLE_EQ(out.bids[0].price, 0.49);
    EXPECT_DOUBLE_EQ(out.bids[1].quantity, 200.5);
    ASSERT_EQ(out.asks.size(), 1u);
    EXPECT_DOUBLE_EQ(out.tick_size, 0.001);
}

TEST(MarketEventCodecTest, BookUpdateRoundTripsIncludingDeletions) {
    BookUpdateEvent original;
    original.header = make_header("tok", 8);
    original.updates = {
        BookUpdate{"tok", Side::Bid, 0.48, 100.0, {}, {}},
        BookUpdate{"tok", Side::Ask, 0.52, 0.0, {}, {}},  // a deletion
    };

    const auto decoded = decode_event_line(encode_event_line(MarketEvent{original}));
    ASSERT_TRUE(decoded.has_value());

    const auto& out = std::get<BookUpdateEvent>(*decoded);
    expect_header_round_trip(original.header, out.header);
    ASSERT_EQ(out.updates.size(), 2u);
    EXPECT_EQ(out.updates[0].side, Side::Bid);
    EXPECT_DOUBLE_EQ(out.updates[0].price, 0.48);
    EXPECT_EQ(out.updates[1].side, Side::Ask);
    EXPECT_DOUBLE_EQ(out.updates[1].quantity, 0.0);
    // Updates inherit the event's timestamps so a replayed update carries the
    // same time information a live one did.
    EXPECT_EQ(to_epoch_millis(out.updates[0].exchange_timestamp),
              to_epoch_millis(original.header.exchange_timestamp));
}

TEST(MarketEventCodecTest, TradeRoundTrips) {
    TradeEvent original;
    original.header = make_header("tok", 9);
    original.trade.token_id = "tok";
    original.trade.price = 0.08;
    original.trade.quantity = 219.217767;
    original.trade.aggressor_side = Side::Ask;

    const auto decoded = decode_event_line(encode_event_line(MarketEvent{original}));
    ASSERT_TRUE(decoded.has_value());

    const auto& out = std::get<TradeEvent>(*decoded);
    EXPECT_EQ(out.trade.token_id, "tok");
    EXPECT_DOUBLE_EQ(out.trade.price, 0.08);
    EXPECT_DOUBLE_EQ(out.trade.quantity, 219.217767);
    EXPECT_EQ(out.trade.aggressor_side, Side::Ask);
}

TEST(MarketEventCodecTest, MetadataRoundTrips) {
    MarketMetadataEvent original;
    original.header = make_header("tok", 10);
    original.question = "Will it rain?";
    original.category = "weather";
    original.token_ids = {"tok", "tok2"};
    original.outcomes = {"Yes", "No"};
    original.active = true;
    original.tick_size = 0.01;

    const auto decoded = decode_event_line(encode_event_line(MarketEvent{original}));
    ASSERT_TRUE(decoded.has_value());

    const auto& out = std::get<MarketMetadataEvent>(*decoded);
    EXPECT_EQ(out.question, "Will it rain?");
    EXPECT_EQ(out.outcomes, (std::vector<std::string>{"Yes", "No"}));
    EXPECT_TRUE(out.active);
    EXPECT_DOUBLE_EQ(out.tick_size, 0.01);
}

TEST(MarketEventCodecTest, ConnectionEventRoundTripsWithoutAToken) {
    ConnectionEvent original;
    original.header.exchange = "polymarket";
    original.header.sequence = 1;
    original.state = ConnectionState::Reconnecting;
    original.detail = "read: end of stream";
    original.attempt = 3;
    original.subscribed_tokens = 50;

    const auto decoded = decode_event_line(encode_event_line(MarketEvent{original}));
    ASSERT_TRUE(decoded.has_value());
    ASSERT_TRUE(std::holds_alternative<ConnectionEvent>(*decoded));

    const auto& out = std::get<ConnectionEvent>(*decoded);
    EXPECT_EQ(out.state, ConnectionState::Reconnecting);
    EXPECT_EQ(out.detail, "read: end of stream");
    EXPECT_EQ(out.attempt, 3u);
    EXPECT_EQ(out.subscribed_tokens, 50u);
}

TEST(MarketEventCodecTest, EncodedLineIsSingleLineJsonWithRequiredFields) {
    BookUpdateEvent event;
    event.header = make_header("tok", 11);
    event.updates = {BookUpdate{"tok", Side::Bid, 0.48, 1200.0, {}, {}}};

    const std::string line = encode_event_line(MarketEvent{event});
    EXPECT_EQ(std::count(line.begin(), line.end(), '\n'), 1);
    EXPECT_EQ(line.back(), '\n');

    const auto node = nlohmann::json::parse(line);
    for (const char* key : {"event_type", "market_id", "token_id", "exchange_ts", "receive_ts",
                            "seq"}) {
        EXPECT_TRUE(node.contains(key)) << key;
    }
    EXPECT_EQ(node["event_type"], "book_update");
}

TEST(MarketEventCodecTest, MalformedLinesDecodeToNulloptWithAReason) {
    for (const auto* line : {"", "not json", "[1,2,3]", R"({"event_type":"book_update"})",
                             R"({"event_type":"nope","token_id":"t","seq":1})",
                             R"({"event_type":"book_update","token_id":"t","seq":1})"}) {
        std::string error;
        EXPECT_FALSE(decode_event_line(line, &error).has_value()) << line;
        EXPECT_FALSE(error.empty()) << line;
    }
}

TEST(MarketEventCodecTest, HeaderAccessorsCoverEveryAlternative) {
    const std::vector<MarketEvent> events{
        BookSnapshotEvent{make_header("a", 1), {}, {}, 0.0},
        BookUpdateEvent{make_header("b", 2), {}},
        TradeEvent{make_header("c", 3), {}},
        MarketMetadataEvent{make_header("d", 4), {}, {}, {}, {}, false, false, 0.0},
        ConnectionEvent{make_header("", 5), ConnectionState::Connected, {}, 0, 0},
    };
    const std::vector<std::string> expected_names{"book_snapshot", "book_update", "trade",
                                                  "market_metadata", "connection"};
    for (std::size_t i = 0; i < events.size(); ++i) {
        EXPECT_EQ(header_of(events[i]).sequence, i + 1);
        EXPECT_EQ(event_type_name(events[i]), expected_names[i]);
    }
}

}  // namespace
}  // namespace market_data
