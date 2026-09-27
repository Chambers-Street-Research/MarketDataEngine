// Replay tests. The property under test is the one that makes replay the
// foundation of a backtester: feeding a recorded log back through the pipeline
// reconstructs exactly the books the live session held.
#include <gtest/gtest.h>

#include <sstream>

#include "market_data/event_codec.hpp"
#include "market_data/order_book_manager.hpp"
#include "market_data/replay.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

Config test_config() {
    Config config;
    config.order_book_depth = 20;
    config.health.stale_after_ms = 60'000;
    config.health.resync_cooldown_ms = 0;
    return config;
}

class SessionBuilder {
public:
    MarketEvent snapshot(const TokenId& token,
                         std::vector<PriceLevel> bids,
                         std::vector<PriceLevel> asks) {
        BookSnapshotEvent event;
        event.header = header(token);
        event.bids = std::move(bids);
        event.asks = std::move(asks);
        event.tick_size = 0.001;
        return MarketEvent{std::move(event)};
    }

    MarketEvent update(const TokenId& token, Side side, double price, double quantity) {
        BookUpdateEvent event;
        event.header = header(token);
        BookUpdate u;
        u.token_id = token;
        u.side = side;
        u.price = price;
        u.quantity = quantity;
        event.updates.push_back(u);
        return MarketEvent{std::move(event)};
    }

    MarketEvent trade(const TokenId& token, double price, double quantity) {
        TradeEvent event;
        event.header = header(token);
        event.trade.token_id = token;
        event.trade.price = price;
        event.trade.quantity = quantity;
        return MarketEvent{std::move(event)};
    }

    MarketEvent connection(ConnectionState state) {
        ConnectionEvent event;
        event.header = header("");
        event.state = state;
        return MarketEvent{std::move(event)};
    }

private:
    EventHeader header(const TokenId& token) {
        EventHeader h;
        h.exchange = "polymarket";
        h.market_id = "0xmarket";
        h.token_id = token;
        h.exchange_timestamp = from_epoch_millis(1'790'540'000'000 + static_cast<std::int64_t>(seq_));
        h.receive_timestamp = from_steady_nanos(static_cast<std::int64_t>(seq_) * 1'000'000);
        h.sequence = ++seq_;
        return h;
    }
    std::uint64_t seq_ = 0;
};

std::string to_jsonl(const std::vector<MarketEvent>& events) {
    std::string out;
    for (const auto& event : events) {
        out += encode_event_line(event);
    }
    return out;
}

// Builds a session, applies it live, then replays the recording and compares.
struct SessionResult {
    std::string live_digest;
    std::string replay_digest;
    ReplayStats stats;
};

SessionResult run_session(const std::vector<MarketEvent>& events) {
    OrderBookManager live(test_config());
    for (const auto& event : events) {
        live.apply(event);
    }

    ReplayEngine replay(test_config());
    std::istringstream input(to_jsonl(events));
    replay.replay_stream(input);

    return SessionResult{snapshot_digest(live.build_all_snapshots()),
                         replay.final_state_digest(), replay.stats()};
}

TEST(ReplayTest, ReplayReproducesLiveBookStateExactly) {
    SessionBuilder b;
    const std::vector<MarketEvent> session{
        b.snapshot("tokA", {{0.49, 100.0}, {0.48, 200.0}}, {{0.51, 150.0}, {0.52, 300.0}}),
        b.snapshot("tokB", {{0.20, 500.0}}, {{0.22, 400.0}}),
        b.update("tokA", Side::Bid, 0.50, 400.0),
        b.trade("tokA", 0.51, 25.0),
        b.update("tokA", Side::Bid, 0.50, 0.0),
        b.update("tokB", Side::Ask, 0.22, 10.0),
        b.update("tokA", Side::Ask, 0.52, 0.0),
    };

    const auto result = run_session(session);

    EXPECT_FALSE(result.live_digest.empty());
    EXPECT_EQ(result.replay_digest, result.live_digest);
    EXPECT_EQ(result.stats.malformed_lines, 0u);
    EXPECT_EQ(result.stats.sequence_gaps, 0u);
    EXPECT_EQ(result.stats.books_invalid_at_end, 0u);
}

TEST(ReplayTest, ReplayIsDeterministicAcrossRuns) {
    SessionBuilder b;
    const std::vector<MarketEvent> session{
        b.snapshot("tokA", {{0.49, 100.0}}, {{0.51, 150.0}}),
        b.update("tokA", Side::Bid, 0.50, 400.0),
        b.snapshot("tokB", {{0.10, 5.0}}, {{0.90, 7.0}}),
        b.update("tokB", Side::Ask, 0.80, 1.0),
    };
    const std::string jsonl = to_jsonl(session);

    std::string first;
    std::string second;
    for (std::string* out : {&first, &second}) {
        ReplayEngine replay(test_config());
        std::istringstream input(jsonl);
        replay.replay_stream(input);
        *out = replay.final_state_digest();
    }
    EXPECT_EQ(first, second);
    EXPECT_FALSE(first.empty());
}

TEST(ReplayTest, ReconstructsExpectedTopOfBookFromSnapshotPlusUpdates) {
    SessionBuilder b;
    ReplayEngine replay(test_config());
    std::istringstream input(to_jsonl({
        b.snapshot("tok", {{0.49, 100.0}, {0.48, 200.0}}, {{0.51, 150.0}, {0.52, 300.0}}),
        b.update("tok", Side::Bid, 0.50, 400.0),
    }));
    replay.replay_stream(input);

    const auto snapshots = replay.final_snapshots();
    ASSERT_EQ(snapshots.size(), 1u);
    EXPECT_DOUBLE_EQ(snapshots[0].best_bid, 0.50);
    EXPECT_DOUBLE_EQ(snapshots[0].best_bid_size, 400.0);
    EXPECT_DOUBLE_EQ(snapshots[0].best_ask, 0.51);
    EXPECT_DOUBLE_EQ(snapshots[0].spread, 0.01);
    EXPECT_EQ(snapshots[0].bid_levels, 3u);
    EXPECT_EQ(snapshots[0].ask_levels, 2u);
}

TEST(ReplayTest, MalformedAndTruncatedLinesAreCountedNotFatal) {
    SessionBuilder b;
    std::string jsonl = to_jsonl({
        b.snapshot("tok", {{0.49, 100.0}}, {{0.51, 150.0}}),
        b.update("tok", Side::Bid, 0.50, 400.0),
    });
    jsonl += "{ this line is corrupt\n";
    // A torn final record is the normal result of killing a live engine.
    jsonl += R"({"event_type":"book_update","token_id":"tok","se)";

    ReplayEngine replay(test_config());
    std::istringstream input(jsonl);
    replay.replay_stream(input);

    EXPECT_EQ(replay.stats().malformed_lines, 2u);
    EXPECT_EQ(replay.stats().events_decoded, 2u);
    // The good events still produced a usable book.
    ASSERT_EQ(replay.final_snapshots().size(), 1u);
    EXPECT_DOUBLE_EQ(replay.final_snapshots()[0].best_bid, 0.50);
}

TEST(ReplayTest, DetectsSequenceGapsAndOutOfOrderEvents) {
    SessionBuilder b;
    auto events = std::vector<MarketEvent>{
        b.snapshot("tok", {{0.49, 100.0}}, {{0.51, 150.0}}),
        b.update("tok", Side::Bid, 0.48, 10.0),
        b.update("tok", Side::Bid, 0.47, 10.0),
    };
    header_of(events[1]).sequence = 50;  // gap
    header_of(events[2]).sequence = 20;  // backwards

    ReplayEngine replay(test_config());
    std::istringstream input(to_jsonl(events));
    replay.replay_stream(input);

    EXPECT_EQ(replay.stats().sequence_gaps, 1u);
    EXPECT_EQ(replay.stats().out_of_order_events, 1u);
    EXPECT_EQ(replay.stats().first_sequence, 1u);
}

TEST(ReplayTest, DetectsDuplicateSequences) {
    SessionBuilder b;
    auto events = std::vector<MarketEvent>{
        b.snapshot("tok", {{0.49, 100.0}}, {{0.51, 150.0}}),
        b.update("tok", Side::Bid, 0.48, 10.0),
    };
    header_of(events[1]).sequence = header_of(events[0]).sequence;

    ReplayEngine replay(test_config());
    std::istringstream input(to_jsonl(events));
    replay.replay_stream(input);

    EXPECT_EQ(replay.stats().duplicate_sequences, 1u);
}

TEST(ReplayTest, UpdatesBeforeASnapshotDoNotBuildAFabricatedBook) {
    SessionBuilder b;
    ReplayEngine replay(test_config());
    // Applying increments to a book with an unknown base would yield a
    // plausible-looking but wrong book, so nothing must be published.
    std::istringstream input(to_jsonl({
        b.update("tok", Side::Bid, 0.49, 100.0),
        b.update("tok", Side::Ask, 0.51, 150.0),
    }));
    replay.replay_stream(input);

    EXPECT_TRUE(replay.final_snapshots().empty());
    EXPECT_EQ(replay.stats().events_decoded, 2u);
    EXPECT_GT(replay.stats().resyncs_requested, 0u);
    EXPECT_GT(replay.stats().books_invalid_at_end, 0u);
}

TEST(ReplayTest, RecoversOnceTheSnapshotArrives) {
    SessionBuilder b;
    ReplayEngine replay(test_config());
    std::istringstream input(to_jsonl({
        b.update("tok", Side::Bid, 0.49, 100.0),
        b.snapshot("tok", {{0.49, 100.0}}, {{0.51, 150.0}}),
        b.update("tok", Side::Bid, 0.50, 400.0),
    }));
    replay.replay_stream(input);

    ASSERT_EQ(replay.final_snapshots().size(), 1u);
    EXPECT_DOUBLE_EQ(replay.final_snapshots()[0].best_bid, 0.50);
    EXPECT_EQ(replay.stats().books_invalid_at_end, 0u);
}

TEST(ReplayTest, DisconnectInvalidatesBooksUntilTheyAreResnapshotted) {
    SessionBuilder b;
    ReplayEngine replay(test_config());
    std::istringstream input(to_jsonl({
        b.snapshot("tok", {{0.49, 100.0}}, {{0.51, 150.0}}),
        b.connection(ConnectionState::Disconnected),
        // Updates arriving after a gap must not be trusted against the stale
        // local book.
        b.update("tok", Side::Bid, 0.50, 400.0),
    }));
    replay.replay_stream(input);

    EXPECT_TRUE(replay.final_snapshots().empty());
    EXPECT_GT(replay.stats().books_invalid_at_end, 0u);
}

TEST(ReplayTest, ReplayPerformsNoResyncIoAndStillCompletes) {
    SessionBuilder b;
    ReplayEngine replay(test_config());
    std::istringstream input(to_jsonl({
        b.snapshot("tok", {{0.49, 100.0}}, {{0.51, 150.0}}),
        // A crossed update invalidates the book; live this would trigger a
        // REST fetch, but replay must stay offline and deterministic.
        b.update("tok", Side::Bid, 0.55, 10.0),
    }));
    replay.replay_stream(input);

    EXPECT_GT(replay.stats().resyncs_requested, 0u);
    EXPECT_EQ(replay.stats().crossed_incidents, 1u);
}

TEST(ReplayTest, EmptyInputIsHandledCleanly) {
    ReplayEngine replay(test_config());
    std::istringstream input("");
    replay.replay_stream(input);

    EXPECT_EQ(replay.stats().lines_read, 0u);
    EXPECT_EQ(replay.stats().events_decoded, 0u);
    EXPECT_TRUE(replay.final_state_digest().empty());
}

TEST(ReplayTest, ObserverSeesEveryDecodedEvent) {
    SessionBuilder b;
    ReplayEngine replay(test_config());
    std::size_t observed = 0;
    replay.set_event_observer([&observed](const MarketEvent&, const ReplayStats&) { ++observed; });

    std::istringstream input(to_jsonl({
        b.snapshot("tok", {{0.49, 100.0}}, {{0.51, 150.0}}),
        b.update("tok", Side::Bid, 0.50, 400.0),
        b.trade("tok", 0.51, 5.0),
    }));
    replay.replay_stream(input);

    EXPECT_EQ(observed, 3u);
}

TEST(ReplayTest, DigestIsIndependentOfConfiguredDisplayDepth) {
    SessionBuilder b;
    const std::string jsonl = to_jsonl({
        b.snapshot("tok",
                   {{0.49, 100.0}, {0.48, 90.0}, {0.47, 80.0}, {0.46, 70.0},
                    {0.45, 60.0}, {0.44, 50.0}, {0.43, 40.0}},
                   {{0.51, 150.0}, {0.52, 140.0}, {0.53, 130.0}}),
    });

    std::string shallow;
    std::string deep;
    for (auto [depth, out] : {std::pair<std::size_t, std::string*>{2, &shallow},
                              std::pair<std::size_t, std::string*>{50, &deep}}) {
        Config config = test_config();
        config.order_book_depth = depth;
        ReplayEngine replay(config);
        std::istringstream input(jsonl);
        replay.replay_stream(input);
        *out = replay.final_state_digest();
    }
    // The digest describes the book, not the view of it, so comparing a live
    // session against a replay cannot be defeated by a --depth flag.
    EXPECT_EQ(shallow, deep);
}

}  // namespace
}  // namespace market_data
