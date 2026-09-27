// Storage integration: Parquet on disk, queried back through DuckDB.
//
// The end-to-end case drives synthetic market events through the real
// OrderBookManager, generates snapshots the way the engine does, writes them
// as Parquet and then asserts on the result via SQL -- so a break anywhere in
// that chain shows up here rather than in production.
#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include <fstream>
#include <sstream>

#include "market_data/event_codec.hpp"
#include "market_data/order_book_manager.hpp"
#include "market_data/replay.hpp"
#include "market_data/storage/storage_manager.hpp"
#include "market_data/storage/duckdb_catalog.hpp"
#include "market_data/storage/parquet_writer.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

class TempRoot {
public:
    explicit TempRoot(const std::string& name)
        : path_(std::filesystem::temp_directory_path() / ("mde_storage_" + name)) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempRoot() { std::filesystem::remove_all(path_); }
    [[nodiscard]] std::string str() const { return path_.string(); }
    [[nodiscard]] const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

Config config_for(const TempRoot& root) {
    Config config;
    config.storage.root_directory = root.str();
    config.storage.parquet.batch_rows = 25;
    config.storage.parquet.flush_interval_seconds = 3600;
    config.order_book_depth = 20;
    config.health.resync_cooldown_ms = 0;
    return config;
}

ExchangeTime fixed_capture_time() { return from_epoch_millis(1'790'533'800'000); }

bool storage_stack_available() {
    return parquet_support_available() && duckdb_support_available();
}

MarketSnapshot make_snapshot(const std::string& token, double bid, double ask, double bid_size,
                             double ask_size) {
    MarketSnapshot s;
    s.token_id = token;
    s.market_id = "0xmarket";
    s.outcome = "Yes";
    s.best_bid_ticks = price_to_ticks(bid);
    s.best_ask_ticks = price_to_ticks(ask);
    s.best_bid = bid;
    s.best_ask = ask;
    s.best_bid_size = bid_size;
    s.best_ask_size = ask_size;
    s.midpoint = (bid + ask) / 2.0;
    s.spread = ask - bid;
    s.bid_depth_l1 = bid_size;
    s.ask_depth_l1 = ask_size;
    s.bid_depth_l5 = bid_size * 3.0;
    s.ask_depth_l5 = ask_size * 3.0;
    s.bid_depth_l10 = bid_size * 6.0;
    s.ask_depth_l10 = ask_size * 6.0;
    s.imbalance_l1 = (bid_size - ask_size) / (bid_size + ask_size);
    s.imbalance_l5 = s.imbalance_l1;
    s.imbalance_l10 = s.imbalance_l1;
    s.bid_levels = 5;
    s.ask_levels = 5;
    s.book_health = "healthy";
    s.sequence_number = 1;
    s.timestamp = now_receive();
    s.exchange_timestamp = fixed_capture_time();
    return s;
}

// ---------------------------------------------------------------------------
// Catalog behaviour
// ---------------------------------------------------------------------------

TEST(StorageIntegrationTest, CountsExactlyTheRowsThatWereWritten) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("count");
    const Config config = config_for(root);

    {
        ParquetStorageWriter writer(config);
        std::string error;
        ASSERT_TRUE(writer.start(error)) << error;
        for (int i = 0; i < 100; ++i) {
            writer.write_book_snapshot(
                SnapshotRow{make_snapshot("tok", 0.49, 0.51, 100.0, 150.0), fixed_capture_time()});
        }
        writer.close();
    }

    DuckDbCatalog catalog(config);
    std::string error;
    ASSERT_TRUE(catalog.open(error)) << error;
    const auto status = catalog.refresh_views();
    EXPECT_TRUE(status.ok()) << (status.errors.empty() ? "" : status.errors.front());

    const auto count = catalog.query("SELECT COUNT(*) FROM book_snapshots");
    ASSERT_TRUE(count.ok()) << count.error;
    EXPECT_EQ(count.scalar(), "100");
    catalog.close();
}

TEST(StorageIntegrationTest, ValuesSurviveTheRoundTripThroughParquet) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("values");
    const Config config = config_for(root);

    {
        ParquetStorageWriter writer(config);
        std::string error;
        ASSERT_TRUE(writer.start(error)) << error;
        writer.write_book_snapshot(
            SnapshotRow{make_snapshot("known-token", 0.49, 0.51, 100.0, 150.0),
                        fixed_capture_time()});
        writer.close();
    }

    DuckDbCatalog catalog(config);
    std::string error;
    ASSERT_TRUE(catalog.open(error)) << error;
    ASSERT_TRUE(catalog.refresh_views().ok());

    const auto row = catalog.query(R"(
        SELECT market_id, token_id, outcome,
               best_bid, best_ask, best_bid_ticks, best_ask_ticks,
               best_bid_size, best_ask_size,
               midpoint, spread, imbalance_l1, bid_depth_l5, ask_depth_l5,
               book_health
        FROM book_snapshots WHERE token_id = 'known-token')");
    ASSERT_TRUE(row.ok()) << row.error;
    ASSERT_EQ(row.row_count(), 1u);

    const auto& r = row.rows[0];
    EXPECT_EQ(r[0], "0xmarket");
    EXPECT_EQ(r[1], "known-token");
    EXPECT_EQ(r[2], "Yes");
    EXPECT_DOUBLE_EQ(std::stod(r[3]), 0.49);
    EXPECT_DOUBLE_EQ(std::stod(r[4]), 0.51);
    // Integer ticks persist alongside the decimals, so a researcher never has
    // to re-derive them.
    EXPECT_EQ(r[5], "490");
    EXPECT_EQ(r[6], "510");
    EXPECT_DOUBLE_EQ(std::stod(r[7]), 100.0);
    EXPECT_DOUBLE_EQ(std::stod(r[8]), 150.0);
    EXPECT_DOUBLE_EQ(std::stod(r[9]), 0.50);
    EXPECT_NEAR(std::stod(r[10]), 0.02, 1e-9);
    EXPECT_NEAR(std::stod(r[11]), -0.2, 1e-9);
    EXPECT_DOUBLE_EQ(std::stod(r[12]), 300.0);
    EXPECT_DOUBLE_EQ(std::stod(r[13]), 450.0);
    EXPECT_EQ(r[14], "healthy");
    catalog.close();
}

TEST(StorageIntegrationTest, UndefinedStatisticsBecomeSqlNullsNotZeroOrNaN) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("nulls");
    const Config config = config_for(root);

    {
        ParquetStorageWriter writer(config);
        std::string error;
        ASSERT_TRUE(writer.start(error)) << error;

        // A one-sided book: no bid at all. Zero is a valid price here, so
        // absence must be a NULL, and NaN would poison every AVG downstream.
        MarketSnapshot one_sided;
        one_sided.token_id = "one-sided";
        one_sided.market_id = "0xmarket";
        one_sided.best_ask_ticks = 510;
        one_sided.best_ask = 0.51;
        one_sided.best_ask_size = 150.0;
        one_sided.book_health = "healthy";
        one_sided.timestamp = now_receive();
        writer.write_book_snapshot(SnapshotRow{one_sided, fixed_capture_time()});
        writer.close();
    }

    DuckDbCatalog catalog(config);
    std::string error;
    ASSERT_TRUE(catalog.open(error)) << error;
    ASSERT_TRUE(catalog.refresh_views().ok());

    const auto nulls = catalog.query(R"(
        SELECT best_bid IS NULL, best_bid_ticks IS NULL,
               midpoint IS NULL, spread IS NULL, imbalance_l1 IS NULL,
               best_ask IS NULL
        FROM book_snapshots WHERE token_id = 'one-sided')");
    ASSERT_TRUE(nulls.ok()) << nulls.error;
    ASSERT_EQ(nulls.row_count(), 1u);
    EXPECT_EQ(nulls.rows[0][0], "true");
    EXPECT_EQ(nulls.rows[0][1], "true");
    EXPECT_EQ(nulls.rows[0][2], "true");
    EXPECT_EQ(nulls.rows[0][3], "true");
    EXPECT_EQ(nulls.rows[0][4], "true");
    EXPECT_EQ(nulls.rows[0][5], "false");

    // The spreads view exists precisely so this row is excluded by default.
    const auto spreads = catalog.query("SELECT COUNT(*) FROM spreads");
    ASSERT_TRUE(spreads.ok()) << spreads.error;
    EXPECT_EQ(spreads.scalar(), "0");
    catalog.close();
}

TEST(StorageIntegrationTest, HivePartitionKeysAreQueryableColumns) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("hive");
    const Config config = config_for(root);

    {
        ParquetStorageWriter writer(config);
        std::string error;
        ASSERT_TRUE(writer.start(error)) << error;
        writer.write_book_snapshot(
            SnapshotRow{make_snapshot("tok", 0.49, 0.51, 100.0, 150.0), fixed_capture_time()});
        writer.write_book_snapshot(
            SnapshotRow{make_snapshot("tok", 0.49, 0.51, 100.0, 150.0),
                        fixed_capture_time() + std::chrono::hours{1}});
        writer.close();
    }

    DuckDbCatalog catalog(config);
    std::string error;
    ASSERT_TRUE(catalog.open(error)) << error;
    ASSERT_TRUE(catalog.refresh_views().ok());

    // date/hour come from the directory names, not from a stored column.
    const auto partitions = catalog.query(
        "SELECT date, hour, COUNT(*) FROM book_snapshots GROUP BY date, hour ORDER BY hour");
    ASSERT_TRUE(partitions.ok()) << partitions.error;
    ASSERT_EQ(partitions.row_count(), 2u);
    EXPECT_EQ(partitions.rows[0][0], "2026-09-27");
    EXPECT_EQ(partitions.rows[0][1], "18");
    EXPECT_EQ(partitions.rows[1][1], "19");
    catalog.close();
}

TEST(StorageIntegrationTest, TradeSideIsNeverFabricated) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("tradeside");
    const Config config = config_for(root);

    {
        ParquetStorageWriter writer(config);
        std::string error;
        ASSERT_TRUE(writer.start(error)) << error;
        for (const auto side : {std::optional<Side>{Side::Bid}, std::optional<Side>{Side::Ask},
                                std::optional<Side>{}}) {
            Trade trade;
            trade.token_id = "tok";
            trade.market_id = "0xmarket";
            trade.price = 0.51;
            trade.quantity = 25.0;
            trade.aggressor_side = side;
            trade.receive_timestamp = now_receive();
            trade.exchange_timestamp = fixed_capture_time();
            writer.write_trade(TradeRow{trade, fixed_capture_time()});
        }
        writer.close();
    }

    DuckDbCatalog catalog(config);
    std::string error;
    ASSERT_TRUE(catalog.open(error)) << error;
    ASSERT_TRUE(catalog.refresh_views().ok());

    const auto sides =
        catalog.query("SELECT side, COUNT(*) FROM trades GROUP BY side ORDER BY side");
    ASSERT_TRUE(sides.ok()) << sides.error;
    ASSERT_EQ(sides.row_count(), 3u);
    EXPECT_EQ(sides.rows[0][0], "buy");
    EXPECT_EQ(sides.rows[1][0], "sell");
    // An unreported side stays "unknown" rather than being guessed into buy.
    EXPECT_EQ(sides.rows[2][0], "unknown");
    catalog.close();
}

TEST(StorageIntegrationTest, MarketMetadataIsOneRowPerToken) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("markets");
    const Config config = config_for(root);

    {
        ParquetStorageWriter writer(config);
        std::string error;
        ASSERT_TRUE(writer.start(error)) << error;

        Market market;
        market.market_id = "0xmarket";
        market.question = "Will it rain?";
        market.category = "weather";
        market.token_ids = {"tokA", "tokB"};
        market.outcomes = {"Yes", "No"};
        market.active = true;
        market.reported_volume = 1234.5;
        market.reported_liquidity = 678.9;
        market.min_tick_size = 0.001;
        writer.write_market_metadata(MarketRow{market, fixed_capture_time()});
        writer.close();
    }

    DuckDbCatalog catalog(config);
    std::string error;
    ASSERT_TRUE(catalog.open(error)) << error;
    ASSERT_TRUE(catalog.refresh_views().ok());

    const auto rows = catalog.query(
        "SELECT token_id, outcome, question, minimum_tick_size FROM markets ORDER BY token_id");
    ASSERT_TRUE(rows.ok()) << rows.error;
    ASSERT_EQ(rows.row_count(), 2u);
    EXPECT_EQ(rows.rows[0][0], "tokA");
    EXPECT_EQ(rows.rows[0][1], "Yes");
    EXPECT_EQ(rows.rows[1][0], "tokB");
    EXPECT_EQ(rows.rows[1][1], "No");
    EXPECT_EQ(rows.rows[0][2], "Will it rain?");
    EXPECT_NEAR(std::stod(rows.rows[0][3]), 0.001, 1e-9);
    catalog.close();
}

TEST(StorageIntegrationTest, ResearchViewsAnswerTheDocumentedQueries) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("views");
    const Config config = config_for(root);

    {
        ParquetStorageWriter writer(config);
        std::string error;
        ASSERT_TRUE(writer.start(error)) << error;
        for (int i = 0; i < 10; ++i) {
            writer.write_book_snapshot(
                SnapshotRow{make_snapshot("wide", 0.40, 0.60, 100.0, 100.0),
                            fixed_capture_time()});
            writer.write_book_snapshot(
                SnapshotRow{make_snapshot("tight", 0.49, 0.51, 100.0, 300.0),
                            fixed_capture_time()});
        }
        writer.close();
    }

    DuckDbCatalog catalog(config);
    std::string error;
    ASSERT_TRUE(catalog.open(error)) << error;
    const auto status = catalog.refresh_views();
    ASSERT_TRUE(status.ok()) << (status.errors.empty() ? "" : status.errors.front());
    for (const char* view : {"book_snapshots", "spreads", "order_book_features", "market_summary"}) {
        EXPECT_NE(std::find(status.views_created.begin(), status.views_created.end(), view),
                  status.views_created.end())
            << view;
    }

    // The exact aggregation from the acceptance criteria.
    const auto ranked = catalog.query(R"(
        SELECT token_id, AVG(spread) AS avg_spread, AVG(ABS(imbalance_l1)) AS avg_imbalance
        FROM book_snapshots
        GROUP BY token_id
        ORDER BY avg_spread DESC)");
    ASSERT_TRUE(ranked.ok()) << ranked.error;
    ASSERT_EQ(ranked.row_count(), 2u);
    EXPECT_EQ(ranked.rows[0][0], "wide");
    EXPECT_NEAR(std::stod(ranked.rows[0][1]), 0.20, 1e-9);
    EXPECT_NEAR(std::stod(ranked.rows[0][2]), 0.0, 1e-9);
    EXPECT_EQ(ranked.rows[1][0], "tight");
    EXPECT_NEAR(std::stod(ranked.rows[1][1]), 0.02, 1e-9);
    EXPECT_NEAR(std::stod(ranked.rows[1][2]), 0.5, 1e-9);

    const auto summary = catalog.query(
        "SELECT token_id, snapshot_count, median_spread FROM market_summary ORDER BY token_id");
    ASSERT_TRUE(summary.ok()) << summary.error;
    ASSERT_EQ(summary.row_count(), 2u);
    EXPECT_EQ(summary.rows[0][1], "10");
    EXPECT_EQ(summary.rows[1][1], "10");

    const auto features = catalog.query(
        "SELECT COUNT(*) FROM order_book_features WHERE imbalance_l5 IS NOT NULL");
    ASSERT_TRUE(features.ok()) << features.error;
    EXPECT_EQ(features.scalar(), "20");
    catalog.close();
}

TEST(StorageIntegrationTest, EmptyDatasetSkipsViewsInsteadOfFailing) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("empty");
    DuckDbCatalog catalog(config_for(root));
    std::string error;
    ASSERT_TRUE(catalog.open(error)) << error;

    // The normal state on a first run, before the first flush.
    const auto status = catalog.refresh_views();
    EXPECT_TRUE(status.errors.empty());
    EXPECT_TRUE(status.views_created.empty());
    EXPECT_FALSE(status.views_skipped.empty());
    EXPECT_TRUE(std::filesystem::exists(catalog.status().database_path));
    catalog.close();
}

TEST(StorageIntegrationTest, InvalidSqlIsReportedNotThrown) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("badsql");
    DuckDbCatalog catalog(config_for(root));
    std::string error;
    ASSERT_TRUE(catalog.open(error)) << error;

    const auto result = catalog.query("SELECT * FROM table_that_does_not_exist");
    EXPECT_FALSE(result.ok());
    EXPECT_FALSE(result.error.empty());
    catalog.close();
}

// ---------------------------------------------------------------------------
// End-to-end: synthetic events -> OrderBookManager -> snapshots -> Parquet -> SQL
// ---------------------------------------------------------------------------

TEST(StorageIntegrationTest, EndToEndFromMarketEventsThroughParquetToSql) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("endtoend");
    const Config config = config_for(root);

    constexpr int kSnapshotRounds = 50;
    OrderBookManager manager(config);
    manager.set_tracked_tokens({"tokA", "tokB"});

    const auto header = [](const TokenId& token, std::uint64_t seq) {
        EventHeader h;
        h.exchange = "polymarket";
        h.market_id = "0xmarket";
        h.token_id = token;
        h.exchange_timestamp = now_exchange();
        h.receive_timestamp = now_receive();
        h.sequence = seq;
        return h;
    };

    std::uint64_t seq = 0;
    {
        BookSnapshotEvent a;
        a.header = header("tokA", ++seq);
        a.bids = {{0.49, 100.0}, {0.48, 200.0}};
        a.asks = {{0.51, 150.0}, {0.52, 300.0}};
        manager.apply(MarketEvent{a});

        BookSnapshotEvent b;
        b.header = header("tokB", ++seq);
        b.bids = {{0.20, 500.0}};
        b.asks = {{0.22, 400.0}};
        manager.apply(MarketEvent{b});
    }

    ParquetStorageWriter writer(config);
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;

    std::vector<Trade> expected_trades;
    for (int round = 0; round < kSnapshotRounds; ++round) {
        // Nudge the book so the dataset is not a constant.
        BookUpdateEvent update;
        update.header = header("tokA", ++seq);
        update.updates.push_back(
            BookUpdate{"tokA", Side::Bid, 0.48, 200.0 + round, {}, {}});
        manager.apply(MarketEvent{update});

        for (auto snapshot : manager.build_all_snapshots()) {
            snapshot.outcome = snapshot.token_id == "tokA" ? "Yes" : "No";
            writer.write_book_snapshot(SnapshotRow{snapshot, fixed_capture_time()});
        }

        if (round % 10 == 0) {
            Trade trade;
            trade.token_id = "tokA";
            trade.market_id = "0xmarket";
            trade.outcome = "Yes";
            trade.price = 0.51;
            trade.quantity = 25.0;
            trade.aggressor_side = Side::Ask;
            trade.sequence_number = seq;
            trade.receive_timestamp = now_receive();
            trade.exchange_timestamp = now_exchange();
            writer.write_trade(TradeRow{trade, fixed_capture_time()});
            expected_trades.push_back(trade);
        }
    }
    writer.close();

    DuckDbCatalog catalog(config);
    ASSERT_TRUE(catalog.open(error)) << error;
    ASSERT_TRUE(catalog.refresh_views().ok());

    // Two tracked books, one snapshot each per round.
    const auto total = catalog.query("SELECT COUNT(*) FROM book_snapshots");
    ASSERT_TRUE(total.ok()) << total.error;
    EXPECT_EQ(total.scalar(), std::to_string(kSnapshotRounds * 2));

    const auto trades = catalog.query("SELECT COUNT(*) FROM trades");
    ASSERT_TRUE(trades.ok()) << trades.error;
    EXPECT_EQ(trades.scalar(), std::to_string(expected_trades.size()));

    // A known market, checked against what the order book actually held.
    const auto known = catalog.query(R"(
        SELECT best_bid, best_ask, spread, midpoint, bid_depth_l5, ask_depth_l5, outcome
        FROM book_snapshots
        WHERE token_id = 'tokB'
        LIMIT 1)");
    ASSERT_TRUE(known.ok()) << known.error;
    ASSERT_EQ(known.row_count(), 1u);
    EXPECT_DOUBLE_EQ(std::stod(known.rows[0][0]), 0.20);
    EXPECT_DOUBLE_EQ(std::stod(known.rows[0][1]), 0.22);
    EXPECT_NEAR(std::stod(known.rows[0][2]), 0.02, 1e-9);
    EXPECT_NEAR(std::stod(known.rows[0][3]), 0.21, 1e-9);
    EXPECT_DOUBLE_EQ(std::stod(known.rows[0][4]), 500.0);
    EXPECT_DOUBLE_EQ(std::stod(known.rows[0][5]), 400.0);
    EXPECT_EQ(known.rows[0][6], "No");

    // The depth nudge must be visible as variation in tokA's L5 bid depth.
    const auto varying = catalog.query(
        "SELECT COUNT(DISTINCT bid_depth_l5) FROM book_snapshots WHERE token_id = 'tokA'");
    ASSERT_TRUE(varying.ok()) << varying.error;
    EXPECT_GT(std::stoi(varying.scalar()), 1);

    // Every persisted book was one the engine trusted.
    const auto unhealthy = catalog.query(
        "SELECT COUNT(*) FROM book_snapshots WHERE book_health NOT IN ('healthy', 'stale')");
    ASSERT_TRUE(unhealthy.ok()) << unhealthy.error;
    EXPECT_EQ(unhealthy.scalar(), "0");

    catalog.close();
}

// ---------------------------------------------------------------------------
// Milestone 7: rebuild the normalized dataset by replaying the raw archive.
//
// This is the property that makes the raw archive the recovery path it claims
// to be: if Parquet is lost, or the schema changes, the dataset can be
// regenerated from the log without recollecting.
// ---------------------------------------------------------------------------

TEST(StorageIntegrationTest, ReplayRebuildsAnEquivalentNormalizedDataset) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("rebuild");
    Config config = config_for(root);
    config.storage.parquet.snapshot_interval_ms = 100;

    // --- a synthetic recorded session -------------------------------------
    const auto base = now_receive();
    std::uint64_t seq = 0;
    std::vector<MarketEvent> session;

    const auto header = [&](const TokenId& token, std::chrono::milliseconds offset) {
        EventHeader h;
        h.exchange = "polymarket";
        h.market_id = "0xmarket";
        h.token_id = token;
        h.exchange_timestamp = from_epoch_millis(1'790'533'800'000 + offset.count());
        h.receive_timestamp = base + offset;
        h.sequence = ++seq;
        return h;
    };

    {
        BookSnapshotEvent a;
        a.header = header("tokA", std::chrono::milliseconds{0});
        a.bids = {{0.49, 100.0}, {0.48, 200.0}};
        a.asks = {{0.51, 150.0}, {0.52, 300.0}};
        session.emplace_back(std::move(a));

        BookSnapshotEvent b;
        b.header = header("tokB", std::chrono::milliseconds{1});
        b.bids = {{0.20, 500.0}};
        b.asks = {{0.22, 400.0}};
        session.emplace_back(std::move(b));
    }
    for (int i = 1; i <= 20; ++i) {
        BookUpdateEvent update;
        update.header = header("tokA", std::chrono::milliseconds{i * 100});
        update.updates.push_back(
            BookUpdate{"tokA", Side::Bid, 0.48, 200.0 + i, {}, {}});
        session.emplace_back(std::move(update));

        if (i % 5 == 0) {
            TradeEvent trade;
            trade.header = header("tokA", std::chrono::milliseconds{i * 100 + 1});
            trade.trade.token_id = "tokA";
            trade.trade.market_id = "0xmarket";
            trade.trade.price = 0.51;
            trade.trade.quantity = 10.0;
            trade.trade.aggressor_side = Side::Ask;
            trade.trade.receive_timestamp = trade.header.receive_timestamp;
            trade.trade.exchange_timestamp = trade.header.exchange_timestamp;
            session.emplace_back(std::move(trade));
        }
    }

    const auto log_path = root.path() / "session.jsonl";
    {
        std::ofstream out(log_path, std::ios::binary);
        ASSERT_TRUE(out.good());
        for (const auto& event : session) {
            out << encode_event_line(event);
        }
    }

    // --- rebuild from the log ---------------------------------------------
    ReplayEngine replay(config);
    StorageManager storage(config);
    std::string error;
    ASSERT_TRUE(storage.start(error)) << error;

    replay.set_snapshot_observer(
        [&storage](const MarketSnapshot& snapshot) { storage.record_snapshot(snapshot); });
    replay.set_trade_observer([&storage](const Trade& trade) { storage.record_trade(trade); });

    ASSERT_TRUE(replay.replay_file(log_path.string(), error)) << error;
    storage.shutdown();

    EXPECT_EQ(replay.stats().malformed_lines, 0u);
    EXPECT_EQ(replay.stats().sequence_gaps, 0u);
    EXPECT_GT(replay.stats().snapshots_emitted, 0u);
    EXPECT_EQ(replay.stats().trades_emitted, 4u);

    // Everything the replay emitted must be on disk.
    const auto stats = storage.stats();
    EXPECT_EQ(stats.book_snapshots_written, replay.stats().snapshots_emitted);
    EXPECT_EQ(stats.trades_written, replay.stats().trades_emitted);
    EXPECT_EQ(stats.events_dropped, 0u);
    EXPECT_EQ(stats.storage_errors, 0u);

    // --- and the rebuilt dataset must describe the same books --------------
    DuckDbCatalog catalog(config);
    ASSERT_TRUE(catalog.open(error)) << error;
    ASSERT_TRUE(catalog.refresh_views().ok());

    const auto counts = catalog.query("SELECT COUNT(*) FROM book_snapshots");
    ASSERT_TRUE(counts.ok()) << counts.error;
    EXPECT_EQ(counts.scalar(), std::to_string(replay.stats().snapshots_emitted));

    // tokB never changed after its snapshot, so every row must agree with the
    // book state the replay reconstructed.
    const auto tok_b = catalog.query(R"(
        SELECT DISTINCT best_bid, best_ask, spread, bid_depth_l5, ask_depth_l5
        FROM book_snapshots WHERE token_id = 'tokB')");
    ASSERT_TRUE(tok_b.ok()) << tok_b.error;
    ASSERT_EQ(tok_b.row_count(), 1u);
    EXPECT_DOUBLE_EQ(std::stod(tok_b.rows[0][0]), 0.20);
    EXPECT_DOUBLE_EQ(std::stod(tok_b.rows[0][1]), 0.22);
    EXPECT_NEAR(std::stod(tok_b.rows[0][2]), 0.02, 1e-9);
    EXPECT_DOUBLE_EQ(std::stod(tok_b.rows[0][3]), 500.0);
    EXPECT_DOUBLE_EQ(std::stod(tok_b.rows[0][4]), 400.0);

    // The final row for tokA must match the final in-memory book exactly.
    const auto final_book = replay.books().build_snapshot("tokA");
    ASSERT_TRUE(final_book.has_value());
    const auto tok_a = catalog.query(R"(
        SELECT best_bid, best_ask, bid_depth_l5
        FROM book_snapshots WHERE token_id = 'tokA'
        ORDER BY receive_timestamp_ns DESC LIMIT 1)");
    ASSERT_TRUE(tok_a.ok()) << tok_a.error;
    ASSERT_EQ(tok_a.row_count(), 1u);
    EXPECT_DOUBLE_EQ(std::stod(tok_a.rows[0][0]), final_book->best_bid);
    EXPECT_DOUBLE_EQ(std::stod(tok_a.rows[0][1]), final_book->best_ask);
    EXPECT_DOUBLE_EQ(std::stod(tok_a.rows[0][2]), final_book->bid_depth_l5);

    catalog.close();
}

TEST(StorageIntegrationTest, RebuildingTheSameLogTwiceProducesTheSameRows) {
    if (!storage_stack_available()) {
        GTEST_SKIP() << "built without Arrow or DuckDB";
    }
    const TempRoot root("determinism");
    Config config = config_for(root);
    config.storage.parquet.snapshot_interval_ms = 100;

    const auto base = now_receive();
    std::uint64_t seq = 0;
    std::ostringstream log;
    {
        BookSnapshotEvent a;
        a.header.exchange = "polymarket";
        a.header.market_id = "0xmarket";
        a.header.token_id = "tok";
        a.header.receive_timestamp = base;
        a.header.exchange_timestamp = from_epoch_millis(1'790'533'800'000);
        a.header.sequence = ++seq;
        a.bids = {{0.49, 100.0}};
        a.asks = {{0.51, 150.0}};
        log << encode_event_line(MarketEvent{a});
    }
    for (int i = 1; i <= 30; ++i) {
        BookUpdateEvent u;
        u.header.exchange = "polymarket";
        u.header.market_id = "0xmarket";
        u.header.token_id = "tok";
        u.header.receive_timestamp = base + std::chrono::milliseconds{i * 50};
        u.header.exchange_timestamp = from_epoch_millis(1'790'533'800'000 + i * 50);
        u.header.sequence = ++seq;
        u.updates.push_back(BookUpdate{"tok", Side::Bid, 0.48, 10.0 * i, {}, {}});
        log << encode_event_line(MarketEvent{u});
    }

    // Replay is deterministic, so two rebuilds of one log must agree on both
    // the row count and the final book -- that is what makes a regenerated
    // dataset trustworthy.
    std::vector<std::string> counts;
    std::vector<std::string> digests;
    for (int run = 0; run < 2; ++run) {
        Config run_config = config;
        run_config.storage.root_directory = (root.path() / ("run" + std::to_string(run))).string();

        ReplayEngine replay(run_config);
        StorageManager storage(run_config);
        std::string error;
        ASSERT_TRUE(storage.start(error)) << error;
        replay.set_snapshot_observer(
            [&storage](const MarketSnapshot& s) { storage.record_snapshot(s); });

        std::istringstream input(log.str());
        replay.replay_stream(input);
        storage.shutdown();
        digests.push_back(replay.final_state_digest());

        DuckDbCatalog catalog(run_config);
        ASSERT_TRUE(catalog.open(error)) << error;
        ASSERT_TRUE(catalog.refresh_views().ok());
        const auto result = catalog.query("SELECT COUNT(*) FROM book_snapshots");
        ASSERT_TRUE(result.ok()) << result.error;
        counts.push_back(result.scalar());
        catalog.close();
    }

    EXPECT_EQ(counts[0], counts[1]);
    EXPECT_EQ(digests[0], digests[1]);
    EXPECT_NE(counts[0], "0");
}

}  // namespace
}  // namespace market_data
