// Parquet writer tests.
//
// These run only when the build has Arrow. Where Arrow is absent the writer
// degrades to a null sink, and that degradation is asserted too.
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>

#include "market_data/storage/parquet_writer.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

class TempRoot {
public:
    explicit TempRoot(const std::string& name)
        : path_(std::filesystem::temp_directory_path() / ("mde_parquet_" + name)) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempRoot() { std::filesystem::remove_all(path_); }

    [[nodiscard]] std::string str() const { return path_.string(); }
    [[nodiscard]] const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

Config config_for(const TempRoot& root, std::size_t batch_rows = 10) {
    Config config;
    config.storage.root_directory = root.str();
    config.storage.parquet.batch_rows = batch_rows;
    config.storage.parquet.flush_interval_seconds = 3600;  // Only row-count flushes.
    config.storage.parquet.compression = "zstd";
    return config;
}

// A fixed instant so partition paths are predictable: 2026-09-27T18:30:00Z.
ExchangeTime fixed_capture_time() { return from_epoch_millis(1'790'533'800'000); }

MarketSnapshot make_snapshot(const std::string& token, double bid, double ask) {
    MarketSnapshot s;
    s.token_id = token;
    s.market_id = "0xmarket";
    s.outcome = "Yes";
    s.best_bid_ticks = price_to_ticks(bid);
    s.best_ask_ticks = price_to_ticks(ask);
    s.best_bid = bid;
    s.best_ask = ask;
    s.best_bid_size = 100.0;
    s.best_ask_size = 150.0;
    s.midpoint = (bid + ask) / 2.0;
    s.spread = ask - bid;
    s.bid_depth_l1 = 100.0;
    s.ask_depth_l1 = 150.0;
    s.bid_depth_l5 = 300.0;
    s.ask_depth_l5 = 450.0;
    s.bid_depth_l10 = 600.0;
    s.ask_depth_l10 = 900.0;
    s.imbalance_l1 = -0.2;
    s.imbalance_l5 = -0.2;
    s.imbalance_l10 = -0.2;
    s.bid_levels = 5;
    s.ask_levels = 5;
    s.book_health = "healthy";
    s.sequence_number = 42;
    s.timestamp = now_receive();
    s.exchange_timestamp = fixed_capture_time();
    return s;
}

Trade make_trade(const std::string& token, double price, std::optional<Side> side) {
    Trade t;
    t.token_id = token;
    t.market_id = "0xmarket";
    t.outcome = "Yes";
    t.price = price;
    t.quantity = 25.0;
    t.aggressor_side = side;
    t.trade_id = "0xdeadbeef";
    t.sequence_number = 7;
    t.exchange_timestamp = fixed_capture_time();
    t.receive_timestamp = now_receive();
    return t;
}

Market make_market() {
    Market m;
    m.market_id = "0xmarket";
    m.question = "Will it rain?";
    m.category = "weather";
    m.slug = "will-it-rain";
    m.token_ids = {"tokA", "tokB"};
    m.outcomes = {"Yes", "No"};
    m.active = true;
    m.closed = false;
    m.accepting_orders = true;
    m.reported_volume = 1234.5;
    m.reported_liquidity = 678.9;
    m.reported_volume_24h = 42.0;
    m.min_tick_size = 0.001;
    m.resolution_time = from_epoch_millis(1'800'000'000'000);
    return m;
}

std::vector<std::filesystem::path> find_files(const std::filesystem::path& root,
                                              const std::string& extension) {
    std::vector<std::filesystem::path> found;
    if (!std::filesystem::exists(root)) {
        return found;
    }
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file() && entry.path().extension() == extension) {
            found.push_back(entry.path());
        }
    }
    std::sort(found.begin(), found.end());
    return found;
}

// ---------------------------------------------------------------------------

TEST(ParquetWriterTest, SchemaMatchesTheDocumentedResearchColumns) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const auto books = ParquetStorageWriter::book_columns();
    for (const char* required :
         {"receive_timestamp_ns", "exchange_timestamp_ns", "market_id", "token_id", "outcome",
          "best_bid_ticks", "best_ask_ticks", "best_bid", "best_ask", "best_bid_size",
          "best_ask_size", "midpoint", "spread", "bid_depth_l1", "ask_depth_l1", "bid_depth_l5",
          "ask_depth_l5", "bid_depth_l10", "ask_depth_l10", "imbalance_l1", "imbalance_l5",
          "imbalance_l10", "book_health", "sequence_number"}) {
        EXPECT_NE(std::find(books.begin(), books.end(), required), books.end()) << required;
    }

    const auto trades = ParquetStorageWriter::trade_columns();
    for (const char* required : {"receive_timestamp_ns", "exchange_timestamp_ns", "market_id",
                                 "token_id", "outcome", "price_ticks", "price", "quantity",
                                 "side", "trade_id", "sequence_number"}) {
        EXPECT_NE(std::find(trades.begin(), trades.end(), required), trades.end()) << required;
    }

    const auto markets = ParquetStorageWriter::market_columns();
    for (const char* required :
         {"snapshot_timestamp_ns", "market_id", "condition_id", "question", "category",
          "token_id", "outcome", "active", "closed", "resolution_timestamp_ns",
          "reported_volume", "reported_liquidity", "minimum_tick_size"}) {
        EXPECT_NE(std::find(markets.begin(), markets.end(), required), markets.end()) << required;
    }
}

TEST(ParquetWriterTest, WritesHivePartitionedBookFiles) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("books");
    ParquetStorageWriter writer(config_for(root));
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;

    for (int i = 0; i < 100; ++i) {
        writer.write_book_snapshot(SnapshotRow{make_snapshot("tok", 0.49, 0.51),
                                               fixed_capture_time()});
    }
    writer.close();

    // Exactly the layout the acceptance criteria describe.
    const auto expected = root.path() / "books" / "date=2026-09-27" / "hour=18" / "books.parquet";
    ASSERT_TRUE(std::filesystem::exists(expected)) << expected;
    EXPECT_GT(std::filesystem::file_size(expected), 0u);

    EXPECT_EQ(writer.stats().book_snapshots_written, 100u);
    EXPECT_EQ(writer.stats().errors, 0u);
    EXPECT_GE(writer.stats().files_created, 1u);
}

TEST(ParquetWriterTest, BatchesRatherThanWritingAFilePerRow) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("batching");
    ParquetStorageWriter writer(config_for(root, /*batch_rows=*/10));
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;

    for (int i = 0; i < 100; ++i) {
        writer.write_book_snapshot(SnapshotRow{make_snapshot("tok", 0.49, 0.51),
                                               fixed_capture_time()});
    }
    writer.close();

    // 100 rows at a batch size of 10 is ten row groups in ONE file, not ten
    // files and certainly not a hundred.
    EXPECT_EQ(find_files(root.path() / "books", ".parquet").size(), 1u);
    EXPECT_GE(writer.stats().flush_count, 10u);
}

TEST(ParquetWriterTest, RollsToANewPartitionWhenTheHourChanges) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("rollover");
    ParquetStorageWriter writer(config_for(root, /*batch_rows=*/1000));
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;

    const auto hour_18 = fixed_capture_time();
    const auto hour_19 = hour_18 + std::chrono::hours{1};
    for (int i = 0; i < 5; ++i) {
        writer.write_book_snapshot(SnapshotRow{make_snapshot("tok", 0.49, 0.51), hour_18});
    }
    for (int i = 0; i < 5; ++i) {
        writer.write_book_snapshot(SnapshotRow{make_snapshot("tok", 0.49, 0.51), hour_19});
    }
    writer.close();

    EXPECT_TRUE(std::filesystem::exists(root.path() / "books" / "date=2026-09-27" / "hour=18" /
                                        "books.parquet"));
    EXPECT_TRUE(std::filesystem::exists(root.path() / "books" / "date=2026-09-27" / "hour=19" /
                                        "books.parquet"));
    EXPECT_EQ(writer.stats().book_snapshots_written, 10u);
}

TEST(ParquetWriterTest, HourPartitioningCanBeDisabled) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("daily");
    Config config = config_for(root);
    config.storage.parquet.partition_by_hour = false;

    ParquetStorageWriter writer(config);
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;
    writer.write_book_snapshot(SnapshotRow{make_snapshot("tok", 0.49, 0.51), fixed_capture_time()});
    writer.close();

    EXPECT_TRUE(
        std::filesystem::exists(root.path() / "books" / "date=2026-09-27" / "books.parquet"));
}

TEST(ParquetWriterTest, WritesTradesAndMarkets) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("trades");
    ParquetStorageWriter writer(config_for(root));
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;

    writer.write_trade(TradeRow{make_trade("tok", 0.51, Side::Ask), fixed_capture_time()});
    writer.write_trade(TradeRow{make_trade("tok", 0.52, std::nullopt), fixed_capture_time()});
    writer.write_market_metadata(MarketRow{make_market(), fixed_capture_time()});
    writer.close();

    EXPECT_TRUE(std::filesystem::exists(root.path() / "trades" / "date=2026-09-27" / "hour=18" /
                                        "trades.parquet"));
    EXPECT_TRUE(
        std::filesystem::exists(root.path() / "markets" / "date=2026-09-27" / "markets.parquet"));

    EXPECT_EQ(writer.stats().trades_written, 2u);
    // One row per (market, token): the binary market becomes two rows.
    EXPECT_EQ(writer.stats().metadata_rows_written, 2u);
}

TEST(ParquetWriterTest, NoTemporaryFilesSurviveACleanClose) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("atomic");
    ParquetStorageWriter writer(config_for(root));
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;
    writer.write_book_snapshot(SnapshotRow{make_snapshot("tok", 0.49, 0.51), fixed_capture_time()});
    writer.write_trade(TradeRow{make_trade("tok", 0.51, Side::Bid), fixed_capture_time()});

    // Before close the data is still under a .tmp name and must not be
    // mistaken for a finished partition.
    EXPECT_FALSE(find_files(root.path(), ".tmp").empty());

    writer.close();

    EXPECT_TRUE(find_files(root.path(), ".tmp").empty());
    EXPECT_FALSE(find_files(root.path(), ".parquet").empty());
}

TEST(ParquetWriterTest, RestartWithinTheSamePartitionDoesNotClobber) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("restart");
    for (int run = 0; run < 2; ++run) {
        ParquetStorageWriter writer(config_for(root));
        std::string error;
        ASSERT_TRUE(writer.start(error)) << error;
        writer.write_book_snapshot(
            SnapshotRow{make_snapshot("tok", 0.49, 0.51), fixed_capture_time()});
        writer.close();
    }
    // The second run must not destroy the first run's finished partition.
    EXPECT_EQ(find_files(root.path() / "books", ".parquet").size(), 2u);
}

TEST(ParquetWriterTest, FlushWithNothingBufferedIsHarmless) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("emptyflush");
    ParquetStorageWriter writer(config_for(root));
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;

    writer.flush();
    writer.flush();
    writer.close();
    writer.close();  // Idempotent.

    EXPECT_EQ(writer.stats().errors, 0u);
    EXPECT_EQ(writer.stats().rows_written(), 0u);
}

TEST(ParquetWriterTest, DisabledConfigYieldsANullWriterThatStillCounts) {
    const TempRoot root("disabled");
    Config config = config_for(root);
    config.storage.parquet.enabled = false;

    const auto writer = make_storage_writer(config);
    ASSERT_NE(writer, nullptr);
    std::string error;
    EXPECT_TRUE(writer->start(error));

    writer->write_book_snapshot(SnapshotRow{make_snapshot("tok", 0.49, 0.51), fixed_capture_time()});
    writer->flush();
    writer->close();

    EXPECT_EQ(writer->stats().book_snapshots_written, 1u);
    EXPECT_TRUE(find_files(root.path(), ".parquet").empty());
    EXPECT_EQ(writer->describe(), "disabled");
}

TEST(ParquetWriterTest, FactoryReturnsAWriterWhenEnabled) {
    const TempRoot root("factory");
    const auto writer = make_storage_writer(config_for(root));
    ASSERT_NE(writer, nullptr);
    if (parquet_support_available()) {
        EXPECT_NE(writer->describe().find("parquet"), std::string::npos);
    } else {
        // Without Arrow the engine must still run, just without the dataset.
        EXPECT_EQ(writer->describe(), "disabled");
    }
}

}  // namespace
}  // namespace market_data
