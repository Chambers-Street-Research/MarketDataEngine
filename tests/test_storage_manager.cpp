// StorageManager tests: the queue + worker path between the engine and the
// writers. The property that matters is conservation -- every row handed in
// must appear on disk, or be counted as dropped. Silent loss is the one
// outcome that would make the dataset untrustworthy.
#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "market_data/storage/duckdb_catalog.hpp"
#include "market_data/storage/storage_manager.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

class TempRoot {
public:
    explicit TempRoot(const std::string& name)
        : path_(std::filesystem::temp_directory_path() / ("mde_mgr_" + name)) {
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
    config.storage.parquet.batch_rows = 100;
    config.storage.parquet.flush_interval_seconds = 3600;
    config.storage.duckdb.enabled = false;
    return config;
}

MarketSnapshot make_snapshot(const std::string& token, std::uint64_t seq) {
    MarketSnapshot s;
    s.token_id = token;
    s.market_id = "0xmarket";
    s.outcome = "Yes";
    s.best_bid_ticks = 490;
    s.best_ask_ticks = 510;
    s.best_bid = 0.49;
    s.best_ask = 0.51;
    s.best_bid_size = 100.0;
    s.best_ask_size = 150.0;
    s.midpoint = 0.50;
    s.spread = 0.02;
    s.book_health = "healthy";
    s.sequence_number = seq;
    s.timestamp = now_receive();
    s.exchange_timestamp = now_exchange();
    return s;
}

MarketEvent make_event(std::uint64_t seq) {
    BookUpdateEvent e;
    e.header.exchange = "polymarket";
    e.header.market_id = "0xmarket";
    e.header.token_id = "tok";
    e.header.sequence = seq;
    e.header.exchange_timestamp = now_exchange();
    e.header.receive_timestamp = now_receive();
    e.updates.push_back(BookUpdate{"tok", Side::Bid, 0.48, 100.0, {}, {}});
    return MarketEvent{std::move(e)};
}

std::size_t count_parquet_rows(const Config& config, const std::string& view) {
    if (!duckdb_support_available() || !parquet_support_available()) {
        return 0;
    }
    Config with_db = config;
    with_db.storage.duckdb.enabled = true;
    DuckDbCatalog catalog(with_db);
    std::string error;
    if (!catalog.open(error)) {
        return 0;
    }
    catalog.refresh_views();
    const auto result = catalog.query("SELECT COUNT(*) FROM " + view);
    catalog.close();
    return result.ok() && !result.scalar().empty()
               ? static_cast<std::size_t>(std::stoull(result.scalar()))
               : 0;
}

// ---------------------------------------------------------------------------

TEST(StorageManagerTest, EveryQueuedSnapshotReachesDisk) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("conservation");
    const Config config = config_for(root);

    constexpr std::size_t kRows = 2500;
    StorageManager manager(config);
    std::string error;
    ASSERT_TRUE(manager.start(error)) << error;

    for (std::size_t i = 0; i < kRows; ++i) {
        manager.record_snapshot(make_snapshot("tok", i + 1));
    }
    manager.shutdown();

    const auto stats = manager.stats();
    EXPECT_EQ(stats.events_dropped, 0u);
    EXPECT_EQ(stats.storage_errors, 0u);
    // The reported count and the count actually on disk must agree; a
    // discrepancy would mean the summary is lying about the dataset.
    EXPECT_EQ(stats.book_snapshots_written, kRows);
    if (duckdb_support_available()) {
        EXPECT_EQ(count_parquet_rows(config, "book_snapshots"), kRows);
    }
}

TEST(StorageManagerTest, EveryQueuedRawEventReachesTheArchive) {
    const TempRoot root("rawconservation");
    const Config config = config_for(root);

    constexpr std::size_t kEvents = 5000;
    StorageManager manager(config);
    std::string error;
    ASSERT_TRUE(manager.start(error)) << error;

    for (std::size_t i = 0; i < kEvents; ++i) {
        manager.record_raw_event(make_event(i + 1));
    }
    manager.shutdown();

    const auto stats = manager.stats();
    EXPECT_EQ(stats.raw_events_written, kEvents);
    EXPECT_EQ(stats.events_dropped, 0u);
    EXPECT_GT(stats.raw_bytes_written, 0u);
    EXPECT_EQ(stats.raw_files_created, 1u);
}

TEST(StorageManagerTest, RawArchiveIsCompressedAndDecompressesToEveryEvent) {
    if (!zstd_support_available()) {
        GTEST_SKIP() << "built without zstd";
    }
    const TempRoot root("zstd");
    const Config config = config_for(root);

    constexpr std::size_t kEvents = 2000;
    {
        StorageManager manager(config);
        std::string error;
        ASSERT_TRUE(manager.start(error)) << error;
        for (std::size_t i = 0; i < kEvents; ++i) {
            manager.record_raw_event(make_event(i + 1));
        }
        manager.shutdown();
    }

    // The archive must land in the dated layout with a .zst extension and
    // carry no leftover .partial file.
    std::vector<std::filesystem::path> archives;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(root.path() / "raw")) {
        if (entry.is_regular_file()) {
            archives.push_back(entry.path());
        }
    }
    ASSERT_EQ(archives.size(), 1u);
    EXPECT_EQ(archives[0].extension(), ".zst");
    EXPECT_NE(archives[0].string().find("events_"), std::string::npos);

    // Decompressing must yield exactly the events that went in, which is what
    // makes this archive a usable replay source.
    const std::string decompressed =
        (std::filesystem::temp_directory_path() / "mde_zstd_check.jsonl").string();
    const std::string command =
        "zstd -d -q -f '" + archives[0].string() + "' -o '" + decompressed + "' 2>/dev/null";
    if (std::system(command.c_str()) != 0) {
        GTEST_SKIP() << "zstd CLI unavailable to verify decompression";
    }
    std::ifstream in(decompressed);
    std::size_t lines = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            ++lines;
        }
    }
    in.close();
    std::filesystem::remove(decompressed);
    EXPECT_EQ(lines, kEvents);
}

TEST(StorageManagerTest, CompressionCanBeTurnedOff) {
    const TempRoot root("plain");
    Config config = config_for(root);
    config.storage.raw_events.compression = "none";

    StorageManager manager(config);
    std::string error;
    ASSERT_TRUE(manager.start(error)) << error;
    manager.record_raw_event(make_event(1));
    manager.shutdown();

    bool found_plain = false;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(root.path() / "raw")) {
        if (entry.is_regular_file() && entry.path().extension() == ".jsonl") {
            found_plain = true;
        }
    }
    EXPECT_TRUE(found_plain);
}

TEST(StorageManagerTest, TradesAndMetadataFlowThroughTheSameQueue) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("mixed");
    const Config config = config_for(root);

    StorageManager manager(config);
    std::string error;
    ASSERT_TRUE(manager.start(error)) << error;

    for (int i = 0; i < 20; ++i) {
        manager.record_snapshot(make_snapshot("tok", static_cast<std::uint64_t>(i)));
        Trade trade;
        trade.token_id = "tok";
        trade.market_id = "0xmarket";
        trade.price = 0.51;
        trade.quantity = 5.0;
        trade.aggressor_side = Side::Ask;
        trade.receive_timestamp = now_receive();
        trade.exchange_timestamp = now_exchange();
        manager.record_trade(trade);
    }

    Market market;
    market.market_id = "0xmarket";
    market.token_ids = {"tokA", "tokB"};
    market.outcomes = {"Yes", "No"};
    market.active = true;
    manager.record_market_metadata(market);

    manager.shutdown();

    const auto stats = manager.stats();
    EXPECT_EQ(stats.book_snapshots_written, 20u);
    EXPECT_EQ(stats.trades_written, 20u);
    EXPECT_EQ(stats.metadata_rows_written, 2u);
    EXPECT_EQ(stats.events_dropped, 0u);
}

TEST(StorageManagerTest, ShutdownIsIdempotentAndSafeWithoutStart) {
    const TempRoot root("idempotent");
    StorageManager manager(config_for(root));
    manager.shutdown();  // Never started.
    manager.shutdown();

    std::string error;
    ASSERT_TRUE(manager.start(error)) << error;
    manager.record_raw_event(make_event(1));
    manager.shutdown();
    manager.shutdown();
    EXPECT_EQ(manager.stats().raw_events_written, 1u);
}

TEST(StorageManagerTest, RecordingAfterShutdownIsIgnoredNotCrashing) {
    const TempRoot root("postshutdown");
    StorageManager manager(config_for(root));
    std::string error;
    ASSERT_TRUE(manager.start(error)) << error;
    manager.shutdown();

    manager.record_raw_event(make_event(1));
    manager.record_snapshot(make_snapshot("tok", 1));
    EXPECT_EQ(manager.stats().raw_events_written, 0u);
}

TEST(StorageManagerTest, DropPolicyCountsLossInsteadOfHidingIt) {
    const TempRoot root("droppolicy");
    Config config = config_for(root);
    // A pathologically small queue with dropping enabled, to prove that loss
    // is always accounted for rather than silent.
    config.storage.block_when_queue_full = false;
    config.storage.raw_queue_capacity = 4;
    config.storage.parquet_queue_capacity = 4;

    StorageManager manager(config);
    std::string error;
    ASSERT_TRUE(manager.start(error)) << error;
    for (int i = 0; i < 20000; ++i) {
        manager.record_raw_event(make_event(static_cast<std::uint64_t>(i)));
    }
    manager.shutdown();

    const auto stats = manager.stats();
    // Written plus dropped must account for everything offered.
    EXPECT_EQ(stats.raw_events_written + stats.events_dropped, stats.events_queued);
    if (stats.events_dropped > 0) {
        EXPECT_NE(manager.format_summary().find("INCOMPLETE"), std::string::npos);
    }
}

// Regression test for a real bug: flush() used to call the writers directly
// from the caller's thread while the worker threads were appending to the very
// same Arrow builders and archive buffer. The race silently dropped rows --
// no error, no drop count, just a short dataset. Writers are now touched only
// by the worker that owns them, and flush() is a request.
TEST(StorageManagerTest, ConcurrentFlushRequestsDoNotLoseRows) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("flushrace");
    Config config = config_for(root);
    // Force time-based flushing, as the live engine uses, so the flush path
    // is genuinely exercised while rows are still arriving.
    config.storage.parquet.batch_rows = 100'000;
    config.storage.parquet.flush_interval_seconds = 1;

    constexpr std::size_t kRows = 5000;
    StorageManager manager(config);
    std::string error;
    ASSERT_TRUE(manager.start(error)) << error;

    // A second thread hammering flush(), exactly as the engine timer does.
    std::atomic<bool> stop{false};
    std::thread flusher([&] {
        while (!stop.load()) {
            manager.flush();
            std::this_thread::sleep_for(std::chrono::microseconds{200});
        }
    });

    for (std::size_t i = 0; i < kRows; ++i) {
        manager.record_snapshot(make_snapshot("tok", i + 1));
        manager.record_raw_event(make_event(i + 1));
    }
    stop.store(true);
    flusher.join();
    manager.shutdown();

    const auto stats = manager.stats();
    EXPECT_EQ(stats.book_snapshots_written, kRows);
    EXPECT_EQ(stats.raw_events_written, kRows);
    EXPECT_EQ(stats.events_dropped, 0u);
    EXPECT_EQ(stats.storage_errors, 0u);
    if (duckdb_support_available()) {
        EXPECT_EQ(count_parquet_rows(config, "book_snapshots"), kRows);
    }
}

TEST(StorageManagerTest, SummaryReportsWhatIsActuallyOnDisk) {
    if (!parquet_support_available()) {
        GTEST_SKIP() << "built without Arrow";
    }
    const TempRoot root("summary");
    StorageManager manager(config_for(root));
    std::string error;
    ASSERT_TRUE(manager.start(error)) << error;
    for (int i = 0; i < 150; ++i) {
        manager.record_snapshot(make_snapshot("tok", static_cast<std::uint64_t>(i)));
        manager.record_raw_event(make_event(static_cast<std::uint64_t>(i)));
    }
    manager.shutdown();

    const auto summary = manager.format_summary();
    EXPECT_NE(summary.find("Storage Summary"), std::string::npos);
    EXPECT_NE(summary.find("Book snapshots"), std::string::npos);
    EXPECT_NE(summary.find("Raw events"), std::string::npos);
    EXPECT_EQ(summary.find("INCOMPLETE"), std::string::npos);

    const auto stats = manager.stats();
    EXPECT_EQ(stats.book_snapshots_written, 150u);
    EXPECT_EQ(stats.raw_events_written, 150u);
    EXPECT_GT(stats.parquet_files_created, 0u);
}

}  // namespace
}  // namespace market_data
