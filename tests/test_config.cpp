#include <gtest/gtest.h>

#include "market_data/config.hpp"

namespace market_data {
namespace {

TEST(ConfigTest, DefaultsAreValid) {
    const Config config;
    std::string error;
    EXPECT_TRUE(config.validate(error)) << error;
}

TEST(ConfigTest, ParsesTheShippedExampleShape) {
    const std::string text = R"({
      "exchange": "polymarket",
      "output_directory": "/tmp/mde",
      "snapshot_interval_ms": 500,
      "order_book_depth": 10,
      "log_level": "debug",
      "endpoints": {"gamma_base_url": "https://example.test"},
      "subscribe": {"top_markets_by_volume": 5, "max_tokens": 40},
      "reconnect": {"initial_backoff_ms": 100, "max_backoff_ms": 1000},
      "health": {"ping_interval_ms": 5000, "read_timeout_ms": 20000},
      "storage": {"raw_events": {"enabled": false, "max_file_bytes": 1024},
                  "parquet": {"batch_rows": 500, "partition_by_hour": false},
                  "duckdb": {"enabled": false}}
    })";

    Config config;
    std::string error;
    ASSERT_TRUE(Config::load_from_json(text, config, error)) << error;

    EXPECT_EQ(config.storage.root_directory, "/tmp/mde");
    EXPECT_EQ(config.snapshot_interval_ms, 500u);
    EXPECT_EQ(config.order_book_depth, 10u);
    EXPECT_EQ(config.log_level, LogLevel::Debug);
    EXPECT_EQ(config.polymarket.gamma_base_url, "https://example.test");
    // Unspecified endpoints keep their defaults rather than becoming empty.
    EXPECT_FALSE(config.polymarket.clob_base_url.empty());
    EXPECT_EQ(config.subscribe.top_markets_by_volume, 5u);
    EXPECT_EQ(config.reconnect.max_backoff_ms, 1000u);
    EXPECT_FALSE(config.storage.raw_events.enabled);
    EXPECT_EQ(config.storage.raw_events.max_file_bytes, 1024u);
    EXPECT_EQ(config.storage.parquet.batch_rows, 500u);
    EXPECT_FALSE(config.storage.parquet.partition_by_hour);
    EXPECT_FALSE(config.storage.duckdb.enabled);
    // Unspecified sub-fields keep their defaults rather than resetting.
    EXPECT_EQ(config.storage.raw_events.compression, "zstd");
    EXPECT_EQ(config.storage.parquet.compression, "zstd");
}

TEST(ConfigTest, DerivedDirectoriesHangOffTheStorageRoot) {
    Config config;
    config.storage.root_directory = "/tmp/mde";
    EXPECT_EQ(config.raw_directory(), "/tmp/mde/raw");
    EXPECT_EQ(config.normalized_directory(), "/tmp/mde/normalized");
    EXPECT_EQ(config.metadata_directory(), "/tmp/mde/metadata");
    EXPECT_EQ(config.books_directory(), "/tmp/mde/books");
    EXPECT_EQ(config.trades_directory(), "/tmp/mde/trades");
    EXPECT_EQ(config.markets_directory(), "/tmp/mde/markets");
    // A relative duckdb path is anchored to the storage root, so the whole
    // dataset stays movable as one directory.
    EXPECT_EQ(config.duckdb_path(), "/tmp/mde/db/market_data.duckdb");
    config.storage.duckdb.path = "/var/db/other.duckdb";
    EXPECT_EQ(config.duckdb_path(), "/var/db/other.duckdb");
}

TEST(ConfigTest, StorageRootInheritsOutputDirectoryWhenNotSet) {
    // Old configs that only know about output_directory keep working.
    Config config;
    std::string error;
    ASSERT_TRUE(Config::load_from_json(R"({"output_directory":"/tmp/legacy"})", config, error));
    EXPECT_EQ(config.storage.root_directory, "/tmp/legacy");
    EXPECT_EQ(config.raw_directory(), "/tmp/legacy/raw");
}

TEST(ConfigTest, ExplicitStorageRootWinsOverOutputDirectory) {
    Config config;
    std::string error;
    ASSERT_TRUE(Config::load_from_json(
        R"({"output_directory":"/tmp/legacy","storage":{"root_directory":"/tmp/explicit"}})",
        config, error));
    EXPECT_EQ(config.storage.root_directory, "/tmp/explicit");

}

TEST(ConfigTest, ParquetSnapshotIntervalAndTimerCadenceStayInSync) {
    Config config;
    std::string error;
    // Two places name this interval; they must never silently disagree.
    ASSERT_TRUE(Config::load_from_json(
        R"({"snapshot_interval_ms":250,"storage":{"parquet":{"snapshot_interval_ms":750}}})",
        config, error));
    EXPECT_EQ(config.storage.parquet.snapshot_interval_ms, 750u);
    EXPECT_EQ(config.snapshot_interval_ms, 750u);

    ASSERT_TRUE(Config::load_from_json(R"({"snapshot_interval_ms":250})", config, error));
    EXPECT_EQ(config.storage.parquet.snapshot_interval_ms, 250u);
}

TEST(ConfigTest, RejectsOutOfRangeStorageValues) {
    struct Case {
        const char* json;
        const char* expected_substring;
    };
    const Case cases[] = {
        {R"({"storage":{"raw_events":{"format":"avro"}}})", "format"},
        {R"({"storage":{"raw_events":{"compression":"lz4"}}})", "compression"},
        {R"({"storage":{"raw_events":{"rotation_minutes":0}}})", "rotation_minutes"},
        {R"({"storage":{"parquet":{"compression":"brotli"}}})", "compression"},
        {R"({"storage":{"parquet":{"batch_rows":0}}})", "batch_rows"},
        {R"({"storage":{"parquet":{"flush_interval_seconds":0}}})", "flush_interval_seconds"},
        {R"({"storage":{"parquet":{"snapshot_interval_ms":0}}})", "snapshot_interval_ms"},
        {R"({"storage":{"duckdb":{"enabled":true,"path":""}}})", "duckdb.path"},
        {R"({"storage":{"raw_queue_capacity":0}})", "queue capacities"},
        {R"({"storage":{"queue_warn_fraction":1.5}})", "queue_warn_fraction"},
    };
    for (const auto& c : cases) {
        Config config;
        std::string error;
        EXPECT_FALSE(Config::load_from_json(c.json, config, error)) << c.json;
        EXPECT_NE(error.find(c.expected_substring), std::string::npos)
            << c.json << " -> " << error;
    }
}

TEST(ConfigTest, RejectsMalformedJson) {
    Config config;
    std::string error;
    EXPECT_FALSE(Config::load_from_json("{ nope", config, error));
    EXPECT_FALSE(error.empty());
    EXPECT_FALSE(Config::load_from_json("[1,2]", config, error));
}

TEST(ConfigTest, RejectsUnknownLogLevel) {
    Config config;
    std::string error;
    EXPECT_FALSE(Config::load_from_json(R"({"log_level":"shout"})", config, error));
    EXPECT_NE(error.find("shout"), std::string::npos);
}

TEST(ConfigTest, RejectsOutOfRangeValues) {
    struct Case {
        const char* json;
        const char* expected_substring;
    };
    const Case cases[] = {
        {R"({"exchange":"binance"})", "exchange"},
        {R"({"storage":{"root_directory":""}})", "root_directory"},
        {R"({"snapshot_interval_ms":0})", "snapshot_interval_ms"},
        {R"({"order_book_depth":0})", "order_book_depth"},
        {R"({"event_queue_capacity":0})", "event_queue_capacity"},
        {R"({"subscribe":{"top_markets_by_volume":0}})", "top_markets_by_volume"},
        {R"({"reconnect":{"initial_backoff_ms":0}})", "initial_backoff_ms"},
        {R"({"reconnect":{"initial_backoff_ms":5000,"max_backoff_ms":100}})", "max_backoff_ms"},
        {R"({"reconnect":{"backoff_multiplier":0.5}})", "backoff_multiplier"},
        {R"({"reconnect":{"jitter_fraction":2.0}})", "jitter_fraction"},
    };
    for (const auto& c : cases) {
        Config config;
        std::string error;
        EXPECT_FALSE(Config::load_from_json(c.json, config, error)) << c.json;
        EXPECT_NE(error.find(c.expected_substring), std::string::npos)
            << c.json << " -> " << error;
    }
}

TEST(ConfigTest, ReadTimeoutMustExceedTheHeartbeatInterval) {
    // Otherwise a healthy but quiet connection is torn down every cycle.
    Config config;
    std::string error;
    EXPECT_FALSE(Config::load_from_json(
        R"({"health":{"ping_interval_ms":10000,"read_timeout_ms":10000}})", config, error));
    EXPECT_NE(error.find("read_timeout_ms"), std::string::npos);
}

TEST(ConfigTest, MissingFileIsReportedNotThrown) {
    Config config;
    std::string error;
    EXPECT_FALSE(Config::load_from_file("/nonexistent/path/config.json", config, error));
    EXPECT_NE(error.find("cannot open"), std::string::npos);
}

}  // namespace
}  // namespace market_data
