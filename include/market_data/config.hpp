// Engine configuration, loaded from JSON.
//
// Endpoints are configurable rather than hardcoded so the engine can be pointed
// at a staging venue or a local mock without a rebuild. Nothing here is a
// credential: this engine reads public market data only.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "market_data/logging.hpp"

namespace market_data {

struct PolymarketEndpoints {
    std::string gamma_base_url = "https://gamma-api.polymarket.com";
    std::string clob_base_url = "https://clob.polymarket.com";
    std::string market_ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
};

struct SubscribeConfig {
    // Number of markets, most-traded first, whose tokens will be subscribed.
    // A binary market contributes two tokens.
    std::size_t top_markets_by_volume = 25;
    // Hard ceiling on tokens in one subscription, to stay within what the
    // venue will accept in a single frame. Zero means no cap.
    std::size_t max_tokens = 400;
    // Ignore markets thinner than this when ranking. Zero disables the filter.
    double min_reported_liquidity = 0.0;
};

struct ReconnectConfig {
    std::uint32_t initial_backoff_ms = 500;
    std::uint32_t max_backoff_ms = 30'000;
    double backoff_multiplier = 2.0;
    // Random fraction of the delay added on top, so a fleet of engines does
    // not reconnect in lockstep after a venue-side outage.
    double jitter_fraction = 0.25;
    // Zero means retry forever, which is what a long-running collector wants.
    std::uint32_t max_attempts = 0;
};

struct HealthConfig {
    // A book with no message for this long is marked Stale. Stale is
    // informational, not a fault: a thin prediction market can legitimately go
    // minutes without a quote change, and the book stays usable.
    std::uint32_t stale_after_ms = 60'000;
    // Interval of the venue's application-level heartbeat.
    std::uint32_t ping_interval_ms = 10'000;
    // No frame at all for this long means the socket is dead even if TCP
    // has not noticed yet.
    std::uint32_t read_timeout_ms = 40'000;
    // Wait this long before re-requesting a snapshot for an invalid book, so a
    // persistently crossed feed cannot turn into a REST hot loop.
    std::uint32_t resync_cooldown_ms = 2'000;
};

// Raw append-only archive. Highest fidelity, used for replay, debugging,
// auditability and recovering from bugs in the normalized datasets. It is
// never superseded by Parquet or DuckDB.
struct RawEventConfig {
    bool enabled = true;
    // "jsonl" only for now; the field exists so a future format is a config
    // change rather than a flag.
    std::string format = "jsonl";
    // "zstd" or "none". Falls back to "none" with a warning when the build
    // has no zstd support.
    std::string compression = "zstd";
    int compression_level = 3;
    // Roll the raw log this often. Hourly keeps a crashed file's blast radius
    // to one hour and makes date/hour partitions line up with Parquet.
    std::uint32_t rotation_minutes = 60;
    // Also roll on size. Zero disables size-based rotation.
    std::uint64_t max_file_bytes = 512ULL * 1024 * 1024;
    // Flush at least this often so a hard kill loses bounded data.
    std::uint32_t flush_interval_ms = 1'000;
};

// Normalized columnar datasets for research and backtesting.
struct ParquetConfig {
    bool enabled = true;
    // "zstd", "snappy", "gzip" or "none".
    std::string compression = "zstd";
    int compression_level = 3;
    // One normalized book snapshot per token per interval. The raw archive
    // still records every update; this is the deliberately lower-frequency
    // research view.
    std::uint32_t snapshot_interval_ms = 1'000;
    // Flush a row group when either threshold trips, or on shutdown.
    std::size_t batch_rows = 10'000;
    std::uint32_t flush_interval_seconds = 10;
    // Hive partitioning: date=YYYY-MM-DD/hour=HH. With this false, only the
    // date level is used.
    bool partition_by_hour = true;
};

struct DuckDbConfig {
    bool enabled = true;
    // Relative paths are resolved against storage.root_directory.
    std::string path = "db/market_data.duckdb";
    // Recreate the views at startup so a schema change lands without manual
    // intervention. Views are cheap; the data lives in Parquet.
    bool create_views = true;
};

struct StorageConfig {
    // All datasets live under here: raw/, books/, trades/, markets/, db/.
    std::string root_directory = "./data";

    RawEventConfig raw_events;
    ParquetConfig parquet;
    DuckDbConfig duckdb;

    // Legacy single-file CSV snapshot sink. Superseded by Parquet and off by
    // default; kept because it needs no Arrow and is handy for quick eyeballs.
    bool write_csv_snapshots = false;

    // Capacity of the queues between the engine and the storage workers.
    std::size_t raw_queue_capacity = 262'144;
    std::size_t parquet_queue_capacity = 262'144;
    // Blocking preserves every row at the cost of backpressure; see
    // docs/architecture.md. Dropping is counted and marks the interval
    // incomplete.
    bool block_when_queue_full = true;
    // Log a critical warning once the queue passes this fraction of capacity.
    double queue_warn_fraction = 0.8;
};

struct Config {
    std::string exchange = "polymarket";
    // The dataset root lives in storage.root_directory. There is deliberately
    // no second copy here: two fields naming the same directory is a bug
    // waiting to happen. The legacy top-level "output_directory" JSON key is
    // still accepted on load and folded into storage.root_directory.

    std::uint32_t snapshot_interval_ms = 1'000;
    std::size_t order_book_depth = 20;
    // Refresh market metadata (and persist a metadata snapshot) this often.
    std::uint32_t market_refresh_interval_ms = 300'000;
    std::uint32_t status_interval_ms = 5'000;

    // Capacity of the queue between the network thread and the consumer.
    std::size_t event_queue_capacity = 262'144;
    // When true, a full queue blocks the network thread rather than dropping
    // events. Preserving data is the default; see docs/architecture.md.
    bool block_when_queue_full = true;

    LogLevel log_level = LogLevel::Info;

    PolymarketEndpoints polymarket;
    SubscribeConfig subscribe;
    ReconnectConfig reconnect;
    HealthConfig health;
    StorageConfig storage;

    // Loads and validates `path`. On failure returns false and fills `error`.
    static bool load_from_file(const std::string& path, Config& out, std::string& error);
    static bool load_from_json(const std::string& text, Config& out, std::string& error);

    // Returns false and fills `error` if any field is out of range.
    [[nodiscard]] bool validate(std::string& error) const;

    [[nodiscard]] std::string raw_directory() const;
    [[nodiscard]] std::string normalized_directory() const;
    [[nodiscard]] std::string metadata_directory() const;
    [[nodiscard]] std::string books_directory() const;
    [[nodiscard]] std::string trades_directory() const;
    [[nodiscard]] std::string markets_directory() const;
    [[nodiscard]] std::string duckdb_path() const;
};

}  // namespace market_data
