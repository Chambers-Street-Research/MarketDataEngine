// The storage façade.
//
// The engine calls record_*() and nothing else; file layout, compression,
// batching, partitioning and the DuckDB catalog all live behind here.
//
// Threading
//
//   engine consumer thread ---> raw queue ------> raw worker  ---> JSONL.zst
//                          \
//                           -> parquet queue --> parquet worker -> Parquet
//   engine timer thread ------^
//
// Two queues and two workers, deliberately: the raw archive is the audit trail
// and the recovery path for everything else, so a slow Parquet flush must not
// be able to stall it. Both queues are bounded; neither producer is the
// network thread, so backpressure reaches ingestion only through the engine's
// own event queue.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "market_data/config.hpp"
#include "market_data/event_bus.hpp"
#include "market_data/market.hpp"
#include "market_data/market_event.hpp"
#include "market_data/storage/duckdb_catalog.hpp"
#include "market_data/storage/parquet_writer.hpp"
#include "market_data/storage/raw_archive.hpp"
#include "market_data/storage/storage_writer.hpp"

namespace market_data {

struct StorageStats {
    std::uint64_t raw_events_written = 0;
    std::uint64_t raw_bytes_written = 0;
    std::uint64_t raw_uncompressed_bytes = 0;
    std::uint64_t raw_files_created = 0;

    std::uint64_t book_snapshots_written = 0;
    std::uint64_t trades_written = 0;
    std::uint64_t metadata_rows_written = 0;

    std::uint64_t parquet_files_created = 0;
    std::uint64_t parquet_bytes_written = 0;
    std::uint64_t parquet_flush_count = 0;
    std::uint64_t parquet_flush_latency_ms_total = 0;
    std::uint64_t parquet_max_flush_latency_ms = 0;

    std::uint64_t raw_queue_depth = 0;
    std::uint64_t raw_queue_high_water = 0;
    std::uint64_t parquet_queue_depth = 0;
    std::uint64_t parquet_queue_high_water = 0;

    std::uint64_t events_queued = 0;
    std::uint64_t events_dropped = 0;
    std::uint64_t storage_errors = 0;

    [[nodiscard]] double raw_compression_ratio() const {
        return raw_bytes_written == 0 ? 0.0
                                      : static_cast<double>(raw_uncompressed_bytes) /
                                            static_cast<double>(raw_bytes_written);
    }
};

class StorageManager {
public:
    explicit StorageManager(Config config);
    ~StorageManager();

    StorageManager(const StorageManager&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;

    // Opens every layer and starts the worker threads. A failure of an
    // optional layer (Parquet, DuckDB) is logged and tolerated; a failure of
    // the raw archive is fatal, because it is the recovery path.
    bool start(std::string& error);

    // All four are non-blocking enqueues under the drop policy, and may block
    // briefly under the (default) no-drop policy.
    void record_raw_event(const MarketEvent& event);
    void record_snapshot(const MarketSnapshot& snapshot);
    void record_trade(const Trade& trade);
    void record_market_metadata(const Market& market);

    // Asks the workers to flush at their next opportunity and returns.
    //
    // Deliberately does *not* touch the writers: each writer is owned by
    // exactly one worker thread, and reaching into one from the caller's
    // thread would race with that worker mid-append. Flushing is therefore a
    // request, not an action.
    void flush();

    // Ordered teardown: close queues, drain, flush, finalize Parquet, refresh
    // and checkpoint DuckDB. Idempotent.
    void shutdown();

    [[nodiscard]] StorageStats stats() const;
    [[nodiscard]] std::string format_summary() const;

    // Where each layer is writing, for the startup log.
    [[nodiscard]] std::string describe() const;

    [[nodiscard]] const DuckDbCatalog& catalog() const { return *catalog_; }

private:
    // One variant per Parquet dataset, so a single queue and worker cover all
    // three without three sets of plumbing.
    using ParquetItem = std::variant<SnapshotRow, TradeRow, MarketRow>;

    void raw_worker();
    void parquet_worker();
    void warn_if_queue_deep(const char* queue_name, std::size_t depth, std::size_t capacity);

    Config config_;

    RawEventArchive raw_archive_;
    std::unique_ptr<StorageWriter> parquet_writer_;
    std::unique_ptr<DuckDbCatalog> catalog_;

    BoundedQueue<MarketEvent> raw_queue_;
    BoundedQueue<ParquetItem> parquet_queue_;

    std::thread raw_thread_;
    std::thread parquet_thread_;

    std::atomic<bool> running_{false};
    std::atomic<std::uint64_t> events_queued_{0};
    std::atomic<std::uint64_t> events_dropped_{0};
    std::atomic<bool> raw_queue_warned_{false};
    std::atomic<bool> parquet_queue_warned_{false};
    // Set by flush(), consumed by the worker that owns the writer.
    std::atomic<bool> raw_flush_requested_{false};
    std::atomic<bool> parquet_flush_requested_{false};
};

}  // namespace market_data
