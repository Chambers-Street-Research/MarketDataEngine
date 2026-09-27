// The normalized-storage boundary.
//
// The engine hands rows to a StorageWriter and knows nothing about Parquet,
// Arrow, partition layout or file naming. That keeps the columnar dependency
// confined to one translation unit and makes a different backend (or a null
// backend, in tests and in builds without Arrow) a constructor-argument
// change rather than a refactor.
#pragma once

#include <cstdint>
#include <string>

#include "market_data/market.hpp"
#include "market_data/types.hpp"

namespace market_data {

// A row plus the local wall-clock instant it was captured.
//
// Partitioning needs a wall clock, and none of the row payloads reliably has
// one: a snapshot's `timestamp` is a steady clock with an arbitrary epoch, and
// `exchange_timestamp` is absent on a book that has not received data yet.
// Stamping capture time where the row is produced also means a replay writes
// rows into partitions derived from the replay, not from today.
template <typename Payload>
struct CapturedRow {
    Payload payload;
    ExchangeTime captured_at{};
};

using SnapshotRow = CapturedRow<MarketSnapshot>;
using TradeRow = CapturedRow<Trade>;
using MarketRow = CapturedRow<Market>;

struct StorageWriterStats {
    std::uint64_t book_snapshots_written = 0;
    std::uint64_t trades_written = 0;
    std::uint64_t metadata_rows_written = 0;

    std::uint64_t files_created = 0;
    std::uint64_t bytes_written = 0;

    std::uint64_t flush_count = 0;
    std::uint64_t flush_latency_ms_total = 0;
    std::uint64_t max_flush_latency_ms = 0;

    std::uint64_t errors = 0;

    [[nodiscard]] std::uint64_t rows_written() const {
        return book_snapshots_written + trades_written + metadata_rows_written;
    }
};

class StorageWriter {
public:
    virtual ~StorageWriter() = default;

    StorageWriter() = default;
    StorageWriter(const StorageWriter&) = delete;
    StorageWriter& operator=(const StorageWriter&) = delete;

    // Prepares output directories and any open handles. Returns false and
    // fills `error` if storage cannot be initialized.
    virtual bool start(std::string& error) = 0;

    virtual void write_book_snapshot(const SnapshotRow& row) = 0;
    virtual void write_trade(const TradeRow& row) = 0;
    virtual void write_market_metadata(const MarketRow& row) = 0;

    // Writes out whatever is buffered. Cheap to call when nothing is pending.
    virtual void flush() = 0;

    // Flushes, finalizes every open file and releases handles. Idempotent.
    virtual void close() = 0;

    [[nodiscard]] virtual StorageWriterStats stats() const = 0;

    // Human-readable description of where rows are going, for the startup log.
    [[nodiscard]] virtual std::string describe() const = 0;
};

// Accepts and counts rows without writing anything. Used when Parquet is
// disabled in config, and in builds compiled without Arrow, so the engine's
// call sites never need a null check.
class NullStorageWriter final : public StorageWriter {
public:
    bool start(std::string&) override { return true; }
    void write_book_snapshot(const SnapshotRow&) override { ++stats_.book_snapshots_written; }
    void write_trade(const TradeRow&) override { ++stats_.trades_written; }
    void write_market_metadata(const MarketRow&) override { ++stats_.metadata_rows_written; }
    void flush() override {}
    void close() override {}
    [[nodiscard]] StorageWriterStats stats() const override { return stats_; }
    [[nodiscard]] std::string describe() const override { return "disabled"; }

private:
    StorageWriterStats stats_;
};

}  // namespace market_data
