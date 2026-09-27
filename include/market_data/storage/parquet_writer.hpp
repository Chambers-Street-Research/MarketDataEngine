// Parquet implementation of StorageWriter.
//
// Layout (Hive-partitioned, so DuckDB's hive_partitioning=true exposes the
// partition keys as ordinary columns):
//
//   <root>/books/date=YYYY-MM-DD/hour=HH/books.parquet
//   <root>/trades/date=YYYY-MM-DD/hour=HH/trades.parquet
//   <root>/markets/date=YYYY-MM-DD/markets.parquet
//
// One file per partition, not one per flush: each flush appends a row group
// to the currently open file. That keeps file counts proportional to elapsed
// hours rather than to message volume, which is what makes the dataset fast
// to scan later.
//
// File safety: a partition is written as `<name>.parquet.tmp` and atomically
// renamed once its footer is on disk. A crash therefore leaves a `.tmp` that
// is obviously incomplete and can be ignored -- the raw JSONL archive is the
// recovery path for that interval.
#pragma once

#include <memory>
#include <string>

#include "market_data/config.hpp"
#include "market_data/storage/storage_writer.hpp"

namespace market_data {

// True when the build has Arrow/Parquet support compiled in.
bool parquet_support_available();

class ParquetStorageWriter final : public StorageWriter {
public:
    explicit ParquetStorageWriter(Config config);
    ~ParquetStorageWriter() override;

    bool start(std::string& error) override;

    void write_book_snapshot(const SnapshotRow& row) override;
    void write_trade(const TradeRow& row) override;
    void write_market_metadata(const MarketRow& row) override;

    void flush() override;
    void close() override;

    [[nodiscard]] StorageWriterStats stats() const override;
    [[nodiscard]] std::string describe() const override;

    // Column names of each dataset, exposed so tests and the DuckDB catalog
    // can assert on the schema without linking Arrow.
    static std::vector<std::string> book_columns();
    static std::vector<std::string> trade_columns();
    static std::vector<std::string> market_columns();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Chooses a Parquet writer when it is both enabled and available, and a null
// writer otherwise. Never returns nullptr, so call sites stay branch-free.
std::unique_ptr<StorageWriter> make_storage_writer(const Config& config);

}  // namespace market_data
