// DuckDB analytical catalog over the Parquet datasets.
//
// DuckDB is deliberately a *query* layer, not an ingestion path. The collector
// writes buffered Parquet; DuckDB holds views over those files. Nothing is
// copied into DuckDB tables, so the database file stays tiny, the datasets
// remain portable (and readable without DuckDB at all), and a crashed
// collector cannot corrupt the query layer.
//
// Views are (re)created from whatever Parquet exists at the time. DuckDB
// resolves `read_parquet` globs eagerly, so a view over an empty dataset
// cannot be created; those are skipped and reported rather than treated as a
// failure, which is the normal state on a first run before the first flush.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "market_data/config.hpp"

namespace market_data {

// True when the build has DuckDB support compiled in.
bool duckdb_support_available();

struct CatalogStatus {
    bool opened = false;
    std::vector<std::string> views_created;
    std::vector<std::string> views_skipped;   // Dataset empty at refresh time.
    std::vector<std::string> errors;
    std::string database_path;

    [[nodiscard]] bool ok() const { return opened && errors.empty(); }
};

// The result of a query, as strings. Enough for a CLI and for assertions;
// research goes through Python or the DuckDB CLI, not through this.
struct QueryResult {
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> rows;
    std::string error;

    [[nodiscard]] bool ok() const { return error.empty(); }
    [[nodiscard]] std::size_t row_count() const { return rows.size(); }
    // First column of the first row, for scalar queries like COUNT(*).
    [[nodiscard]] std::string scalar() const {
        return rows.empty() || rows.front().empty() ? std::string{} : rows.front().front();
    }
};

class DuckDbCatalog {
public:
    explicit DuckDbCatalog(Config config);
    ~DuckDbCatalog();

    DuckDbCatalog(const DuckDbCatalog&) = delete;
    DuckDbCatalog& operator=(const DuckDbCatalog&) = delete;

    // Opens (creating if needed) the database file. Returns false on failure;
    // the engine treats that as non-fatal and keeps collecting.
    bool open(std::string& error);

    // Drops and recreates every view against the Parquet on disk. Safe to call
    // repeatedly; called at startup and again at shutdown once the final
    // partitions have been published.
    CatalogStatus refresh_views();

    [[nodiscard]] QueryResult query(const std::string& sql);

    // CHECKPOINT then close, so the database file is consistent on disk.
    void close();

    [[nodiscard]] bool is_open() const;
    [[nodiscard]] const CatalogStatus& status() const { return status_; }

    // SQL of the views this catalog defines, exposed for documentation and
    // for tests that assert the catalog and the docs agree.
    static std::vector<std::pair<std::string, std::string>> view_definitions(
        const std::string& books_glob,
        const std::string& trades_glob,
        const std::string& markets_glob);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    Config config_;
    CatalogStatus status_;
};

}  // namespace market_data
