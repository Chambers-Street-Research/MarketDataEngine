#include "market_data/storage/duckdb_catalog.hpp"

#include <filesystem>
#include <system_error>

#include "market_data/logging.hpp"

namespace market_data {
namespace {

// Base views, one per dataset. Everything else is derived from these, so a
// schema change lands in one place.
std::string base_view(const std::string& name, const std::string& glob) {
    return "CREATE OR REPLACE VIEW " + name +
           " AS SELECT * FROM read_parquet('" + glob + "', hive_partitioning = true)";
}

}  // namespace

std::vector<std::pair<std::string, std::string>> DuckDbCatalog::view_definitions(
    const std::string& books_glob,
    const std::string& trades_glob,
    const std::string& markets_glob) {
    std::vector<std::pair<std::string, std::string>> views;

    views.emplace_back("book_snapshots", base_view("book_snapshots", books_glob));
    views.emplace_back("trades", base_view("trades", trades_glob));
    views.emplace_back("markets", base_view("markets", markets_glob));

    // Quoted-spread view: rows where both sides actually exist, so a
    // researcher never has to remember the NULL filter.
    views.emplace_back("spreads", R"(CREATE OR REPLACE VIEW spreads AS
SELECT
    receive_timestamp_ns,
    capture_timestamp_ns,
    date,
    market_id,
    token_id,
    outcome,
    best_bid,
    best_ask,
    midpoint,
    spread
FROM book_snapshots
WHERE best_bid IS NOT NULL
  AND best_ask IS NOT NULL)");

    // The microstructure feature set, restricted to books the engine trusted.
    views.emplace_back("order_book_features", R"(CREATE OR REPLACE VIEW order_book_features AS
SELECT
    receive_timestamp_ns,
    capture_timestamp_ns,
    date,
    market_id,
    token_id,
    outcome,
    midpoint,
    spread,
    imbalance_l1,
    imbalance_l5,
    imbalance_l10,
    bid_depth_l1,
    ask_depth_l1,
    bid_depth_l5,
    ask_depth_l5,
    bid_depth_l10,
    ask_depth_l10,
    book_health
FROM book_snapshots
WHERE book_health IN ('healthy', 'stale'))");

    // Per-token daily summary. Useful on its own and the natural starting
    // point for market selection.
    views.emplace_back("market_summary", R"(CREATE OR REPLACE VIEW market_summary AS
SELECT
    market_id,
    token_id,
    any_value(outcome)                AS outcome,
    date,
    AVG(spread)                       AS avg_spread,
    median(spread)                    AS median_spread,
    AVG(bid_depth_l5)                 AS avg_bid_depth,
    AVG(ask_depth_l5)                 AS avg_ask_depth,
    AVG(ABS(imbalance_l1))            AS avg_abs_imbalance,
    COUNT(*)                          AS snapshot_count
FROM book_snapshots
GROUP BY market_id, token_id, date)");

    return views;
}

#ifdef MDE_WITH_DUCKDB

}  // namespace market_data

#include <duckdb.hpp>

namespace market_data {

struct DuckDbCatalog::Impl {
    std::unique_ptr<duckdb::DuckDB> database;
    std::unique_ptr<duckdb::Connection> connection;
};

bool duckdb_support_available() { return true; }

DuckDbCatalog::DuckDbCatalog(Config config)
    : impl_(std::make_unique<Impl>()), config_(std::move(config)) {}

DuckDbCatalog::~DuckDbCatalog() { DuckDbCatalog::close(); }

bool DuckDbCatalog::open(std::string& error) {
    const std::string path = config_.duckdb_path();
    status_.database_path = path;

    std::error_code ec;
    const std::filesystem::path file{path};
    if (file.has_parent_path()) {
        std::filesystem::create_directories(file.parent_path(), ec);
        if (ec) {
            error = "cannot create duckdb directory: " + ec.message();
            status_.errors.push_back(error);
            return false;
        }
    }

    try {
        impl_->database = std::make_unique<duckdb::DuckDB>(path);
        impl_->connection = std::make_unique<duckdb::Connection>(*impl_->database);
    } catch (const std::exception& e) {
        error = std::string{"cannot open duckdb database: "} + e.what();
        status_.errors.push_back(error);
        impl_->connection.reset();
        impl_->database.reset();
        return false;
    }

    status_.opened = true;
    MDE_INFO("duckdb_opened", field("path", path));
    return true;
}

bool DuckDbCatalog::is_open() const { return impl_ && impl_->connection != nullptr; }

CatalogStatus DuckDbCatalog::refresh_views() {
    status_.views_created.clear();
    status_.views_skipped.clear();
    status_.errors.clear();

    if (!is_open()) {
        status_.errors.emplace_back("duckdb catalog is not open");
        return status_;
    }

    const auto glob_for = [](const std::string& dir) { return dir + "/**/*.parquet"; };
    const auto books_glob = glob_for(config_.books_directory());
    const auto trades_glob = glob_for(config_.trades_directory());
    const auto markets_glob = glob_for(config_.markets_directory());

    // A dataset with no files yet cannot back a view: DuckDB resolves the glob
    // when the view is created. That is the normal state before the first
    // flush, so it is reported, not treated as an error.
    const auto has_parquet = [](const std::string& dir) {
        std::error_code ec;
        if (!std::filesystem::exists(dir, ec)) {
            return false;
        }
        for (const auto& entry : std::filesystem::recursive_directory_iterator(dir, ec)) {
            if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
                return true;
            }
        }
        return false;
    };

    const bool books_ready = has_parquet(config_.books_directory());
    const bool trades_ready = has_parquet(config_.trades_directory());
    const bool markets_ready = has_parquet(config_.markets_directory());

    for (const auto& [name, sql] :
         view_definitions(books_glob, trades_glob, markets_glob)) {
        // Derived views depend on book_snapshots, so they stand or fall with it.
        const bool depends_on_books = name != "trades" && name != "markets";
        const bool ready = name == "trades"    ? trades_ready
                           : name == "markets" ? markets_ready
                                               : (depends_on_books && books_ready);
        if (!ready) {
            status_.views_skipped.push_back(name);
            continue;
        }

        const auto result = impl_->connection->Query(sql);
        if (result->HasError()) {
            status_.errors.push_back(name + ": " + result->GetError());
            MDE_WARN("duckdb_view_failed", field("view", name),
                     field("error", result->GetError()));
            continue;
        }
        status_.views_created.push_back(name);
    }

    MDE_INFO("duckdb_views_refreshed", field("created", status_.views_created.size()),
             field("skipped", status_.views_skipped.size()),
             field("errors", status_.errors.size()));
    return status_;
}

QueryResult DuckDbCatalog::query(const std::string& sql) {
    QueryResult out;
    if (!is_open()) {
        out.error = "duckdb catalog is not open";
        return out;
    }

    std::unique_ptr<duckdb::MaterializedQueryResult> result;
    try {
        result = impl_->connection->Query(sql);
    } catch (const std::exception& e) {
        out.error = e.what();
        return out;
    }
    if (result->HasError()) {
        out.error = result->GetError();
        return out;
    }

    for (const auto& name : result->names) {
        out.column_names.push_back(name);
    }
    for (std::size_t row = 0; row < result->RowCount(); ++row) {
        std::vector<std::string> values;
        values.reserve(out.column_names.size());
        for (std::size_t col = 0; col < out.column_names.size(); ++col) {
            const auto value = result->GetValue(col, row);
            values.push_back(value.IsNull() ? std::string{} : value.ToString());
        }
        out.rows.push_back(std::move(values));
    }
    return out;
}

void DuckDbCatalog::close() {
    if (!impl_ || impl_->connection == nullptr) {
        return;
    }
    // CHECKPOINT before dropping the handle so the file on disk is complete
    // rather than depending on the write-ahead log being replayed later.
    const auto result = impl_->connection->Query("CHECKPOINT");
    if (result->HasError()) {
        MDE_WARN("duckdb_checkpoint_failed", field("error", result->GetError()));
    }
    impl_->connection.reset();
    impl_->database.reset();
    MDE_INFO("duckdb_closed", field("path", status_.database_path));
}

}  // namespace market_data

#else  // MDE_WITH_DUCKDB

struct DuckDbCatalog::Impl {};

bool duckdb_support_available() { return false; }

DuckDbCatalog::DuckDbCatalog(Config config) : impl_(nullptr), config_(std::move(config)) {}
DuckDbCatalog::~DuckDbCatalog() = default;

bool DuckDbCatalog::open(std::string& error) {
    error = "this build has no DuckDB support";
    status_.errors.push_back(error);
    return false;
}

bool DuckDbCatalog::is_open() const { return false; }

CatalogStatus DuckDbCatalog::refresh_views() {
    status_.errors.emplace_back("this build has no DuckDB support");
    return status_;
}

QueryResult DuckDbCatalog::query(const std::string&) {
    QueryResult out;
    out.error = "this build has no DuckDB support";
    return out;
}

void DuckDbCatalog::close() {}

}  // namespace market_data

#endif  // MDE_WITH_DUCKDB
