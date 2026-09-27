#include "market_data/storage/storage_manager.hpp"

#include <iomanip>
#include <sstream>

#include "market_data/logging.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

constexpr std::size_t kRawBatchSize = 1024;
constexpr std::size_t kParquetBatchSize = 1024;
// How often a worker wakes with no work, to honour a flush request and to
// give the raw archive a chance to roll on a time boundary while idle.
constexpr auto kWorkerPollInterval = std::chrono::milliseconds{100};

OverflowPolicy policy_for(const Config& config) {
    return config.storage.block_when_queue_full ? OverflowPolicy::Block
                                                : OverflowPolicy::DropNewest;
}

std::string with_thousands(std::uint64_t value) {
    std::string digits = std::to_string(value);
    std::string out;
    const std::size_t lead = digits.size() % 3 == 0 ? 3 : digits.size() % 3;
    for (std::size_t i = 0; i < digits.size(); ++i) {
        if (i == lead || (i > lead && (i - lead) % 3 == 0)) {
            out.push_back(',');
        }
        out.push_back(digits[i]);
    }
    return out;
}

std::string human_bytes(std::uint64_t bytes) {
    static constexpr const char* kUnits[] = {"B", "KB", "MB", "GB", "TB"};
    auto value = static_cast<double>(bytes);
    std::size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < std::size(kUnits)) {
        value /= 1024.0;
        ++unit;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(unit == 0 ? 0 : 1) << value << ' ' << kUnits[unit];
    return out.str();
}

}  // namespace

StorageManager::StorageManager(Config config)
    : config_(std::move(config)),
      raw_archive_(config_),
      parquet_writer_(make_storage_writer(config_)),
      catalog_(std::make_unique<DuckDbCatalog>(config_)),
      raw_queue_(config_.storage.raw_queue_capacity, policy_for(config_)),
      parquet_queue_(config_.storage.parquet_queue_capacity, policy_for(config_)) {}

StorageManager::~StorageManager() { shutdown(); }

bool StorageManager::start(std::string& error) {
    if (running_.exchange(true)) {
        return true;
    }

    // The raw archive is the only layer whose failure is fatal: Parquet and
    // DuckDB are derived and can be rebuilt from it, but nothing can rebuild
    // the archive itself.
    if (!raw_archive_.start(error)) {
        running_.store(false);
        return false;
    }

    std::string parquet_error;
    if (!parquet_writer_->start(parquet_error)) {
        // Degrade to a null writer rather than refuse to collect.
        MDE_ERROR("parquet_start_failed", field("error", parquet_error));
        parquet_writer_ = std::make_unique<NullStorageWriter>();
        static_cast<void>(parquet_writer_->start(parquet_error));
    }

    if (config_.storage.duckdb.enabled) {
        std::string db_error;
        if (!catalog_->open(db_error)) {
            MDE_WARN("duckdb_start_failed", field("error", db_error));
        } else if (config_.storage.duckdb.create_views) {
            // Views over whatever already exists; refreshed again at shutdown
            // once this session's partitions have been published.
            const auto status = catalog_->refresh_views();
            MDE_INFO("duckdb_catalog_ready", field("views", status.views_created.size()),
                     field("pending", status.views_skipped.size()));
        }
    }

    raw_thread_ = std::thread([this] { raw_worker(); });
    parquet_thread_ = std::thread([this] { parquet_worker(); });

    MDE_INFO("storage_started", field("raw", raw_archive_.describe()),
             field("parquet", parquet_writer_->describe()));
    return true;
}

// ---------------------------------------------------------------------------
// Enqueue
// ---------------------------------------------------------------------------

void StorageManager::warn_if_queue_deep(const char* queue_name, std::size_t depth,
                                        std::size_t capacity) {
    auto& flag = (std::string_view{queue_name} == "raw") ? raw_queue_warned_
                                                         : parquet_queue_warned_;
    const auto threshold =
        static_cast<std::size_t>(static_cast<double>(capacity) * config_.storage.queue_warn_fraction);

    if (depth >= threshold) {
        // Latched so a sustained backlog logs once, not once per event.
        if (!flag.exchange(true)) {
            MDE_ERROR("storage_queue_critical", field("queue", queue_name),
                      field("depth", depth), field("capacity", capacity));
        }
    } else if (depth < threshold / 2) {
        flag.store(false);
    }
}

void StorageManager::record_raw_event(const MarketEvent& event) {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }
    warn_if_queue_deep("raw", raw_queue_.size(), raw_queue_.capacity());
    events_queued_.fetch_add(1, std::memory_order_relaxed);
    if (!raw_queue_.push(MarketEvent{event})) {
        // Under the default blocking policy this is only reachable after
        // close(); under DropNewest it is a real, counted loss.
        events_dropped_.fetch_add(1, std::memory_order_relaxed);
    }
}

void StorageManager::record_snapshot(const MarketSnapshot& snapshot) {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }
    warn_if_queue_deep("parquet", parquet_queue_.size(), parquet_queue_.capacity());
    events_queued_.fetch_add(1, std::memory_order_relaxed);
    if (!parquet_queue_.push(ParquetItem{SnapshotRow{snapshot, now_exchange()}})) {
        events_dropped_.fetch_add(1, std::memory_order_relaxed);
    }
}

void StorageManager::record_trade(const Trade& trade) {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }
    events_queued_.fetch_add(1, std::memory_order_relaxed);
    if (!parquet_queue_.push(ParquetItem{TradeRow{trade, now_exchange()}})) {
        events_dropped_.fetch_add(1, std::memory_order_relaxed);
    }
}

void StorageManager::record_market_metadata(const Market& market) {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }
    events_queued_.fetch_add(1, std::memory_order_relaxed);
    if (!parquet_queue_.push(ParquetItem{MarketRow{market, now_exchange()}})) {
        events_dropped_.fetch_add(1, std::memory_order_relaxed);
    }
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

void StorageManager::raw_worker() {
    // raw_archive_ is touched only here, for its whole lifetime between
    // start() and the join in shutdown().
    std::vector<MarketEvent> batch;
    batch.reserve(kRawBatchSize);
    auto next_flush = std::chrono::steady_clock::now();

    while (true) {
        const auto count = raw_queue_.pop_batch_for(batch, kRawBatchSize, kWorkerPollInterval);
        if (!count) {
            break;  // Closed and drained.
        }
        for (const auto& event : batch) {
            raw_archive_.record(event);
        }

        const auto now = std::chrono::steady_clock::now();
        if (raw_flush_requested_.exchange(false) || now >= next_flush) {
            raw_archive_.flush();
            next_flush =
                now + std::chrono::milliseconds{config_.storage.raw_events.flush_interval_ms};
        }
    }
    raw_archive_.flush();
    MDE_DEBUG("raw_worker_drained", field("events", raw_archive_.stats().events_written));
}

void StorageManager::parquet_worker() {
    // parquet_writer_ is touched only here between start() and the join in
    // shutdown(); the Arrow builders it owns are not thread safe.
    std::vector<ParquetItem> batch;
    batch.reserve(kParquetBatchSize);

    while (true) {
        const auto count =
            parquet_queue_.pop_batch_for(batch, kParquetBatchSize, kWorkerPollInterval);
        if (!count) {
            break;
        }
        for (const auto& item : batch) {
            std::visit(
                [this]<typename T>(const T& row) {
                    using R = std::decay_t<T>;
                    if constexpr (std::is_same_v<R, SnapshotRow>) {
                        parquet_writer_->write_book_snapshot(row);
                    } else if constexpr (std::is_same_v<R, TradeRow>) {
                        parquet_writer_->write_trade(row);
                    } else {
                        parquet_writer_->write_market_metadata(row);
                    }
                },
                item);
        }
        // The writer decides when a row group is due; this only hands it rows.
        if (parquet_flush_requested_.exchange(false)) {
            parquet_writer_->flush();
        }
    }
    parquet_writer_->flush();
    MDE_DEBUG("parquet_worker_drained",
              field("rows", parquet_writer_->stats().rows_written()));
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void StorageManager::flush() {
    // A request, not an action: the owning worker performs the flush on its
    // next pass. Calling the writers from here would race with the worker
    // that is appending to them.
    raw_flush_requested_.store(true, std::memory_order_relaxed);
    parquet_flush_requested_.store(true, std::memory_order_relaxed);
}

void StorageManager::shutdown() {
    if (!running_.exchange(false)) {
        return;
    }

    // 1. Stop accepting, then let the workers drain what is already queued.
    //    close() wakes them; pop_batch keeps returning until empty.
    raw_queue_.close();
    parquet_queue_.close();
    if (raw_thread_.joinable()) {
        raw_thread_.join();
    }
    if (parquet_thread_.joinable()) {
        parquet_thread_.join();
    }

    // 2. Flush and finalize. The workers have been joined, so this thread is
    //    now the sole owner of both writers and may touch them directly.
    //    Parquet partitions are renamed from .tmp here, so until this returns
    //    the dataset is deliberately not readable.
    raw_archive_.flush();
    raw_archive_.stop();
    parquet_writer_->flush();
    parquet_writer_->close();

    // 3. Only now can the catalog see this session's files.
    if (catalog_ && catalog_->is_open()) {
        if (config_.storage.duckdb.create_views) {
            const auto status = catalog_->refresh_views();
            MDE_INFO("duckdb_catalog_finalized", field("views", status.views_created.size()),
                     field("errors", status.errors.size()));
        }
        catalog_->close();
    }

    MDE_INFO("storage_shutdown_complete",
             field("raw_events", raw_archive_.stats().events_written),
             field("dropped", events_dropped_.load(std::memory_order_relaxed)));
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

StorageStats StorageManager::stats() const {
    const auto raw = raw_archive_.stats();
    const auto parquet = parquet_writer_->stats();

    StorageStats s;
    s.raw_events_written = raw.events_written;
    s.raw_bytes_written = raw.bytes_written;
    s.raw_uncompressed_bytes = raw.uncompressed_bytes;
    s.raw_files_created = raw.files_created;

    s.book_snapshots_written = parquet.book_snapshots_written;
    s.trades_written = parquet.trades_written;
    s.metadata_rows_written = parquet.metadata_rows_written;

    s.parquet_files_created = parquet.files_created;
    s.parquet_bytes_written = parquet.bytes_written;
    s.parquet_flush_count = parquet.flush_count;
    s.parquet_flush_latency_ms_total = parquet.flush_latency_ms_total;
    s.parquet_max_flush_latency_ms = parquet.max_flush_latency_ms;

    s.raw_queue_depth = raw_queue_.size();
    s.raw_queue_high_water = raw_queue_.high_water_mark();
    s.parquet_queue_depth = parquet_queue_.size();
    s.parquet_queue_high_water = parquet_queue_.high_water_mark();

    s.events_queued = events_queued_.load(std::memory_order_relaxed);
    // Counted once, where the failed push was observed. The queues keep their
    // own drop counters too, and adding both would double count.
    s.events_dropped = events_dropped_.load(std::memory_order_relaxed);
    s.storage_errors = raw.errors + parquet.errors;
    return s;
}

std::string StorageManager::describe() const {
    std::ostringstream out;
    out << "raw=" << raw_archive_.describe() << "  parquet=" << parquet_writer_->describe();
    if (catalog_ && catalog_->is_open()) {
        out << "  duckdb=" << catalog_->status().database_path;
    }
    return out.str();
}

std::string StorageManager::format_summary() const {
    const auto s = stats();
    std::ostringstream out;
    out << "\nStorage Summary\n\n"
        << "  Raw events:        " << std::setw(14) << with_thousands(s.raw_events_written) << '\n'
        << "  Book snapshots:    " << std::setw(14) << with_thousands(s.book_snapshots_written)
        << '\n'
        << "  Trades:            " << std::setw(14) << with_thousands(s.trades_written) << '\n'
        << "  Metadata rows:     " << std::setw(14) << with_thousands(s.metadata_rows_written)
        << "\n\n"
        << "  Raw data:          " << std::setw(14) << human_bytes(s.raw_bytes_written);
    if (s.raw_compression_ratio() > 1.0) {
        out << std::fixed << std::setprecision(1) << "  (" << s.raw_compression_ratio()
            << "x compression)";
    }
    out << '\n'
        << "  Parquet data:      " << std::setw(14) << human_bytes(s.parquet_bytes_written) << '\n'
        << "  Raw files:         " << std::setw(14) << s.raw_files_created << '\n'
        << "  Parquet files:     " << std::setw(14) << s.parquet_files_created << "\n\n"
        << "  Queue high-water:  " << std::setw(14)
        << (std::to_string(s.raw_queue_high_water) + " raw, " +
            std::to_string(s.parquet_queue_high_water) + " parquet")
        << '\n'
        << "  Parquet flushes:   " << std::setw(14) << s.parquet_flush_count;
    if (s.parquet_flush_count > 0) {
        out << "  (max " << s.parquet_max_flush_latency_ms << " ms)";
    }
    out << '\n'
        << "  Dropped events:    " << std::setw(14) << s.events_dropped << '\n'
        << "  Storage errors:    " << std::setw(14) << s.storage_errors << '\n';

    if (s.events_dropped > 0) {
        out << "\n  WARNING: " << s.events_dropped
            << " events were dropped; this interval is INCOMPLETE.\n";
    }
    if (s.storage_errors > 0) {
        out << "\n  WARNING: " << s.storage_errors
            << " storage errors occurred; check the log before trusting this dataset.\n";
    }
    return out.str();
}

}  // namespace market_data
