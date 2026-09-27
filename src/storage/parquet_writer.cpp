#include "market_data/storage/parquet_writer.hpp"

#include <vector>

#include "market_data/logging.hpp"
#include "market_data/time_util.hpp"

#ifdef MDE_WITH_PARQUET

#include <chrono>
#include <filesystem>
#include <optional>
#include <system_error>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

namespace market_data {
namespace {

// ---------------------------------------------------------------------------
// Column definitions
//
// Each dataset is described once, as a list of (name, arrow type, builder).
// Nullability is deliberate: a nullable column means "this value genuinely may
// not exist", and every such column is appended as a real Parquet null rather
// than a sentinel. Zero is a valid price on a prediction market, so a sentinel
// would be indistinguishable from data.
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> book_schema() {
    return arrow::schema({
        arrow::field("receive_timestamp_ns", arrow::int64(), /*nullable=*/false),
        arrow::field("exchange_timestamp_ns", arrow::int64(), true),
        arrow::field("capture_timestamp_ns", arrow::int64(), false),
        arrow::field("market_id", arrow::utf8(), false),
        arrow::field("token_id", arrow::utf8(), false),
        arrow::field("outcome", arrow::utf8(), true),
        arrow::field("best_bid_ticks", arrow::int32(), true),
        arrow::field("best_ask_ticks", arrow::int32(), true),
        arrow::field("best_bid", arrow::float64(), true),
        arrow::field("best_ask", arrow::float64(), true),
        arrow::field("best_bid_size", arrow::float64(), false),
        arrow::field("best_ask_size", arrow::float64(), false),
        arrow::field("midpoint", arrow::float64(), true),
        arrow::field("spread", arrow::float64(), true),
        arrow::field("bid_depth_l1", arrow::float64(), false),
        arrow::field("ask_depth_l1", arrow::float64(), false),
        arrow::field("bid_depth_l5", arrow::float64(), false),
        arrow::field("ask_depth_l5", arrow::float64(), false),
        arrow::field("bid_depth_l10", arrow::float64(), false),
        arrow::field("ask_depth_l10", arrow::float64(), false),
        arrow::field("imbalance_l1", arrow::float64(), true),
        arrow::field("imbalance_l5", arrow::float64(), true),
        arrow::field("imbalance_l10", arrow::float64(), true),
        arrow::field("bid_levels", arrow::int32(), false),
        arrow::field("ask_levels", arrow::int32(), false),
        arrow::field("book_health", arrow::utf8(), false),
        arrow::field("sequence_number", arrow::int64(), true),
    });
}

std::shared_ptr<arrow::Schema> trade_schema() {
    return arrow::schema({
        arrow::field("receive_timestamp_ns", arrow::int64(), false),
        arrow::field("exchange_timestamp_ns", arrow::int64(), true),
        arrow::field("capture_timestamp_ns", arrow::int64(), false),
        arrow::field("market_id", arrow::utf8(), false),
        arrow::field("token_id", arrow::utf8(), false),
        arrow::field("outcome", arrow::utf8(), true),
        arrow::field("price_ticks", arrow::int32(), true),
        arrow::field("price", arrow::float64(), false),
        arrow::field("quantity", arrow::float64(), false),
        // "buy", "sell" or "unknown" -- never guessed.
        arrow::field("side", arrow::utf8(), false),
        arrow::field("trade_id", arrow::utf8(), true),
        arrow::field("sequence_number", arrow::int64(), true),
    });
}

std::shared_ptr<arrow::Schema> market_schema() {
    // One row per (market, token): a binary market contributes two rows, so
    // joining to the book and trade datasets on token_id needs no unnesting.
    return arrow::schema({
        arrow::field("snapshot_timestamp_ns", arrow::int64(), false),
        arrow::field("market_id", arrow::utf8(), false),
        arrow::field("condition_id", arrow::utf8(), true),
        arrow::field("question", arrow::utf8(), true),
        arrow::field("category", arrow::utf8(), true),
        arrow::field("slug", arrow::utf8(), true),
        arrow::field("token_id", arrow::utf8(), false),
        arrow::field("outcome", arrow::utf8(), true),
        arrow::field("active", arrow::boolean(), false),
        arrow::field("closed", arrow::boolean(), false),
        arrow::field("accepting_orders", arrow::boolean(), false),
        arrow::field("resolution_timestamp_ns", arrow::int64(), true),
        arrow::field("reported_volume", arrow::float64(), true),
        arrow::field("reported_liquidity", arrow::float64(), true),
        arrow::field("reported_volume_24h", arrow::float64(), true),
        arrow::field("minimum_tick_size", arrow::float64(), true),
    });
}

std::vector<std::string> names_of(const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<std::string> names;
    names.reserve(static_cast<std::size_t>(schema->num_fields()));
    for (const auto& field : schema->fields()) {
        names.push_back(field->name());
    }
    return names;
}

parquet::Compression::type compression_from(const std::string& name) {
    if (name == "zstd") return parquet::Compression::ZSTD;
    if (name == "snappy") return parquet::Compression::SNAPPY;
    if (name == "gzip") return parquet::Compression::GZIP;
    return parquet::Compression::UNCOMPRESSED;
}

// A NaN double means "undefined" everywhere in this engine, so it is written
// as a Parquet null. Leaving NaN in the file would make every downstream
// AVG() and comparison silently wrong.
arrow::Status append_double(arrow::DoubleBuilder& builder, double value) {
    if (std::isnan(value)) {
        return builder.AppendNull();
    }
    return builder.Append(value);
}

arrow::Status append_optional_int32(arrow::Int32Builder& builder,
                                    std::optional<PriceTicks> value) {
    if (!value) {
        return builder.AppendNull();
    }
    return builder.Append(*value);
}

arrow::Status append_optional_string(arrow::StringBuilder& builder, const std::string& value) {
    if (value.empty()) {
        return builder.AppendNull();
    }
    return builder.Append(value);
}

arrow::Status append_optional_int64(arrow::Int64Builder& builder, std::uint64_t value) {
    if (value == 0) {
        return builder.AppendNull();
    }
    return builder.Append(static_cast<std::int64_t>(value));
}

// ---------------------------------------------------------------------------
// PartitionedDataset
//
// Owns the currently open Parquet file for one dataset, buffers rows as Arrow
// builders, and rolls to a new file when the partition key changes.
// ---------------------------------------------------------------------------

struct PartitionKey {
    std::string date;  // YYYY-MM-DD
    std::string hour;  // HH, empty when not partitioning by hour

    bool operator==(const PartitionKey& other) const {
        return date == other.date && hour == other.hour;
    }
};

class PartitionedDataset {
public:
    PartitionedDataset(std::string root,
                       std::string file_stem,
                       std::shared_ptr<arrow::Schema> schema,
                       const ParquetConfig& config,
                       bool partition_by_hour)
        : root_(std::move(root)),
          file_stem_(std::move(file_stem)),
          schema_(std::move(schema)),
          config_(config),
          partition_by_hour_(partition_by_hour) {}

    ~PartitionedDataset() { close(); }

    [[nodiscard]] const std::shared_ptr<arrow::Schema>& schema() const { return schema_; }

    // Rolls the file if `key` differs from the open partition. Returns false
    // if the roll failed, in which case the caller should count an error.
    bool ensure_partition(const PartitionKey& key, StorageWriterStats& stats) {
        if (writer_ && current_ == key) {
            return true;
        }
        if (writer_ && !finalize(stats)) {
            return false;
        }
        current_ = key;
        return open(stats);
    }

    [[nodiscard]] bool is_open() const { return writer_ != nullptr; }

    // True once the buffer has reached the configured row threshold or the
    // flush interval has elapsed.
    [[nodiscard]] bool should_flush(std::size_t buffered_rows,
                                    std::chrono::steady_clock::time_point now) const {
        if (buffered_rows == 0) {
            return false;
        }
        if (buffered_rows >= config_.batch_rows) {
            return true;
        }
        return now - last_flush_ >=
               std::chrono::seconds{config_.flush_interval_seconds};
    }

    // Appends `table` as one row group to the open file.
    bool write_table(const std::shared_ptr<arrow::Table>& table, StorageWriterStats& stats) {
        if (writer_ == nullptr || table->num_rows() == 0) {
            return true;
        }
        const auto started = std::chrono::steady_clock::now();
        const auto status = writer_->WriteTable(*table, table->num_rows());
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - started)
                                 .count();

        last_flush_ = std::chrono::steady_clock::now();
        ++stats.flush_count;
        stats.flush_latency_ms_total += static_cast<std::uint64_t>(elapsed);
        stats.max_flush_latency_ms =
            std::max(stats.max_flush_latency_ms, static_cast<std::uint64_t>(elapsed));

        if (!status.ok()) {
            ++stats.errors;
            MDE_ERROR("parquet_write_failed", field("dataset", file_stem_),
                      field("error", status.ToString()));
            return false;
        }
        return true;
    }

    // Closes the current file and atomically publishes it.
    bool finalize(StorageWriterStats& stats) {
        if (writer_ == nullptr) {
            return true;
        }
        const auto status = writer_->Close();
        writer_.reset();
        if (output_) {
            static_cast<void>(output_->Close());
            output_.reset();
        }
        if (!status.ok()) {
            ++stats.errors;
            MDE_ERROR("parquet_close_failed", field("path", tmp_path_),
                      field("error", status.ToString()));
            return false;
        }

        std::error_code ec;
        const auto size = std::filesystem::file_size(tmp_path_, ec);
        if (!ec) {
            stats.bytes_written += size;
        }
        // Atomic publish: readers only ever see a file with a valid footer.
        std::filesystem::rename(tmp_path_, final_path_, ec);
        if (ec) {
            ++stats.errors;
            MDE_ERROR("parquet_rename_failed", field("from", tmp_path_),
                      field("to", final_path_), field("error", ec.message()));
            return false;
        }
        ++stats.files_created;
        MDE_INFO("parquet_file_completed", field("path", final_path_),
                 field("bytes", static_cast<std::uint64_t>(ec ? 0 : size)));
        return true;
    }

    void close() {
        if (writer_ != nullptr) {
            StorageWriterStats discarded;
            static_cast<void>(finalize(discarded));
        }
    }

private:
    bool open(StorageWriterStats& stats) {
        std::filesystem::path dir = std::filesystem::path{root_} / ("date=" + current_.date);
        if (partition_by_hour_ && !current_.hour.empty()) {
            dir /= "hour=" + current_.hour;
        }

        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        if (ec) {
            ++stats.errors;
            MDE_ERROR("parquet_mkdir_failed", field("dir", dir.string()),
                      field("error", ec.message()));
            return false;
        }

        final_path_ = (dir / (file_stem_ + ".parquet")).string();
        tmp_path_ = final_path_ + ".tmp";

        // A restart within the same hour would otherwise clobber the finished
        // file from the previous run, so an existing partition gets a suffix.
        if (std::filesystem::exists(final_path_, ec)) {
            for (int attempt = 1; attempt < 1000; ++attempt) {
                const auto candidate =
                    (dir / (file_stem_ + "-" + std::to_string(attempt) + ".parquet")).string();
                if (!std::filesystem::exists(candidate, ec)) {
                    final_path_ = candidate;
                    tmp_path_ = final_path_ + ".tmp";
                    break;
                }
            }
        }

        auto output = arrow::io::FileOutputStream::Open(tmp_path_);
        if (!output.ok()) {
            ++stats.errors;
            MDE_ERROR("parquet_open_failed", field("path", tmp_path_),
                      field("error", output.status().ToString()));
            return false;
        }
        output_ = *output;

        auto props = parquet::WriterProperties::Builder()
                         .compression(compression_from(config_.compression))
                         ->compression_level(config_.compression_level)
                         ->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();

        auto writer = parquet::arrow::FileWriter::Open(
            *schema_, arrow::default_memory_pool(), output_, props, arrow_props);
        if (!writer.ok()) {
            ++stats.errors;
            MDE_ERROR("parquet_writer_open_failed", field("path", tmp_path_),
                      field("error", writer.status().ToString()));
            output_.reset();
            return false;
        }
        writer_ = std::move(*writer);
        last_flush_ = std::chrono::steady_clock::now();
        MDE_DEBUG("parquet_file_opened", field("path", tmp_path_));
        return true;
    }

    std::string root_;
    std::string file_stem_;
    std::shared_ptr<arrow::Schema> schema_;
    ParquetConfig config_;
    bool partition_by_hour_;

    PartitionKey current_;
    std::string final_path_;
    std::string tmp_path_;
    std::shared_ptr<arrow::io::FileOutputStream> output_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
    std::chrono::steady_clock::time_point last_flush_{std::chrono::steady_clock::now()};
};

PartitionKey partition_for(ExchangeTime captured_at, bool by_hour) {
    PartitionKey key;
    key.date = format_utc_date(captured_at);
    if (by_hour) {
        key.hour = format_utc_hour(captured_at);
    }
    return key;
}

}  // namespace

// ---------------------------------------------------------------------------
// ParquetStorageWriter
// ---------------------------------------------------------------------------

struct ParquetStorageWriter::Impl {
    explicit Impl(Config cfg)
        : config(std::move(cfg)),
          books(config.books_directory(), "books", book_schema(), config.storage.parquet,
                config.storage.parquet.partition_by_hour),
          trades(config.trades_directory(), "trades", trade_schema(), config.storage.parquet,
                 config.storage.parquet.partition_by_hour),
          markets(config.markets_directory(), "markets", market_schema(),
                  config.storage.parquet, /*partition_by_hour=*/false) {}

    Config config;
    PartitionedDataset books;
    PartitionedDataset trades;
    PartitionedDataset markets;

    std::vector<SnapshotRow> book_buffer;
    std::vector<TradeRow> trade_buffer;
    std::vector<MarketRow> market_buffer;

    StorageWriterStats stats;
    bool started = false;

    // --- table building ---------------------------------------------------

    std::shared_ptr<arrow::Table> build_book_table() {
        arrow::Int64Builder receive_ts;
        arrow::Int64Builder exchange_ts;
        arrow::Int64Builder capture_ts;
        arrow::StringBuilder market_id;
        arrow::StringBuilder token_id;
        arrow::StringBuilder outcome;
        arrow::Int32Builder bid_ticks;
        arrow::Int32Builder ask_ticks;
        arrow::DoubleBuilder best_bid;
        arrow::DoubleBuilder best_ask;
        arrow::DoubleBuilder bid_size;
        arrow::DoubleBuilder ask_size;
        arrow::DoubleBuilder midpoint;
        arrow::DoubleBuilder spread;
        arrow::DoubleBuilder bid_l1;
        arrow::DoubleBuilder ask_l1;
        arrow::DoubleBuilder bid_l5;
        arrow::DoubleBuilder ask_l5;
        arrow::DoubleBuilder bid_l10;
        arrow::DoubleBuilder ask_l10;
        arrow::DoubleBuilder imb_l1;
        arrow::DoubleBuilder imb_l5;
        arrow::DoubleBuilder imb_l10;
        arrow::Int32Builder bid_levels;
        arrow::Int32Builder ask_levels;
        arrow::StringBuilder health;
        arrow::Int64Builder sequence;

        for (const auto& row : book_buffer) {
            const auto& s = row.payload;
            ARROW_UNUSED(receive_ts.Append(to_steady_nanos(s.timestamp)));
            const auto exchange_ns = to_epoch_millis(s.exchange_timestamp) * 1'000'000;
            ARROW_UNUSED(exchange_ns == 0 ? exchange_ts.AppendNull()
                                          : exchange_ts.Append(exchange_ns));
            ARROW_UNUSED(capture_ts.Append(to_epoch_millis(row.captured_at) * 1'000'000));
            ARROW_UNUSED(market_id.Append(s.market_id));
            ARROW_UNUSED(token_id.Append(s.token_id));
            ARROW_UNUSED(append_optional_string(outcome, s.outcome));
            ARROW_UNUSED(append_optional_int32(bid_ticks, s.best_bid_ticks));
            ARROW_UNUSED(append_optional_int32(ask_ticks, s.best_ask_ticks));
            ARROW_UNUSED(append_double(best_bid, s.best_bid));
            ARROW_UNUSED(append_double(best_ask, s.best_ask));
            ARROW_UNUSED(bid_size.Append(s.best_bid_size));
            ARROW_UNUSED(ask_size.Append(s.best_ask_size));
            ARROW_UNUSED(append_double(midpoint, s.midpoint));
            ARROW_UNUSED(append_double(spread, s.spread));
            ARROW_UNUSED(bid_l1.Append(s.bid_depth_l1));
            ARROW_UNUSED(ask_l1.Append(s.ask_depth_l1));
            ARROW_UNUSED(bid_l5.Append(s.bid_depth_l5));
            ARROW_UNUSED(ask_l5.Append(s.ask_depth_l5));
            ARROW_UNUSED(bid_l10.Append(s.bid_depth_l10));
            ARROW_UNUSED(ask_l10.Append(s.ask_depth_l10));
            ARROW_UNUSED(append_double(imb_l1, s.imbalance_l1));
            ARROW_UNUSED(append_double(imb_l5, s.imbalance_l5));
            ARROW_UNUSED(append_double(imb_l10, s.imbalance_l10));
            ARROW_UNUSED(bid_levels.Append(static_cast<std::int32_t>(s.bid_levels)));
            ARROW_UNUSED(ask_levels.Append(static_cast<std::int32_t>(s.ask_levels)));
            ARROW_UNUSED(health.Append(s.book_health));
            ARROW_UNUSED(append_optional_int64(sequence, s.sequence_number));
        }

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (arrow::ArrayBuilder* builder : std::vector<arrow::ArrayBuilder*>{
                 &receive_ts, &exchange_ts, &capture_ts, &market_id, &token_id, &outcome,
                 &bid_ticks, &ask_ticks, &best_bid, &best_ask, &bid_size, &ask_size, &midpoint,
                 &spread, &bid_l1, &ask_l1, &bid_l5, &ask_l5, &bid_l10, &ask_l10, &imb_l1,
                 &imb_l5, &imb_l10, &bid_levels, &ask_levels, &health, &sequence}) {
            std::shared_ptr<arrow::Array> array;
            ARROW_UNUSED(builder->Finish(&array));
            arrays.push_back(std::move(array));
        }
        return arrow::Table::Make(books.schema(), arrays);
    }

    std::shared_ptr<arrow::Table> build_trade_table() {
        arrow::Int64Builder receive_ts;
        arrow::Int64Builder exchange_ts;
        arrow::Int64Builder capture_ts;
        arrow::StringBuilder market_id;
        arrow::StringBuilder token_id;
        arrow::StringBuilder outcome;
        arrow::Int32Builder price_ticks;
        arrow::DoubleBuilder price;
        arrow::DoubleBuilder quantity;
        arrow::StringBuilder side;
        arrow::StringBuilder trade_id;
        arrow::Int64Builder sequence;

        for (const auto& row : trade_buffer) {
            const auto& t = row.payload;
            ARROW_UNUSED(receive_ts.Append(to_steady_nanos(t.receive_timestamp)));
            const auto exchange_ns = to_epoch_millis(t.exchange_timestamp) * 1'000'000;
            ARROW_UNUSED(exchange_ns == 0 ? exchange_ts.AppendNull()
                                          : exchange_ts.Append(exchange_ns));
            ARROW_UNUSED(capture_ts.Append(to_epoch_millis(row.captured_at) * 1'000'000));
            ARROW_UNUSED(market_id.Append(t.market_id));
            ARROW_UNUSED(token_id.Append(t.token_id));
            ARROW_UNUSED(append_optional_string(outcome, t.outcome));
            ARROW_UNUSED(price_ticks.Append(price_to_ticks(t.price)));
            ARROW_UNUSED(price.Append(t.price));
            ARROW_UNUSED(quantity.Append(t.quantity));
            ARROW_UNUSED(side.Append(trade_side_label(t.aggressor_side)));
            ARROW_UNUSED(append_optional_string(trade_id, t.trade_id));
            ARROW_UNUSED(append_optional_int64(sequence, t.sequence_number));
        }

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (arrow::ArrayBuilder* builder : std::vector<arrow::ArrayBuilder*>{
                 &receive_ts, &exchange_ts, &capture_ts, &market_id, &token_id, &outcome,
                 &price_ticks, &price, &quantity, &side, &trade_id, &sequence}) {
            std::shared_ptr<arrow::Array> array;
            ARROW_UNUSED(builder->Finish(&array));
            arrays.push_back(std::move(array));
        }
        return arrow::Table::Make(trades.schema(), arrays);
    }

    std::shared_ptr<arrow::Table> build_market_table() {
        arrow::Int64Builder snapshot_ts;
        arrow::StringBuilder market_id;
        arrow::StringBuilder condition_id;
        arrow::StringBuilder question;
        arrow::StringBuilder category;
        arrow::StringBuilder slug;
        arrow::StringBuilder token_id;
        arrow::StringBuilder outcome;
        arrow::BooleanBuilder active;
        arrow::BooleanBuilder closed;
        arrow::BooleanBuilder accepting;
        arrow::Int64Builder resolution_ts;
        arrow::DoubleBuilder volume;
        arrow::DoubleBuilder liquidity;
        arrow::DoubleBuilder volume_24h;
        arrow::DoubleBuilder tick_size;

        for (const auto& row : market_buffer) {
            const auto& m = row.payload;
            for (std::size_t i = 0; i < m.token_ids.size(); ++i) {
                ARROW_UNUSED(snapshot_ts.Append(to_epoch_millis(row.captured_at) * 1'000'000));
                ARROW_UNUSED(market_id.Append(m.market_id));
                // On Polymarket the market id *is* the condition id; the column
                // is kept distinct so another venue can populate it differently.
                ARROW_UNUSED(append_optional_string(condition_id, m.market_id));
                ARROW_UNUSED(append_optional_string(question, m.question));
                ARROW_UNUSED(append_optional_string(category, m.category));
                ARROW_UNUSED(append_optional_string(slug, m.slug));
                ARROW_UNUSED(token_id.Append(m.token_ids[i]));
                ARROW_UNUSED(append_optional_string(
                    outcome, i < m.outcomes.size() ? m.outcomes[i] : std::string{}));
                ARROW_UNUSED(active.Append(m.active));
                ARROW_UNUSED(closed.Append(m.closed));
                ARROW_UNUSED(accepting.Append(m.accepting_orders));
                if (m.resolution_time) {
                    ARROW_UNUSED(
                        resolution_ts.Append(to_epoch_millis(*m.resolution_time) * 1'000'000));
                } else {
                    ARROW_UNUSED(resolution_ts.AppendNull());
                }
                ARROW_UNUSED(volume.Append(m.reported_volume));
                ARROW_UNUSED(liquidity.Append(m.reported_liquidity));
                ARROW_UNUSED(volume_24h.Append(m.reported_volume_24h));
                // Zero means the venue did not report a tick size.
                ARROW_UNUSED(m.min_tick_size > 0.0 ? tick_size.Append(m.min_tick_size)
                                                   : tick_size.AppendNull());
            }
        }

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (arrow::ArrayBuilder* builder : std::vector<arrow::ArrayBuilder*>{
                 &snapshot_ts, &market_id, &condition_id, &question, &category, &slug, &token_id,
                 &outcome, &active, &closed, &accepting, &resolution_ts, &volume, &liquidity,
                 &volume_24h, &tick_size}) {
            std::shared_ptr<arrow::Array> array;
            ARROW_UNUSED(builder->Finish(&array));
            arrays.push_back(std::move(array));
        }
        return arrow::Table::Make(markets.schema(), arrays);
    }

    void flush_books(bool force) {
        if (book_buffer.empty()) {
            return;
        }
        if (!force && !books.should_flush(book_buffer.size(), std::chrono::steady_clock::now())) {
            return;
        }
        const auto table = build_book_table();
        if (books.write_table(table, stats)) {
            stats.book_snapshots_written += static_cast<std::uint64_t>(table->num_rows());
        }
        book_buffer.clear();
    }

    void flush_trades(bool force) {
        if (trade_buffer.empty()) {
            return;
        }
        if (!force && !trades.should_flush(trade_buffer.size(), std::chrono::steady_clock::now())) {
            return;
        }
        const auto table = build_trade_table();
        if (trades.write_table(table, stats)) {
            stats.trades_written += static_cast<std::uint64_t>(table->num_rows());
        }
        trade_buffer.clear();
    }

    void flush_markets(bool force) {
        if (market_buffer.empty()) {
            return;
        }
        if (!force &&
            !markets.should_flush(market_buffer.size(), std::chrono::steady_clock::now())) {
            return;
        }
        const auto table = build_market_table();
        if (markets.write_table(table, stats)) {
            stats.metadata_rows_written += static_cast<std::uint64_t>(table->num_rows());
        }
        market_buffer.clear();
    }
};

bool parquet_support_available() { return true; }

ParquetStorageWriter::ParquetStorageWriter(Config config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

ParquetStorageWriter::~ParquetStorageWriter() { ParquetStorageWriter::close(); }

bool ParquetStorageWriter::start(std::string& error) {
    std::error_code ec;
    for (const auto& dir : {impl_->config.books_directory(), impl_->config.trades_directory(),
                            impl_->config.markets_directory()}) {
        std::filesystem::create_directories(dir, ec);
        if (ec) {
            error = "cannot create parquet directory " + dir + ": " + ec.message();
            return false;
        }
    }
    impl_->started = true;
    MDE_INFO("parquet_storage_started", field("root", impl_->config.storage.root_directory),
             field("compression", impl_->config.storage.parquet.compression),
             field("batch_rows", impl_->config.storage.parquet.batch_rows));
    return true;
}

void ParquetStorageWriter::write_book_snapshot(const SnapshotRow& row) {
    const auto key = partition_for(row.captured_at, impl_->config.storage.parquet.partition_by_hour);
    // Roll before buffering, so every row in a batch belongs to one partition.
    if (impl_->books.is_open() && !impl_->book_buffer.empty()) {
        const auto current = partition_for(impl_->book_buffer.front().captured_at,
                                           impl_->config.storage.parquet.partition_by_hour);
        if (!(current == key)) {
            impl_->flush_books(/*force=*/true);
        }
    }
    if (!impl_->books.ensure_partition(key, impl_->stats)) {
        return;
    }
    impl_->book_buffer.push_back(row);
    impl_->flush_books(/*force=*/false);
}

void ParquetStorageWriter::write_trade(const TradeRow& row) {
    const auto key = partition_for(row.captured_at, impl_->config.storage.parquet.partition_by_hour);
    if (impl_->trades.is_open() && !impl_->trade_buffer.empty()) {
        const auto current = partition_for(impl_->trade_buffer.front().captured_at,
                                           impl_->config.storage.parquet.partition_by_hour);
        if (!(current == key)) {
            impl_->flush_trades(/*force=*/true);
        }
    }
    if (!impl_->trades.ensure_partition(key, impl_->stats)) {
        return;
    }
    impl_->trade_buffer.push_back(row);
    impl_->flush_trades(/*force=*/false);
}

void ParquetStorageWriter::write_market_metadata(const MarketRow& row) {
    const auto key = partition_for(row.captured_at, /*by_hour=*/false);
    if (impl_->markets.is_open() && !impl_->market_buffer.empty()) {
        const auto current = partition_for(impl_->market_buffer.front().captured_at, false);
        if (!(current == key)) {
            impl_->flush_markets(/*force=*/true);
        }
    }
    if (!impl_->markets.ensure_partition(key, impl_->stats)) {
        return;
    }
    impl_->market_buffer.push_back(row);
    impl_->flush_markets(/*force=*/false);
}

void ParquetStorageWriter::flush() {
    impl_->flush_books(/*force=*/true);
    impl_->flush_trades(/*force=*/true);
    impl_->flush_markets(/*force=*/true);
}

void ParquetStorageWriter::close() {
    if (!impl_ || !impl_->started) {
        return;
    }
    flush();
    // Finalize publishes each partition under its real name; until this runs
    // the files on disk are .tmp and deliberately unreadable as a dataset.
    static_cast<void>(impl_->books.finalize(impl_->stats));
    static_cast<void>(impl_->trades.finalize(impl_->stats));
    static_cast<void>(impl_->markets.finalize(impl_->stats));
    impl_->started = false;
}

StorageWriterStats ParquetStorageWriter::stats() const { return impl_->stats; }

std::string ParquetStorageWriter::describe() const {
    return "parquet(" + impl_->config.storage.parquet.compression + ") -> " +
           impl_->config.storage.root_directory;
}

std::vector<std::string> ParquetStorageWriter::book_columns() { return names_of(book_schema()); }
std::vector<std::string> ParquetStorageWriter::trade_columns() { return names_of(trade_schema()); }
std::vector<std::string> ParquetStorageWriter::market_columns() {
    return names_of(market_schema());
}

}  // namespace market_data

#else  // MDE_WITH_PARQUET

namespace market_data {

bool parquet_support_available() { return false; }

struct ParquetStorageWriter::Impl {};

ParquetStorageWriter::ParquetStorageWriter(Config) : impl_(nullptr) {}
ParquetStorageWriter::~ParquetStorageWriter() = default;

bool ParquetStorageWriter::start(std::string& error) {
    error = "this build has no Arrow/Parquet support";
    return false;
}

void ParquetStorageWriter::write_book_snapshot(const SnapshotRow&) {}
void ParquetStorageWriter::write_trade(const TradeRow&) {}
void ParquetStorageWriter::write_market_metadata(const MarketRow&) {}
void ParquetStorageWriter::flush() {}
void ParquetStorageWriter::close() {}
StorageWriterStats ParquetStorageWriter::stats() const { return {}; }
std::string ParquetStorageWriter::describe() const { return "parquet(unavailable)"; }
std::vector<std::string> ParquetStorageWriter::book_columns() { return {}; }
std::vector<std::string> ParquetStorageWriter::trade_columns() { return {}; }
std::vector<std::string> ParquetStorageWriter::market_columns() { return {}; }

}  // namespace market_data

#endif  // MDE_WITH_PARQUET

namespace market_data {

std::unique_ptr<StorageWriter> make_storage_writer(const Config& config) {
    if (!config.storage.parquet.enabled) {
        MDE_INFO("parquet_storage_disabled", field("reason", "disabled in config"));
        return std::make_unique<NullStorageWriter>();
    }
    if (!parquet_support_available()) {
        // Degrade rather than refuse to run: the raw archive still captures
        // everything, and the dataset can be rebuilt from it by replay.
        MDE_WARN("parquet_storage_unavailable",
                 field("reason", "build has no Arrow/Parquet support"));
        return std::make_unique<NullStorageWriter>();
    }
    return std::make_unique<ParquetStorageWriter>(config);
}

}  // namespace market_data
