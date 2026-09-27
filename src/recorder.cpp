#include "market_data/recorder.hpp"

#include <cmath>
#include <optional>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <system_error>

#include "market_data/event_codec.hpp"
#include "market_data/logging.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

// Formats a double for CSV. NaN becomes an empty field so downstream tools
// read it as a missing value rather than the string "nan" or a bogus 0.
std::string csv_double(double value, int precision = 6) {
    if (std::isnan(value)) {
        return {};
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(precision) << value;
    return out.str();
}

// An absent quote is an empty field, never a number: zero is a valid price.
std::string csv_optional_ticks(std::optional<PriceTicks> ticks) {
    return ticks ? std::to_string(*ticks) : std::string{};
}

}  // namespace

// ---------------------------------------------------------------------------
// RotatingFile
// ---------------------------------------------------------------------------

RotatingFile::RotatingFile(std::string directory,
                           std::string prefix,
                           std::string extension,
                           std::uint64_t max_bytes)
    : directory_(std::move(directory)),
      prefix_(std::move(prefix)),
      extension_(std::move(extension)),
      max_bytes_(max_bytes) {}

RotatingFile::~RotatingFile() { close(); }

void RotatingFile::set_file_header(std::string header) { file_header_ = std::move(header); }

bool RotatingFile::open(std::string& error) {
    std::error_code ec;
    std::filesystem::create_directories(directory_, ec);
    if (ec) {
        error = "cannot create directory " + directory_ + ": " + ec.message();
        return false;
    }
    current_date_ = format_utc_date(now_exchange());
    sequence_in_day_ = 0;
    if (!open_current()) {
        error = "cannot open output file in " + directory_;
        return false;
    }
    return true;
}

bool RotatingFile::open_current() {
    std::ostringstream name;
    name << prefix_ << '-' << current_date_;
    if (sequence_in_day_ > 0) {
        name << '-' << std::setfill('0') << std::setw(3) << sequence_in_day_;
    }
    name << '.' << extension_;

    const auto path = std::filesystem::path{directory_} / name.str();
    current_path_ = path.string();

    // Append rather than truncate: restarting the engine must never destroy a
    // session already recorded today.
    const bool existed = std::filesystem::exists(path);
    stream_.open(path, std::ios::binary | std::ios::app);
    if (!stream_.is_open()) {
        return false;
    }

    std::error_code ec;
    current_bytes_ = existed ? std::filesystem::file_size(path, ec) : 0;
    if (ec) {
        current_bytes_ = 0;
    }

    if (!file_header_.empty() && current_bytes_ == 0) {
        stream_ << file_header_;
        current_bytes_ += file_header_.size();
        total_bytes_ += file_header_.size();
    }

    MDE_INFO("file_opened", field("path", current_path_), field("bytes", current_bytes_));
    return true;
}

bool RotatingFile::rotate_if_needed() {
    const std::string today = format_utc_date(now_exchange());
    const bool day_changed = today != current_date_;
    const bool too_big = max_bytes_ > 0 && current_bytes_ >= max_bytes_;

    if (!day_changed && !too_big) {
        return true;
    }

    stream_.flush();
    stream_.close();
    ++rotations_;

    if (day_changed) {
        current_date_ = today;
        sequence_in_day_ = 0;
    } else {
        ++sequence_in_day_;
    }

    MDE_INFO("file_rotated", field("reason", day_changed ? "utc_day" : "size"),
             field("previous", current_path_));
    return open_current();
}

std::size_t RotatingFile::write(const std::string& record) {
    if (!stream_.is_open() && !rotate_if_needed()) {
        return 0;
    }
    if (!rotate_if_needed()) {
        return 0;
    }
    // One write per whole record, so a crash truncates between records.
    stream_.write(record.data(), static_cast<std::streamsize>(record.size()));
    if (!stream_) {
        MDE_ERROR("file_write_failed", field("path", current_path_));
        return 0;
    }
    current_bytes_ += record.size();
    total_bytes_ += record.size();
    return record.size();
}

void RotatingFile::flush() {
    if (stream_.is_open()) {
        stream_.flush();
    }
}

void RotatingFile::close() {
    if (stream_.is_open()) {
        stream_.flush();
        stream_.close();
        MDE_INFO("file_closed", field("path", current_path_), field("bytes", total_bytes_));
    }
}

// ---------------------------------------------------------------------------
// RawRecorder
// ---------------------------------------------------------------------------

RawRecorder::RawRecorder(const Config& config)
    : enabled_(config.storage.raw_events.enabled),
      file_(config.raw_directory(), "events", "jsonl", config.storage.raw_events.max_file_bytes) {}

RawRecorder::~RawRecorder() { stop(); }

bool RawRecorder::start(std::string& error) {
    if (!enabled_) {
        return true;
    }
    return file_.open(error);
}

std::size_t RawRecorder::record(const MarketEvent& event) {
    if (!enabled_) {
        return 0;
    }
    const std::size_t bytes = file_.write(encode_event_line(event));
    if (bytes > 0) {
        ++events_written_;
    }
    return bytes;
}

void RawRecorder::flush() {
    if (enabled_) {
        file_.flush();
    }
}

void RawRecorder::stop() {
    if (enabled_) {
        file_.close();
    }
}

// ---------------------------------------------------------------------------
// CsvSnapshotWriter
// ---------------------------------------------------------------------------

const char* CsvSnapshotWriter::csv_header() {
    return "timestamp_ns,exchange_ts_ms,market_id,token_id,outcome,"
           "best_bid_ticks,best_ask_ticks,best_bid,best_ask,"
           "best_bid_size,best_ask_size,midpoint,spread,"
           "bid_depth_l1,ask_depth_l1,bid_depth_l5,ask_depth_l5,"
           "bid_depth_l10,ask_depth_l10,"
           "imbalance_l1,imbalance_l5,imbalance_l10,"
           "bid_levels,ask_levels,book_health,sequence_number\n";
}

CsvSnapshotWriter::CsvSnapshotWriter(const Config& config)
    : enabled_(config.storage.write_csv_snapshots),
      file_(config.normalized_directory(), "snapshots", "csv",
            config.storage.raw_events.max_file_bytes) {
    file_.set_file_header(csv_header());
}

CsvSnapshotWriter::~CsvSnapshotWriter() { stop(); }

bool CsvSnapshotWriter::start(std::string& error) {
    if (!enabled_) {
        return true;
    }
    return file_.open(error);
}

std::size_t CsvSnapshotWriter::write(const MarketSnapshot& snapshot) {
    if (!enabled_) {
        return 0;
    }

    std::ostringstream row;
    row << to_steady_nanos(snapshot.timestamp) << ','
        << to_epoch_millis(snapshot.exchange_timestamp) << ','
        << snapshot.market_id << ',' << snapshot.token_id << ','
        << snapshot.outcome << ','
        << csv_optional_ticks(snapshot.best_bid_ticks) << ','
        << csv_optional_ticks(snapshot.best_ask_ticks) << ','
        << csv_double(snapshot.best_bid, 4) << ','
        << csv_double(snapshot.best_ask, 4) << ','
        << csv_double(snapshot.best_bid_size) << ','
        << csv_double(snapshot.best_ask_size) << ','
        << csv_double(snapshot.midpoint, 5) << ','
        << csv_double(snapshot.spread, 4) << ','
        << csv_double(snapshot.bid_depth_l1) << ','
        << csv_double(snapshot.ask_depth_l1) << ','
        << csv_double(snapshot.bid_depth_l5) << ','
        << csv_double(snapshot.ask_depth_l5) << ','
        << csv_double(snapshot.bid_depth_l10) << ','
        << csv_double(snapshot.ask_depth_l10) << ','
        << csv_double(snapshot.imbalance_l1) << ','
        << csv_double(snapshot.imbalance_l5) << ','
        << csv_double(snapshot.imbalance_l10) << ','
        << snapshot.bid_levels << ',' << snapshot.ask_levels << ','
        << snapshot.book_health << ',' << snapshot.sequence_number << '\n';

    const std::size_t bytes = file_.write(row.str());
    if (bytes > 0) {
        ++rows_written_;
    }
    return bytes;
}

void CsvSnapshotWriter::flush() {
    if (enabled_) {
        file_.flush();
    }
}

void CsvSnapshotWriter::stop() {
    if (enabled_) {
        file_.close();
    }
}

}  // namespace market_data
