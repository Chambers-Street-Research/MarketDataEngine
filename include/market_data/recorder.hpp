// Append-only raw event log, plus the research-friendly snapshot writer.
//
// Both write through a small file handle that rotates on a UTC day boundary
// and on size, and both write whole records: a record is formatted into a
// buffer and appended in one write, so a crash truncates at a record boundary
// rather than mid-JSON. Replay additionally tolerates a torn final line.
#pragma once

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "market_data/config.hpp"
#include "market_data/market_event.hpp"
#include "market_data/types.hpp"

namespace market_data {

// A rotating append-only text file.
class RotatingFile {
public:
    // `prefix` names the file stem; files land at
    // <directory>/<prefix>-<UTC date>[-<n>].<extension>
    RotatingFile(std::string directory,
                 std::string prefix,
                 std::string extension,
                 std::uint64_t max_bytes);
    ~RotatingFile();

    RotatingFile(const RotatingFile&) = delete;
    RotatingFile& operator=(const RotatingFile&) = delete;

    // Opens the current file, creating directories as needed.
    bool open(std::string& error);

    // Appends `record` whole, rotating first if needed. Returns bytes written,
    // or 0 on failure.
    std::size_t write(const std::string& record);

    // Written once at the top of every new file, e.g. a CSV header.
    void set_file_header(std::string header);

    void flush();
    void close();

    [[nodiscard]] const std::string& current_path() const { return current_path_; }
    [[nodiscard]] std::uint64_t bytes_written() const { return total_bytes_; }
    [[nodiscard]] std::uint64_t rotations() const { return rotations_; }
    [[nodiscard]] bool is_open() const { return stream_.is_open(); }

private:
    bool rotate_if_needed();
    bool open_current();

    std::string directory_;
    std::string prefix_;
    std::string extension_;
    std::uint64_t max_bytes_;
    std::string file_header_;

    std::ofstream stream_;
    std::string current_path_;
    std::string current_date_;
    std::uint64_t current_bytes_ = 0;
    std::uint64_t total_bytes_ = 0;
    std::uint64_t rotations_ = 0;
    std::uint32_t sequence_in_day_ = 0;
};

// Writes every normalized event, in arrival order, as JSON Lines.
class RawRecorder {
public:
    explicit RawRecorder(const Config& config);
    ~RawRecorder();

    bool start(std::string& error);

    // Returns bytes appended, or 0 if recording is disabled or the write failed.
    std::size_t record(const MarketEvent& event);

    void flush();
    void stop();

    [[nodiscard]] std::uint64_t events_written() const { return events_written_; }
    [[nodiscard]] std::uint64_t bytes_written() const { return file_.bytes_written(); }
    [[nodiscard]] std::uint64_t rotations() const { return file_.rotations(); }
    [[nodiscard]] const std::string& current_path() const { return file_.current_path(); }

private:
    bool enabled_;
    RotatingFile file_;
    std::uint64_t events_written_ = 0;
};

// Periodic research snapshots as CSV.
//
// CSV rather than Parquet for the first release: it is debuggable with head(1)
// and loads in one line of pandas. The writer is an interface boundary
// (SnapshotSink) so a Parquet/Arrow implementation can be dropped in without
// touching the engine.
class SnapshotSink {
public:
    virtual ~SnapshotSink() = default;
    virtual bool start(std::string& error) = 0;
    virtual std::size_t write(const MarketSnapshot& snapshot) = 0;
    virtual void flush() = 0;
    virtual void stop() = 0;
    [[nodiscard]] virtual std::uint64_t rows_written() const = 0;
};

class CsvSnapshotWriter final : public SnapshotSink {
public:
    explicit CsvSnapshotWriter(const Config& config);
    ~CsvSnapshotWriter() override;

    bool start(std::string& error) override;
    std::size_t write(const MarketSnapshot& snapshot) override;
    void flush() override;
    void stop() override;

    [[nodiscard]] std::uint64_t rows_written() const override { return rows_written_; }
    [[nodiscard]] std::uint64_t bytes_written() const { return file_.bytes_written(); }
    [[nodiscard]] const std::string& current_path() const { return file_.current_path(); }

    static const char* csv_header();

private:
    bool enabled_;
    RotatingFile file_;
    std::uint64_t rows_written_ = 0;
};

}  // namespace market_data
