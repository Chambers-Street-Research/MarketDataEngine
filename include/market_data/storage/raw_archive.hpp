// The raw event archive: append-only, replayable, highest fidelity.
//
// This is the recovery path for everything else. Parquet and DuckDB are
// derived views that can be rebuilt by replaying this archive; the archive
// itself can be rebuilt from nothing. It therefore stays the simplest possible
// format -- one JSON object per line -- and is never superseded.
//
// Layout:
//   <root>/raw/YYYY/MM/DD/events_HHMM.jsonl[.zst]
//
// The nested date directories keep a directory listing usable after months of
// collection, and the HHMM stem makes the rotation boundary obvious. Rotation
// is time-based (hourly by default) and size-based, whichever trips first.
//
// Compression: zstd streaming, flushed at frame boundaries. A file killed
// mid-write stays decodable up to the last flush, so a crash costs at most
// `flush_interval_ms` of events rather than the whole hour.
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "market_data/config.hpp"
#include "market_data/market_event.hpp"

namespace market_data {

// True when the build can write zstd-compressed archives.
bool zstd_support_available();

struct RawArchiveStats {
    std::uint64_t events_written = 0;
    // Bytes of JSON handed to the archive, before compression.
    std::uint64_t uncompressed_bytes = 0;
    // Bytes actually written to disk.
    std::uint64_t bytes_written = 0;
    std::uint64_t files_created = 0;
    std::uint64_t rotations = 0;
    std::uint64_t errors = 0;

    [[nodiscard]] double compression_ratio() const {
        return bytes_written == 0
                   ? 0.0
                   : static_cast<double>(uncompressed_bytes) / static_cast<double>(bytes_written);
    }
};

class RawEventArchive {
public:
    explicit RawEventArchive(Config config);
    ~RawEventArchive();

    RawEventArchive(const RawEventArchive&) = delete;
    RawEventArchive& operator=(const RawEventArchive&) = delete;

    bool start(std::string& error);

    // Appends one event. Returns bytes written to disk, or 0 when recording is
    // disabled or the write failed.
    std::size_t record(const MarketEvent& event);

    void flush();
    void stop();

    [[nodiscard]] RawArchiveStats stats() const;
    [[nodiscard]] std::string current_path() const;
    // "jsonl.zst" or "jsonl", reflecting what is actually being written.
    [[nodiscard]] std::string describe() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace market_data
