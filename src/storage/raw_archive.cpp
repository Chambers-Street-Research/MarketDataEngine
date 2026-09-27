#include "market_data/storage/raw_archive.hpp"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <system_error>
#include <vector>

#include "market_data/event_codec.hpp"
#include "market_data/logging.hpp"
#include "market_data/time_util.hpp"

#ifdef MDE_WITH_ZSTD
#include <zstd.h>
#endif

namespace market_data {
namespace {

// Compressing in chunks rather than per line: zstd's frame overhead would
// otherwise dominate for the small JSON lines this archive writes.
constexpr std::size_t kCompressChunkBytes = 256 * 1024;

std::string two_digits(int value) {
    std::ostringstream out;
    out << std::setfill('0') << std::setw(2) << value;
    return out.str();
}

// Start-of-period instant for `tp`, so every file in a run lands on the same
// grid regardless of when the process happened to start.
ExchangeTime period_start(ExchangeTime tp, std::uint32_t rotation_minutes) {
    using namespace std::chrono;
    const auto minutes_since_epoch = duration_cast<minutes>(tp.time_since_epoch()).count();
    const auto period = static_cast<std::int64_t>(rotation_minutes == 0 ? 60 : rotation_minutes);
    const auto aligned = (minutes_since_epoch / period) * period;
    return ExchangeTime{minutes{aligned}};
}

}  // namespace

bool zstd_support_available() {
#ifdef MDE_WITH_ZSTD
    return true;
#else
    return false;
#endif
}

struct RawEventArchive::Impl {
    explicit Impl(Config cfg) : config(std::move(cfg)) {
        const auto& raw = config.storage.raw_events;
        compress = raw.compression == "zstd";
        if (compress && !zstd_support_available()) {
            // Degrade rather than refuse: an uncompressed archive is still a
            // complete archive, which is the property that matters.
            MDE_WARN("raw_archive_compression_unavailable",
                     field("requested", raw.compression), field("using", "none"));
            compress = false;
        }
    }

    ~Impl() { close_file(); }

    Config config;
    bool compress = false;
    bool enabled = false;

    std::FILE* file = nullptr;
    std::string current_path;
    std::string tmp_path;
    ExchangeTime current_period{};
    std::uint64_t current_bytes = 0;

    // Staging buffer for the compressor.
    std::string pending;
    RawArchiveStats stats;

#ifdef MDE_WITH_ZSTD
    ZSTD_CCtx* cctx = nullptr;
    std::vector<char> out_buffer;
#endif

    // -----------------------------------------------------------------------

    [[nodiscard]] std::string extension() const {
        return compress ? "jsonl.zst" : "jsonl";
    }

    std::filesystem::path path_for(ExchangeTime period) const {
        const std::time_t secs = std::chrono::system_clock::to_time_t(period);
        std::tm tm_utc{};
#if defined(_WIN32)
        gmtime_s(&tm_utc, &secs);
#else
        gmtime_r(&secs, &tm_utc);
#endif
        std::filesystem::path dir{config.raw_directory()};
        dir /= std::to_string(tm_utc.tm_year + 1900);
        dir /= two_digits(tm_utc.tm_mon + 1);
        dir /= two_digits(tm_utc.tm_mday);

        const std::string stem =
            "events_" + two_digits(tm_utc.tm_hour) + two_digits(tm_utc.tm_min);
        return dir / (stem + "." + extension());
    }

    bool open_file(ExchangeTime period, std::string& error) {
        auto path = path_for(period);

        std::error_code ec;
        std::filesystem::create_directories(path.parent_path(), ec);
        if (ec) {
            error = "cannot create raw directory " + path.parent_path().string() + ": " +
                    ec.message();
            return false;
        }

        // A restart inside the same period must not clobber the previous run's
        // file, and a compressed stream cannot simply be appended to.
        if (std::filesystem::exists(path, ec)) {
            for (int attempt = 1; attempt < 1000; ++attempt) {
                auto candidate = path.parent_path() /
                                 (path.stem().stem().string() + "-" + std::to_string(attempt) +
                                  "." + extension());
                if (!std::filesystem::exists(candidate, ec)) {
                    path = candidate;
                    break;
                }
            }
        }

        current_path = path.string();
        // Written under a .partial name and renamed on close, so a reader can
        // tell a finished archive file from one still being appended to.
        tmp_path = current_path + ".partial";

        file = std::fopen(tmp_path.c_str(), "wb");
        if (file == nullptr) {
            error = "cannot open raw archive file " + tmp_path;
            return false;
        }

#ifdef MDE_WITH_ZSTD
        if (compress) {
            cctx = ZSTD_createCCtx();
            if (cctx == nullptr) {
                error = "cannot create zstd compression context";
                std::fclose(file);
                file = nullptr;
                return false;
            }
            ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel,
                                   config.storage.raw_events.compression_level);
            // Checksums make a truncated or corrupted frame detectable at
            // decompression instead of silently yielding garbage.
            ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);
            out_buffer.resize(ZSTD_CStreamOutSize());
        }
#endif

        current_period = period;
        current_bytes = 0;
        ++stats.files_created;
        MDE_INFO("raw_archive_opened", field("path", current_path),
                 field("compression", compress ? "zstd" : "none"));
        return true;
    }

#ifdef MDE_WITH_ZSTD
    // Pushes `pending` through the compressor with the given end directive.
    bool compress_pending(ZSTD_EndDirective mode) {
        if (cctx == nullptr || file == nullptr) {
            return false;
        }
        ZSTD_inBuffer input{pending.data(), pending.size(), 0};
        bool finished = false;
        do {
            ZSTD_outBuffer output{out_buffer.data(), out_buffer.size(), 0};
            const std::size_t remaining = ZSTD_compressStream2(cctx, &output, &input, mode);
            if (ZSTD_isError(remaining) != 0) {
                ++stats.errors;
                MDE_ERROR("raw_archive_compress_failed",
                          field("error", ZSTD_getErrorName(remaining)));
                return false;
            }
            if (output.pos > 0) {
                if (std::fwrite(out_buffer.data(), 1, output.pos, file) != output.pos) {
                    ++stats.errors;
                    MDE_ERROR("raw_archive_write_failed", field("path", tmp_path));
                    return false;
                }
                current_bytes += output.pos;
                stats.bytes_written += output.pos;
            }
            finished = (mode == ZSTD_e_continue) ? (input.pos == input.size) : (remaining == 0);
        } while (!finished);
        pending.clear();
        return true;
    }
#endif

    bool write_pending_plain() {
        if (pending.empty() || file == nullptr) {
            return true;
        }
        if (std::fwrite(pending.data(), 1, pending.size(), file) != pending.size()) {
            ++stats.errors;
            MDE_ERROR("raw_archive_write_failed", field("path", tmp_path));
            return false;
        }
        current_bytes += pending.size();
        stats.bytes_written += pending.size();
        pending.clear();
        return true;
    }

    bool drain(bool force_flush) {
#ifdef MDE_WITH_ZSTD
        if (compress) {
            if (pending.empty() && !force_flush) {
                return true;
            }
            return compress_pending(force_flush ? ZSTD_e_flush : ZSTD_e_continue);
        }
#else
        // Without zstd `compress` is always false, so there is no compressed
        // path and nothing to distinguish a forced flush from a normal one:
        // a plain write is already durable at the stdio layer.
        static_cast<void>(force_flush);
#endif
        return write_pending_plain();
    }

    void close_file() {
        if (file == nullptr) {
            return;
        }
#ifdef MDE_WITH_ZSTD
        if (compress && cctx != nullptr) {
            // ZSTD_e_end writes the frame epilogue, including the checksum.
            static_cast<void>(compress_pending(ZSTD_e_end));
            ZSTD_freeCCtx(cctx);
            cctx = nullptr;
        } else {
            static_cast<void>(write_pending_plain());
        }
#else
        static_cast<void>(write_pending_plain());
#endif
        std::fflush(file);
        std::fclose(file);
        file = nullptr;

        std::error_code ec;
        std::filesystem::rename(tmp_path, current_path, ec);
        if (ec) {
            ++stats.errors;
            MDE_ERROR("raw_archive_rename_failed", field("from", tmp_path),
                      field("to", current_path), field("error", ec.message()));
        } else {
            MDE_INFO("raw_archive_closed", field("path", current_path),
                     field("bytes", current_bytes));
        }
    }

    // Rolls when the time period changes or the file grows past its cap.
    bool rotate_if_needed() {
        const auto now = now_exchange();
        const auto period = period_start(now, config.storage.raw_events.rotation_minutes);
        const bool period_changed = period != current_period;
        const bool too_big = config.storage.raw_events.max_file_bytes > 0 &&
                             current_bytes >= config.storage.raw_events.max_file_bytes;

        if (file != nullptr && !period_changed && !too_big) {
            return true;
        }
        if (file != nullptr) {
            static_cast<void>(drain(/*force_flush=*/true));
            close_file();
            ++stats.rotations;
            MDE_INFO("raw_archive_rotated",
                     field("reason", period_changed ? "period" : "size"));
        }

        std::string error;
        if (!open_file(period, error)) {
            ++stats.errors;
            MDE_ERROR("raw_archive_open_failed", field("error", error));
            return false;
        }
        return true;
    }
};

RawEventArchive::RawEventArchive(Config config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

RawEventArchive::~RawEventArchive() { RawEventArchive::stop(); }

bool RawEventArchive::start(std::string& error) {
    if (!impl_->config.storage.raw_events.enabled) {
        return true;
    }
    const auto period = period_start(now_exchange(),
                                     impl_->config.storage.raw_events.rotation_minutes);
    if (!impl_->open_file(period, error)) {
        return false;
    }
    impl_->enabled = true;
    return true;
}

std::size_t RawEventArchive::record(const MarketEvent& event) {
    if (!impl_->enabled) {
        return 0;
    }
    if (!impl_->rotate_if_needed()) {
        return 0;
    }

    const std::string line = encode_event_line(event);
    impl_->pending += line;
    impl_->stats.uncompressed_bytes += line.size();
    ++impl_->stats.events_written;

    if (impl_->pending.size() >= kCompressChunkBytes) {
        static_cast<void>(impl_->drain(/*force_flush=*/false));
    }
    return line.size();
}

void RawEventArchive::flush() {
    if (!impl_->enabled) {
        return;
    }
    // A forced flush closes the current zstd block, so everything written so
    // far is decodable even if the process dies immediately afterwards.
    static_cast<void>(impl_->drain(/*force_flush=*/true));
    if (impl_->file != nullptr) {
        std::fflush(impl_->file);
    }
}

void RawEventArchive::stop() {
    if (impl_ == nullptr || !impl_->enabled) {
        return;
    }
    static_cast<void>(impl_->drain(/*force_flush=*/true));
    impl_->close_file();
    impl_->enabled = false;
}

RawArchiveStats RawEventArchive::stats() const { return impl_->stats; }

std::string RawEventArchive::current_path() const { return impl_->current_path; }

std::string RawEventArchive::describe() const {
    return impl_->extension() + " -> " + impl_->config.raw_directory();
}

}  // namespace market_data
