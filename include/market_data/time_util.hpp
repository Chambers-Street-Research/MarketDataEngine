// Time conversion helpers. All persisted timestamps are epoch milliseconds so
// that recorded files are portable and trivially sortable; ISO-8601 is used
// only for human-facing logs.
#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string>

#include "market_data/types.hpp"

namespace market_data {

inline std::int64_t to_epoch_millis(ExchangeTime tp) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}

inline ExchangeTime from_epoch_millis(std::int64_t ms) {
    return ExchangeTime{std::chrono::milliseconds{ms}};
}

// Steady-clock instants are only meaningful within one process, so they are
// persisted as nanoseconds since that clock's arbitrary epoch. Replay compares
// them to each other, never to a wall clock.
inline std::int64_t to_steady_nanos(ReceiveTime tp) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch()).count();
}

inline ReceiveTime from_steady_nanos(std::int64_t ns) {
    return ReceiveTime{std::chrono::nanoseconds{ns}};
}

inline ExchangeTime now_exchange() { return std::chrono::system_clock::now(); }
inline ReceiveTime now_receive() { return std::chrono::steady_clock::now(); }

// "2026-09-27T14:03:11.412Z"
inline std::string format_iso8601(ExchangeTime tp) {
    using namespace std::chrono;
    const auto ms = duration_cast<milliseconds>(tp.time_since_epoch()) % 1000;
    const std::time_t secs = system_clock::to_time_t(tp);
    std::tm tm_utc{};
#if defined(_WIN32)
    gmtime_s(&tm_utc, &secs);
#else
    gmtime_r(&secs, &tm_utc);
#endif
    std::ostringstream out;
    out << std::put_time(&tm_utc, "%Y-%m-%dT%H:%M:%S") << '.' << std::setfill('0')
        << std::setw(3) << ms.count() << 'Z';
    return out.str();
}

// "2026-09-27" in UTC, used for daily file rotation.
inline std::string format_utc_date(ExchangeTime tp) {
    const std::time_t secs = std::chrono::system_clock::to_time_t(tp);
    std::tm tm_utc{};
#if defined(_WIN32)
    gmtime_s(&tm_utc, &secs);
#else
    gmtime_r(&secs, &tm_utc);
#endif
    std::ostringstream out;
    out << std::put_time(&tm_utc, "%Y-%m-%d");
    return out.str();
}

// "18" in UTC, used for hourly partitions and hourly raw-file rotation.
inline std::string format_utc_hour(ExchangeTime tp) {
    const std::time_t secs = std::chrono::system_clock::to_time_t(tp);
    std::tm tm_utc{};
#if defined(_WIN32)
    gmtime_s(&tm_utc, &secs);
#else
    gmtime_r(&secs, &tm_utc);
#endif
    std::ostringstream out;
    out << std::put_time(&tm_utc, "%H");
    return out.str();
}

// Parses "2029-01-01T04:59:00Z" and the fractional-second variants Gamma
// returns. Returns nullopt on anything it does not understand.
inline std::optional<ExchangeTime> parse_iso8601(const std::string& text) {
    if (text.size() < 19) {
        return std::nullopt;
    }
    std::tm tm_utc{};
    std::istringstream in(text.substr(0, 19));
    in >> std::get_time(&tm_utc, "%Y-%m-%dT%H:%M:%S");
    if (in.fail()) {
        return std::nullopt;
    }
#if defined(_WIN32)
    const std::time_t secs = _mkgmtime(&tm_utc);
#else
    const std::time_t secs = timegm(&tm_utc);
#endif
    if (secs == static_cast<std::time_t>(-1)) {
        return std::nullopt;
    }
    return std::chrono::system_clock::from_time_t(secs);
}

}  // namespace market_data
