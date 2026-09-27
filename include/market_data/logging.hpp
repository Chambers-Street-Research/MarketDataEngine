// Structured logging.
//
// Every record is one line of `key=value` pairs prefixed by timestamp, level
// and event name, which greps cleanly and ingests into anything that reads
// logfmt. Market-data updates are deliberately *not* logged at info: at a few
// thousand messages a second the log would cost more than the feed.
#pragma once

#include <atomic>
#include <cstdint>
#include <initializer_list>
#include <mutex>
#include <type_traits>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace market_data {

enum class LogLevel : std::uint8_t { Trace = 0, Debug, Info, Warn, Error, Off };

std::string_view to_string(LogLevel level);

// Parses "trace"/"debug"/"info"/"warn"/"error"/"off"; returns false if the
// text is not a level name.
bool parse_log_level(std::string_view text, LogLevel& out);

// One key=value pair. Values are stringified at the call site.
struct LogField {
    std::string key;
    std::string value;
};

LogField field(std::string key, std::string value);
LogField field(std::string key, const char* value);
LogField field(std::string key, std::string_view value);
LogField field(std::string key, bool value);
LogField field(std::string key, double value);

// One template rather than an overload per width: `field("n", some_uint32)`
// was otherwise ambiguous between int, size_t and the fixed-width types.
template <typename T>
    requires std::is_integral_v<T> && (!std::is_same_v<std::remove_cv_t<T>, bool>)
LogField field(std::string key, T value) {
    return LogField{std::move(key), std::to_string(value)};
}

// Process-wide logger. This is the one piece of shared mutable state in the
// engine; it is confined to a single object with an internal mutex rather than
// scattered globals, and the level is atomic so hot paths can check it without
// contending on the lock.
class Logger {
public:
    static Logger& instance();

    void set_level(LogLevel level) { level_.store(level, std::memory_order_relaxed); }
    [[nodiscard]] LogLevel level() const { return level_.load(std::memory_order_relaxed); }
    [[nodiscard]] bool enabled(LogLevel level) const { return level >= this->level(); }

    // Redirect output, e.g. to a file or a test buffer. The stream must outlive
    // the logger's use of it.
    void set_stream(std::ostream& stream);

    void log(LogLevel level, std::string_view event, std::initializer_list<LogField> fields);

private:
    Logger() = default;

    std::atomic<LogLevel> level_{LogLevel::Info};
    std::mutex mutex_;
    std::ostream* stream_ = nullptr;  // Defaults to std::clog on first use.
};

// Guarded so that argument stringification is skipped entirely when the level
// is disabled -- the reason these are macros rather than functions.
#define MDE_LOG(level_value, event_name, ...)                                     \
    do {                                                                          \
        auto& mde_logger = ::market_data::Logger::instance();                     \
        if (mde_logger.enabled(level_value)) {                                    \
            mde_logger.log(level_value, event_name, {__VA_ARGS__});               \
        }                                                                         \
    } while (false)

#define MDE_TRACE(event_name, ...) MDE_LOG(::market_data::LogLevel::Trace, event_name, __VA_ARGS__)
#define MDE_DEBUG(event_name, ...) MDE_LOG(::market_data::LogLevel::Debug, event_name, __VA_ARGS__)
#define MDE_INFO(event_name, ...)  MDE_LOG(::market_data::LogLevel::Info,  event_name, __VA_ARGS__)
#define MDE_WARN(event_name, ...)  MDE_LOG(::market_data::LogLevel::Warn,  event_name, __VA_ARGS__)
#define MDE_ERROR(event_name, ...) MDE_LOG(::market_data::LogLevel::Error, event_name, __VA_ARGS__)

}  // namespace market_data
