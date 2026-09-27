#include "market_data/logging.hpp"

#include <iostream>
#include <sstream>

#include "market_data/time_util.hpp"

namespace market_data {
namespace {

// logfmt: quote only when the value would otherwise be ambiguous.
std::string escape_value(const std::string& value) {
    const bool needs_quotes =
        value.empty() || value.find_first_of(" \t\"=\n\r") != std::string::npos;
    if (!needs_quotes) {
        return value;
    }
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('"');
    for (const char c : value) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:   out.push_back(c); break;
        }
    }
    out.push_back('"');
    return out;
}

std::string to_compact_string(double value) {
    std::ostringstream out;
    out << value;
    return out.str();
}

}  // namespace

std::string_view to_string(LogLevel level) {
    switch (level) {
        case LogLevel::Trace: return "trace";
        case LogLevel::Debug: return "debug";
        case LogLevel::Info:  return "info";
        case LogLevel::Warn:  return "warn";
        case LogLevel::Error: return "error";
        case LogLevel::Off:   return "off";
    }
    return "unknown";
}

bool parse_log_level(std::string_view text, LogLevel& out) {
    if (text == "trace") { out = LogLevel::Trace; return true; }
    if (text == "debug") { out = LogLevel::Debug; return true; }
    if (text == "info")  { out = LogLevel::Info;  return true; }
    if (text == "warn" || text == "warning") { out = LogLevel::Warn; return true; }
    if (text == "error") { out = LogLevel::Error; return true; }
    if (text == "off" || text == "none") { out = LogLevel::Off; return true; }
    return false;
}

LogField field(std::string key, std::string value) { return {std::move(key), std::move(value)}; }
LogField field(std::string key, const char* value) { return {std::move(key), value ? value : ""}; }
LogField field(std::string key, std::string_view value) { return {std::move(key), std::string{value}}; }
LogField field(std::string key, bool value) { return {std::move(key), value ? "true" : "false"}; }
LogField field(std::string key, double value) { return {std::move(key), to_compact_string(value)}; }

Logger& Logger::instance() {
    static Logger logger;
    return logger;
}

void Logger::set_stream(std::ostream& stream) {
    const std::lock_guard lock(mutex_);
    stream_ = &stream;
}

void Logger::log(LogLevel level, std::string_view event, std::initializer_list<LogField> fields) {
    if (!enabled(level)) {
        return;
    }

    // Format outside the lock; hold it only for the write, so concurrent
    // producers serialize on I/O rather than on string building.
    std::ostringstream line;
    line << format_iso8601(now_exchange()) << ' ' << to_string(level) << ' ' << event;
    for (const auto& f : fields) {
        line << ' ' << f.key << '=' << escape_value(f.value);
    }
    line << '\n';

    const std::lock_guard lock(mutex_);
    std::ostream& out = (stream_ != nullptr) ? *stream_ : std::clog;
    out << line.str();
    // Errors are flushed immediately: if the process is about to die, the
    // reason must already be on disk.
    if (level >= LogLevel::Error) {
        out.flush();
    }
}

}  // namespace market_data
