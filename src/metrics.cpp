#include "market_data/metrics.hpp"

#include <iomanip>
#include <iterator>
#include <sstream>

#include "market_data/time_util.hpp"

namespace market_data {
namespace {

constexpr auto kRelaxed = std::memory_order_relaxed;

std::string with_thousands(std::uint64_t value) {
    std::string digits = std::to_string(value);
    std::string out;
    out.reserve(digits.size() + digits.size() / 3);
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
    static constexpr const char* kUnits[] = {"B", "KiB", "MiB", "GiB", "TiB"};
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

std::string human_duration(double seconds) {
    const auto total = static_cast<std::uint64_t>(seconds);
    std::ostringstream out;
    out << total / 3600 << "h " << (total % 3600) / 60 << "m " << total % 60 << 's';
    return out.str();
}

}  // namespace

Metrics::Metrics() : started_at_(now_receive()) {}

void Metrics::set_markets_discovered(std::uint64_t value) { markets_discovered_.store(value, kRelaxed); }
void Metrics::set_tokens_subscribed(std::uint64_t value) { tokens_subscribed_.store(value, kRelaxed); }

void Metrics::set_queue_depth(std::uint64_t depth, std::uint64_t high_water) {
    queue_depth_.store(depth, kRelaxed);
    queue_high_water_.store(high_water, kRelaxed);
}

void Metrics::set_book_health_counts(std::uint64_t healthy, std::uint64_t stale,
                                     std::uint64_t invalid) {
    books_healthy_.store(healthy, kRelaxed);
    books_stale_.store(stale, kRelaxed);
    books_invalid_.store(invalid, kRelaxed);
}

void Metrics::on_message_received() { messages_received_.fetch_add(1, kRelaxed); }
void Metrics::on_book_snapshot() { book_snapshots_.fetch_add(1, kRelaxed); }
void Metrics::on_book_update(std::uint64_t count) { book_updates_.fetch_add(count, kRelaxed); }
void Metrics::on_trade() { trades_.fetch_add(1, kRelaxed); }

void Metrics::set_persistence_counters(std::uint64_t events_persisted,
                                       std::uint64_t bytes_written,
                                       std::uint64_t events_dropped) {
    events_persisted_.store(events_persisted, kRelaxed);
    bytes_written_.store(bytes_written, kRelaxed);
    events_dropped_.store(events_dropped, kRelaxed);
}

void Metrics::set_storage_queue_depth(std::uint64_t depth, std::uint64_t high_water) {
    storage_queue_depth_.store(depth, kRelaxed);
    storage_queue_high_water_.store(high_water, kRelaxed);
}

void Metrics::on_snapshot_written() { snapshots_written_.fetch_add(1, kRelaxed); }
void Metrics::on_events_dropped(std::uint64_t count) { events_dropped_.fetch_add(count, kRelaxed); }
void Metrics::on_invalid_message(std::uint64_t count) { invalid_messages_.fetch_add(count, kRelaxed); }
void Metrics::on_crossed_book() { crossed_books_.fetch_add(1, kRelaxed); }
void Metrics::on_resync_started() { resyncs_started_.fetch_add(1, kRelaxed); }
void Metrics::on_resync_completed() { resyncs_completed_.fetch_add(1, kRelaxed); }
void Metrics::set_transport_counters(std::uint64_t bytes_received,
                                     std::uint64_t malformed_messages,
                                     std::uint64_t reconnects) {
    bytes_received_.store(bytes_received, kRelaxed);
    invalid_messages_.store(malformed_messages, kRelaxed);
    reconnects_.store(reconnects, kRelaxed);
}

MetricsSnapshot Metrics::snapshot() const {
    MetricsSnapshot s;
    s.markets_discovered = markets_discovered_.load(kRelaxed);
    s.tokens_subscribed = tokens_subscribed_.load(kRelaxed);
    s.messages_received = messages_received_.load(kRelaxed);
    s.book_snapshots = book_snapshots_.load(kRelaxed);
    s.book_updates = book_updates_.load(kRelaxed);
    s.trades = trades_.load(kRelaxed);
    s.events_persisted = events_persisted_.load(kRelaxed);
    s.snapshots_written = snapshots_written_.load(kRelaxed);
    s.queue_depth = queue_depth_.load(kRelaxed);
    s.queue_high_water = queue_high_water_.load(kRelaxed);
    s.storage_queue_depth = storage_queue_depth_.load(kRelaxed);
    s.storage_queue_high_water = storage_queue_high_water_.load(kRelaxed);
    s.events_dropped = events_dropped_.load(kRelaxed);
    s.reconnects = reconnects_.load(kRelaxed);
    s.invalid_messages = invalid_messages_.load(kRelaxed);
    s.crossed_books = crossed_books_.load(kRelaxed);
    s.resyncs_started = resyncs_started_.load(kRelaxed);
    s.resyncs_completed = resyncs_completed_.load(kRelaxed);
    s.bytes_written = bytes_written_.load(kRelaxed);
    s.bytes_received = bytes_received_.load(kRelaxed);
    s.books_healthy = books_healthy_.load(kRelaxed);
    s.books_stale = books_stale_.load(kRelaxed);
    s.books_invalid = books_invalid_.load(kRelaxed);
    s.uptime_seconds =
        std::chrono::duration<double>(now_receive() - started_at_).count();
    return s;
}

MetricsRates compute_rates(const MetricsSnapshot& previous, const MetricsSnapshot& current) {
    MetricsRates rates;
    rates.interval_seconds = current.uptime_seconds - previous.uptime_seconds;
    if (rates.interval_seconds <= 0.0) {
        return rates;
    }
    const auto per_sec = [&rates](std::uint64_t now, std::uint64_t before) {
        // Counters are monotonic, but guard anyway: a wrapped or reset counter
        // must not produce a negative rate in the status line.
        return now >= before ? static_cast<double>(now - before) / rates.interval_seconds : 0.0;
    };
    rates.messages_per_sec = per_sec(current.messages_received, previous.messages_received);
    rates.book_updates_per_sec = per_sec(current.book_updates, previous.book_updates);
    rates.trades_per_sec = per_sec(current.trades, previous.trades);
    rates.events_persisted_per_sec = per_sec(current.events_persisted, previous.events_persisted);
    return rates;
}

std::string Metrics::format_status(const MetricsSnapshot& s, const MetricsRates& r) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(0);
    out << "messages/sec: " << r.messages_per_sec
        << "   updates/sec: " << r.book_updates_per_sec
        << "   trades/sec: " << std::setprecision(1) << r.trades_per_sec << '\n'
        << std::setprecision(0)
        << "queue depth: " << s.queue_depth << " (high-water " << s.queue_high_water
        << ")   storage queue: " << s.storage_queue_depth << " (high-water "
        << s.storage_queue_high_water << ")\n"
        << "raw events persisted: " << with_thousands(s.events_persisted)
        << "   dropped: " << s.events_dropped << '\n'
        << "books: " << s.books_healthy << " healthy, " << s.books_stale << " stale, "
        << s.books_invalid << " unhealthy"
        << "   reconnects: " << s.reconnects
        << "   resyncs: " << s.resyncs_completed << "/" << s.resyncs_started << '\n'
        << "invalid messages: " << s.invalid_messages
        << "   crossed books: " << s.crossed_books
        << "   written: " << human_bytes(s.bytes_written)
        << "   received: " << human_bytes(s.bytes_received)
        << "   uptime: " << human_duration(s.uptime_seconds);
    return out.str();
}

}  // namespace market_data
