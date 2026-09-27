#include "market_data/engine.hpp"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <filesystem>
#include <fstream>

#include "market_data/logging.hpp"
#include "market_data/replay.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

// Drained per wake-up. Large enough to amortize the mutex at high message
// rates, small enough that shutdown never waits on a huge batch.
constexpr std::size_t kConsumerBatchSize = 1024;
constexpr auto kTimerTick = std::chrono::milliseconds{100};

std::string format_price(double value) {
    if (std::isnan(value) || value <= 0.0) {
        return "-";
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(3) << value;
    return out.str();
}

std::string shorten(const std::string& text, std::size_t width) {
    if (text.size() <= width) {
        return text;
    }
    return text.substr(0, width - 3) + "...";
}

}  // namespace

Engine::Engine(Config config, std::unique_ptr<ExchangeAdapter> adapter)
    : config_(std::move(config)),
      adapter_(std::move(adapter)),
      book_manager_(config_),
      queue_(config_.event_queue_capacity,
             config_.block_when_queue_full ? OverflowPolicy::Block
                                           : OverflowPolicy::DropNewest),
      storage_(config_),
      csv_writer_(std::make_unique<CsvSnapshotWriter>(config_)) {}

Engine::~Engine() { shutdown(); }

bool Engine::start(std::string& error) {
    if (running_.exchange(true)) {
        return true;
    }

    if (!storage_.start(error)) {
        running_.store(false);
        return false;
    }
    if (!csv_writer_->start(error)) {
        running_.store(false);
        return false;
    }

    refresh_markets(/*initial=*/true);
    if (registry_.market_count() == 0) {
        error = "market discovery returned no markets; refusing to start blind";
        running_.store(false);
        return false;
    }

    // The sink runs on the adapter's I/O thread. Its only job is to stamp a
    // sequence number and hand the event off, so the socket is never waiting
    // on a book update or a disk write.
    adapter_->set_event_sink([this](MarketEvent&& event) {
        header_of(event).sequence = next_sequence_++;
        metrics_.on_message_received();
        if (!queue_.push(std::move(event))) {
            // Only reachable under the DropNewest policy, or after close().
            metrics_.on_events_dropped(1);
        }
    });

    book_manager_.set_resync_handler([this](const TokenId& token) {
        metrics_.on_resync_started();
        MDE_INFO("resync_started", field("token", token));
        adapter_->request_snapshot(token);
    });

    consumer_thread_ = std::thread([this] { consumer_loop(); });
    timer_thread_ = std::thread([this] { timer_loop(); });

    adapter_->connect();

    const auto tokens = registry_.top_tokens_by_volume(config_.subscribe.top_markets_by_volume);
    book_manager_.set_tracked_tokens(tokens);
    adapter_->subscribe(tokens);
    metrics_.set_tokens_subscribed(tokens.size());

    MDE_INFO("engine_started", field("markets", registry_.market_count()),
             field("tokens", tokens.size()),
             field("queue_capacity", queue_.capacity()));

    std::cout << "Connected to Polymarket market-data feed\n"
              << "Discovered: " << registry_.market_count() << " active markets\n"
              << "Subscribed: " << tokens.size() << " tokens\n";
    return true;
}

void Engine::request_stop() { stop_requested_.store(true, std::memory_order_relaxed); }

void Engine::run_until_stopped() {
    while (!stop_requested_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(kTimerTick);
    }
}

void Engine::consumer_loop() {
    std::vector<MarketEvent> batch;
    batch.reserve(kConsumerBatchSize);

    while (true) {
        const std::size_t count = queue_.pop_batch(batch, kConsumerBatchSize);
        if (count == 0) {
            // pop_batch returns empty only when the queue is closed and drained.
            break;
        }
        for (const auto& event : batch) {
            handle_event(event);
        }
        metrics_.set_queue_depth(queue_.size(), queue_.high_water_mark());
    }
    MDE_DEBUG("consumer_drained", field("events", metrics_.snapshot().events_persisted));
}

void Engine::handle_event(const MarketEvent& event) {
    // Record before applying. If the process dies between the two, the replay
    // is a superset of what the live books saw, never a subset -- and a replay
    // that is missing events it acted on would be undebuggable.
    storage_.record_raw_event(event);

    std::visit(
        [this]<typename T>(const T& typed) {
            using E = std::decay_t<T>;
            if constexpr (std::is_same_v<E, BookSnapshotEvent>) {
                metrics_.on_book_snapshot();
            } else if constexpr (std::is_same_v<E, BookUpdateEvent>) {
                metrics_.on_book_update(typed.updates.size());
            } else if constexpr (std::is_same_v<E, TradeEvent>) {
                metrics_.on_trade();
                Trade trade = typed.trade;
                // The adapter knows the token; only the registry knows which
                // outcome that token represents.
                if (const auto market = registry_.market_for_token(trade.token_id)) {
                    if (trade.market_id.empty()) {
                        trade.market_id = market->market_id;
                    }
                    trade.outcome = market->outcome_for(trade.token_id);
                }
                trade.sequence_number = typed.header.sequence;
                storage_.record_trade(trade);
            }
            // Reconnect and malformed-message counts are mirrored from the
            // adapter on the timer tick, not incremented here.
        },
        event);

    const auto crossed_before = book_manager_.total_crossed_incidents();
    book_manager_.apply(event);
    if (book_manager_.total_crossed_incidents() > crossed_before) {
        metrics_.on_crossed_book();
    }

    if (std::holds_alternative<BookSnapshotEvent>(event)) {
        const auto& token = header_of(event).token_id;
        if (const auto state = book_manager_.state(token);
            state && state->health == BookHealth::Healthy && state->resync_requests > 0) {
            metrics_.on_resync_completed();
        }
    }
}

void Engine::timer_loop() {
    auto next_snapshot = std::chrono::steady_clock::now();
    auto next_status = next_snapshot;
    auto next_flush = next_snapshot;
    auto next_refresh =
        next_snapshot + std::chrono::milliseconds{config_.market_refresh_interval_ms};

    last_status_snapshot_ = metrics_.snapshot();

    while (!stop_requested_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(kTimerTick);
        const auto now = std::chrono::steady_clock::now();

        if (now >= next_snapshot) {
            book_manager_.run_maintenance();
            write_snapshots();
            next_snapshot = now + std::chrono::milliseconds{config_.snapshot_interval_ms};
        }
        if (now >= next_flush) {
            storage_.flush();
            csv_writer_->flush();
            next_flush = now + std::chrono::milliseconds{config_.storage.raw_events.flush_interval_ms};
        }
        if (now >= next_status) {
            print_status();
            next_status = now + std::chrono::milliseconds{config_.status_interval_ms};
        }
        if (now >= next_refresh) {
            refresh_markets(/*initial=*/false);
            next_refresh = now + std::chrono::milliseconds{config_.market_refresh_interval_ms};
        }
    }
}

void Engine::refresh_markets(bool initial) {
    auto markets = adapter_->discover_markets();
    if (markets.empty()) {
        if (!initial) {
            // A failed refresh is survivable: the existing registry stays in
            // place and we keep collecting rather than dropping the feed.
            MDE_WARN("market_refresh_empty", field("keeping_markets", registry_.market_count()));
        }
        return;
    }

    const std::size_t accepted = registry_.upsert_all(std::move(markets));
    metrics_.set_markets_discovered(registry_.market_count());

    const std::string path = config_.metadata_directory() + "/markets.jsonl";
    if (!registry_.write_snapshot(path)) {
        MDE_WARN("metadata_snapshot_failed", field("path", path));
    }
    for (const auto& market : registry_.all_markets()) {
        storage_.record_market_metadata(market);
    }

    MDE_INFO("markets_refreshed", field("accepted", accepted),
             field("total", registry_.market_count()), field("initial", initial));
}

void Engine::write_snapshots() {
    const auto storage_stats = storage_.stats();
    metrics_.set_persistence_counters(storage_stats.raw_events_written,
                                      storage_stats.raw_bytes_written,
                                      storage_stats.events_dropped);
    metrics_.set_storage_queue_depth(
        storage_stats.raw_queue_depth + storage_stats.parquet_queue_depth,
        storage_stats.raw_queue_high_water + storage_stats.parquet_queue_high_water);

    const auto transport = adapter_->adapter_metrics();
    metrics_.set_transport_counters(transport.bytes_received, transport.malformed_messages,
                                    transport.reconnects);

    std::uint64_t healthy = 0;
    std::uint64_t stale = 0;
    std::uint64_t invalid = 0;
    book_manager_.health_counts(healthy, stale, invalid);
    metrics_.set_book_health_counts(healthy, stale, invalid);

    for (auto snapshot : book_manager_.build_all_snapshots()) {
        if (const auto market = registry_.market_for_token(snapshot.token_id)) {
            snapshot.outcome = market->outcome_for(snapshot.token_id);
            if (snapshot.market_id.empty()) {
                snapshot.market_id = market->market_id;
            }
        }
        storage_.record_snapshot(snapshot);
        static_cast<void>(csv_writer_->write(snapshot));
        metrics_.on_snapshot_written();
    }
}

std::string Engine::format_book_table(std::size_t max_rows) const {
    std::ostringstream out;
    out << std::left << std::setw(24) << "TOKEN" << std::setw(34) << "MARKET"
        << std::right << std::setw(9) << "BID" << std::setw(9) << "ASK"
        << std::setw(9) << "SPREAD" << std::setw(9) << "MID"
        << std::setw(9) << "IMB(L5)" << "  HEALTH\n"
        << std::string(115, '-') << '\n';

    auto states = book_manager_.all_states();
    // Most active books first, so the table shows what is actually moving.
    std::stable_sort(states.begin(), states.end(), [](const BookState& a, const BookState& b) {
        return a.updates_processed > b.updates_processed;
    });

    std::size_t rows = 0;
    for (const auto& state : states) {
        if (rows++ >= max_rows) {
            break;
        }
        const auto snapshot = book_manager_.build_snapshot(state.token_id);
        std::string question;
        if (const auto market = registry_.market_for_token(state.token_id)) {
            const auto outcome = market->outcome_for(state.token_id);
            question = shorten(market->question, 24) + (outcome.empty() ? "" : " [" + outcome + "]");
        }

        out << std::left << std::setw(24) << shorten(state.token_id, 22)
            << std::setw(34) << shorten(question, 32) << std::right;
        if (snapshot) {
            out << std::setw(9) << format_price(snapshot->best_bid)
                << std::setw(9) << format_price(snapshot->best_ask)
                << std::setw(9) << format_price(snapshot->spread)
                << std::setw(9) << format_price(snapshot->midpoint);
            if (std::isnan(snapshot->imbalance_l5)) {
                out << std::setw(9) << "-";
            } else {
                std::ostringstream imb;
                imb << std::fixed << std::setprecision(2) << snapshot->imbalance_l5;
                out << std::setw(9) << imb.str();
            }
        } else {
            out << std::setw(9) << "-" << std::setw(9) << "-" << std::setw(9) << "-"
                << std::setw(9) << "-" << std::setw(9) << "-";
        }
        out << "  " << to_string(state.health) << '\n';
    }
    return out.str();
}

void Engine::print_status() {
    const auto current = metrics_.snapshot();
    const auto rates = compute_rates(last_status_snapshot_, current);
    last_status_snapshot_ = current;

    std::cout << '\n' << format_book_table(15) << '\n'
              << Metrics::format_status(current, rates) << '\n';
    std::cout.flush();
}

std::string Engine::format_shutdown_summary() const {
    const auto snapshot = metrics_.snapshot();
    std::ostringstream out;
    out << snapshot.events_persisted << " events persisted\n"
        << snapshot.events_dropped << " events dropped\n"
        << snapshot.snapshots_written << " snapshots generated\n"
        << "queue high-water mark: " << snapshot.queue_high_water << '/' << queue_.capacity()
        << '\n'
        << "reconnects: " << snapshot.reconnects
        << ", resyncs: " << snapshot.resyncs_completed << '/' << snapshot.resyncs_started
        << ", crossed books: " << snapshot.crossed_books
        << ", invalid messages: " << snapshot.invalid_messages << '\n';
    if (snapshot.events_dropped > 0) {
        out << "WARNING: this session has gaps; " << snapshot.events_dropped
            << " events were not recorded\n";
    }
    const auto persisted = storage_.stats().book_snapshots_written;
    if (persisted != snapshot.snapshots_written) {
        // Generated and persisted are counted by different subsystems on
        // different threads. They must agree; if they do not, the dataset is
        // missing rows and the discrepancy is the evidence.
        out << "WARNING: " << snapshot.snapshots_written << " snapshots generated but "
            << persisted << " persisted; the dataset is incomplete\n";
    }
    return out.str();
}

void Engine::shutdown() {
    if (!running_.exchange(false)) {
        return;
    }
    stop_requested_.store(true, std::memory_order_relaxed);

    std::cout << "\nShutdown requested\n";

    // 1. Stop accepting new network events. The adapter joins its I/O thread,
    //    so no further push() can happen after this returns.
    MDE_INFO("shutdown_disconnecting", {});
    adapter_->disconnect();

    if (timer_thread_.joinable()) {
        timer_thread_.join();
    }

    // 2. Drain whatever is already queued. close() wakes the consumer but does
    //    not discard: pop_batch keeps returning until the queue is empty.
    std::cout << "Draining event queue... (" << queue_.size() << " events)\n";
    queue_.close();
    if (consumer_thread_.joinable()) {
        consumer_thread_.join();
    }

    // 3. Final snapshots, then flush and close every file.
    std::cout << "Flushing recorder...\n";
    write_snapshots();
    csv_writer_->flush();
    csv_writer_->stop();

    // Storage shuts down last of the data paths: it drains its own queues,
    // finalizes the Parquet partitions and checkpoints DuckDB.
    std::cout << "Flushing storage layers...\n";
    storage_.shutdown();

    const std::string path = config_.metadata_directory() + "/markets.jsonl";
    static_cast<void>(registry_.write_snapshot(path));

    // The final-state digest is what makes Milestone 7 checkable: replaying
    // this session's raw log must reproduce this exact digest.
    const auto digest = snapshot_digest(book_manager_.build_all_snapshots());
    const auto digest_path = std::filesystem::path{config_.storage.root_directory} / "final_state.digest";
    if (std::ofstream out(digest_path, std::ios::binary | std::ios::trunc); out) {
        out << digest;
        std::cout << "final-state digest written to " << digest_path.string() << '\n';
    }

    std::cout << format_shutdown_summary() << storage_.format_summary()
              << "\nShutdown complete\n";
    MDE_INFO("shutdown_complete", {});
}

}  // namespace market_data
