// The market data engine: owns the pipeline and the threads that drive it.
//
// Threading model
//
//   adapter I/O thread   socket -> parse -> normalize -> stamp seq -> queue.push
//          |
//          v
//   [ bounded event queue ]
//          |
//          v
//   consumer thread      queue.pop_batch -> OrderBookManager
//                                        -> RawRecorder
//                                        -> Metrics
//          |
//   timer thread         periodic snapshots, book maintenance, status print,
//                        recorder flush, market metadata refresh
//
// Network I/O never blocks on disk I/O: the adapter thread's only interaction
// with persistence is a push onto the in-memory queue.
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "market_data/config.hpp"
#include "market_data/event_bus.hpp"
#include "market_data/exchange/exchange_adapter.hpp"
#include "market_data/market_registry.hpp"
#include "market_data/metrics.hpp"
#include "market_data/order_book_manager.hpp"
#include "market_data/recorder.hpp"
#include "market_data/storage/storage_manager.hpp"

namespace market_data {

class Engine {
public:
    Engine(Config config, std::unique_ptr<ExchangeAdapter> adapter);
    ~Engine();

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // Discovers markets, opens output files, connects and subscribes.
    // Returns false and fills `error` if startup could not complete.
    bool start(std::string& error);

    // Blocks until stop() is called from a signal handler or another thread.
    void run_until_stopped();

    // Safe to call from a signal handler: only sets an atomic flag.
    void request_stop();

    // Ordered shutdown: stop ingest, drain, flush, close, report.
    void shutdown();

    [[nodiscard]] const Metrics& metrics() const { return metrics_; }
    [[nodiscard]] const MarketRegistry& registry() const { return registry_; }
    [[nodiscard]] const OrderBookManager& books() const { return book_manager_; }

    // Renders the live token table printed alongside the metrics block.
    [[nodiscard]] std::string format_book_table(std::size_t max_rows) const;
    [[nodiscard]] std::string format_shutdown_summary() const;

    [[nodiscard]] const StorageManager& storage() const { return storage_; }

private:
    void consumer_loop();
    void timer_loop();
    void handle_event(const MarketEvent& event);
    void refresh_markets(bool initial);
    void write_snapshots();
    void print_status();

    Config config_;
    std::unique_ptr<ExchangeAdapter> adapter_;

    MarketRegistry registry_;
    OrderBookManager book_manager_;
    Metrics metrics_;

    BoundedQueue<MarketEvent> queue_;
    // Every persistence concern -- raw archive, Parquet datasets, DuckDB
    // catalog, their queues and worker threads -- lives behind this one
    // object. The engine knows nothing about file layout.
    StorageManager storage_;
    // Legacy single-file CSV sink, off unless explicitly enabled.
    std::unique_ptr<SnapshotSink> csv_writer_;

    std::thread consumer_thread_;
    std::thread timer_thread_;

    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    // Assigned on the adapter thread only, so a plain increment is enough and
    // the resulting order is a true total order over ingested events.
    std::uint64_t next_sequence_ = 1;

    MetricsSnapshot last_status_snapshot_;
};

}  // namespace market_data
