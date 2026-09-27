// Polymarket implementation of ExchangeAdapter.
//
// This is the only translation unit outside polymarket_parser.cpp that knows
// what a Polymarket endpoint looks like. It owns:
//   * Gamma REST discovery, paged
//   * CLOB REST book fetches, used to resync an invalidated book
//   * the market websocket, its heartbeat, and its reconnect/resubscribe cycle
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "market_data/config.hpp"
#include "market_data/exchange/exchange_adapter.hpp"
#include "market_data/http_client.hpp"
#include "market_data/market.hpp"

namespace market_data {

// Counters the adapter owns. The engine folds these into its metrics snapshot.
struct AdapterStats {
    std::atomic<std::uint64_t> frames_received{0};
    std::atomic<std::uint64_t> bytes_received{0};
    std::atomic<std::uint64_t> malformed_messages{0};
    std::atomic<std::uint64_t> reconnects{0};
    std::atomic<std::uint64_t> snapshots_requested{0};
    std::atomic<std::uint64_t> snapshot_request_failures{0};
    std::atomic<std::uint64_t> discovery_failures{0};
};

class PolymarketAdapter final : public ExchangeAdapter {
public:
    explicit PolymarketAdapter(Config config);
    ~PolymarketAdapter() override;

    [[nodiscard]] std::string name() const override;

    void set_event_sink(EventSink sink) override;

    // Pages Gamma until it runs dry or the configured ceiling is reached.
    // Markets that fail to parse are logged and skipped, never fatal.
    std::vector<Market> discover_markets() override;

    void connect() override;
    void subscribe(const std::vector<TokenId>& tokens) override;

    // Fetches the book over REST and emits it as a BookSnapshotEvent. Called
    // from the consumer thread when a book is found invalid, so it must not
    // touch the websocket's state.
    bool request_snapshot(const TokenId& token) override;

    void disconnect() override;

    [[nodiscard]] bool is_connected() const override;

    [[nodiscard]] AdapterMetricsView adapter_metrics() const override;

    [[nodiscard]] const AdapterStats& stats() const { return stats_; }

private:
    class Session;  // Owns the websocket; defined in the .cpp.

    void emit(MarketEvent&& event);

    Config config_;
    HttpClient http_;
    // Separate client for resync so a slow book fetch on the consumer thread
    // cannot serialize behind a discovery sweep on the refresh thread.
    HttpClient resync_http_;

    mutable std::mutex sink_mutex_;
    EventSink sink_;

    // Shared rather than unique: the session hands `shared_from_this` to every
    // outstanding async operation, so it must outlive the adapter's pointer if
    // a completion handler is still in flight when we drop it.
    std::shared_ptr<Session> session_;
    // Remembered so a subscribe() that arrives before connect() still takes
    // effect once the socket opens.
    std::vector<TokenId> pending_tokens_;
    AdapterStats stats_;
};

}  // namespace market_data
