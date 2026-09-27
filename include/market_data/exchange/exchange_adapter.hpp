// The venue boundary.
//
// An adapter owns every piece of exchange-specific knowledge: endpoints, wire
// formats, subscription protocol, reconnect semantics. It emits normalized
// MarketEvents and nothing else. Adding a second prediction market means adding
// one implementation of this interface, with no change downstream.
#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "market_data/market.hpp"
#include "market_data/market_event.hpp"
#include "market_data/types.hpp"

namespace market_data {

// Called on the adapter's ingest thread for every normalized event. Must not
// block: the engine's implementation pushes onto a bounded queue and returns.
using EventSink = std::function<void(MarketEvent&&)>;

// A venue-agnostic view of an adapter's counters, so the engine can fold
// transport statistics into its metrics without knowing which venue it is
// talking to.
struct AdapterMetricsView {
    std::uint64_t frames_received = 0;
    std::uint64_t bytes_received = 0;
    std::uint64_t malformed_messages = 0;
    std::uint64_t reconnects = 0;
    std::uint64_t snapshots_requested = 0;
    std::uint64_t snapshot_request_failures = 0;
};

class ExchangeAdapter {
public:
    virtual ~ExchangeAdapter() = default;

    ExchangeAdapter() = default;
    ExchangeAdapter(const ExchangeAdapter&) = delete;
    ExchangeAdapter& operator=(const ExchangeAdapter&) = delete;

    [[nodiscard]] virtual std::string name() const = 0;

    // Must be called before connect(). The sink is invoked from the adapter's
    // own thread once run() is active.
    virtual void set_event_sink(EventSink sink) = 0;

    // Fetch current market metadata. Synchronous and safe to call from the
    // caller's thread; returns the markets discovered so the caller can load
    // them into a registry.
    virtual std::vector<Market> discover_markets() = 0;

    // Start the live feed. Non-blocking: spins up the adapter's I/O thread.
    virtual void connect() = 0;

    // Replace the subscription set. Safe to call before or after connect(); if
    // called before, the tokens are subscribed as soon as the socket opens.
    virtual void subscribe(const std::vector<TokenId>& tokens) = 0;

    // Fetch a fresh book for one token out of band and emit it as a
    // BookSnapshotEvent. This is the resync path used when a local book is
    // found to be invalid. Returns false if the request failed.
    virtual bool request_snapshot(const TokenId& token) = 0;

    // Stop the feed and join the I/O thread. Idempotent.
    virtual void disconnect() = 0;

    [[nodiscard]] virtual bool is_connected() const = 0;

    [[nodiscard]] virtual AdapterMetricsView adapter_metrics() const = 0;
};

}  // namespace market_data
