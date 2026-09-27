// Polymarket wire-format parsing.
//
// Deliberately separated from PolymarketAdapter and kept free of I/O: these are
// pure functions over strings, so every quirk of the venue's encoding can be
// pinned down by a unit test against a captured fixture rather than against a
// live socket.
//
// Verified against the live venue on 2026-09-27:
//   * Gamma encodes `clobTokenIds` and `outcomes` as JSON *strings* that
//     themselves contain JSON arrays, so they need a second parse.
//   * Gamma exposes no `category` field; the grouping lives on events[].ticker.
//   * The market websocket may deliver a bare object OR a JSON array of events
//     in a single frame -- initial book snapshots arrive batched.
//   * The websocket answers an application-level "PING" with a raw, non-JSON
//     "PONG" text frame.
//   * Live `book` frames carry `tick_size` and `last_trade_price`, neither of
//     which is in the published schema.
#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "market_data/market.hpp"
#include "market_data/market_event.hpp"
#include "market_data/types.hpp"

namespace market_data::polymarket {

inline constexpr const char* kExchangeName = "polymarket";

// Outcome of parsing one wire payload. Malformed input yields diagnostics
// rather than an exception: a bad frame must never take the process down.
struct ParseOutcome {
    std::vector<MarketEvent> events;
    // One entry per message (or sub-message) that could not be understood.
    std::vector<std::string> errors;
    // Frames that are valid protocol but carry no market data, e.g. "PONG".
    bool was_control_frame = false;

    [[nodiscard]] bool has_errors() const { return !errors.empty(); }
};

// Parses a Gamma `/markets` response body (a JSON array of market objects).
// Markets that fail to parse are skipped and reported in `errors`; the ones
// that parsed are still returned.
std::vector<Market> parse_gamma_markets(std::string_view body,
                                        ExchangeTime fetched_at,
                                        std::vector<std::string>* errors = nullptr);

// Parses a CLOB `/book?token_id=...` response into a snapshot event. Returns
// false and fills `error` if the body is not a usable book.
bool parse_clob_book(std::string_view body,
                     ReceiveTime receive_time,
                     BookSnapshotEvent& out,
                     std::string* error = nullptr);

// Parses one websocket text frame. Handles both the bare-object and
// JSON-array framings, and the raw "PONG" control frame.
ParseOutcome parse_ws_frame(std::string_view frame, ReceiveTime receive_time);

// Exposed for testing: Gamma double-encodes string arrays.
std::vector<std::string> parse_embedded_json_string_array(std::string_view encoded);

}  // namespace market_data::polymarket
