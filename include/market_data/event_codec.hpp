// Serialization of normalized events to and from JSON Lines.
//
// This is the contract that makes replay meaningful: the live engine writes
// events through encode_event(), the replay tool reads them through
// decode_event(), and both then feed the *same* OrderBookManager. If a field
// is not encoded here, replay cannot reconstruct it, so the round trip is
// covered by a test over every event type.
#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "market_data/market_event.hpp"

namespace market_data {

// One self-describing JSON object per event.
nlohmann::json encode_event(const MarketEvent& event);

// Serialized form: compact JSON with a trailing newline, ready to append.
std::string encode_event_line(const MarketEvent& event);

// Returns nullopt if the line is not a well-formed event; `error` is filled
// with the reason. A corrupt line must never abort a replay.
std::optional<MarketEvent> decode_event(const nlohmann::json& node, std::string* error = nullptr);
std::optional<MarketEvent> decode_event_line(std::string_view line, std::string* error = nullptr);

}  // namespace market_data
