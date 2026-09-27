#include "market_data/exchange/polymarket_parser.hpp"

#include <algorithm>
#include <cstdlib>
#include <optional>

#include <nlohmann/json.hpp>

#include "market_data/time_util.hpp"

namespace market_data::polymarket {
namespace {

using nlohmann::json;

// Gamma returns most numbers as numbers but some as strings, and the CLOB
// returns every price and size as a string. One tolerant accessor keeps the
// call sites readable.
std::optional<double> as_double(const json& node) {
    if (node.is_number()) {
        return node.get<double>();
    }
    if (node.is_string()) {
        const std::string& text = node.get_ref<const std::string&>();
        if (text.empty()) {
            return std::nullopt;
        }
        char* end = nullptr;
        const double value = std::strtod(text.c_str(), &end);
        if (end == text.c_str() || *end != '\0') {
            return std::nullopt;
        }
        return value;
    }
    return std::nullopt;
}

double double_or(const json& parent, const char* key, double fallback) {
    const auto it = parent.find(key);
    if (it == parent.end()) {
        return fallback;
    }
    return as_double(*it).value_or(fallback);
}

std::string string_or(const json& parent, const char* key, std::string fallback = {}) {
    const auto it = parent.find(key);
    if (it == parent.end() || !it->is_string()) {
        return fallback;
    }
    return it->get<std::string>();
}

bool bool_or(const json& parent, const char* key, bool fallback) {
    const auto it = parent.find(key);
    if (it == parent.end() || !it->is_boolean()) {
        return fallback;
    }
    return it->get<bool>();
}

// The venue stamps events with epoch milliseconds, as a string.
ExchangeTime timestamp_or_now(const json& parent, const char* key = "timestamp") {
    const auto it = parent.find(key);
    if (it != parent.end()) {
        if (const auto ms = as_double(*it); ms && *ms > 0.0) {
            return from_epoch_millis(static_cast<std::int64_t>(*ms));
        }
    }
    return now_exchange();
}

// Polymarket labels a price_change by the side of the resting order: BUY is a
// bid level, SELL an ask level.
std::optional<Side> parse_side(std::string_view text) {
    if (text == "BUY" || text == "buy" || text == "BID" || text == "bid") {
        return Side::Bid;
    }
    if (text == "SELL" || text == "sell" || text == "ASK" || text == "ask") {
        return Side::Ask;
    }
    return std::nullopt;
}

std::vector<PriceLevel> parse_levels(const json& node) {
    std::vector<PriceLevel> levels;
    if (!node.is_array()) {
        return levels;
    }
    levels.reserve(node.size());
    for (const auto& entry : node) {
        if (!entry.is_object()) {
            continue;
        }
        const auto price = as_double(entry.value("price", json{}));
        const auto size = as_double(entry.value("size", json{}));
        if (!price || !size) {
            continue;
        }
        levels.push_back(PriceLevel{*price, *size});
    }
    return levels;
}

EventHeader make_header(const json& node, ReceiveTime receive_time, TokenId token) {
    EventHeader header;
    header.exchange = kExchangeName;
    header.market_id = string_or(node, "market");
    header.token_id = std::move(token);
    header.exchange_timestamp = timestamp_or_now(node);
    header.receive_timestamp = receive_time;
    header.venue_hash = string_or(node, "hash");
    return header;
}

bool parse_book_object(const json& node,
                       ReceiveTime receive_time,
                       BookSnapshotEvent& out,
                       std::string* error) {
    if (!node.is_object()) {
        if (error != nullptr) *error = "book payload is not an object";
        return false;
    }
    auto token = string_or(node, "asset_id");
    if (token.empty()) {
        if (error != nullptr) *error = "book payload has no asset_id";
        return false;
    }
    const bool has_sides = node.contains("bids") || node.contains("asks");
    if (!has_sides) {
        if (error != nullptr) *error = "book payload has neither bids nor asks";
        return false;
    }

    out = BookSnapshotEvent{};
    out.header = make_header(node, receive_time, std::move(token));
    out.bids = parse_levels(node.value("bids", json::array()));
    out.asks = parse_levels(node.value("asks", json::array()));
    out.tick_size = double_or(node, "tick_size", 0.0);
    return true;
}

// price_change frames batch several level replacements; the token is on each
// entry, so one frame can legitimately touch both outcomes of a market.
void parse_price_change(const json& node,
                        ReceiveTime receive_time,
                        ParseOutcome& outcome) {
    const auto changes = node.find("price_changes");
    if (changes == node.end() || !changes->is_array()) {
        outcome.errors.emplace_back("price_change frame has no price_changes array");
        return;
    }

    const MarketId market_id = string_or(node, "market");
    const ExchangeTime exchange_ts = timestamp_or_now(node);

    // Group by token so each emitted event covers exactly one book, which is
    // what the book manager wants to dispatch on.
    std::vector<std::pair<TokenId, BookUpdateEvent>> by_token;

    for (const auto& change : *changes) {
        if (!change.is_object()) {
            outcome.errors.emplace_back("price_change entry is not an object");
            continue;
        }
        auto token = string_or(change, "asset_id");
        const auto side = parse_side(string_or(change, "side"));
        const auto price = as_double(change.value("price", json{}));
        const auto size = as_double(change.value("size", json{}));

        if (token.empty() || !side || !price || !size) {
            outcome.errors.emplace_back("price_change entry missing asset_id/side/price/size");
            continue;
        }

        auto it = std::find_if(by_token.begin(), by_token.end(),
                               [&token](const auto& pair) { return pair.first == token; });
        if (it == by_token.end()) {
            BookUpdateEvent event;
            event.header.exchange = kExchangeName;
            event.header.market_id = market_id;
            event.header.token_id = token;
            event.header.exchange_timestamp = exchange_ts;
            event.header.receive_timestamp = receive_time;
            event.header.venue_hash = string_or(change, "hash");
            by_token.emplace_back(token, std::move(event));
            it = std::prev(by_token.end());
        }

        BookUpdate update;
        update.token_id = token;
        update.side = *side;
        update.price = *price;
        update.quantity = *size;
        update.exchange_timestamp = exchange_ts;
        update.receive_timestamp = receive_time;
        it->second.updates.push_back(std::move(update));
    }

    for (auto& [token, event] : by_token) {
        static_cast<void>(token);
        if (!event.updates.empty()) {
            outcome.events.emplace_back(std::move(event));
        }
    }
}

void parse_last_trade_price(const json& node,
                            ReceiveTime receive_time,
                            ParseOutcome& outcome) {
    auto token = string_or(node, "asset_id");
    const auto price = as_double(node.value("price", json{}));
    const auto size = as_double(node.value("size", json{}));
    if (token.empty() || !price || !size) {
        outcome.errors.emplace_back("last_trade_price frame missing asset_id/price/size");
        return;
    }

    TradeEvent event;
    event.header = make_header(node, receive_time, token);
    event.trade.token_id = std::move(token);
    event.trade.market_id = event.header.market_id;
    event.trade.price = *price;
    event.trade.quantity = *size;
    // The taker's direction, only when the venue actually states it. Left
    // absent otherwise: guessing would corrupt trade-flow research.
    event.trade.aggressor_side = parse_side(string_or(node, "side"));
    event.trade.trade_id = string_or(node, "transaction_hash");
    event.trade.exchange_timestamp = event.header.exchange_timestamp;
    event.trade.receive_timestamp = receive_time;
    outcome.events.emplace_back(std::move(event));
}

// A tick_size_change means the venue re-gridded the book. The old local book is
// not trustworthy afterwards, so this surfaces as a metadata event that drives
// a resync rather than being silently absorbed.
void parse_tick_size_change(const json& node,
                            ReceiveTime receive_time,
                            ParseOutcome& outcome) {
    auto token = string_or(node, "asset_id");
    if (token.empty()) {
        outcome.errors.emplace_back("tick_size_change frame has no asset_id");
        return;
    }
    MarketMetadataEvent event;
    event.header = make_header(node, receive_time, std::move(token));
    event.tick_size = double_or(node, "new_tick_size", 0.0);
    event.active = true;
    outcome.events.emplace_back(std::move(event));
}

void parse_one_ws_object(const json& node, ReceiveTime receive_time, ParseOutcome& outcome) {
    if (!node.is_object()) {
        outcome.errors.emplace_back("websocket element is not an object");
        return;
    }
    const std::string type = string_or(node, "event_type");

    if (type == "book") {
        BookSnapshotEvent event;
        std::string error;
        if (parse_book_object(node, receive_time, event, &error)) {
            outcome.events.emplace_back(std::move(event));
        } else {
            outcome.errors.push_back("book frame: " + error);
        }
    } else if (type == "price_change") {
        parse_price_change(node, receive_time, outcome);
    } else if (type == "last_trade_price") {
        parse_last_trade_price(node, receive_time, outcome);
    } else if (type == "tick_size_change") {
        parse_tick_size_change(node, receive_time, outcome);
    } else if (type.empty()) {
        outcome.errors.emplace_back("websocket frame has no event_type");
    } else {
        // Unknown but well-formed: the venue may add event types at any time,
        // and refusing to run against a newer venue would be worse than
        // ignoring what we do not model yet.
        outcome.errors.push_back("unhandled event_type: " + type);
    }
}

}  // namespace

std::vector<std::string> parse_embedded_json_string_array(std::string_view encoded) {
    std::vector<std::string> out;
    if (encoded.empty()) {
        return out;
    }
    const json parsed = json::parse(encoded, nullptr, false);
    if (parsed.is_discarded() || !parsed.is_array()) {
        return out;
    }
    out.reserve(parsed.size());
    for (const auto& entry : parsed) {
        if (entry.is_string()) {
            out.push_back(entry.get<std::string>());
        }
    }
    return out;
}

std::vector<Market> parse_gamma_markets(std::string_view body,
                                        ExchangeTime fetched_at,
                                        std::vector<std::string>* errors) {
    std::vector<Market> markets;

    const json root = json::parse(body, nullptr, false);
    if (root.is_discarded()) {
        if (errors != nullptr) errors->emplace_back("gamma response is not valid JSON");
        return markets;
    }
    // Gamma returns a bare array today, but has shipped {"data": [...]} on
    // sibling endpoints; accept either rather than break on a wrapper.
    const json* array = &root;
    if (root.is_object()) {
        const auto data = root.find("data");
        if (data == root.end() || !data->is_array()) {
            if (errors != nullptr) errors->emplace_back("gamma response object has no data array");
            return markets;
        }
        array = &*data;
    }
    if (!array->is_array()) {
        if (errors != nullptr) errors->emplace_back("gamma response is not an array");
        return markets;
    }

    markets.reserve(array->size());
    for (const auto& node : *array) {
        if (!node.is_object()) {
            if (errors != nullptr) errors->emplace_back("gamma entry is not an object");
            continue;
        }

        Market market;
        market.market_id = string_or(node, "conditionId");
        if (market.market_id.empty()) {
            if (errors != nullptr) errors->emplace_back("gamma market has no conditionId");
            continue;
        }

        market.question = string_or(node, "question");
        market.slug = string_or(node, "slug");
        market.token_ids = parse_embedded_json_string_array(string_or(node, "clobTokenIds"));
        market.outcomes = parse_embedded_json_string_array(string_or(node, "outcomes"));

        if (market.token_ids.empty()) {
            if (errors != nullptr) {
                errors->push_back("gamma market " + market.market_id + " has no clobTokenIds");
            }
            continue;
        }
        if (!market.outcomes.empty() && market.outcomes.size() != market.token_ids.size()) {
            if (errors != nullptr) {
                errors->push_back("gamma market " + market.market_id +
                                  " has mismatched outcomes/clobTokenIds");
            }
            continue;
        }

        // There is no category field on the market; the closest stable grouping
        // is the parent event's ticker.
        if (const auto events = node.find("events");
            events != node.end() && events->is_array() && !events->empty()) {
            const auto& first = events->front();
            if (first.is_object()) {
                market.category = string_or(first, "ticker");
                if (market.category.empty()) {
                    market.category = string_or(first, "slug");
                }
            }
        }

        market.active = bool_or(node, "active", false);
        market.closed = bool_or(node, "closed", false);
        market.accepting_orders = bool_or(node, "acceptingOrders", false);
        market.reported_volume = double_or(node, "volumeNum", double_or(node, "volume", 0.0));
        market.reported_liquidity =
            double_or(node, "liquidityNum", double_or(node, "liquidity", 0.0));
        market.reported_volume_24h = double_or(node, "volume24hr", 0.0);
        market.min_tick_size = double_or(node, "orderPriceMinTickSize", 0.0);
        market.fetched_at = fetched_at;

        if (const auto end_date = node.find("endDate");
            end_date != node.end() && end_date->is_string()) {
            market.resolution_time = parse_iso8601(end_date->get<std::string>());
        }

        markets.push_back(std::move(market));
    }

    return markets;
}

bool parse_clob_book(std::string_view body,
                     ReceiveTime receive_time,
                     BookSnapshotEvent& out,
                     std::string* error) {
    const json root = json::parse(body, nullptr, false);
    if (root.is_discarded()) {
        if (error != nullptr) *error = "clob book response is not valid JSON";
        return false;
    }
    return parse_book_object(root, receive_time, out, error);
}

ParseOutcome parse_ws_frame(std::string_view frame, ReceiveTime receive_time) {
    ParseOutcome outcome;

    // Trim whitespace so a stray newline does not look like a payload.
    const auto first = frame.find_first_not_of(" \t\r\n");
    if (first == std::string_view::npos) {
        outcome.was_control_frame = true;
        return outcome;
    }
    const auto last = frame.find_last_not_of(" \t\r\n");
    const std::string_view trimmed = frame.substr(first, last - first + 1);

    // The venue's application-level heartbeat reply is a bare, non-JSON text
    // frame. Feeding it to a JSON parser would log an error every 10 seconds.
    if (trimmed == "PONG" || trimmed == "PING") {
        outcome.was_control_frame = true;
        return outcome;
    }

    const json root = json::parse(trimmed, nullptr, false);
    if (root.is_discarded()) {
        outcome.errors.emplace_back("websocket frame is not valid JSON");
        return outcome;
    }

    if (root.is_array()) {
        // Initial book snapshots arrive batched in a single frame.
        for (const auto& element : root) {
            parse_one_ws_object(element, receive_time, outcome);
        }
    } else {
        parse_one_ws_object(root, receive_time, outcome);
    }
    return outcome;
}

}  // namespace market_data::polymarket
