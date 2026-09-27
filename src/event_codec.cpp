#include "market_data/event_codec.hpp"

#include "market_data/time_util.hpp"

namespace market_data {
namespace {

using nlohmann::json;

json encode_levels(const std::vector<PriceLevel>& levels) {
    json out = json::array();
    for (const auto& level : levels) {
        out.push_back(json{{"price", level.price}, {"size", level.quantity}});
    }
    return out;
}

std::vector<PriceLevel> decode_levels(const json& node) {
    std::vector<PriceLevel> levels;
    if (!node.is_array()) {
        return levels;
    }
    levels.reserve(node.size());
    for (const auto& entry : node) {
        if (entry.is_object() && entry.contains("price") && entry.contains("size")) {
            levels.push_back(PriceLevel{entry["price"].get<double>(),
                                        entry["size"].get<double>()});
        }
    }
    return levels;
}

void encode_header(const EventHeader& header, json& out) {
    out["exchange"] = header.exchange;
    out["market_id"] = header.market_id;
    out["token_id"] = header.token_id;
    out["exchange_ts"] = to_epoch_millis(header.exchange_timestamp);
    // Steady-clock nanos: meaningful only relative to other events in the same
    // file, which is exactly what replay needs.
    out["receive_ts"] = to_steady_nanos(header.receive_timestamp);
    out["seq"] = header.sequence;
    if (!header.venue_hash.empty()) {
        out["hash"] = header.venue_hash;
    }
}

bool decode_header(const json& node, EventHeader& header, std::string* error) {
    if (!node.contains("token_id") || !node.contains("seq")) {
        if (error != nullptr) *error = "event is missing token_id or seq";
        return false;
    }
    header.exchange = node.value("exchange", std::string{});
    header.market_id = node.value("market_id", std::string{});
    header.token_id = node.value("token_id", std::string{});
    header.exchange_timestamp = from_epoch_millis(node.value("exchange_ts", std::int64_t{0}));
    header.receive_timestamp = from_steady_nanos(node.value("receive_ts", std::int64_t{0}));
    header.sequence = node.value("seq", std::uint64_t{0});
    header.venue_hash = node.value("hash", std::string{});
    return true;
}

Side side_from_string(const std::string& text) {
    return text == "ask" ? Side::Ask : Side::Bid;
}

// Round-trips trade_side_label(). Anything unrecognized stays unknown rather
// than collapsing to a default direction.
std::optional<Side> trade_side_from_string(const std::string& text) {
    if (text == "buy" || text == "bid") {
        return Side::Bid;
    }
    if (text == "sell" || text == "ask") {
        return Side::Ask;
    }
    return std::nullopt;
}

ConnectionState connection_state_from_string(const std::string& text) {
    if (text == "connecting") return ConnectionState::Connecting;
    if (text == "connected") return ConnectionState::Connected;
    if (text == "subscription_sent") return ConnectionState::SubscriptionSent;
    if (text == "reconnecting") return ConnectionState::Reconnecting;
    if (text == "failed") return ConnectionState::Failed;
    return ConnectionState::Disconnected;
}

}  // namespace

json encode_event(const MarketEvent& event) {
    json out;
    out["event_type"] = event_type_name(event);
    encode_header(header_of(event), out);

    std::visit(
        [&out]<typename T>(const T& typed) {
            using E = std::decay_t<T>;
            if constexpr (std::is_same_v<E, BookSnapshotEvent>) {
                out["bids"] = encode_levels(typed.bids);
                out["asks"] = encode_levels(typed.asks);
                out["tick_size"] = typed.tick_size;
            } else if constexpr (std::is_same_v<E, BookUpdateEvent>) {
                json updates = json::array();
                for (const auto& update : typed.updates) {
                    updates.push_back(json{{"side", to_string(update.side)},
                                           {"price", update.price},
                                           {"quantity", update.quantity}});
                }
                out["updates"] = std::move(updates);
            } else if constexpr (std::is_same_v<E, TradeEvent>) {
                out["price"] = typed.trade.price;
                out["quantity"] = typed.trade.quantity;
                out["aggressor_side"] = trade_side_label(typed.trade.aggressor_side);
                if (!typed.trade.trade_id.empty()) {
                    out["trade_id"] = typed.trade.trade_id;
                }
            } else if constexpr (std::is_same_v<E, MarketMetadataEvent>) {
                out["question"] = typed.question;
                out["category"] = typed.category;
                out["token_ids"] = typed.token_ids;
                out["outcomes"] = typed.outcomes;
                out["active"] = typed.active;
                out["closed"] = typed.closed;
                out["tick_size"] = typed.tick_size;
            } else {
                out["state"] = to_string(typed.state);
                out["detail"] = typed.detail;
                out["attempt"] = typed.attempt;
                out["subscribed_tokens"] = typed.subscribed_tokens;
            }
        },
        event);

    return out;
}

std::string encode_event_line(const MarketEvent& event) {
    std::string line = encode_event(event).dump();
    line.push_back('\n');
    return line;
}

std::optional<MarketEvent> decode_event(const json& node, std::string* error) {
    if (!node.is_object()) {
        if (error != nullptr) *error = "event is not a JSON object";
        return std::nullopt;
    }
    const std::string type = node.value("event_type", std::string{});

    EventHeader header;
    // Connection events legitimately carry no token, so they decode their
    // header with a relaxed rule.
    if (type == "connection") {
        header.exchange = node.value("exchange", std::string{});
        header.market_id = node.value("market_id", std::string{});
        header.token_id = node.value("token_id", std::string{});
        header.exchange_timestamp = from_epoch_millis(node.value("exchange_ts", std::int64_t{0}));
        header.receive_timestamp = from_steady_nanos(node.value("receive_ts", std::int64_t{0}));
        header.sequence = node.value("seq", std::uint64_t{0});

        ConnectionEvent event;
        event.header = std::move(header);
        event.state = connection_state_from_string(node.value("state", std::string{}));
        event.detail = node.value("detail", std::string{});
        event.attempt = node.value("attempt", std::uint32_t{0});
        event.subscribed_tokens = node.value("subscribed_tokens", std::size_t{0});
        return MarketEvent{std::move(event)};
    }

    if (!decode_header(node, header, error)) {
        return std::nullopt;
    }

    if (type == "book_snapshot") {
        BookSnapshotEvent event;
        event.header = std::move(header);
        event.bids = decode_levels(node.value("bids", json::array()));
        event.asks = decode_levels(node.value("asks", json::array()));
        event.tick_size = node.value("tick_size", 0.0);
        return MarketEvent{std::move(event)};
    }
    if (type == "book_update") {
        BookUpdateEvent event;
        const auto updates = node.find("updates");
        if (updates == node.end() || !updates->is_array()) {
            if (error != nullptr) *error = "book_update has no updates array";
            return std::nullopt;
        }
        for (const auto& entry : *updates) {
            if (!entry.is_object() || !entry.contains("price") || !entry.contains("quantity")) {
                continue;
            }
            BookUpdate update;
            update.token_id = header.token_id;
            update.side = side_from_string(entry.value("side", std::string{"bid"}));
            update.price = entry["price"].get<double>();
            update.quantity = entry["quantity"].get<double>();
            update.exchange_timestamp = header.exchange_timestamp;
            update.receive_timestamp = header.receive_timestamp;
            event.updates.push_back(std::move(update));
        }
        if (event.updates.empty()) {
            if (error != nullptr) *error = "book_update has no usable updates";
            return std::nullopt;
        }
        event.header = std::move(header);
        return MarketEvent{std::move(event)};
    }
    if (type == "trade") {
        TradeEvent event;
        event.trade.token_id = header.token_id;
        event.trade.price = node.value("price", 0.0);
        event.trade.quantity = node.value("quantity", 0.0);
        event.trade.market_id = header.market_id;
        event.trade.aggressor_side = trade_side_from_string(
            node.value("aggressor_side", std::string{"unknown"}));
        event.trade.trade_id = node.value("trade_id", std::string{});
        event.trade.sequence_number = header.sequence;
        event.trade.exchange_timestamp = header.exchange_timestamp;
        event.trade.receive_timestamp = header.receive_timestamp;
        event.header = std::move(header);
        return MarketEvent{std::move(event)};
    }
    if (type == "market_metadata") {
        MarketMetadataEvent event;
        event.header = std::move(header);
        event.question = node.value("question", std::string{});
        event.category = node.value("category", std::string{});
        event.token_ids = node.value("token_ids", std::vector<TokenId>{});
        event.outcomes = node.value("outcomes", std::vector<std::string>{});
        event.active = node.value("active", false);
        event.closed = node.value("closed", false);
        event.tick_size = node.value("tick_size", 0.0);
        return MarketEvent{std::move(event)};
    }

    if (error != nullptr) *error = "unknown event_type: " + type;
    return std::nullopt;
}

std::optional<MarketEvent> decode_event_line(std::string_view line, std::string* error) {
    const json node = json::parse(line, nullptr, false);
    if (node.is_discarded()) {
        if (error != nullptr) *error = "line is not valid JSON";
        return std::nullopt;
    }
    return decode_event(node, error);
}

}  // namespace market_data
