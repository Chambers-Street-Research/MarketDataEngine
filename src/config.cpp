#include "market_data/config.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>

#include <nlohmann/json.hpp>

namespace market_data {
namespace {

using nlohmann::json;

template <typename T>
void read_number(const json& parent, const char* key, T& out) {
    const auto it = parent.find(key);
    if (it != parent.end() && it->is_number()) {
        out = it->get<T>();
    }
}

void read_bool(const json& parent, const char* key, bool& out) {
    const auto it = parent.find(key);
    if (it != parent.end() && it->is_boolean()) {
        out = it->get<bool>();
    }
}

void read_string(const json& parent, const char* key, std::string& out) {
    const auto it = parent.find(key);
    if (it != parent.end() && it->is_string()) {
        out = it->get<std::string>();
    }
}

std::string join_path(const std::string& base, const char* leaf) {
    return (std::filesystem::path{base} / leaf).string();
}

}  // namespace

bool Config::load_from_json(const std::string& text, Config& out, std::string& error) {
    const json root = json::parse(text, nullptr, false);
    if (root.is_discarded()) {
        error = "config is not valid JSON";
        return false;
    }
    if (!root.is_object()) {
        error = "config root must be an object";
        return false;
    }

    Config cfg;
    read_string(root, "exchange", cfg.exchange);
    // Legacy key, still honoured: folded into storage.root_directory below.
    std::string legacy_output_directory;
    read_string(root, "output_directory", legacy_output_directory);
    read_number(root, "snapshot_interval_ms", cfg.snapshot_interval_ms);
    read_number(root, "order_book_depth", cfg.order_book_depth);
    read_number(root, "market_refresh_interval_ms", cfg.market_refresh_interval_ms);
    read_number(root, "status_interval_ms", cfg.status_interval_ms);
    read_number(root, "event_queue_capacity", cfg.event_queue_capacity);
    read_bool(root, "block_when_queue_full", cfg.block_when_queue_full);

    if (const auto it = root.find("log_level"); it != root.end() && it->is_string()) {
        if (!parse_log_level(it->get<std::string>(), cfg.log_level)) {
            error = "unknown log_level: " + it->get<std::string>();
            return false;
        }
    }

    if (const auto it = root.find("endpoints"); it != root.end() && it->is_object()) {
        read_string(*it, "gamma_base_url", cfg.polymarket.gamma_base_url);
        read_string(*it, "clob_base_url", cfg.polymarket.clob_base_url);
        read_string(*it, "market_ws_url", cfg.polymarket.market_ws_url);
    }

    if (const auto it = root.find("subscribe"); it != root.end() && it->is_object()) {
        read_number(*it, "top_markets_by_volume", cfg.subscribe.top_markets_by_volume);
        read_number(*it, "max_tokens", cfg.subscribe.max_tokens);
        read_number(*it, "min_reported_liquidity", cfg.subscribe.min_reported_liquidity);
    }

    if (const auto it = root.find("reconnect"); it != root.end() && it->is_object()) {
        read_number(*it, "initial_backoff_ms", cfg.reconnect.initial_backoff_ms);
        read_number(*it, "max_backoff_ms", cfg.reconnect.max_backoff_ms);
        read_number(*it, "backoff_multiplier", cfg.reconnect.backoff_multiplier);
        read_number(*it, "jitter_fraction", cfg.reconnect.jitter_fraction);
        read_number(*it, "max_attempts", cfg.reconnect.max_attempts);
    }

    if (const auto it = root.find("health"); it != root.end() && it->is_object()) {
        read_number(*it, "stale_after_ms", cfg.health.stale_after_ms);
        read_number(*it, "ping_interval_ms", cfg.health.ping_interval_ms);
        read_number(*it, "read_timeout_ms", cfg.health.read_timeout_ms);
        read_number(*it, "resync_cooldown_ms", cfg.health.resync_cooldown_ms);
    }

    if (const auto it = root.find("storage"); it != root.end() && it->is_object()) {
        read_string(*it, "root_directory", cfg.storage.root_directory);
        read_bool(*it, "write_csv_snapshots", cfg.storage.write_csv_snapshots);
        read_number(*it, "raw_queue_capacity", cfg.storage.raw_queue_capacity);
        read_number(*it, "parquet_queue_capacity", cfg.storage.parquet_queue_capacity);
        read_bool(*it, "block_when_queue_full", cfg.storage.block_when_queue_full);
        read_number(*it, "queue_warn_fraction", cfg.storage.queue_warn_fraction);

        if (const auto raw = it->find("raw_events"); raw != it->end() && raw->is_object()) {
            read_bool(*raw, "enabled", cfg.storage.raw_events.enabled);
            read_string(*raw, "format", cfg.storage.raw_events.format);
            read_string(*raw, "compression", cfg.storage.raw_events.compression);
            read_number(*raw, "compression_level", cfg.storage.raw_events.compression_level);
            read_number(*raw, "rotation_minutes", cfg.storage.raw_events.rotation_minutes);
            read_number(*raw, "max_file_bytes", cfg.storage.raw_events.max_file_bytes);
            read_number(*raw, "flush_interval_ms", cfg.storage.raw_events.flush_interval_ms);
        }
        if (const auto pq = it->find("parquet"); pq != it->end() && pq->is_object()) {
            read_bool(*pq, "enabled", cfg.storage.parquet.enabled);
            read_string(*pq, "compression", cfg.storage.parquet.compression);
            read_number(*pq, "compression_level", cfg.storage.parquet.compression_level);
            read_number(*pq, "batch_rows", cfg.storage.parquet.batch_rows);
            read_number(*pq, "flush_interval_seconds", cfg.storage.parquet.flush_interval_seconds);
            read_bool(*pq, "partition_by_hour", cfg.storage.parquet.partition_by_hour);
            // The snapshot cadence lives in two places for backwards
            // compatibility; the storage block wins and drives the timer too,
            // so the two can never silently disagree.
            if (const auto interval = pq->find("snapshot_interval_ms");
                interval != pq->end() && interval->is_number()) {
                cfg.storage.parquet.snapshot_interval_ms = interval->get<std::uint32_t>();
                cfg.snapshot_interval_ms = cfg.storage.parquet.snapshot_interval_ms;
            } else {
                cfg.storage.parquet.snapshot_interval_ms = cfg.snapshot_interval_ms;
            }
        } else {
            cfg.storage.parquet.snapshot_interval_ms = cfg.snapshot_interval_ms;
        }
        if (const auto db = it->find("duckdb"); db != it->end() && db->is_object()) {
            read_bool(*db, "enabled", cfg.storage.duckdb.enabled);
            read_string(*db, "path", cfg.storage.duckdb.path);
            read_bool(*db, "create_views", cfg.storage.duckdb.create_views);
        }
    } else {
        cfg.storage.parquet.snapshot_interval_ms = cfg.snapshot_interval_ms;
    }

    // One effective root. An explicit storage.root_directory wins; otherwise
    // the legacy top-level output_directory is adopted, so older configs keep
    // working unchanged.
    if (!legacy_output_directory.empty() &&
        cfg.storage.root_directory == Config{}.storage.root_directory) {
        cfg.storage.root_directory = legacy_output_directory;
    }

    if (!cfg.validate(error)) {
        return false;
    }
    out = std::move(cfg);
    return true;
}

bool Config::load_from_file(const std::string& path, Config& out, std::string& error) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        error = "cannot open config file: " + path;
        return false;
    }
    std::ostringstream buffer;
    buffer << in.rdbuf();
    return load_from_json(buffer.str(), out, error);
}

bool Config::validate(std::string& error) const {
    if (exchange != "polymarket") {
        error = "unsupported exchange: " + exchange;
        return false;
    }

    if (snapshot_interval_ms == 0) {
        error = "snapshot_interval_ms must be greater than zero";
        return false;
    }
    if (order_book_depth == 0) {
        error = "order_book_depth must be greater than zero";
        return false;
    }
    if (event_queue_capacity == 0) {
        error = "event_queue_capacity must be greater than zero";
        return false;
    }
    if (subscribe.top_markets_by_volume == 0) {
        error = "subscribe.top_markets_by_volume must be greater than zero";
        return false;
    }
    if (reconnect.initial_backoff_ms == 0) {
        error = "reconnect.initial_backoff_ms must be greater than zero";
        return false;
    }
    if (reconnect.max_backoff_ms < reconnect.initial_backoff_ms) {
        error = "reconnect.max_backoff_ms must be at least initial_backoff_ms";
        return false;
    }
    if (reconnect.backoff_multiplier < 1.0) {
        error = "reconnect.backoff_multiplier must be at least 1.0";
        return false;
    }
    if (reconnect.jitter_fraction < 0.0 || reconnect.jitter_fraction > 1.0) {
        error = "reconnect.jitter_fraction must be within [0, 1]";
        return false;
    }
    if (health.ping_interval_ms == 0) {
        error = "health.ping_interval_ms must be greater than zero";
        return false;
    }
    // A read timeout at or below the heartbeat interval would tear down a
    // perfectly healthy but quiet connection on every cycle.
    if (health.read_timeout_ms <= health.ping_interval_ms) {
        error = "health.read_timeout_ms must exceed health.ping_interval_ms";
        return false;
    }
    if (storage.root_directory.empty()) {
        error = "storage.root_directory must not be empty";
        return false;
    }
    if (storage.raw_events.format != "jsonl") {
        error = "storage.raw_events.format must be \"jsonl\"";
        return false;
    }
    if (storage.raw_events.compression != "zstd" && storage.raw_events.compression != "none") {
        error = "storage.raw_events.compression must be \"zstd\" or \"none\"";
        return false;
    }
    if (storage.raw_events.rotation_minutes == 0) {
        error = "storage.raw_events.rotation_minutes must be greater than zero";
        return false;
    }
    if (storage.raw_events.flush_interval_ms == 0) {
        error = "storage.raw_events.flush_interval_ms must be greater than zero";
        return false;
    }
    if (storage.parquet.compression != "zstd" && storage.parquet.compression != "snappy" &&
        storage.parquet.compression != "gzip" && storage.parquet.compression != "none") {
        error = "storage.parquet.compression must be zstd, snappy, gzip or none";
        return false;
    }
    if (storage.parquet.batch_rows == 0) {
        error = "storage.parquet.batch_rows must be greater than zero";
        return false;
    }
    if (storage.parquet.flush_interval_seconds == 0) {
        error = "storage.parquet.flush_interval_seconds must be greater than zero";
        return false;
    }
    if (storage.parquet.snapshot_interval_ms == 0) {
        error = "storage.parquet.snapshot_interval_ms must be greater than zero";
        return false;
    }
    if (storage.duckdb.enabled && storage.duckdb.path.empty()) {
        error = "storage.duckdb.path must not be empty when duckdb is enabled";
        return false;
    }
    if (storage.raw_queue_capacity == 0 || storage.parquet_queue_capacity == 0) {
        error = "storage queue capacities must be greater than zero";
        return false;
    }
    if (storage.queue_warn_fraction <= 0.0 || storage.queue_warn_fraction > 1.0) {
        error = "storage.queue_warn_fraction must be within (0, 1]";
        return false;
    }
    if (polymarket.gamma_base_url.empty() || polymarket.clob_base_url.empty() ||
        polymarket.market_ws_url.empty()) {
        error = "endpoints must not be empty";
        return false;
    }
    return true;
}

std::string Config::raw_directory() const { return join_path(storage.root_directory, "raw"); }
std::string Config::normalized_directory() const {
    return join_path(storage.root_directory, "normalized");
}
std::string Config::metadata_directory() const {
    return join_path(storage.root_directory, "metadata");
}
std::string Config::books_directory() const { return join_path(storage.root_directory, "books"); }
std::string Config::trades_directory() const { return join_path(storage.root_directory, "trades"); }
std::string Config::markets_directory() const {
    return join_path(storage.root_directory, "markets");
}

std::string Config::duckdb_path() const {
    const std::filesystem::path configured{storage.duckdb.path};
    // An absolute path is honoured as given; a relative one is anchored to the
    // storage root so the whole dataset stays movable as one directory.
    if (configured.is_absolute()) {
        return configured.string();
    }
    return (std::filesystem::path{storage.root_directory} / configured).string();
}

}  // namespace market_data
