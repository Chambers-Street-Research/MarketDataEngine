// market_data_engine -- live Polymarket market-data collector.
#include <atomic>
#include <csignal>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

#include "market_data/config.hpp"
#include "market_data/engine.hpp"
#include "market_data/exchange/polymarket_adapter.hpp"
#include "market_data/logging.hpp"

namespace {

// The only global the process has. A signal handler may touch nothing else,
// so it sets this flag and main() does the real work.
std::atomic<bool> g_stop_requested{false};

extern "C" void handle_signal(int /*signal*/) {
    g_stop_requested.store(true, std::memory_order_relaxed);
}

void print_usage(const char* program) {
    std::cout <<
        R"(market_data_engine -- collect, normalize and persist prediction-market data.

USAGE
  )" << program << R"( [--config <path>] [options]

OPTIONS
  --config <path>       JSON configuration file. See config/config.example.json.
                        Without it, built-in defaults are used.
  --output <dir>        Override storage.root_directory (the dataset root).
  --markets <n>         Override subscribe.top_markets_by_volume.
  --log-level <level>   trace | debug | info | warn | error | off
  --no-record           Do not write the raw event log.
  --no-snapshots        Do not write normalized snapshots.
  --dry-run             Validate config, discover markets, then exit.
  -h, --help            Show this help.

OUTPUT
  <output>/raw/events-<UTC date>.jsonl        every normalized event, replayable
  <output>/normalized/snapshots-<date>.csv    periodic research snapshots
  <output>/metadata/markets.jsonl             market metadata snapshots

SIGNALS
  SIGINT / SIGTERM      graceful shutdown: stop ingest, drain, flush, close.

NOTES
  This tool reads public market data only. It holds no credentials and places
  no orders.
)";
}

}  // namespace

int main(int argc, char** argv) {
    using namespace market_data;

    Config config;
    std::string config_path;
    std::string output_override;
    std::string log_level_override;
    long markets_override = -1;
    bool dry_run = false;
    bool no_record = false;
    bool no_snapshots = false;

    const auto next_arg = [&](int& i, const char* flag) -> const char* {
        if (i + 1 >= argc) {
            std::cerr << "error: " << flag << " requires a value\n";
            std::exit(2);
        }
        return argv[++i];
    };

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--config") {
            config_path = next_arg(i, "--config");
        } else if (arg == "--output") {
            output_override = next_arg(i, "--output");
        } else if (arg == "--markets") {
            markets_override = std::strtol(next_arg(i, "--markets"), nullptr, 10);
        } else if (arg == "--log-level") {
            log_level_override = next_arg(i, "--log-level");
        } else if (arg == "--no-record") {
            no_record = true;
        } else if (arg == "--no-snapshots") {
            no_snapshots = true;
        } else if (arg == "--dry-run") {
            dry_run = true;
        } else {
            std::cerr << "error: unknown argument '" << arg << "'\n\n";
            print_usage(argv[0]);
            return 2;
        }
    }

    if (!config_path.empty()) {
        std::string error;
        if (!Config::load_from_file(config_path, config, error)) {
            std::cerr << "config error: " << error << '\n';
            return 1;
        }
    }

    if (!output_override.empty()) {
        config.storage.root_directory = output_override;
    }
    if (markets_override > 0) {
        config.subscribe.top_markets_by_volume = static_cast<std::size_t>(markets_override);
    }
    if (!log_level_override.empty() &&
        !parse_log_level(log_level_override, config.log_level)) {
        std::cerr << "error: unknown log level '" << log_level_override << "'\n";
        return 2;
    }
    if (no_record) {
        config.storage.raw_events.enabled = false;
    }
    if (no_snapshots) {
        config.storage.write_csv_snapshots = false;
    }

    std::string error;
    if (!config.validate(error)) {
        std::cerr << "config error: " << error << '\n';
        return 1;
    }
    Logger::instance().set_level(config.log_level);

    if (dry_run) {
        PolymarketAdapter adapter(config);
        const auto markets = adapter.discover_markets();
        std::cout << "config OK; discovered " << markets.size() << " markets\n";
        return markets.empty() ? 1 : 0;
    }

    struct sigaction action{};
    action.sa_handler = handle_signal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    sigaction(SIGINT, &action, nullptr);
    sigaction(SIGTERM, &action, nullptr);
    // A recorder writing to a closed pipe must not take the process down.
    std::signal(SIGPIPE, SIG_IGN);

    Engine engine(config, std::make_unique<PolymarketAdapter>(config));
    if (!engine.start(error)) {
        std::cerr << "startup failed: " << error << '\n';
        return 1;
    }

    while (!g_stop_requested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }

    engine.shutdown();
    return 0;
}
