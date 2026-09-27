// market_data_replay -- reconstruct order books from a recorded event log.
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>

#include <memory>

#include "market_data/config.hpp"
#include "market_data/logging.hpp"
#include "market_data/replay.hpp"
#include "market_data/storage/storage_manager.hpp"

namespace {

void print_usage(const char* program) {
    std::cout <<
        R"(market_data_replay -- replay a recorded market-data session.

USAGE
  )" << program << R"( <events.jsonl> [options]
  )" << program << R"( --input <events.jsonl> [options]

OPTIONS
  --input <path>        Raw event log written by market_data_engine.
  --config <path>       JSON config; only order_book_depth is used here.
  --print-snapshots     Print the final book for every token.
  --print-digest        Print the final-state digest (compare across runs).
  --depth <n>           Levels per side to print. Default 5.
  --token <id>          Restrict printed output to one token.
  --progress <n>        Print a progress line every n events. Default 0 (off).
  --write-dataset <dir> Rebuild the normalized Parquet dataset (and DuckDB
                        catalog) from this log into <dir>. This is how a
                        dataset is regenerated after a storage bug, or after
                        the schema changes, without recollecting.
  --snapshot-ms <n>     Snapshot cadence for --write-dataset, in recorded
                        time. Defaults to the config value (1000).
  --log-level <level>   trace | debug | info | warn | error | off
  -h, --help            Show this help.

EXIT STATUS
  0  replay completed and every book ended in a trustworthy state
  1  replay could not run
  3  replay completed but found gaps, malformed lines or invalid books

NOTES
  Replay is offline and deterministic. It performs no network I/O: a resync the
  live engine would have issued is counted, not executed.
)";
}

}  // namespace

int main(int argc, char** argv) {
    using namespace market_data;

    std::string input_path;
    std::string config_path;
    std::string only_token;
    std::string log_level = "warn";
    bool print_snapshots = false;
    bool print_digest = false;
    std::size_t depth = 5;
    std::uint64_t progress_every = 0;
    std::string dataset_dir;
    long snapshot_ms = 0;

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
        } else if (arg == "--input") {
            input_path = next_arg(i, "--input");
        } else if (arg == "--config") {
            config_path = next_arg(i, "--config");
        } else if (arg == "--print-snapshots") {
            print_snapshots = true;
        } else if (arg == "--print-digest") {
            print_digest = true;
        } else if (arg == "--depth") {
            depth = static_cast<std::size_t>(std::strtoul(next_arg(i, "--depth"), nullptr, 10));
        } else if (arg == "--token") {
            only_token = next_arg(i, "--token");
        } else if (arg == "--progress") {
            progress_every = std::strtoull(next_arg(i, "--progress"), nullptr, 10);
        } else if (arg == "--write-dataset") {
            dataset_dir = next_arg(i, "--write-dataset");
        } else if (arg == "--snapshot-ms") {
            snapshot_ms = std::strtol(next_arg(i, "--snapshot-ms"), nullptr, 10);
        } else if (arg == "--log-level") {
            log_level = next_arg(i, "--log-level");
        } else if (!arg.empty() && arg.front() != '-' && input_path.empty()) {
            input_path = arg;  // Positional form: replay <file>.
        } else {
            std::cerr << "error: unknown argument '" << arg << "'\n\n";
            print_usage(argv[0]);
            return 2;
        }
    }

    if (input_path.empty()) {
        std::cerr << "error: no input file given\n\n";
        print_usage(argv[0]);
        return 2;
    }

    Config config;
    if (!config_path.empty()) {
        std::string error;
        if (!Config::load_from_file(config_path, config, error)) {
            std::cerr << "config error: " << error << '\n';
            return 1;
        }
    }
    if (depth > 0) {
        config.order_book_depth = depth;
    }

    LogLevel level = LogLevel::Warn;
    if (!parse_log_level(log_level, level)) {
        std::cerr << "error: unknown log level '" << log_level << "'\n";
        return 2;
    }
    Logger::instance().set_level(level);

    if (!dataset_dir.empty()) {
        config.storage.root_directory = dataset_dir;
        // Rebuilding the normalized dataset must never touch the raw archive
        // it is being rebuilt from.
        config.storage.raw_events.enabled = false;
        if (snapshot_ms > 0) {
            config.storage.parquet.snapshot_interval_ms = static_cast<std::uint32_t>(snapshot_ms);
        }
        std::string config_error;
        if (!config.validate(config_error)) {
            std::cerr << "config error: " << config_error << '\n';
            return 1;
        }
    }

    ReplayEngine replay(config);

    std::unique_ptr<StorageManager> storage;
    if (!dataset_dir.empty()) {
        storage = std::make_unique<StorageManager>(config);
        std::string storage_error;
        if (!storage->start(storage_error)) {
            std::cerr << "storage error: " << storage_error << '\n';
            return 1;
        }
        replay.set_snapshot_observer(
            [&storage](const MarketSnapshot& snapshot) { storage->record_snapshot(snapshot); });
        replay.set_trade_observer(
            [&storage](const Trade& trade) { storage->record_trade(trade); });
    }

    if (progress_every > 0) {
        replay.set_event_observer([progress_every](const MarketEvent&, const ReplayStats& stats) {
            if (stats.events_decoded % progress_every == 0) {
                std::cerr << "\rreplayed " << stats.events_decoded << " events" << std::flush;
            }
        });
    }

    std::string error;
    if (!replay.replay_file(input_path, error)) {
        std::cerr << error << '\n';
        return 1;
    }
    if (progress_every > 0) {
        std::cerr << '\n';
    }

    if (storage) {
        storage->shutdown();
    }

    const auto& stats = replay.stats();
    std::cout << "Replayed " << input_path << "\n\n"
              << "  lines read           : " << stats.lines_read << '\n'
              << "  events decoded       : " << stats.events_decoded << '\n'
              << "  events applied       : " << stats.events_applied << '\n'
              << "  malformed lines      : " << stats.malformed_lines << '\n'
              << "  sequence range       : " << stats.first_sequence << " .. "
              << stats.last_sequence << '\n'
              << "  sequence gaps        : " << stats.sequence_gaps << '\n'
              << "  out-of-order events  : " << stats.out_of_order_events << '\n'
              << "  duplicate sequences  : " << stats.duplicate_sequences << '\n'
              << "  crossed incidents    : " << stats.crossed_incidents << '\n'
              << "  resyncs requested    : " << stats.resyncs_requested << '\n'
              << "  books reconstructed  : " << replay.books().book_count() << '\n'
              << "  books untrusted      : " << stats.books_invalid_at_end << '\n';

    if (storage) {
        std::cout << "  snapshots emitted    : " << stats.snapshots_emitted << '\n'
                  << "  trades emitted       : " << stats.trades_emitted << '\n'
                  << storage->format_summary();
    }

    if (print_snapshots) {
        std::cout << "\nFinal books\n";
        for (const auto& snapshot : replay.final_snapshots()) {
            if (!only_token.empty() && snapshot.token_id != only_token) {
                continue;
            }
            std::cout << "\n  token " << snapshot.token_id << "  market " << snapshot.market_id
                      << "\n    best bid " << std::fixed << std::setprecision(3)
                      << snapshot.best_bid << " x " << std::setprecision(2)
                      << snapshot.best_bid_size << "    best ask " << std::setprecision(3)
                      << snapshot.best_ask << " x " << std::setprecision(2)
                      << snapshot.best_ask_size << "\n    spread " << std::setprecision(3)
                      << snapshot.spread << "   midpoint " << snapshot.midpoint
                      << "   imbalance(L5) " << std::setprecision(3) << snapshot.imbalance_l5
                      << '\n';
            const std::size_t rows = std::min(depth, std::max(snapshot.bids.size(), snapshot.asks.size()));
            for (std::size_t i = 0; i < rows; ++i) {
                std::cout << "      ";
                if (i < snapshot.bids.size()) {
                    std::cout << std::setw(8) << std::setprecision(3) << snapshot.bids[i].price
                              << " x " << std::setw(12) << std::setprecision(2)
                              << snapshot.bids[i].quantity;
                } else {
                    std::cout << std::setw(23) << ' ';
                }
                std::cout << "   |   ";
                if (i < snapshot.asks.size()) {
                    std::cout << std::setw(8) << std::setprecision(3) << snapshot.asks[i].price
                              << " x " << std::setw(12) << std::setprecision(2)
                              << snapshot.asks[i].quantity;
                }
                std::cout << '\n';
            }
        }
    }

    if (print_digest) {
        std::cout << "\nFinal-state digest\n" << replay.final_state_digest();
    }

    const bool clean = stats.malformed_lines == 0 && stats.sequence_gaps == 0 &&
                       stats.out_of_order_events == 0 && stats.books_invalid_at_end == 0;
    return clean ? 0 : 3;
}
