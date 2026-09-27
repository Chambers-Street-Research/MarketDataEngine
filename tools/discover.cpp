// Milestone 2 demonstrator: fetch live Polymarket metadata, load it into a
// MarketRegistry, exercise every required lookup and persist a snapshot.
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>

#include "market_data/config.hpp"
#include "market_data/exchange/polymarket_adapter.hpp"
#include "market_data/logging.hpp"
#include "market_data/market_registry.hpp"
#include "market_data/time_util.hpp"

int main(int argc, char** argv) {
    using namespace market_data;

    Config config;
    if (argc > 1) {
        std::string error;
        if (!Config::load_from_file(argv[1], config, error)) {
            std::cerr << "config error: " << error << '\n';
            return 1;
        }
    }
    if (argc > 2) {
        config.subscribe.top_markets_by_volume = static_cast<std::size_t>(std::atoi(argv[2]));
    }
    Logger::instance().set_level(config.log_level);

    PolymarketAdapter adapter(config);
    MarketRegistry registry;

    const std::size_t accepted = registry.upsert_all(adapter.discover_markets());

    std::cout << "\nDiscovered: " << accepted << " active markets, "
              << registry.token_count() << " outcome tokens\n\n";

    const auto top = registry.top_markets_by_volume(config.subscribe.top_markets_by_volume);
    std::cout << std::left << std::setw(58) << "QUESTION" << std::setw(24) << "CATEGORY"
              << std::right << std::setw(16) << "VOLUME" << std::setw(8) << "TICK"
              << std::setw(8) << "TOKENS" << '\n'
              << std::string(114, '-') << '\n';

    for (const auto& market : top) {
        std::string question = market.question;
        if (question.size() > 55) {
            question = question.substr(0, 52) + "...";
        }
        std::string category = market.category;
        if (category.size() > 21) {
            category = category.substr(0, 18) + "...";
        }
        std::cout << std::left << std::setw(58) << question << std::setw(24) << category
                  << std::right << std::fixed << std::setprecision(0) << std::setw(16)
                  << market.reported_volume << std::setprecision(3) << std::setw(8)
                  << market.min_tick_size << std::setw(8) << market.token_ids.size() << '\n';
    }

    // Prove every lookup the specification requires.
    if (!top.empty()) {
        const auto& sample = top.front();
        const auto& token = sample.token_ids.front();
        std::cout << "\nLookups for token " << token.substr(0, 16) << "...\n"
                  << "  token -> market      : " << registry.market_for_token(token)->market_id
                  << '\n'
                  << "  token -> outcome     : "
                  << registry.outcome_for_token(token).value_or("(none)") << '\n'
                  << "  market -> tokens     : "
                  << registry.tokens_for_market(sample.market_id).size() << '\n'
                  << "  market -> category   : "
                  << registry.category_for_market(sample.market_id).value_or("(none)") << '\n'
                  << "  market -> resolution : ";
        if (const auto when = registry.resolution_time_for_market(sample.market_id)) {
            std::cout << format_iso8601(*when) << '\n';
        } else {
            std::cout << "(none)\n";
        }
    }

    const std::string path = config.metadata_directory() + "/markets.jsonl";
    if (const auto written = registry.write_snapshot(path)) {
        std::cout << "\nPersisted " << *written << " markets to " << path << '\n';
    } else {
        std::cerr << "\nfailed to persist metadata snapshot to " << path << '\n';
        return 1;
    }

    const auto tokens = registry.top_tokens_by_volume(config.subscribe.top_markets_by_volume);
    std::cout << "Subscription set would be " << tokens.size() << " tokens\n";
    return 0;
}
