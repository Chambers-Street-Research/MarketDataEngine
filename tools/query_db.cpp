// market_data_query -- run SQL against the collected datasets.
//
// A thin convenience over the DuckDB CLI: it resolves the database path from
// the same config the collector uses, refreshes the views so a freshly
// collected session is visible, and prints results as a table or as CSV.
#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "market_data/config.hpp"
#include "market_data/logging.hpp"
#include "market_data/storage/duckdb_catalog.hpp"

namespace {

void print_usage(const char* program) {
    std::cout <<
        R"(market_data_query -- run SQL against the collected market-data datasets.

USAGE
  )" << program << R"( [--db <path> | --config <path>] [--sql <query> | --file <path>]
  )" << program << R"( --list-views
  )" << program << R"(                                    (interactive)

OPTIONS
  --db <path>        DuckDB database file. Default: ./data/db/market_data.duckdb
  --config <path>    Read the database location from an engine config instead.
  --sql <query>      Run one query and exit.
  --file <path>      Run every statement in a file (semicolon separated).
  --csv              Emit CSV instead of an aligned table.
  --no-refresh       Skip refreshing the views before querying.
  --list-views       Print the available views and their columns, then exit.
  --limit <n>        Row cap for interactive output. Default 50, 0 for all.
  -h, --help         Show this help.

VIEWS
  book_snapshots        every normalized book snapshot (Hive: date, hour)
  trades                every recorded trade
  markets               market metadata, one row per (market, token)
  spreads               snapshots where both sides are quoted
  order_book_features   microstructure features for trusted books
  market_summary        per-token daily aggregates

EXAMPLES
  )" << program << R"( --sql "SELECT COUNT(*) FROM book_snapshots"

  )" << program << R"( --sql "
      SELECT market_id, AVG(spread) AS avg_spread, AVG(ABS(imbalance_l1)) AS avg_imb
      FROM book_snapshots GROUP BY market_id ORDER BY avg_spread DESC LIMIT 20"

NOTE
  The DuckDB CLI works directly on the same file and is the better tool for
  serious exploration:

      duckdb data/db/market_data.duckdb
)";
}

void print_table(const market_data::QueryResult& result, bool csv, std::size_t limit) {
    if (result.column_names.empty()) {
        return;
    }
    const std::size_t rows_to_print =
        limit == 0 ? result.rows.size() : std::min(limit, result.rows.size());

    if (csv) {
        for (std::size_t i = 0; i < result.column_names.size(); ++i) {
            std::cout << (i == 0 ? "" : ",") << result.column_names[i];
        }
        std::cout << '\n';
        for (std::size_t r = 0; r < rows_to_print; ++r) {
            for (std::size_t c = 0; c < result.rows[r].size(); ++c) {
                const auto& value = result.rows[r][c];
                // Quote only when the value would otherwise break the CSV.
                const bool needs_quotes =
                    value.find_first_of(",\"\n") != std::string::npos;
                std::cout << (c == 0 ? "" : ",");
                if (needs_quotes) {
                    std::cout << '"';
                    for (const char ch : value) {
                        if (ch == '"') std::cout << '"';
                        std::cout << ch;
                    }
                    std::cout << '"';
                } else {
                    std::cout << value;
                }
            }
            std::cout << '\n';
        }
        return;
    }

    std::vector<std::size_t> widths;
    widths.reserve(result.column_names.size());
    for (const auto& name : result.column_names) {
        widths.push_back(name.size());
    }
    for (std::size_t r = 0; r < rows_to_print; ++r) {
        for (std::size_t c = 0; c < result.rows[r].size() && c < widths.size(); ++c) {
            widths[c] = std::max(widths[c], result.rows[r][c].size());
        }
    }
    // Keep a pathological column (a long question, a token id) from wrecking
    // the layout of every other column.
    for (auto& width : widths) {
        width = std::min<std::size_t>(width, 48);
    }

    // Truncation marker is ASCII and the padding subtraction is guarded: a
    // multi-byte ellipsis made the truncated string *longer* than `width`,
    // and the unsigned subtraction below then wrapped to a huge length.
    const auto pad = [](const std::string& text, std::size_t width) {
        std::string out = text;
        if (out.size() > width) {
            out = width > 3 ? out.substr(0, width - 3) + "..." : out.substr(0, width);
        }
        if (out.size() < width) {
            out.append(width - out.size(), ' ');
        }
        return out;
    };

    for (std::size_t c = 0; c < widths.size(); ++c) {
        std::cout << (c == 0 ? "" : "  ") << pad(result.column_names[c], widths[c]);
    }
    std::cout << '\n';
    for (std::size_t c = 0; c < widths.size(); ++c) {
        std::cout << (c == 0 ? "" : "  ") << std::string(widths[c], '-');
    }
    std::cout << '\n';
    for (std::size_t r = 0; r < rows_to_print; ++r) {
        for (std::size_t c = 0; c < widths.size(); ++c) {
            const std::string value = c < result.rows[r].size() ? result.rows[r][c] : std::string{};
            std::cout << (c == 0 ? "" : "  ") << pad(value, widths[c]);
        }
        std::cout << '\n';
    }
    if (rows_to_print < result.rows.size()) {
        std::cout << "... " << (result.rows.size() - rows_to_print) << " more rows ("
                  << result.rows.size() << " total; use --limit 0 for all)\n";
    } else {
        std::cout << "(" << result.rows.size() << " rows)\n";
    }
}

int run_statements(market_data::DuckDbCatalog& catalog,
                   const std::string& text,
                   bool csv,
                   std::size_t limit) {
    // Naive split on ';' is fine here: these are analyst queries, not a SQL
    // parser's input, and DuckDB reports anything it cannot make sense of.
    std::stringstream stream(text);
    std::string statement;
    int status = 0;
    while (std::getline(stream, statement, ';')) {
        const auto first = statement.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) {
            continue;
        }
        const auto result = catalog.query(statement);
        if (!result.ok()) {
            std::cerr << "error: " << result.error << '\n';
            status = 1;
            continue;
        }
        print_table(result, csv, limit);
    }
    return status;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace market_data;

    std::string db_path;
    std::string config_path;
    std::string sql;
    std::string sql_file;
    bool csv = false;
    bool refresh = true;
    bool list_views = false;
    std::size_t limit = 50;

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
        } else if (arg == "--db") {
            db_path = next_arg(i, "--db");
        } else if (arg == "--config") {
            config_path = next_arg(i, "--config");
        } else if (arg == "--sql") {
            sql = next_arg(i, "--sql");
        } else if (arg == "--file") {
            sql_file = next_arg(i, "--file");
        } else if (arg == "--csv") {
            csv = true;
        } else if (arg == "--no-refresh") {
            refresh = false;
        } else if (arg == "--list-views") {
            list_views = true;
        } else if (arg == "--limit") {
            limit = static_cast<std::size_t>(std::strtoul(next_arg(i, "--limit"), nullptr, 10));
        } else {
            std::cerr << "error: unknown argument '" << arg << "'\n\n";
            print_usage(argv[0]);
            return 2;
        }
    }

    if (!duckdb_support_available()) {
        std::cerr << "error: this build has no DuckDB support "
                     "(configure with -DMDE_WITH_DUCKDB=ON)\n";
        return 1;
    }

    Config config;
    if (!config_path.empty()) {
        std::string error;
        if (!Config::load_from_file(config_path, config, error)) {
            std::cerr << "config error: " << error << '\n';
            return 1;
        }
    }
    if (!db_path.empty()) {
        config.storage.duckdb.path = db_path;
        // The database normally lives at <root>/db/market_data.duckdb, so when
        // only --db is given, infer the dataset root from it. Without this the
        // view refresh would look for Parquet under the default ./data and
        // report an empty dataset that is actually right next to the database.
        if (config_path.empty()) {
            const std::filesystem::path file{db_path};
            const auto db_dir = file.parent_path();
            if (db_dir.filename() == "db" && db_dir.has_parent_path()) {
                config.storage.root_directory = db_dir.parent_path().string();
            } else if (!db_dir.empty()) {
                config.storage.root_directory = db_dir.string();
            }
        }
    }
    Logger::instance().set_level(LogLevel::Warn);

    DuckDbCatalog catalog(config);
    std::string error;
    if (!catalog.open(error)) {
        std::cerr << "error: " << error << '\n';
        return 1;
    }

    if (refresh) {
        const auto status = catalog.refresh_views();
        for (const auto& message : status.errors) {
            std::cerr << "warning: " << message << '\n';
        }
        if (status.views_created.empty()) {
            std::cerr << "warning: no datasets found under " << config.storage.root_directory
                      << " -- has the collector run yet?\n";
        }
    }

    if (list_views) {
        for (const auto& view : catalog.status().views_created) {
            std::cout << "\n" << view << '\n';
            const auto columns = catalog.query(
                "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM " + view + ")");
            if (!columns.ok()) {
                // Reported rather than swallowed: a silently empty column list
                // looks like an empty view, which is a very different problem.
                std::cerr << "  warning: cannot describe " << view << ": " << columns.error
                          << '\n';
                continue;
            }
            for (const auto& row : columns.rows) {
                std::cout << "  " << row[0] << "  " << (row.size() > 1 ? row[1] : "") << '\n';
            }
        }
        catalog.close();
        return 0;
    }

    int status = 0;
    if (!sql_file.empty()) {
        std::ifstream in(sql_file);
        if (!in) {
            std::cerr << "error: cannot open " << sql_file << '\n';
            catalog.close();
            return 1;
        }
        std::ostringstream buffer;
        buffer << in.rdbuf();
        status = run_statements(catalog, buffer.str(), csv, limit);
    } else if (!sql.empty()) {
        status = run_statements(catalog, sql, csv, limit);
    } else {
        std::cout << "market_data_query -- " << catalog.status().database_path
                  << "\nType SQL and press enter. Blank line or Ctrl+D to exit.\n\n";
        std::string line;
        while (true) {
            std::cout << "sql> " << std::flush;
            if (!std::getline(std::cin, line) || line.empty()) {
                break;
            }
            run_statements(catalog, line, csv, limit);
            std::cout << '\n';
        }
    }

    catalog.close();
    return status;
}
