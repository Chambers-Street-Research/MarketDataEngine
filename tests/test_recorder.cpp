#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "market_data/event_codec.hpp"
#include "market_data/recorder.hpp"
#include "market_data/time_util.hpp"

namespace market_data {
namespace {

class TempDir {
public:
    TempDir() : path_(std::filesystem::temp_directory_path() / "mde_recorder_test") {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }

    [[nodiscard]] std::string str() const { return path_.string(); }
    [[nodiscard]] const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

Config config_for(const TempDir& dir) {
    Config config;
    config.storage.root_directory = dir.str();
    // Parquet supersedes CSV, so the CSV sink is off by default; these tests
    // exercise it directly.
    config.storage.write_csv_snapshots = true;
    return config;
}

MarketEvent make_update(std::uint64_t seq) {
    BookUpdateEvent event;
    event.header.exchange = "polymarket";
    event.header.market_id = "0xmarket";
    event.header.token_id = "tok";
    event.header.exchange_timestamp = now_exchange();
    event.header.receive_timestamp = now_receive();
    event.header.sequence = seq;
    event.updates.push_back(BookUpdate{"tok", Side::Bid, 0.48, 1200.0, {}, {}});
    return MarketEvent{std::move(event)};
}

std::vector<std::string> read_lines(const std::string& path) {
    std::ifstream in(path);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        lines.push_back(line);
    }
    return lines;
}

TEST(RawRecorderTest, WritesOneDecodableJsonLinePerEvent) {
    const TempDir dir;
    RawRecorder recorder(config_for(dir));
    std::string error;
    ASSERT_TRUE(recorder.start(error)) << error;

    for (std::uint64_t seq = 1; seq <= 3; ++seq) {
        EXPECT_GT(recorder.record(make_update(seq)), 0u);
    }
    const std::string path = recorder.current_path();
    recorder.stop();

    const auto lines = read_lines(path);
    ASSERT_EQ(lines.size(), 3u);
    EXPECT_EQ(recorder.events_written(), 3u);
    for (std::size_t i = 0; i < lines.size(); ++i) {
        const auto decoded = decode_event_line(lines[i]);
        ASSERT_TRUE(decoded.has_value()) << lines[i];
        EXPECT_EQ(header_of(*decoded).sequence, i + 1);
    }
}

TEST(RawRecorderTest, FileLandsUnderTheRawDirectory) {
    const TempDir dir;
    RawRecorder recorder(config_for(dir));
    std::string error;
    ASSERT_TRUE(recorder.start(error)) << error;

    const std::filesystem::path path = recorder.current_path();
    EXPECT_EQ(path.parent_path(), dir.path() / "raw");
    EXPECT_EQ(path.extension(), ".jsonl");
    EXPECT_NE(path.filename().string().find("events-"), std::string::npos);
}

TEST(RawRecorderTest, RestartingAppendsRatherThanTruncating) {
    const TempDir dir;
    std::string path;
    {
        RawRecorder recorder(config_for(dir));
        std::string error;
        ASSERT_TRUE(recorder.start(error)) << error;
        recorder.record(make_update(1));
        path = recorder.current_path();
    }
    {
        // Restarting the engine must never destroy a session already recorded.
        RawRecorder recorder(config_for(dir));
        std::string error;
        ASSERT_TRUE(recorder.start(error)) << error;
        recorder.record(make_update(2));
    }
    EXPECT_EQ(read_lines(path).size(), 2u);
}

TEST(RawRecorderTest, DisabledRecorderWritesNothingAndStillStarts) {
    const TempDir dir;
    Config config = config_for(dir);
    config.storage.raw_events.enabled = false;

    RawRecorder recorder(config);
    std::string error;
    ASSERT_TRUE(recorder.start(error));
    EXPECT_EQ(recorder.record(make_update(1)), 0u);
    EXPECT_EQ(recorder.events_written(), 0u);
    EXPECT_FALSE(std::filesystem::exists(dir.path() / "raw"));
}

TEST(RawRecorderTest, RotatesOnSizeAndKeepsEveryEvent) {
    const TempDir dir;
    Config config = config_for(dir);
    config.storage.raw_events.max_file_bytes = 400;  // A handful of events per file.

    RawRecorder recorder(config);
    std::string error;
    ASSERT_TRUE(recorder.start(error)) << error;
    for (std::uint64_t seq = 1; seq <= 40; ++seq) {
        recorder.record(make_update(seq));
    }
    recorder.stop();

    EXPECT_GT(recorder.rotations(), 0u);

    // Every event must be recoverable across the rotation boundary.
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path() / "raw")) {
        files.push_back(entry.path());
    }
    std::sort(files.begin(), files.end());
    EXPECT_GT(files.size(), 1u);

    std::vector<std::uint64_t> sequences;
    for (const auto& file : files) {
        for (const auto& line : read_lines(file.string())) {
            const auto decoded = decode_event_line(line);
            ASSERT_TRUE(decoded.has_value()) << line;
            sequences.push_back(header_of(*decoded).sequence);
        }
    }
    std::sort(sequences.begin(), sequences.end());
    ASSERT_EQ(sequences.size(), 40u);
    for (std::size_t i = 0; i < sequences.size(); ++i) {
        EXPECT_EQ(sequences[i], i + 1);
    }
}

TEST(CsvSnapshotWriterTest, WritesAHeaderThenOneRowPerSnapshot) {
    const TempDir dir;
    CsvSnapshotWriter writer(config_for(dir));
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;

    MarketSnapshot snapshot;
    snapshot.token_id = "tok";
    snapshot.market_id = "0xmarket";
    snapshot.best_bid = 0.49;
    snapshot.best_ask = 0.51;
    snapshot.best_bid_ticks = 490;
    snapshot.best_ask_ticks = 510;
    snapshot.best_bid_size = 100.0;
    snapshot.best_ask_size = 150.0;
    snapshot.midpoint = 0.50;
    snapshot.spread = 0.02;
    snapshot.bid_depth_l5 = 300.0;
    snapshot.ask_depth_l5 = 450.0;
    snapshot.imbalance_l1 = -0.2;
    snapshot.imbalance_l5 = -0.2;
    snapshot.bid_levels = 2;
    snapshot.ask_levels = 2;
    snapshot.timestamp = now_receive();
    snapshot.exchange_timestamp = now_exchange();

    ASSERT_GT(writer.write(snapshot), 0u);
    ASSERT_GT(writer.write(snapshot), 0u);
    const std::string path = writer.current_path();
    writer.stop();

    const auto lines = read_lines(path);
    ASSERT_EQ(lines.size(), 3u);
    EXPECT_EQ(lines[0] + "\n", std::string(CsvSnapshotWriter::csv_header()));
    EXPECT_EQ(writer.rows_written(), 2u);

    const std::size_t columns =
        static_cast<std::size_t>(std::count(lines[0].begin(), lines[0].end(), ',')) + 1;
    for (std::size_t i = 1; i < lines.size(); ++i) {
        const std::size_t row_columns =
            static_cast<std::size_t>(std::count(lines[i].begin(), lines[i].end(), ',')) + 1;
        EXPECT_EQ(row_columns, columns) << lines[i];
    }
}

TEST(CsvSnapshotWriterTest, UndefinedStatisticsBecomeEmptyFieldsNotZero) {
    const TempDir dir;
    CsvSnapshotWriter writer(config_for(dir));
    std::string error;
    ASSERT_TRUE(writer.start(error)) << error;

    // A one-sided book: midpoint and spread are undefined. Writing 0 would be
    // read downstream as a real price of zero.
    MarketSnapshot snapshot;
    snapshot.token_id = "tok";
    snapshot.best_ask = 0.51;
    snapshot.best_ask_ticks = 510;
    snapshot.timestamp = now_receive();
    ASSERT_GT(writer.write(snapshot), 0u);
    const std::string path = writer.current_path();
    writer.stop();

    const auto lines = read_lines(path);
    ASSERT_EQ(lines.size(), 2u);

    std::vector<std::string> fields;
    std::istringstream row(lines[1]);
    std::string field;
    while (std::getline(row, field, ',')) {
        fields.push_back(field);
    }
    // Locate midpoint and spread by name rather than by index, so adding a
    // column cannot quietly make this test assert about the wrong field.
    std::vector<std::string> headers;
    std::istringstream header_row(lines[0]);
    std::string header;
    while (std::getline(header_row, header, ',')) {
        headers.push_back(header);
    }
    ASSERT_EQ(headers.size(), fields.size());

    const auto column = [&](const std::string& name) {
        const auto it = std::find(headers.begin(), headers.end(), name);
        EXPECT_NE(it, headers.end()) << name;
        return fields[static_cast<std::size_t>(std::distance(headers.begin(), it))];
    };
    EXPECT_TRUE(column("midpoint").empty()) << lines[1];
    EXPECT_TRUE(column("spread").empty()) << lines[1];
    // The absent bid must also be empty: 0.0 is a real price on this venue.
    EXPECT_TRUE(column("best_bid").empty()) << lines[1];
    EXPECT_TRUE(column("best_bid_ticks").empty()) << lines[1];
    EXPECT_FALSE(column("best_ask").empty()) << lines[1];
}

TEST(CsvSnapshotWriterTest, DisabledWriterCreatesNoFile) {
    const TempDir dir;
    Config config = config_for(dir);
    config.storage.write_csv_snapshots = false;

    CsvSnapshotWriter writer(config);
    std::string error;
    ASSERT_TRUE(writer.start(error));
    EXPECT_EQ(writer.write(MarketSnapshot{}), 0u);
    EXPECT_FALSE(std::filesystem::exists(dir.path() / "normalized"));
}

}  // namespace
}  // namespace market_data
