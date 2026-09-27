# market-data-engine

A low-latency, event-driven C++20 market-data engine for prediction markets,
targeting **Polymarket** first.

It discovers active markets, subscribes to the live feed, maintains a correct
L2 order book per outcome token, records every normalized event in a replayable
format, and writes research-ready snapshots for later quantitative work.

**Scope:** market-data infrastructure only. No strategy logic, no order
placement, no portfolio or PnL code, no credentials. Public endpoints only.

---

## What it does

1. Discovers active Polymarket markets and their outcome tokens (Gamma REST).
2. Subscribes to the live market websocket for the top *N* markets by volume.
3. Maintains an L2 order book per token, with snapshot + incremental updates.
4. Records every normalized event append-only as JSON Lines.
5. Writes periodic normalized snapshots as CSV.
6. Recovers from disconnects, malformed data and book corruption.
7. Exposes runtime metrics and per-token health.
8. Replays a recorded session through the *same* pipeline, deterministically.

---

## Architecture

```text
                   +----------------------+
                   | Gamma REST discovery |
                   +----------+-----------+
                              |
                              v
                   +----------------------+
                   |   MarketRegistry     |
                   +----------+-----------+
                              |
                              v
+-------------+     +----------------------+     +--------------------+
| WebSocket   | --> | PolymarketAdapter    | --> | Event Normalizer   |
| (Beast/TLS) |     | (only venue-aware    |     | (pure, no I/O)     |
+-------------+     |  component)          |     +---------+----------+
                    +----------------------+               |
                                                 normalized MarketEvent
                                                           |
                                                 +---------v------------+
                                                 |  Bounded Queue       |
                                                 +---------+------------+
                                                           |
                               +---------------------------+-----------------+
                               |                           |                 |
                               v                           v                 v
                      +----------------+         +----------------+  +----------------+
                      | OrderBook      |         | Raw Recorder   |  | Metrics/Health |
                      | Manager        |         | (JSONL)        |  |                |
                      +-------+--------+         +----------------+  +----------------+
                              |
                              v
                      +--------------------+
                      |  StorageManager    |
                      +---------+----------+
                                |
              +-----------------+------------------+
              v                                    v
      +----------------+                  +------------------+
      | Raw worker     |                  | Parquet worker   |
      | JSONL + zstd   |                  | books / trades / |
      | hourly rotate  |                  | markets          |
      +----------------+                  +--------+---------+
                                                   |
                                                   v
                                          +------------------+
                                          | DuckDB catalog   |
                                          | (views only)     |
                                          +--------+---------+
                                                   |
                                                   v
                                           Python / SQL research
```

Everything to the right of `ExchangeAdapter` consumes normalized events and
knows nothing about Polymarket. Adding another venue means one new adapter.

See [`docs/architecture.md`](docs/architecture.md) for the threading model,
event ordering, book reconstruction, resync logic, container rationale and
known failure modes.

---

## Dependencies

**Required:**

| Dependency | Version | Purpose | How it is obtained |
|---|---|---|---|
| CMake | ≥ 3.20 | build | system |
| C++ compiler | C++20 (GCC 11+, Clang 14+, Apple Clang 14+) | — | system |
| Boost | ≥ 1.75 (Beast, Asio) | async websocket + HTTPS | system |
| OpenSSL | 3.x | TLS | system |
| nlohmann/json | ≥ 3.9 | JSON | system |
| GoogleTest | 1.17 | tests | fetched automatically by CMake |

**Optional** — each is a CMake feature flag. Without it the engine still
collects and replays; it just produces less. A missing dependency downgrades
the feature with a warning rather than failing the build.

| Dependency | Feature flag | Enables | Without it |
|---|---|---|---|
| Apache Arrow + Parquet | `MDE_WITH_PARQUET` | normalized Parquet datasets | raw archive only; rebuild later with `--write-dataset` |
| DuckDB | `MDE_WITH_DUCKDB` | SQL catalog, `market_data_query` | query the Parquet directly instead |
| libzstd | `MDE_WITH_ZSTD` | compressed raw archive (~7× observed) | plain `.jsonl` |

```bash
# macOS
brew install cmake boost openssl@3 nlohmann-json          # required
brew install apache-arrow duckdb zstd                     # optional

# Debian / Ubuntu
sudo apt install cmake g++ libboost-all-dev libssl-dev nlohmann-json3-dev
sudo apt install libarrow-dev libparquet-dev libduckdb-dev libzstd-dev
```

Only GoogleTest is downloaded, and only when `MDE_BUILD_TESTS=ON` (the default).
The build prints what it found:

```text
-- market-data-engine features: parquet=ON duckdb=ON zstd=ON
```

---

## Build

```bash
mkdir build && cd build
cmake ..
cmake --build . -j
ctest
```

The project is warning-clean under `-Wall -Wextra -Wpedantic -Wshadow
-Wconversion -Wsign-conversion`. To enforce that:

```bash
cmake .. -DMDE_WERROR=ON
```

| CMake option | Default | Effect |
|---|---|---|
| `MDE_BUILD_TESTS` | `ON` | build the test suite (fetches GoogleTest) |
| `MDE_WERROR` | `OFF` | treat warnings as errors |
| `MDE_WITH_PARQUET` | `ON` | Parquet datasets (auto-off if Arrow is missing) |
| `MDE_WITH_DUCKDB` | `ON` | DuckDB catalog and query tool |
| `MDE_WITH_ZSTD` | `ON` | zstd-compressed raw archive |

The suite is green either way: with the optional stack absent, 53 storage
tests skip explicitly rather than silently passing.

---

## Run

```bash
./market_data_engine --config ../config/config.example.json
```

```text
Connected to Polymarket market-data feed
Discovered: 200 active markets
Subscribed: 40 tokens

TOKEN                   MARKET                                  BID      ASK   SPREAD      MID  IMB(L5)  HEALTH
-------------------------------------------------------------------------------------------------------------------
1075058827677314893...  Will the US confirm t... [Yes]        0.037    0.038    0.001    0.037    -0.58  healthy
7305630249804085635...  Will the US confirm t... [No]         0.962    0.963    0.001    0.963     0.58  healthy
5179715774304650421...  Will Jesus Christ ret... [No]         0.982    0.983    0.001    0.983    -0.73  healthy

messages/sec: 3   updates/sec: 3   trades/sec: 0.0
queue depth: 0 (high-water 46)   events persisted: 171   dropped: 0
books: 20 healthy, 30 stale, 0 unhealthy   reconnects: 0   resyncs: 0/0
invalid messages: 0   crossed books: 0   written: 170.2 KiB   received: 169.1 KiB   uptime: 0h 0m 36s
```

`Ctrl+C` shuts down in order — stop ingest, drain the queue, flush, close,
report:

```text
Shutdown requested
Draining event queue... (0 events)
Flushing recorder...
final-state digest written to ./data/final_state.digest
299 events persisted
0 events dropped
1560 snapshots written
queue high-water mark: 40/262144
reconnects: 0, resyncs: 0/0, crossed books: 0, invalid messages: 0
Shutdown complete
```

### CLI

```text
--config <path>       JSON configuration file
--output <dir>        override output_directory
--markets <n>         override subscribe.top_markets_by_volume
--log-level <level>   trace | debug | info | warn | error | off
--no-record           do not write the raw event log
--no-snapshots        do not write normalized snapshots
--dry-run             validate config, discover markets, exit
-h, --help            full help
```

There is also `./market_data_discover <config>` — a discovery-only tool that
lists the top markets and exercises every registry lookup without connecting to
the feed.

---

## Configuration

See [`config/config.example.json`](config/config.example.json). Endpoints are
configurable rather than hardcoded, so the engine can be pointed at a mock or a
staging venue without a rebuild.

```json
{
  "exchange": "polymarket",
  "output_directory": "./data",
  "snapshot_interval_ms": 1000,
  "order_book_depth": 20,
  "log_level": "info",
  "event_queue_capacity": 262144,
  "block_when_queue_full": true,
  "subscribe":  { "top_markets_by_volume": 25, "max_tokens": 400 },
  "reconnect":  { "initial_backoff_ms": 500, "max_backoff_ms": 30000,
                  "backoff_multiplier": 2.0, "jitter_fraction": 0.25 },
  "health":     { "stale_after_ms": 60000, "ping_interval_ms": 10000,
                  "read_timeout_ms": 40000, "resync_cooldown_ms": 2000 },

  "storage": {
    "root_directory": "./data",
    "raw_events": {
      "enabled": true, "format": "jsonl", "compression": "zstd",
      "compression_level": 3, "rotation_minutes": 60,
      "max_file_bytes": 536870912, "flush_interval_ms": 1000
    },
    "parquet": {
      "enabled": true, "compression": "zstd", "compression_level": 3,
      "snapshot_interval_ms": 1000, "batch_rows": 10000,
      "flush_interval_seconds": 10, "partition_by_hour": true
    },
    "duckdb": {
      "enabled": true, "path": "db/market_data.duckdb", "create_views": true
    },
    "block_when_queue_full": true,
    "raw_queue_capacity": 262144,
    "parquet_queue_capacity": 262144
  }
}
```

`storage.root_directory` is the single dataset root; `--output` overrides it.
A relative `duckdb.path` is anchored to that root, so the whole dataset stays
movable as one directory. The legacy top-level `output_directory` key is still
accepted and folded into `storage.root_directory`.

Config is validated on load; an out-of-range value is a startup error with a
named field, not a surprise at runtime.

**No credentials belong in this file.** The engine reads public market data
only.

---

## Data storage

Three layers, in decreasing fidelity and increasing convenience. The
separation is deliberate: each layer can be rebuilt from the one above it.

```text
RAW          data/raw/       JSONL + zstd    every event, replayable, auditable
                                             |
NORMALIZED   data/books/     Parquet         research + backtesting
             data/trades/
             data/markets/
                                             |
QUERY        data/db/        DuckDB          SQL views over the Parquet
```

**Why raw JSONL exists.** It is the audit trail and the recovery path. Every
event, at full rate, in the order it was ingested. If a bug corrupts the
normalized dataset — or the schema changes — the Parquet is regenerated by
replaying this archive, with no recollection. Nothing can rebuild the archive
itself, so it is the one layer whose write failure is fatal at startup.

**Why Parquet.** Research reads columns, not rows. A question like "average
spread per market per hour" touches three columns out of twenty-six; a
columnar file with zstd and per-column statistics reads a fraction of the
bytes a JSONL scan would. Snapshots are written at a fixed cadence (1/sec per
token by default) rather than per update, so the research dataset stays a
manageable size while the raw archive keeps every tick.

**Why DuckDB.** So a researcher writes `FROM book_snapshots` instead of
locating files. It holds **views only** — no data is copied in. The database
file is a few hundred KB, the datasets stay portable and readable without
DuckDB at all, and a crashed collector cannot corrupt the query layer.

> DuckDB is deliberately *not* the ingestion path. The collector writes
> buffered Parquet; DuckDB queries it.

### Directory structure

```text
data/
├── raw/2026/09/27/events_2300.jsonl.zst      hourly, compressed
├── books/date=2026-09-27/hour=23/books.parquet
├── trades/date=2026-09-27/hour=23/trades.parquet
├── markets/date=2026-09-27/markets.parquet   daily (metadata moves slowly)
├── metadata/markets.jsonl                    human-readable metadata snapshot
├── db/market_data.duckdb                     views over the above
└── final_state.digest                        written on clean shutdown
```

Partitioning is Hive-style, so `date` and `hour` are queryable columns rather
than strings to parse. `partition_by_hour: false` collapses to date only.

**One file per partition, not per flush.** Each flush appends a *row group* to
the open file, so file count tracks elapsed hours rather than message volume.
A partition is written as `books.parquet.tmp` and atomically renamed once its
footer is on disk — a `.tmp` left behind is an obviously incomplete file from
a crashed run, safe to delete, and that interval is recoverable from the raw
archive.

### Schemas

**`book_snapshots`** — one row per token per snapshot interval.

| Column | Type | Notes |
|---|---|---|
| `receive_timestamp_ns` | int64 | local **steady** clock; ordering and latency, session-relative |
| `exchange_timestamp_ns` | int64 ∅ | venue wall clock |
| `capture_timestamp_ns` | int64 | local wall clock; what `date`/`hour` derive from |
| `market_id`, `token_id` | string | |
| `outcome` | string ∅ | "Yes"/"No", joined from the registry |
| `best_bid_ticks`, `best_ask_ticks` | int32 ∅ | exact integer prices (1/1000) |
| `best_bid`, `best_ask` | double ∅ | the same prices as decimals |
| `best_bid_size`, `best_ask_size` | double | |
| `midpoint`, `spread` | double ∅ | null when either side is empty |
| `bid_depth_l1/l5/l10`, `ask_depth_l1/l5/l10` | double | cumulative size |
| `imbalance_l1/l5/l10` | double ∅ | `(bid − ask) / (bid + ask)` |
| `bid_levels`, `ask_levels` | int32 | true resting level counts |
| `book_health` | string | `healthy`, `stale`, … — filter on this |
| `sequence_number` | int64 ∅ | last event applied to the book |

∅ = nullable. **Null means undefined, never zero.** Zero is a valid price on a
prediction market, so encoding "no bid" as `0.0` would be indistinguishable
from a real quote at zero. NaN is likewise never written — it would poison
every downstream `AVG()`.

Both tick and decimal prices are stored so nobody re-derives them.

**`trades`** — `receive_timestamp_ns`, `exchange_timestamp_ns`,
`capture_timestamp_ns`, `market_id`, `token_id`, `outcome`, `price_ticks`,
`price`, `quantity`, `side`, `trade_id`, `sequence_number`.

`side` is `buy`, `sell`, or **`unknown`**. When the venue does not report the
aggressor, `unknown` is recorded — a guessed side would silently corrupt
trade-flow and adverse-selection research.

**`markets`** — one row per **(market, token)**, so joining to `book_snapshots`
on `token_id` needs no unnesting: `snapshot_timestamp_ns`, `market_id`,
`condition_id`, `question`, `category`, `slug`, `token_id`, `outcome`,
`active`, `closed`, `accepting_orders`, `resolution_timestamp_ns`,
`reported_volume`, `reported_liquidity`, `reported_volume_24h`,
`minimum_tick_size`.

Fields Polymarket does not expose are not invented. There is no `category`
field on the API, so it is derived from the parent event's ticker; reward
parameters are not currently collected.

### Raw event log

One self-describing JSON object per line, in sequence order:

```json
{"event_type":"book_update","exchange":"polymarket",
 "market_id":"0x747dc8...","token_id":"10750588...",
 "exchange_ts":1790540018860,"receive_ts":1276100591259583,"seq":54,
 "hash":"4359c810cea5a282d14e4dcd096191f15396aa6a",
 "updates":[{"side":"bid","price":0.027,"quantity":217065.13}]}
```

Event types: `book_snapshot`, `book_update`, `trade`, `market_metadata`,
`connection`. Compression is streaming zstd flushed at frame boundaries, so a
process killed mid-write loses at most `flush_interval_ms` of events rather
than the whole hour.

```bash
zstd -dc data/raw/2026/09/27/events_2300.jsonl.zst | head -3
```

### Querying

```bash
duckdb data/db/market_data.duckdb
```

```sql
SELECT COUNT(*) FROM book_snapshots;

SELECT market_id, AVG(spread) AS avg_spread
FROM book_snapshots
WHERE date = '2026-09-27'
GROUP BY market_id
ORDER BY avg_spread DESC;

SELECT token_id, AVG(imbalance_l1) FROM book_snapshots GROUP BY token_id;
```

Or with the bundled tool, which resolves the database from your config and
refreshes the views first:

```bash
./market_data_query --db data/db/market_data.duckdb \
    --sql "SELECT COUNT(*) FROM book_snapshots"

./market_data_query --db data/db/market_data.duckdb --list-views
./market_data_query --db data/db/market_data.duckdb          # interactive
```

**Views:**

| View | Contents |
|---|---|
| `book_snapshots` | every snapshot; `date`/`hour` from the partitions |
| `trades` | every recorded trade |
| `markets` | metadata, one row per (market, token) |
| `spreads` | snapshots where both sides are quoted |
| `order_book_features` | microstructure features, trusted books only |
| `market_summary` | per-token daily avg/median spread, depth, abs imbalance |

### From Python

Python is a **research** dependency only — the collector never needs it.

```python
import duckdb

con = duckdb.connect("data/db/market_data.duckdb", read_only=True)
df = con.execute("""
    SELECT receive_timestamp_ns, market_id, token_id,
           midpoint, spread, imbalance_l1, imbalance_l5
    FROM book_snapshots
    ORDER BY receive_timestamp_ns
    LIMIT 100000
""").df()
```

Or straight from the files, with no database at all:

```python
df = duckdb.sql("""
    SELECT * FROM read_parquet('data/books/**/*.parquet', hive_partitioning=true)
""").df()
```

Nulls arrive as `NaN`/`NA` in pandas, so `df.spread.mean()` ignores undefined
rows instead of averaging in zeros.

### How much storage to expect

Measured on a 90-second live run, 30 markets / 60 tokens, 1 snapshot/sec:

| Layer | Size | Note |
|---|---|---|
| Raw (zstd) | 35 KB | 7.0× compression on the JSONL |
| Parquet | 1.3 MB | 5,040 book rows + 600 metadata rows |
| DuckDB | ~270 KB | views only; does not grow with the data |

Parquet dominates here because the snapshot cadence is fixed per token while
these markets were quiet — raw volume scales with market activity, normalized
volume with `tokens × time`. Rough extrapolation for 60 tokens at 1/sec:
**~1 GB/day of Parquet**, and raw anywhere from tens of MB (quiet) to several
GB (busy). Lower `snapshot_interval_ms` multiplies the Parquet figure
directly.

### Rebuilding the normalized dataset

Because the raw archive is authoritative, the Parquet and DuckDB layers can be
regenerated at any time — after a storage bug, or a schema change:

```bash
zstd -dc data/raw/2026/09/27/events_2300.jsonl.zst > /tmp/session.jsonl
./market_data_replay /tmp/session.jsonl --write-dataset ./rebuilt --snapshot-ms 1000
```

Replay emits snapshots on *recorded* time, not wall-clock, so the rebuild is
deterministic and comparable to the live run. Note it covers the span of
recorded events; a live session that sat quiet after its last event will have
produced a few more snapshots than the rebuild does.

## Testing

```bash
cd build && ctest --output-on-failure
```

152 tests covering:

| Area | What is pinned |
|---|---|
| Order book | snapshot construction, insert/modify/delete, best bid/ask, spread, midpoint, top-N, depth, imbalance, crossed and locked detection, tick round-trip over the whole grid |
| Polymarket parser | live-captured fixtures: double-encoded arrays, batched array frames, `PONG` control frames, multi-token `price_change`, malformed input |
| Market registry | every required lookup, stale token retirement, deterministic volume ranking, atomic snapshot write |
| Event codec | round-trip for every event type — if a field fails here, replay cannot reconstruct it |
| Book manager | the full health state machine, including "never healthy on updates alone" |
| Bounded queue | ordering, high-water mark, drop accounting, multi-producer no-loss under overload, drain-on-close |
| Recorder | JSONL decodability, size rotation without event loss, append-on-restart, empty-field CSV semantics |
| Config | validation of every range rule |
| Replay | live/replay digest equality, determinism across runs, gap and duplicate detection, tolerance of torn lines |
| Parquet writer | schema, Hive partitioning, batching (not a file per row), hour rollover, atomic `.tmp` → publish, restart without clobber |
| Storage manager | **conservation** — every queued row reaches disk or is counted as dropped; zstd round trip; drop accounting; concurrent-flush race regression |
| Storage integration | Parquet → DuckDB counts and values, SQL NULL semantics, trade side never fabricated, all research views, dataset rebuilt from a raw log |

Two of these are regression tests for bugs found during development and are
worth keeping honest: `ConcurrentFlushRequestsDoNotLoseRows` fails (4,488 of
5,000 rows) against the pre-fix code, and `EveryQueuedSnapshotReachesDisk`
pins the invariant that the reported count matches what SQL can actually see.

Parser fixtures in `tests/fixtures/` were captured from the live venue and pin
the wire format as it actually is, not as the published schema describes it.

---

## Replay

```bash
./market_data_replay ../data/raw/2026/09/27/events_2300.jsonl
./market_data_replay --input events.jsonl --print-snapshots --depth 10
./market_data_replay --input events.jsonl --print-digest

# Regenerate the normalized Parquet + DuckDB dataset from the raw archive.
./market_data_replay --input events.jsonl --write-dataset ./rebuilt
```

```text
Replayed events-2026-09-27.jsonl

  lines read           : 139
  events decoded       : 139
  events applied       : 136
  malformed lines      : 0
  sequence range       : 1 .. 139
  sequence gaps        : 0
  out-of-order events  : 0
  duplicate sequences  : 0
  crossed incidents    : 0
  books reconstructed  : 40
  books untrusted      : 0
```

Exit status: `0` clean, `1` could not run, `3` completed but found gaps,
malformed lines or untrusted books.

Replay uses the *same* `OrderBookManager` as the live engine and performs no
network I/O, so it is deterministic and offline. Verify a session round-trips:

```bash
./market_data_engine --config ../config/config.example.json --output /tmp/session
# ... Ctrl+C ...
./market_data_replay /tmp/session/raw/events-*.jsonl --print-digest \
  | sed -n '/Final-state digest/,$p' | tail -n +2 > /tmp/replay.digest
diff /tmp/session/final_state.digest /tmp/replay.digest && echo "books match"
```

This is the foundation for a backtesting simulator.

---

## Known limitations

* **Polymarket only.** The adapter interface is venue-agnostic, but only one
  adapter exists.
* **No Parquet yet.** CSV and JSONL first, behind the `SnapshotSink` interface
  so Arrow/Parquet is an additive change.
* **No Prometheus endpoint.** Metrics are printed periodically; an exporter
  would go behind an optional build flag.
* **Backpressure can cost a connection.** Under the default no-drop policy, a
  sustained slow consumer blocks the producer, which can eventually trigger a
  venue-side disconnect. That is recoverable and detectable, and preferred over
  silent data loss — but it is a real trade-off.
* **No absolute feed latency.** The two clocks have unrelated epochs, so only
  *relative* jitter between consecutive events is sound.
* **Trade side is venue-reported.** `aggressor_side` is trusted only when the
  venue states it; buy/sell volume is not otherwise inferred.
* **Fixtures are a point-in-time capture** (2026-09-27) and need periodic
  refresh against the live venue.
* **`std::map` order book.** Correct and maintainable, not optimal. The array
  ladder is the documented next step, once measurement justifies it.
* **Single websocket connection.** Very large token sets would want sharding
  across connections.
* **Snapshot cadence is fixed**, not event-driven. Research around trades will
  want snapshots anchored to trade timestamps; the pipeline is shaped for it
  (`ReplayEngine` already emits on recorded time) but it is not implemented.
* **Parquet volume scales with `tokens × time`**, not with market activity, so
  quiet markets cost the same as busy ones.
* **DuckDB views are created eagerly.** A view over a dataset with no files yet
  cannot be created, so views are (re)created at startup and again at
  shutdown; a database inspected mid-run may be missing the newest partition.
* **Trade capture is unverified against live data.** The path is covered by
  tests, but no trades occurred on the sampled markets during the runs so far.
* **No compaction.** Many short runs in one hour leave several small
  `books-N.parquet` files rather than one.

---

## Roadmap

1. Event-driven snapshots around trades, for markout and adverse-selection work.
2. A compaction tool to merge small Parquet files within a partition.
3. Optional Prometheus exporter behind a build flag.
4. Fixed-array price ladder, once profiling justifies it.
5. Sharded websocket connections for large subscription sets.
6. A second venue adapter, to prove the boundary holds.
7. Backtesting simulator built on the replay engine.
8. Continuous fixture refresh in CI to catch venue format drift.

---

## Engineering principles

Correctness before optimization · exchange-specific code isolated behind one
adapter · no global mutable state beyond a single logger · RAII throughout ·
value semantics and smart pointers · `std::chrono` for time · raw data
preserved · network I/O never blocks on disk I/O · replay is deterministic ·
tests before clever optimizations.

No strategy logic. No order placement. No trading credentials. No PnL.
