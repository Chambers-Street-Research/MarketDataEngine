# Architecture

## 1. Scope

This repository is market-data infrastructure only. It discovers markets,
maintains order books, and records data. It contains no strategy logic, no
order placement, no portfolio or PnL code, and no credentials. It reads public
endpoints only.

## 2. Component map

```text
                   +----------------------+
                   | Gamma REST discovery |
                   +----------+-----------+
                              |
                              v
                   +----------------------+
                   |   MarketRegistry     |   token -> market, market -> tokens,
                   +----------+-----------+   outcome, category, resolution time
                              |
                              v
+-------------+     +----------------------+     +--------------------+
| WebSocket   | --> | PolymarketAdapter    | --> | polymarket_parser  |
| (Beast/TLS) |     | (the only venue-     |     | (pure, no I/O)     |
+-------------+     |  aware component)    |     +---------+----------+
                    +----------------------+               |
                                                 normalized MarketEvent
                                                           |
                                                 +---------v------------+
                                                 |  BoundedQueue        |
                                                 +---------+------------+
                                                           |
                               +---------------------------+-----------------+
                               |                           |                 |
                               v                           v                 v
                      +----------------+         +----------------+  +----------------+
                      | OrderBook      |         | RawRecorder    |  | Metrics        |
                      | Manager        |         | (JSONL)        |  |                |
                      +-------+--------+         +----------------+  +----------------+
                              |
                              v
                      +----------------------+
                      |   StorageManager     |   the only persistence facade
                      +----------+-----------+
                                 |
            +--------------------+---------------------+
            v                                          v
   +------------------+                     +---------------------+
   | raw worker       |                     | parquet worker      |
   | RawEventArchive  |                     | ParquetStorageWriter|
   | JSONL + zstd     |                     | books/trades/markets|
   +------------------+                     +----------+----------+
                                                       |
                                            +----------v----------+
                                            |   DuckDbCatalog     |
                                            |   (views only)      |
                                            +---------------------+
```

`ExchangeAdapter` is the venue boundary. Everything to its right consumes
`MarketEvent` and knows nothing about Polymarket. Adding another prediction
market means one new `ExchangeAdapter` implementation and nothing else.

## 3. Data ingestion path

1. **Discovery.** `PolymarketAdapter::discover_markets()` pages
   `GET https://gamma-api.polymarket.com/markets` ordered by `volumeNum`.
   Two encoding quirks are handled in the parser, both verified against the
   live API: `clobTokenIds` and `outcomes` are JSON-encoded *strings* that need
   a second parse, and there is no `category` field — the closest stable
   grouping is `events[0].ticker`.
2. **Registry load.** Markets go into `MarketRegistry`, which maintains the
   token→market index and every required lookup.
3. **Subscription.** The top *N* markets by volume contribute their tokens.
   Ranking is a `stable_sort` over an id-ordered map with `market_id` breaking
   volume ties, so the same input always selects the same tokens — which is
   what makes a replay comparable to the live run that produced it.
4. **Websocket.** One TLS websocket to `wss://ws-subscriptions-clob.polymarket.com/ws/market`,
   subscribed with `{"assets_ids": [...], "type": "market"}`.
5. **Parse and normalize.** `polymarket_parser` converts each frame into zero
   or more `MarketEvent`s. It performs no I/O and is unit-tested against
   fixtures captured from the live venue.
6. **Sequence and enqueue.** The adapter's event sink stamps a monotonically
   increasing `sequence` and pushes onto the bounded queue.

### Wire-format details that are not in the published schema

Verified by probing the live venue on 2026-09-27:

| Behaviour | Why it matters |
|---|---|
| A frame may be a **JSON array** of events or a bare object | Initial book snapshots arrive batched; assuming an object drops the entire first burst. |
| The heartbeat reply `PONG` is a **raw non-JSON text frame** | Feeding it to a JSON parser logs a spurious "malformed message" every 10 seconds. |
| Live `book` frames carry `tick_size` and `last_trade_price` | Useful, and undocumented. |
| One `price_change` frame can touch **several tokens** | Both legs of a binary market update together. The parser groups by `asset_id` and emits one event per book. |
| `price_change.size` is the **absolute** resting size, not a delta | Treating it as a delta corrupts the book immediately. |
| `side: BUY` means a **bid** level; `SELL` an **ask** level | It describes the resting order, not an aggressor. |

## 4. Threading model

| Thread | Owns (exclusively) | Blocks on |
|---|---|---|
| Adapter I/O (one `io_context`, one strand) | socket, TLS, outbox, backoff timers, subscription set | network readiness only |
| Consumer | `OrderBookManager`, `Metrics` | the event queue |
| Timer | snapshot generation, book maintenance, status, market refresh | its own sleep |
| Storage: raw worker | `RawEventArchive` | the raw queue (100 ms poll) |
| Storage: Parquet worker | `ParquetStorageWriter` and its Arrow builders | the Parquet queue (100 ms poll) |
| Main | signal handling | a stop flag |
| (Transient) resync | one blocking HTTPS GET per invalid book | that request |

**Exclusive ownership is load-bearing, not stylistic.** The Arrow builders and
the archive's staging buffer are not thread safe. An early version had
`StorageManager::flush()` call the writers directly from the engine's timer
thread while the workers were appending to the same objects; the race silently
dropped roughly 10% of rows with no error and no drop count — a measured
4,488 of 5,000 in the regression test that now guards it. `flush()` is
therefore a *request*: it sets an atomic flag that the owning worker honours on
its next pass. The workers poll with a timeout rather than blocking
indefinitely, so a flush request is still honoured on an idle queue.

The only point where another thread touches a writer is inside
`StorageManager::shutdown()`, *after* both workers have been joined and this
thread is provably their sole owner.

**Network I/O never blocks on disk I/O.** The adapter thread's only
interaction with persistence is an in-memory `push()`. All file writes happen
on the consumer and timer threads.

The websocket session is confined to a single strand, so the socket, outbox,
backoff state and subscription list need no locking among themselves. Only
state genuinely shared across threads (`connected_`, `running_`, the counters)
is atomic.

Resync is deliberately *not* run on the adapter's I/O thread: a blocking REST
fetch there would stall the live feed for every other token.

## 5. Event ordering

`sequence` is assigned in the adapter's event sink, which runs on the single
I/O thread. That makes it a true total order over ingested events, not a
partial one. The raw log is written in that order, so:

* file order == sequence order == ingestion order
* replay reads the file top to bottom and needs no sorting
* a gap or repeat in `seq` during replay is a detected, reported defect

Two clocks are preserved on every event. `exchange_timestamp` is wall clock
from the venue, for cross-venue ordering and latency research.
`receive_timestamp` is a local *steady* clock, which never jumps and so
survives an NTP step mid-session. They have unrelated epochs and must never be
subtracted from one another.

## 6. Order-book reconstruction

### Representation

Prices are `PriceTicks` (`int32_t`) on a 1/1000 grid; quantities are
`QtyUnits` (`int64_t`) in micro-units.

Polymarket quotes on a 0.01 or 0.001 tick and sizes are USDC-denominated with
at most six decimals, so both grids are represented **exactly**. This buys two
things that matter more than speed:

* `quantity == 0` — "delete this level" — is an exact integer test. With
  doubles it is an epsilon comparison, and the wrong epsilon leaves ghost
  levels at `1e-18` sitting at the top of the book forever.
* Two prices are equal iff their ticks are equal. No `fabs(a - b) < eps`
  anywhere in book maintenance.

Doubles appear only at the API boundary, where the outside world expects
decimal prices.

### Container choice

```cpp
std::map<PriceTicks, QtyUnits, std::greater<PriceTicks>>  // bids, best first
std::map<PriceTicks, QtyUnits, std::less<PriceTicks>>     // asks, best first
```

| Operation | Cost |
|---|---|
| best bid / best ask | O(1) — always `begin()` |
| top N levels | O(n), ordered iteration, no sort |
| insert / update / erase a level | O(log n) |

The alternative — `unordered_map` plus cached best prices — makes the common
update faster but turns "the best level was just deleted" into a full scan and
top-N into a sort. Prediction-market books are shallow (tens of levels) and
update at modest rates, so the ordered map wins on correctness per line of
code, which is the stated priority for a first implementation.

**The optimization path, when measurement justifies it:** prices are bounded to
0..1000 ticks, so an entire side fits in a fixed 1001-slot array ladder with
O(1) update and a bitset to locate best-of-book. That is a contained change
behind the existing `OrderBook` interface, and the test suite already pins the
behaviour it must preserve.

### Crossed books

A crossing update is **applied, then reported** — not silently rejected. The
feed is describing real state; discarding it would hide the fact that the local
view has diverged. `OrderBook` counts the incident, `OrderBookManager` drives
the book to `Recovering` and requests a snapshot. A locked book (`bid == ask`)
counts as crossed: it is not quotable state on this venue.

## 7. Health states and resync

```text
Initializing --snapshot--> Healthy
     ^                        |
     |          no message for stale_after_ms
     |                        v
     |                     Stale --update--> Healthy
     |                        |
     |    crossed book, phantom delete, tick re-grid, or disconnect
     |                        v
     +---- snapshot ----- Recovering <-- resync requested
                              |
                    resync repeatedly failed
                              v
                           Invalid
```

**The rule that governs everything else: a book is never `Healthy` on the
strength of incremental updates alone.** It must have been seeded by a snapshot
since the last invalidation. Applying increments to a book with an unknown base
state produces a plausible-looking and wrong book, which is far worse than
publishing nothing.

Invalidation triggers:

* **Crossed book** — the local view diverged.
* **Phantom delete** — we were told to remove a level we never had. Harmless
  once; a sign of drift, and only a snapshot can fix it.
* **Tick-size change** — the venue re-gridded; levels on the old grid may no
  longer exist.
* **Any connection gap** — `Disconnected`, `Reconnecting` or `Failed`
  invalidates *every* book. A local book is never assumed valid across a gap.

`Stale` is informational, not a fault. A thin prediction market can go minutes
without a quote change; the book stays usable and is still exported.

### Reconnect sequence

1. Reconnect with exponential backoff (`initial_backoff_ms` ×
   `backoff_multiplier`, capped at `max_backoff_ms`) plus random jitter up to
   `jitter_fraction`, so a fleet of collectors does not stampede the venue in
   lockstep after an outage.
2. Resubscribe the full token set.
3. The venue answers a subscription with a fresh `book` for every token.
4. Only then does each book return to `Healthy`.

Bulk invalidation deliberately does *not* fire N REST resyncs — the
resubscription already produces a snapshot per token, which is cheaper and more
correct. Per-token resync over `GET /book` is reserved for a single book that
went bad while the connection is otherwise fine, and is rate-limited by
`resync_cooldown_ms` so a persistently bad feed cannot become a REST hot loop.

## 8. Queue and backpressure

Capacity is `event_queue_capacity` (default 262,144). At observed rates that is
roughly a minute of buffer — enough to ride out a slow `fsync` or a log
rotation without ever touching the network thread.

| Policy | Behaviour |
|---|---|
| `Block` (default) | The producer waits. **Events are never dropped.** Backpressure reaches the socket, TCP's window closes, and the venue eventually disconnects us — which triggers reconnect and resync, a recoverable *and detectable* outcome. |
| `DropNewest` | The event is discarded and counted. Latency is preserved at the cost of a gap. |

**Can events be dropped?** Under the default policy, no. `push()` returns false
only after `close()`, which happens during shutdown *after* ingestion stops.

**How is a drop detected?** Every drop increments `dropped_count()`, which
appears in the status line and the shutdown summary. A non-zero drop count
means the recorded session has a hole and must not be treated as complete —
replay will also report the resulting `seq` gap independently.

## 9. Persistence

Three layers, each rebuildable from the one above it.

```text
RAW          data/raw/YYYY/MM/DD/events_HHMM.jsonl.zst
                 every event, ingestion order, hourly files
                     |  replay
                     v
NORMALIZED   data/{books,trades,markets}/date=.../hour=.../*.parquet
                 fixed-cadence snapshots, columnar, zstd
                     |  read_parquet
                     v
QUERY        data/db/market_data.duckdb
                 views only; no data copied in
```

### Raw archive

Append-only JSON Lines, optionally zstd-compressed, rotated hourly and on
size. Nested `YYYY/MM/DD` directories keep a listing usable after months of
collection.

Events are recorded **before** they are applied to the books, so a crash
between the two leaves the log a superset of what the books saw, never a
subset — a replay missing events the live engine acted on would be
undebuggable.

Compression is streaming with `ZSTD_e_flush` at each flush interval, so the
file stays decodable up to the last flush; a kill costs at most
`flush_interval_ms` of events, not the hour. A checksum is written into the
frame so truncation is detectable rather than silently yielding garbage. Files
are written as `.partial` and renamed on close.

This is the only layer whose failure is fatal at startup. Parquet and DuckDB
are derived and can be regenerated; nothing can regenerate the archive.

### Normalized Parquet

One row per token per `snapshot_interval_ms` (default 1/sec), not per update.
The raw archive keeps every tick; this layer is deliberately the coarser
research view.

Hive-partitioned by `date` and (optionally) `hour`, so DuckDB's
`hive_partitioning = true` exposes both as ordinary columns without storing
them in the files.

**One file per partition, not per flush.** Each flush appends a row group to
the open `parquet::arrow::FileWriter`; the file is closed and renamed when the
partition rolls or at shutdown. File count therefore tracks elapsed hours, not
message volume — which is what keeps the dataset fast to scan later.

Flush triggers, whichever comes first: `batch_rows` buffered (10,000),
`flush_interval_seconds` elapsed (10), a partition change, or shutdown.

**Nullability is semantic.** Every nullable column is written as a real
Parquet null, never a sentinel:

* Zero is a *valid* price on a prediction market, so "no bid" cannot be `0.0`.
  `best_bid_ticks` is the authoritative presence signal.
* NaN — the engine's internal "undefined" — is converted to null on write.
  Leaving NaN in the file would make every downstream `AVG()` and comparison
  silently wrong.
* An unreported trade aggressor is `side = 'unknown'`, never guessed. A
  fabricated side would corrupt exactly the trade-flow and adverse-selection
  research this dataset exists to support.

Both integer ticks and decimal prices are persisted so researchers never
re-derive them.

### File safety

A partition is written to `<name>.parquet.tmp` and atomically renamed once the
footer is on disk. A crash leaves a `.tmp` that is obviously incomplete, safe
to ignore or delete, and that interval is recoverable from the raw archive. A
restart within a live partition writes `books-1.parquet` rather than
clobbering the finished file from the previous run.

### DuckDB catalog

Views over the Parquet globs — nothing is copied in. The database file stays a
few hundred KB, the datasets remain portable and readable without DuckDB, and
a crashed collector cannot corrupt the query layer.

DuckDB resolves `read_parquet` globs eagerly, so a view over a dataset with no
files yet cannot be created. That is the normal state before the first flush,
so such views are skipped and reported rather than treated as an error. Views
are refreshed at startup and again at shutdown, once the session's partitions
have been published. A database inspected mid-run may therefore be missing the
newest partition.

`CHECKPOINT` runs before the handle is dropped, so the file on disk is
complete rather than depending on WAL replay.

### Storage queues and backpressure

Two bounded queues, one per worker, so a slow Parquet flush cannot stall the
raw archive — the layer everything else is rebuilt from.

Neither producer is the network thread: the engine's consumer thread enqueues
raw events and the timer thread enqueues snapshots, so storage backpressure
reaches ingestion only indirectly, through the engine's own event queue.

Default policy is block-never-drop, as for the ingestion queue. Depth crossing
`queue_warn_fraction` (0.8) of capacity logs a **critical** warning, latched so
a sustained backlog logs once rather than per event. Under the optional
`DropNewest` policy every drop is counted and the shutdown summary marks the
interval `INCOMPLETE`.

Tracked: queue depth, high-water mark, events queued, events written, events
dropped, flush count, total and maximum flush latency, storage errors.

### Reported vs. actual

The shutdown summary reports snapshots *generated* (counted by the engine) and
snapshots *persisted* (counted by the storage layer) as separate numbers, and
warns when they disagree. They are produced by different subsystems on
different threads, and collapsing them into one figure is precisely what hid
the flush race described in §4. A divergence is the evidence.

## 9a. Rebuilding the normalized dataset

Because the raw archive is authoritative, the Parquet and DuckDB layers are
regenerable. `market_data_replay --write-dataset <root>` drives the recorded
log through the same `OrderBookManager` and the same `StorageManager` the live
engine uses, emitting snapshots on the recorded receive-time cadence rather
than wall-clock.

That cadence choice is what keeps the rebuild deterministic and comparable to
the live run: the same log always produces the same rows. One asymmetry is
expected and harmless — the live engine keeps snapshotting during quiet
periods after the last event, so a live session can hold slightly more
snapshots than a rebuild of its own log.

Rebuilding never touches the archive it reads from: `raw_events.enabled` is
forced off for the replay's storage manager.

## 10. Replay determinism

`ReplayEngine` feeds decoded events into the **same** `OrderBookManager` the
live engine uses. No book-construction, validation or health logic is
reimplemented, so a live/replay divergence can only come from the recording
itself.

Replay performs **no network I/O**. A resync the live engine would have issued
is counted, not executed — otherwise a replay would depend on the venue being
reachable and on what it says *today*, which has nothing to do with the
recorded session.

Verification is concrete: on shutdown the engine writes
`final_state.digest` — a sorted, per-token record of every book's final top of
book and true ladder depth. `market_data_replay --print-digest` over that
session's raw log must reproduce it exactly. The digest uses true ladder depth
rather than the truncated display view, so it cannot be defeated by a `--depth`
flag.

## 11. Known failure modes

| Failure | Handling | Residual risk |
|---|---|---|
| Websocket disconnect | Exponential backoff + jitter, resubscribe, re-snapshot, then healthy | Data for the gap is lost; the gap is visible as a `connection` event in the log |
| Silent socket (no TCP error) | `read_timeout_ms` deadline tears it down | Up to `read_timeout_ms` of stale data before detection |
| Malformed JSON / missing fields | Counted, logged at debug, skipped | A venue-side format change degrades silently; watch `invalid messages` |
| Unknown `event_type` | Counted and ignored, never fatal | New venue features go uncollected until implemented |
| Duplicate snapshot | Skipped by venue hash comparison | — |
| Crossed or drifted book | Invalidated, resynced | Book is unavailable during recovery |
| Consumer falls behind | Blocks the producer; never drops | Backpressure can eventually cause a venue-side disconnect |
| Disk full | Writes fail, logged at error, engine keeps running | Data loss; `bytes written` stops advancing |
| Clock step (NTP) | Book logic uses the steady clock only | `exchange_ts` in already-written records is unaffected |
| Venue changes the wire format | Parser tests fail against fixtures | Fixtures are a point-in-time capture and need periodic refresh |
| Crash mid-Parquet-write | `.tmp` left behind, obviously incomplete | That partition is lost; rebuild it from the raw archive by replay |
| Crash mid-raw-write | zstd frame flushed at the last interval; `.partial` left behind | At most `flush_interval_ms` of events lost |
| Storage worker falls behind | Bounded queue blocks the producer; critical warning at 80% depth | Backpressure reaches ingestion through the engine queue |
| Arrow or DuckDB missing at build time | Feature auto-disables with a warning; raw archive unaffected | No columnar dataset until rebuilt with `--write-dataset` |
| Many short runs in one hour | Suffixed `books-N.parquet` files rather than one | More small files; no compaction tool yet |
| Market resolves mid-session | Metadata refresh drops it from the ranking | Books for a resolved token go `Stale` until the next refresh |

## 12. Testing

152 tests across order book, parser (against live-captured fixtures), registry,
codec round-trip, book-manager health machine, bounded queue (including
multi-producer no-loss), recorder rotation, config validation, replay
determinism, the Parquet writer, storage-queue conservation, and the
Parquet → DuckDB integration path.

The storage tests assert *conservation*: every row handed to `StorageManager`
must appear on disk or be counted as dropped. Silent loss is the one outcome
that would make the dataset untrustworthy, and it is the bug that actually
occurred (§4), so it is pinned by a regression test that fails against the
pre-fix code.

53 of those tests skip explicitly when the build has no Arrow or DuckDB, rather
than passing vacuously.

The build is warning-clean under `-Wall -Wextra -Wpedantic -Wshadow
-Wconversion -Wsign-conversion`, and CI-style builds can add `-DMDE_WERROR=ON`.
