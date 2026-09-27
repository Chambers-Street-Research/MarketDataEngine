#!/usr/bin/env python3
"""Inspect data written by market_data_engine.

Reads the raw JSON Lines event log and/or the normalized snapshot CSV and
prints a summary: event mix, message rates, per-token coverage, feed latency
(exchange timestamp vs local receive timestamp) and book statistics.

Requires only the standard library. If pandas is installed, --pandas prints a
DataFrame-based summary of the snapshot CSV instead.

Usage:
    ./inspect_data.py data/raw/events-2026-09-27.jsonl
    ./inspect_data.py --snapshots data/normalized/snapshots-2026-09-27.csv
    ./inspect_data.py data/raw/events.jsonl --token <token_id> --limit 20
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import math
import pathlib
import statistics
import sys


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[index]


def inspect_raw(path: pathlib.Path, token_filter: str | None, limit: int) -> int:
    counts: collections.Counter[str] = collections.Counter()
    per_token: collections.Counter[str] = collections.Counter()
    markets: set[str] = set()
    connection_states: collections.Counter[str] = collections.Counter()

    malformed = 0
    sequences: list[int] = []
    receive_ns: list[int] = []
    printed = 0

    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                # A torn final record is normal after a hard kill.
                malformed += 1
                continue

            kind = event.get("event_type", "?")
            token = event.get("token_id", "")
            if token_filter and token != token_filter:
                continue

            counts[kind] += 1
            if token:
                per_token[token] += 1
            if event.get("market_id"):
                markets.add(event["market_id"])
            if kind == "connection":
                connection_states[event.get("state", "?")] += 1
            if isinstance(event.get("seq"), int):
                sequences.append(event["seq"])
            if isinstance(event.get("receive_ts"), int):
                receive_ns.append(event["receive_ts"])

            if limit and printed < limit:
                print(json.dumps(event)[:200])
                printed += 1

    total = sum(counts.values())
    print(f"\n=== raw event log: {path} ===")
    print(f"events            : {total:,}")
    print(f"malformed lines   : {malformed:,}")
    print(f"distinct markets  : {len(markets):,}")
    print(f"distinct tokens   : {len(per_token):,}")

    if sequences:
        expected = sequences[-1] - sequences[0] + 1
        gaps = expected - len(sequences)
        print(f"sequence range    : {sequences[0]:,} .. {sequences[-1]:,}")
        print(f"missing sequences : {gaps:,}" + ("  <-- recording has holes" if gaps else ""))

    if len(receive_ns) > 1:
        span_s = (receive_ns[-1] - receive_ns[0]) / 1e9
        if span_s > 0:
            print(f"wall-clock span   : {span_s:,.1f} s")
            print(f"mean message rate : {total / span_s:,.1f} events/sec")

    print("\nevent mix")
    for kind, count in counts.most_common():
        print(f"  {kind:<18} {count:>10,}  {100.0 * count / max(total, 1):5.1f}%")

    if connection_states:
        print("\nconnection states")
        for state, count in connection_states.most_common():
            print(f"  {state:<18} {count:>10,}")

    print("\nbusiest tokens")
    for token, count in per_token.most_common(10):
        print(f"  {token[:24]:<26} {count:>10,}")

    # No absolute exchange->receive latency is printed: exchange_ts is wall
    # clock and receive_ts is a steady clock with an unrelated epoch, so their
    # difference is meaningless. Comparing *deltas* between consecutive events
    # is the sound way to study feed jitter.
    return 0


def inspect_snapshots(path: pathlib.Path, token_filter: str | None, use_pandas: bool) -> int:
    if use_pandas:
        try:
            import pandas as pd
        except ImportError:
            print("pandas is not installed; falling back to the stdlib summary", file=sys.stderr)
        else:
            frame = pd.read_csv(path)
            if token_filter:
                frame = frame[frame["token_id"].astype(str) == token_filter]
            print(f"\n=== snapshots: {path} ===")
            print(f"rows: {len(frame):,}   tokens: {frame['token_id'].nunique():,}")
            print("\n", frame[["best_bid", "best_ask", "spread", "midpoint",
                               "imbalance_l1", "imbalance_l5"]].describe())
            return 0

    rows = 0
    tokens: set[str] = set()
    spreads: list[float] = []
    midpoints: list[float] = []
    imbalances: list[float] = []
    crossed = 0

    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            rows += 1
            tokens.add(row.get("token_id", ""))
            if token_filter and row.get("token_id") != token_filter:
                continue
            try:
                spread = float(row["spread"]) if row.get("spread") else math.nan
                midpoint = float(row["midpoint"]) if row.get("midpoint") else math.nan
                imbalance = float(row["imbalance_l1"]) if row.get("imbalance_l1") else math.nan
            except (KeyError, ValueError):
                continue
            if not math.isnan(spread):
                spreads.append(spread)
                # A negative spread in recorded output would mean the engine
                # published a crossed book, which it is designed never to do.
                if spread < 0:
                    crossed += 1
            if not math.isnan(midpoint):
                midpoints.append(midpoint)
            if not math.isnan(imbalance):
                imbalances.append(imbalance)

    print(f"\n=== snapshots: {path} ===")
    print(f"rows              : {rows:,}")
    print(f"distinct tokens   : {len(tokens):,}")
    print(f"rows with a spread: {len(spreads):,}")
    print(f"crossed rows      : {crossed:,}" + ("  <-- BUG" if crossed else "  (as expected)"))

    def describe(name: str, values: list[float]) -> None:
        if not values:
            print(f"{name:<18}: (none)")
            return
        print(f"{name:<18}: mean {statistics.fmean(values):8.4f}   "
              f"p50 {_percentile(values, 50):8.4f}   "
              f"p95 {_percentile(values, 95):8.4f}   "
              f"max {max(values):8.4f}")

    describe("spread", spreads)
    describe("midpoint", midpoints)
    describe("imbalance L1", imbalances)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", nargs="?", help="raw .jsonl event log")
    parser.add_argument("--snapshots", help="normalized snapshot .csv")
    parser.add_argument("--token", help="restrict the summary to one token id")
    parser.add_argument("--limit", type=int, default=0, help="print the first N raw events")
    parser.add_argument("--pandas", action="store_true",
                        help="use pandas for the snapshot summary if available")
    args = parser.parse_args()

    if not args.input and not args.snapshots:
        parser.error("give a raw .jsonl file, --snapshots <csv>, or both")

    status = 0
    if args.input:
        path = pathlib.Path(args.input)
        if not path.exists():
            print(f"no such file: {path}", file=sys.stderr)
            return 1
        status |= inspect_raw(path, args.token, args.limit)
    if args.snapshots:
        path = pathlib.Path(args.snapshots)
        if not path.exists():
            print(f"no such file: {path}", file=sys.stderr)
            return 1
        status |= inspect_snapshots(path, args.token, args.pandas)
    return status


if __name__ == "__main__":
    sys.exit(main())
