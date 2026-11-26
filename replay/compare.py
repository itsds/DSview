"""
compare.py

Checks a replay against what it replayed (Phase 4a's definition of done):
every minute of the replay's `candles_1m` against the reference's, field by
field, exactly.

- **gold**: live's Gold candles (`gold/candles_1m`), what the live stream
  finalized from the same trades a replay of Bronze's `trades` plays.
  Live's `candles_1m` won't do: a backfill replaces its live minutes with
  the exchange's. Gold is a Spark file sink, read through its log like
  Bronze (bronze_source.committed_files). OHLCV and the trade count.
- **klines**: Binance's own 1m klines (`bronze/klines_1m`), what a replay of
  the backfill's aggTrades should rebuild. OHLCV only: an aggregate trade
  counts as one where a kline counts every fill.

Minutes only the replay has are listed but don't fail the check (Gold
lacks a minute live never finalized: the last one before the writer
stopped). A minute only the reference has, or one that differs, fails it.

Run from replay/ after a replay (replay_service.py --once):

    python compare.py --symbol BTCUSDT --start 2026-11-20T00:00Z --end 2026-11-21T00:00Z --against gold

Author: @DS
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, Tuple

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from config.settings import LIVE_PIPELINE, REPLAY_PIPELINE

from bronze_source import TIMESTAMP, committed_files

GOLD, KLINES = "gold", "klines"
OHLCV = ("open", "high", "low", "close", "volume")
SHOWN_DIFFERENCES = 10

Candle = Dict[str, object]


def replay_candles(symbol: str, start: datetime, end: Optional[datetime]) -> Dict[datetime, Candle]:
    query = "SELECT window_start, open, high, low, close, volume, trade_count FROM candles_1m WHERE symbol = %s AND window_start >= %s"
    params = [symbol, start]
    if end is not None:
        query += " AND window_start < %s"
        params.append(end)
    with psycopg.connect(REPLAY_PIPELINE.postgres_dsn) as conn:
        rows = conn.execute(query, params).fetchall()
    return {row[0]: dict(zip(("open", "high", "low", "close", "volume", "trade_count"), row[1:])) for row in rows}


def gold_candles(data_root: str, symbol: str, start: datetime, end: Optional[datetime]) -> Dict[datetime, Candle]:
    dataset = Path(data_root) / "gold" / "candles_1m"
    files = [f.path for f in committed_files(dataset) if f.path.parent.name == f"symbol={symbol}"]
    candles = {}
    for path in files:
        table = pq.read_table(path, columns=["window_start", *OHLCV, "trade_count"])
        starts = table.column("window_start").cast(pa.timestamp("us")).cast(TIMESTAMP).to_pylist()
        for i, minute in enumerate(starts):
            if minute >= start and (end is None or minute < end):
                candles[minute] = {name: table.column(name)[i].as_py() for name in (*OHLCV, "trade_count")}
    return candles


def kline_candles(data_root: str, symbol: str, start: datetime, end: Optional[datetime]) -> Dict[datetime, Candle]:
    candles = {}
    day = start.date()
    last = (end or datetime.now(timezone.utc)).date()
    while day <= last:
        path = Path(data_root) / "bronze" / "klines_1m" / f"symbol={symbol}" / f"date={day.isoformat()}" / "klines.parquet"
        if path.exists():
            for row in pq.read_table(path, columns=["window_start", *OHLCV, "trade_count"]).to_pylist():
                minute = row["window_start"]
                if row["trade_count"] and minute >= start and (end is None or minute < end):
                    candles[minute] = {name: row[name] for name in OHLCV}
        day += timedelta(days=1)
    return candles


def compare(replayed: Dict[datetime, Candle], reference: Dict[datetime, Candle], fields: Tuple[str, ...]) -> Dict[str, list]:
    both = sorted(replayed.keys() & reference.keys())
    differ = []
    for minute in both:
        diffs = {f: (replayed[minute][f], reference[minute][f]) for f in fields if _value(replayed[minute][f]) != _value(reference[minute][f])}
        if diffs:
            differ.append((minute, diffs))
    return {
        "both": both,
        "differ": differ,
        "only_replay": sorted(replayed.keys() - reference.keys()),
        "only_reference": sorted(reference.keys() - replayed.keys()),
    }


def _value(value: object) -> object:
    return Decimal(value) if isinstance(value, (Decimal, str)) else value  # NUMERIC compares by value: 1.50 == 1.5


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Compare a replay's candles with what it replayed")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end")
    parser.add_argument("--against", choices=[GOLD, KLINES], required=True)
    args = parser.parse_args(argv)
    start = _time(args.start)
    end = _time(args.end) if args.end else None

    replayed = replay_candles(args.symbol, start, end)
    if args.against == GOLD:
        reference, fields = gold_candles(LIVE_PIPELINE.data_root, args.symbol, start, end), (*OHLCV, "trade_count")
    else:
        reference, fields = kline_candles(LIVE_PIPELINE.data_root, args.symbol, start, end), OHLCV
    result = compare(replayed, reference, fields)

    print(f"{len(replayed):,} replayed minutes, {len(reference):,} {args.against} minutes, {len(result['both']):,} in both")
    print(f"  equal: {len(result['both']) - len(result['differ']):,}   differ: {len(result['differ']):,}")
    for minute, diffs in result["differ"][:SHOWN_DIFFERENCES]:
        shown = ", ".join(f"{f} {r} vs {g}" for f, (r, g) in diffs.items())
        print(f"    {minute:%Y-%m-%d %H:%M}  {shown}")
    for key, label in (("only_replay", "only in the replay"), ("only_reference", f"only in {args.against}")):
        minutes = result[key]
        if minutes:
            shown = ", ".join(f"{m:%H:%M}" for m in minutes[:SHOWN_DIFFERENCES])
            print(f"  {label}: {len(minutes):,} ({shown}{', …' if len(minutes) > SHOWN_DIFFERENCES else ''})")
    return 1 if result["differ"] or result["only_reference"] or not result["both"] else 0


def _time(text: str) -> datetime:
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    sys.exit(main())
