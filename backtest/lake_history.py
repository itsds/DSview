"""
lake_history.py

What a backtest reads from the lake (Phase 4's kickoff decision 3, in
docs/ROADMAP.md): one symbol's 1m klines from Bronze's `klines_1m`, every
stored day, oldest first; and, for a strategy that asks for it, each bar's
footprint rebuilt from Bronze's `agg_trades`. Both are live's Bronze, as
backfill/history_backfill.py writes it; backfill/bronze_files.py owns the
layout and is reused here rather than copied.

**Minutes with trades only.** `klines_1m` keeps Binance's zero-volume
klines, but `candles_1m` doesn't (backfill/candle_store.py skips them: the
live pipeline never makes such a candle), and Spark's EMAs come from
`candles_1m`. A zero-trade minute's close repeats the previous close, so
it only changes anything when it is a whole bar on its own (an exchange
outage on 1m, say), but then it would be a bar Spark never saw and every
EMA after it would differ.

**Closes as text**, exactly as stored: streaming/indicator_engine.py parses
each close with float(), as Spark's warm-up feeds it Postgres's
`close::text`. Python's float() of a decimal string is correctly rounded,
so the same decimal gives the same double either way, however many
trailing zeros it carries.

**The footprint** (`read_footprint()`): each day's aggTrades bucketed into
$1 levels per minute by backfill/footprint_rows.py, the same rows the
backfill loads into `footprint_1m`, then summed per bar and level. Only
bars inside the span the day files cover get one (bronze_files.coverage();
30 days by default): a bar half covered would show half its volume.

Author: @DS
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from bronze_files import AGG_TRADES, KLINES, coverage, day_path
from footprint_rows import footprint_table

# One footprint level of a bar: (price, buy volume, sell volume), the price
# the level's lower bound (config/settings.py's FOOTPRINT_PRICE_STEP).
Level = Tuple[Decimal, Decimal, Decimal]

_KLINE_COLUMNS = ["window_start", "open", "high", "low", "close", "volume", "trade_count"]


class NoHistory(Exception):
    """Bronze holds no klines for the symbol (run the backfill first)."""


@dataclass(frozen=True)
class Minutes:
    """A symbol's traded 1m klines, oldest first, as parallel lists."""

    start: List[int]  # Unix seconds, each minute's start
    open: List[Decimal]
    high: List[Decimal]
    low: List[Decimal]
    close: List[Decimal]
    volume: List[Decimal]

    def __len__(self) -> int:
        return len(self.start)


def kline_days(data_root: str, symbol: str) -> List[date]:
    """Every UTC day Bronze has a klines file for, oldest first."""
    symbol_dir = day_path(data_root, KLINES, symbol, date(2000, 1, 1)).parent.parent
    if not symbol_dir.is_dir():
        return []
    days = []
    for entry in symbol_dir.iterdir():
        if entry.name.startswith("date=") and (entry / KLINES.file_name).is_file():
            days.append(date.fromisoformat(entry.name[len("date="):]))
    return sorted(days)


def read_minutes(data_root: str, symbol: str) -> Minutes:
    """
    Every traded minute of `symbol` in Bronze's klines_1m, oldest first:
    all of it, however far before a backtest's start, because an EMA
    depends on every bar before it.
    """
    tables = []
    for day in kline_days(data_root, symbol):
        table = pq.read_table(day_path(data_root, KLINES, symbol, day), columns=_KLINE_COLUMNS)
        table = table.filter(pc.greater(table["trade_count"], 0))
        tables.append(table)
    if not tables:
        raise NoHistory(
            f"no klines for {symbol} in {Path(data_root) / 'bronze' / KLINES.name}: "
            "run the backfill first (backfill/history_backfill.py)"
        )
    table = pa.concat_tables(tables)
    seconds = pc.divide(table["window_start"].cast(pa.int64()), 1_000_000)  # microseconds, UTC
    table = table.append_column("seconds", seconds).sort_by("seconds")
    if table.num_rows == 0:
        raise NoHistory(f"Bronze's klines for {symbol} have no minute with a trade")
    return Minutes(
        start=table["seconds"].to_pylist(),
        open=table["open"].to_pylist(),
        high=table["high"].to_pylist(),
        low=table["low"].to_pylist(),
        close=table["close"].to_pylist(),
        volume=table["volume"].to_pylist(),
    )


def read_footprint(
    data_root: str, symbol: str, bar_seconds: int, start: int, end: int
) -> Dict[int, Tuple[Level, ...]]:
    """
    Each bar's footprint, by bar start (Unix seconds), for the bars from
    `start` to `end` that Bronze's aggTrades cover whole; prices ascending.
    Every chart timeframe divides a day, so a bar never spans two day files.
    """
    footprints: Dict[int, Tuple[Level, ...]] = {}
    day = datetime.fromtimestamp(start, timezone.utc).date()
    last = datetime.fromtimestamp(max(start, end - 1), timezone.utc).date()
    while day <= last:
        path = day_path(data_root, AGG_TRADES, symbol, day)
        span = coverage(path)
        if span is not None:
            covered_from, covered_until = span[0] // 1000, span[1] // 1000
            rows = footprint_table(pq.read_table(path, columns=["event_time", "price", "quantity", "side"]))
            footprints.update(_bars(rows, bar_seconds, max(start, covered_from), min(end, covered_until)))
        day = date.fromordinal(day.toordinal() + 1)
    return footprints


def _bars(rows: pa.Table, bar_seconds: int, start: int, end: int) -> Dict[int, Tuple[Level, ...]]:
    """footprint_1m-shaped rows summed into whole bars inside [start, end)."""
    first_bar = -(-start // bar_seconds) * bar_seconds  # the first bar starting at or after `start`
    last_bar = end // bar_seconds * bar_seconds  # bars from here on end after `end`
    if rows.num_rows == 0 or first_bar >= last_bar:
        return {}
    minute = pc.divide(rows["window_start"].cast(pa.int64()), 1_000_000)
    bar = pc.multiply(pc.divide(minute, bar_seconds), bar_seconds)  # integer division floors: times are positive
    rows = rows.append_column("bar", bar)
    rows = rows.filter(pc.and_(pc.greater_equal(rows["bar"], first_bar), pc.less(rows["bar"], last_bar)))
    summed = rows.group_by(["bar", "price"]).aggregate([("buy_volume", "sum"), ("sell_volume", "sum")])
    summed = summed.sort_by([("bar", "ascending"), ("price", "ascending")])
    bars: Dict[int, List[Level]] = {}
    for bar_start, price, buy, sell in zip(
        summed["bar"].to_pylist(),
        summed["price"].to_pylist(),
        summed["buy_volume_sum"].to_pylist(),
        summed["sell_volume_sum"].to_pylist(),
    ):
        bars.setdefault(bar_start, []).append((price, buy, sell))
    return {bar_start: tuple(levels) for bar_start, levels in bars.items()}
