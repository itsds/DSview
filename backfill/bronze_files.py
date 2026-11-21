"""
bronze_files.py

Bronze's backfilled datasets: what history_backfill.py fetched from
Binance, kept raw (unaggregated, every column Binance gives) as Parquet,
the durable copy that replay and backtests (Phase 4) and the footprint's
history (Phase 3b) read, and that candles_1m can be reloaded from.

    DATA_ROOT/bronze/klines_1m/symbol=BTCUSDT/date=2026-11-20/klines.parquet
    DATA_ROOT/bronze/agg_trades/symbol=BTCUSDT/date=2026-11-20/agg_trades.parquet

**Separate from `bronze/trades`.** The live Bronze dataset is a Spark
streaming file sink: readers list its files from its `_spark_metadata`
log, so a file written there by anything else would be invisible. And its
rows are individual trades, while aggTrades are aggregates with ids of
their own; the two overlap in time and must never be summed together.
Hive-style `symbol=`/`date=` directories, as Spark writes, so
`spark.read.parquet(".../bronze/agg_trades")` discovers both as columns
(which is also why neither is a column inside the files).

**One file per symbol and UTC day, replaced whole.** A day is written to a
hidden temporary file in its directory (Spark skips names starting with a
dot) and renamed over the old file only once complete, so a reader never
sees half a day and an interrupted run leaves the previous file intact.
Each file records the span it covers in its Parquet metadata
(`coverage()`): a rerun skips a day whose file already covers what it
would fetch, and refetches a day it only has part of (today's, say). That
is what makes reruns safe and cheap: no duplicate rows, nothing fetched
twice.

Author: @DS
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterator, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from binance_rest import AGG_TRADE_COLUMNS, KLINE_COLUMNS, Page

_DECIMAL = pa.decimal128(38, 18)  # trades_source.py's DECIMAL_TYPE, so Spark reads both alike
_TIMESTAMP = pa.timestamp("us", tz="UTC")  # Spark's own precision
_COVERS_FROM = b"dsview.covers_from_ms"
_COVERS_UNTIL = b"dsview.covers_until_ms"
_SOURCE = b"dsview.source"

KLINES_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("window_start", _TIMESTAMP),
        ("window_end", _TIMESTAMP),
        ("open", _DECIMAL),
        ("high", _DECIMAL),
        ("low", _DECIMAL),
        ("close", _DECIMAL),
        ("volume", _DECIMAL),
        ("quote_volume", _DECIMAL),
        ("trade_count", pa.int64()),
        ("taker_buy_volume", _DECIMAL),
        ("taker_buy_quote_volume", _DECIMAL),
        ("ingested_at", _TIMESTAMP),
    ]
)
AGG_TRADES_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("agg_trade_id", pa.int64()),
        ("event_time", _TIMESTAMP),
        ("price", _DECIMAL),
        ("quantity", _DECIMAL),
        ("side", pa.string()),
        ("first_trade_id", pa.int64()),
        ("last_trade_id", pa.int64()),
        ("is_best_match", pa.bool_()),
        ("ingested_at", _TIMESTAMP),
    ]
)


@dataclass(frozen=True)
class Dataset:
    name: str  # directory under bronze/
    file_name: str
    schema: pa.Schema
    page_columns: Tuple[str, ...]
    source: str


KLINES = Dataset("klines_1m", "klines.parquet", KLINES_SCHEMA, KLINE_COLUMNS, "binance-rest-klines-1m")
AGG_TRADES = Dataset("agg_trades", "agg_trades.parquet", AGG_TRADES_SCHEMA, AGG_TRADE_COLUMNS, "binance-rest-aggTrades")


def day_path(data_root: str, dataset: Dataset, symbol: str, day: date) -> Path:
    return Path(data_root) / "bronze" / dataset.name / f"symbol={symbol}" / f"date={day.isoformat()}" / dataset.file_name


def coverage(path: Path) -> Optional[Tuple[int, int]]:
    """The [from, until) span (Unix ms) a day file holds, or None if there's no readable file."""
    try:
        metadata = pq.read_schema(path).metadata or {}
        return int(metadata[_COVERS_FROM]), int(metadata[_COVERS_UNTIL])
    except (FileNotFoundError, KeyError, ValueError, pa.ArrowInvalid):
        return None


def covers(path: Path, start_ms: int, end_ms: int) -> bool:
    span = coverage(path)
    return span is not None and span[0] <= start_ms and span[1] >= end_ms


def page_table(dataset: Dataset, page: Page, exchange: str = "binance") -> pa.Table:
    """A normalized page (binance_rest.py's columns) as a table in the dataset's schema."""
    size = len(page[dataset.page_columns[0]])
    arrays = []
    for field in dataset.schema:
        if field.name == "exchange":
            arrays.append(pa.array([exchange] * size, pa.string()))
        elif field.name == "window_end":
            arrays.append(pa.array([start * 1000 + 60_000_000 for start in page["window_start"]], pa.int64()).cast(_TIMESTAMP))
        elif field.type == _TIMESTAMP:
            arrays.append(pa.array([ms * 1000 for ms in page[field.name]], pa.int64()).cast(_TIMESTAMP))
        elif field.type == _DECIMAL:
            arrays.append(pa.array(page[field.name], pa.string()).cast(_DECIMAL))
        else:
            arrays.append(pa.array(page[field.name], field.type))
    return pa.Table.from_arrays(arrays, schema=dataset.schema)


class DayWriter:
    """Writes one day file a table at a time; commit() puts it in place, abandon() leaves the old one."""

    def __init__(self, path: Path, dataset: Dataset, start_ms: int, end_ms: int) -> None:
        self.path = path
        self.rows = 0
        path.parent.mkdir(parents=True, exist_ok=True)
        self._tmp = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
        schema = dataset.schema.with_metadata(
            {_COVERS_FROM: str(start_ms), _COVERS_UNTIL: str(end_ms), _SOURCE: dataset.source}
        )
        self._writer = pq.ParquetWriter(self._tmp, schema)

    def write(self, table: pa.Table) -> None:
        if table.num_rows:
            self._writer.write_table(table)
            self.rows += table.num_rows

    def commit(self) -> None:
        self._writer.close()
        os.replace(self._tmp, self.path)

    def abandon(self) -> None:
        self._writer.close()
        self._tmp.unlink(missing_ok=True)


@contextmanager
def day_writer(data_root: str, dataset: Dataset, symbol: str, day: date, start_ms: int, end_ms: int) -> Iterator[DayWriter]:
    """A DayWriter committed when the block finishes, abandoned if it raises (or is cancelled)."""
    writer = DayWriter(day_path(data_root, dataset, symbol, day), dataset, start_ms, end_ms)
    try:
        yield writer
    except BaseException:
        writer.abandon()
        raise
    writer.commit()
