"""
bronze_source.py

The replay's input: one symbol's trades over a span, read from Bronze in
event-time order, as a stream of Arrow tables (TRADE_COLUMNS, each table
sorted, every table after the last). Plain pyarrow, no Spark: reading
files needs no JVM, and the replay's Spark jobs are the ones consuming the
result.

Two sources (Phase 4a, docs/ROADMAP.md):

- **trades** (`bronze/trades`): the trades the live pipeline recorded,
  exactly as storage_writer.py's Bronze query wrote them. That's a Spark
  file sink, so which files count is its log's business (`_spark_metadata`:
  one file per batch, folded into a `.compact` file every ten): a Parquet
  file there that the log doesn't list belongs to a batch that failed and
  was rerun, and reading it would replay its trades twice. Spark writes
  `event_time` as INT96, which has no Parquet statistics, so a file's time
  span can't be read from its footer. The log is enough to find a span's
  files anyway: it lists them in commit order with their write times, and
  a file can only hold a trade at or after `start` if it was written after
  `start`. So reading begins at the first file written after `start`
  (minus CLOCK_SKEW for the exchange's clock against the laptop's), and
  stops at the first file whose trades all come after `end` plus
  REORDER_SLACK. A batch the writer caught up on after a restart (written
  long after its trades) still lies between the two, so it is read too.
- **agg_trades** (`bronze/agg_trades`): the backfill's aggTrades, one file
  per UTC day (backfill/bronze_files.py), for days live didn't record. An
  aggregate trade is every fill of one taker order at one price, so its
  candles are exactly Binance's klines (bar the trade count).

Files are read in commit order, which is Kafka's: event times rise with it
but not strictly (a batch spans a moment, symbols interleave). A reorder
buffer sorts by event time and holds back the newest REORDER_SLACK, which
a later file might still reach into. The sort is stable, so trades sharing
a millisecond keep the order Kafka gave them live: Spark's min_by/max_by
then pick the same open and close a replay as they did live.

Author: @DS
"""

from __future__ import annotations

import bisect
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, List, Optional, Sequence
from urllib.parse import unquote, urlsplit

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

TRADES = "trades"
AGG_TRADES = "agg_trades"
SOURCES = (TRADES, AGG_TRADES)

# What every table this module yields holds, in this order. The trade's
# symbol is the caller's, and its exchange "binance" (Bronze's only one).
TIMESTAMP = pa.timestamp("us", tz="UTC")
TRADE_SCHEMA = pa.schema(
    [
        ("event_time", TIMESTAMP),
        ("trade_id", pa.string()),
        ("price", pa.decimal128(38, 18)),
        ("quantity", pa.decimal128(38, 18)),
        ("side", pa.string()),
        ("exchange", pa.string()),
    ]
)
TRADE_COLUMNS = tuple(TRADE_SCHEMA.names)

# How far the exchange's clock (trade times) may run ahead of the laptop's
# (file write times) before a file would wrongly be skipped as too early.
CLOCK_SKEW = timedelta(minutes=5)
# How far out of event-time order trades may arrive across files. Live, a
# trade more than the watermark (10 s) late was dropped anyway.
REORDER_SLACK = timedelta(seconds=60)
# Rows the reorder buffer collects before it sorts and hands some on.
CHUNK_ROWS = 50_000
# Files read ahead of the one being consumed: Bronze's are tiny (one per
# micro-batch, often a second's trades), so reading them one at a time
# would be all latency.
READ_AHEAD_FILES = 64
READ_THREADS = 8

SINK_LOG = "_spark_metadata"


class NoHistory(Exception):
    """Neither source has trades for the symbol at the replay's start."""


@dataclass(frozen=True)
class SinkFile:
    path: Path
    written_ms: int  # the sink log's modificationTime


def committed_files(dataset_dir: Path) -> List[SinkFile]:
    """
    The Parquet files a Spark file sink at `dataset_dir` has committed, in
    commit order: the newest `.compact` log file's entries, then those of
    every batch after it. Empty if there's no log (nothing written yet).
    """
    log_dir = dataset_dir / SINK_LOG
    if not log_dir.is_dir():
        return []
    batches = {}
    compacts = set()
    for entry in log_dir.iterdir():
        name = entry.name
        if name.startswith("."):
            continue  # Hadoop's .crc files and in-flight temporaries
        if name.endswith(".compact"):
            batch = name[: -len(".compact")]
            if batch.isdigit():
                compacts.add(int(batch))
                batches[int(batch)] = entry
        elif name.isdigit() and int(name) not in compacts:
            batches.setdefault(int(name), entry)
    if not batches:
        return []

    newest_compact = max(compacts) if compacts else -1
    files = {}  # path -> SinkFile, insertion-ordered
    for batch in sorted(b for b in batches if b >= newest_compact):
        for line in batches[batch].read_text().splitlines()[1:]:  # line 1 is the log version ("v1")
            if not line.strip():
                continue
            record = json.loads(line)
            path = _local_path(record["path"], dataset_dir)
            if record.get("action", "add") == "delete":
                files.pop(path, None)
            elif not record.get("isDir", False):
                files[path] = SinkFile(path, int(record["modificationTime"]))
    return list(files.values())


def _local_path(uri: str, dataset_dir: Path) -> Path:
    """A log entry's file URI as a local path, under `dataset_dir` even if the data root has moved since."""
    path = Path(unquote(urlsplit(uri).path))
    if path.exists():
        return path
    parts = path.parts
    for i in range(len(parts) - 1, -1, -1):
        if parts[i].startswith("symbol="):
            return dataset_dir.joinpath(*parts[i:])
    return path


def live_trade_files(data_root: str, symbol: str) -> List[SinkFile]:
    """`symbol`'s committed bronze/trades files, in commit order."""
    dataset_dir = Path(data_root) / "bronze" / "trades"
    partition = f"symbol={symbol}"
    return [f for f in committed_files(dataset_dir) if f.path.parent.name == partition]


def agg_trade_day_file(data_root: str, symbol: str, day: date) -> Path:
    """Where the backfill keeps `symbol`'s aggTrades for `day` (backfill/bronze_files.py's layout)."""
    return Path(data_root) / "bronze" / "agg_trades" / f"symbol={symbol}" / f"date={day.isoformat()}" / "agg_trades.parquet"


def choose_source(data_root: str, symbol: str, start: datetime) -> str:
    """
    The source a replay from `start` should read when the caller didn't
    say: the live recording if it has trades from within a minute of
    `start`, else the backfill's aggTrades if that day was backfilled.
    Raises NoHistory if neither does.
    """
    if _first_live_trade(data_root, symbol, start, start + timedelta(minutes=1)) is not None:
        return TRADES
    if agg_trade_day_file(data_root, symbol, start.astimezone(timezone.utc).date()).exists():
        return AGG_TRADES
    raise NoHistory(f"no Bronze trades for {symbol} at {start.isoformat()}: neither recorded live nor backfilled")


def read_trades(
    source: str, data_root: str, symbol: str, start: datetime, end: Optional[datetime] = None
) -> Iterator[pa.Table]:
    """
    `symbol`'s trades in [start, end) from `source`, as TRADE_SCHEMA tables
    in event-time order (end None: everything there is). Lazy: files are
    read as the iterator is consumed.
    """
    if source == TRADES:
        return _live_trades(data_root, symbol, start, end)
    if source == AGG_TRADES:
        return _agg_trades(data_root, symbol, start, end)
    raise ValueError(f"unknown source {source!r}: one of {list(SOURCES)}")


def _first_live_trade(data_root: str, symbol: str, start: datetime, end: datetime) -> Optional[datetime]:
    for table in _live_trades(data_root, symbol, start, end):
        if table.num_rows:
            return table.column("event_time")[0].as_py()
    return None


def _live_trades(data_root: str, symbol: str, start: datetime, end: Optional[datetime]) -> Iterator[pa.Table]:
    files = live_trade_files(data_root, symbol)
    first = _first_written_after(files, start - CLOCK_SKEW)
    stop_after = None if end is None else end + REORDER_SLACK
    buffer = _ReorderBuffer()
    for table in _read_in_order([f.path for f in files[first:]]):
        if table.num_rows == 0:
            continue
        times = table.column("event_time")
        if stop_after is not None and pc.min(times).as_py() >= stop_after:
            break  # every later file's trades are later still (see the module docstring)
        yield from buffer.add(_in_span(table, start, end), newest=pc.max(times).as_py())
    yield from buffer.flush()


def _first_written_after(files: Sequence[SinkFile], moment: datetime) -> int:
    """Index of the first file written at or after `moment` (write times rise with commit order, but not strictly)."""
    written = []
    newest = 0
    for f in files:
        newest = max(newest, f.written_ms)
        written.append(newest)
    return bisect.bisect_left(written, int(moment.timestamp() * 1000))


def _read_in_order(paths: Sequence[Path]) -> Iterator[pa.Table]:
    """Each file as a TRADE_SCHEMA table, in the order given, READ_AHEAD_FILES read ahead on threads."""
    if not paths:
        return
    with ThreadPoolExecutor(READ_THREADS) as pool:
        pending = [pool.submit(_read_live_file, p) for p in paths[:READ_AHEAD_FILES]]
        following = READ_AHEAD_FILES
        while pending:
            table = pending.pop(0).result()
            if following < len(paths):
                pending.append(pool.submit(_read_live_file, paths[following]))
                following += 1
            yield table


def _read_live_file(path: Path) -> pa.Table:
    table = pq.read_table(path, columns=["event_time", "trade_id", "price", "quantity", "side", "exchange"])
    # Spark's INT96 reads back as naive nanoseconds; the instant is UTC (the
    # session time zone), and the values are microseconds, Spark's precision.
    event_time = table.column("event_time").cast(pa.timestamp("us")).cast(TIMESTAMP)
    return table.set_column(0, "event_time", event_time).cast(TRADE_SCHEMA)


def _agg_trades(data_root: str, symbol: str, start: datetime, end: Optional[datetime]) -> Iterator[pa.Table]:
    day = start.astimezone(timezone.utc).date()
    last_day = None if end is None else (end - timedelta(microseconds=1)).astimezone(timezone.utc).date()
    while last_day is None or day <= last_day:
        path = agg_trade_day_file(data_root, symbol, day)
        if not path.exists():
            if last_day is None and day > datetime.now(timezone.utc).date():
                return
            day += timedelta(days=1)
            continue
        table = pq.read_table(path, columns=["event_time", "agg_trade_id", "price", "quantity", "side", "exchange"])
        table = table.sort_by([("event_time", "ascending"), ("agg_trade_id", "ascending")])
        trades = pa.table(
            {
                "event_time": table.column("event_time").cast(TIMESTAMP),
                "trade_id": pc.cast(table.column("agg_trade_id"), pa.string()),
                "price": table.column("price"),
                "quantity": table.column("quantity"),
                "side": table.column("side"),
                "exchange": table.column("exchange"),
            },
            schema=TRADE_SCHEMA,
        )
        trades = _in_span(trades, start, end)
        for offset in range(0, trades.num_rows, CHUNK_ROWS):
            yield trades.slice(offset, CHUNK_ROWS)
        day += timedelta(days=1)


def _in_span(table: pa.Table, start: datetime, end: Optional[datetime]) -> pa.Table:
    times = table.column("event_time")
    keep = pc.greater_equal(times, pa.scalar(start, TIMESTAMP))
    if end is not None:
        keep = pc.and_(keep, pc.less(times, pa.scalar(end, TIMESTAMP)))
    return table.filter(keep)


class _ReorderBuffer:
    """Collects tables in arrival order and hands them on sorted by event time, holding back the newest REORDER_SLACK."""

    def __init__(self) -> None:
        self._tables: List[pa.Table] = []
        self._rows = 0
        self._newest: Optional[datetime] = None

    def add(self, table: pa.Table, newest: datetime) -> Iterator[pa.Table]:
        if table.num_rows:
            self._tables.append(table)
            self._rows += table.num_rows
        if self._newest is None or newest > self._newest:
            self._newest = newest
        if self._rows < CHUNK_ROWS:
            return
        combined = self._sorted()
        held_from = pa.scalar(self._newest - REORDER_SLACK, TIMESTAMP)
        cut = pc.sum(pc.less(combined.column("event_time"), held_from)).as_py() or 0  # sorted: the rows before it
        ready, held = combined.slice(0, cut), combined.slice(cut)
        self._tables, self._rows = ([held] if held.num_rows else []), held.num_rows
        if ready.num_rows:
            yield ready

    def flush(self) -> Iterator[pa.Table]:
        if self._rows:
            yield self._sorted()
        self._tables, self._rows = [], 0

    def _sorted(self) -> pa.Table:
        combined = pa.concat_tables(self._tables)
        return combined.take(pc.sort_indices(combined, sort_keys=[("event_time", "ascending")]))
