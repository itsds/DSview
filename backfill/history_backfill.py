"""
history_backfill.py

The backfill job (Phase 3a in docs/ROADMAP.md): the history the live
pipeline never saw, fetched from Binance's REST API (binance_rest.py), so
the chart has something to show on 4h and 1D, Spark's EMA 200 can seed on
every timeframe, and the footprint (Phase 3b) and replay (Phase 4) have
trades to work from. Two lanes per symbol, as decided at kickoff:

- **Candles**: 1m klines from `--days` back (365 by default) into
  Postgres's candles_1m (candle_store.py: the exchange's candle wins over
  a live one for the same minute) and Bronze's `klines_1m`
  (bronze_files.py). About 530 requests a year, so a few minutes.
- **Trades**: aggTrades from `--trade-days` back (30 by default) into
  Bronze's `agg_trades`. Roughly a million a day for BTCUSDT, so this is
  the slow lane: ~1000 requests a day, several days fetched at once
  (`--concurrency`) under one request-weight budget. Each day then goes
  into footprint_1m as the volume footprint's history (Phase 3b,
  footprint_rows.py): buy and sell volume per $1 level per minute,
  replacing whatever the live footprint wrote for those minutes, as a
  candle does. Replay (Phase 4) will read them too.

Both work a UTC day at a time, newest day first, so an interrupted run has
already landed the days that matter most. They stop at the minute that
closed SETTLE_MINUTES ago, by when the live pipeline has finished that
minute too (its watermark is 10 s), so the two never race over it.

**Safe to rerun.** Every Bronze day file records the span it covers and is
replaced whole (bronze_files.py), so a rerun skips the trade days it
already has and refetches only today's. A day already in Bronze isn't
refetched either: its candles are reloaded from its file into candles_1m
and its trades' footprint into footprint_1m, which costs no writes when
the rows are already there (unchanged rows are not rewritten), and
restores them if a table was emptied. No run adds a duplicate row.

**Indicators.** If any candle was inserted or changed, the symbol's
revision in candle_revisions is bumped as the run ends (even an
interrupted one), and the live indicator job rebuilds that symbol from
candles_1m on its next micro-batch, with no restart: no checkpoint to
delete. `--rebuild-indicators-only` asks for that rebuild without fetching
anything. Gold is untouched: Gold's candles and indicators are what the
live stream finalized; filling Gold from Bronze's history belongs with
replay (Phase 4).

Run from backfill/, with the repo root on PYTHONPATH, while the dev stack
is up (it needs Postgres; the Spark jobs can be running or not):

    cd backfill && python history_backfill.py                  # SYMBOLS: a year of candles, 30 days of trades
    python history_backfill.py --days 30 --trade-days 2        # less
    python history_backfill.py --symbol ETHUSDT --trade-days 0 # candles only, another symbol
    python history_backfill.py --rebuild-indicators-only

Author: @DS
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Awaitable, Callable, Dict, List, Optional, Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from config.settings import BINANCE_REST_URL, DATA_ROOT, PIPELINE, SYMBOLS

from binance_rest import WEIGHT_BUDGET_PER_MINUTE, BinanceError, BinanceHistoryClient, WeightBudget
from bronze_files import AGG_TRADES, KLINES, covers, day_path, day_writer, page_table
from candle_store import CandleStore, StoreError
from footprint_rows import footprint_table

logger = logging.getLogger(__name__)

DEFAULT_CANDLE_DAYS = 365  # EMA 200 on 1D needs 200 days; a year leaves it seeded with room to scroll
DEFAULT_TRADE_DAYS = 30
DEFAULT_CONCURRENCY = 4  # trade days fetched at once; the weight budget, not this, sets the pace
SETTLE_MINUTES = 2
# aggTrades rows per Parquet row group: bounds memory to a few tens of MB
# per day in flight, however busy the day.
FLUSH_ROWS = 200_000

MINUTE_MS = 60_000
DAY_MS = 24 * 60 * MINUTE_MS


@dataclass(frozen=True)
class DaySpan:
    """One UTC day's share of a lane: [start_ms, end_ms), the whole day except for today's."""

    day: date
    start_ms: int
    end_ms: int


def settled_end(now: float) -> int:
    """Where a run stops (Unix ms, exclusive): the end of the last minute that closed SETTLE_MINUTES or more ago."""
    return (int(now) // 60 - SETTLE_MINUTES) * MINUTE_MS


def plan_days(days: int, end_ms: int) -> List[DaySpan]:
    """The UTC days covering `days` days back from end_ms, whole days from midnight, newest first."""
    if days <= 0:
        return []
    start = (end_ms - days * DAY_MS) // DAY_MS * DAY_MS
    spans = []
    while start < end_ms:
        spans.append(DaySpan(datetime.fromtimestamp(start / 1000, timezone.utc).date(), start, min(start + DAY_MS, end_ms)))
        start += DAY_MS
    return spans[::-1]


@dataclass
class SymbolResult:
    candle_days_fetched: int = 0
    candle_days_reloaded: int = 0
    candles_changed: int = 0
    oldest_change: Optional[datetime] = None
    trade_days_fetched: int = 0
    trade_days_skipped: int = 0
    trades: int = 0
    footprint_changed: int = 0
    revision: Optional[int] = None

    def add_changes(self, count: int, oldest: Optional[datetime]) -> None:
        self.candles_changed += count
        if oldest is not None and (self.oldest_change is None or oldest < self.oldest_change):
            self.oldest_change = oldest

    def summary(self, symbol: str) -> str:
        candles = (
            f"candles: {self.candle_days_fetched + self.candle_days_reloaded} days "
            f"({self.candle_days_fetched} fetched, {self.candle_days_reloaded} reloaded from Bronze), "
            f"{self.candles_changed:,} rows inserted or changed"
        )
        if self.oldest_change is not None:
            candles += f" (oldest {self.oldest_change:%Y-%m-%d %H:%M} UTC)"
        trades = (
            f"aggTrades: {self.trade_days_fetched + self.trade_days_skipped} days "
            f"({self.trade_days_fetched} fetched, {self.trade_days_skipped} already in Bronze), "
            f"{self.trades:,} trades fetched, {self.footprint_changed:,} footprint rows inserted, changed or removed"
        )
        rebuild = (
            f"indicator rebuild requested (revision {self.revision})"
            if self.revision is not None
            else "indicators unchanged"
        )
        return f"{symbol} — {candles}; {trades}; {rebuild}"


async def _all_or_nothing(*coros: Awaitable[None]) -> None:
    """Run coros concurrently; the first failure (or our own cancellation) cancels the rest, then is raised."""
    tasks = [asyncio.ensure_future(coro) for coro in coros]
    if not tasks:
        return
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    for task in done:
        if not task.cancelled() and task.exception() is not None:
            raise task.exception()


async def _each(spans: Sequence[DaySpan], concurrency: int, work: Callable[[DaySpan], Awaitable[None]]) -> None:
    """Run `work` over the spans in order, at most `concurrency` at once; the first failure cancels the rest."""
    queue = list(spans)

    async def worker() -> None:
        while queue:
            await work(queue.pop(0))

    await _all_or_nothing(*(worker() for _ in range(min(concurrency, len(queue)))))


class HistoryBackfill:
    """One run: both lanes for each symbol, then the revision bump. history_backfill.main() wires it up."""

    def __init__(
        self,
        client: BinanceHistoryClient,
        store: CandleStore,
        data_root: str = DATA_ROOT,
        concurrency: int = DEFAULT_CONCURRENCY,
        candle_concurrency: int = 2,  # a candle day is two requests: two days at once is plenty
    ) -> None:
        self._client = client
        self._store = store
        self._data_root = data_root
        self._concurrency = max(1, concurrency)
        self._candle_concurrency = max(1, candle_concurrency)
        self._store_lock = asyncio.Lock()  # one connection: one statement at a time

    async def run(
        self, symbols: Sequence[str], candle_days: int, trade_days: int, end_ms: int, rebuild: bool = False
    ) -> Dict[str, SymbolResult]:
        results: Dict[str, SymbolResult] = {}
        for symbol in symbols:
            result = results[symbol] = SymbolResult()
            try:
                await _all_or_nothing(
                    _each(
                        plan_days(candle_days, end_ms),
                        self._candle_concurrency,
                        lambda span: self._candle_day(symbol, span, result),
                    ),
                    _each(plan_days(trade_days, end_ms), self._concurrency, lambda span: self._trade_day(symbol, span, result)),
                )
            finally:
                # Changed rows are committed even if the run stopped halfway:
                # the indicators must hear about them either way.
                if result.candles_changed or rebuild:
                    async with self._store_lock:
                        result.revision = await asyncio.to_thread(
                            self._store.bump_revision, symbol, result.oldest_change
                        )
            logger.info("%s", result.summary(symbol))
        return results

    async def _upsert(self, symbol: str, table: pa.Table):
        async with self._store_lock:
            return await asyncio.to_thread(self._store.upsert_candles, symbol, table)

    async def _candle_day(self, symbol: str, span: DaySpan, result: SymbolResult) -> None:
        path = day_path(self._data_root, KLINES, symbol, span.day)
        if covers(path, span.start_ms, span.end_ms):
            table = await asyncio.to_thread(pq.read_table, path)
            changed, oldest = await self._upsert(symbol, table)
            result.candle_days_reloaded += 1
            if changed:
                logger.info("%s candles %s: reloaded from Bronze, %d rows inserted or changed", symbol, span.day, changed)
        else:
            tables = [page_table(KLINES, page) async for page in self._client.klines(symbol, span.start_ms, span.end_ms)]
            table = pa.concat_tables(tables) if tables else KLINES.schema.empty_table()
            # Postgres first: a day's Bronze file then also means its candles are in.
            changed, oldest = await self._upsert(symbol, table)
            await asyncio.to_thread(self._write_klines, symbol, span, table)
            result.candle_days_fetched += 1
            logger.info("%s candles %s: %d minutes, %d rows inserted or changed", symbol, span.day, table.num_rows, changed)
        result.add_changes(changed, oldest)

    def _write_klines(self, symbol: str, span: DaySpan, table: pa.Table) -> None:
        with day_writer(self._data_root, KLINES, symbol, span.day, span.start_ms, span.end_ms) as writer:
            writer.write(table)

    async def _trade_day(self, symbol: str, span: DaySpan, result: SymbolResult) -> None:
        path = day_path(self._data_root, AGG_TRADES, symbol, span.day)
        if covers(path, span.start_ms, span.end_ms):
            result.trade_days_skipped += 1
        else:
            await self._fetch_trade_day(symbol, span, result)
        # From the day's file either way: a fetched day and one already in Bronze load alike.
        changed = await self._load_footprint(symbol, span, path)
        result.footprint_changed += changed
        if changed:
            logger.info("%s footprint %s: %d rows inserted, changed or removed", symbol, span.day, changed)

    async def _load_footprint(self, symbol: str, span: DaySpan, path) -> int:
        def rows() -> pa.Table:
            trades = pq.read_table(path, columns=["event_time", "price", "quantity", "side"])
            # A file can cover more than this run's span (an earlier run went further, --until).
            in_span = pc.and_(
                pc.greater_equal(trades["event_time"], pa.scalar(span.start_ms * 1000, pa.timestamp("us", tz="UTC"))),
                pc.less(trades["event_time"], pa.scalar(span.end_ms * 1000, pa.timestamp("us", tz="UTC"))),
            )
            return footprint_table(trades.filter(in_span))

        table = await asyncio.to_thread(rows)
        async with self._store_lock:
            return await asyncio.to_thread(self._store.replace_footprint, symbol, span.start_ms, span.end_ms, table)

    async def _fetch_trade_day(self, symbol: str, span: DaySpan, result: SymbolResult) -> None:
        started = time.monotonic()
        with day_writer(self._data_root, AGG_TRADES, symbol, span.day, span.start_ms, span.end_ms) as writer:
            pending: List[pa.Table] = []
            buffered = 0
            async for page in self._client.agg_trades(symbol, span.start_ms, span.end_ms):
                pending.append(page_table(AGG_TRADES, page))
                buffered += pending[-1].num_rows
                if buffered >= FLUSH_ROWS:
                    await asyncio.to_thread(writer.write, pa.concat_tables(pending))
                    pending, buffered = [], 0
            if pending:
                await asyncio.to_thread(writer.write, pa.concat_tables(pending))
        result.trade_days_fetched += 1
        result.trades += writer.rows
        logger.info("%s aggTrades %s: %s trades in %.0f s", symbol, span.day, f"{writer.rows:,}", time.monotonic() - started)


def _parse_until(text: str) -> float:
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill history from Binance: candles_1m + Bronze klines, Bronze aggTrades")
    parser.add_argument("--symbol", action="append", dest="symbols", help=f"repeatable; default: SYMBOLS ({', '.join(SYMBOLS)})")
    parser.add_argument("--days", type=int, default=DEFAULT_CANDLE_DAYS, help="days of 1m candles (default %(default)s)")
    parser.add_argument("--trade-days", type=int, default=DEFAULT_TRADE_DAYS, help="days of aggTrades (default %(default)s)")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="trade days fetched at once (default %(default)s)")
    parser.add_argument(
        "--weight-budget", type=int, default=WEIGHT_BUDGET_PER_MINUTE,
        help="Binance request weight per minute to stay under (default %(default)s; Binance allows 6000)",
    )
    parser.add_argument("--until", help="stop here instead of now (ISO 8601, UTC unless it says otherwise)")
    parser.add_argument(
        "--rebuild-indicators-only", action="store_true",
        help="fetch nothing; make the live indicator job rebuild each symbol from candles_1m",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not PIPELINE.is_live:
        # The exchange's history is live's; a replay seeds its own from it (replay/).
        parser.error(f"the backfill fills the live pipeline only (DSVIEW_PIPELINE is {PIPELINE.name!r})")

    symbols = [s.upper() for s in (args.symbols or SYMBOLS)]
    if args.rebuild_indicators_only:
        args.days = args.trade_days = 0
    end_ms = settled_end(_parse_until(args.until) if args.until else time.time())

    async def run() -> None:
        store = CandleStore()
        try:
            async with BinanceHistoryClient(BINANCE_REST_URL, budget=WeightBudget(args.weight_budget)) as client:
                backfill = HistoryBackfill(client, store, data_root=DATA_ROOT, concurrency=args.concurrency)
                started = time.monotonic()
                await backfill.run(symbols, args.days, args.trade_days, end_ms, rebuild=args.rebuild_indicators_only)
                if client.requests:
                    logger.info("Binance: %s requests in %.0f s", f"{client.requests:,}", time.monotonic() - started)
        finally:
            store.close()

    end_text = datetime.fromtimestamp(end_ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    logger.info(
        "Backfilling %s up to %s: %d days of candles, %d days of aggTrades", ", ".join(symbols), end_text, args.days, args.trade_days
    )
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.warning("Interrupted. Every finished day is kept; rerun to fetch the rest.")
        return 130
    except BinanceError as exc:
        logger.error("%s. Every finished day is kept; rerun to fetch the rest.", exc)
        return 1
    except StoreError as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
