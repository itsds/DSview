"""
candle_store.py

The backfill's side of Postgres, and the one place backfill/ touches
psycopg: writes backfilled 1m candles into candles_1m and the footprint
into footprint_1m, and tells the live indicator job when the candle
history changed (candle_revisions).

**Candles.** Klines are the exchange's own candles for closed minutes, so
they overwrite whatever candles_1m holds for those minutes, a live candle
included: the live one can only be short of trades (a minute ingestion
started halfway through, a reconnect), never ahead. Each batch is COPYed
into a temporary table and upserted from there, and a row is only
rewritten if a value actually differs (IS DISTINCT FROM: NUMERIC compares
by value, so "97000.10000000" equals a live "97000.1"); upsert_candles()
returns how many rows were inserted or changed and the oldest of them.
Klines for minutes without a single trade are skipped: the live pipeline
never makes such a candle, and the chart shows a gap there either way.

**Footprint.** replace_footprint() makes footprint_1m hold exactly a
span's rows from the exchange's aggTrades (footprint_rows.py): changed
levels are rewritten, new ones inserted, and levels the exchange doesn't
have for those minutes removed, so what the live footprint wrote for a
minute is replaced whole, as a live candle is. Unchanged rows aren't
touched, so reloading a day costs no writes. Nothing derived from the
footprint keeps state, so it needs no revision.

**Revisions.** An EMA depends on every bar before it, so candles inserted
or changed anywhere but the newest minutes change indicator values all the
way to the present. candle_revisions holds a counter per symbol that
bump_revision() raises after a backfill changed candles_1m; the live
indicator job (streaming/indicator_state.py) checks it every micro-batch
and rebuilds a symbol whose revision moved, from candles_1m, without a
restart. The live pipeline's own writes at the head of the history never
bump it: the running state already follows those.

Author: @DS
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Tuple

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv

from config.settings import POSTGRES_DSN

_COLUMNS = ("symbol", "window_start", "window_end", "open", "high", "low", "close", "volume", "trade_count")
_VALUES = ("window_end", "open", "high", "low", "close", "volume", "trade_count")

_STAGING_SQL = """
    CREATE TEMP TABLE IF NOT EXISTS candles_1m_incoming
        (LIKE candles_1m INCLUDING DEFAULTS) ON COMMIT DELETE ROWS
"""
_UPSERT_SQL = f"""
    INSERT INTO candles_1m ({", ".join(_COLUMNS)})
    SELECT {", ".join(_COLUMNS)} FROM candles_1m_incoming
    ON CONFLICT (symbol, window_start) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in _VALUES)}
    WHERE ({", ".join(f"candles_1m.{c}" for c in _VALUES)})
          IS DISTINCT FROM ({", ".join(f"EXCLUDED.{c}" for c in _VALUES)})
    RETURNING window_start
"""
_BUMP_SQL = """
    INSERT INTO candle_revisions (symbol, revision, changed_at, oldest_change)
    VALUES (%(symbol)s, 1, now(), %(oldest)s)
    ON CONFLICT (symbol) DO UPDATE SET
        revision = candle_revisions.revision + 1,
        changed_at = EXCLUDED.changed_at,
        oldest_change = EXCLUDED.oldest_change
    RETURNING revision
"""


_FOOTPRINT_COLUMNS = ("symbol", "window_start", "price", "buy_volume", "sell_volume")
_FOOTPRINT_STAGING_SQL = """
    CREATE TEMP TABLE IF NOT EXISTS footprint_1m_incoming
        (LIKE footprint_1m INCLUDING DEFAULTS) ON COMMIT DELETE ROWS
"""
_FOOTPRINT_DELETE_SQL = """
    DELETE FROM footprint_1m existing
    WHERE existing.symbol = %(symbol)s
      AND existing.window_start >= %(start)s AND existing.window_start < %(end)s
      AND NOT EXISTS (
          SELECT 1 FROM footprint_1m_incoming incoming
          WHERE incoming.window_start = existing.window_start AND incoming.price = existing.price
      )
"""
_FOOTPRINT_UPSERT_SQL = f"""
    INSERT INTO footprint_1m ({", ".join(_FOOTPRINT_COLUMNS)})
    SELECT {", ".join(_FOOTPRINT_COLUMNS)} FROM footprint_1m_incoming
    ON CONFLICT (symbol, window_start, price) DO UPDATE SET
        buy_volume = EXCLUDED.buy_volume,
        sell_volume = EXCLUDED.sell_volume
    WHERE (footprint_1m.buy_volume, footprint_1m.sell_volume)
          IS DISTINCT FROM (EXCLUDED.buy_volume, EXCLUDED.sell_volume)
"""

_TABLES = ("candles_1m", "candle_revisions", "footprint_1m")


class StoreError(RuntimeError):
    """Postgres can't be reached, or lacks a table the backfill writes."""


class CandleStore:
    """One connection's worth of candles_1m upserts and revision bumps. Not thread-safe: one caller at a time."""

    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        try:
            self._conn = psycopg.connect(dsn, autocommit=True)
        except psycopg.OperationalError as exc:
            raise StoreError(f"can't reach Postgres ({exc}); is the dev stack up?") from exc
        missing = [
            table
            for table in _TABLES
            if self._conn.execute("SELECT to_regclass(%s)", (table,)).fetchone()[0] is None
        ]
        if missing:
            self._conn.close()
            raise StoreError(f"Postgres has no {' or '.join(missing)} table: run `python storage/init_db.py` first (it's idempotent)")

    def upsert_candles(self, symbol: str, klines: pa.Table) -> Tuple[int, Optional[datetime]]:
        """
        Upsert `symbol`'s 1m candles from a table in Bronze's klines schema
        (bronze_files.KLINES_SCHEMA; only the candle columns are used).
        Returns (rows inserted or changed, the oldest such window_start).
        """
        traded = klines.filter(pc.greater(klines["trade_count"], 0))  # a minute without trades: no candle, as live
        rows = pa.table(
            {"symbol": pa.array([symbol] * traded.num_rows, pa.string()), **{c: traded[c] for c in _COLUMNS[1:]}}
        )
        # Arrow writes the CSV in C++ (timestamps as ISO 8601 with a Z, decimals
        # in full), so a year of candles goes in seconds, not row by row.
        csv = pa.BufferOutputStream()
        pa_csv.write_csv(rows, csv, pa_csv.WriteOptions(include_header=False))
        with self._conn.transaction(), self._conn.cursor() as cursor:
            cursor.execute(_STAGING_SQL)
            with cursor.copy(f"COPY candles_1m_incoming ({', '.join(_COLUMNS)}) FROM STDIN WITH (FORMAT csv)") as copy:
                copy.write(csv.getvalue().to_pybytes())
            cursor.execute(_UPSERT_SQL)
            changed = [row[0] for row in cursor.fetchall()]
        return len(changed), (min(changed) if changed else None)

    def replace_footprint(self, symbol: str, start_ms: int, end_ms: int, footprint: pa.Table) -> int:
        """
        Make footprint_1m's rows for `symbol` in [start_ms, end_ms) (Unix ms,
        whole minutes) exactly `footprint` (footprint_rows.FOOTPRINT_SCHEMA,
        every row inside the span). Returns how many rows were inserted,
        changed or removed.
        """
        rows = pa.table({"symbol": pa.array([symbol] * footprint.num_rows, pa.string()), **{c: footprint[c] for c in _FOOTPRINT_COLUMNS[1:]}})
        csv = pa.BufferOutputStream()
        pa_csv.write_csv(rows, csv, pa_csv.WriteOptions(include_header=False))
        span = {
            "symbol": symbol,
            "start": datetime.fromtimestamp(start_ms / 1000, timezone.utc),
            "end": datetime.fromtimestamp(end_ms / 1000, timezone.utc),
        }
        with self._conn.transaction(), self._conn.cursor() as cursor:
            cursor.execute(_FOOTPRINT_STAGING_SQL)
            with cursor.copy(f"COPY footprint_1m_incoming ({', '.join(_FOOTPRINT_COLUMNS)}) FROM STDIN WITH (FORMAT csv)") as copy:
                copy.write(csv.getvalue().to_pybytes())
            cursor.execute(_FOOTPRINT_DELETE_SQL, span)
            removed = cursor.rowcount
            cursor.execute(_FOOTPRINT_UPSERT_SQL)
            return removed + cursor.rowcount

    def bump_revision(self, symbol: str, oldest_change: Optional[datetime] = None) -> int:
        """Mark `symbol`'s candles_1m history as changed (indicators rebuild); returns the new revision."""
        return self._conn.execute(_BUMP_SQL, {"symbol": symbol, "oldest": oldest_change}).fetchone()[0]

    def close(self) -> None:
        self._conn.close()
