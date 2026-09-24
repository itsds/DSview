"""
postgres_client.py

Read-only Postgres access for REST endpoints — the query-side counterpart
to streaming/postgres_sink.py's write-side CandlePostgresSink. Kept as
its own module so routers/candles.py never touches `psycopg` directly,
mirroring the same boundary pattern redis_client.py uses for Redis.

Only 1-minute candles are stored (candles_1m); every bigger timeframe is
rolled up from them at query time with date_bin() rather than kept as
extra tables — see _HISTORY_SQL.

Author: @DS
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg.rows import dict_row

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
POSTGRES_DSN = "postgresql://dsview:dsview@localhost:5432/dsview"

ONE_MINUTE = timedelta(minutes=1)

# Rolls candles_1m up into `bucket`-sized candles, newest `limit` first.
#
# The inner query takes only as many 1m rows as `limit` buckets can hold
# (limit x minutes-per-bucket), so the scan stays bounded instead of
# aggregating a symbol's whole history. That row cap can cut the oldest
# bucket short — but a bucket holds at most minutes-per-bucket rows, so
# whenever the cap bites the rows span *more* than `limit` buckets, and the
# truncated one is always the oldest, which the outer LIMIT drops. Every
# returned bucket is complete (apart from the in-progress newest one).
#
# Buckets are aligned to the Unix epoch — floor(t / bucket) * bucket — the
# same alignment frontend/src/lib/timeframes.js uses to roll live 1m
# candles up, so live and historical bars for a bucket share a start time.
_HISTORY_SQL = """
    SELECT
        symbol,
        bucket AS window_start,
        bucket + %(bucket)s AS window_end,
        (array_agg(open ORDER BY window_start))[1] AS open,
        max(high) AS high,
        min(low) AS low,
        (array_agg(close ORDER BY window_start DESC))[1] AS close,
        sum(volume) AS volume,
        sum(trade_count) AS trade_count
    FROM (
        SELECT *, date_bin(%(bucket)s, window_start, TIMESTAMPTZ '1970-01-01 00:00:00+00') AS bucket
        FROM candles_1m
        WHERE symbol = %(symbol)s
          AND window_start < COALESCE(%(before)s::timestamptz, 'infinity')
        ORDER BY window_start DESC
        LIMIT %(row_limit)s
    ) recent
    GROUP BY symbol, bucket
    ORDER BY bucket DESC
    LIMIT %(limit)s
"""


class CandleHistoryReader:
    """Reads candles for a symbol from the candles_1m serving table, rolled up to any bucket size."""

    # TODO(@DS): a connection per request is fine at dev request volumes;
    # move to a pool (e.g. psycopg_pool) if this ever sees real traffic.
    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)

    def get_candles(
        self,
        symbol: str,
        limit: int,
        bucket: timedelta = ONE_MINUTE,
        before: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Return up to `limit` `bucket`-sized candles for `symbol`, oldest first (chart order).

        With `before`, only candles whose window starts before it — pass the
        oldest window_start already loaded to page backwards.
        """
        rows = self._conn.execute(
            _HISTORY_SQL,
            {
                "symbol": symbol,
                "bucket": bucket,
                "before": before,
                "limit": limit,
                "row_limit": limit * (bucket // ONE_MINUTE),
            },
        ).fetchall()
        return list(reversed(rows))  # query is DESC to LIMIT to the *latest* rows; charts want ascending

    def close(self) -> None:
        self._conn.close()
