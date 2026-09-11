"""
postgres_client.py

Read-only Postgres access for REST endpoints — the query-side counterpart
to streaming/postgres_sink.py's write-side CandlePostgresSink. Kept as
its own module so routers/candles.py never touches `psycopg` directly,
mirroring the same boundary pattern redis_client.py uses for Redis.

Author: @DS
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
POSTGRES_DSN = "postgresql://dsview:dsview@localhost:5432/dsview"

_HISTORY_SQL = """
    SELECT symbol, window_start, window_end, open, high, low, close, volume, trade_count
    FROM candles_1m
    WHERE symbol = %(symbol)s
    ORDER BY window_start DESC
    LIMIT %(limit)s
"""


class CandleHistoryReader:
    """Reads recent candles for a symbol from the candles_1m serving table."""

    # TODO(@DS): a connection per request is fine at dev request volumes;
    # move to a pool (e.g. psycopg_pool) if this ever sees real traffic.
    def __init__(self, dsn: str = POSTGRES_DSN) -> None:
        self._conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)

    def get_recent_candles(self, symbol: str, limit: int) -> list[dict[str, Any]]:
        """Return up to `limit` candles for `symbol`, oldest first (chart order)."""
        rows = self._conn.execute(_HISTORY_SQL, {"symbol": symbol, "limit": limit}).fetchall()
        return list(reversed(rows))  # query is DESC to LIMIT to the *latest* rows; charts want ascending

    def close(self) -> None:
        self._conn.close()
