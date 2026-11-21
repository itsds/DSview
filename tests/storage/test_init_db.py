"""Tests for storage/init_db.py: the DDL applies, and applies again harmlessly."""

from __future__ import annotations

import init_db


def test_init_db_is_idempotent_and_creates_candles_1m(pg):
    init_db.main()  # the `pg` fixture already ran it once
    init_db.main()

    columns = {
        row["column_name"]: row["data_type"]
        for row in pg.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'candles_1m'"
        )
    }
    assert columns == {
        "symbol": "character varying",
        "window_start": "timestamp with time zone",
        "window_end": "timestamp with time zone",
        "open": "numeric",
        "high": "numeric",
        "low": "numeric",
        "close": "numeric",
        "volume": "numeric",
        "trade_count": "integer",
    }
    [primary_key] = pg.execute(
        """
        SELECT array_agg(a.attname ORDER BY a.attnum) AS columns
        FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'candles_1m'::regclass AND i.indisprimary
        """
    ).fetchall()
    assert primary_key["columns"] == ["symbol", "window_start"]
