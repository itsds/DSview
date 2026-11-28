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


def test_init_db_creates_candle_indicators_with_a_column_per_server_ema(pg):
    from config.settings import SERVER_EMA_LENGTHS

    columns = {
        row["column_name"]: row["data_type"]
        for row in pg.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'candle_indicators'"
        )
    }
    assert columns == {
        "symbol": "character varying",
        "timeframe": "character varying",
        "window_start": "timestamp with time zone",
        **{f"ema_{period}": "double precision" for period in SERVER_EMA_LENGTHS},
    }
    [primary_key] = pg.execute(
        """
        SELECT array_agg(a.attname ORDER BY a.attnum) AS columns
        FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'candle_indicators'::regclass AND i.indisprimary
        """
    ).fetchall()
    assert primary_key["columns"] == ["symbol", "timeframe", "window_start"]


def test_init_db_creates_footprint_1m_keyed_by_symbol_minute_and_level(pg):
    columns = {
        row["column_name"]: row["data_type"]
        for row in pg.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'footprint_1m'"
        )
    }
    assert columns == {
        "symbol": "character varying",
        "window_start": "timestamp with time zone",
        "price": "numeric",
        "buy_volume": "numeric",
        "sell_volume": "numeric",
    }
    [primary_key] = pg.execute(
        """
        SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)) AS columns
        FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'footprint_1m'::regclass AND i.indisprimary
        """
    ).fetchall()
    assert primary_key["columns"] == ["symbol", "window_start", "price"]


def test_init_db_gives_the_replay_pipeline_the_same_tables_in_its_own_schema(pg, symbol):
    import psycopg

    from config.settings import REPLAY_PIPELINE

    tables = {
        (row["table_schema"], row["table_name"])
        for row in pg.execute(
            "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('public', 'replay')"
        )
    }
    names = {"candles_1m", "candle_indicators", "candle_revisions", "footprint_1m"}
    assert {name for schema, name in tables if schema == "replay"} == names
    assert {name for schema, name in tables if schema == "public"} >= names

    # The replay's DSN writes the replay's tables, and live's never see it.
    with psycopg.connect(REPLAY_PIPELINE.postgres_dsn, autocommit=True) as replay:
        replay.execute(
            "INSERT INTO candles_1m VALUES (%s, '2026-11-20T10:00Z', '2026-11-20T10:01Z', 1, 1, 1, 1, 1, 1)", (symbol,)
        )
    assert pg.execute("SELECT count(*) AS n FROM candles_1m WHERE symbol = %s", (symbol,)).fetchone()["n"] == 0
    assert pg.execute("SELECT count(*) AS n FROM replay.candles_1m WHERE symbol = %s", (symbol,)).fetchone()["n"] == 1


def test_init_db_creates_futures_ratios_in_lives_schema_only(pg):
    columns = {
        row["column_name"]: row["data_type"]
        for row in pg.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'futures_ratios'"
        )
    }
    assert columns == {
        "symbol": "character varying",
        "period_start": "timestamp with time zone",
        "kind": "character varying",
        "ratio": "numeric",
        "long_value": "numeric",
        "short_value": "numeric",
    }
    [primary_key] = pg.execute(
        """
        SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)) AS columns
        FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'public.futures_ratios'::regclass AND i.indisprimary
        """
    ).fetchall()
    assert primary_key["columns"] == ["symbol", "period_start", "kind"]
    assert pg.execute("SELECT to_regclass('replay.futures_ratios') AS t").fetchone()["t"] is None
