"""
Tests for indicator_state.py's saved state and postgres_sink.py's revision
read: what lets the live indicator job notice a backfill and rebuild.
(The rebuild itself runs in Spark: tests/streaming/test_indicators.py.)
"""

from __future__ import annotations

import json

from indicator_engine import IndicatorEngine
from indicator_state import decode_state, encode_state
from postgres_sink import read_candle_revision


def test_the_state_records_the_revision_it_was_built_from():
    engine = IndicatorEngine()
    engine.update(1_800_000_000 - 1_800_000_000 % 60, "97000.5")
    engine.drain()
    revision, saved = decode_state(encode_state(4, engine))
    assert revision == 4
    assert IndicatorEngine.from_state(saved).to_state() == engine.to_state()


def test_a_state_saved_before_revisions_reads_as_revision_0():
    engine = IndicatorEngine()
    bare = json.dumps(engine.to_state())  # what Phase 2 checkpoints hold
    assert decode_state(bare) == (0, engine.to_state())


def test_the_revision_is_0_until_a_backfill_bumps_it(pg, symbol):
    assert read_candle_revision(symbol) == 0
    pg.execute(
        "INSERT INTO candle_revisions (symbol, revision, changed_at) VALUES (%s, 3, now())", (symbol,)
    )
    try:
        assert read_candle_revision(symbol) == 3
    finally:
        pg.execute("DELETE FROM candle_revisions WHERE symbol = %s", (symbol,))


def test_a_database_without_the_table_reads_as_revision_0(postgres_schema, symbol):
    import psycopg

    from config.settings import POSTGRES_DSN

    # A schema of its own with no candle_revisions, as a database set up before Phase 3a.
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS before_phase_3a")
    try:
        dsn = f"{POSTGRES_DSN}?options=-csearch_path%3Dbefore_phase_3a"
        assert read_candle_revision(symbol, dsn=dsn) == 0
    finally:
        with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
            conn.execute("DROP SCHEMA before_phase_3a")
