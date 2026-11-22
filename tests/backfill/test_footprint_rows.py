"""Tests for backfill/footprint_rows.py: a day of Bronze aggTrades as footprint_1m rows."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa

from bronze_files import AGG_TRADES_SCHEMA
from footprint_rows import FOOTPRINT_SCHEMA, footprint_table

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


def _trades(rows):
    """aggTrades in Bronze's schema from (seconds after BASE, price, quantity, side)."""
    columns = {name: [] for name in AGG_TRADES_SCHEMA.names}
    for i, (seconds, price, quantity, side) in enumerate(rows):
        columns["exchange"].append("binance")
        columns["agg_trade_id"].append(i)
        columns["event_time"].append(BASE + timedelta(seconds=seconds))
        columns["price"].append(Decimal(price))
        columns["quantity"].append(Decimal(quantity))
        columns["side"].append(side)
        columns["first_trade_id"].append(i)
        columns["last_trade_id"].append(i)
        columns["is_best_match"].append(True)
        columns["ingested_at"].append(BASE)
    return pa.table(columns, schema=AGG_TRADES_SCHEMA)


def _rows(table):
    return [
        (r["window_start"], r["price"], r["buy_volume"], r["sell_volume"]) for r in table.to_pylist()
    ]


def test_trades_land_on_the_level_below_them_split_by_taker_side_ordered_by_minute_and_price():
    table = footprint_table(_trades([
        (65, "97000.50", "4", "buy"),  # the next minute, listed first
        (5, "97000.40", "1", "buy"),
        (6, "97000.99", "0.5", "sell"),
        (7, "97000.00", "0.25", "buy"),
        (8, "97001.00", "2", "sell"),
        (59.999, "96999.999", "3", "buy"),
    ]))

    assert table.schema == FOOTPRINT_SCHEMA
    assert _rows(table) == [
        (BASE, Decimal("96999"), Decimal("3"), Decimal("0")),
        (BASE, Decimal("97000"), Decimal("1.25"), Decimal("0.5")),
        (BASE, Decimal("97001"), Decimal("0"), Decimal("2")),
        (BASE + timedelta(minutes=1), Decimal("97000"), Decimal("4"), Decimal("0")),
    ]


def test_a_coarser_step_merges_levels():
    table = footprint_table(_trades([(1, "97003.5", "1", "buy"), (2, "97005", "1", "sell"), (3, "97009.99", "1", "sell")]), Decimal("5"))
    assert _rows(table) == [
        (BASE, Decimal("97000"), Decimal("1"), Decimal("0")),
        (BASE, Decimal("97005"), Decimal("0"), Decimal("2")),
    ]


def test_no_trades_no_rows():
    assert footprint_table(_trades([])).num_rows == 0
    assert footprint_table(_trades([])).schema == FOOTPRINT_SCHEMA
