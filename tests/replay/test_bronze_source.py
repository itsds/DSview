"""Tests for replay/bronze_source.py: a span of Bronze's trades, in event-time order, from the sink's log or the backfill's files."""

from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

import bronze_source
from bronze_source import AGG_TRADES, TRADES, NoHistory, choose_source, committed_files, read_trades

from bronze_fakes import T0, at, write_agg_trades


def _trades(source, lake, symbol, start, end=None):
    tables = list(read_trades(source, str(lake), symbol, start, end))
    for table in tables:
        assert table.schema == bronze_source.TRADE_SCHEMA
    return pa.concat_tables(tables).to_pylist() if tables else []


def _ids(rows):
    return [row["trade_id"] for row in rows]


def test_only_files_the_sink_committed_are_read(sink, lake):
    sink.batch("BTCUSDT", [(at(1), "1", "100", "1", "buy")])
    sink.file("BTCUSDT", [(at(2), "2", "100", "1", "buy")])  # a failed batch's file: in no log entry
    sink.batch("BTCUSDT", [(at(2), "2", "100", "1", "buy"), (at(3), "3", "101", "2", "sell")])  # its rerun

    rows = _trades(TRADES, lake, "BTCUSDT", T0)
    assert _ids(rows) == ["1", "2", "3"]
    assert rows[2]["event_time"] == at(3) and rows[2]["side"] == "sell" and str(rows[2]["price"]).startswith("101")


def test_a_compacted_log_lists_every_file_up_to_it(sink, lake):
    for i in range(23):  # batches 0..22: compactions at 9 and 19
        sink.batch("BTCUSDT", [(at(i), str(i), "100", "1", "buy")])
    assert len(committed_files(lake / "bronze" / "trades")) == 23
    assert _ids(_trades(TRADES, lake, "BTCUSDT", T0)) == [str(i) for i in range(23)]


def test_other_symbols_files_are_left_alone(sink, lake):
    sink.batch("ETHUSDT", [(at(1), "e1", "3000", "1", "buy")])
    sink.batch("BTCUSDT", [(at(1), "b1", "100", "1", "buy")])
    assert _ids(_trades(TRADES, lake, "BTCUSDT", T0)) == ["b1"]


def test_the_span_is_start_inclusive_end_exclusive(sink, lake):
    sink.batch("BTCUSDT", [(at(s), str(s), "100", "1", "buy") for s in range(10)])
    assert _ids(_trades(TRADES, lake, "BTCUSDT", at(3), at(6))) == ["3", "4", "5"]


def test_files_written_before_the_start_are_never_opened(sink, lake):
    sink.batch("BTCUSDT", [], written=T0 - timedelta(hours=2), corrupt=True)
    sink.batch("BTCUSDT", [(at(1), "1", "100", "1", "buy")])
    assert _ids(_trades(TRADES, lake, "BTCUSDT", T0)) == ["1"]


def test_reading_stops_at_the_first_file_past_the_end(sink, lake):
    sink.batch("BTCUSDT", [(at(1), "1", "100", "1", "buy")])
    sink.batch("BTCUSDT", [(at(600), "600", "100", "1", "buy")])  # well past end + slack
    sink.batch("BTCUSDT", [], written=at(700), corrupt=True)
    assert _ids(_trades(TRADES, lake, "BTCUSDT", T0, at(60))) == ["1"]


def test_a_batch_caught_up_after_a_restart_is_still_read(sink, lake):
    # The writer was down for an hour while ingestion ran: its first batch
    # back holds the whole hour, written long after the trades in it.
    sink.batch("BTCUSDT", [(at(s), str(s), "100", "1", "buy") for s in (10, 1800, 3500)], written=at(3600))
    sink.batch("BTCUSDT", [(at(3601), "3601", "100", "1", "buy")])
    assert _ids(_trades(TRADES, lake, "BTCUSDT", at(1000), at(3550))) == ["1800", "3500"]


def test_trades_arriving_out_of_order_come_out_sorted_ties_in_arrival_order(sink, lake, monkeypatch):
    monkeypatch.setattr(bronze_source, "CHUNK_ROWS", 3)  # force the reorder buffer to hand on mid-stream
    sink.batch("BTCUSDT", [(at(5), "a", "100", "1", "buy"), (at(7), "b", "100", "1", "buy")])
    sink.batch("BTCUSDT", [(at(6), "c", "100", "1", "buy"), (at(7), "d", "100", "1", "buy")])
    sink.batch("BTCUSDT", [(at(100), "e", "100", "1", "buy"), (at(101), "f", "100", "1", "buy")])
    sink.batch("BTCUSDT", [(at(100), "g", "100", "1", "buy")])
    rows = _trades(TRADES, lake, "BTCUSDT", T0)
    assert _ids(rows) == ["a", "c", "b", "d", "e", "g", "f"]


def test_files_are_found_after_the_data_root_has_moved(sink, lake, tmp_path):
    sink.batch("BTCUSDT", [(at(1), "1", "100", "1", "buy")])
    moved = tmp_path / "moved"
    shutil.move(str(lake), str(moved))
    assert _ids(_trades(TRADES, moved, "BTCUSDT", T0)) == ["1"]


def test_no_log_means_no_trades(lake):
    assert _trades(TRADES, lake, "BTCUSDT", T0) == []


def test_agg_trades_come_from_the_backfills_day_files_across_midnight(lake):
    midnight = int(datetime(2026, 11, 21, tzinfo=timezone.utc).timestamp() * 1000)
    written = write_agg_trades(lake, "BTCUSDT", midnight - 120_000, midnight + 120_000)

    start = datetime.fromtimestamp((midnight - 60_000) / 1000, timezone.utc)
    end = datetime.fromtimestamp((midnight + 60_000) / 1000, timezone.utc)
    rows = _trades(AGG_TRADES, lake, "BTCUSDT", start, end)

    expected = [t for t in written if midnight - 60_000 <= t["T"] < midnight + 60_000]
    assert _ids(rows) == [str(t["a"]) for t in expected]
    first = rows[0]
    assert int(first["event_time"].timestamp() * 1000) == expected[0]["T"]
    assert first["side"] == ("sell" if expected[0]["m"] else "buy")
    assert first["exchange"] == "binance"


def test_the_source_is_the_live_recording_where_it_has_the_start(sink, lake):
    sink.batch("BTCUSDT", [(at(30), "1", "100", "1", "buy")])
    start_ms = int(T0.timestamp() * 1000)
    write_agg_trades(lake, "BTCUSDT", start_ms, start_ms + 3_600_000)

    assert choose_source(str(lake), "BTCUSDT", T0) == TRADES
    assert choose_source(str(lake), "BTCUSDT", at(1800)) == AGG_TRADES  # nothing recorded live then


def test_no_history_at_the_start_is_said_so(lake):
    with pytest.raises(NoHistory, match="neither recorded live nor backfilled"):
        choose_source(str(lake), "BTCUSDT", T0)


def test_an_unknown_source_is_refused(lake):
    with pytest.raises(ValueError, match="unknown source"):
        read_trades("klines", str(lake), "BTCUSDT", T0)

