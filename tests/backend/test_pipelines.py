"""Tests for the `pipeline` parameter every data route takes (routers/pipelines.py): live by default, replay on request."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from starlette.websockets import WebSocketDisconnect

from config.settings import REPLAY_PIPELINE

from helpers import parse_utc, wait_for

BASE = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


@pytest.fixture
def replay_db(postgres_schema):
    with psycopg.connect(REPLAY_PIPELINE.postgres_dsn, autocommit=True) as conn:
        yield conn


def _insert_candle(conn, symbol: str, minute: datetime, close: int) -> None:
    conn.execute(
        "INSERT INTO candles_1m VALUES (%s, %s, %s, %s, %s, %s, %s, 1, 1)",
        (symbol, minute, minute + timedelta(minutes=1), close, close, close, close),
    )


def test_rest_history_comes_from_the_pipeline_asked_for(client, pg, replay_db, symbol):
    _insert_candle(pg, symbol, BASE, 100)
    _insert_candle(replay_db, symbol, BASE + timedelta(minutes=1), 200)
    try:
        live = client.get(f"/candles/{symbol}").json()
        replay = client.get(f"/candles/{symbol}", params={"pipeline": "replay"}).json()
    finally:
        pg.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))
        replay_db.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))

    assert [(parse_utc(c["window_start"]), float(c["close"])) for c in live] == [(BASE, 100)]
    assert [(parse_utc(c["window_start"]), float(c["close"])) for c in replay] == [(BASE + timedelta(minutes=1), 200)]


def test_the_footprint_comes_from_the_pipeline_asked_for(client, replay_db, symbol):
    replay_db.execute("INSERT INTO footprint_1m VALUES (%s, %s, 97000, 2, 1)", (symbol, BASE))
    try:
        params = {"start": BASE.isoformat(), "end": (BASE + timedelta(minutes=1)).isoformat()}
        live = client.get(f"/footprint/{symbol}", params=params).json()
        replay = client.get(f"/footprint/{symbol}", params={**params, "pipeline": "replay"}).json()
    finally:
        replay_db.execute("DELETE FROM footprint_1m WHERE symbol = %s", (symbol,))

    assert [bar["levels"] for bar in live] in ([], [[]])
    assert [bar["levels"] for bar in replay] == [[["97000", "2", "1"]]]


def test_an_unknown_pipeline_is_a_422(client, symbol):
    assert client.get(f"/candles/{symbol}", params={"pipeline": "staging"}).status_code == 422


def test_the_live_candle_socket_serves_the_replays_keys_when_asked(client, redis_client, symbol):
    live = json.dumps({"symbol": symbol, "close": "100"})
    replay = json.dumps({"symbol": symbol, "close": "200"})
    redis_client.set(f"candle:{symbol}:1m", live, ex=60)
    redis_client.set(f"replay:candle:{symbol}:1m", replay, ex=60)
    try:
        with client.websocket_connect(f"/ws/candles/{symbol}?pipeline=replay") as ws:
            assert ws.receive_text() == replay
            channel = f"replay:candle_updates:{symbol}:1m"
            wait_for(lambda: dict(redis_client.pubsub_numsub(channel))[channel] == 1, timeout=5)
            update = json.dumps({"symbol": symbol, "close": "201"})
            redis_client.publish(channel, update)
            assert ws.receive_text() == update
            ws.close()
            wait_for(lambda: dict(redis_client.pubsub_numsub(channel))[channel] == 0, timeout=5)
        with client.websocket_connect(f"/ws/candles/{symbol}") as ws:
            assert ws.receive_text() == live
            ws.close()
            channel = f"candle_updates:{symbol}:1m"
            wait_for(lambda: dict(redis_client.pubsub_numsub(channel))[channel] == 0, timeout=5)
    finally:
        redis_client.delete(f"candle:{symbol}:1m", f"replay:candle:{symbol}:1m")


def test_a_socket_asking_for_an_unknown_pipeline_is_closed_with_1008(client, symbol):
    with pytest.raises(WebSocketDisconnect) as closed:
        with client.websocket_connect(f"/ws/indicators/{symbol}?pipeline=staging") as ws:
            ws.receive_text()
    assert closed.value.code == 1008
