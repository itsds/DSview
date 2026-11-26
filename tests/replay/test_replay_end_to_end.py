"""
A replay through the real jobs (replay/replay_jobs.py starts the live jobs as
the replay pipeline: two Spark applications and the whale detector), over
real Kafka, Redis and Postgres. Six minutes of the backfill's aggTrades
should come back as exactly Binance's klines for them (fake_binance.py's
SyntheticMarket computes both), with the footprint, indicators and whales
all in the replay's namespace and none of it in live's.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg
import pytest

from config.settings import REPLAY_COMMANDS_KEY, REPLAY_PIPELINE, REPLAY_STATUS_KEY, WHALE_ALERTS_TOPIC

from control import ReplayControl
from namespace import ReplayNamespace
from producer import MarketDataProducer
from replay_jobs import ReplayJobs
from replay_service import FINISHED, ReplayService, parse_start, replayed_trade_count

from bronze_fakes import write_agg_trades
from fake_binance import SyntheticMarket
from helpers import TopicTail, async_wait_for

START = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)
START_MS = int(START.timestamp() * 1000)
MINUTES = 6


@pytest.mark.spark
@pytest.mark.timeout(600)
async def test_a_replay_rebuilds_binances_klines_in_its_own_namespace(
    test_services, postgres_schema, kafka_topics, pg, redis_client, lake, tmp_path, symbol
):
    market = SyntheticMarket()
    trades = write_agg_trades(lake, symbol, START_MS, START_MS + MINUTES * 60_000, market)
    # SyntheticMarket's price level depends on the symbol, so the whale
    # threshold comes from the trades: the top tenth or so of them clear it.
    notionals = sorted(Decimal(t["p"]) * Decimal(t["q"]) for t in trades)
    threshold = notionals[len(notionals) * 9 // 10]
    whales = [t for t in trades if Decimal(t["p"]) * Decimal(t["q"]) >= threshold]
    assert 0 < len(whales) < len(trades)
    # Live history before the start, for the seed (and the indicators' warm-up).
    for i in range(1, 31):
        minute = START - timedelta(minutes=i)
        pg.execute("INSERT INTO candles_1m VALUES (%s, %s, %s, 100, 100, 100, 100, 1, 1)", (symbol, minute, minute + timedelta(minutes=1)))

    redis_client.delete(REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY)
    control = ReplayControl()
    producer = MarketDataProducer(pipeline=REPLAY_PIPELINE, client_id="replay-e2e")
    await producer.start()
    jobs_env = {"DSVIEW_DATA_ROOT": str(lake), "DSVIEW_WHALE_THRESHOLD_USD": str(threshold)}
    service = ReplayService(
        control,
        producer,
        namespace=ReplayNamespace(data_root=str(lake / "replay")),
        data_root=str(lake),
        jobs_factory=lambda: ReplayJobs(env=jobs_env, log_dir=tmp_path / "logs"),
    )
    try:
        async with TopicTail(REPLAY_PIPELINE.whale_alerts_topic) as replay_whales, TopicTail(WHALE_ALERTS_TOPIC) as live_whales:
            await service.start_run(
                parse_start({"command": "start", "symbol": symbol, "start": START.isoformat(), "speed": "max"})
            )
            await asyncio.wait_for(service.done.wait(), 400)
            status = service.status
            assert status["state"] == FINISHED, f"{status}\nlogs: {tmp_path / 'logs'}"
            assert status["trades_sent"] == len(trades)

            await async_wait_for(
                lambda: replayed_trade_count(symbol, START) == len(trades), timeout=120, interval=1,
                message="Spark never had every replayed trade in the replay's candles",
            )
            alerts = await replay_whales.collect_json(lambda a: a["symbol"] == symbol, quiet=3.0)
            assert sorted(a["trade_id"] for a in alerts) == sorted(str(t["a"]) for t in whales)
            assert await live_whales.collect_json(lambda a: a["symbol"] == symbol, quiet=0.5) == []

        with psycopg.connect(REPLAY_PIPELINE.postgres_dsn) as replay:
            candles = {
                row[0]: row[1:]
                for row in replay.execute(
                    "SELECT window_start, open, high, low, close, volume FROM candles_1m WHERE symbol = %s AND window_start >= %s",
                    (symbol, START),
                )
            }
            seeded = replay.execute("SELECT count(*) FROM candles_1m WHERE symbol = %s AND window_start < %s", (symbol, START)).fetchone()[0]
            footprint = dict(
                replay.execute(
                    "SELECT window_start, sum(buy_volume + sell_volume) FROM footprint_1m WHERE symbol = %s AND window_start >= %s GROUP BY 1",
                    (symbol, START),
                ).fetchall()
            )
            await async_wait_for(
                lambda: _indicator_rows(symbol) > 0, timeout=60, interval=1, message="no indicator rows in the replay's schema"
            )

        klines = {
            datetime.fromtimestamp(k[0] / 1000, timezone.utc): tuple(Decimal(v) for v in k[1:6])
            for k in market.klines_between(symbol, START_MS, START_MS + MINUTES * 60_000)
        }
        assert {minute: tuple(Decimal(v) for v in values) for minute, values in candles.items()} == klines
        assert seeded == 30
        assert footprint == {minute: volume for minute, (*_, volume) in candles.items()}

        # Live's namespace saw none of it.
        assert pg.execute("SELECT count(*) AS n FROM candles_1m WHERE symbol = %s AND window_start >= %s", (symbol, START)).fetchone()["n"] == 0
        assert pg.execute("SELECT count(*) AS n FROM footprint_1m WHERE symbol = %s", (symbol,)).fetchone()["n"] == 0
        assert redis_client.get(f"candle:{symbol}:1m") is None
        assert redis_client.get(f"replay:candle:{symbol}:1m") is not None
    finally:
        await service.stop_run()
        await producer.stop()
        await control.close()
        pg.execute("DELETE FROM candles_1m WHERE symbol = %s", (symbol,))
        redis_client.delete(REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY)


def _indicator_rows(symbol: str) -> int:
    with psycopg.connect(REPLAY_PIPELINE.postgres_dsn) as replay:
        return replay.execute("SELECT count(*) FROM candle_indicators WHERE symbol = %s", (symbol,)).fetchone()[0]
