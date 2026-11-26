"""
Tests for replay/replay_service.py over real Kafka, Redis and Postgres, with
the replay's Spark jobs faked (test_replay_end_to_end.py runs the real ones):
a run clears, seeds, plays Bronze onto the replay's trades topic, and obeys
its commands, given directly or through the Redis controls.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from config.settings import REPLAY_COMMANDS_KEY, REPLAY_PIPELINE, REPLAY_STATUS_KEY

from control import ReplayControl
from namespace import ReplayNamespace
from producer import MarketDataProducer
from replay_service import FAILED, FINISHED, IDLE, PAUSED, PLAYING, ReplayService, parse_start

from bronze_fakes import write_agg_trades
from helpers import TopicTail, async_wait_for

START = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)
START_MS = int(START.timestamp() * 1000)


class FakeJobs:
    """Stands in for ReplayJobs: no Spark, but records what the service asked of it."""

    instances = []

    def __init__(self) -> None:
        self.started = self.stopped = False
        FakeJobs.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def wait_ready(self) -> None:
        pass

    def check(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped = True


@pytest.fixture
async def service(test_services, postgres_schema, kafka_topics, lake, tmp_path, redis_client):
    redis_client.delete(REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY)
    FakeJobs.instances.clear()
    control = ReplayControl()
    producer = MarketDataProducer(pipeline=REPLAY_PIPELINE, client_id="replay-test")
    await producer.start()
    service = ReplayService(
        control, producer, namespace=ReplayNamespace(data_root=str(tmp_path / "replay")), data_root=str(lake), jobs_factory=FakeJobs
    )
    try:
        yield service
    finally:
        await service.stop_run()
        await producer.stop()
        await control.close()
        redis_client.delete(REPLAY_COMMANDS_KEY, REPLAY_STATUS_KEY)


def _start(symbol, speed="max", **extra):
    return parse_start({"command": "start", "symbol": symbol, "start": START.isoformat(), "speed": speed, **extra})


async def test_a_run_plays_bronze_onto_the_replays_trades_topic_then_finishes(service, lake, symbol):
    written = write_agg_trades(lake, symbol, START_MS, START_MS + 5 * 60_000)

    async with TopicTail(REPLAY_PIPELINE.trades_topic) as tail:
        await service.start_run(_start(symbol, end=(START + timedelta(minutes=3)).isoformat()))
        await asyncio.wait_for(service.done.wait(), 60)
        status = service.status
        assert status["state"] == FINISHED, status
        sent = await tail.collect_json(lambda m: m["symbol"] == symbol, quiet=1.0)

    expected = [t for t in written if t["T"] < START_MS + 3 * 60_000]
    assert [m["trade_id"] for m in sent] == [str(t["a"]) for t in expected]
    assert status["trades_sent"] == len(expected)
    assert (status["source"], status["seeded"]) == ("agg_trades", True)  # "auto" resolved: nothing recorded live
    assert FakeJobs.instances[-1].started and not FakeJobs.instances[-1].stopped  # kept for the chart until "stop"

    await service.stop_run()
    assert service.status["state"] == IDLE and FakeJobs.instances[-1].stopped


async def test_pause_holds_a_run_and_resume_carries_it_on(service, lake, symbol):
    write_agg_trades(lake, symbol, START_MS, START_MS + 10 * 60_000)
    await service.start_run(_start(symbol, speed=60))  # a trade a second or so, at 60x
    await async_wait_for(lambda: service.status["state"] == PLAYING and service.status["trades_sent"] > 0, timeout=30)

    await service.handle({"command": "pause"})
    assert service.status["state"] == PAUSED
    await asyncio.sleep(0.3)  # a batch already on its way when the pause came still lands
    held = service.status["trades_sent"]
    await asyncio.sleep(1.0)
    assert service.status["trades_sent"] == held

    await service.handle({"command": "speed", "speed": "max"})
    await service.handle({"command": "resume"})
    await asyncio.wait_for(service.done.wait(), 60)
    assert service.status["state"] == FINISHED and service.status["trades_sent"] > held


async def test_a_bad_command_leaves_the_run_alone_and_says_why(service, lake, symbol):
    write_agg_trades(lake, symbol, START_MS, START_MS + 10 * 60_000)
    await service.start_run(_start(symbol, speed=1))
    await async_wait_for(lambda: service.status["state"] == PLAYING, timeout=30)

    await service.handle({"command": "speed", "speed": -3})
    await service.handle({"command": "rewind"})
    assert service.status["state"] == PLAYING
    assert "unknown command 'rewind'" in service.status["error"]


async def test_a_start_with_no_history_fails_the_run_and_stops_its_jobs(service, symbol):
    await service.start_run(_start(symbol))
    await asyncio.wait_for(service.done.wait(), 30)
    assert service.status["state"] == FAILED
    assert "neither recorded live nor backfilled" in service.status["error"]


async def test_commands_and_status_travel_through_redis(service, lake, symbol, redis_client):
    write_agg_trades(lake, symbol, START_MS, START_MS + 60_000)
    stop = asyncio.Event()
    serving = asyncio.create_task(service.serve(stop))
    try:
        await async_wait_for(lambda: redis_client.get(REPLAY_STATUS_KEY) is not None, timeout=10)
        assert json.loads(redis_client.get(REPLAY_STATUS_KEY))["state"] == IDLE

        command = {"command": "start", "symbol": symbol, "start": START.isoformat(), "speed": "max"}
        redis_client.lpush(REPLAY_COMMANDS_KEY, json.dumps(command))
        await async_wait_for(lambda: json.loads(redis_client.get(REPLAY_STATUS_KEY) or "{}").get("state") == FINISHED, timeout=60)
        status = json.loads(redis_client.get(REPLAY_STATUS_KEY))
        assert (status["symbol"], status["source"]) == (symbol, "agg_trades")
    finally:
        stop.set()
        await asyncio.wait_for(serving, 30)
    assert redis_client.get(REPLAY_STATUS_KEY) is None  # a stopped service reads as gone at once
