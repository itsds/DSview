"""
Tests for `python futures_main.py` itself, run as a subprocess the way
scripts/dev.sh runs it, against a fake Binance on the real clock: a first
start publishes the 30 days Binance keeps (the backfill), a restart
resumes from what the topic holds without repeating it, and Ctrl+C stops
it cleanly. Plus the refusals: the replay pipeline, a bad --refetch-days,
a symbol Binance doesn't know.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from binance_futures_rest import PERIOD_MS
from config.settings import FUTURES_RATIO_KINDS, FUTURES_RATIOS_TOPIC

from helpers import TopicTail, async_wait_for

INGESTION_DIR = Path(__file__).resolve().parents[2] / "ingestion"
DAY_MS = 86_400_000


def _env(fake, symbol, **overrides) -> dict:
    env = dict(os.environ)
    env.update(
        PYTHONPATH=str(INGESTION_DIR.parent),
        DSVIEW_BINANCE_FUTURES_REST_URL=fake.url,
        DSVIEW_SYMBOLS=symbol,
        **overrides,
    )
    return env


def _start(env, log: Path, *args) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "futures_main.py", *args], cwd=INGESTION_DIR, env=env, stdout=log.open("w"), stderr=subprocess.STDOUT
    )


def _stop(process: subprocess.Popen) -> int:
    process.send_signal(signal.SIGINT)
    return process.wait(timeout=30)


def _log(path: Path) -> str:
    return path.read_text() if path.exists() else ""


def _start_ms(message) -> int:
    return int(datetime.fromisoformat(message["event_time"].replace("Z", "+00:00")).timestamp() * 1000)


async def test_a_first_start_publishes_30_days_and_a_restart_repeats_none_of_it(fake_binance, kafka_topics, symbol, tmp_path):
    log = tmp_path / "poller.log"
    async with TopicTail(FUTURES_RATIOS_TOPIC) as tail:
        started = int(time.time() * 1000)
        poller = _start(_env(fake_binance, symbol), log)
        try:
            await async_wait_for(lambda: _log(log).count("caught up to") == 4, timeout=120, message="never caught up")
        finally:
            assert _stop(poller) == 0, log.read_text()
        first = await tail.collect_json(lambda m: m["symbol"] == symbol, quiet=2.0)

        restarted = _start(_env(fake_binance, symbol), tmp_path / "restart.log")
        try:
            await async_wait_for(lambda: "Resuming 4 of 4 ratios" in _log(tmp_path / "restart.log"), timeout=60)
            await asyncio.sleep(2)  # anything it would repeat comes at once
        finally:
            assert _stop(restarted) == 0, (tmp_path / "restart.log").read_text()
        again = await tail.collect_json(lambda m: m["symbol"] == symbol, quiet=1.0)

    assert "Futures poller stopped cleanly" in log.read_text()
    for kind in FUTURES_RATIO_KINDS:
        starts = [_start_ms(m) for m in first if m["kind"] == kind]
        assert starts == list(range(starts[0], starts[-1] + PERIOD_MS, PERIOD_MS)), kind
        assert starts[0] - (started - 30 * DAY_MS) < 2 * PERIOD_MS, "the 30 days Binance keeps"
        assert len(starts) >= 30 * 288 - 2
        # The restart only adds a period that closed meanwhile, never one already there.
        repeats = {_start_ms(m) for m in again if m["kind"] == kind} & set(starts)
        assert not repeats, kind


def test_refuses_the_replay_pipeline(fake_binance, test_services, symbol, tmp_path):
    result = subprocess.run(
        [sys.executable, "futures_main.py"],
        cwd=INGESTION_DIR,
        env=_env(fake_binance, symbol, DSVIEW_PIPELINE="replay"),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0 and "feeds the live pipeline only" in result.stderr


def test_refetch_days_must_be_inside_binances_30(fake_binance, symbol):
    result = subprocess.run(
        [sys.executable, "futures_main.py", "--refetch-days", "45"],
        cwd=INGESTION_DIR,
        env=_env(fake_binance, symbol),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 2 and "at most 30" in result.stderr


def test_a_symbol_binance_refuses_stops_it_with_status_1(fake_binance, kafka_topics, tmp_path):
    log = tmp_path / "poller.log"
    poller = _start(_env(fake_binance, "BTCBUSD"), log)
    assert poller.wait(timeout=60) == 1
    assert "Futures poller stopped: Binance Futures refused the request" in log.read_text()
