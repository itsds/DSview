"""
replay_service.py

The replay service (Phase 4a): a long-running process that starts, steers
and stops replays when the chart asks, through the backend
(backend/routers/replay.py) and two Redis keys (control.py). A replay feeds
Bronze's history (bronze_source.py) through the replay pipeline's own copy
of the live jobs at a chosen speed, so the chart can watch a past day form
bar by bar, with its indicators, footprint and whales, while live carries on
untouched (docs/ROADMAP.md, Phase 4's kickoff decisions 1 and 2).

A run, from a "start" command:

1. **Clear** the replay pipeline (namespace.py): its topics, tables, Redis
   keys and checkpoints. One replay at a time; a new one replaces the last.
2. **Seed** its history: live's candles before the start, and the day of
   footprint before it, so the chart has bars to the left of the first
   replayed one and the indicators warm up on the same history live's did.
3. **Start its jobs** (replay_jobs.py) and wait until their Spark queries run.
4. **Play** (player.py) at the requested speed, until the history runs out
   or a "stop" comes. Pause, resume and speed changes apply at once.
5. **Finished**: the jobs keep running, so the chart keeps what it shows,
   until "stop" ends the run (or the next "start" replaces it).

Commands (JSON objects; see control.py for the transport):

    {"command": "start", "symbol": "BTCUSDT", "start": "<ISO 8601>",
     "speed": 10 | "max", "source": "auto" | "trades" | "agg_trades",
     "end": "<ISO 8601>" | null}
    {"command": "pause"}   {"command": "resume"}   {"command": "stop"}
    {"command": "speed", "speed": 100}

Status, rewritten every second:

    {"state": "idle" | "starting" | "playing" | "paused" | "finished" | "failed",
     "phase": what a starting run is doing, "run": a number per run,
     "symbol", "source", "start", "end", "speed", "clock": the replay's
     current time, "trades_sent", "seeded": true once the replay's history
     is in (a chart switching to the replay then finds it), "error",
     "updated_at"}

Run from replay/, with the repo root on PYTHONPATH (scripts/dev.sh gives it
a window):

    cd replay && python replay_service.py

Or play one replay from start to end without the chart, wait until Spark
has every trade in the replay's candles, and stop (how a replay is checked
against live; see compare.py):

    python replay_service.py --once --symbol BTCUSDT --start 2026-11-20T00:00Z --end 2026-11-21T00:00Z --speed max

Author: @DS
"""

from __future__ import annotations

import sys
from pathlib import Path

# replay/ sends through ingestion's MarketDataProducer and TradeEvent, which
# import their siblings by bare name, as every service here does from its
# own folder. (Unlike streaming/, nothing here starts Python workers that
# would need it on PYTHONPATH instead.)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))

import argparse  # noqa: E402
import asyncio  # noqa: E402
import contextlib  # noqa: E402
import logging  # noqa: E402
import signal  # noqa: E402
import time  # noqa: E402
from datetime import datetime, timezone  # noqa: E402
from typing import Any, Dict, Optional  # noqa: E402

import psycopg  # noqa: E402

from config.settings import KAFKA_BOOTSTRAP_SERVERS, LIVE_PIPELINE, REPLAY_PIPELINE  # noqa: E402
from producer import MarketDataProducer  # noqa: E402

from bronze_source import SOURCES, choose_source, read_trades  # noqa: E402
from clock import MAX, ReplayClock, parse_speed  # noqa: E402
from control import ReplayControl  # noqa: E402
from replay_jobs import ReplayJobs  # noqa: E402
from namespace import ReplayNamespace  # noqa: E402
from player import ReplayPlayer  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger("replay")

IDLE, STARTING, PLAYING, PAUSED, FINISHED, FAILED = "idle", "starting", "playing", "paused", "finished", "failed"
AUTO = "auto"
DEFAULT_SPEED = 10.0
HEARTBEAT_SECONDS = 1.0
COMMAND_POLL_SECONDS = 1.0
JOB_CHECK_SECONDS = 2.0
# --once: how long Spark may take to have every trade in the replay's candles.
DRAIN_TIMEOUT_SECONDS = 600


class BadCommand(ValueError):
    pass


def parse_time(text: str) -> datetime:
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def parse_start(command: Dict[str, Any]) -> Dict[str, Any]:
    """A start command's parameters, checked: raises BadCommand with what's wrong."""
    try:
        symbol = str(command["symbol"]).upper()
        start = parse_time(command["start"])
        end = parse_time(command["end"]) if command.get("end") else None
        speed = parse_speed(command.get("speed", DEFAULT_SPEED))
    except (KeyError, TypeError, ValueError) as error:
        raise BadCommand(f"bad start command: {error}") from None
    source = command.get("source") or AUTO
    if source != AUTO and source not in SOURCES:
        raise BadCommand(f"source must be {AUTO!r} or one of {list(SOURCES)}")
    if end is not None and end <= start:
        raise BadCommand("end must be after start")
    return {"symbol": symbol, "start": start, "end": end, "speed": speed, "source": source}


class ReplayService:
    def __init__(
        self,
        control: ReplayControl,
        producer: MarketDataProducer,
        namespace: Optional[ReplayNamespace] = None,
        data_root: str = LIVE_PIPELINE.data_root,  # where Bronze is: live's lake
        jobs_factory=ReplayJobs,
    ) -> None:
        self._control = control
        self._producer = producer
        self._namespace = namespace or ReplayNamespace()
        self._data_root = data_root
        self._jobs_factory = jobs_factory
        self._jobs: Optional[ReplayJobs] = None
        self._player: Optional[ReplayPlayer] = None
        self._run_task: Optional[asyncio.Task] = None
        self._runs = 0
        self._status: Dict[str, Any] = {"state": IDLE}
        self._wanted_speed: Any = None
        self._paused = False
        self.done = asyncio.Event()  # set whenever a run ends (finished, failed or stopped)

    @property
    def status(self) -> Dict[str, Any]:
        status = dict(self._status)
        if self._player is not None:
            status["clock"] = self._player.clock.time().isoformat()
            status["trades_sent"] = self._player.trades_sent
            status["speed"] = self._player.clock.speed
        status["updated_at"] = datetime.now(timezone.utc).isoformat()
        return status

    async def serve(self, stop: asyncio.Event) -> None:
        """Handle commands until `stop` is set; then end any run."""
        await self._control.clear_commands()
        heartbeat = asyncio.create_task(self._heartbeat())
        logger.info("Replay service ready")
        try:
            while not stop.is_set():
                command = await self._control.next_command(COMMAND_POLL_SECONDS)
                if command is not None:
                    await self.handle(command)
        finally:
            await self.stop_run()
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            await self._control.clear_status()

    async def handle(self, command: Dict[str, Any]) -> None:
        name = command.get("command")
        logger.info("Command: %s", command)
        try:
            if name == "start":
                await self.start_run(parse_start(command))
            elif name == "stop":
                await self.stop_run()
            elif name == "pause":
                self._set_paused(True)
            elif name == "resume":
                self._set_paused(False)
            elif name == "speed":
                self._set_speed(command.get("speed"))
            else:
                raise BadCommand(f"unknown command {name!r}")
        except BadCommand as error:
            # A bad command leaves a running replay alone; the status says what was wrong.
            logger.warning("%s", error)
            self._status["error"] = str(error)

    async def start_run(self, params: Dict[str, Any]) -> None:
        await self.stop_run()
        self._runs += 1
        self._paused = False
        self._wanted_speed = params["speed"]
        self.done.clear()
        self._status = {
            "state": STARTING,
            "phase": "clearing the last replay",
            "run": self._runs,
            "symbol": params["symbol"],
            "source": params["source"],
            "start": params["start"].isoformat(),
            "end": params["end"].isoformat() if params["end"] else None,
            "speed": params["speed"],
            "clock": params["start"].isoformat(),
            "trades_sent": 0,
            "seeded": False,
            "error": None,
        }
        self._run_task = asyncio.create_task(self._run(params))

    async def stop_run(self) -> None:
        """End the current run, if any, and stop its jobs; back to idle."""
        task, self._run_task = self._run_task, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self._stop_jobs()
        if self._status.get("state") != IDLE:
            logger.info("Replay %s stopped", self._status.get("run"))
        self._player = None
        self._status = {"state": IDLE}
        self.done.set()

    async def _run(self, params: Dict[str, Any]) -> None:
        symbol, start, end = params["symbol"], params["start"], params["end"]
        try:
            await self._namespace.reset()
            source = params["source"]
            if source == AUTO:
                source = await asyncio.to_thread(choose_source, self._data_root, symbol, start)
            self._status.update(source=source, phase="copying the history before the start")
            copied = await self._namespace.seed(symbol, start)
            logger.info("Seeded the replay with %s", copied)
            self._status.update(seeded=True, phase="starting the replay's Spark jobs")

            self._jobs = self._jobs_factory()
            await self._jobs.start()
            await self._jobs.wait_ready()

            clock = ReplayClock(start, self._wanted_speed)
            self._player = ReplayPlayer(read_trades(source, self._data_root, symbol, start, end), clock, self._producer.send_batch, symbol)
            self._status.update(phase=None)
            self._apply_play_state()
            logger.info("Replaying %s from %s (%s) at %s", symbol, start.isoformat(), source, clock.speed)
            await self._play_watching_jobs()
            self._status.update(state=FINISHED)
            logger.info("Replay %s finished: %d trades", self._status["run"], self._player.trades_sent)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # NoHistory, JobFailed, a broker or database error: all end the run alike
            logger.exception("Replay %s failed", self._status.get("run"))
            self._status.update(state=FAILED, phase=None, error=str(error))
            await self._stop_jobs()
        finally:
            if self._status.get("state") in (FINISHED, FAILED):
                self.done.set()

    async def _play_watching_jobs(self) -> None:
        """Play, failing the run if a replay job dies meanwhile (its trades would go nowhere)."""
        play = asyncio.create_task(self._player.run())
        try:
            while not play.done():
                await asyncio.wait([play], timeout=JOB_CHECK_SECONDS)
                self._jobs.check()
            play.result()
        finally:
            if not play.done():
                play.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await play

    def _set_paused(self, paused: bool) -> None:
        if self._status.get("state") in (IDLE, FAILED, FINISHED):
            raise BadCommand(f"nothing to {'pause' if paused else 'resume'}")
        self._paused = paused
        self._apply_play_state()

    def _set_speed(self, value: Any) -> None:
        try:
            speed = parse_speed(value)
        except ValueError as error:
            raise BadCommand(str(error)) from None
        self._wanted_speed = speed
        self._status["speed"] = speed
        if self._player is not None:
            self._player.set_speed(speed)

    def _apply_play_state(self) -> None:
        if self._player is None:
            return  # still starting: applied when the player exists
        if self._paused:
            self._player.pause()
            self._status["state"] = PAUSED
        else:
            self._player.play()
            self._status["state"] = PLAYING

    async def _stop_jobs(self) -> None:
        jobs, self._jobs = self._jobs, None
        if jobs is not None:
            await jobs.stop()

    async def _heartbeat(self) -> None:
        while True:
            try:
                await self._control.publish_status(self.status)
            except Exception:  # noqa: BLE001 - Redis down: try again next beat
                logger.warning("Couldn't publish the replay status", exc_info=True)
            await asyncio.sleep(HEARTBEAT_SECONDS)


def replayed_trade_count(symbol: str, start: datetime) -> int:
    """Trades in the replay's candles from `start` on (each counted once its minute's last batch is in)."""
    with psycopg.connect(REPLAY_PIPELINE.postgres_dsn) as conn:
        row = conn.execute(
            "SELECT coalesce(sum(trade_count), 0) FROM candles_1m WHERE symbol = %s AND window_start >= %s", (symbol, start)
        ).fetchone()
    return int(row[0])


async def run_once(service: ReplayService, params: Dict[str, Any]) -> int:
    await service.start_run(params)
    await service.done.wait()
    status = service.status
    if status["state"] != FINISHED:
        print(f"Replay failed: {status.get('error')}", file=sys.stderr)
        await service.stop_run()
        return 1
    sent = status["trades_sent"]
    deadline = time.monotonic() + DRAIN_TIMEOUT_SECONDS
    counted = -1
    while time.monotonic() < deadline:
        counted = await asyncio.to_thread(replayed_trade_count, params["symbol"], params["start"])
        if counted >= sent:
            break
        await asyncio.sleep(2)
    await service.stop_run()
    print(f"Replayed {sent:,} trades ({status['source']}) from {status['start']} to {status['clock']}; "
          f"the replay's candles hold {counted:,} of them")
    return 0 if counted == sent else 1


async def main_async(args: argparse.Namespace) -> int:
    control = ReplayControl()
    producer = MarketDataProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="dsview-replay", pipeline=REPLAY_PIPELINE)
    await producer.start()
    try:
        service = ReplayService(control, producer)
        if args.once:
            params = parse_start(
                {"command": "start", "symbol": args.symbol, "start": args.start, "end": args.end, "speed": args.speed, "source": args.source}
            )
            return await run_once(service, params)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await service.serve(stop)
        return 0
    finally:
        await producer.stop()
        await control.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="DSView's replay service (Phase 4a)")
    parser.add_argument("--once", action="store_true", help="play one replay to its end, then exit (no chart needed)")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start", help="ISO 8601, UTC unless it says otherwise (--once)")
    parser.add_argument("--end", help="ISO 8601 (--once; default: as far as the history goes)")
    parser.add_argument("--speed", default=MAX, help=f"a multiple of real time, or {MAX!r} (--once; default %(default)s)")
    parser.add_argument("--source", default=AUTO, choices=[AUTO, *SOURCES], help="--once; default %(default)s")
    args = parser.parse_args(argv)
    if args.once and not args.start:
        parser.error("--once needs --start")
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
