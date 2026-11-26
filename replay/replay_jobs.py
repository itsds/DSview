"""
replay_jobs.py

ReplayJobs: the replay pipeline's own streaming jobs, started for a run and
stopped with it (Phase 4a), so nothing holds a JVM between replays. They
are live's jobs, unchanged, run with DSVIEW_PIPELINE=replay
(config/settings.py's Pipeline):

- the candle aggregator (candles and the live footprint),
- the indicator job (EMA 20/50/200),
- the whale detector, so replayed whales reach the right panel. Never
  Telegram: the bot only ever reads live's whales.

The storage writer isn't one of them: a replay re-derives what live already
keeps, and its Gold would be a copy of live's.

Each job's output goes to `<replay data root>/_logs/<job>.log`. The Spark
jobs say when their queries are running (streaming/readiness.py), which
wait_ready() waits for; a job that exits early fails the wait with the end
of its log.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import redis.asyncio as redis

from config.settings import REDIS_HOST, REDIS_PORT, REPLAY_PIPELINE, REPO_ROOT, Pipeline

logger = logging.getLogger(__name__)

STREAMING_DIR = REPO_ROOT / "streaming"
# A cold Spark start resolves the Kafka connector and starts two JVMs, and
# the indicator job then warms up on a year of candles before its first
# batch completes: minutes on a slow laptop, seconds on a warm one.
READY_TIMEOUT_SECONDS = 600
STOP_TIMEOUT_SECONDS = 30
LOG_TAIL_LINES = 20


@dataclass(frozen=True)
class Job:
    name: str
    module: str  # run as `python -m <module>` from streaming/
    announces_ready: bool  # streaming/readiness.py's announce_when_running()


JOBS = (
    Job("candle-aggregator", "jobs.candle_aggregator", True),
    Job("indicators", "jobs.indicators", True),
    Job("whale-detector", "jobs.whale_detector", False),
)


class JobFailed(Exception):
    pass


class ReplayJobs:
    def __init__(
        self,
        pipeline: Pipeline = REPLAY_PIPELINE,
        jobs: Sequence[Job] = JOBS,
        redis_host: str = REDIS_HOST,
        redis_port: int = REDIS_PORT,
        env: Optional[Dict[str, str]] = None,  # more for the jobs' environment (tests: their data root, a whale threshold)
        log_dir: Optional[Path] = None,
    ) -> None:
        if pipeline.is_live:
            raise ValueError("ReplayJobs runs a replay pipeline's jobs, not live's")
        self._pipeline = pipeline
        self._jobs = list(jobs)
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._env = dict(env or {})
        self._log_dir = log_dir or Path(pipeline.data_root) / "_logs"
        self._processes: Dict[str, asyncio.subprocess.Process] = {}

    async def start(self) -> None:
        self._log_dir.mkdir(parents=True, exist_ok=True)
        env = {**os.environ, **self._env}
        env["DSVIEW_PIPELINE"] = self._pipeline.name
        env["PYTHONPATH"] = os.pathsep.join([str(REPO_ROOT), *filter(None, [env.get("PYTHONPATH")])])
        env["PYTHONUNBUFFERED"] = "1"
        for job in self._jobs:
            with open(self._log_path(job), "wb") as log:
                self._processes[job.name] = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    job.module,
                    cwd=STREAMING_DIR,
                    env=env,
                    stdout=log,
                    stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True,  # a Ctrl+C in the service's terminal reaches the service, which stops them
                )
            logger.info("Started replay job %s (pid %d), log %s", job.name, self._processes[job.name].pid, self._log_path(job))

    async def wait_ready(self, timeout: float = READY_TIMEOUT_SECONDS) -> None:
        """Until every job that announces readiness has; raises JobFailed if any job exits meanwhile."""
        keys = [f"{self._pipeline.redis_prefix}ready:{job.name}" for job in self._jobs if job.announces_ready]
        client = redis.Redis(host=self._redis_host, port=self._redis_port)
        deadline = time.monotonic() + timeout
        try:
            while True:
                self.check()
                if not keys or await client.exists(*keys) == len(keys):
                    return
                if time.monotonic() > deadline:
                    raise JobFailed(f"replay jobs not ready after {timeout:.0f} s (logs in {self._log_dir})")
                await asyncio.sleep(0.5)
        finally:
            await client.aclose()

    def check(self) -> None:
        """Raise JobFailed if a job has exited."""
        for job in self._jobs:
            process = self._processes.get(job.name)
            if process is not None and process.returncode is not None:
                raise JobFailed(
                    f"replay job {job.name} exited with code {process.returncode}:\n{self._tail(job)}"
                )

    async def stop(self) -> None:
        """Ctrl+C every job (they shut down cleanly on it), then SIGTERM and SIGKILL any that linger."""
        running = {name: p for name, p in self._processes.items() if p.returncode is None}
        for sig, wait in ((signal.SIGINT, STOP_TIMEOUT_SECONDS), (signal.SIGTERM, 10), (signal.SIGKILL, 10)):
            if not running:
                break
            for process in running.values():
                try:
                    os.killpg(process.pid, sig)  # the whole session: Spark's JVM too
                except ProcessLookupError:
                    pass
            await asyncio.wait([asyncio.ensure_future(p.wait()) for p in running.values()], timeout=wait)
            running = {name: p for name, p in running.items() if p.returncode is None}
        for name in running:
            logger.error("Replay job %s didn't stop", name)
        self._processes.clear()

    @property
    def log_dir(self) -> Path:
        return self._log_dir

    def _log_path(self, job: Job) -> Path:
        return self._log_dir / f"{job.name}.log"

    def _tail(self, job: Job) -> str:
        try:
            lines = self._log_path(job).read_text(errors="replace").splitlines()
        except OSError:
            return "(no log)"
        return "\n".join(lines[-LOG_TAIL_LINES:])

    def pids(self) -> List[Optional[int]]:
        return [p.pid for p in self._processes.values()]
