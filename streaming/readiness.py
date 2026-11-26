"""
readiness.py

Tells replay/ when a Spark job's queries are actually running, so a replay
run's clock starts once Spark can keep up rather than while its JVM is
still resolving the Kafka connector (tens of seconds on a cold start).

Only for a pipeline other than live: live jobs serve whatever arrives from
now on and nothing waits for them. Replay's jobs read their topics from the
start of the run (Pipeline.starting_offsets), so a trade produced before a
job is ready is never lost; readiness only keeps the first replayed minutes
from all landing in one batch.

Author: @DS
"""

from __future__ import annotations

import time
from typing import Sequence

from config.settings import PIPELINE

from redis_sink import announce_ready

POLL_SECONDS = 0.5


def announce_when_running(queries: Sequence["pyspark.sql.streaming.StreamingQuery"], job: str) -> None:  # noqa: F821
    """
    Once every query in `queries` has completed its first batch (Spark sets
    lastProgress then), announce `job` ready (redis_sink.announce_ready).
    Returns without announcing if a query stops first: the caller's
    awaitTermination() then raises its error. A no-op on the live pipeline.
    """
    if PIPELINE.is_live:
        return
    while any(query.lastProgress is None for query in queries):
        if not all(query.isActive for query in queries):
            return
        time.sleep(POLL_SECONDS)
    announce_ready(job)
