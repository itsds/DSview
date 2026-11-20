"""
settings.py

DSView's one config module. Every service (ingestion/, streaming/,
backend/, storage/) reads its broker address, Redis and Postgres
locations, data paths, symbols, topic names and whale threshold from
here instead of keeping its own copy.

Two kinds of value live here:

- **Deployment settings**: where things run and what to ingest. Each can
  be overridden with a DSVIEW_* environment variable (named next to it
  below), so running a service in a container later, where Redis is
  `redis:6379` rather than `localhost:6379`, or pointing it at the cloud
  needs an environment variable, not a code edit. The defaults match
  docker-compose.yml, so local dev needs no variables at all.
- **Pipeline contract**: the Kafka topic names. The producer and every
  consumer must agree on them, so they are deliberately not overridable
  from the environment — renaming a topic is a code change, made here
  once.

What stays out: constants that tune one module and carry their own
explanation there — the Redis key TTL (streaming/redis_sink.py), the
dedup watermark (streaming/jobs/storage_writer.py), the REST page limits
and the timeframe table (backend/routers/candles.py). Moving those here
would split each value from the comment that justifies it.

Importing: services are run from inside their own folders, so the repo
root has to be on PYTHONPATH for `from config.settings import ...` to
resolve. scripts/dev.sh sets it for every service, PyCharm adds the
project's content root by default, and by hand it is one
`export PYTHONPATH="$PWD"` from the repo root (see CLAUDE.md). An
environment variable rather than a sys.path.insert() in each entrypoint
also reaches Spark's Python worker processes, which inherit PYTHONPATH
but not the driver's sys.path.

Author: @DS
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parent.parent

_ENV_PREFIX = "DSVIEW_"


def _env(name: str, default: str) -> str:
    """DSVIEW_<name> if it's set and non-empty, else `default`."""
    return os.environ.get(_ENV_PREFIX + name) or default


def _env_list(name: str, default: List[str]) -> List[str]:
    """A comma-separated DSVIEW_<name> as a list, else `default`."""
    raw = os.environ.get(_ENV_PREFIX + name)
    if not raw:
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


# --- Deployment settings (DSVIEW_* overridable) ------------------------------

# Symbols ingestion subscribes to. DSVIEW_SYMBOLS="BTCUSDT,ETHUSDT"
SYMBOLS: List[str] = _env_list("SYMBOLS", ["BTCUSDT"])

# Redpanda's external listener, for processes on the host (see
# docker-compose.yml for why containers use redpanda:9093 instead).
# DSVIEW_KAFKA_BOOTSTRAP_SERVERS
KAFKA_BOOTSTRAP_SERVERS: str = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# DSVIEW_REDIS_HOST, DSVIEW_REDIS_PORT
REDIS_HOST: str = _env("REDIS_HOST", "localhost")
REDIS_PORT: int = int(_env("REDIS_PORT", "6379"))

# Local-dev credentials from docker-compose.yml — never reuse them anywhere
# real; set DSVIEW_POSTGRES_DSN instead.
POSTGRES_DSN: str = _env("POSTGRES_DSN", "postgresql://dsview:dsview@localhost:5432/dsview")

# Root of the Bronze/Silver/Gold Parquet layers (gitignored). Absolute, so
# it no longer depends on which folder a job is started from.
# DSVIEW_DATA_ROOT
DATA_ROOT: str = _env("DATA_ROOT", str(REPO_ROOT / "storage" / "data"))

# Notional (price x quantity, in the quote currency) at or above which
# streaming/jobs/whale_detector.py flags a trade. DSVIEW_WHALE_THRESHOLD_USD
WHALE_THRESHOLD_USD: Decimal = Decimal(_env("WHALE_THRESHOLD_USD", "100000"))

# Browser origins allowed to call the REST API: the Vite dev server for
# now. DSVIEW_CORS_ALLOWED_ORIGINS="https://a.example,https://b.example"
CORS_ALLOWED_ORIGINS: List[str] = _env_list("CORS_ALLOWED_ORIGINS", ["http://localhost:5173"])


# --- Pipeline contract (not overridable) -------------------------------------

# ingestion/schema.py's kafka_topic_for() maps each event type to one of
# these; streaming/ consumers subscribe to them by the same names.
TRADES_TOPIC = "market.trades.raw"
DEPTH_TOPIC = "market.depth.raw"

# streaming/jobs/whale_detector.py publishes each flagged trade here (see its
# docstring for the message shape); the Telegram bot and the backend's
# /ws/whales are meant to consume it.
WHALE_ALERTS_TOPIC = "alerts.whales"
