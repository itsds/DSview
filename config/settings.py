"""
settings.py

DSView's one config module. Every service (ingestion/, streaming/,
backend/, storage/, alerts/) reads its broker address, Redis and Postgres
locations, data paths, symbols, topic names and whale threshold from
here instead of keeping its own copy.

Two kinds of value live here:

- **Deployment settings**: where things run and what to ingest. Each can
  be overridden with a DSVIEW_* environment variable (named next to it
  below), so running a service in a container later, where Redis is
  `redis:6379` rather than `localhost:6379`, or pointing it at the cloud
  needs an environment variable, not a code edit. The defaults match
  docker-compose.yml, so local dev needs no variables at all.
- **Pipeline contract**: the Kafka topic names, the chart timeframes and
  the EMA lengths Spark precomputes. A producer and its consumers must
  agree on them, so they are deliberately not overridable from the
  environment — renaming a topic or adding a timeframe is a code change,
  made here once.

What stays out: constants that tune one module and carry their own
explanation there — the Redis key TTL (streaming/redis_sink.py), the
dedup watermark (streaming/jobs/storage_writer.py), the REST page limits
(backend/routers/candles.py), the backfill's default depths and request
budget (backfill/). Moving those here would split each value from the
comment that justifies it.

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
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

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

# Which pipeline this process belongs to: "live" (the default, fed from
# Binance by ingestion/) or "replay" (fed from Bronze's history by replay/,
# Phase 4a). Everything a pipeline owns follows from its name, so the same
# jobs run in either one without touching the other's data: see Pipeline
# below. DSVIEW_PIPELINE=replay
PIPELINE_NAME: str = _env("PIPELINE", "live")

# Local-dev credentials from docker-compose.yml — never reuse them anywhere
# real; set DSVIEW_POSTGRES_DSN instead. The database every pipeline keeps
# its schema in: use POSTGRES_DSN (below), which is this pipeline's.
_DATABASE_DSN: str = _env("POSTGRES_DSN", "postgresql://dsview:dsview@localhost:5432/dsview")

# Root of the Bronze/Silver/Gold Parquet layers (gitignored), the live
# pipeline's; another pipeline's are under it, in a folder of its name.
# Absolute, so it doesn't depend on which folder a job is started from. Use
# DATA_ROOT (below), which is this pipeline's. DSVIEW_DATA_ROOT
_LAKE_ROOT: str = _env("DATA_ROOT", str(REPO_ROOT / "storage" / "data"))

# Notional (price x quantity, in the quote currency) at or above which
# streaming/jobs/whale_detector.py flags a trade. DSVIEW_WHALE_THRESHOLD_USD
WHALE_THRESHOLD_USD: Decimal = Decimal(_env("WHALE_THRESHOLD_USD", "100000"))

# Browser origins allowed to call the REST API: the Vite dev server for
# now. DSVIEW_CORS_ALLOWED_ORIGINS="https://a.example,https://b.example"
CORS_ALLOWED_ORIGINS: List[str] = _env_list("CORS_ALLOWED_ORIGINS", ["http://localhost:5173"])

# alerts/telegram_bot.py: the bot's token (from @BotFather) and the chat it
# posts to. Secrets, so no defaults: set them in the environment, or in the
# repo root's gitignored .env, which scripts/dev.sh loads (see .env.example).
# DSVIEW_TELEGRAM_BOT_TOKEN, DSVIEW_TELEGRAM_CHAT_ID
TELEGRAM_BOT_TOKEN: str = _env("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = _env("TELEGRAM_CHAT_ID", "")

# The Bot API's base URL. Only the tests point it elsewhere (at a fake
# server). DSVIEW_TELEGRAM_API_URL
TELEGRAM_API_URL: str = _env("TELEGRAM_API_URL", "https://api.telegram.org")

# Binance's spot REST API, which backfill/ pages history from (klines and
# aggTrades; public market data, no key). The tests point it at a fake
# server. DSVIEW_BINANCE_REST_URL
BINANCE_REST_URL: str = _env("BINANCE_REST_URL", "https://api.binance.com")


# --- Pipeline contract (not overridable) -------------------------------------

# The live pipeline's topic names; another pipeline's are the same with its
# name in front (replay.market.trades.raw). Use TRADES_TOPIC & co. (below),
# which are this pipeline's.
#
# ingestion/schema.py's kafka_topic_for() maps each event type to one of the
# first two; streaming/ consumers subscribe to them by the same names.
_TRADES_TOPIC = "market.trades.raw"
_DEPTH_TOPIC = "market.depth.raw"

# streaming/jobs/whale_detector.py publishes each flagged trade here (see its
# docstring for the message shape); backend/routers/whales.py serves it
# over /ws/whales, and alerts/telegram_bot.py sends it to Telegram.
_WHALE_ALERTS_TOPIC = "alerts.whales"

# streaming/jobs/candle_aggregator.py publishes every 1-minute candle update
# here, keyed by symbol; streaming/jobs/indicators.py computes indicators
# from it. streaming/candle_topic.py owns the message shape.
_CANDLES_TOPIC = "market.candles.1m"

LIVE = "live"
REPLAY = "replay"
PIPELINE_NAMES: Tuple[str, ...] = (LIVE, REPLAY)

# The replay's controls (Phase 4a): backend/routers/replay.py pushes commands
# onto the list, replay/replay_service.py pops them and keeps its status in
# the key. Outside every pipeline's Redis prefix, so clearing the replay
# pipeline's keys before a run never takes its controls with them.
REPLAY_COMMANDS_KEY = "replay_control:commands"
REPLAY_STATUS_KEY = "replay_control:status"


@dataclass(frozen=True)
class Pipeline:
    """
    Where one pipeline keeps what it owns: Kafka topics and consumer groups,
    a Redis key prefix, a Postgres schema, and a Parquet root (with its
    checkpoints).

    The live pipeline's are the names DSView always had: no prefix, the
    `public` schema, DATA_ROOT itself. Replay's (Phase 4a's kickoff decision
    1, docs/ROADMAP.md) are the same names prefixed with "replay", so the
    same jobs, started with DSVIEW_PIPELINE=replay, read and write only
    replay's data. It can't share live's topics: a Spark query's watermark
    is one value for every symbol it reads, so behind live trades every
    replayed trade would be dropped as late.
    """

    name: str

    def __post_init__(self) -> None:
        if self.name not in PIPELINE_NAMES:
            raise ValueError(f"unknown pipeline {self.name!r}: DSVIEW_PIPELINE must be one of {list(PIPELINE_NAMES)}")

    @property
    def is_live(self) -> bool:
        return self.name == LIVE

    def _prefixed(self, live_name: str) -> str:
        return live_name if self.is_live else f"{self.name}.{live_name}"

    @property
    def trades_topic(self) -> str:
        return self._prefixed(_TRADES_TOPIC)

    @property
    def depth_topic(self) -> str:
        return self._prefixed(_DEPTH_TOPIC)

    @property
    def whale_alerts_topic(self) -> str:
        return self._prefixed(_WHALE_ALERTS_TOPIC)

    @property
    def candles_topic(self) -> str:
        return self._prefixed(_CANDLES_TOPIC)

    def consumer_group(self, live_group: str) -> str:
        """This pipeline's name for a consumer group live calls `live_group`."""
        return self._prefixed(live_group)

    @property
    def starting_offsets(self) -> str:
        """
        Where a consumer with nothing to resume from starts reading.

        Live starts at the end: it serves what happens from now on. Replay
        starts at the beginning, which is the start of the current run (the
        replay service truncates its topics before each one), so trades
        produced while a Spark job is still starting are never skipped.
        """
        return "latest" if self.is_live else "earliest"

    @property
    def redis_prefix(self) -> str:
        """Put in front of every key and channel this pipeline writes ("" for live)."""
        return "" if self.is_live else f"{self.name}:"

    @property
    def postgres_schema(self) -> str:
        return "public" if self.is_live else self.name

    @property
    def postgres_dsn(self) -> str:
        """
        The database's DSN, with this pipeline's schema as the only one on
        the search path (live's is the DSN as given: `public`, as always).

        The only one, not first of several: a table replay lacks must fail,
        not quietly resolve to live's.
        """
        return _DATABASE_DSN if self.is_live else _with_search_path(_DATABASE_DSN, self.postgres_schema)

    @property
    def data_root(self) -> str:
        return _LAKE_ROOT if self.is_live else str(Path(_LAKE_ROOT) / self.name)


def _with_search_path(dsn: str, schema: str) -> str:
    """`dsn` (a URL or key=value conninfo) with libpq's options set to `-c search_path=<schema>`."""
    option = f"-c search_path={schema}"
    if "://" in dsn:
        parts = urlsplit(dsn)
        query = parse_qsl(parts.query, keep_blank_values=True)
        if any(key == "options" for key, _ in query):
            raise ValueError("DSVIEW_POSTGRES_DSN already sets `options`; a pipeline's DSN needs to set its own")
        query.append(("options", option))
        return urlunsplit(parts._replace(query=urlencode(query, quote_via=quote)))
    if "options=" in dsn:
        raise ValueError("DSVIEW_POSTGRES_DSN already sets `options`; a pipeline's DSN needs to set its own")
    return f"{dsn} options='{option}'"


LIVE_PIPELINE = Pipeline(LIVE)
REPLAY_PIPELINE = Pipeline(REPLAY)

# This process's pipeline, and its names: what every service uses unless it
# names a pipeline itself (the backend serves both; replay/ reads live's
# Bronze; the Telegram bot only ever reads live's whales).
PIPELINE = Pipeline(PIPELINE_NAME)
TRADES_TOPIC = PIPELINE.trades_topic
DEPTH_TOPIC = PIPELINE.depth_topic
WHALE_ALERTS_TOPIC = PIPELINE.whale_alerts_topic
CANDLES_TOPIC = PIPELINE.candles_topic
REDIS_PREFIX = PIPELINE.redis_prefix
POSTGRES_DSN = PIPELINE.postgres_dsn
DATA_ROOT = PIPELINE.data_root

# The chart's timeframes, by the ids the REST API, Redis keys and Postgres
# rows use. Every one is rolled up from 1-minute candles in buckets aligned
# to the Unix epoch, by backend/postgres_client.py (history) and
# streaming/indicator_engine.py (indicators) alike. Must match
# frontend/src/lib/timeframes.js's TIMEFRAMES.
TIMEFRAMES: Dict[str, timedelta] = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1D": timedelta(days=1),
}

# The EMA lengths streaming/jobs/indicators.py precomputes on every timeframe
# (Phase 2's kickoff decision; docs/ROADMAP.md). An EMA at any other length
# is computed in the browser. Each is a column of storage/ddl/indicators.sql
# (ema_20, ...) and must match frontend/src/indicators/server.js.
SERVER_EMA_LENGTHS: Tuple[int, ...] = (20, 50, 200)

# The price step footprint_1m stores buy and sell volume at (Phase 3's
# kickoff decision 3): a trade's volume counts towards the level
# floor(price / step) * step, so with $1 a trade at 97,000.40 lands on the
# 97,000 row. streaming/footprint_windowing.py (live) and
# backfill/footprint_rows.py (history) both bucket by it; the backend merges
# rows to multiples of it ($5, $10, ...) for display. Changing it means
# emptying footprint_1m and backfilling again. Must match
# frontend/src/footprint/levels.js.
FOOTPRINT_PRICE_STEP: Decimal = Decimal("1")

# The chart timeframes the footprint is served on (decision 4): 4h and 1D
# stay plain candles. The volume profile tools work on every timeframe.
# Must match frontend/src/footprint/levels.js.
FOOTPRINT_TIMEFRAMES: Tuple[str, ...] = ("1m", "5m", "15m", "1h")
