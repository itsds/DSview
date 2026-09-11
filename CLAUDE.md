# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

DSView — a real-time, multi-asset trading data pipeline (Kafka, Spark Structured Streaming, FastAPI) with custom volume footprint, whale tracking, and technical indicators.

`ingestion/` and `streaming/` have real implementation. `storage/` is still an empty stub package (`__init__.py` only) — nothing downstream of Redpanda/Spark persists anywhere yet.

## Setup / running

```bash
pip install -r ingestion/requirements.txt
pip install -r streaming/requirements.txt   # pyspark; also requires a JDK (17 tested) on PATH
```

Local infra (Redpanda + its web Console, browsable at `http://localhost:8080`):

```bash
docker compose up -d
```

Run the ingestion service (requires the Redpanda broker above, reachable at `localhost:9092`):

```bash
cd ingestion && python main.py
```

Run the whale detector (plain asyncio consumer, no Spark):

```bash
cd streaming && python -m jobs.whale_detector
```

Run the candle aggregator (Spark Structured Streaming; prints OHLCV candles to the console for now — no storage/backend/frontend wiring yet):

```bash
cd streaming && python -m jobs.candle_aggregator
```

There is no test suite, linter, or build step configured yet anywhere in the repo.

## Architecture

The ingestion layer follows a deliberate SRP/OCP/DIP split — read `ingestion/base_client.py`'s and `ingestion/producer.py`'s module docstrings before changing the wiring, they explain the reasoning:

- **`schema.py`** is the canonical contract. Every exchange adapter normalizes its wire format into `TradeEvent` / `DepthUpdate` (union type `MarketEvent`) before anything else sees the data. `kafka_topic_for()` is the single place that maps an event type to a Kafka topic — add new topics here, not per-caller.
- **`base_client.py`** defines `BaseExchangeClient`, an abstract WebSocket client that owns connection lifecycle and exponential-backoff reconnect. It knows nothing about Kafka or any specific exchange. Subclasses implement only `ws_url`, `build_subscription_message()`, and `parse_message()`. It depends on an injected `on_event` async callback rather than importing the producer directly — this is what keeps ingestion testable without a broker.
- **`binance_client.py`** is the reference implementation of that contract (first and currently only exchange adapter). `Exchange` enum in `schema.py` already anticipates Coinbase and Kraken — new exchanges should be added the same way: a new subclass of `BaseExchangeClient`, no changes needed to `base_client.py`.
- **`producer.py`** (`MarketDataProducer`) is the sole boundary between ingestion and Kafka — a thin async wrapper over `AIOKafkaProducer`. Exchange clients never touch it directly; they only ever call the `on_event` callback they were constructed with. Partitions by symbol (`event.symbol` as the Kafka key) to preserve per-instrument ordering for downstream windowed aggregations in Spark.
- **`main.py`** is the only file that imports both a concrete exchange client and the producer, wiring them together and running clients concurrently under asyncio with graceful SIGINT/SIGTERM shutdown. `SYMBOLS` and `KAFKA_BOOTSTRAP_SERVERS` are hardcoded here with a `TODO(@DS)` to move them into a `config/settings.py` module once it exists — check whether that module has been scaffolded before adding more hardcoded config here.

When extending ingestion: normalize to `schema.py` types, keep new exchange adapters as `BaseExchangeClient` subclasses in their own file, and don't let Kafka-specific concerns leak upstream of `producer.py`.

`streaming/` consumes what `ingestion/` publishes, and splits per-event vs. windowed work into two different execution models rather than forcing everything through Spark:

- **`jobs/whale_detector.py`** is a plain asyncio `aiokafka` consumer — flagging one trade against a size threshold needs no windowing or cross-event state, so Spark's startup/resource cost would be pure overhead here.
- **`jobs/candle_aggregator.py`** is a Spark Structured Streaming job — OHLCV needs aggregation across many trades within a time window, which is what Spark earns its keep on. `spark_session.py` centralizes the SparkSession config (including the `spark.jars.packages` coordinate for the Kafka connector, version-pinned to the installed pyspark) so every windowed job shares one config instead of drifting. `schemas.py` declares the raw JSON wire shape as a Spark `StructType` (all `StringType`, cast to real types after `from_json`) rather than importing `ingestion/schema.py`'s pydantic models — same reasoning `whale_detector.py` documents: once an event is on the wire it's just JSON, so a consumer shouldn't share Python types with the producer across the topic boundary.
- Both jobs read `market.trades.raw` directly; neither imports the other or `ingestion/`.
- `candle_aggregator.py` currently only writes to the console sink for verification — Redis/Postgres/backend/frontend wiring is deliberately not built yet (see the Deployment approach section below: verify each layer before adding the next).

# DSView — Project Context

This section captures design decisions made before any code existed, so they don't
need to be re-derived or re-explained. `/init` can infer build commands and file
layout from the code itself — it can't infer *why* these choices were made, so keep
this section even as the codebase grows.

## What this project is

A real-time, multi-exchange crypto market data pipeline (starting with BTC-USD,
designed to extend to gold/XAU-USD later) that replicates TradingView's premium
features from scratch: volume footprint, whale tracking, and technical indicators.
Two goals: close a Kafka / Spark Structured Streaming skill gap, and produce
something genuinely useful for trading decisions.

## Data sources

- **Primary sources**: Binance, Coinbase Advanced Trade WebSocket, Kraken WebSocket v2
  — all exchange-native, real trade-level data (price, quantity, buy/sell side) plus
  order book depth. Build fully end-to-end on Binance first, then add Coinbase and
  Kraken as additional adapters behind the same interface.
- **Not using CoinGecko as a primary source** — it's an aggregator (blended price
  across exchanges, rate-limited REST/WS, no per-trade side or real order book), which
  makes it unusable for footprint or whale detection. May add it later purely as a
  slow-cadence source for market-wide context (e.g. BTC dominance).
- **No official TradingView data API exists.** Do not integrate any third-party site
  branded "TradingView API" — those are unofficial and not affiliated with TradingView.

## Feature roadmap (build in this order)

1. **Whale tracking** — per-transaction size threshold filter, no windowing needed.
   Simplest, first end-to-end milestone.
2. **Technical indicators** — EMA/RSI on tumbling windows via Spark Structured Streaming.
3. **Volume footprint** — price-bucketed buy/sell volume within a candle. Hardest;
   needs careful price-bucket granularity choices.
4. **Later**: backtest, replay mode (reuse the live pipeline, feed it historical data
   at controlled speed), long/short ratio (needs Binance **Futures** API, not spot).
5. **Stretch goal**: order book heatmap / depth imbalance (needs order book state
   reconstruction, not just trades).
6. **Future phase, not yet planned in detail**: an AI layer using open-source Hugging
   Face models — FinBERT/CryptoBERT for sentiment, a Kronos-style time-series model
   for candlestick forecasting.

## Architecture

| Stage | Tech | Notes |
|---|---|---|
| Ingestion | Python WebSocket clients per exchange | Normalize into one canonical trade/depth schema before it hits Kafka |
| Message bus | Redpanda (Kafka-API-compatible) | Chosen over vanilla Kafka to save laptop resources during local dev — same client code either way |
| Stream processing (windowed) | Spark Structured Streaming, local mode | Indicators + footprint. Not containerized — run directly for faster iteration |
| Stream processing (per-event) | Plain Python consumer | Whale trigger — deliberately *not* Spark, no windowing needed |
| Storage | Bronze/Silver/Gold Iceberg or Parquet tables | Bronze = raw immutable, doubles as replay/backtest source |
| Serving | Redis | Latest real-time state (current candle, footprint grid) |
| Historical | Postgres (ClickHouse later if needed) | Queryable history for backtesting/UI load |
| Backend | FastAPI | REST for historical candles, WebSocket for live push from Redis pub/sub |
| Frontend | React + `lightweight-charts` (TradingView's open-source rendering library) | Candlestick/volume built in; footprint needs a custom panel |
| Alerts | Kafka topic → Telegram bot | Whale/anomaly notifications |
| Data quality | Great Expectations | Same tool used on an earlier project (Travel Tag pipeline) |
| Observability | Prometheus + Grafana | Add once core pipeline is stable, not needed day one |

## Deployment approach

- Get ingestion → Redpanda → Spark → Redis working directly on the host first —
  don't containerize prematurely, it slows down debugging early on.
- Once that chain works, wrap the stateful services (Redpanda, Redis, Postgres) into
  a single `docker-compose.yml`.
- Containerize FastAPI and the frontend later, once stable.
- Start entirely local (laptop). Move to cloud (AWS: MSK/EMR/ElastiCache/S3) later,
  incrementally — not decided in detail yet.

## Repo & code conventions

- **Flat Python project layout** at the repo root: `ingestion/`, `streaming/`,
  `backend/`, `alerts/`, `config/`, `tests/`, `scripts/`. Deliberately **not** using a
  Java-style wrapping root package (e.g. `src/dsview/`) — that convention is for
  publishable libraries, not a single standalone application repo like this one.
- `frontend/` (React) is a separate top-level folder, not part of the Python package
  tree.
- Working in PyCharm.
- Kafka/Redis/Postgres/table naming should be **symbol-based** (`trades.BTCUSDT`,
  `trades.XAUUSD`), not crypto-specific — the gold (XAU-USD) extension is planned to
  live in this same repo, reusing every downstream stage. Only the ingestion adapter
  and a per-asset feature flag (footprint/whale off for gold — spot gold's OTC market
  structure doesn't expose real trade-side volume) differ per asset.

## Naming

Project name: **DSView** (initials + "View," echoing TradingView).
