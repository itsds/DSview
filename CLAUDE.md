# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

DSView — a real-time, multi-asset trading data pipeline (Kafka, Spark Structured Streaming, FastAPI) with custom volume footprint, whale tracking, and technical indicators.

`ingestion/`, `streaming/`, `storage/`, `backend/`, and `frontend/` all have real implementation: live Binance trades → Redpanda → Spark → Redis (live state) + Postgres/Parquet (history) → FastAPI (WebSocket + REST) → a `lightweight-charts` chart that backfills history on load, then live-updates. Not built yet: technical indicators in Spark (the chart computes them in the browser for now — see `frontend/src/indicators/`), volume footprint, alerts, and the other planned top-level folders (`config/`, `tests/`, `data_quality/`, `observability/`).

`docs/architecture.html` maps the whole pipeline: ten left-to-right stages, four lanes (hot path, Medallion lakehouse, whale alerts, order book), every component and every data movement, each marked built / roadmap / suggested, plus a table of all 66 movements. One self-contained file — open it in a browser (it sketches with rough.js from a CDN and falls back to plain boxes offline). When a stage lands, change that node's `status` in the file's `NODES` array rather than redrawing anything.

## Setup / running

```bash
pip install -r ingestion/requirements.txt
pip install -r streaming/requirements.txt   # pyspark + redis + psycopg; pyspark also requires a JDK (17 tested) on PATH
pip install -r backend/requirements.txt     # fastapi, uvicorn, redis, psycopg
pip install -r storage/requirements.txt     # psycopg, for init_db.py
cd frontend && npm install
```

**One command for everything below** — infra up, readiness waits, `init_db.py`, then every service in its own window of a `dsview` tmux session (switch windows with `Ctrl+b` + number, detach with `Ctrl+b d`):

```bash
./scripts/dev.sh        # start (or re-attach if already running)
./scripts/dev.sh stop   # Ctrl+C every service, then `docker compose stop`
```

The individual commands, for running a single service by hand:

Local infra (Redpanda + its web Console at `http://localhost:8080`, Redis + RedisInsight at `http://localhost:5540` — add a connection there pointing at host `redis`, port `6379` — and Postgres):

```bash
docker compose up -d
python storage/init_db.py   # one-time: creates the candles_1m table (see storage/ddl/)
```

Run the ingestion service (requires the Redpanda broker above, reachable at `localhost:9092`):

```bash
cd ingestion && python main.py
```

Run the whale detector (plain asyncio consumer, no Spark):

```bash
cd streaming && python -m jobs.whale_detector
```

Run the candle aggregator (Spark Structured Streaming; prints OHLCV candles to the console and writes the latest candle per symbol to Redis + Postgres):

```bash
cd streaming && python -m jobs.candle_aggregator
```

Run the storage writer (Spark Structured Streaming; Bronze/Silver/Gold Parquet under `storage/data/`, gitignored):

```bash
cd streaming && python -m jobs.storage_writer
```

Run the backend (WebSocket live push at `/ws/candles/{symbol}`, REST history at `/candles/{symbol}`):

```bash
cd backend && uvicorn main:app --reload --port 8000
```

Run the frontend dev server (Vite, hot-reloading):

```bash
cd frontend && npm run dev
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
- **`trades_source.py`** (`read_trades_stream()`) and **`candle_windowing.py`** (`build_candles()`) are the shared Kafka-read/parse and tumbling-window-aggregation steps, factored out so `jobs/candle_aggregator.py` and `jobs/storage_writer.py` can't drift against each other — both consume `market.trades.raw` and compute the exact same OHLCV logic, just with different output modes and sinks (see below). `schemas.py` declares the raw JSON wire shape as a Spark `StructType` (all `StringType`, cast to real types after `from_json`) rather than importing `ingestion/schema.py`'s pydantic models — same reasoning `whale_detector.py` documents: once an event is on the wire it's just JSON, so a consumer shouldn't share Python types with the producer across the topic boundary. `spark_session.py` centralizes the SparkSession config (including the `spark.jars.packages` coordinate for the Kafka connector, version-pinned to the installed pyspark) so every job shares one config instead of drifting.
- **`jobs/candle_aggregator.py`** runs the windowed aggregation in `outputMode("update")` for **live serving**: writes console + `redis_sink.py` + `postgres_sink.py` inside one `foreachBatch` (Structured Streaming has no built-in Redis/Postgres sink, so one streaming query does all three rather than three separate queries against Kafka).
  - **`redis_sink.py`** (`CandleRedisSink`) — analogous to `producer.py`'s role for Kafka: nothing else touches `redis` directly. Every write goes to two places: a TTL'd SET (`candle:{symbol}:{timeframe}`) so a newly-connected client can fetch current state immediately, and a PUBLISH (`candle_updates:{symbol}:{timeframe}`) so already-connected clients get pushed each update.
  - **`postgres_sink.py`** (`CandlePostgresSink`) — upserts into Postgres's `candles_1m` table (`ON CONFLICT (symbol, window_start) DO UPDATE`) on every batch. Upserting repeatedly rather than writing once-on-close is deliberate: update mode never signals "this window is finalized," but repeated upserts converge to the correct final row once the watermark stops emitting updates for that window — no separate close-detection needed.
  - **Timestamps**: PySpark's `collect()` returns `TimestampType` as a *naive* datetime in the driver's local timezone (IST on the dev laptop), not UTC. `_row_to_candle()` converts `window_start`/`window_end` to aware UTC before either sink sees them — any future `foreachBatch` job writing timestamps out must do the same. Skipping it once left Postgres storing every candle 5h30m in the future and the chart's backfilled history misaligned with its live updates.
- **`jobs/storage_writer.py`** runs three *separate* streaming queries in one Spark application (one JVM, one Kafka-connector resolution, instead of three) for the Bronze/Silver/Gold Parquet layers under `storage/data/` (gitignored):
  - **Bronze** (`storage/data/bronze/trades/`) — `market.trades.raw` written append-only, exactly as `trades_source.py` parses it. The durable replay/backtest source (feature roadmap item 4).
  - **Silver** (`storage/data/silver/trades/`) — Bronze deduplicated by `trade_id` within a watermark window. Ingestion's pydantic validation already guarantees well-formed data, so dedup + a compacted layout is Silver's whole job for now.
  - **Gold** (`storage/data/gold/candles_1m/`) — the *same* `candle_windowing.build_candles()` aggregation as `candle_aggregator.py`, but in `outputMode("append")` instead of `"update"`. Append mode only emits a window once its watermark has passed — exactly once per finalized candle — which is what makes it safe to write to an append-only Parquet sink; writing `candle_aggregator.py`'s update-mode output to Parquet instead would pile up duplicate, progressively-refined rows per window. This is why Gold can't just consume `candle_aggregator.py`'s stream and needs its own query built from the shared aggregation function instead.
- `storage/ddl/gold_candles.sql` defines the `candles_1m` Postgres table `postgres_sink.py` upserts into; apply it once via `storage/init_db.py` (see Setup above). Not using a migration tool yet — revisit once there's more than one table.

`backend/` is a FastAPI service with both a live push and a historical-read side:

- **`redis_client.py`** (`CandleRedisReader`) is the read-side counterpart to `streaming/redis_sink.py` — the sole place `backend/` touches `redis`. `get_latest()` reads the cached SET (for a client's initial state); `subscribe_updates()` is an async generator over the PUBLISH channel.
- **`routers/ws.py`** exposes `/ws/candles/{symbol}`: sends the current cached candle on connect, then forwards every subsequent pub/sub update until the client disconnects.
- **`postgres_client.py`** (`CandleHistoryReader`) is the read-side counterpart to `streaming/postgres_sink.py` — the sole place `backend/` touches `psycopg`/Postgres. One connection per request for now (dev request volumes only; a pool is a documented future TODO, not built prematurely).
- **`routers/candles.py`** exposes `GET /candles/{symbol}?timeframe=1m|5m|15m|1h|4h|1D&limit=N&before=<ISO timestamp>`: candles oldest first (chart order); `before` pages backwards for the chart's scroll-back. Only `candles_1m` is stored — bigger timeframes are rolled up at query time by `postgres_client.py`'s `date_bin()` query (its comment explains why capping the scan at `limit × minutes-per-bucket` rows never returns a truncated bucket). Buckets are epoch-aligned, the same alignment `frontend/src/lib/timeframes.js` uses to roll live candles up; the two `TIMEFRAMES` lists must match. This is a genuinely different data path from `ws.py` — it never touches Redis, which has no history to serve.
- **`main.py`** wires up both routers, plus `CORSMiddleware` (allowing `http://localhost:5173`, the Vite dev server) — REST, unlike WebSocket, is subject to browser CORS once frontend and backend are different origins/ports.

`frontend/` is a Vite + React app (plain JS, `.jsx`, no TypeScript):

- **`api/candles.js`** (`fetchCandles`) is the one client for `GET /candles/{symbol}`; **`lib/timeframes.js`** defines the timeframes and the epoch-aligned bucket math shared by history and live roll-ups.
- **`hooks/useCandleHistory.js`** fetches the newest page for the current symbol + timeframe — and returns an empty list until it arrives, never the previous timeframe's bars. Older pages are fetched by `CandleChart.jsx` itself on scroll-back.
- **`hooks/useLiveCandles.js`** owns the WebSocket connection to `/ws/candles/{symbol}` and exposes the latest parsed 1m candle as React state. No reconnect/backoff logic yet.
- **`hooks/useTimeframeCandle.js`** rolls that 1m stream up into the live candle for the selected timeframe (the backend only streams 1m). It keeps the current bucket's 1m candles in a Map and re-aggregates on every update — the same 1m candle is re-sent many times while it forms, so running totals would double-count — and seeds the Map over REST first, so a bucket already in progress at page load isn't missing its earlier minutes.
- **`components/chart/CandleChart.jsx`** wraps `lightweight-charts` v5 (TradingView's open-source rendering library — unrelated to any "TradingView API," see the Data sources section below; v5's API is `chart.addSeries(CandlestickSeries)`, not v4's `addCandlestickSeries()`) and takes both `history` and `candle` props: `history` seeds the chart once via `setData()` (standard lightweight-charts backfill pattern), then `candle` keeps it live via `series.update()`. `update()` specifically — not `setData()` again — for the live path: the backend re-sends the *same* `window_start` repeatedly while a candle is still forming, and `update()` replaces a bar with a matching timestamp instead of duplicating it, while a new timestamp appends a new bar — exactly matching how `candle_aggregator.py` emits data. `update()` throws on a bar *older* than the last one, so live candles older than the last bar (late-trade refinements of an already-superseded window) are dropped rather than passed through. Scrolling to near the oldest loaded bar fetches the page before it and prepends it, merged with `series.data()` rather than the original history array so bars appended live since load survive. `App.jsx` keys the chart by symbol + timeframe, so a switch mounts a fresh chart instead of resetting refs by hand.
- **`components/chart/chartTheme.js`** reads `styles/theme.css`'s CSS variables into chart/series options — the chart draws on a canvas, which can't use CSS variables, so this keeps `theme.css` the only place colours are defined. Keep `attributionLogo` on: lightweight-charts' license requires a tradingview.com link on the page, and that option provides it.
- **`components/chart/Legend.jsx`** is the top-left O/H/L/C readout, driven by the chart's `subscribeCrosshairMove` (latest bar when the crosshair is off the chart).
- **`drawing/`** is the drawing tools. `DrawingLayer.js` is a single lightweight-charts series primitive that renders every drawing (plus axis labels and the hover cursor); `DrawingController.js` handles interaction — placing via `subscribeClick`/`subscribeCrosshairMove`, selecting/dragging via a capture-phase `mousedown` on the chart's wrapper that stops the press before lightweight-charts (which listens for mouse events, not pointer events) can pan, and Esc/Delete — skipping any key `lib/keyboard.js`'s `isChartKey()` rejects: typed into a field or dialog, or already handled (`preventDefault()`) by something else, which is how an open toolbar menu keeps its Esc from also leaving the tool. `tools.js` holds each tool's anchor count, draw and hit-test, all in pixels. Rays and extended lines store only their two anchors and are run to the pane's edge afresh on every draw; the trend-angle and info-line angles are on-screen angles, so they change with zoom (as TradingView's do). The `measure` key is the toolbar's "Date and price range" — renaming a tool's key orphans drawings already saved under it. A tool can also define `create` (sizes itself from one click), `moveHandle` (constrained or coupled handles), `handles` (where its handles sit) and a function `priceAxisLabel` — see `tools.js`'s header. A long/short position is four points kept in step (entry, target, stop, end), so dragging the whole drawing needs no special case; its target and stop move vertically only and never cross the entry. `barData.js` gives tools the candles plus volume (`bars.between(from, to)`, read fresh on every call, so scroll-back pages and the live bar are included). Anchored VWAP sums from the bar its anchor falls in, on any timeframe, sharing its running sum with the VWAP indicator (`indicators/math.js`), and draws nothing while that bar isn't loaded rather than a wrong average from a later one. Drawings are stored per symbol in `localStorage` (`drawingStore.js`) as time/price, never pixels; `coordinates.js` places a time relative to the bar `timeToIndex(time, true)` returns (`index + (time − barTime) / barSeconds`), so a drawing made on one timeframe lands correctly on every other. `App.jsx` owns the active tool and magnet state; the controller resets the tool to the crosshair itself after each drawing, not just via React, so a click arriving before React re-renders isn't handled with the old tool.
- **`indicators/`** is the indicators: SMA, EMA, Bollinger Bands and VWAP over the candles; RSI and MACD each in their own pane. `math.js` holds the formulas as pure functions; `definitions.js` describes each indicator (params, pane, lines) and its `compute()` — the one seam to swap for Spark's output when roadmap item 2 lands. `IndicatorManager.js` (created by `CandleChart.jsx`, like `DrawingController.js`) owns the indicator series and oscillator panes, rebuilds them all on any list change rather than diffing, and recomputes from `candleSeries.data()` on every data change — `setData()` after history loads or a scroll-back page is prepended, `update()` of the last point on live ticks. `hooks/useIndicators.js` persists the list to `localStorage`; each instance keeps the colour slot it got when added, so removing one never repaints the others. Colours are three validated hues (`--color-series-1..3` in `theme.css`, avoiding the hues that already mean up / down / drawings), then the same three dashed — so over-the-candles indicators cap at six. Legend rows (`components/chart/IndicatorLegend.jsx`) keep values in text colours with a coloured swatch beside each; an oscillator pane's legend is positioned from `pane.getHTMLElement()`.
- **`components/layout/`** holds the workspace around the chart — `TopBar`, `DrawingToolbar`, `RightPanel`, `BottomBar` — placed by `App.jsx`'s CSS grid. Controls for features that don't exist yet are rendered anyway through `ToolbarButton`'s `notBuilt` prop (dimmed, inert, tooltip says "not built yet"); when a feature lands, drop `notBuilt` and wire `onClick` rather than adding a new button. `DrawingToolbar` groups tools TradingView-style: `drawingToolGroups.js` is the whole layout (groups → titled sections → tools, each with its icon and Alt-key shortcut; planned tools are `notBuilt` entries, same convention — when one lands, add it to `tools.js`'s `TOOLS` and drop the flag). `ToolGroupButton` shows each group's last-used tool (persisted by `hooks/useLastUsedTools.js`) with an arrow that opens `ToolMenu`, which is portalled to `<body>` because the toolbar scrolls and would clip it. Icons lucide lacks are drawn in `toolIcons.js` with lucide's `createLucideIcon` — never copy TradingView's artwork.
- **`components/ErrorBoundary.jsx`** wraps the chart, so a chart error shows a retry button instead of React unmounting the whole app to a blank page.
- Styling is CSS Modules (`*.module.css`, one per component) on top of the design tokens in `styles/theme.css`; icons come from `lucide-react`.
- **`App.jsx`** holds the UI state — timeframe, drawing tool, magnet, the indicator list (via `useIndicators`) and which indicator dialog is open — and hardcodes `SYMBOL = "BTCUSDT"` — symbol selection is future work, not implemented.

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
