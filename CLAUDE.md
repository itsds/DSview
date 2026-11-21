# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

DSView — a real-time, multi-asset trading data pipeline (Kafka, Spark Structured Streaming, FastAPI) with custom volume footprint, whale tracking, and technical indicators.

`ingestion/`, `streaming/`, `storage/`, `backend/`, `alerts/` and `frontend/` all have real implementation: live Binance trades → Redpanda → Spark → Redis (live state) + Postgres/Parquet (history) → FastAPI (WebSocket + REST) → a `lightweight-charts` chart that backfills history on load, then live-updates. Whale alerts flow detector → `alerts.whales` → both `/ws/whales` (the right panel's whale list) and the Telegram bot (your phone). `tests/` holds a pytest suite over real Kafka, Redis and Postgres, the frontend has Vitest tests, and GitHub Actions runs both on every push. Not built yet: technical indicators in Spark (the chart computes them in the browser for now — see `frontend/src/indicators/`), volume footprint, and the other planned top-level folders (`data_quality/`, `observability/`).

`docs/architecture.html` maps the whole pipeline: ten left-to-right stages, four lanes (hot path, Medallion lakehouse, whale alerts, order book), every component and every data movement, each marked built / roadmap / suggested, plus a table of all 66 movements. One self-contained file — open it in a browser (it sketches with rough.js from a CDN and falls back to plain boxes offline). When a stage lands, change that node's `status` in the file's `NODES` array rather than redrawing anything.

## Setup / running

```bash
pip install -r ingestion/requirements.txt
pip install -r streaming/requirements.txt   # pyspark + aiokafka + redis + psycopg; pyspark also requires a JDK (17 tested) on PATH
pip install -r backend/requirements.txt     # fastapi, uvicorn, redis, psycopg, aiokafka
pip install -r storage/requirements.txt     # psycopg, for init_db.py
pip install -r alerts/requirements.txt      # aiokafka + httpx, for the Telegram bot
pip install -r tests/requirements.txt       # pytest & co., for scripts/test.sh
cd frontend && npm install
```

The Telegram bot needs a bot token and a chat id. Copy `.env.example` to `.env` in the repo root (gitignored) and fill in both — the file says how to get them. `dev.sh` loads `.env` into every window; by hand, `set -a; . ./.env; set +a` does the same. Never commit the token or paste it anywhere.

**One command for everything below** — infra up, readiness waits, `init_db.py`, then every service in its own window of a `dsview` tmux session, each window with `.env` loaded (switch windows with `Ctrl+b` + number, detach with `Ctrl+b d`):

```bash
./scripts/dev.sh        # start (or re-attach if already running)
./scripts/dev.sh stop   # Ctrl+C every service, then `docker compose stop`
```

The individual commands, for running a single service by hand. Every Python service imports `config/settings.py` from the repo root while running from inside its own folder, so first put the repo root on `PYTHONPATH`, once per shell (`dev.sh` does this itself, and PyCharm adds the project root by default):

```bash
export PYTHONPATH="$PWD"   # from the repo root
```

Local infra (Redpanda + its web Console at `http://localhost:8080`, Redis + RedisInsight at `http://localhost:5540` — add a connection there pointing at host `redis`, port `6379` — and Postgres):

```bash
docker compose up -d
python storage/init_db.py   # one-time: creates the candles_1m table (see storage/ddl/)
```

Run the ingestion service (requires the Redpanda broker above, reachable at `localhost:9092`):

```bash
cd ingestion && python main.py
```

Run the whale detector (plain asyncio consumer, no Spark; logs each whale trade and publishes it to `alerts.whales`):

```bash
cd streaming && python -m jobs.whale_detector
```

Run the Telegram bot (sends every whale on `alerts.whales` to your Telegram chat; needs `DSVIEW_TELEGRAM_BOT_TOKEN` and `DSVIEW_TELEGRAM_CHAT_ID`, see above):

```bash
cd alerts && python telegram_bot.py
```

To try it without messaging your phone, run `python tests/fake_telegram.py --port 8081` (prints each message) and start the bot with `DSVIEW_TELEGRAM_API_URL=http://127.0.0.1:8081 DSVIEW_TELEGRAM_BOT_TOKEN=test-token DSVIEW_TELEGRAM_CHAT_ID=42`.

Run the candle aggregator (Spark Structured Streaming; prints OHLCV candles to the console and writes the latest candle per symbol to Redis + Postgres):

```bash
cd streaming && python -m jobs.candle_aggregator
```

Run the storage writer (Spark Structured Streaming; Bronze/Silver/Gold Parquet under `storage/data/`, gitignored):

```bash
cd streaming && python -m jobs.storage_writer
```

Run the backend (WebSocket live push at `/ws/candles/{symbol}`, REST history at `/candles/{symbol}`, whale alerts at `/ws/whales`):

```bash
cd backend && uvicorn main:app --reload --port 8000
```

Run the frontend dev server (Vite, hot-reloading):

```bash
cd frontend && npm run dev
```

Run the whole test suite — throwaway Redpanda, Redis and Postgres in Docker (`tests/docker-compose.yml`, compose project `dsview-test`, ports 19092/16379/15432, so it runs alongside the dev stack), then pytest, then the frontend's Vitest tests:

```bash
./scripts/test.sh                  # everything (~2 minutes)
./scripts/test.sh -m "not spark"   # skip the Spark tests; any other pytest arguments work too
./scripts/test.sh --no-frontend    # Python tests only
./scripts/test.sh down             # remove the test services
cd frontend && npm test            # the Vitest tests alone, no services needed
```

`.github/workflows/tests.yml` runs `./scripts/test.sh --no-docker` on every push, with Kafka (`apache/kafka`, since a service container can't take Redpanda's start command), Redis and Postgres as service containers. There is no linter or build step in CI yet.

## Architecture

**`config/settings.py`** is the one config module every Python service imports (`from config.settings import ...`): `SYMBOLS`, the Kafka broker, Redis host/port, the Postgres DSN, the Parquet `DATA_ROOT`, the whale threshold, the CORS origins, and the Telegram bot's token, chat id and API URL (the token and chat have no defaults: they come from the environment or `.env`, never from git). Each can be overridden by a `DSVIEW_*` environment variable, and the defaults match `docker-compose.yml`, so local dev needs none. The Kafka topic names live there too but are deliberately not overridable — producer and consumers must agree on them. New deployment config (hosts, paths, credentials, thresholds) goes there, never into a module constant; a constant that tunes one module and is explained in place (the Redis key TTL, watermark delays, the REST page limits, the timeframe table) stays in its module. It's dependency-free on purpose: every service imports it, so it can't pull in a library one of them doesn't install.

The ingestion layer follows a deliberate SRP/OCP/DIP split — read `ingestion/base_client.py`'s and `ingestion/producer.py`'s module docstrings before changing the wiring, they explain the reasoning:

- **`schema.py`** is the canonical contract. Every exchange adapter normalizes its wire format into `TradeEvent` / `DepthUpdate` (union type `MarketEvent`) before anything else sees the data. `kafka_topic_for()` is the single place that maps an event type to a Kafka topic — add new topics here, not per-caller (the topic name itself goes in `config/settings.py`, because `streaming/` subscribes to the same names and must not import from `ingestion/`).
- **`base_client.py`** defines `BaseExchangeClient`, an abstract WebSocket client that owns connection lifecycle and exponential-backoff reconnect. It knows nothing about Kafka or any specific exchange. Subclasses implement only `ws_url`, `build_subscription_message()`, and `parse_message()`. It depends on an injected `on_event` async callback rather than importing the producer directly — this is what keeps ingestion testable without a broker.
- **`binance_client.py`** is the reference implementation of that contract (first and currently only exchange adapter). `Exchange` enum in `schema.py` already anticipates Coinbase and Kraken — new exchanges should be added the same way: a new subclass of `BaseExchangeClient`, no changes needed to `base_client.py`.
- **`producer.py`** (`MarketDataProducer`) is the sole boundary between ingestion and Kafka — a thin async wrapper over `AIOKafkaProducer`. Exchange clients never touch it directly; they only ever call the `on_event` callback they were constructed with. Partitions by symbol (`event.symbol` as the Kafka key) to preserve per-instrument ordering for downstream windowed aggregations in Spark.
- **`main.py`** is the only file that imports both a concrete exchange client and the producer, wiring them together and running clients concurrently under asyncio with graceful SIGINT/SIGTERM shutdown. It reads `SYMBOLS` and `KAFKA_BOOTSTRAP_SERVERS` from `config/settings.py`.

When extending ingestion: normalize to `schema.py` types, keep new exchange adapters as `BaseExchangeClient` subclasses in their own file, and don't let Kafka-specific concerns leak upstream of `producer.py`.

`streaming/` consumes what `ingestion/` publishes, and splits per-event vs. windowed work into two different execution models rather than forcing everything through Spark:

- **`jobs/whale_detector.py`** is a plain asyncio `aiokafka` consumer — flagging one trade against a size threshold needs no windowing or cross-event state, so Spark's startup/resource cost would be pure overhead here. Each trade at or over `WHALE_THRESHOLD_USD` is logged and handed to an injected `on_whale` callback (the same DIP as ingestion's `on_event`, so detection runs without a broker); `_main()` wires that callback to **`alert_publisher.py`** (`AlertPublisher`), the one place `streaming/` produces to Kafka, which publishes it to `alerts.whales` keyed by symbol. The alert's JSON shape is documented in the detector's docstring — it's the contract `/ws/whales` forwards as-is and the Telegram bot consumes. Delivery is best-effort (offsets auto-commit, so a crash can repeat an alert and a publish that fails after the producer's retries is logged and skipped); consumers dedupe on `(exchange, trade_id)`. Shutdown is bounded: `stop_within()` gives the consumer and then the publisher 5 s each to close, because with the broker down aiokafka's `consumer.stop()` either retries its final commit forever (Ctrl+C used to hang for good) or raises a `CancelledError` of its own making. It runs the close as a task of its own rather than under `asyncio.wait_for()`, so that leaked `CancelledError` reads as a failed close instead of cancelling the shutdown into a traceback; any new Kafka consumer's shutdown should go through it too.
- **`trades_source.py`** (`read_trades_stream()`) and **`candle_windowing.py`** (`build_candles()`) are the shared Kafka-read/parse and tumbling-window-aggregation steps, factored out so `jobs/candle_aggregator.py` and `jobs/storage_writer.py` can't drift against each other — both consume `market.trades.raw` and compute the exact same OHLCV logic, just with different output modes and sinks (see below). `schemas.py` declares the raw JSON wire shape as a Spark `StructType` (all `StringType`, cast to real types after `from_json`) rather than importing `ingestion/schema.py`'s pydantic models — same reasoning `whale_detector.py` documents: once an event is on the wire it's just JSON, so a consumer shouldn't share Python types with the producer across the topic boundary. `spark_session.py` centralizes the SparkSession config (including the `spark.jars.packages` coordinate for the Kafka connector, version-pinned to the installed pyspark) so every job shares one config instead of drifting.
- **`jobs/candle_aggregator.py`** runs the windowed aggregation in `outputMode("update")` for **live serving**: writes console + `redis_sink.py` + `postgres_sink.py` inside one `foreachBatch` (Structured Streaming has no built-in Redis/Postgres sink, so one streaming query does all three rather than three separate queries against Kafka).
  - **`redis_sink.py`** (`CandleRedisSink`) — analogous to `producer.py`'s role for Kafka: nothing else touches `redis` directly. Every write goes to two places: a TTL'd SET (`candle:{symbol}:{timeframe}`) so a newly-connected client can fetch current state immediately, and a PUBLISH (`candle_updates:{symbol}:{timeframe}`) so already-connected clients get pushed each update.
  - **`postgres_sink.py`** (`CandlePostgresSink`) — upserts into Postgres's `candles_1m` table (`ON CONFLICT (symbol, window_start) DO UPDATE`) on every batch. Upserting repeatedly rather than writing once-on-close is deliberate: update mode never signals "this window is finalized," but repeated upserts converge to the correct final row once the watermark stops emitting updates for that window — no separate close-detection needed.
  - **Timestamps**: PySpark's `collect()` returns `TimestampType` as a *naive* datetime in the driver's local timezone (IST on the dev laptop), not UTC. `_row_to_candle()` converts `window_start`/`window_end` to aware UTC before either sink sees them — any future `foreachBatch` job writing timestamps out must do the same. Skipping it once left Postgres storing every candle 5h30m in the future and the chart's backfilled history misaligned with its live updates.
- **`jobs/storage_writer.py`** runs three *separate* streaming queries in one Spark application (one JVM, one Kafka-connector resolution, instead of three) for the Bronze/Silver/Gold Parquet layers under `storage/data/` (gitignored). `start_storage_queries(spark, data_root)` starts them (the tests point it at a temporary directory); `main()` adds the session and the wait:
  - **Bronze** (`storage/data/bronze/trades/`) — `market.trades.raw` written append-only, exactly as `trades_source.py` parses it. The durable replay/backtest source (feature roadmap item 4).
  - **Silver** (`storage/data/silver/trades/`) — Bronze deduplicated by `trade_id` within a watermark window. Ingestion's pydantic validation already guarantees well-formed data, so dedup + a compacted layout is Silver's whole job for now.
  - **Gold** (`storage/data/gold/candles_1m/`) — the *same* `candle_windowing.build_candles()` aggregation as `candle_aggregator.py`, but in `outputMode("append")` instead of `"update"`. Append mode only emits a window once its watermark has passed — exactly once per finalized candle — which is what makes it safe to write to an append-only Parquet sink; writing `candle_aggregator.py`'s update-mode output to Parquet instead would pile up duplicate, progressively-refined rows per window. This is why Gold can't just consume `candle_aggregator.py`'s stream and needs its own query built from the shared aggregation function instead.
- `storage/ddl/gold_candles.sql` defines the `candles_1m` Postgres table `postgres_sink.py` upserts into; apply it once via `storage/init_db.py` (see Setup above). Not using a migration tool yet — revisit once there's more than one table.

`alerts/` is the Telegram bot, the last leg of the whale lane (`alerts.whales` → your phone). Like ingestion, each concern is its own module and only the entrypoint wires them together:

- **`telegram_bot.py`** (`WhaleTelegramBot`) reads `alerts.whales` in its own consumer group, `dsview-telegram-bot` (so it resumes after a restart; its first-ever start begins at the end of the topic). Delivery is **at-least-once**: offsets are committed by hand, and `_commit()` never commits past a whale still held — waiting for its group to close, or for Telegram to come back — so a crash re-reads it instead of losing it. Repeats are ignored on `(exchange, trade_id)` for the last 10,000 whales. Ctrl+C sends the open group at once; each shutdown step gets 5 s (`stop_within()`, as in the detector). `main()` refuses to start without `DSVIEW_TELEGRAM_BOT_TOKEN` and `DSVIEW_TELEGRAM_CHAT_ID`.
- **`whale_batcher.py`** (`WhaleBatcher`) implements the kickoff decision "whales within 10 seconds of each other share one message": a gap rule on the trades' own `event_time`, so a backlog read after a restart groups the way it happened live. A group is sent when a whale arrives that doesn't fit, after 10 s with no new whale, after 60 s of a continuous burst, or at 20 whales (far under Telegram's 4096-character limit). Pure and clock-agnostic — the caller passes `now` — so its tests need no waiting. A lone whale reaches the phone about 10 s after it trades.
- **`whale_card.py`** formats each whale as the decided three-line card (`🐋 BUY · BTCUSDT` / `$242.5K — 2.5 @ 97,000.10` / `Binance · 10:16:57 UTC`), numbers the way the right panel shows them, and `parse_time()` reads every timestamp shape the pipeline writes — Python 3.10's `fromisoformat()` rejects a `Z` suffix, which pydantic writes on every trade.
- **`telegram_client.py`** (`TelegramClient`) is the one place that talks to the Bot API. Sends are spaced 3 s apart (a group chat's 20/minute), a 429's `retry_after` is honoured, network errors and 5xx retry with backoff (1 s → 60 s) for as long as Telegram is down, other 4xx are logged and dropped, and `check()` (getMe + getChat) fails fast on a wrong token or chat. **The token is part of every Bot API URL**: nothing logs a URL or an httpx exception's text, and httpx's own per-request INFO logging is switched off — keep it that way.

`backend/` is a FastAPI service with live push (candles from Redis, whale alerts from Kafka) and a historical-read side:

- **`redis_client.py`** (`CandleRedisReader`) is the read-side counterpart to `streaming/redis_sink.py` — the sole place `backend/` touches `redis`. `get_latest()` reads the cached SET (for a client's initial state); `subscribe_updates()` is an async generator over the PUBLISH channel.
- **`routers/ws.py`** exposes `/ws/candles/{symbol}`: sends the current cached candle on connect, then forwards every subsequent pub/sub update until the client disconnects; if Redis is unreachable it closes the socket with 1011.
- **`routers/push.py`** (`push_until_disconnect`) is how every WebSocket route pushes: it runs the route's push loop alongside a `receive()` that watches for the client leaving, and cancels the loop as soon as it does. A server that only sends notices a closed connection when its next send fails, so without this a closed tab kept its Redis subscription (or Kafka consumer) until the next candle (or whale) — forever when the source is quiet, which also hung `uvicorn --reload` at "Waiting for background tasks to complete". New WebSocket routes should push through it too.
- **`postgres_client.py`** (`CandleHistoryReader`) is the read-side counterpart to `streaming/postgres_sink.py` — the sole place `backend/` touches `psycopg`/Postgres. One connection per request for now (dev request volumes only; a pool is a documented future TODO, not built prematurely).
- **`routers/candles.py`** exposes `GET /candles/{symbol}?timeframe=1m|5m|15m|1h|4h|1D&limit=N&before=<ISO timestamp>`: candles oldest first (chart order); `before` pages backwards for the chart's scroll-back. Only `candles_1m` is stored — bigger timeframes are rolled up at query time by `postgres_client.py`'s `date_bin()` query (its comment explains why capping the scan at `limit × minutes-per-bucket` rows never returns a truncated bucket). Buckets are epoch-aligned, the same alignment `frontend/src/lib/timeframes.js` uses to roll live candles up; the two `TIMEFRAMES` lists must match. This is a genuinely different data path from `ws.py` — it never touches Redis, which has no history to serve.
- **`kafka_client.py`** (`WhaleAlertReader`) is the sole place `backend/` touches `aiokafka`. `stream(backlog)` is an async generator: one consumer per call with **no consumer group** (each WebSocket reads every partition itself and commits nothing, so two tabs both get every alert), seeked `backlog` messages before the end, then live. It *subscribes* rather than `assign()`s: a bare aiokafka consumer never learns a topic's partitions (`partitions_for_topic()` stays `None` even for an existing topic), while a group-less subscription gets every partition assigned, including ones added later. If `alerts.whales` doesn't exist yet, that subscription's metadata request creates it (Redpanda auto-creates topics, as the detector's first publish would). Stop the consumer through `_stop()`, not `consumer.stop()` directly: while the broker is unreachable, aiokafka's `stop()` leaks a `CancelledError` from fetch requests it just cancelled, which would replace the real error and make the stream look cancelled instead of failed. One Kafka connection per open tab is fine at dev scale; a shared consumer fanning out to all sockets is the upgrade if that changes.
- **`routers/whales.py`** exposes `/ws/whales?backlog=N` (default 50, max 500): the last N alerts oldest first, then live ones, each the detector's alert JSON forwarded unchanged. Like `ws.py`, it pushes through `push.py`, so a closed tab releases its consumer at once rather than at the next whale. If the broker can't be reached while a connection starts up (at once if it refuses connections, after aiokafka's 40 s request timeout if it vanishes mid-startup), it closes the socket with code 1011 — clients should reconnect, and the backlog replays what they missed. A stream that's already running rides out broker outages. Nothing else in the backend depends on Kafka.
- **`main.py`** wires up all three routers, plus `CORSMiddleware` (allowing `CORS_ALLOWED_ORIGINS` from `config/settings.py`, by default `http://localhost:5173`, the Vite dev server) — REST, unlike WebSocket, is subject to browser CORS once frontend and backend are different origins/ports.

`frontend/` is a Vite + React app (plain JS, `.jsx`, no TypeScript):

- **`api/candles.js`** (`fetchCandles`) is the one client for `GET /candles/{symbol}`; **`lib/timeframes.js`** defines the timeframes and the epoch-aligned bucket math shared by history and live roll-ups.
- **`hooks/useCandleHistory.js`** fetches the newest page for the current symbol + timeframe — and returns an empty list until it arrives, never the previous timeframe's bars. Older pages are fetched by `CandleChart.jsx` itself on scroll-back.
- **`lib/reconnectingSocket.js`** (`openReconnectingSocket`) is the WebSocket every backend hook uses: it reconnects on every close it didn't ask for (a backend reload, a 1011 when Redis or Kafka is unreachable) with exponential backoff, 1 s doubling to 30 s; only a socket that stayed open 10 s resets the delay, so a dependency that's down — accept, then 1011 at once — isn't retried every second. Plain JS with an injectable WebSocket and timers, so it's tested without React.
- **`hooks/useLiveCandles.js`** owns the connection to `/ws/candles/{symbol}` (through `reconnectingSocket.js`) and exposes the latest parsed 1m candle as React state. After a backend restart it resumes with the minute in progress (the backend sends its cached candle on every connect); minutes that closed entirely during a longer outage aren't filled in until a reload — a Phase 6 task in `docs/ROADMAP.md`.
- **`hooks/useWhaleTrades.js`** owns the connection to `/ws/whales` and returns `{ trades, status }`: every symbol's whale alerts newest first (the detector's alert JSON as-is, capped at the 50 a connect replays) and `"connecting" | "live" | "reconnecting"`. Trades are deduplicated on `(exchange, trade_id)`, which is what makes reconnecting (through `reconnectingSocket.js`) safe: each connect replays the backlog, so a reconnect fills in what was missed without repeating what's shown. `parseTrade()` parses each message *before* the state update and ignores (and logs) anything that isn't a JSON object — parsed inside the updater, one bad message on the topic unmounted the whole app, and the backlog replay brought the blank page back on every reload.
- **`config.js`** holds the backend's HTTP and WebSocket base URLs — the one place they're defined; every client of `backend/` imports them.
- **`hooks/useTimeframeCandle.js`** rolls that 1m stream up into the live candle for the selected timeframe (the backend only streams 1m). It keeps the current bucket's 1m candles in a Map and re-aggregates on every update — the same 1m candle is re-sent many times while it forms, so running totals would double-count — and seeds the Map over REST first, so a bucket already in progress at page load isn't missing its earlier minutes.
- **`components/chart/CandleChart.jsx`** wraps `lightweight-charts` v5 (TradingView's open-source rendering library — unrelated to any "TradingView API," see the Data sources section below; v5's API is `chart.addSeries(CandlestickSeries)`, not v4's `addCandlestickSeries()`) and takes both `history` and `candle` props: `history` seeds the chart once via `setData()` (standard lightweight-charts backfill pattern), then `candle` keeps it live via `series.update()`. `update()` specifically — not `setData()` again — for the live path: the backend re-sends the *same* `window_start` repeatedly while a candle is still forming, and `update()` replaces a bar with a matching timestamp instead of duplicating it, while a new timestamp appends a new bar — exactly matching how `candle_aggregator.py` emits data. `update()` throws on a bar *older* than the last one, so live candles older than the last bar (late-trade refinements of an already-superseded window) are dropped rather than passed through. Scrolling to near the oldest loaded bar fetches the page before it and prepends it, merged with `series.data()` rather than the original history array so bars appended live since load survive. `App.jsx` keys the chart by symbol + timeframe, so a switch mounts a fresh chart instead of resetting refs by hand.
- **`components/chart/chartTheme.js`** reads `styles/theme.css`'s CSS variables into chart/series options — the chart draws on a canvas, which can't use CSS variables, so this keeps `theme.css` the only place colours are defined. Keep `attributionLogo` on: lightweight-charts' license requires a tradingview.com link on the page, and that option provides it.
- **`components/chart/Legend.jsx`** is the top-left O/H/L/C readout, driven by the chart's `subscribeCrosshairMove` (latest bar when the crosshair is off the chart).
- **`drawing/`** is the drawing tools. `DrawingLayer.js` is a single lightweight-charts series primitive that renders every drawing (plus axis labels and the hover cursor); `DrawingController.js` handles interaction — placing via `subscribeClick`/`subscribeCrosshairMove`, selecting/dragging via a capture-phase `mousedown` on the chart's wrapper that stops the press before lightweight-charts (which listens for mouse events, not pointer events) can pan, and Esc/Delete — skipping any key `lib/keyboard.js`'s `isChartKey()` rejects: typed into a field or dialog, or already handled (`preventDefault()`) by something else, which is how an open toolbar menu keeps its Esc from also leaving the tool. `tools.js` holds each tool's anchor count, draw and hit-test, all in pixels. Rays and extended lines store only their two anchors and are run to the pane's edge afresh on every draw; the trend-angle and info-line angles are on-screen angles, so they change with zoom (as TradingView's do). The `measure` key is the toolbar's "Date and price range" — renaming a tool's key orphans drawings already saved under it. A tool can also define `create` (sizes itself from one click), `moveHandle` (constrained or coupled handles), `handles` (where its handles sit) and a function `priceAxisLabel` — see `tools.js`'s header. A long/short position is four points kept in step (entry, target, stop, end), so dragging the whole drawing needs no special case; its target and stop move vertically only and never cross the entry. `barData.js` gives tools the candles plus volume (`bars.between(from, to)`, read fresh on every call, so scroll-back pages and the live bar are included). Anchored VWAP sums from the bar its anchor falls in, on any timeframe, sharing its running sum with the VWAP indicator (`indicators/math.js`), and draws nothing while that bar isn't loaded rather than a wrong average from a later one. Drawings are stored per symbol in `localStorage` (`drawingStore.js`) as time/price, never pixels; `coordinates.js` places a time relative to the bar `timeToIndex(time, true)` returns (`index + (time − barTime) / barSeconds`), so a drawing made on one timeframe lands correctly on every other. `App.jsx` owns the active tool and magnet state; the controller resets the tool to the crosshair itself after each drawing, not just via React, so a click arriving before React re-renders isn't handled with the old tool.
- **`indicators/`** is the indicators: SMA, EMA, Bollinger Bands and VWAP over the candles; RSI and MACD each in their own pane. `math.js` holds the formulas as pure functions; `definitions.js` describes each indicator (params, pane, lines) and its `compute()` — the one seam to swap for Spark's output when roadmap item 2 lands. `IndicatorManager.js` (created by `CandleChart.jsx`, like `DrawingController.js`) owns the indicator series and oscillator panes, rebuilds them all on any list change rather than diffing, and recomputes from `candleSeries.data()` on every data change — `setData()` after history loads or a scroll-back page is prepended, `update()` of the last point on live ticks. `hooks/useIndicators.js` persists the list to `localStorage`; each instance keeps the colour slot it got when added, so removing one never repaints the others. Colours are three validated hues (`--color-series-1..3` in `theme.css`, avoiding the hues that already mean up / down / drawings), then the same three dashed — so over-the-candles indicators cap at six. Legend rows (`components/chart/IndicatorLegend.jsx`) keep values in text colours with a coloured swatch beside each; an oscillator pane's legend is positioned from `pane.getHTMLElement()`.
- **`components/layout/`** holds the workspace around the chart — `TopBar`, `DrawingToolbar`, `RightPanel` (watchlist, plus the whale feed from `useWhaleTrades`, times in UTC like the chart's axis), `BottomBar` — placed by `App.jsx`'s CSS grid. Controls for features that don't exist yet are rendered anyway through `ToolbarButton`'s `notBuilt` prop (dimmed, inert, tooltip says "not built yet"); when a feature lands, drop `notBuilt` and wire `onClick` rather than adding a new button. `DrawingToolbar` groups tools TradingView-style: `drawingToolGroups.js` is the whole layout (groups → titled sections → tools, each with its icon and Alt-key shortcut; planned tools are `notBuilt` entries, same convention — when one lands, add it to `tools.js`'s `TOOLS` and drop the flag). `ToolGroupButton` shows each group's last-used tool (persisted by `hooks/useLastUsedTools.js`) with an arrow that opens `ToolMenu`, which is portalled to `<body>` because the toolbar scrolls and would clip it. Icons lucide lacks are drawn in `toolIcons.js` with lucide's `createLucideIcon` — never copy TradingView's artwork.
- **`components/ErrorBoundary.jsx`** wraps the chart, so a chart error shows a retry button instead of React unmounting the whole app to a blank page.
- Styling is CSS Modules (`*.module.css`, one per component) on top of the design tokens in `styles/theme.css`; icons come from `lucide-react`.
- **`App.jsx`** holds the UI state — timeframe, drawing tool, magnet, the indicator list (via `useIndicators`) and which indicator dialog is open — and hardcodes `SYMBOL = "BTCUSDT"` — symbol selection is future work, not implemented.


`tests/` is the pytest suite, run by `scripts/test.sh` (see Setup / running), against real Kafka, Redis and Postgres rather than mocks — offsets, consumer groups, pub/sub, upserts and Spark's Kafka source are where things actually break. Folders mirror the services (`tests/ingestion/`, `tests/streaming/`, …); `pytest.ini` puts each service folder on `sys.path`, the way running it from its own folder does, so tests import modules the same bare way the services do (`ingestion/` and `backend/` both have a `main.py`, so the backend tests load theirs by path). Conventions:

- **Never the dev stack.** Every service fixture fails unless `DSVIEW_KAFKA_BOOTSTRAP_SERVERS`, `DSVIEW_REDIS_PORT` and `DSVIEW_POSTGRES_DSN` are set, and `test.sh` clears every other `DSVIEW_*` variable. On the dev broker the suite's synthetic whales would reach the right panel and the real Telegram bot.
- **A symbol per test** (the `symbol` fixture): topic names are fixed, so tests share topics and tell their own messages, rows and keys apart by symbol.
- **Synthetic trades go through ingestion's real `MarketDataProducer`** (`trade_producer` fixture, `helpers.make_trade()`), so downstream tests read exactly what live ingestion publishes. A consumer starting at "latest" must be positioned before a test produces (`helpers.wait_until_positioned()`); a Spark query has pinned its offsets once `query.lastProgress` is set (its first, empty batch).
- **Telegram is `tests/fake_telegram.py`**, a stand-in Bot API that records messages and can fail on demand (429, 5xx, 400).
- **WebSocket tests disconnect by hand and wait for the handler to finish** before leaving the `with` block: TestClient cancels the app the moment the block exits, which uvicorn never does.
- Spark tests are marked `spark` (`-m "not spark"` skips them) and share one session; the conftest pins Spark's Python workers to the test interpreter.

Frontend tests are Vitest files next to the code they test (`frontend/src/**/*.test.js`), node environment, no DOM: pure functions and plain-JS modules only (hooks keep their logic in exported functions, like `addTrade()` and `parseTrade()`).

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

`docs/ROADMAP.md` is the execution plan built from this list: what's left, grouped into phases, each with its tasks, kickoff decisions and a definition of done. Work from it, and tick tasks there as they land.

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
