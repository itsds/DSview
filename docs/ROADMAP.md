# DSView roadmap

The execution plan for everything DSView still needs, grouped into phases
that are built and shipped one at a time. CLAUDE.md's "Feature roadmap"
section holds the original design intent and build order; this file turns
it into phases with tasks, decisions and a definition of done.

Keep it current: tick a task when its commit lands, update the phase table
when a phase is merged, and change the matching node's `status` in
`docs/architecture.html` as each stage is built.

Last updated: 2026-11-21

## Where things stand

Built and running end to end: live Binance trades → Redpanda → Spark
(1-minute candles to Redis and Postgres; Bronze/Silver/Gold Parquet) →
FastAPI (live candles over WebSocket, history over REST) → a
`lightweight-charts` chart with backfill, scroll-back, six timeframes,
drawing tools and indicators. EMA 20, 50 and 200 come from Spark on every
timeframe, live and on history (candles → `market.candles.1m` →
`applyInPandasWithState` → Postgres, Redis and Gold); every other
indicator is computed in the browser. Whale trades flow from the
detector to the `alerts.whales` topic, out through `/ws/whales` into the
right panel, and through the Telegram bot to your phone. A re-runnable
backfill fills `candles_1m` a year back from Binance's REST klines and
lands 30 days of aggTrades in Bronze, after which the live indicators
rebuild themselves. All hardcoded
config lives in `config/settings.py`. `./scripts/test.sh` runs a pytest
suite over real Kafka, Redis and Postgres plus the frontend's Vitest
tests, and GitHub Actions runs it on every push.

## How a phase runs

1. **Kickoff.** A short design note listing the decisions that are the
   owner's to make (each phase below names them). Answered once, up front.
2. **Build.** A fresh branch from `main`. One commit per logical step,
   each tested before the next one starts.
3. **Phase end.** A full end-to-end run plus the whole test suite (from
   Phase 1 on), then a pull request with the test evidence and a laptop
   checklist.
4. **Owner.** Runs the checklist on the laptop with live data, then
   merges. The next phase starts from `main`.

## What can be tested where

Each phase is tested as far as possible in Claude Code's cloud container,
then confirmed on the laptop for what only the laptop has.

| Piece | Cloud container | Laptop |
|---|---|---|
| Kafka | Apache Kafka 4.3 (same protocol as Redpanda) | Redpanda via `docker compose` |
| Redis, Postgres | Local installs | `docker compose` |
| Spark + Kafka connector | Spark 3.5.9 on JDK 17 (Java 21 breaks the Arrow transfer `applyInPandasWithState` uses) | Spark on JDK 17 |
| Frontend | Real Chromium via Playwright | Your browser |
| Live market data | Blocked: synthetic trades through ingestion's real producer | Live Binance |
| Telegram API | Blocked: a fake Telegram server | Your real bot |
| `docker compose`, `scripts/dev.sh` | No Docker daemon | Yes |

## Phases at a glance

| Phase | Goal | Size | Status |
|---|---|---|---|
| 0 | Prerequisites (owner) | S | Done |
| 1 | Finish whale tracking; make everything trustworthy | M | Done |
| 2 | Technical indicators in Spark | L | Done |
| 3a | History backfill | M | In progress |
| 3b | Volume footprint | L | Not started |
| 4 | Replay, backtest, long/short ratio | L | Not started |
| 5 | More markets | L | Not started |
| 6 | Production readiness | L | Not started |
| 7 | Stretch and future | XL | Not started |

CLAUDE.md roadmap items map to phases as: item 1 (whale tracking) → 1,
item 2 (indicators) → 2, item 3 (footprint) → 3a and 3b, item 4 (backtest, replay,
long/short) → 4, items 5–6 (order book, AI layer) → 7. The diagram's
"step 4" backfill lands in Phase 3a, because the footprint needs history to
show anything useful; its replay and backtest nodes land in Phase 4.

## Phase 0 — prerequisites (owner)

- [x] Commit `frontend/src/lib/` (`timeframes.js`, `keyboard.js`, `ids.js`)
      from the laptop. The root `.gitignore` hid it until commit `959afc1`,
      so the frontend can't build from the repo until it's in. Landed on
      `main` as `81b164c`.
- [x] Merge branch `claude/vibrant-edison-ql23n3` into `main` (pull
      request #1).

## Phase 1 — finish whale tracking; make everything trustworthy

- [x] Telegram bot, `alerts/telegram_bot.py`: reads `alerts.whales` in its
      own consumer group, ignores repeats on `(exchange, trade_id)`,
      respects Telegram's rate limits when several whales land at once,
      takes its token from an environment variable (never committed), and
      gets a `scripts/dev.sh` window.
- [x] Whale detector: a time limit on shutdown. With the broker down,
      `consumer.stop()` hung indefinitely; each client now gets 5 s.
- [x] `useLiveCandles`: reconnect with backoff, like `useWhaleTrades`.
      The chart stopped updating after any backend restart until the
      page was reloaded; both hooks now share `lib/reconnectingSocket.js`.
- [x] `tests/`: a `pytest` suite against real Kafka, Redis and Postgres
      covering everything built so far, plus frontend tests once
      `frontend/src/lib/` is committed, with a one-command runner
      (`./scripts/test.sh`).
- [x] GitHub Actions running the suite on every push (decision 3 below):
      `.github/workflows/tests.yml`.

**Kickoff decisions** (made 2026-11-20):

1. **Message format.** Each whale is a three-line card:

   ```
   🐋 BUY · BTCUSDT
   $242.5K — 2.5 @ 97,000.10
   Binance · 10:16:57 UTC
   ```

2. **What gets sent.** Every whale the right panel shows also goes to
   Telegram. Whales within 10 seconds of each other are combined into one
   message rather than one notification each.
3. **CI: yes.** GitHub Actions runs the full test suite on every push,
   with Kafka, Redis and Postgres as service containers.

**Done when:** a live whale reaches both the phone and the right panel; the
detector exits promptly on Ctrl+C even with the broker down; the chart
resumes live updates after a backend restart without a page reload; the
test suite passes.

## Phase 2 — technical indicators in Spark

- [x] `candle_aggregator.py` also publishes every 1m candle update to a new
      topic, `market.candles.1m` (decision 3 below): `streaming/candle_topic.py`.
- [x] Spark job computing EMA 20, 50 and 200 on all six timeframes from that
      topic, keeping running state between candles with
      `applyInPandasWithState`; written to Redis (live) and Postgres
      (history), with history warmed up from `candles_1m` on first start:
      `streaming/jobs/indicators.py`, around `indicator_engine.py` (the
      math) and `indicator_state.py` (the Spark state).
- [x] Gold: the same computation over Gold's finalized candles, into
      `gold/indicators` (a fourth query in `storage_writer.py`).
- [x] Backend: REST history and live push for indicator values: each
      candle from `GET /candles` carries its `indicators`, and
      `/ws/indicators/{symbol}?timeframe=` pushes changed bars.
- [x] Frontend: switch `compute()` in `indicators/definitions.js` to the
      backend's values for the standard set (`indicators/server.js`,
      `hooks/useLiveIndicators.js`).

Found along the way and fixed in this phase: the chart's scroll-back
could fire before the first history page landed (when the live candle got
there first) and then throw "data must be asc ordered"; and Spark 3.5's
streaming Parquet source misreads a schema whose partition column isn't
last, which crashed the JVM until the Gold indicator query's read schema
was reordered.

**Kickoff decisions** (made 2026-11-21):

1. **Indicators: EMA 20, 50 and 200.** 20 is the chart's default length, 50
   and 200 the usual trend lengths. These gain most from Spark: an EMA
   depends on every bar before it, so the browser's value depends on how
   much history happens to be loaded (with 300 bars, its EMA 200 still
   carries ~37% of its starting guess at the newest bar). Everything else
   (RSI, MACD, VWAP, SMA, Bollinger, and EMA at any other length) stays
   computed in the browser.
2. **Timeframes: all six** (1m, 5m, 15m, 1h, 4h, 1D), rolled up from 1m
   candles in the chart's own epoch-aligned buckets. The big timeframes
   stay sparse until Phase 3's backfill adds history.
3. **State: Spark's stateful API**, `applyInPandasWithState`, keyed by
   symbol and checkpointed. Spark 3.5 refuses to run it after a streaming
   aggregation in the same query (in update mode and in append mode alike;
   checked at kickoff), so the candles reach it through a Kafka topic
   rather than straight from `candle_windowing.py`. It needs pandas and
   pyarrow, pinned below the versions Spark 3.5 breaks on.

**Done when:** Spark's values match the browser's `indicators/math.js` on
identical candles (a golden test in the suite), and the chart shows server
values both live and on history.

Met in the cloud container: golden tests require exact equality with
math.js for the engine alone, from trades through both Spark jobs into
Postgres, and for Gold; an end-to-end run with ten hours of history and
live synthetic trades showed no mismatch on any bar of any timeframe,
before and after a restart from the checkpoint, and Chromium showed
Spark's values live and on scroll-back. Left for the laptop: the same
with live Binance data.

## Phase 3 — history backfill + volume footprint

Split at kickoff into 3a (backfill) and 3b (footprint), each its own
branch, pull request and laptop check: 3b builds on history 3a has
already proved on live Binance.

**Kickoff decisions** (made 2026-11-21):

1. **Split: 3a backfill, 3b footprint.**
2. **Backfill source: klines for candles, aggTrades for trades.** Binance
   REST 1m klines fill `candles_1m` (a year back by default: about 530
   requests, a few minutes), so EMA 200 seeds on every timeframe, 1D
   included (it needs 200 days). aggTrades for the last 30 days by default
   go to Bronze, for the footprint and, later, replay. Both depths are
   flags. Paging a year of aggTrades instead (roughly a million a day for
   BTCUSDT) would take hours and many GB.
3. **Footprint rows: $1 stored, merged for display.** `footprint_1m`
   keeps $1 price levels; each bar shows rows merged to a multiple ($5,
   $10, $25…) picked to fit the zoom and timeframe, with a manual
   override in the footprint's settings (TradingView's "ticks per row").
4. **Footprint timeframes: 1m to 1h.** Fetched only for the bars on
   screen, once zoomed in far enough to read cells; 4h and 1D stay plain
   candles. The volume profile tools work on every timeframe.

### Phase 3a — history backfill

- [x] Backfill job: pages Binance REST klines into `candles_1m` and
      aggTrades into Bronze (decision 2); safe to rerun. Also fixes the
      thin chart on higher timeframes: `backfill/history_backfill.py`,
      around `binance_rest.py` (the client), `bronze_files.py` (Bronze's
      `klines_1m` and `agg_trades`) and `candle_store.py` (Postgres).
- [x] Rebuild the indicators after a backfill: an EMA depends on every bar
      before it, so history inserted before the oldest stored candle
      changes every value after it. Until now that meant deleting
      `_checkpoints/indicators` by hand; now the backfill bumps the
      symbol's revision in a new `candle_revisions` table and the running
      indicator job rebuilds that symbol on its next batch.

Found along the way and fixed in this phase: with a year of history, a
page of 1D bars took ~3 s from `GET /candles`, because the roll-up sorted
432,000 minute rows on disk to pick each bucket's open and close; they now
come from a primary-key lookup of each bucket's first and last minute
(0.2 s, same results). Gold is deliberately untouched by the backfill:
Gold holds what the live stream finalized, and filling it from Bronze's
history belongs with replay (Phase 4).

**Done when:** rerunning the backfill adds no duplicate rows; the chart
shows a year of history on every timeframe with Spark's EMA 200 on 1D;
the live indicators rebuild themselves after a backfill, without a
restart.

Met in the cloud container, against a fake Binance (`tests/fake_binance.py`)
with the live pipeline running on synthetic trades: a year of candles
(526,619 rows, 751 requests) in 21 s; the running indicator job noticed
the new revision within 3 s and rebuilt in ~25 s; every EMA value of
every timeframe over the whole year (2,035,164 values) equals math.js's;
a rerun added one row (the minute between the first run's end and the
live data) and no duplicates; a restart from the checkpoint didn't
rebuild; the chart showed months of 1D and 4h bars with Spark's EMA 50
and 200. A million aggTrades a day cost 6.6 s of CPU and 26 MB of
Parquet. Left for the laptop: the same against live Binance, where the
request budget, not the client, sets the pace (~40 minutes for 30 days of
aggTrades).

### Phase 3b — volume footprint

- [ ] Footprint: buy and sell volume per $1 price level within each
      candle, computed in Spark, stored in a new `footprint_1m` table and
      in Redis; history for the backfilled range from Bronze's aggTrades.
- [ ] Optional (a suggestion in the diagram): an append-mode Gold copy,
      `gold/footprint_1m`.
- [ ] Backend: `GET /footprint/{symbol}` plus live push.
- [ ] Frontend: a footprint overlay drawn on the chart (1m to 1h), and the
      fixed-range and anchored volume profile tools.

**Done when:** each candle's footprint buy + sell volume equals its candle
volume; the footprint renders live and on history.

## Phase 4 — replay, backtest, long/short ratio

- [ ] Replay mode: feeds Bronze history back through the live pipeline at a
      controlled speed.
- [ ] Backtest framework over the stored history.
- [ ] Binance Futures adapter and the long/short ratio in the UI (spot data
      doesn't carry it).

**Kickoff decisions:** how replayed data is kept apart from live data.

**Done when:** replaying a recorded day reproduces the candles stored from
it live; a sample strategy backtests over stored history; the long/short
ratio shows live.

## Phase 5 — more markets

- [ ] Coinbase Advanced Trade and Kraken WS v2 adapters (subclasses of
      `BaseExchangeClient`).
- [ ] Symbol switcher and a multi-row watchlist (`App.jsx` hardcodes
      `BTCUSDT` today).
- [ ] Per-asset feature flags; gold (XAU-USD), with footprint and whale
      detection off.
- [ ] Per-symbol whale thresholds.
- [ ] Silver: deduplicate on `(exchange, symbol, trade_id)`, not `trade_id`
      alone (`streaming/jobs/storage_writer.py`). Binance numbers trades
      per symbol, so once a second symbol flows, two trades can share an
      id and Silver would drop one. Found in Phase 1; harmless while
      BTCUSDT is the only symbol.
- [ ] Price formatting from each symbol's tick size (TODO in `Legend.jsx`).
- [ ] A timeframe query parameter on `/ws/candles` (TODO in `routers/ws.py`).

**Kickoff decisions:** which gold data feed to use.

**Done when:** two or more exchanges flow at once; switching symbols in
the UI works end to end; gold runs with footprint and whale detection off.

## Phase 6 — production readiness

- [ ] Data quality: Great Expectations checks on Bronze, Silver and Gold.
- [ ] Dead-letter topic `market.trades.dlq`, so malformed trades are kept
      instead of dropped (a suggestion in the diagram).
- [ ] Schema Registry for the trade schema (a suggestion in the diagram).
- [ ] Observability: Prometheus and Grafana.
- [ ] Containerize the backend and frontend.
- [ ] Frontend backend URLs from environment variables (TODO in
      `frontend/src/config.js`).
- [ ] Postgres connection pool (TODO in `backend/postgres_client.py`).
- [ ] Chart: fill in the bars missed while the live socket was
      disconnected. Since Phase 1 it reconnects and resumes with the
      minute in progress, but minutes that closed entirely during a
      longer outage stay missing until a reload (a backend restart's
      second or two misses nothing). Needs a fetch since the last bar,
      merged like a scroll-back page, plus reseeding
      `useTimeframeCandle`'s open bucket.
- [ ] Chart: the browser's VWAP starts at the first loaded bar rather than
      at midnight UTC, so on 1m (300 bars, five hours) it is wrong for most
      of the day. Found at Phase 2's kickoff; VWAP wasn't chosen for Spark.
      Fix by fetching the day's bars for it, or move it to Spark.
- [ ] Optional (suggestions in the diagram): a batch scheduler and a
      Gold → Postgres sync.

**Kickoff decisions:** which of these must land before any move to the
cloud.

**Done when:** quality checks run against every layer; a Grafana dashboard
shows pipeline lag and throughput; the backend and frontend run in
containers.

## Phase 7 — stretch and future

- [ ] Order book: reconstruct book state from `market.depth.raw` (flowing
      since day one, unread so far); depth imbalance; heatmap.
- [ ] AI layer: FinBERT or CryptoBERT sentiment from a news and social
      source; a Kronos-style candlestick forecast overlay.
- [ ] Optional: CoinGecko for market-wide context (e.g. BTC dominance).

Scoped in detail at kickoff.

## Not scheduled

- **Cloud move (AWS: MSK, EMR, ElastiCache, S3):** its own plan, after
  Phase 6.
