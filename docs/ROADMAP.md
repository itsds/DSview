# DSView roadmap

The execution plan for everything DSView still needs, grouped into phases
that are built and shipped one at a time. CLAUDE.md's "Feature roadmap"
section holds the original design intent and build order; this file turns
it into phases with tasks, decisions and a definition of done.

Keep it current: tick a task when its commit lands, update the phase table
when a phase is merged, and change the matching node's `status` in
`docs/architecture.html` as each stage is built.

Last updated: 2026-11-20

## Where things stand

Built and running end to end: live Binance trades → Redpanda → Spark
(1-minute candles to Redis and Postgres; Bronze/Silver/Gold Parquet) →
FastAPI (live candles over WebSocket, history over REST) → a
`lightweight-charts` chart with backfill, scroll-back, six timeframes,
drawing tools and browser-computed indicators. Whale trades flow from the
detector to the `alerts.whales` topic, out through `/ws/whales`, and into
the right panel. All hardcoded config lives in `config/settings.py`.

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
| Spark + Kafka connector | Spark 3.5.9 on Java 21 | Spark on JDK 17 |
| Frontend | Real Chromium via Playwright | Your browser |
| Live market data | Blocked: synthetic trades through ingestion's real producer | Live Binance |
| Telegram API | Blocked: a fake Telegram server | Your real bot |
| `docker compose`, `scripts/dev.sh` | No Docker daemon | Yes |

## Phases at a glance

| Phase | Goal | Size | Status |
|---|---|---|---|
| 0 | Prerequisites (owner) | S | Done |
| 1 | Finish whale tracking; make everything trustworthy | M | In progress |
| 2 | Technical indicators in Spark | L | Not started |
| 3 | History backfill + volume footprint | XL | Not started |
| 4 | Replay, backtest, long/short ratio | L | Not started |
| 5 | More markets | L | Not started |
| 6 | Production readiness | L | Not started |
| 7 | Stretch and future | XL | Not started |

CLAUDE.md roadmap items map to phases as: item 1 (whale tracking) → 1,
item 2 (indicators) → 2, item 3 (footprint) → 3, item 4 (backtest, replay,
long/short) → 4, items 5–6 (order book, AI layer) → 7. The diagram's
"step 4" backfill lands in Phase 3, because the footprint needs history to
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
- [ ] `useLiveCandles`: reconnect with backoff, like `useWhaleTrades`.
      Today the chart stops updating after any backend restart until the
      page is reloaded.
- [ ] `tests/`: a `pytest` suite against real Kafka, Redis and Postgres
      covering everything built so far, plus frontend tests once
      `frontend/src/lib/` is committed, with a one-command runner.
- [ ] GitHub Actions running the suite on every push (decision 3 below).

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

- [ ] Spark job computing EMA and RSI (and whichever others are chosen) over
      1-minute candles, keeping running state between windows; written to
      Redis (live), Postgres (history) and Gold.
- [ ] Backend: REST history and live push for indicator values.
- [ ] Frontend: switch `compute()` in `indicators/definitions.js` to the
      backend's values for the standard set.

**Kickoff decisions:** the UI accepts any indicator settings, but Spark can
only precompute a fixed set. Choose which indicators, settings and
timeframes Spark computes; anything custom stays computed in the browser.

**Done when:** Spark's values match the browser's `indicators/math.js` on
identical candles (a golden test in the suite), and the chart shows server
values both live and on history.

## Phase 3 — history backfill + volume footprint

- [ ] Backfill job: pages Binance REST `aggTrades` into Bronze and
      `candles_1m`; safe to rerun. Also fixes the thin chart on higher
      timeframes.
- [ ] Footprint: buy and sell volume per price level within each candle,
      computed in Spark, stored in a new `footprint_1m` table and in Redis.
- [ ] Optional (a suggestion in the diagram): an append-mode Gold copy,
      `gold/footprint_1m`.
- [ ] Backend: `GET /footprint/{symbol}` plus live push.
- [ ] Frontend: a footprint overlay drawn on the chart, and a volume
      profile.

**Kickoff decisions:** the price-bucket size. This phase can also split
into "backfill" and "footprint" if smaller phases are preferred.

**Done when:** rerunning the backfill adds no duplicate rows; each candle's
footprint buy + sell volume equals its candle volume; the footprint renders
live and on history.

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
