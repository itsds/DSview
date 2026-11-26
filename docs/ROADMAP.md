# DSView roadmap

The execution plan for everything DSView still needs, grouped into phases
that are built and shipped one at a time. CLAUDE.md's "Feature roadmap"
section holds the original design intent and build order; this file turns
it into phases with tasks, decisions and a definition of done.

Keep it current: tick a task when its commit lands, update the phase table
when a phase is merged, and change the matching node's `status` in
`docs/architecture.html` as each stage is built.

Last updated: 2026-11-26

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
rebuild themselves. The volume footprint (buy and sell volume per $1
price level per minute) is computed live in Spark next to the candles
and backfilled from those aggTrades; the chart draws it on 1m to 1h once
zoomed in, and fixed-range and anchored volume profiles work on every
timeframe. Replay plays Bronze's history (the live recording, or the
backfill's aggTrades) back through a separate copy of the pipeline with
its own topics, schema and keys, at 1× to 1000× or as fast as Spark keeps
up: the Replay button picks a bar and the chart, indicators, footprint
and whales show the past forming bar by bar while live carries on. All hardcoded
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
| 3a | History backfill | M | Done |
| 3b | Volume footprint | L | Done |
| 4a | Replay | L | In progress |
| 4b | Backtest | M | Not started |
| 4c | Long/short ratio | M | Not started |
| 5 | More markets | L | Not started |
| 6 | Production readiness | L | Not started |
| 7 | Stretch and future | XL | Not started |

CLAUDE.md roadmap items map to phases as: item 1 (whale tracking) → 1,
item 2 (indicators) → 2, item 3 (footprint) → 3a and 3b, item 4 (backtest, replay,
long/short) → 4a, 4b and 4c, items 5–6 (order book, AI layer) → 7. The diagram's
"step 4" backfill lands in Phase 3a, because the footprint needs history to
show anything useful; its replay and backtest nodes land in Phases 4a and 4b.

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

- [x] Footprint: buy and sell volume per $1 price level within each
      candle, computed in Spark, stored in a new `footprint_1m` table and
      in Redis; history for the backfilled range from Bronze's aggTrades:
      `streaming/footprint_windowing.py` (a second query in
      `candle_aggregator.py`'s app, with the candles' window and
      watermark), `storage/ddl/footprint.sql`, and
      `backfill/footprint_rows.py` for the history.
- [x] Optional (a suggestion in the diagram): an append-mode Gold copy,
      `gold/footprint_1m` (a fifth query in `storage_writer.py`).
- [x] Backend: `GET /footprint/{symbol}` plus live push
      (`/ws/footprint/{symbol}`), and `GET /footprint/{symbol}/profile`
      for the volume profiles: `backend/routers/footprint.py`.
- [x] Frontend: a footprint overlay drawn on the chart (1m to 1h), and the
      fixed-range and anchored volume profile tools: `frontend/src/footprint/`,
      the chart type menu in the top bar, and two drawing tools.

**Choices made while building** (inside the kickoff decisions; easy to
change if you'd rather they went another way):

- **Cells:** each bar shows bid × ask, the volume sold at the bid on the
  left and bought at the ask on the right, tinted by size, the point of
  control outlined, diagonal imbalances (3:1) in bold, and the bar's delta
  beneath. A slim candle runs along each column's left edge.
- **When it shows:** bars at least 48 px apart; turning the footprint on
  zooms to 72 px. Auto row size is the smallest of $1, $2, $5, $10, $25,
  $50, $100, $250, $500 or $1000 whose rows are 14 px tall; the chart type
  menu fixes one instead.
- **Volume profiles:** 24 rows over the range's traded prices
  (TradingView's default), a 70% value area, buy and sell split per row.
- **Gold copy built**, as the diagram suggested: a past minute's
  footprint or profile can be rebuilt from Gold without replaying Bronze.

Found along the way and fixed in this phase: lightweight-charts' default
right margin is 8 bars, which at footprint widths is half the screen, so
the footprint narrows it to 2 while it shows.

**Done when:** each candle's footprint buy + sell volume equals its candle
volume; the footprint renders live and on history.

Met in the cloud container, with the live pipeline on synthetic trades
and the backfill against a fake Binance (`tests/fake_binance.py`): every
one of 3,038 minutes with a footprint (two days backfilled from aggTrades
plus the live minutes) adds up exactly to its candle's volume, with no
duplicate level and no footprint without a candle; the live minute's
grid reaches `/ws/footprint` every 5 s; Gold's footprint equals Postgres
for every finalized live minute; a rerun of the backfill fetched only
today and replaced the live minutes with the exchange's, still exact;
the minute still forming can be a batch apart between the candle and
footprint queries and agrees once it closes. Chromium showed the
footprint on history and updating live on 1m and 5m, plain candles when
zoomed out and on 4h/1D, and both volume profile tools placed through the
toolbar. Postgres timings over 30 days (1.4 million rows): a screen of 1m
bars 5 ms, 100 hourly bars 0.2 s, a 30-day profile 0.2 s; the backfill
adds about a second per million aggTrades. Left for the laptop: the same
with live Binance, where real trades crowd many more onto each level.

## Phase 4 — replay, backtest, long/short ratio

Split at kickoff into 4a (replay), 4b (backtest) and 4c (long/short ratio),
each its own branch, pull request and laptop check, as Phase 3 was. None
depends on another; they run in the feature roadmap's order.

**Kickoff decisions** (made 2026-11-22):

1. **Replay runs in a namespace of its own.** The same jobs, started with
   one setting, get their own Kafka topics (`replay.market.trades.raw`, …),
   Postgres schema (`replay`), Redis key prefix (`replay:`), Parquet root,
   checkpoints and consumer groups. The names stay in `config/settings.py`,
   derived from the pipeline's name, so a producer and its consumers still
   can't disagree. Live is never touched, and the Telegram bot only ever
   reads the live topic. One replay at a time; starting one clears the last.

   Checked at kickoff: replaying into the live topic can't work at all. A
   query's watermark is one value for every symbol it reads, so once live
   trades have moved it to "now", every replayed trade is dropped as late,
   with no error. On Spark 3.5.9 with the repo's own `build_candles()`, 240
   trades from the day before made no candles, under the live symbol and
   under a renamed one alike, and Spark counted them as dropped by the
   watermark. They would also land in Bronze a second time.
2. **Replay is driven from the chart.** A Replay button in the top bar:
   pick the start bar and a speed (1×, 10×, 100×, or as fast as Spark keeps
   up), then play, pause or stop. While it runs the chart shows the
   replay's data, then returns to live. Updates arrive once per Spark batch,
   as live ones do.
3. **Backtests: strategies in Python, results on the chart.** A strategy is
   a Python class with `on_bar()`, run from the command line over the lake's
   history: Bronze's `klines_1m` (a year, from the exchange) for candles,
   with EMAs from Spark's own `IndicatorEngine` so a strategy sees the
   values the chart shows, and `agg_trades` for strategies that read the
   footprint. Fills at the next bar's open, 0.1% fee per side (Binance
   spot's base taker fee). Each run's trades and equity curve go to
   Postgres; the chart marks the trades, and a results panel shows the
   curve and the stats. Sample strategy: EMA 20/50 crossover, long and
   short.
4. **Long/short: all four of Binance Futures' ratios**, each in its own
   pane under the chart, like RSI: global accounts
   (`globalLongShortAccountRatio`), top traders' positions
   (`topLongShortPositionRatio`), top traders' accounts
   (`topLongShortAccountRatio`) and taker buy/sell volume
   (`takerlongshortRatio`), for the USDⓈ-M perpetual of the same name
   (BTCUSDT). They are REST endpoints with 5-minute periods at the finest,
   and Binance keeps only the latest 30 days: the backfill fetches those,
   and the poller keeps everything after, so DSView's history outgrows
   Binance's.

**Choices made at kickoff** (inside the decisions; easy to change):

- **Replay's source:** Bronze's live `trades` for days the laptop recorded,
  `agg_trades` (the backfilled 30 days) for the rest, picked per run.
  Replayed aggTrades make Binance's own klines, so either source has an
  exact reference to check against.
- **Replay's jobs run only during a replay:** the candle aggregator (with
  its footprint query) and the indicator job start with a replay and stop
  with it, so nothing holds a JVM between replays. The whale detector runs
  in the namespace too, so replayed whales reach the right panel, never
  Telegram.
- **Replay's indicators start from the history before the start bar:** the
  `replay` schema's `candles_1m` is seeded from live's before the first
  trade, so EMA 200 on 1D has its 200 days, as it did live.
- **Backtests:** one position at a time, $10,000 starting capital, all of
  it in each trade. Run the backfill first to bring `klines_1m` up to date.
- **Long/short pipeline:** a Futures poller in `ingestion/` (a REST poll at
  each 5-minute close, so not a `BaseExchangeClient`, as the diagram
  anticipated) → a `market.futures.ratios` topic through
  `MarketDataProducer` → a plain Python consumer (no windowing, so no
  Spark, like the whale detector) → Postgres and Redis → the backend → the
  chart.

### Phase 4a — replay

- [x] Pipeline namespaces: one setting in `config/settings.py` from which
      every topic, Redis prefix, Postgres schema, Parquet root, checkpoint
      and consumer group follows; `init_db.py` creates the `replay` schema
      from the same DDL: `Pipeline` and `DSVIEW_PIPELINE`.
- [x] Replay service: reads a span of Bronze (`trades` or `agg_trades`) in
      event-time order and produces it to the replay namespace's trades
      topic through `MarketDataProducer` on a scaled clock, with pause,
      resume, speed and stop; seeds the replay schema's history; starts and
      stops the replay namespace's jobs: `replay/`.
- [x] Backend: start and control a replay, and serve every live route for
      the replay namespace too: `?pipeline=replay`, `backend/routers/replay.py`.
- [x] Frontend: the Replay button and its controls in the top bar; the
      chart, indicators, footprint and whale panel switch to the replay's
      data and back: `components/replay/ReplayBar.jsx`, `hooks/useReplay.js`,
      `lib/pipelines.js`.
- [x] `docs/architecture.html`: the replay node feeds the replay
      namespace's own topic, not `market.trades.raw` as drawn today.

**Done when:** replaying a recorded day reproduces Gold's candles for it
(what the live stream finalized from the same trades; `candles_1m` won't
do, since a backfill replaces live minutes with the exchange's) on every
full minute, exactly; replaying a backfilled day reproduces Binance's
klines; live runs untouched throughout; the chart plays, pauses and stops
a replay.

**Choices made while building** (inside the kickoff decisions; easy to
change if you'd rather they went another way):

- **Live's names don't change.** Topics, Redis keys, the `public` schema
  and `storage/data/` are exactly what they were, so the laptop needs no
  migration; replay's are the same names prefixed (`replay.` topics,
  `replay:` keys, a `replay` schema reached through the DSN's
  `search_path`, `storage/data/replay/`). The backend serves both from one
  process: every route takes `?pipeline=replay`.
- **A run's topics are truncated, not recreated** (Kafka's DeleteRecords),
  and the replay's consumers start from the earliest offset, so a trade
  produced while Spark is still starting is never missed, and the replay
  whale detector's committed offsets line up with the new run.
- **Bronze's live recording is read through Spark's own log.** A file the
  log doesn't list is a failed batch's, whose trades the rerun wrote again.
  Spark writes `event_time` as INT96, which has no Parquet statistics, so
  the log's write times are what find a span's files (a file can only
  hold a trade at or after the start if it was written after it).
- **The replay starts at the clicked bar's start**, so that bar is the
  first to form; the chart shows the history before it.
- **The service is a process of its own** (`replay/replay_service.py`, a
  `dev.sh` window), steered through two Redis keys, so a backend reload
  never touches a running replay. It keeps the replay's jobs running once
  the history runs out, so the chart keeps what it shows, until the exit.
- **Holes in the recording are skipped:** a trade over a minute ahead of
  the replay's clock means the laptop was off, not a quiet market, so the
  clock jumps to it rather than replay the silence at 1×.
- **The replay's candle aggregator triggers every second**, live's every
  5 s: at 100× a 5 s batch is eight minutes of bars.
- **Speeds on the bar: 1×, 10×, 100× and Max**; the API takes any multiple
  up to 1000.

Met in the cloud container, on a dev-style stack (Kafka, Redis, Postgres)
with live synthetic trades through ingestion's real producer, all four
live jobs running, and a backfill against a fake Binance: 12 recorded
live minutes (6,415 trades, read from 1,079 Bronze files through Spark's
log) replayed at max speed equal Gold on every minute, open, high, low,
close, volume and trade count, the `auto` source picking the recording;
two backfilled hours (480 aggTrades) equal the fake Binance's klines on
all 120 minutes; live's latest candle kept updating throughout both.
Chromium: the Replay button, picking a bar, the replay forming at 10×
and 100× with the whale panel following, pause holding the clock,
resume, speed, and the exit back to live, with no page errors. The
replay's Spark jobs took 15 to 30 s to start (two JVMs, alongside live's
three, on four cores). The suite adds an end-to-end test through the
real jobs: six minutes of aggTrades come back as exactly the klines,
with the footprint adding up to each candle's volume, EMAs computed in
the replay's schema, and whales on the replay's topic, none of it in
live's. Left for the laptop: the same with live Binance data, where a
day is a few million trades and Bronze's tiny files are the slow part
of a replay at Max.

**Known limitations:** trades live received more than the watermark
(10 s) late were dropped live but are in Bronze, so a replay, which
plays them in order, keeps them: such a minute differs from Gold, and
`compare.py` lists it. Redis's cached candle expires after 2 minutes, so
a chart reopened on a replay paused for longer than that waits for the
next update to show the bar in progress.

### Phase 4b — backtest

- [ ] Backtest engine (`backtest/`): reads the lake's history, rolls it up
      into the chart's timeframes, steps a strategy bar by bar with
      `IndicatorEngine`'s EMAs, fills at the next bar's open with fees.
- [ ] Sample strategy: EMA 20/50 crossover, long and short.
- [ ] Results: each run's trades, equity curve and stats in Postgres, served
      by the backend.
- [ ] Frontend: trade markers on the chart and a results panel (equity
      curve, net profit, win rate, max drawdown, profit factor, the trade
      list).

**Done when:** the sample strategy backtests over a year of stored
history; the EMAs it trades on equal Spark's on every bar; a rerun gives
the same result; the trades show on the chart.

### Phase 4c — long/short ratio

- [ ] Futures poller and the `market.futures.ratios` topic.
- [ ] Consumer: Postgres (a ratios table) and Redis.
- [ ] Backfill: the latest 30 days of all four ratios.
- [ ] Backend: REST history and live push.
- [ ] Frontend: four indicators, each in its own pane.
- [ ] `tests/fake_binance.py`: the four Futures endpoints.

**Done when:** all four ratios show live and on 30 days of history; a
restarted poller fills in the periods it missed (inside Binance's 30
days).

Confirmed at kickoff from Binance's docs (through a search, since the docs
site is blocked in the cloud container): each endpoint takes `symbol`,
`period` (5m to 1d), `limit` (default 30, max 500) and `startTime`/
`endTime`; only the latest 30 days are kept; since August 2026
`globalLongShortAccountRatio` and `topLongShortPositionRatio` take an
optional `contractType`, and without it return all contract types
combined. Left for the build: the exact request weights and rate limit
(one page states 1000 requests per 5 minutes per IP), and whether to pass
`contractType=PERPETUAL`.

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
