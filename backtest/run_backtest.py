"""
run_backtest.py

Runs one backtest from the command line (Phase 4's kickoff decision 3):
a strategy (strategies/) over live's lake history, its EMAs checked
against Spark's, its results stored for the chart's results panel.

1. **Read** every traded 1m kline of the symbol from Bronze's `klines_1m`
   (lake_history.py): run the backfill first to bring it up to date.
2. **Roll up** into the timeframe's bars, with EMAs from Spark's own
   IndicatorEngine warmed up from the oldest minute (bar_series.py), and,
   for a strategy that asks, each bar's footprint from `agg_trades`.
3. **Step** the strategy from `--start` (backtester.py): fills at the next
   bar's open, 0.1% per side, $10,000 all in, one position at a time.
4. **Check** every bar's EMA 20, 50 and 200 against `candle_indicators`
   (indicator_check.py); any difference fails the run (exit status 1).
5. **Store** the run, its trades and equity curve in Postgres
   (backtest_store.py), and say if an earlier run came out the same.

Run from backtest/, with the repo root on PYTHONPATH (it's a one-off job,
not a service: no dev.sh window), after the backfill and init_db.py:

    cd backtest && python run_backtest.py --strategy ema_cross
    python run_backtest.py --strategy ema_cross --timeframe 4h --start 2026-06-01 --param fast=20 --param slow=200

backtest/ reuses streaming/'s IndicatorEngine and backfill/'s Bronze layout
and footprint bucketing, so it puts both folders on sys.path itself, to
import their modules by bare name as every service here imports its own.

Author: @DS
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
for _folder in ("streaming", "backfill"):
    if str(_REPO / _folder) not in sys.path:
        sys.path.insert(0, str(_REPO / _folder))

import argparse  # noqa: E402
import time  # noqa: E402
from datetime import datetime, timezone  # noqa: E402
from decimal import Decimal  # noqa: E402
from typing import Dict, List, Optional  # noqa: E402

from config.settings import LIVE_PIPELINE, SYMBOLS, TIMEFRAMES  # noqa: E402

from backtest_stats import compute_stats  # noqa: E402
from backtest_store import BacktestStore, result_digest  # noqa: E402
from backtester import run  # noqa: E402
from bar_series import TIMEFRAME_SECONDS, build_bars, with_footprints  # noqa: E402
from broker import DEFAULT_CAPITAL, DEFAULT_FEE_RATE  # noqa: E402
from indicator_check import compare_emas  # noqa: E402
from lake_history import NoHistory, read_footprint, read_minutes  # noqa: E402
from strategies import STRATEGIES, strategy_class  # noqa: E402


def parse_time(text: str) -> datetime:
    """An ISO 8601 date or time; UTC unless it says otherwise."""
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def parse_pairs(pairs: List[str]) -> Dict[str, str]:
    parsed = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise ValueError(f"--param takes name=value, not {pair!r}")
        parsed[key] = value
    return parsed


def main(argv: Optional[List[str]] = None, data_root: str = LIVE_PIPELINE.data_root, dsn: str = LIVE_PIPELINE.postgres_dsn) -> int:
    parser = argparse.ArgumentParser(description="Backtest a strategy over the lake's history")
    parser.add_argument("--strategy", required=True, choices=sorted(STRATEGIES))
    parser.add_argument("--symbol", default=SYMBOLS[0])
    parser.add_argument("--timeframe", choices=list(TIMEFRAMES), help="default: the strategy's own (ema_cross: 1h)")
    parser.add_argument("--start", type=parse_time, help="the first bar to trade (default: the oldest stored)")
    parser.add_argument("--end", type=parse_time, help="trade only bars that end by then (default: the newest stored)")
    parser.add_argument("--param", action="append", default=[], metavar="NAME=VALUE", help="a strategy parameter; repeatable")
    parser.add_argument("--capital", type=Decimal, default=DEFAULT_CAPITAL)
    parser.add_argument("--fee", type=Decimal, default=DEFAULT_FEE_RATE, help="per side, a fraction of the traded value")
    parser.add_argument("--no-check", action="store_true", help="skip the check against Spark's EMAs")
    parser.add_argument("--no-save", action="store_true", help="print the results without storing them")
    args = parser.parse_args(argv)

    cls = strategy_class(args.strategy)
    try:
        strategy = cls(**cls.parse_params(parse_pairs(args.param)))
    except ValueError as error:
        parser.error(str(error))
    timeframe = args.timeframe or cls.default_timeframe

    began = time.monotonic()
    try:
        minutes = read_minutes(data_root, args.symbol)
    except NoHistory as error:
        print(error, file=sys.stderr)
        return 2
    bars = build_bars(minutes, timeframe, strategy.ema_lengths)
    if args.end is not None:
        bars = [bar for bar in bars if bar.time + TIMEFRAME_SECONDS[timeframe] <= args.end.timestamp()]
    start = None if args.start is None else int(args.start.timestamp())
    if strategy.needs_footprint and bars:
        first = bars[0].time if start is None else max(start, bars[0].time)
        footprints = read_footprint(data_root, args.symbol, TIMEFRAME_SECONDS[timeframe], first, bars[-1].time + TIMEFRAME_SECONDS[timeframe])
        bars = with_footprints(bars, footprints)
    result = run(strategy, bars, start, capital=args.capital, fee_rate=args.fee)
    if not result.bars:
        print(f"no {timeframe} bars to trade for {args.symbol} in that span", file=sys.stderr)
        return 2
    stats = compute_stats(result)
    print(
        f"{args.symbol} {timeframe}: {len(minutes):,} minutes read, {len(result.bars):,} bars traded "
        f"({_day(result.bars[0].time)} to {_day(result.bars[-1].time)}), {time.monotonic() - began:.1f} s"
    )
    _print_stats(strategy, stats)

    check = None
    store = None if args.no_save and args.no_check else BacktestStore(dsn)
    try:
        if not args.no_check:
            check = compare_emas(result.bars, store.spark_emas(args.symbol, timeframe, result.bars[0].time, result.bars[-1].time))
            _print_check(check)
        if not args.no_save:
            run_id, same = store.save(args.symbol, cls.name, strategy.params, timeframe, result, stats, check, result_digest(result))
            print(f"stored as run {run_id}" + (f": the same result as run {same}" if same else ""))
    finally:
        if store is not None:
            store.close()
    return 1 if check is not None and check["differ"] else 0


def _print_stats(strategy, stats) -> None:
    def money(value):
        return "n/a" if value is None else f"{value:,.2f}"

    def pct(value):
        return "n/a" if value is None else f"{value:.2f}%"

    print(f"  {strategy.name} {strategy.params}")
    print(
        f"  net profit {money(stats['net_profit'])} ({pct(stats['net_profit_pct'])}), "
        f"{stats['total_trades']} trades, win rate {pct(stats['win_rate_pct'])}, "
        f"profit factor {'n/a' if stats['profit_factor'] is None else format(stats['profit_factor'], '.3f')}"
    )
    print(
        f"  max drawdown {money(stats['max_drawdown'])} ({pct(stats['max_drawdown_pct'])}), "
        f"fees {money(stats['total_fees'])}, buy and hold {pct(stats['buy_and_hold_pct'])}"
    )


def _print_check(check) -> None:
    line = f"  EMAs vs Spark: {check['equal']:,} of {check['bars']:,} bars equal"
    if check["differ"]:
        line += f", {check['differ']:,} DIFFER (first: {check['first_difference']})"
    if check["missing"]:
        line += f", {check['missing']:,} without a Spark row (is the indicator job running?)"
    print(line)


def _day(seconds: int) -> str:
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    sys.exit(main())
