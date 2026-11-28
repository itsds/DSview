"""
fake_binance.py

A stand-in for Binance's spot REST API, for the tests and for trying the
backfill without Binance (the cloud container can't reach it). It answers
the two endpoints backfill/binance_rest.py pages, /api/v3/klines and
/api/v3/aggTrades, the way Binance does — limits, the one-hour
startTime/endTime rule, fromId paging, X-MBX-USED-WEIGHT-1M — from a
synthetic market that's a pure function of symbol and time, so a test can
work out every expected value itself (`SyntheticMarket`). It records every
request, and can be told to fail the next ones (429 with Retry-After, 418,
500, 400).

It also stands in for Binance Futures' four long/short ratio endpoints
(Phase 4c), which ingestion/binance_futures_rest.py polls:
/futures/data/globalLongShortAccountRatio, topLongShortAccountRatio,
topLongShortPositionRatio and takerlongshortRatio, at the 5-minute period
(the only one the fake knows). Their values come from `SyntheticRatios`,
again a pure function of symbol, ratio, contract type and period. Like
Binance it keeps only the latest 30 days, publishes a period once it has
closed (plus `publish_delay_ms`), caps a page at 500 and refuses
parameters it doesn't read: which name the contract goes by (`symbol` or
`pair`) and whether `contractType` is taken are per endpoint
(`futures_params`), so a test can play either side of Binance's August
2026 change. Without a contractType an endpoint that takes one answers
for every contract combined, values that differ from the perpetual's.

The market: TRADES_PER_MINUTE aggregate trades in every minute, each
standing for one to three trades, prices a deterministic walk around a
per-symbol level, alternating sides. Its klines are computed from those
same trades, the way Binance's are, so candles built from either source
must agree. `outages` are spans with neither trades nor klines (an exchange
outage); `quiet` spans have no trades but zero-volume klines.

In tests, as a fixture (`fake_binance`): start(), then point the backfill
at `.url` (DSVIEW_BINANCE_REST_URL in a subprocess).

By hand:

    python tests/fake_binance.py --port 8082
    # in backfill/'s shell:
    export DSVIEW_BINANCE_REST_URL=http://127.0.0.1:8082
    # in the Futures poller's (ingestion/futures_main.py):
    export DSVIEW_BINANCE_FUTURES_REST_URL=http://127.0.0.1:8082

Author: @DS
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import zlib
from dataclasses import dataclass, field
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

MINUTE_MS = 60_000
TRADES_PER_MINUTE = 4
WEIGHTS = {"/api/v3/klines": 2, "/api/v3/aggTrades": 4}
MAX_LIMIT = 1000

# Binance Futures' ratio endpoints, by the ratio ids config/settings.py's
# FUTURES_RATIO_KINDS uses.
FUTURES_PATHS = {
    "/futures/data/globalLongShortAccountRatio": "global_accounts",
    "/futures/data/topLongShortAccountRatio": "top_accounts",
    "/futures/data/topLongShortPositionRatio": "top_positions",
    "/futures/data/takerlongshortRatio": "taker_volume",
}
FUTURES_PERIOD_MS = 300_000  # "5m", the only period the fake knows
FUTURES_RETENTION_MS = 30 * 86_400_000  # Binance keeps the latest 30 days
FUTURES_MAX_LIMIT = 500
CONTRACT_TYPES = ("PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER")
ALL_CONTRACTS = "ALL"  # what an endpoint taking contractType answers without one


@dataclass(frozen=True)
class FuturesParams:
    """What one ratio endpoint reads: the contract's parameter name, and whether it takes contractType."""

    instrument: str  # "symbol" or "pair"
    contract_type: bool


# As the change log reads after August 2026 (docs/ROADMAP.md, Phase 4c).
DEFAULT_FUTURES_PARAMS = {
    "global_accounts": FuturesParams("symbol", True),
    "top_accounts": FuturesParams("pair", True),
    "top_positions": FuturesParams("symbol", True),
    "taker_volume": FuturesParams("symbol", False),
}
# Before it: every endpoint by symbol, none taking a contract type.
LEGACY_FUTURES_PARAMS = {kind: FuturesParams("symbol", False) for kind in DEFAULT_FUTURES_PARAMS}


def _ratio_fmt(value: Decimal) -> str:
    """Binance's ratio format: four decimals."""
    return f"{value:.4f}"


@dataclass
class SyntheticRatios:
    """The four ratios as a pure function of (symbol, ratio, contract type, period start)."""

    # (ratio, period start ms) Binance never published: a hole in its data.
    holes: Sequence[Tuple[str, int]] = ()

    def _hash(self, *parts: Any) -> int:
        return zlib.crc32("|".join(str(part) for part in parts).encode())

    def has(self, kind: str, period_ms: int) -> bool:
        return (kind, period_ms) not in self.holes

    def values(self, symbol: str, kind: str, contract: str, period_ms: int) -> Tuple[str, str, str]:
        """(ratio, long, short) as Binance's strings: long/short shares for accounts and positions, volumes for taker."""
        h = self._hash(symbol, kind, contract, period_ms)
        if kind == "taker_volume":
            buy = Decimal(100 + h % 900) + Decimal(h // 1000 % 10_000) / 10_000
            sell = Decimal(100 + h // 7 % 900) + Decimal(h // 3 % 10_000) / 10_000
            return _ratio_fmt(buy / sell), _ratio_fmt(buy), _ratio_fmt(sell)
        long = Decimal(3000 + h % 4000) / 10_000  # 0.3000 to 0.6999
        short = 1 - long
        return _ratio_fmt(long / short), _ratio_fmt(long), _ratio_fmt(short)

    def row(self, symbol: str, kind: str, contract: str, period_ms: int) -> Dict[str, Any]:
        """One period in the endpoint's JSON shape."""
        ratio, long, short = self.values(symbol, kind, contract, period_ms)
        if kind == "taker_volume":
            return {"buySellRatio": ratio, "buyVol": long, "sellVol": short, "timestamp": period_ms}
        # Binance's docs show the timestamp both as a number and as a string;
        # top positions' comes as a string here, so a client must read both.
        timestamp: Any = str(period_ms) if kind == "top_positions" else period_ms
        return {"symbol": symbol, "longShortRatio": ratio, "longAccount": long, "shortAccount": short, "timestamp": timestamp}


def _fmt(value: Decimal) -> str:
    """Binance's number format: fixed point, eight decimals."""
    return f"{value:.8f}"


@dataclass
class SyntheticMarket:
    """Trades and klines as a pure function of (symbol, time)."""

    trades_per_minute: int = TRADES_PER_MINUTE
    outages: Sequence[Tuple[int, int]] = ()  # [start, end) ms: no trades, no klines
    quiet: Sequence[Tuple[int, int]] = ()  # [start, end) ms: no trades, zero-volume klines

    def _in(self, spans: Sequence[Tuple[int, int]], minute_ms: int) -> bool:
        return any(start <= minute_ms < end for start, end in spans)

    def has_trades(self, minute_ms: int) -> bool:
        return not self._in(self.outages, minute_ms) and not self._in(self.quiet, minute_ms)

    def has_kline(self, minute_ms: int) -> bool:
        return not self._in(self.outages, minute_ms)

    def trades_in_minute(self, symbol: str, minute_ms: int) -> List[Dict[str, Any]]:
        """The minute's aggregate trades in Binance's JSON shape, oldest first."""
        if not self.has_trades(minute_ms):
            return []
        level = 20_000 + zlib.crc32(symbol.encode()) % 50_000  # whole dollars, per symbol
        index = minute_ms // MINUTE_MS
        trades = []
        for j in range(self.trades_per_minute):
            agg_id = index * self.trades_per_minute + j
            cents = level * 100 + (index * 37 + j * 113) % 9_000 - 4_500
            quantity = Decimal(1 + (index * 7 + j * 3) % 13) / Decimal(1000)
            first = agg_id * 3
            trades.append(
                {
                    "a": agg_id,
                    "p": _fmt(Decimal(cents) / 100),
                    "q": _fmt(quantity),
                    "f": first,
                    "l": first + (index + j) % 3,
                    "T": minute_ms + j * (MINUTE_MS // self.trades_per_minute) + 7,
                    "m": (index + j) % 2 == 1,
                    "M": True,
                }
            )
        return trades

    def kline(self, symbol: str, minute_ms: int) -> Optional[List[Any]]:
        """The minute's kline in Binance's JSON shape, from its trades; None during an outage."""
        if not self.has_kline(minute_ms):
            return None
        trades = self.trades_in_minute(symbol, minute_ms)
        if not trades:
            price = "0.00000000"
            return [minute_ms, price, price, price, price, "0.00000000", minute_ms + MINUTE_MS - 1,
                    "0.00000000", 0, "0.00000000", "0.00000000", "0"]
        prices = [Decimal(t["p"]) for t in trades]
        quantities = [Decimal(t["q"]) for t in trades]
        buys = [(p, q) for t, p, q in zip(trades, prices, quantities) if not t["m"]]
        return [
            minute_ms,
            _fmt(prices[0]),
            _fmt(max(prices)),
            _fmt(min(prices)),
            _fmt(prices[-1]),
            _fmt(sum(quantities)),
            minute_ms + MINUTE_MS - 1,
            _fmt(sum(p * q for p, q in zip(prices, quantities))),
            sum(t["l"] - t["f"] + 1 for t in trades),
            _fmt(sum((q for _, q in buys), Decimal(0))),
            _fmt(sum((p * q for p, q in buys), Decimal(0))),
            "0",
        ]

    def klines_between(self, symbol: str, start_ms: int, end_ms: int) -> Iterator[List[Any]]:
        """Klines opening in [start_ms, end_ms), oldest first."""
        minute = -(-start_ms // MINUTE_MS) * MINUTE_MS
        while minute < end_ms:
            kline = self.kline(symbol, minute)
            if kline is not None:
                yield kline
            minute += MINUTE_MS

    def trades_between(self, symbol: str, start_ms: int, end_ms: int) -> Iterator[Dict[str, Any]]:
        """Aggregate trades with start_ms <= time < end_ms, oldest first."""
        minute = start_ms // MINUTE_MS * MINUTE_MS
        while minute < end_ms:
            for trade in self.trades_in_minute(symbol, minute):
                if start_ms <= trade["T"] < end_ms:
                    yield trade
            minute += MINUTE_MS


@dataclass
class FakeBinance:
    market: SyntheticMarket = field(default_factory=SyntheticMarket)
    ratios: SyntheticRatios = field(default_factory=SyntheticRatios)
    port: int = 0
    # Trades exist up to here (ms); like the real exchange, nothing after "now".
    now_ms: Optional[int] = None
    # Or "now" from a clock (Unix seconds), for tests on a fake one; wins over now_ms.
    clock: Optional[Callable[[], float]] = None
    # The ratio endpoints' parameters; and how long after a period closes it's published.
    futures_params: Dict[str, FuturesParams] = field(default_factory=lambda: dict(DEFAULT_FUTURES_PARAMS))
    publish_delay_ms: int = 0
    # (status, headers, body) answers for the next requests, used up in order.
    failures: List[Tuple[int, Dict[str, str], Any]] = field(default_factory=list)
    # (predicate(path, params), status): every matching request fails, until cleared.
    rules: List[Tuple[Callable[[str, Dict[str, str]], bool], int]] = field(default_factory=list)
    requests: List[Tuple[str, Dict[str, str]]] = field(default_factory=list)
    _weight: Dict[int, int] = field(default_factory=dict)  # used weight per wall-clock minute
    _server: Optional[ThreadingHTTPServer] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def start(self) -> "FakeBinance":
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 (http.server's naming)
                parsed = urlparse(self.path)
                params = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
                status, headers, body = fake.answer(parsed.path, params)
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for name, value in headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args) -> None:
                pass  # quiet

        self._server = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        threading.Thread(target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()

    def fail_next(self, status: int, retry_after: Optional[float] = None, count: int = 1, msg: str = "") -> None:
        headers = {"Retry-After": str(retry_after)} if retry_after is not None else {}
        body = {"code": -1003 if status in (418, 429) else -1000, "msg": msg or f"fake failure {status}"}
        with self._lock:
            self.failures.extend([(status, headers, body)] * count)

    def fail_matching(self, predicate: Callable[[str, Dict[str, str]], bool], status: int) -> None:
        """Fail every request `predicate(path, params)` picks with `status`, until `rules` is cleared."""
        with self._lock:
            self.rules.append((predicate, status))

    def requests_to(self, path: str) -> List[Dict[str, str]]:
        with self._lock:
            return [params for p, params in self.requests if p == path]

    def answer(self, path: str, params: Dict[str, str]) -> Tuple[int, Dict[str, str], Any]:
        with self._lock:
            self.requests.append((path, params))
            minute = int(time.time() // 60)
            self._weight[minute] = self._weight.get(minute, 0) + WEIGHTS.get(path, 1)
            headers = {"X-MBX-USED-WEIGHT-1M": str(self._weight[minute])}
            if self.failures:
                status, extra, body = self.failures.pop(0)
                return status, {**headers, **extra}, body
            for predicate, status in self.rules:
                if predicate(path, params):
                    return status, headers, {"code": -1000, "msg": f"fake failure {status}"}
        if path in FUTURES_PATHS:
            return self._ratios(FUTURES_PATHS[path], params, headers)
        if path not in WEIGHTS:
            return 404, headers, {"code": -1000, "msg": "Not Found"}
        symbol = params.get("symbol", "")
        if not symbol.endswith("USDT"):
            return 400, headers, {"code": -1121, "msg": "Invalid symbol."}
        limit = min(int(params.get("limit", 500)), MAX_LIMIT)
        now = self._now_ms()
        if path == "/api/v3/klines":
            if params.get("interval") != "1m":
                return 400, headers, {"code": -1120, "msg": "Invalid interval."}
            start = int(params["startTime"])
            end = min(int(params.get("endTime", now)), now) + 1  # endTime is inclusive
            rows = []
            for kline in self.market.klines_between(symbol, start, end):
                rows.append(kline)
                if len(rows) == limit:
                    break
            return 200, headers, rows
        # /api/v3/aggTrades
        if "fromId" in params:
            from_id = int(params["fromId"])
            minute = from_id // self.market.trades_per_minute * MINUTE_MS
            rows = []
            while len(rows) < limit and minute <= now:
                for trade in self.market.trades_in_minute(symbol, minute):
                    if trade["a"] >= from_id and trade["T"] <= now and len(rows) < limit:
                        rows.append(trade)
                minute += MINUTE_MS
            return 200, headers, rows
        start = int(params["startTime"])
        end = int(params["endTime"])
        if end - start >= 3_600_000:
            return 400, headers, {"code": -1127, "msg": "More than 1 hours between startTime and endTime."}
        rows = []
        for trade in self.market.trades_between(symbol, start, min(end, now) + 1):
            rows.append(trade)
            if len(rows) == limit:
                break
        return 200, headers, rows

    def _now_ms(self) -> int:
        if self.clock is not None:
            return int(self.clock() * 1000)
        return self.now_ms if self.now_ms is not None else int(time.time() * 1000)

    def _ratios(self, kind: str, params: Dict[str, str], headers: Dict[str, str]) -> Tuple[int, Dict[str, str], Any]:
        """One of the four ratio endpoints: the periods in [startTime, endTime] that are out, oldest first."""
        accepted = self.futures_params[kind]
        readable = {accepted.instrument, "period", "limit", "startTime", "endTime"}
        if accepted.contract_type:
            readable.add("contractType")
        if not set(params) <= readable:
            return 400, headers, {
                "code": -1104,
                "msg": f"Not all sent parameters were read; read '{len(set(params) & readable)}' parameter(s) "
                f"but was sent '{len(params)}'.",
            }
        symbol = params.get(accepted.instrument, "")
        if not symbol:
            return 400, headers, {
                "code": -1102,
                "msg": f"Mandatory parameter '{accepted.instrument}' was not sent, was empty/null, or malformed.",
            }
        if not symbol.endswith("USDT"):
            return 400, headers, {"code": -1121, "msg": "Invalid symbol."}
        if params.get("period") != "5m":
            return 400, headers, {"code": -1120, "msg": "Invalid period (the fake knows 5m only)."}
        contract = params.get("contractType", ALL_CONTRACTS if accepted.contract_type else "PERPETUAL")
        if contract not in CONTRACT_TYPES and contract != ALL_CONTRACTS:
            return 400, headers, {"code": -1130, "msg": "Data sent for parameter 'contractType' is not valid."}

        now = self._now_ms()
        limit = min(int(params.get("limit", 30)), FUTURES_MAX_LIMIT)
        newest = (now - FUTURES_PERIOD_MS - self.publish_delay_ms) // FUTURES_PERIOD_MS * FUTURES_PERIOD_MS
        oldest = -(-(now - FUTURES_RETENTION_MS) // FUTURES_PERIOD_MS) * FUTURES_PERIOD_MS
        end = min(int(params["endTime"]), newest) if "endTime" in params else newest
        start = int(params["startTime"]) if "startTime" in params else end - (limit - 1) * FUTURES_PERIOD_MS
        period = max(-(-start // FUTURES_PERIOD_MS) * FUTURES_PERIOD_MS, oldest)
        rows = []
        while period <= end:
            if self.ratios.has(kind, period):
                rows.append(self.ratios.row(symbol, kind, contract, period))
            period += FUTURES_PERIOD_MS
        return 200, headers, rows[-limit:]  # more than a page: the newest ones, as without times


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fake Binance REST API (spot klines and aggTrades; Futures' four ratios)")
    parser.add_argument("--port", type=int, default=8082)
    parser.add_argument("--trades-per-minute", type=int, default=TRADES_PER_MINUTE)
    args = parser.parse_args()
    fake = FakeBinance(market=SyntheticMarket(trades_per_minute=args.trades_per_minute), port=args.port).start()
    print(f"Fake Binance on {fake.url}; Ctrl+C to stop", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        fake.stop()
