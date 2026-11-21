"""
binance_rest.py

The one place backfill/ talks to Binance: pages history from its spot REST
API's public market data (no API key), the history the live socket never
saw. history_backfill.py decides what to fetch and where it goes; this
module only fetches, politely.

- `klines(symbol, start, end)`: 1-minute candles, oldest first, 1000 per
  request (GET /api/v3/klines, weight 2). Cheap: a year is ~530 requests.
- `agg_trades(symbol, start, end)`: aggregate trades, oldest first, 1000
  per request (GET /api/v3/aggTrades, weight 4). An aggregate trade is
  every fill of one taker order at one price, so per-price buy and sell
  volume is exactly what the individual trades give (all the footprint
  needs), and its first and last trade ids say how many trades it stands
  for. Its ids are a different series from the live stream's trade ids.

Both yield pages already normalized to plain columns (a dict of lists,
named as in Bronze; see KLINE_COLUMNS and AGG_TRADE_COLUMNS), so nothing
downstream sees Binance's wire format: the same rule ingestion/schema.py
sets for the live stream. Times are Unix milliseconds, as Binance's own.

**Rate limits.** Binance allows 6000 request weight per minute per IP,
answers 429 past it, and bans an IP that keeps going (418, for minutes
to days). So every request first takes its weight from a `WeightBudget`
shared by all concurrent requests: a fixed window per wall-clock minute
(Binance's own window), capped at WEIGHT_BUDGET_PER_MINUTE, half the
limit by default, and kept in step with the X-MBX-USED-WEIGHT-1M header
of every response, which also counts anything else on the same IP. A 429
pauses every request for its Retry-After. A 418 stops the run
(BinanceBanned): continuing is what extends a ban, and a backfill is safe
to rerun later.

**Failures.** A network error, a timeout or a 5xx is retried with backoff,
1 s doubling to 30 s, MAX_ATTEMPTS times in all, then BinanceUnavailable
ends the run. Any other 4xx (an unknown symbol, say) is BinanceRequestError
at once, with Binance's own message: retrying can't fix the request.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, AsyncIterator, Callable, Dict, List, Mapping, Optional

import httpx

from config.settings import BINANCE_REST_URL

logger = logging.getLogger(__name__)
# httpx logs every request at INFO; a backfill makes tens of thousands.
logging.getLogger("httpx").setLevel(logging.WARNING)

WEIGHT_LIMIT_PER_MINUTE = 6000  # Binance's REQUEST_WEIGHT limit per IP
WEIGHT_BUDGET_PER_MINUTE = 3000  # what a backfill allows itself: half, leaving room for anything else
KLINES_WEIGHT = 2
AGG_TRADES_WEIGHT = 4
PAGE_LIMIT = 1000  # the most either endpoint returns per request

MAX_ATTEMPTS = 8
BACKOFF_INITIAL_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 30.0
REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_RETRY_AFTER_SECONDS = 60.0  # if a 429 doesn't say

MINUTE_MS = 60_000
# aggTrades refuses a startTime/endTime pair an hour or more apart.
AGG_TRADES_WINDOW_MS = 3_600_000

# The normalized page columns, in Bronze's names. Prices and quantities stay
# strings, exactly as Binance sends them; times are Unix milliseconds.
KLINE_COLUMNS = (
    "window_start",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ingested_at",
)
AGG_TRADE_COLUMNS = (
    "agg_trade_id",
    "event_time",
    "price",
    "quantity",
    "side",
    "first_trade_id",
    "last_trade_id",
    "is_best_match",
    "ingested_at",
)

Page = Dict[str, List[Any]]


class BinanceError(RuntimeError):
    """A request Binance didn't answer with data."""


class BinanceRequestError(BinanceError):
    """A 4xx other than 418/429: the request itself is wrong (an unknown symbol, say)."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"Binance refused the request (HTTP {status}): {message}")
        self.status = status


class BinanceBanned(BinanceError):
    """HTTP 418: this IP is banned for ignoring rate limits. Stop, and rerun after `retry_after` seconds."""

    def __init__(self, retry_after: Optional[float]) -> None:
        wait = f"for {retry_after:.0f} s" if retry_after else "for a while"
        super().__init__(f"Binance has banned this IP {wait} (HTTP 418); rerun the backfill after that")
        self.retry_after = retry_after


class BinanceUnavailable(BinanceError):
    """No good answer after MAX_ATTEMPTS: Binance (or the network) is down."""


class WeightBudget:
    """
    Request weight per wall-clock minute, shared by every request of a run.

    acquire() waits until `weight` fits under the budget in the current
    minute; observe() takes in a response's X-MBX-USED-WEIGHT-1M, Binance's
    own count for this IP and minute (it includes requests from anything
    else on the IP); pause() holds every request back, for a 429.
    """

    def __init__(
        self,
        per_minute: int = WEIGHT_BUDGET_PER_MINUTE,
        wall_clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        if not 0 < per_minute <= WEIGHT_LIMIT_PER_MINUTE:
            raise ValueError(f"a budget of {per_minute} weight per minute is outside 1..{WEIGHT_LIMIT_PER_MINUTE}")
        self.per_minute = per_minute
        self._clock = wall_clock
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._minute = -1
        self._used = 0
        self._paused_until = 0.0

    async def acquire(self, weight: int) -> None:
        async with self._lock:  # one waiter at a time, first come first served
            while True:
                now = self._clock()
                if now < self._paused_until:
                    await self._sleep(self._paused_until - now)
                    continue
                self._roll(now)
                if self._used + weight <= self.per_minute:
                    self._used += weight
                    return
                await self._sleep((self._minute + 1) * 60 - now)  # until the next minute starts

    def observe(self, used_header: Optional[str]) -> None:
        """Binance's count for this minute: raise ours to it (it also counts other clients on the IP)."""
        if not used_header:
            return
        try:
            used = int(used_header)
        except ValueError:
            return
        self._roll(self._clock())
        self._used = max(self._used, used)

    def pause(self, seconds: float) -> None:
        self._paused_until = max(self._paused_until, self._clock() + seconds)

    def _roll(self, now: float) -> None:
        minute = int(now // 60)
        if minute != self._minute:
            self._minute, self._used = minute, 0


class BinanceHistoryClient:
    """Pages Binance spot klines and aggTrades. Use as an async context manager, or call close()."""

    def __init__(
        self,
        base_url: str = BINANCE_REST_URL,
        budget: Optional[WeightBudget] = None,
        max_attempts: int = MAX_ATTEMPTS,
        backoff_initial: float = BACKOFF_INITIAL_SECONDS,
        backoff_max: float = BACKOFF_MAX_SECONDS,
        request_timeout: float = REQUEST_TIMEOUT_SECONDS,
        sleep: Callable[[float], Any] = asyncio.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.budget = budget or WeightBudget(sleep=sleep, wall_clock=clock)
        self._http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=request_timeout)
        self._max_attempts = max_attempts
        self._backoff_initial = backoff_initial
        self._backoff_max = backoff_max
        self._sleep = sleep
        self._clock = clock
        self.requests = 0  # made so far, for the run's summary

    async def __aenter__(self) -> "BinanceHistoryClient":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    async def klines(self, symbol: str, start_ms: int, end_ms: int) -> AsyncIterator[Page]:
        """1m klines opening in [start_ms, end_ms), oldest first, a page at a time; KLINE_COLUMNS."""
        cursor = start_ms
        while cursor < end_ms:
            rows = await self._get(
                "/api/v3/klines",
                {"symbol": symbol, "interval": "1m", "startTime": cursor, "endTime": end_ms - 1, "limit": PAGE_LIMIT},
                KLINES_WEIGHT,
            )
            page = [row for row in rows if cursor <= row[0] < end_ms]
            if page:
                yield self._kline_page(page)
            if len(rows) < PAGE_LIMIT:
                return
            cursor = rows[-1][0] + MINUTE_MS

    async def agg_trades(self, symbol: str, start_ms: int, end_ms: int) -> AsyncIterator[Page]:
        """Aggregate trades with start_ms <= time < end_ms, oldest first, a page at a time; AGG_TRADE_COLUMNS."""
        # The first page: by time, an hour's window at a time (no trades in
        # one, during an exchange outage say, means trying the next). Every
        # page after it follows on by id, which is cheaper and can't skip.
        rows: List[Mapping[str, Any]] = []
        window = start_ms
        while window < end_ms and not rows:
            rows = await self._get(
                "/api/v3/aggTrades",
                {
                    "symbol": symbol,
                    "startTime": window,
                    "endTime": min(window + AGG_TRADES_WINDOW_MS, end_ms) - 1,
                    "limit": PAGE_LIMIT,
                },
                AGG_TRADES_WEIGHT,
            )
            window += AGG_TRADES_WINDOW_MS
        while rows:
            page = [row for row in rows if row["T"] < end_ms]
            if page:
                yield self._agg_trade_page(page)
            if len(page) < len(rows):
                return  # reached end_ms
            rows = await self._get(
                "/api/v3/aggTrades",
                {"symbol": symbol, "fromId": rows[-1]["a"] + 1, "limit": PAGE_LIMIT},
                AGG_TRADES_WEIGHT,
            )

    def _kline_page(self, rows: List[List[Any]]) -> Page:
        # [open time, open, high, low, close, volume, close time, quote volume,
        #  trades, taker buy base volume, taker buy quote volume, unused]
        fetched = int(self._clock() * 1000)
        return {
            "window_start": [row[0] for row in rows],
            "open": [row[1] for row in rows],
            "high": [row[2] for row in rows],
            "low": [row[3] for row in rows],
            "close": [row[4] for row in rows],
            "volume": [row[5] for row in rows],
            "quote_volume": [row[7] for row in rows],
            "trade_count": [row[8] for row in rows],
            "taker_buy_volume": [row[9] for row in rows],
            "taker_buy_quote_volume": [row[10] for row in rows],
            "ingested_at": [fetched] * len(rows),
        }

    def _agg_trade_page(self, rows: List[Mapping[str, Any]]) -> Page:
        # {"a": agg id, "p": price, "q": quantity, "f": first trade id,
        #  "l": last trade id, "T": time, "m": buyer was the maker, "M": best match}
        fetched = int(self._clock() * 1000)
        return {
            "agg_trade_id": [row["a"] for row in rows],
            "event_time": [row["T"] for row in rows],
            "price": [row["p"] for row in rows],
            "quantity": [row["q"] for row in rows],
            # The buyer was the maker, so the taker sold: the live adapter's rule.
            "side": ["sell" if row["m"] else "buy" for row in rows],
            "first_trade_id": [row["f"] for row in rows],
            "last_trade_id": [row["l"] for row in rows],
            "is_best_match": [bool(row.get("M", True)) for row in rows],
            "ingested_at": [fetched] * len(rows),
        }

    async def _get(self, path: str, params: Mapping[str, Any], weight: int) -> Any:
        delay = self._backoff_initial
        attempt = 0
        while True:
            await self.budget.acquire(weight)
            self.requests += 1
            try:
                response = await self._http.get(path, params=params)
            except httpx.TransportError as exc:  # connection errors and timeouts alike
                problem = type(exc).__name__
            else:
                self.budget.observe(response.headers.get("X-MBX-USED-WEIGHT-1M"))
                status = response.status_code
                if status == 200:
                    return response.json()
                if status == 418:
                    raise BinanceBanned(_retry_after(response))
                if status == 429:
                    wait = _retry_after(response) or DEFAULT_RETRY_AFTER_SECONDS
                    logger.warning("Binance: rate limited (HTTP 429); pausing every request for %.0f s", wait)
                    self.budget.pause(wait)
                    continue  # Binance named the wait; not a failed attempt
                if status < 500:
                    raise BinanceRequestError(status, _error_message(response))
                problem = f"HTTP {status}"
            attempt += 1
            if attempt >= self._max_attempts:
                raise BinanceUnavailable(f"{path}: {problem}, {attempt} attempts in a row")
            logger.warning("Binance: %s on %s; retrying in %.0f s", problem, path, delay)
            await self._sleep(delay)
            delay = min(delay * 2, self._backoff_max)


def _retry_after(response: httpx.Response) -> Optional[float]:
    try:
        return float(response.headers["Retry-After"])
    except (KeyError, ValueError):
        return None


def _error_message(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:200]
    if isinstance(body, dict) and "msg" in body:
        return f"{body['msg']} (code {body.get('code')})"
    return str(body)[:200]
