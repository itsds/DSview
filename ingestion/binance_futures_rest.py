"""
binance_futures_rest.py

The one place ingestion/ talks to Binance Futures' REST API: the four
long/short ratio endpoints (Phase 4c), for the USDⓈ-M perpetual of each
symbol's name, at the 5-minute period (config/settings.py's
FUTURES_RATIO_PERIOD, Binance's finest). futures_poller.py decides what to
ask for and when; this module only fetches, politely, and normalizes what
comes back into schema.py's FuturesRatio, so nothing downstream sees
Binance's wire format.

| ratio (FUTURES_RATIO_KINDS) | endpoint                                |
|-----------------------------|-----------------------------------------|
| global_accounts             | /futures/data/globalLongShortAccountRatio |
| top_accounts                | /futures/data/topLongShortAccountRatio  |
| top_positions               | /futures/data/topLongShortPositionRatio |
| taker_volume                | /futures/data/takerlongshortRatio       |

Each takes `period`, `limit` (at most 500) and `startTime`/`endTime`, and
keeps only the latest 30 days. `ratios()` pages a span in windows of
PAGE_LIMIT periods, always sending both ends, so a page is never more than
Binance returns at once, whichever end of a longer window it would keep.

**The perpetual only** (Phase 4c's kickoff decision): `contractType=
PERPETUAL` wherever an endpoint takes one. Which endpoints those are, and
what the contract's parameter is called, is the one thing here not read
from Binance's docs directly (they're blocked in the cloud container
DSView is built in; docs/ROADMAP.md says what searches found: since August
2026 the three account and position endpoints take an optional
contractType, which without it means every contract combined, and
topLongShortAccountRatio names the contract `pair`, not `symbol`). So each
endpoint has its candidate shapes in SHAPES, most likely first, and its
first request walks them: a shape Binance refuses for its parameters (a
-11xx error: "mandatory parameter not sent", "not all sent parameters were
read", ...) moves on to the next, and the first one answered is kept for
the rest of the run and logged. The last resort, `symbol` alone, is only
reached when no shape with a contractType is taken at all, i.e. when
Binance has no such parameter; a symbol then names one contract, and
BTCUSDT's is the perpetual. No shape leaves contractType off where
Binance takes it, so the data is never the contracts combined.

**Rate limits.** These endpoints share an IP limit of 1000 requests per 5
minutes (and carry no weight header to keep in step with), so every request
first takes a slot from a `RequestBudget`, a sliding 5-minute window capped
at REQUEST_BUDGET, half the limit. A 429 pauses every request for its
Retry-After; a 418 (banned for ignoring them) raises BinanceBanned for the
poller to wait out.

**Failures**, as backfill/binance_rest.py's (a different API host, limit
model and caller, so its own copy rather than an import across services):
a network error, a timeout or a 5xx is retried with backoff, 1 s doubling
to 30 s, MAX_ATTEMPTS times, then BinanceUnavailable; any other 4xx is
BinanceRequestError at once, with Binance's message and code.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, AsyncIterator, Callable, Deque, Dict, List, Mapping, Optional, Tuple

import httpx

from config.settings import BINANCE_FUTURES_REST_URL, FUTURES_RATIO_KINDS, FUTURES_RATIO_PERIOD

from schema import Exchange, FuturesRatio

logger = logging.getLogger(__name__)
# httpx logs every request at INFO; a catch-up makes hundreds.
logging.getLogger("httpx").setLevel(logging.WARNING)

PATHS: Dict[str, str] = {
    "global_accounts": "/futures/data/globalLongShortAccountRatio",
    "top_accounts": "/futures/data/topLongShortAccountRatio",
    "top_positions": "/futures/data/topLongShortPositionRatio",
    "taker_volume": "/futures/data/takerlongshortRatio",
}
assert set(PATHS) == set(FUTURES_RATIO_KINDS), "every ratio in config/settings.py needs its endpoint"

CONTRACT_TYPE = "PERPETUAL"  # Phase 4c's kickoff decision
PERIOD = "5m"
PERIOD_MS = int(FUTURES_RATIO_PERIOD.total_seconds() * 1000)
assert PERIOD_MS == 300_000, "PERIOD must name FUTURES_RATIO_PERIOD in Binance's terms"
PAGE_LIMIT = 500  # the most these endpoints return per request

REQUEST_LIMIT = 1000  # Binance's: requests per BUDGET_WINDOW_SECONDS per IP, for these endpoints
REQUEST_BUDGET = 500  # what the poller allows itself: half, leaving room for anything else on the IP
BUDGET_WINDOW_SECONDS = 300.0

MAX_ATTEMPTS = 8
BACKOFF_INITIAL_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 30.0
REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_RETRY_AFTER_SECONDS = 60.0  # if a 429 doesn't say


@dataclass(frozen=True)
class ParamShape:
    """How a request names its contract: by `symbol` or by `pair`, with or without contractType=PERPETUAL."""

    instrument: str
    contract_type: bool

    def params(self, symbol: str) -> Dict[str, str]:
        params = {self.instrument: symbol}
        if self.contract_type:
            params["contractType"] = CONTRACT_TYPE
        return params

    def __str__(self) -> str:
        return f"{self.instrument}" + (f" + contractType={CONTRACT_TYPE}" if self.contract_type else " alone")


_SYMBOL_PERPETUAL = ParamShape("symbol", True)
_PAIR_PERPETUAL = ParamShape("pair", True)
_SYMBOL_ONLY = ParamShape("symbol", False)

# Per endpoint, the shapes to try, most likely first (see the module docstring).
SHAPES: Dict[str, Tuple[ParamShape, ...]] = {
    "global_accounts": (_SYMBOL_PERPETUAL, _PAIR_PERPETUAL, _SYMBOL_ONLY),
    "top_accounts": (_PAIR_PERPETUAL, _SYMBOL_PERPETUAL, _SYMBOL_ONLY),
    "top_positions": (_SYMBOL_PERPETUAL, _PAIR_PERPETUAL, _SYMBOL_ONLY),
    "taker_volume": (_SYMBOL_ONLY,),
}


class BinanceError(RuntimeError):
    """A request Binance didn't answer with data."""


class BinanceRequestError(BinanceError):
    """A 4xx other than 418/429: the request itself is wrong (an unknown symbol, a parameter it doesn't read)."""

    def __init__(self, status: int, message: str, code: Optional[int] = None) -> None:
        super().__init__(f"Binance Futures refused the request (HTTP {status}): {message}")
        self.status = status
        self.code = code
        self.detail = message

    @property
    def is_parameter_error(self) -> bool:
        """Binance's -11xx codes: something about the request's parameters."""
        return self.code is not None and -1199 <= self.code <= -1100


class BinanceBanned(BinanceError):
    """HTTP 418: this IP is banned for ignoring rate limits, for `retry_after` seconds if Binance says."""

    def __init__(self, retry_after: Optional[float]) -> None:
        wait = f"for {retry_after:.0f} s" if retry_after else "for a while"
        super().__init__(f"Binance Futures has banned this IP {wait} (HTTP 418)")
        self.retry_after = retry_after


class BinanceUnavailable(BinanceError):
    """No good answer after MAX_ATTEMPTS: Binance (or the network) is down."""


class RequestBudget:
    """
    Requests per sliding BUDGET_WINDOW_SECONDS, shared by every request.

    acquire() waits until a request fits; pause() holds every request back,
    for a 429.
    """

    def __init__(
        self,
        per_window: int = REQUEST_BUDGET,
        window_seconds: float = BUDGET_WINDOW_SECONDS,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        if not 0 < per_window <= REQUEST_LIMIT:
            raise ValueError(f"a budget of {per_window} requests is outside 1..{REQUEST_LIMIT}")
        self.per_window = per_window
        self._window = window_seconds
        self._clock = clock
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._sent: Deque[float] = deque()
        self._paused_until = 0.0

    async def acquire(self) -> None:
        async with self._lock:  # one waiter at a time, first come first served
            while True:
                now = self._clock()
                if now < self._paused_until:
                    await self._sleep(self._paused_until - now)
                    continue
                while self._sent and self._sent[0] <= now - self._window:
                    self._sent.popleft()
                if len(self._sent) < self.per_window:
                    self._sent.append(now)
                    return
                await self._sleep(self._sent[0] + self._window - now)  # until the oldest leaves the window

    def pause(self, seconds: float) -> None:
        self._paused_until = max(self._paused_until, self._clock() + seconds)


class BinanceFuturesClient:
    """Pages Binance Futures' long/short ratios. Use as an async context manager, or call close()."""

    def __init__(
        self,
        base_url: str = BINANCE_FUTURES_REST_URL,
        budget: Optional[RequestBudget] = None,
        max_attempts: int = MAX_ATTEMPTS,
        backoff_initial: float = BACKOFF_INITIAL_SECONDS,
        backoff_max: float = BACKOFF_MAX_SECONDS,
        request_timeout: float = REQUEST_TIMEOUT_SECONDS,
        sleep: Callable[[float], Any] = asyncio.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.budget = budget or RequestBudget(sleep=sleep, clock=clock)
        self._http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=request_timeout)
        self._max_attempts = max_attempts
        self._backoff_initial = backoff_initial
        self._backoff_max = backoff_max
        self._sleep = sleep
        self._shapes: Dict[str, ParamShape] = {}  # per ratio, the shape Binance took
        self.requests = 0  # made so far

    async def __aenter__(self) -> "BinanceFuturesClient":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._http.aclose()

    def shape(self, kind: str) -> Optional[ParamShape]:
        """The parameter shape Binance took for `kind`, once a request has settled it."""
        return self._shapes.get(kind)

    async def ratios(self, symbol: str, kind: str, start_ms: int, end_ms: int) -> AsyncIterator[List[FuturesRatio]]:
        """`kind`'s periods starting in [start_ms, end_ms), oldest first, a page at a time (none empty)."""
        if kind not in PATHS:
            raise ValueError(f"unknown ratio {kind!r}: one of {list(PATHS)}")
        cursor = -(-start_ms // PERIOD_MS) * PERIOD_MS
        while cursor < end_ms:
            window_end = min(cursor + PAGE_LIMIT * PERIOD_MS, end_ms)
            rows = await self._fetch(symbol, kind, cursor, window_end - 1)  # endTime is inclusive
            page: Dict[int, FuturesRatio] = {}
            for row in rows:
                event = self._event(symbol, kind, row)
                if event is not None and cursor <= _ms(event.event_time) < window_end:
                    page[_ms(event.event_time)] = event
            if page:
                yield [page[start] for start in sorted(page)]
            cursor = window_end

    async def _fetch(self, symbol: str, kind: str, start_ms: int, end_ms: int) -> List[Mapping[str, Any]]:
        settled = self._shapes.get(kind)
        refusals = []
        for shape in (settled,) if settled else SHAPES[kind]:
            params = {**shape.params(symbol), "period": PERIOD, "startTime": start_ms, "endTime": end_ms, "limit": PAGE_LIMIT}
            try:
                rows = await self._get(PATHS[kind], params)
            except BinanceRequestError as exc:
                if settled or not exc.is_parameter_error:
                    raise
                refusals.append(f"{shape}: {exc.detail}")
                logger.info("Binance Futures %s: refused by %s (%s); trying the next shape", kind, shape, exc.detail)
                continue
            if not settled:
                self._shapes[kind] = shape
                logger.info("Binance Futures %s: asking by %s", kind, shape)
                if shape is _SYMBOL_ONLY and len(SHAPES[kind]) > 1:
                    logger.warning(
                        "Binance Futures %s takes no contractType: reading %s by symbol, its perpetual contract",
                        kind,
                        symbol,
                    )
            if not isinstance(rows, list):
                raise BinanceRequestError(200, f"expected a list of periods, got {str(rows)[:200]}")
            return rows
        raise BinanceRequestError(400, f"{PATHS[kind]} took none of its parameter shapes: " + "; ".join(refusals))

    def _event(self, symbol: str, kind: str, row: Mapping[str, Any]) -> Optional[FuturesRatio]:
        """One row as a FuturesRatio, or None (logged) if it's malformed."""
        try:
            start = datetime.fromtimestamp(int(row["timestamp"]) / 1000, tz=timezone.utc)
            if kind == "taker_volume":
                values = row["buySellRatio"], row["buyVol"], row["sellVol"]
            else:
                # Top positions' shares may come named as positions rather than accounts.
                values = (
                    row["longShortRatio"],
                    row.get("longAccount", row.get("longPosition")),
                    row.get("shortAccount", row.get("shortPosition")),
                )
            ratio, long_value, short_value = (Decimal(str(value)) for value in values)
        except (KeyError, TypeError, ValueError, InvalidOperation):
            logger.warning("Binance Futures %s %s: skipping a malformed period %s", kind, symbol, dict(row))
            return None
        return FuturesRatio(
            exchange=Exchange.BINANCE,
            symbol=symbol,
            event_time=start,
            kind=kind,
            period=PERIOD,
            contract_type=CONTRACT_TYPE,
            ratio=ratio,
            long_value=long_value,
            short_value=short_value,
        )

    async def _get(self, path: str, params: Mapping[str, Any]) -> Any:
        delay = self._backoff_initial
        attempt = 0
        while True:
            await self.budget.acquire()
            self.requests += 1
            try:
                response = await self._http.get(path, params=params)
            except httpx.TransportError as exc:  # connection errors and timeouts alike
                problem = type(exc).__name__
            else:
                status = response.status_code
                if status == 200:
                    return response.json()
                if status == 418:
                    raise BinanceBanned(_retry_after(response))
                if status == 429:
                    wait = _retry_after(response) or DEFAULT_RETRY_AFTER_SECONDS
                    logger.warning("Binance Futures: rate limited (HTTP 429); pausing every request for %.0f s", wait)
                    self.budget.pause(wait)
                    continue  # Binance named the wait; not a failed attempt
                if status < 500:
                    message, code = _error(response)
                    raise BinanceRequestError(status, message, code)
                problem = f"HTTP {status}"
            attempt += 1
            if attempt >= self._max_attempts:
                raise BinanceUnavailable(f"{path}: {problem}, {attempt} attempts in a row")
            logger.warning("Binance Futures: %s on %s; retrying in %.0f s", problem, path, delay)
            await self._sleep(delay)
            delay = min(delay * 2, self._backoff_max)


def _ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def _retry_after(response: httpx.Response) -> Optional[float]:
    try:
        return float(response.headers["Retry-After"])
    except (KeyError, ValueError):
        return None


def _error(response: httpx.Response) -> Tuple[str, Optional[int]]:
    """Binance's message and code from an error body, as far as it has them."""
    try:
        body = response.json()
    except ValueError:
        return response.text[:200], None
    if isinstance(body, dict) and "msg" in body:
        code = body.get("code")
        return f"{body['msg']} (code {code})", code if isinstance(code, int) else None
    return str(body)[:200], None
