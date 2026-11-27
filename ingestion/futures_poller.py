"""
futures_poller.py

Polls Binance Futures' long/short ratios (Phase 4c): for every symbol and
each of the four ratios, every 5-minute period once Binance has published
it, handed to an injected `on_ratios` callback (the same DIP as the
exchange clients' `on_event`: futures_main.py wires it to the producer, so
polling runs without a broker in the tests). A REST poll at each period's
close rather than a socket, so not a BaseExchangeClient.

Each symbol and ratio is followed on its own, from where it left off:

- **Where to start.** `resume_from` holds the start of the newest period
  already published (futures_main.py reads it back from the topic's tail,
  ratio_topic.py), and polling carries on from the period after. With
  none (a first start), it starts HISTORY_DAYS back, which is the
  backfill: Binance keeps only the latest 30 days, so that is everything
  there is. A restart after a longer gap loses what Binance no longer
  has. `refetch_days` starts that many days back whatever was published,
  to restore an emptied table downstream (the consumer's upsert makes the
  repeats harmless).
- **Catching up** pages everything from there to the latest closed
  period, PAGE_LIMIT periods a request, publishing each page as it comes
  (a restart mid-way resumes after the last page sent).
- **Then live:** each period is asked for PUBLISH_DELAY_SECONDS after it
  closes, and again every RETRY_SECONDS until Binance has it. A period
  Binance never publishes is a hole: the next one's arrival moves past it.
  A period still missing after MISSING_WARN_SECONDS is logged once.

Binance being down (BinanceUnavailable, after the client's own retries)
or banning the IP (BinanceBanned) pauses that ratio and tries again later;
nothing is lost while it's inside the 30 days. A request Binance refuses
outright (BinanceRequestError: an unknown symbol, a parameter shape it
takes none of) ends the run: retrying can't fix it.

Pure apart from the client it's given: the clock and sleep are injected,
so the tests run it on a fake clock.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from config.settings import FUTURES_RATIO_KINDS

from binance_futures_rest import (
    PERIOD_MS,
    BinanceBanned,
    BinanceFuturesClient,
    BinanceUnavailable,
)
from schema import FuturesRatio

logger = logging.getLogger(__name__)

DAY_MS = 86_400_000
HISTORY_DAYS = 30  # Binance keeps the latest 30 days
# Kept clear of the 30-day edge, which a request crosses while it travels.
RETENTION_MARGIN_MS = PERIOD_MS
PUBLISH_DELAY_SECONDS = 5.0  # after a period closes, before asking for it
RETRY_SECONDS = 15.0  # until Binance has it
MISSING_WARN_SECONDS = 600.0
OUTAGE_RETRY_SECONDS = 60.0  # after BinanceUnavailable
BAN_RETRY_SECONDS = 600.0  # after a 418 that doesn't say how long

OnRatios = Callable[[List[FuturesRatio]], Awaitable[None]]
Key = Tuple[str, str]  # (symbol, ratio)


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


class FuturesRatioPoller:
    """Follows every (symbol, ratio) from where it left off, handing each page of new periods to `on_ratios`."""

    def __init__(
        self,
        client: BinanceFuturesClient,
        symbols: Sequence[str],
        on_ratios: OnRatios,
        resume_from: Optional[Mapping[Key, int]] = None,
        kinds: Sequence[str] = FUTURES_RATIO_KINDS,
        history_days: float = HISTORY_DAYS,
        refetch_days: Optional[float] = None,
        publish_delay: float = PUBLISH_DELAY_SECONDS,
        retry: float = RETRY_SECONDS,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self._client = client
        self._symbols = list(symbols)
        self._kinds = list(kinds)
        self._on_ratios = on_ratios
        self._resume_from = dict(resume_from or {})
        self._history_ms = int(history_days * DAY_MS)
        self._refetch_ms = None if refetch_days is None else int(refetch_days * DAY_MS)
        self._publish_delay_ms = int(publish_delay * 1000)
        self._retry = retry
        self._clock = clock
        self._sleep = sleep
        self.published: Dict[Key, int] = {}  # per (symbol, ratio), the newest period handed on so far

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    def start_for(self, symbol: str, kind: str) -> int:
        """The first period to ask for (Unix ms), as the module docstring says."""
        now = self._now_ms()
        if self._refetch_ms is not None:
            start = now - self._refetch_ms
        elif (symbol, kind) in self._resume_from:
            start = self._resume_from[(symbol, kind)] + PERIOD_MS
        else:
            start = now - self._history_ms
        oldest = now - HISTORY_DAYS * DAY_MS + RETENTION_MARGIN_MS
        if start < oldest:
            if (symbol, kind) in self._resume_from and self._refetch_ms is None:
                logger.warning(
                    "%s %s: periods from %s to %s are older than Binance keeps, and lost",
                    symbol,
                    kind,
                    _iso(self._resume_from[(symbol, kind)] + PERIOD_MS),
                    _iso(oldest),
                )
            start = oldest
        return -(-start // PERIOD_MS) * PERIOD_MS

    async def run(self) -> None:
        """Poll until cancelled; the first follower to fail for good ends the run, and the others with it."""
        followers = [
            asyncio.create_task(self._follow(symbol, kind), name=f"ratio:{symbol}:{kind}")
            for symbol in self._symbols
            for kind in self._kinds
        ]
        try:
            done, _ = await asyncio.wait(followers, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                task.result()  # raises the failure
        finally:
            for task in followers:
                task.cancel()
            await asyncio.gather(*followers, return_exceptions=True)

    async def _follow(self, symbol: str, kind: str) -> None:
        next_ms = self.start_for(symbol, kind)
        behind = (self._now_ms() - next_ms) // PERIOD_MS
        if behind > 1:
            logger.info("%s %s: catching up from %s (%d periods)", symbol, kind, _iso(next_ms), behind)
        catching_up = behind > 1
        waiting_since: Optional[int] = None  # when the period next_ms became due, while Binance hasn't it
        warned = False
        while True:
            now = self._now_ms()
            due = next_ms + PERIOD_MS + self._publish_delay_ms
            if now < due:
                await self._sleep((due - now) / 1000)
                continue
            try:
                got = await self._fetch(symbol, kind, next_ms, now // PERIOD_MS * PERIOD_MS, catching_up)
            except BinanceUnavailable as exc:
                logger.warning("%s %s: %s; trying again in %.0f s", symbol, kind, exc, OUTAGE_RETRY_SECONDS)
                await self._sleep(OUTAGE_RETRY_SECONDS)
                continue
            except BinanceBanned as exc:
                wait = exc.retry_after or BAN_RETRY_SECONDS
                logger.error("%s %s: %s; waiting %.0f s", symbol, kind, exc, wait)
                await self._sleep(wait)
                continue
            if got is not None:
                if catching_up:
                    logger.info("%s %s: caught up to %s", symbol, kind, _iso(got))
                    catching_up = False
                next_ms = got + PERIOD_MS
                waiting_since, warned = None, False
                continue
            waiting_since = waiting_since or due
            if not warned and now - waiting_since >= MISSING_WARN_SECONDS * 1000:
                warned = True
                logger.warning(
                    "%s %s: Binance hasn't published the %s period %.0f minutes after it closed",
                    symbol,
                    kind,
                    _iso(next_ms),
                    (now - next_ms - PERIOD_MS) / 60_000,
                )
            await self._sleep(self._retry)

    async def _fetch(self, symbol: str, kind: str, start_ms: int, end_ms: int, catching_up: bool) -> Optional[int]:
        """Hand on every period in [start_ms, end_ms); the newest one's start, or None if there was none."""
        newest = None
        async for page in self._client.ratios(symbol, kind, start_ms, end_ms):
            await self._on_ratios(page)
            newest = int(page[-1].event_time.timestamp() * 1000)
            self.published[(symbol, kind)] = newest
            if not catching_up:
                for ratio in page:
                    logger.info("%s %s %s: %s", symbol, kind, ratio.event_time.strftime("%H:%M"), ratio.ratio)
        return newest
