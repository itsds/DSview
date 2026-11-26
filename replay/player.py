"""
player.py

ReplayPlayer: hands a stream of trades (bronze_source.py's tables, oldest
first) to `send` as the replay clock reaches each one. In practice `send`
is MarketDataProducer.send_batch with the replay pipeline's topics, so the
trades reach the replay's Spark jobs exactly as live ones reach live's.

- **Speed** is the clock's (clock.py): every trade whose time the clock
  has passed goes in one batch, then the player waits until the next one
  is due, at most TICK_SECONDS at a time. At MAX it sends as fast as the
  broker acknowledges, BATCH_LIMIT trades at a time, moving the clock to
  each batch's last trade.
- **Gaps:** a trade more than GAP_SKIP ahead of the clock means the
  recording has a hole (the laptop was off), not a quiet market, so the
  clock jumps to it rather than replay the silence in real time.
- **Pause, resume, speed** take effect at once, waking a player that's
  waiting; a paused player holds its place and sends nothing.

Like ingestion's clients, it knows nothing about Kafka: it only calls the
`send` it was given (the same DIP as BaseExchangeClient's `on_event`).

Author: @DS
"""

from __future__ import annotations

import asyncio
import bisect
import logging
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Iterator, List, Optional

import pyarrow as pa

from schema import TradeEvent  # ingestion's, like the producer

from clock import MAX, ReplayClock, Speed

logger = logging.getLogger(__name__)

GAP_SKIP = timedelta(seconds=60)
BATCH_LIMIT = 2_000
TICK_SECONDS = 0.25

Send = Callable[[List[TradeEvent]], Awaitable[None]]


class ReplayPlayer:
    def __init__(self, tables: Iterator[pa.Table], clock: ReplayClock, send: Send, symbol: str) -> None:
        self._tables = tables
        self._clock = clock
        self._send = send
        self._symbol = symbol
        self._wake = asyncio.Event()
        self.trades_sent = 0
        self.last_trade_time: Optional[datetime] = None

    @property
    def clock(self) -> ReplayClock:
        return self._clock

    def play(self) -> None:
        self._clock.play()
        self._wake.set()

    def pause(self) -> None:
        self._clock.pause()
        self._wake.set()

    def set_speed(self, speed: Speed) -> None:
        self._clock.set_speed(speed)
        self._wake.set()

    async def run(self) -> None:
        """Play every trade, then return. Raises whatever `send` raises."""
        while True:
            # Reading is blocking file I/O: off the event loop, so commands stay responsive.
            table = await asyncio.to_thread(next, self._tables, None)
            if table is None:
                return
            if table.num_rows:
                await self._play_table(table)

    async def _play_table(self, table: pa.Table) -> None:
        rows = table.to_pylist()
        times = [row["event_time"] for row in rows]
        i = 0
        while i < len(rows):
            if not self._clock.playing:
                await self._wait(None)
                continue
            end = min(len(rows), i + BATCH_LIMIT)
            if self._clock.speed != MAX:
                now = self._clock.time()
                due = times[i]
                if due > now:
                    if due - now > GAP_SKIP:
                        self._clock.advance_to(due)
                    else:
                        await self._wait(min(self._clock.wall_seconds_until(due) or 0.0, TICK_SECONDS))
                    continue
                end = bisect.bisect_right(times, now, i, end)
            await self._send(self._events(rows[i:end]))
            self.trades_sent += end - i
            self.last_trade_time = times[end - 1]
            if self._clock.speed == MAX:
                self._clock.advance_to(times[end - 1])
                await asyncio.sleep(0)  # let commands in between batches
            i = end

    async def _wait(self, timeout: Optional[float]) -> None:
        """Until a command wakes the player, or `timeout` seconds (None: no limit)."""
        try:
            await asyncio.wait_for(self._wake.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        self._wake.clear()

    def _events(self, rows: List[dict]) -> List[TradeEvent]:
        # Bronze's rows were validated when ingestion first produced them, so
        # skip pydantic's validation (model_construct): at MAX speed it would
        # be most of the cost. The replay stamps its own ingested_at.
        now = datetime.now(timezone.utc)
        return [
            TradeEvent.model_construct(
                exchange=row["exchange"],
                symbol=self._symbol,
                event_time=row["event_time"],
                ingested_at=now,
                trade_id=row["trade_id"],
                price=row["price"],
                quantity=row["quantity"],
                side=row["side"],
            )
            for row in rows
        ]

