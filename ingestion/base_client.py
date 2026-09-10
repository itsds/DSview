"""
base_client.py

Abstract base for exchange WebSocket ingestion clients.

Design principles applied here:
  - Single Responsibility: this class owns connection lifecycle, reconnect
    backoff, and message dispatch. It knows nothing about Kafka, Spark, or
    any specific exchange's wire format.
  - Open/Closed: new exchanges are added by subclassing and implementing
    the three abstract members below — this file never needs to change.
  - Dependency Inversion: rather than importing the Kafka producer
    directly, this class depends on an injected `on_event` callback. That
    keeps ingestion testable in isolation (just pass a stub callback) and
    keeps producer.py free to change its interface independently.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Awaitable, Callable, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from schema import MarketEvent

OnEventCallback = Callable[[MarketEvent], Awaitable[None]]

logger = logging.getLogger(__name__)


class BaseExchangeClient(ABC):
    """
    Common skeleton for a single-exchange WebSocket ingestion adapter.

    Subclasses must implement:
      - ws_url: the endpoint to connect to (may already encode subscribed
        streams, as Binance's combined-stream URL does)
      - build_subscription_message: return a message to send immediately
        after connecting, or None if the URL itself handles subscription
      - parse_message: normalize one raw WebSocket frame into zero or more
        MarketEvent instances
    """

    def __init__(
        self,
        symbols: List[str],
        on_event: OnEventCallback,
        max_reconnect_delay: float = 60.0,
    ) -> None:
        if not symbols:
            raise ValueError("At least one symbol must be provided")
        self.symbols = [s.upper() for s in symbols]
        self._on_event = on_event
        self._max_reconnect_delay = max_reconnect_delay
        self._running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    # ------------------------------------------------------------------ #
    # Contract each exchange adapter must fulfil
    # ------------------------------------------------------------------ #

    @property
    @abstractmethod
    def ws_url(self) -> str:
        """Endpoint this client connects to."""
        raise NotImplementedError

    @abstractmethod
    def build_subscription_message(self) -> Optional[str]:
        """
        Return a JSON string to send right after connecting, or None if
        no explicit subscribe step is required (e.g. combined-stream URLs).
        """
        raise NotImplementedError

    @abstractmethod
    async def parse_message(self, raw_message: str) -> List[MarketEvent]:
        """
        Convert one raw WebSocket text frame into normalized MarketEvents.
        Should return an empty list for heartbeats/unrecognized frames
        rather than raising, so one bad frame can't kill the connection.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Shared connection lifecycle — subclasses should not need to touch this
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Run the client until `stop()` is called, reconnecting with
        exponential backoff on any connection failure."""
        self._running = True
        delay = 1.0
        while self._running:
            try:
                await self._run_once()
                delay = 1.0  # reset backoff after a clean session
            except (ConnectionClosed, OSError) as exc:
                if not self._running:
                    break
                logger.warning(
                    "%s: connection lost (%s); reconnecting in %.1fs",
                    self.__class__.__name__,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_reconnect_delay)
            except Exception:
                logger.exception(
                    "%s: unexpected error in ingestion loop", self.__class__.__name__
                )
                if not self._running:
                    break
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_reconnect_delay)

    async def stop(self) -> None:
        """Signal the client to stop and close the underlying socket."""
        self._running = False
        if self._ws is not None:
            await self._ws.close()

    async def _run_once(self) -> None:
        """Single connect -> subscribe -> consume session."""
        logger.info("%s: connecting to %s", self.__class__.__name__, self.ws_url)
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
            self._ws = ws
            subscribe_msg = self.build_subscription_message()
            if subscribe_msg is not None:
                await ws.send(subscribe_msg)
                logger.debug("%s: sent subscription %s", self.__class__.__name__, subscribe_msg)

            async for raw_message in ws:
                try:
                    events = await self.parse_message(raw_message)
                except Exception:
                    logger.exception(
                        "%s: failed to parse message, skipping frame",
                        self.__class__.__name__,
                    )
                    continue

                for event in events:
                    await self._on_event(event)
