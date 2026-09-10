"""
binance_client.py

Binance concrete implementation of BaseExchangeClient.

Uses Binance's combined-stream endpoint so a single connection carries
both trade and partial-depth updates for every subscribed symbol — no
explicit SUBSCRIBE message is required, which keeps this adapter simple.

Reference: https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams

Author: @DS
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

from base_client import BaseExchangeClient
from schema import DepthLevel, DepthUpdate, Exchange, Side, TradeEvent, MarketEvent

logger = logging.getLogger(__name__)


class BinanceClient(BaseExchangeClient):
    """Ingests trade + partial depth streams for a set of symbols from Binance."""

    _COMBINED_STREAM_BASE = "wss://stream.binance.com:9443/stream"
    _DEPTH_LEVELS = 20
    _DEPTH_UPDATE_SPEED_MS = 100

    @property
    def ws_url(self) -> str:
        streams = []
        for symbol in self.symbols:
            lower = symbol.lower()
            streams.append(f"{lower}@trade")
            streams.append(f"{lower}@depth{self._DEPTH_LEVELS}@{self._DEPTH_UPDATE_SPEED_MS}ms")
        return f"{self._COMBINED_STREAM_BASE}?streams={'/'.join(streams)}"

    def build_subscription_message(self) -> Optional[str]:
        # Combined-stream URL already encodes subscriptions; nothing to send.
        return None

    async def parse_message(self, raw_message: str) -> List[MarketEvent]:
        envelope = json.loads(raw_message)
        stream = envelope.get("stream", "")
        payload = envelope.get("data")
        if payload is None:
            logger.debug("Binance: message with no data field, skipping: %s", envelope)
            return []

        if stream.endswith("@trade"):
            return [self._to_trade_event(payload)]

        if "@depth" in stream:
            # Partial depth payloads carry no symbol field, so it must be
            # recovered from the stream name itself, e.g. "btcusdt@depth20@100ms".
            symbol = stream.split("@", 1)[0].upper()
            return [self._to_depth_update(symbol, payload)]

        logger.debug("Binance: unrecognized stream '%s', skipping", stream)
        return []

    # ------------------------------------------------------------------ #
    # Wire-format -> canonical schema translation
    # ------------------------------------------------------------------ #

    @staticmethod
    def _to_trade_event(payload: dict) -> TradeEvent:
        # Binance trade payload: https://binance-docs.github.io/apidocs/spot/en/#trade-streams
        # "m": True means the buyer is the market maker -> taker side was SELL.
        taker_side = Side.SELL if payload["m"] else Side.BUY
        return TradeEvent(
            exchange=Exchange.BINANCE,
            symbol=payload["s"],
            event_time=datetime.fromtimestamp(payload["T"] / 1000, tz=timezone.utc),
            trade_id=str(payload["t"]),
            price=Decimal(payload["p"]),
            quantity=Decimal(payload["q"]),
            side=taker_side,
        )

    @staticmethod
    def _to_depth_update(symbol: str, payload: dict) -> DepthUpdate:
        # Binance partial depth payload has no symbol field or event
        # timestamp, so both are supplied by the caller / stamped on receipt.
        return DepthUpdate(
            exchange=Exchange.BINANCE,
            symbol=symbol,
            event_time=datetime.now(timezone.utc),
            bids=[DepthLevel(price=Decimal(p), quantity=Decimal(q)) for p, q in payload["bids"]],
            asks=[DepthLevel(price=Decimal(p), quantity=Decimal(q)) for p, q in payload["asks"]],
        )
