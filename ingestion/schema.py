"""
schema.py

Canonical data models for DSView's ingestion layer.

Every exchange adapter (Binance, Coinbase, Kraken, ...) is responsible for
normalizing its own wire format into these models before the event is
handed off to the Kafka producer. Nothing downstream (Kafka, Spark,
FastAPI, frontend) should ever need to know which exchange an event
originated from beyond the `exchange` field below.

Author: @DS
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import List, Union

from pydantic import BaseModel, Field


class Exchange(str, Enum):
    """Supported upstream data sources. Extend as new adapters are added."""

    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"


class Side(str, Enum):
    """Which side of the book a trade was executed on (taker side)."""

    BUY = "buy"
    SELL = "sell"


class EventType(str, Enum):
    """Discriminator used for Kafka topic routing and downstream branching."""

    TRADE = "trade"
    DEPTH = "depth"


class _BaseEvent(BaseModel):
    """
    Common fields shared by every normalized market event.

    `event_time` is the timestamp assigned by the exchange (when the trade
    actually happened / the book snapshot was generated). `ingested_at` is
    stamped locally and exists purely for latency monitoring — it should
    never be used for windowed aggregations in Spark.
    """

    exchange: Exchange
    symbol: str
    event_time: datetime
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        use_enum_values = True
        frozen = True  # events are immutable once constructed


class TradeEvent(_BaseEvent):
    """A single executed trade, normalized across exchanges."""

    event_type: EventType = EventType.TRADE
    trade_id: str
    price: Decimal
    quantity: Decimal
    side: Side


class DepthLevel(BaseModel):
    """One price level in an order book snapshot/update."""

    price: Decimal
    quantity: Decimal


class DepthUpdate(_BaseEvent):
    """A partial order book snapshot/update, normalized across exchanges."""

    event_type: EventType = EventType.DEPTH
    bids: List[DepthLevel]
    asks: List[DepthLevel]


# A single WebSocket message can yield zero, one, or more of these.
MarketEvent = Union[TradeEvent, DepthUpdate]


def kafka_topic_for(event: MarketEvent) -> str:
    """
    Central place that maps an event to its destination Kafka topic.

    Keeping this here (rather than duplicating the mapping in producer.py
    and in every client) means adding a new event type only requires a
    change in one place.
    """
    topic_map = {
        EventType.TRADE: "market.trades.raw",
        EventType.DEPTH: "market.depth.raw",
    }
    return topic_map[EventType(event.event_type)]
