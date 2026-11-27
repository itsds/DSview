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

from pydantic import BaseModel, Field, field_validator

from config.settings import FUTURES_RATIO_KINDS, PIPELINE, Pipeline


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
    FUTURES_RATIO = "futures_ratio"


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


class FuturesRatio(_BaseEvent):
    """
    One period of one of Binance Futures' long/short ratios (Phase 4c), for
    the USDⓈ-M perpetual of the symbol's name: polled over REST by
    futures_poller.py rather than streamed, so no BaseExchangeClient.

    `event_time` is the period's start (Binance's `timestamp`, read as a
    kline's open time is) and `period` its length, "5m" (config/settings.py's
    FUTURES_RATIO_PERIOD). `kind` is one of FUTURES_RATIO_KINDS:

    - global_accounts, top_accounts, top_positions: `ratio` is longs over
      shorts (Binance's longShortRatio), `long_value` and `short_value` the
      shares long and short (0 to 1) of all accounts, of the top traders'
      accounts, or of the top traders' positions;
    - taker_volume: `ratio` is taker buy over sell volume (buySellRatio),
      `long_value` and `short_value` that buy and sell volume, in the base
      asset.

    Numbers are Binance's own decimals, as strings on the wire like every
    other event's.
    """

    event_type: EventType = EventType.FUTURES_RATIO
    kind: str
    period: str = "5m"
    contract_type: str = "PERPETUAL"
    ratio: Decimal
    long_value: Decimal
    short_value: Decimal

    @field_validator("kind")
    @classmethod
    def _known_kind(cls, kind: str) -> str:
        if kind not in FUTURES_RATIO_KINDS:
            raise ValueError(f"unknown ratio {kind!r}: one of {list(FUTURES_RATIO_KINDS)}")
        return kind


# A single WebSocket message can yield zero, one, or more of these; a poll,
# FuturesRatios.
MarketEvent = Union[TradeEvent, DepthUpdate, FuturesRatio]


def kafka_topic_for(event: MarketEvent, pipeline: Pipeline = PIPELINE) -> str:
    """
    Central place that maps an event to its destination Kafka topic, in
    `pipeline` (this process's, by default: live for ingestion, replay for
    replay/'s feed).

    Keeping this here (rather than duplicating the mapping in producer.py
    and in every client) means adding a new event type only requires a
    change in one place. The topic names themselves live in
    config/settings.py, because streaming/'s consumers subscribe to the
    same names and must not import from ingestion/.
    """
    topic_map = {
        EventType.TRADE: pipeline.trades_topic,
        EventType.DEPTH: pipeline.depth_topic,
        EventType.FUTURES_RATIO: pipeline.futures_ratios_topic,
    }
    return topic_map[EventType(event.event_type)]
