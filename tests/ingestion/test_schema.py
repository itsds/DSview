"""Tests for ingestion/schema.py: the canonical event contract and its topic map."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pydantic
import pytest

from config.settings import DEPTH_TOPIC, FUTURES_RATIOS_TOPIC, REPLAY_PIPELINE, TRADES_TOPIC

from schema import DepthLevel, DepthUpdate, Exchange, FuturesRatio, Side, TradeEvent, kafka_topic_for

EVENT_TIME = datetime(2026, 11, 20, 10, 16, 57, 123000, tzinfo=timezone.utc)


def _trade() -> TradeEvent:
    return TradeEvent(
        exchange=Exchange.BINANCE,
        symbol="BTCUSDT",
        event_time=EVENT_TIME,
        trade_id="42",
        price=Decimal("97000.10"),
        quantity=Decimal("2.5"),
        side=Side.BUY,
    )


def test_trade_serializes_to_the_wire_shape_consumers_parse():
    payload = _trade().model_dump(mode="json")

    # streaming/schemas.py and whale_detector.py read exactly these fields.
    assert set(payload) == {
        "exchange", "symbol", "event_time", "ingested_at", "event_type", "trade_id", "price", "quantity", "side",
    }
    assert payload["exchange"] == "binance"
    assert payload["event_type"] == "trade"
    assert payload["side"] == "buy"
    # Decimals travel as strings, so no consumer loses precision to a float.
    assert payload["price"] == "97000.10"
    assert payload["quantity"] == "2.5"
    assert datetime.fromisoformat(payload["event_time"].replace("Z", "+00:00")) == EVENT_TIME
    json.dumps(payload)  # plain JSON all the way down


def test_events_are_immutable():
    with pytest.raises(pydantic.ValidationError):
        _trade().price = Decimal("1")


def test_trade_rejects_an_unknown_side():
    with pytest.raises(pydantic.ValidationError):
        TradeEvent(
            exchange="binance", symbol="BTCUSDT", event_time=EVENT_TIME, trade_id="1",
            price=Decimal("1"), quantity=Decimal("1"), side="hold",
        )


def test_each_event_type_maps_to_its_topic():
    depth = DepthUpdate(
        exchange=Exchange.BINANCE,
        symbol="BTCUSDT",
        event_time=EVENT_TIME,
        bids=[DepthLevel(price=Decimal("1"), quantity=Decimal("2"))],
        asks=[],
    )
    assert kafka_topic_for(_trade()) == TRADES_TOPIC
    assert kafka_topic_for(depth) == DEPTH_TOPIC


def _ratio(**overrides) -> FuturesRatio:
    fields = dict(
        exchange=Exchange.BINANCE,
        symbol="BTCUSDT",
        event_time=datetime(2026, 11, 20, 10, 15, tzinfo=timezone.utc),
        kind="global_accounts",
        ratio=Decimal("1.9234"),
        long_value=Decimal("0.6579"),
        short_value=Decimal("0.3421"),
    )
    fields.update(overrides)
    return FuturesRatio(**fields)


def test_a_futures_ratio_serializes_to_the_wire_shape_the_consumer_parses():
    wire = json.loads(json.dumps(_ratio().model_dump(mode="json")))
    assert {k: wire[k] for k in ("event_type", "symbol", "event_time", "kind", "period", "contract_type")} == {
        "event_type": "futures_ratio",
        "symbol": "BTCUSDT",
        "event_time": "2026-11-20T10:15:00Z",
        "kind": "global_accounts",
        "period": "5m",
        "contract_type": "PERPETUAL",
    }
    assert (wire["ratio"], wire["long_value"], wire["short_value"]) == ("1.9234", "0.6579", "0.3421")


def test_a_futures_ratio_of_an_unknown_kind_is_refused():
    with pytest.raises(pydantic.ValidationError, match="unknown ratio"):
        _ratio(kind="funding_rate")


def test_futures_ratios_go_to_their_own_topic():
    assert kafka_topic_for(_ratio()) == FUTURES_RATIOS_TOPIC == "market.futures.ratios"
    assert kafka_topic_for(_ratio(), REPLAY_PIPELINE) == "replay.market.futures.ratios"
