"""Tests for ingestion/binance_client.py: Binance frames in, canonical events out."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

from binance_client import BinanceClient
from schema import DepthUpdate, TradeEvent


async def _ignore(event) -> None:
    pass


def _client(symbols=("BTCUSDT",)) -> BinanceClient:
    return BinanceClient(symbols=list(symbols), on_event=_ignore)


def _trade_frame(buyer_is_maker: bool) -> str:
    # The shape of a real btcusdt@trade frame on the combined stream.
    return json.dumps({
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade", "E": 1763633817124, "s": "BTCUSDT", "t": 5123456789,
            "p": "97000.10000000", "q": "2.50000000", "T": 1763633817123, "m": buyer_is_maker, "M": True,
        },
    })


def test_url_subscribes_to_trades_and_depth_for_every_symbol():
    url = _client(["btcusdt", "ETHUSDT"]).ws_url
    streams = url.split("?streams=", 1)[1].split("/")
    assert streams == ["btcusdt@trade", "btcusdt@depth20@100ms", "ethusdt@trade", "ethusdt@depth20@100ms"]
    assert _client().build_subscription_message() is None  # the URL already subscribes


async def test_trade_frame_becomes_a_trade_event():
    [event] = await _client().parse_message(_trade_frame(buyer_is_maker=False))

    assert isinstance(event, TradeEvent)
    assert event.exchange == "binance"
    assert event.symbol == "BTCUSDT"
    assert event.trade_id == "5123456789"
    assert event.price == Decimal("97000.10000000")
    assert event.quantity == Decimal("2.50000000")
    assert event.event_time == datetime(2025, 11, 20, 10, 16, 57, 123000, tzinfo=timezone.utc)


async def test_taker_side_comes_from_the_buyer_is_maker_flag():
    [taker_bought] = await _client().parse_message(_trade_frame(buyer_is_maker=False))
    [taker_sold] = await _client().parse_message(_trade_frame(buyer_is_maker=True))
    assert taker_bought.side == "buy"
    assert taker_sold.side == "sell"


async def test_depth_frame_takes_its_symbol_from_the_stream_name():
    frame = json.dumps({
        "stream": "ethusdt@depth20@100ms",
        "data": {"lastUpdateId": 1, "bids": [["3500.1", "1.5"], ["3500.0", "2"]], "asks": [["3500.2", "0.7"]]},
    })
    [event] = await _client(["ETHUSDT"]).parse_message(frame)

    assert isinstance(event, DepthUpdate)
    assert event.symbol == "ETHUSDT"
    assert [(level.price, level.quantity) for level in event.bids] == [
        (Decimal("3500.1"), Decimal("1.5")), (Decimal("3500.0"), Decimal("2")),
    ]
    assert len(event.asks) == 1


async def test_unknown_or_empty_frames_yield_nothing():
    client = _client()
    assert await client.parse_message(json.dumps({"stream": "btcusdt@kline_1m", "data": {}})) == []
    assert await client.parse_message(json.dumps({"result": None, "id": 1})) == []
