"""Tests for alerts/whale_card.py: the Telegram card format decided at Phase 1's kickoff."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from whale_card import format_card, format_message, format_price, format_quantity, format_usd, parse_time


def _alert(**overrides):
    # Exactly what whale_detector.py publishes for this trade.
    alert = {
        "event_type": "whale_trade",
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "trade_id": "5123456789",
        "side": "buy",
        "price": "97000.10000000",
        "quantity": "2.50000000",
        "notional": "242500.25",
        "threshold": "100000",
        "event_time": "2026-11-20T10:16:57.123000Z",
        "detected_at": "2026-11-20T10:16:57.301822+00:00",
    }
    alert.update(overrides)
    return alert


def test_card_matches_the_decided_format_exactly():
    assert format_card(_alert()) == "🐋 BUY · BTCUSDT\n$242.5K — 2.5 @ 97,000.10\nBinance · 10:16:57 UTC"


def test_sell_side_and_other_exchanges():
    card = format_card(_alert(side="sell", exchange="kraken", symbol="ETHUSD"))
    assert card.splitlines()[0] == "🐋 SELL · ETHUSD"
    assert card.splitlines()[2] == "Kraken · 10:16:57 UTC"


def test_time_falls_back_to_detection_then_to_none():
    assert format_card(_alert(event_time=None)).endswith("Binance · 10:16:57 UTC")
    assert format_card(_alert(event_time="garbage", detected_at=None)).splitlines()[2] == "Binance"


def test_several_whales_are_one_message_of_cards():
    message = format_message([_alert(), _alert(side="sell", notional="1500000", quantity="15.4", price="97402.5")])
    first, second = message.split("\n\n")
    assert first == format_card(_alert())
    assert second == "🐋 SELL · BTCUSDT\n$1.5M — 15.4 @ 97,402.50\nBinance · 10:16:57 UTC"


def test_twenty_long_cards_stay_far_below_telegrams_limit():
    big = _alert(symbol="XAUUSDTPERPETUAL", quantity="123456789.12345678", price="123456789.12345678", notional="999999999999")
    assert len(format_message([big] * 20)) < 4096


@pytest.mark.parametrize(
    "notional, shown",
    [
        ("242500.25", "$242.5K"),
        ("100000", "$100K"),
        ("100049.99", "$100K"),
        ("100050", "$100.1K"),  # half up, like Intl.NumberFormat
        ("999949", "$999.9K"),
        ("999950", "$1M"),  # 1000.0K carries into the next unit
        ("1234567", "$1.2M"),
        ("12345678", "$12.3M"),
        ("2500000000", "$2.5B"),
        ("999.96", "$1K"),
        ("950", "$950"),
        ("not a number", "$?"),
    ],
)
def test_notional_is_compact_like_the_right_panel(notional, shown):
    assert format_usd(notional) == shown


@pytest.mark.parametrize(
    "quantity, shown",
    [("2.50000000", "2.5"), ("1500.00000000", "1,500"), ("0.00012000", "0.00012"), ("100", "100")],
)
def test_quantity_is_as_traded_without_trailing_zeros(quantity, shown):
    assert format_quantity(quantity) == shown


@pytest.mark.parametrize(
    "price, shown",
    [("97000.10000000", "97,000.10"), ("97000", "97,000.00"), ("3500.25", "3,500.25"), ("0.00001234", "0.00001234")],
)
def test_price_keeps_two_decimals_and_more_only_when_it_has_them(price, shown):
    assert format_price(price) == shown


@pytest.mark.parametrize(
    "text",
    [
        "2026-11-20T10:16:57Z",  # what pydantic writes for a whole second
        "2026-11-20T10:16:57.123000Z",
        "2026-11-20T10:16:57.1Z",
        "2026-11-20T10:16:57.123456789Z",
        "2026-11-20T15:46:57+05:30",
        "2026-11-20T10:16:57",  # naive means UTC
    ],
)
def test_parse_time_reads_every_shape_the_pipeline_writes_on_python_3_10(text):
    parsed = parse_time(text)
    assert parsed.replace(microsecond=0) == datetime(2026, 11, 20, 10, 16, 57, tzinfo=timezone.utc)
