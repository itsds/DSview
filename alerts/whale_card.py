"""
whale_card.py

Turns whale alerts into Telegram message text: one three-line card per
whale, the format chosen at Phase 1's kickoff (docs/ROADMAP.md):

    🐋 BUY · BTCUSDT
    $242.5K — 2.5 @ 97,000.10
    Binance · 10:16:57 UTC

A message holding several whales (see whale_batcher.py) is their cards in
the order they traded, separated by blank lines.

Numbers read the way the right panel shows them
(frontend/src/components/layout/RightPanel.jsx): the notional compact
("$242.5K", "$1.2M", at most one decimal, rounding half up like the
browser's Intl.NumberFormat), the quantity as traded without trailing
zeros, and the time in UTC from the trade's own timestamp. Prices keep at
least two decimals and more only where the price has them, so 97000.1
reads "97,000.10" while a sub-cent price isn't rounded away.

Pure functions over the alert dict whale_detector.py publishes (its
docstring has the shape); nothing here touches Kafka or Telegram.

Author: @DS
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Mapping, Optional, Sequence

WhaleAlert = Mapping[str, Any]

EXCHANGE_NAMES = {"binance": "Binance", "coinbase": "Coinbase", "kraken": "Kraken"}

# Smallest first: (divisor, suffix).
_COMPACT_UNITS = [(Decimal(1), ""), (Decimal(10) ** 3, "K"), (Decimal(10) ** 6, "M"), (Decimal(10) ** 9, "B"), (Decimal(10) ** 12, "T")]
_ONE_DECIMAL = Decimal("0.1")
_QUANTITY_STEP = Decimal("0.00000001")  # Binance's smallest lot step; the panel shows up to 8 decimals too

# Matches an ISO 8601 time's fractional seconds, whatever their length.
_FRACTION = re.compile(r"\.(\d+)")


def format_card(alert: WhaleAlert) -> str:
    """One whale's three-line card."""
    side = str(alert.get("side") or "?").upper()
    symbol = alert.get("symbol") or "?"
    time = parse_time(alert.get("event_time")) or parse_time(alert.get("detected_at"))
    exchange = alert.get("exchange") or "?"
    where_when = EXCHANGE_NAMES.get(exchange, str(exchange).title())
    if time is not None:
        where_when += f" · {time:%H:%M:%S} UTC"
    return "\n".join(
        [
            f"🐋 {side} · {symbol}",
            f"{format_usd(alert.get('notional'))} — {format_quantity(alert.get('quantity'))} @ {format_price(alert.get('price'))}",
            where_when,
        ]
    )


def format_message(alerts: Sequence[WhaleAlert]) -> str:
    """Several whales' cards as one message, blank-line separated, in the order given."""
    return "\n\n".join(format_card(alert) for alert in alerts)


def format_usd(value: Any) -> str:
    """$242.5K, $1.2M, $999.5: compact, at most one decimal, half up."""
    amount = _decimal(value)
    if amount is None:
        return "$?"
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    # The largest unit the amount reaches, then one up if rounding carried
    # it to 1000 of that unit: 999.96K reads $1M, as in the browser.
    index = max((i for i, (size, _) in enumerate(_COMPACT_UNITS) if amount >= size), default=0)
    scaled = (amount / _COMPACT_UNITS[index][0]).quantize(_ONE_DECIMAL, rounding=ROUND_HALF_UP)
    if scaled >= 1000 and index + 1 < len(_COMPACT_UNITS):
        index += 1
        scaled = (amount / _COMPACT_UNITS[index][0]).quantize(_ONE_DECIMAL, rounding=ROUND_HALF_UP)
    return f"{sign}${_plain(scaled)}{_COMPACT_UNITS[index][1]}"


def format_quantity(value: Any) -> str:
    """2.5, 1,500, 0.00012: as traded, grouped, no trailing zeros."""
    amount = _decimal(value)
    if amount is None:
        return "?"
    return f"{_plain(amount.quantize(_QUANTITY_STEP, rounding=ROUND_HALF_UP)):,f}"


def format_price(value: Any) -> str:
    """97,000.10, 0.00001234: grouped, at least two decimals, more only if the price has them."""
    amount = _decimal(value)
    if amount is None:
        return "?"
    decimals = max(2, -amount.normalize().as_tuple().exponent)
    return f"{amount:,.{decimals}f}"


def parse_time(value: Any) -> Optional[datetime]:
    """
    An alert timestamp as an aware UTC datetime, or None.

    Accepts what the pipeline writes: ISO 8601 with "Z" or an offset, any
    number of fractional digits. Python 3.10's fromisoformat takes neither
    "Z" nor fractions other than 3 or 6 digits, so both are normalized first.
    """
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    text = _FRACTION.sub(lambda m: "." + (m.group(1) + "000000")[:6], text, count=1)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)  # a naive time means UTC, like everywhere else here
    return parsed.astimezone(timezone.utc)


def _decimal(value: Any) -> Optional[Decimal]:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return amount if amount.is_finite() else None


def _plain(amount: Decimal) -> Decimal:
    """`amount` without trailing zeros and never in exponent form (1E+2 -> 100)."""
    normalized = amount.normalize()
    return normalized.quantize(Decimal(1)) if normalized.as_tuple().exponent > 0 else normalized
