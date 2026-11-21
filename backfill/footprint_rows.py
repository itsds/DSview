"""
footprint_rows.py

The footprint's history (Phase 3b): a day of Bronze aggTrades turned into
footprint_1m rows, buy and sell volume per price level per minute. The
same bucketing streaming/footprint_windowing.py applies to live trades in
Spark, done here in pyarrow because the backfill is plain Python: a day's
million aggTrades take well under a second, with no JVM to start.

- **Minute**: the aggregate trade's time floored to the minute (UTC).
- **Level**: floor(price / FOOTPRINT_PRICE_STEP) * FOOTPRINT_PRICE_STEP,
  so with the $1 step a trade at 97,000.40 is on the 97,000 row.
- **Side**: the taker's, as Bronze already stores it (binance_rest.py).

An aggregate trade is every fill of one taker order at one price, so
summing them per level gives exactly the volume the individual trades
would: a minute's levels add up to its kline's volume, and so to the
candle candles_1m holds for it.

Author: @DS
"""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc

from config.settings import FOOTPRINT_PRICE_STEP

_DECIMAL = pa.decimal128(38, 18)  # Bronze's and footprint_1m's

FOOTPRINT_SCHEMA = pa.schema(
    [
        ("window_start", pa.timestamp("us", tz="UTC")),
        ("price", _DECIMAL),
        ("buy_volume", _DECIMAL),
        ("sell_volume", _DECIMAL),
    ]
)


def _levels(prices: pa.ChunkedArray, step: Decimal) -> pa.Array:
    """Each price's level. Per distinct price, in Python's exact Decimal: Arrow's decimal division overflows at (38, 18)."""
    distinct = pc.unique(prices)
    levels = pa.array([(price // step) * step for price in distinct.to_pylist()], _DECIMAL)  # prices are positive: // floors
    return pc.take(levels, pc.index_in(prices, value_set=distinct))


def footprint_table(agg_trades: pa.Table, step: Decimal = FOOTPRINT_PRICE_STEP) -> pa.Table:
    """
    footprint rows (FOOTPRINT_SCHEMA) from a table in Bronze's aggTrades
    schema (bronze_files.AGG_TRADES_SCHEMA; event_time, price, quantity and
    side are used), ordered by minute, then price.
    """
    if agg_trades.num_rows == 0:
        return FOOTPRINT_SCHEMA.empty_table()
    zero = pa.scalar(Decimal(0), _DECIMAL)
    is_buy = pc.equal(agg_trades["side"], "buy")
    quantity = agg_trades["quantity"].cast(_DECIMAL)
    rows = pa.table(
        {
            "window_start": pc.floor_temporal(agg_trades["event_time"], unit="minute"),
            "price": _levels(agg_trades["price"].cast(_DECIMAL), step),
            "buy_volume": pc.if_else(is_buy, quantity, zero),
            "sell_volume": pc.if_else(is_buy, zero, quantity),
        }
    )
    summed = rows.group_by(["window_start", "price"]).aggregate([("buy_volume", "sum"), ("sell_volume", "sum")])
    table = pa.table(
        {
            "window_start": summed["window_start"],
            "price": summed["price"],
            "buy_volume": summed["buy_volume_sum"].cast(_DECIMAL),
            "sell_volume": summed["sell_volume_sum"].cast(_DECIMAL),
        },
        schema=FOOTPRINT_SCHEMA,
    )
    return table.sort_by([("window_start", "ascending"), ("price", "ascending")])
