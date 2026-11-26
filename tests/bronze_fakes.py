"""
Bronze written by hand for the replay tests: the way the live pipeline and
the backfill write it, in a temporary data root.

`SinkWriter` writes a Spark file sink's layout by hand: Parquet files with
INT96 timestamps (what Spark writes; no statistics), and the sink's log in
`_spark_metadata`, one file per batch, folded into a `.compact` every ten.
Real Spark output is checked against the same reader in the end-to-end test.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

COMPACT_INTERVAL = 10

LIVE_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("event_time", pa.timestamp("us")),  # naive, UTC in fact: Spark's INT96 reads back this way
        ("ingested_at", pa.string()),
        ("event_type", pa.string()),
        ("trade_id", pa.string()),
        ("price", pa.decimal128(38, 18)),
        ("quantity", pa.decimal128(38, 18)),
        ("side", pa.string()),
    ]
)

Trade = Tuple[datetime, str, str, str, str]  # event_time, trade_id, price, quantity, side


def trade_rows(trades: Iterable[Trade]) -> pa.Table:
    trades = list(trades)
    return pa.table(
        {
            "exchange": ["binance"] * len(trades),
            "event_time": [t[0].astimezone(timezone.utc).replace(tzinfo=None) for t in trades],
            "ingested_at": [t[0].isoformat() for t in trades],
            "event_type": ["trade"] * len(trades),
            "trade_id": [t[1] for t in trades],
            "price": [Decimal(t[2]) for t in trades],
            "quantity": [Decimal(t[3]) for t in trades],
            "side": [t[4] for t in trades],
        },
        schema=LIVE_SCHEMA,
    )


class SinkWriter:
    """bronze/trades in `data_root`, written batch by batch like storage_writer.py's Bronze query."""

    def __init__(self, data_root: Path) -> None:
        self.dir = data_root / "bronze" / "trades"
        self.log = self.dir / "_spark_metadata"
        self.log.mkdir(parents=True, exist_ok=True)
        self._batches: List[List[Dict]] = []

    def batch(self, symbol: str, trades: Iterable[Trade], written: Optional[datetime] = None, corrupt: bool = False) -> Path:
        """Commit one batch: one file for `symbol` (written at `written`, default its last trade + 1 s)."""
        trades = list(trades)
        path = self.file(symbol, trades, corrupt)
        written = written or (max(t[0] for t in trades) + timedelta(seconds=1))
        entry = {
            "path": path.as_uri(),
            "size": path.stat().st_size,
            "isDir": False,
            "modificationTime": int(written.timestamp() * 1000),
            "blockReplication": 1,
            "blockSize": 33554432,
            "action": "add",
        }
        self._batches.append([entry])
        batch_id = len(self._batches) - 1
        if (batch_id + 1) % COMPACT_INTERVAL == 0:
            everything = [e for b in self._batches for e in b]
            (self.log / f"{batch_id}.compact").write_text("v1\n" + "\n".join(json.dumps(e) for e in everything))
        else:
            (self.log / str(batch_id)).write_text("v1\n" + json.dumps(entry))
        (self.log / f".{batch_id}.crc").write_bytes(b"crc")  # Hadoop's checksum files sit alongside
        return path

    def file(self, symbol: str, trades: Iterable[Trade], corrupt: bool = False) -> Path:
        """A Parquet file in the partition, but in no batch: what a failed batch leaves behind."""
        partition = self.dir / f"symbol={symbol}"
        partition.mkdir(parents=True, exist_ok=True)
        path = partition / f"part-00000-{uuid.uuid4()}.c000.snappy.parquet"
        if corrupt:
            path.write_bytes(b"not parquet: reading this file fails the test")
        else:
            pq.write_table(trade_rows(trades), path, use_deprecated_int96_timestamps=True)
        return path


T0 = datetime(2026, 11, 20, 10, 0, tzinfo=timezone.utc)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def write_agg_trades(data_root: Path, symbol: str, start_ms: int, end_ms: int, market=None) -> List[dict]:
    """SyntheticMarket's aggTrades in [start_ms, end_ms) as the backfill writes them (bronze_files.py); returns them."""
    from bronze_files import AGG_TRADES, page_table
    from fake_binance import SyntheticMarket

    from binance_rest import AGG_TRADE_COLUMNS

    market = market or SyntheticMarket()
    trades = list(market.trades_between(symbol, start_ms, end_ms))
    by_day: Dict[str, List[dict]] = {}
    for trade in trades:
        day = datetime.fromtimestamp(trade["T"] / 1000, timezone.utc).date().isoformat()
        by_day.setdefault(day, []).append(trade)
    for day, day_trades in by_day.items():
        page = {column: [] for column in AGG_TRADE_COLUMNS}
        for t in day_trades:
            page["agg_trade_id"].append(t["a"])
            page["event_time"].append(t["T"])
            page["price"].append(t["p"])
            page["quantity"].append(t["q"])
            page["side"].append("sell" if t["m"] else "buy")
            page["first_trade_id"].append(t["f"])
            page["last_trade_id"].append(t["l"])
            page["is_best_match"].append(t["M"])
            page["ingested_at"].append(t["T"])
        path = data_root / "bronze" / "agg_trades" / f"symbol={symbol}" / f"date={day}" / "agg_trades.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(page_table(AGG_TRADES, page), path)
    return trades
