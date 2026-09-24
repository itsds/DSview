"""
candle_aggregator.py

OHLCV candle aggregation for live serving — feature roadmap milestone
toward the lightweight-charts frontend. Reads `market.trades.raw` via
trades_source.read_trades_stream(), tumbling-windows each symbol's trades
via candle_windowing.build_candles() into 1-minute candles, prints each
batch to the console, and writes the latest candle per symbol to Redis
(live push) and Postgres (queryable serving copy for the backend's future
REST historical endpoint) via foreachBatch.

Runs in outputMode("update"): Redis/Postgres want to see a window's
values refine in real time as more trades arrive, not just its final
value. jobs/storage_writer.py's Gold Parquet writer uses outputMode
("append") on the *same* aggregation logic instead, since an append-only
Parquet sink needs exactly one row per finalized window — see that
module's docstring for why these can't share one query.

Structured Streaming has no built-in Redis or Postgres sink, so all three
writes (console, Redis, Postgres) happen inside one foreachBatch — one
streaming query against Kafka, not three.

Run from streaming/ (not repo root, not from inside jobs/) so sibling
modules resolve as bare imports, matching ingestion/'s flat import
convention (see ingestion/main.py's docstring):

    cd streaming && python -m jobs.candle_aggregator

Author: @DS
"""

from __future__ import annotations

from datetime import timezone

from candle_windowing import build_candles
from postgres_sink import CandlePostgresSink
from redis_sink import CandleRedisSink
from spark_session import get_spark_session
from trades_source import read_trades_stream


def _row_to_candle(row) -> dict:
    candle = row.asDict()
    # collect() yields naive datetimes in the driver's local timezone; make them explicit UTC.
    for field in ("window_start", "window_end"):
        candle[field] = candle[field].astimezone(timezone.utc)
    return candle


def build_candle_query(
    spark, redis_sink: CandleRedisSink, postgres_sink: CandlePostgresSink
) -> "pyspark.sql.streaming.StreamingQuery":  # noqa: F821
    """Wire up the read -> parse -> window-aggregate -> console+Redis+Postgres pipeline."""
    candles = build_candles(read_trades_stream(spark))

    def _write_batch(batch_df, batch_id: int) -> None:
        batch_df.show(truncate=False)
        # A batch spanning a minute boundary holds two windows per symbol, and
        # collect() order is arbitrary. Oldest-first keeps the Redis SET holding
        # the newest candle and keeps pub/sub subscribers from seeing time go
        # backwards (lightweight-charts' update() throws on an older bar).
        for row in sorted(batch_df.collect(), key=lambda r: r["window_start"]):
            candle = _row_to_candle(row)
            redis_sink.write_candle(candle)
            postgres_sink.write_candle(candle)

    return (
        candles.writeStream.outputMode("update")
        .foreachBatch(_write_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )


def main() -> None:
    spark = get_spark_session("dsview-candle-aggregator")
    redis_sink = CandleRedisSink()
    postgres_sink = CandlePostgresSink()
    query = build_candle_query(spark, redis_sink, postgres_sink)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        query.stop()
        postgres_sink.close()
        spark.stop()


if __name__ == "__main__":
    main()
