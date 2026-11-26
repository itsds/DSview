"""
candle_aggregator.py

OHLCV candle aggregation for live serving — feature roadmap milestone
toward the lightweight-charts frontend. Reads `market.trades.raw` via
trades_source.read_trades_stream(), tumbling-windows each symbol's trades
via candle_windowing.build_candles() into 1-minute candles, prints each
batch to the console, and writes the latest candle per symbol to Redis
(live push) and Postgres (queryable serving copy the backend's REST
history reads) via foreachBatch — and publishes every candle update to
the market.candles.1m topic (candle_topic.py), which jobs/indicators.py
computes the indicators from.

The same application runs a second query, the live **volume footprint**
(Phase 3b): footprint_windowing.build_footprint() over the same trades,
buy and sell volume per price level per minute, every level a batch
changed upserted into Postgres's footprint_1m and each touched minute's
whole grid pushed through Redis (FootprintRedisSink). Its own query
because a streaming query holds one aggregation; this app rather than a
new one because another Spark process costs another JVM. It shares
build_candles()'s window and watermark (see footprint_windowing.py), so a
minute's levels add up to that minute's candle volume.

Runs in outputMode("update"): Redis/Postgres want to see a window's
values refine in real time as more trades arrive, not just its final
value. jobs/storage_writer.py's Gold Parquet writer uses outputMode
("append") on the *same* aggregation logic instead, since an append-only
Parquet sink needs exactly one row per finalized window — see that
module's docstring for why these can't share one query.

Structured Streaming has no built-in Redis or Postgres sink, so all four
writes (console, Redis, Postgres, the candle topic) happen inside one
foreachBatch — one streaming query against Kafka, not four. The batch is
persisted for them, so the aggregation isn't recomputed for each.

Run from streaming/ (not repo root, not from inside jobs/) so sibling
modules resolve as bare imports, matching ingestion/'s flat import
convention (see ingestion/main.py's docstring), with the repo root on
PYTHONPATH so config/ resolves too (see config/settings.py):

    cd streaming && python -m jobs.candle_aggregator

Author: @DS
"""

from __future__ import annotations

from datetime import timezone

from candle_topic import publish_candles
from candle_windowing import build_candles
from footprint_windowing import build_footprint
from postgres_sink import CandlePostgresSink, FootprintPostgresSink
from readiness import announce_when_running
from redis_sink import CandleRedisSink, FootprintRedisSink
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
    """Wire up the read -> parse -> window-aggregate -> console+Redis+Postgres+Kafka pipeline."""
    candles = build_candles(read_trades_stream(spark))

    def _write_batch(batch_df, batch_id: int) -> None:
        batch_df.persist()
        try:
            rows = batch_df.collect()
            if not rows:
                return  # a no-data batch (Spark runs those to move the watermark on)
            batch_df.show(truncate=False)
            # A batch spanning a minute boundary holds two windows per symbol, and
            # collect() order is arbitrary. Oldest-first keeps the Redis SET holding
            # the newest candle and keeps pub/sub subscribers from seeing time go
            # backwards (lightweight-charts' update() throws on an older bar).
            for row in sorted(rows, key=lambda r: r["window_start"]):
                candle = _row_to_candle(row)
                redis_sink.write_candle(candle)
                postgres_sink.write_candle(candle)
            publish_candles(batch_df)
        finally:
            batch_df.unpersist()

    return (
        candles.writeStream.outputMode("update")
        .foreachBatch(_write_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )


_FOOTPRINT_COLUMNS = ("symbol", "window_start", "price", "buy_volume", "sell_volume")


def build_footprint_query(
    spark, redis_sink: FootprintRedisSink, postgres_sink: FootprintPostgresSink
) -> "pyspark.sql.streaming.StreamingQuery":  # noqa: F821
    """Wire up the read -> parse -> footprint-aggregate -> Postgres+Redis pipeline."""
    levels = build_footprint(read_trades_stream(spark)).select(*_FOOTPRINT_COLUMNS)

    def _write_batch(batch_df, batch_id: int) -> None:
        rows = []
        for row in batch_df.collect():
            level = row.asDict()
            # collect()'s naive local time again (see _row_to_candle).
            level["window_start"] = level["window_start"].astimezone(timezone.utc)
            rows.append(level)
        if not rows:
            return
        # Postgres first: a client that hears of a minute from Redis and then
        # asks the REST API for it finds at least what it heard.
        postgres_sink.write_rows(rows)
        redis_sink.write_rows(rows)

    return (
        levels.writeStream.outputMode("update")
        .foreachBatch(_write_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )


def main() -> None:
    spark = get_spark_session("dsview-candle-aggregator")
    redis_sink = CandleRedisSink()
    postgres_sink = CandlePostgresSink()
    footprint_postgres_sink = FootprintPostgresSink()
    queries = [
        build_candle_query(spark, redis_sink, postgres_sink),
        build_footprint_query(spark, FootprintRedisSink(), footprint_postgres_sink),
    ]
    try:
        announce_when_running(queries, "candle-aggregator")  # replay/ waits for it; a no-op on live
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        pass
    finally:
        for query in queries:
            query.stop()
        postgres_sink.close()
        footprint_postgres_sink.close()
        spark.stop()


if __name__ == "__main__":
    main()
