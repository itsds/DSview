"""
indicators.py

Technical indicators in Spark — feature roadmap item 2, Phase 2 of
docs/ROADMAP.md: EMA 20, 50 and 200 (config/settings.py's
SERVER_EMA_LENGTHS) on every chart timeframe, for live serving.

Reads every 1m candle update candle_aggregator.py publishes to
market.candles.1m (candle_topic.py), keeps each symbol's running EMAs in
Spark state (indicator_state.py, around indicator_engine.py), and in a
foreachBatch writes every changed bar to Postgres's candle_indicators
(history: GET /candles returns it with each candle) and Redis (live push:
/ws/indicators). The first batch that sees a symbol also writes its
indicators over all of candles_1m (indicator_state.py's warm-up).

**Checkpoint.** Unlike candle_aggregator.py, this query keeps a checkpoint
(`DATA_ROOT/_checkpoints/indicators` by default): it holds the EMA state
and the topic offsets, so a restart resumes exactly where it stopped,
replaying candles published while it was down rather than skipping them.
To rebuild everything from candles_1m (after changing SERVER_EMA_LENGTHS,
say, which the saved state refuses), stop the job and delete that
directory; the next start warms every symbol up again. If it stays down
longer than the topic's retention, its saved offsets are gone and Spark
stops with a data-loss error rather than silently skip candles: rebuild
the same way.

Run from streaming/ (see candle_aggregator.py's docstring for why), with
the repo root on PYTHONPATH:

    cd streaming && python -m jobs.indicators

Author: @DS
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import functions as F

from config.settings import DATA_ROOT

from candle_topic import ensure_candles_topic, read_candles_stream
from indicator_engine import INDICATOR_COLUMNS
from indicator_state import compute_indicators
from postgres_sink import IndicatorPostgresSink, read_minute_closes
from redis_sink import IndicatorRedisSink
from spark_session import get_spark_session

DEFAULT_CHECKPOINT = f"{DATA_ROOT}/_checkpoints/indicators"


def _summary(rows) -> str:
    """The newest bar per symbol and timeframe, for the console: what candle_aggregator's show() is to candles."""
    newest = {}
    for row in rows:
        newest[(row["symbol"], row["timeframe"])] = row
    lines = [f"{len(rows)} indicator rows"]
    for (symbol, timeframe), row in sorted(newest.items()):
        values = "  ".join(
            f"{column.upper().replace('_', ' ')} {'—' if row[column] is None else f'{row[column]:,.2f}'}"
            for column in INDICATOR_COLUMNS
        )
        lines.append(f"  {symbol} {timeframe:>3} {row['window_start']:%Y-%m-%d %H:%M}  {values}")
    return "\n".join(lines)


def build_indicator_query(
    spark,
    redis_sink: IndicatorRedisSink,
    postgres_sink: IndicatorPostgresSink,
    checkpoint_location: str = DEFAULT_CHECKPOINT,
    verbose: bool = True,
) -> "pyspark.sql.streaming.StreamingQuery":  # noqa: F821
    """Wire up candle topic -> per-symbol EMA state -> Postgres + Redis. The topic must exist (see main())."""
    indicators = compute_indicators(read_candles_stream(spark), order_by="offset", warm_up=read_minute_closes)

    def _write_batch(batch_df, batch_id: int) -> None:
        # Bar starts as Unix seconds rather than collect()'s naive local
        # datetimes (see candle_aggregator.py's _row_to_candle()).
        collected = batch_df.withColumn("window_start", F.col("window_start").cast("long")).collect()
        if not collected:
            return
        rows = []
        for row in collected:
            values = row.asDict()
            values["window_start"] = datetime.fromtimestamp(values["window_start"], timezone.utc)
            rows.append(values)
        # Oldest first within each symbol and timeframe, so the Redis SET ends on
        # the newest bar and subscribers see a series in time order.
        rows.sort(key=lambda r: (r["symbol"], r["timeframe"], r["window_start"]))
        postgres_sink.write_rows(rows)
        redis_sink.write_rows(rows, INDICATOR_COLUMNS)
        if verbose:
            print(f"Batch {batch_id}: {_summary(rows)}", flush=True)

    return (
        indicators.writeStream.outputMode("update")
        .foreachBatch(_write_batch)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )


def main(checkpoint_location: Optional[str] = None) -> None:
    ensure_candles_topic()  # on a fresh broker, this job can start before the first candle
    spark = get_spark_session("dsview-indicators")
    redis_sink = IndicatorRedisSink()
    postgres_sink = IndicatorPostgresSink()
    query = build_indicator_query(spark, redis_sink, postgres_sink, checkpoint_location or DEFAULT_CHECKPOINT)
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
