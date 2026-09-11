"""
spark_session.py

Shared SparkSession builder for everything under streaming/jobs/.

Centralized here so every job (candle_aggregator.py, and later
indicators.py/footprint.py) uses the same local-mode config and Kafka
connector version — a per-job SparkSession.builder call would risk drift
between jobs (e.g. one resolving a Kafka connector version that doesn't
match the installed pyspark build).

Author: @DS
"""

from __future__ import annotations

from pyspark.sql import SparkSession

# Must track the installed pyspark version (see streaming/requirements.txt).
# spark-sql-kafka-0-10 isn't bundled with pyspark; a version mismatch here
# fails at runtime with a confusing NoClassDefFoundError/UnsatisfiedLinkError
# rather than a clear "wrong version" message.
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9"


def get_spark_session(app_name: str) -> SparkSession:
    """
    Build (or fetch the existing) local-mode SparkSession for a streaming job.

    local[*] + a small shuffle-partition count matches CLAUDE.md's
    deployment approach: host-first, laptop-scale, not containerized yet.
    spark.jars.packages resolves the Kafka connector via Maven/Ivy at
    startup, so no JAR needs to be installed manually.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
