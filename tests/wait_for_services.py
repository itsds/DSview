"""
wait_for_services.py

Waits until the Kafka broker, Redis and Postgres that config/settings.py
points at (through the DSVIEW_* variables scripts/test.sh sets) all
accept requests, then exits 0; exits 1 naming whatever is still down
after the timeout.

Checks each service with its real client rather than an open-port probe:
a container's port is published, and accepts TCP, well before the
service behind it can answer.

    python tests/wait_for_services.py [timeout_seconds]

Author: @DS
"""

from __future__ import annotations

import asyncio
import sys
import time

import psycopg
import redis
from aiokafka import AIOKafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVERS, POSTGRES_DSN, REDIS_HOST, REDIS_PORT

DEFAULT_TIMEOUT_SECONDS = 90


async def _kafka_ready() -> None:
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=5000)
    await producer.start()  # fetches cluster metadata, so the broker must be answering
    await producer.stop()


def kafka_ready() -> None:
    asyncio.run(_kafka_ready())


def redis_ready() -> None:
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=5)
    try:
        client.ping()
    finally:
        client.close()


def postgres_ready() -> None:
    with psycopg.connect(POSTGRES_DSN, connect_timeout=5) as conn:
        conn.execute("SELECT 1")


CHECKS = {
    f"Kafka at {KAFKA_BOOTSTRAP_SERVERS}": kafka_ready,
    f"Redis at {REDIS_HOST}:{REDIS_PORT}": redis_ready,
    "Postgres": postgres_ready,
}


def main() -> int:
    timeout = float(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_TIMEOUT_SECONDS
    deadline = time.monotonic() + timeout
    for name, check in CHECKS.items():
        print(f"Waiting for {name}", end="", flush=True)
        while True:
            try:
                check()
                break
            except Exception as exc:  # any failure just means "not yet"
                if time.monotonic() > deadline:
                    print(f"\n{name} not ready after {timeout:.0f}s: {exc!r}", file=sys.stderr)
                    return 1
                print(".", end="", flush=True)
                time.sleep(1)
        print(" ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
