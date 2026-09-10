"""
main.py

Ingestion service entrypoint.

Wires exchange clients to the Kafka producer and runs them concurrently.
This is deliberately the only place that imports both a concrete exchange
client (BinanceClient) and the producer — base_client.py and the clients
themselves stay decoupled from Kafka, as documented in producer.py.

Author: @DS
"""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import List

from base_client import BaseExchangeClient
from binance_client import BinanceClient
from producer import MarketDataProducer
from schema import MarketEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
SYMBOLS = ["BTCUSDT"]
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


async def run() -> None:
    producer = MarketDataProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    async def on_event(event: MarketEvent) -> None:
        await producer.send(event)

    clients: List[BaseExchangeClient] = [
        BinanceClient(symbols=SYMBOLS, on_event=on_event),
        # Coinbase/Kraken clients get added here once their adapters exist —
        # base_client.py's contract means no other file needs to change.
    ]

    stop_event = asyncio.Event()

    def _handle_shutdown_signal() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_shutdown_signal)

    client_tasks = [asyncio.create_task(client.start()) for client in clients]

    await stop_event.wait()

    logger.info("Stopping ingestion clients...")
    for client in clients:
        await client.stop()
    await asyncio.gather(*client_tasks, return_exceptions=True)

    await producer.stop()
    logger.info("Ingestion service stopped cleanly")


if __name__ == "__main__":
    asyncio.run(run())
