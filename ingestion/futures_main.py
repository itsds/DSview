"""
futures_main.py

The Futures poller's entrypoint (Phase 4c): Binance Futures' four
long/short ratios for every SYMBOLS entry's USDⓈ-M perpetual, every 5
minutes, onto `market.futures.ratios` (FUTURES_RATIOS_TOPIC) through the
same MarketDataProducer the trade stream uses, keyed by symbol;
streaming/jobs/futures_ratios.py takes them to Postgres and Redis.

A process of its own, beside main.py's trade stream rather than inside it:
a Futures outage or an IP ban (418) then never touches the trades, and
either restarts alone. Like main.py, the only place that wires a concrete
client to the producer.

On start it reads back where each ratio left off (ratio_topic.py) and
resumes from the period after; a ratio the topic doesn't have starts 30
days back, which on a first start is the backfill (Binance keeps nothing
older). Run it from ingestion/, with the repo root on PYTHONPATH:

    cd ingestion && python futures_main.py
    python futures_main.py --refetch-days 30   # fetch the 30 days again, to restore an emptied table

Ctrl+C or SIGTERM stops it; a request Binance refuses outright (an unknown
symbol, say) stops it with exit status 1.

Author: @DS
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from typing import List, Optional

from config.settings import FUTURES_RATIO_KINDS, FUTURES_RATIOS_TOPIC, KAFKA_BOOTSTRAP_SERVERS, PIPELINE, SYMBOLS

from binance_futures_rest import BinanceFuturesClient
from futures_poller import HISTORY_DAYS, FuturesRatioPoller
from producer import MarketDataProducer
from ratio_topic import newest_periods

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll Binance Futures' long/short ratios onto Kafka")
    parser.add_argument(
        "--refetch-days",
        type=float,
        default=None,
        help=f"fetch this many days again, whatever the topic holds (at most {HISTORY_DAYS}: Binance keeps no more)",
    )
    args = parser.parse_args(argv)
    if args.refetch_days is not None and not 0 < args.refetch_days <= HISTORY_DAYS:
        parser.error(f"--refetch-days must be more than 0 and at most {HISTORY_DAYS}")
    return args


async def run(refetch_days: Optional[float] = None) -> int:
    if not PIPELINE.is_live:
        # Exchange data is live's: the replay pipeline has no Futures feed.
        raise SystemExit(f"the Futures poller feeds the live pipeline only (DSVIEW_PIPELINE is {PIPELINE.name!r})")

    producer = MarketDataProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="dsview-futures-poller")
    await producer.start()
    client = BinanceFuturesClient()
    try:
        resume_from = {}
        if refetch_days is None:
            wanted = [(symbol, kind) for symbol in SYMBOLS for kind in FUTURES_RATIO_KINDS]
            resume_from = await newest_periods(wanted)
            logger.info(
                "Resuming %d of %d ratios from '%s'; the rest start %d days back",
                len(resume_from),
                len(wanted),
                FUTURES_RATIOS_TOPIC,
                HISTORY_DAYS,
            )
        poller = FuturesRatioPoller(
            client, SYMBOLS, on_ratios=producer.send_batch, resume_from=resume_from, refetch_days=refetch_days
        )

        stop_event = asyncio.Event()

        def _handle_shutdown_signal() -> None:
            logger.info("Shutdown signal received")
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_shutdown_signal)

        polling = asyncio.create_task(poller.run())
        stopping = asyncio.create_task(stop_event.wait())
        await asyncio.wait({polling, stopping}, return_when=asyncio.FIRST_COMPLETED)
        for task in (polling, stopping):
            task.cancel()
        results = await asyncio.gather(polling, stopping, return_exceptions=True)
        failure = results[0] if isinstance(results[0], Exception) else None
        if failure is not None:
            logger.error("Futures poller stopped: %s", failure)
            return 1
        logger.info("Futures poller stopped cleanly")
        return 0
    finally:
        await client.close()
        await producer.stop()


if __name__ == "__main__":
    sys.exit(asyncio.run(run(parse_args().refetch_days)))
