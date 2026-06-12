"""
ingestion/main.py
Main entry point cho Ingestion Service.

Kết hợp:
  - Startup Backfill (gap-fill on restart)
  - BinanceWebSocketProducer (real-time streaming)
  - APScheduler (backfill + data quality)

Flow on startup:
  1. Detect how long the service was down (using persistent marker file)
  2. Backfill missing data from Binance REST API → Kafka
  3. Start WebSocket real-time streaming
  4. Start APScheduler for daily backfill + hourly data quality checks
  5. Heartbeat writer keeps updating the marker file every 60s

Thay thế cả Airflow DAGs cho streaming và scheduled tasks.
"""

import asyncio
import logging
import signal
import sys

from ingestion.binance_ws_producer import BinanceWebSocketProducer
from ingestion.scheduler import create_scheduler
from ingestion.startup_backfill import (
    run_startup_backfill,
    write_last_active_timestamp,
    HeartbeatWriter,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("IngestionService")

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]


async def main():
    logger.info("Starting Ingestion Service...")

    # ── Step 1: Startup Backfill ─────────────────────────────────────────────
    # Detect the gap since last shutdown and backfill missing data
    logger.info("Phase 1/3: Running startup backfill to fill any data gaps...")
    backfilled = await run_startup_backfill(
        symbols=DEFAULT_SYMBOLS,
        interval="1m",
    )
    if backfilled > 0:
        logger.info("Startup backfill produced %d records.", backfilled)
    else:
        logger.info("No gap detected or no data to backfill.")

    # ── Step 2: Start Heartbeat Writer ────────────────────────────────────────
    # Keeps updating the "last active" marker file so we know when we went down
    heartbeat = HeartbeatWriter()
    heartbeat.start()

    # Write an initial timestamp now that we're live
    write_last_active_timestamp()

    # ── Step 3: Start APScheduler (background thread) ────────────────────────
    scheduler = create_scheduler()
    scheduler.start()
    logger.info("APScheduler started with %d jobs.", len(scheduler.get_jobs()))

    # ── Step 4: Start WebSocket producer ─────────────────────────────────────
    producer = BinanceWebSocketProducer(symbols=DEFAULT_SYMBOLS, interval="1m")

    # Graceful shutdown
    loop = asyncio.get_running_loop()

    def _shutdown():
        logger.info("Shutdown signal received.")
        producer.stop()
        scheduler.shutdown(wait=False)
        # Write final timestamp and stop heartbeat
        heartbeat.stop()
        logger.info("Final timestamp saved. Data gap tracking is active.")

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            try:
                signal.signal(sig, lambda _s, _f: _shutdown())
            except Exception:
                pass

    logger.info("Phase 2/3: Starting WebSocket producer for %s...", DEFAULT_SYMBOLS)
    await producer.run()

    # Cleanup
    heartbeat.stop()
    scheduler.shutdown(wait=False)
    logger.info("Ingestion Service stopped.")


if __name__ == "__main__":
    # Ensure the package is importable when run via python -m
    if __package__ is None:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    asyncio.run(main())
