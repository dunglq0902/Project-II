"""
ingestion/startup_backfill.py
Startup Gap-Fill — automatically backfills missing data when the service starts.

Problem:
    When the system is shut down (e.g., laptop turned off at 18:45),
    the WebSocket stream stops and no data is collected. When the system
    restarts (e.g., at 20:00), there's a gap from 18:45 → 20:00.

Solution:
    1. Before shutdown: record the current UTC timestamp to a marker file.
    2. On startup: read the marker, fetch all missing candles from Binance REST
       API for the gap period, push them into Kafka, THEN start the WebSocket.
    3. A background thread keeps updating the marker every 60s while running.

The marker file is stored in a Docker volume so it persists across restarts.
"""

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger("StartupBackfill")

# Marker file path — should be on a persistent Docker volume
MARKER_DIR = os.getenv("BACKFILL_MARKER_DIR", "/data/ingestion")
MARKER_FILE = os.path.join(MARKER_DIR, "last_active_timestamp.txt")

# How often (seconds) to update the "last active" marker while running
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "60"))

# Maximum gap to backfill on startup (hours) — safety limit
MAX_BACKFILL_HOURS = int(os.getenv("MAX_BACKFILL_HOURS", "48"))


def read_last_active_timestamp() -> datetime | None:
    """Read the last active timestamp from the marker file."""
    try:
        with open(MARKER_FILE, "r") as f:
            ts_str = f.read().strip()
            if not ts_str:
                return None
            return datetime.fromisoformat(ts_str)
    except (FileNotFoundError, ValueError, OSError) as exc:
        logger.info("No previous timestamp marker found: %s", exc)
        return None


def write_last_active_timestamp(dt: datetime | None = None):
    """Write the current UTC timestamp to the marker file."""
    if dt is None:
        dt = datetime.now(tz=timezone.utc)
    try:
        os.makedirs(MARKER_DIR, exist_ok=True)
        with open(MARKER_FILE, "w") as f:
            f.write(dt.isoformat())
    except OSError as exc:
        logger.warning("Failed to write timestamp marker: %s", exc)


class HeartbeatWriter:
    """
    Background thread that updates the marker file every HEARTBEAT_INTERVAL seconds.
    This ensures the marker always reflects the last time the service was running.
    """

    def __init__(self):
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        """Start the heartbeat writer in a daemon thread."""
        self._thread = threading.Thread(target=self._run, daemon=True, name="heartbeat-writer")
        self._thread.start()
        logger.info("Heartbeat writer started (interval=%ds, marker=%s)", HEARTBEAT_INTERVAL, MARKER_FILE)

    def stop(self):
        """Stop the heartbeat writer."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        # Write one final timestamp
        write_last_active_timestamp()
        logger.info("Heartbeat writer stopped. Final timestamp saved.")

    def _run(self):
        while not self._stop_event.is_set():
            write_last_active_timestamp()
            self._stop_event.wait(timeout=HEARTBEAT_INTERVAL)


async def run_startup_backfill(symbols: list[str], interval: str = "1m") -> int:
    """
    Detect the gap between last active timestamp and now, then backfill
    using Binance REST API → Kafka.

    Returns the number of records backfilled.
    """
    last_active = read_last_active_timestamp()
    now = datetime.now(tz=timezone.utc)

    if last_active is None:
        logger.info(
            "No previous timestamp found — this is likely the first run. "
            "Backfilling last 5 minutes to seed initial data..."
        )
        last_active = now - timedelta(minutes=5)

    gap_seconds = (now - last_active).total_seconds()

    # Skip if gap is tiny (< 2 minutes — WebSocket would cover this)
    if gap_seconds < 120:
        logger.info(
            "Gap is only %.0f seconds (< 2 min). No backfill needed.", gap_seconds
        )
        return 0

    # Safety: cap the backfill range
    max_gap = timedelta(hours=MAX_BACKFILL_HOURS)
    if (now - last_active) > max_gap:
        logger.warning(
            "Gap of %.1f hours exceeds MAX_BACKFILL_HOURS=%d. "
            "Capping backfill to last %d hours.",
            gap_seconds / 3600, MAX_BACKFILL_HOURS, MAX_BACKFILL_HOURS,
        )
        last_active = now - max_gap

    gap_minutes = gap_seconds / 60
    logger.info(
        "═══════════════════════════════════════════════════════════════"
    )
    logger.info(
        "STARTUP BACKFILL: Detected gap of %.0f minutes (%.1f hours)",
        gap_minutes, gap_minutes / 60,
    )
    logger.info(
        "  From: %s", last_active.strftime("%Y-%m-%d %H:%M:%S UTC")
    )
    logger.info(
        "  To:   %s", now.strftime("%Y-%m-%d %H:%M:%S UTC")
    )
    logger.info(
        "  Symbols: %s", symbols
    )
    logger.info(
        "═══════════════════════════════════════════════════════════════"
    )

    try:
        from ingestion.binance_rest_producer import BinanceRESTProducer

        producer = BinanceRESTProducer(
            symbols=symbols,
            interval=interval,
            start_date=last_active,
            end_date=now,
        )
        await producer.run()

        total = producer._total_produced
        logger.info(
            "═══════════════════════════════════════════════════════════════"
        )
        logger.info(
            "STARTUP BACKFILL COMPLETE: %d records fetched and sent to Kafka.",
            total,
        )
        logger.info(
            "═══════════════════════════════════════════════════════════════"
        )
        return total

    except Exception as exc:
        logger.error("Startup backfill failed: %s", exc, exc_info=True)
        logger.info("Continuing with WebSocket stream despite backfill failure...")
        return 0
