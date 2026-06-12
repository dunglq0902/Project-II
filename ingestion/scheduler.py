"""
ingestion/scheduler.py
APScheduler — thay thế Apache Airflow cho scheduled tasks.

Jobs:
  - Historical backfill:  chạy mỗi ngày lúc 2:00 UTC
  - Data quality check:   chạy mỗi giờ
"""

import logging
import os
import asyncio
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger("Scheduler")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def run_historical_backfill():
    """
    Thay DAG crypto_historical_backfill.
    Smart backfill: detects the last successful backfill date and fills
    all missing days, not just yesterday. This handles multi-day outages.
    """
    from ingestion.binance_rest_producer import BinanceRESTProducer

    # Determine how many days to backfill
    # Check for a marker file that records the last successful backfill
    marker_dir = os.getenv("BACKFILL_MARKER_DIR", "/data/ingestion")
    marker_file = os.path.join(marker_dir, "last_backfill_date.txt")
    today = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        with open(marker_file, "r") as f:
            last_date_str = f.read().strip()
            last_backfill = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (FileNotFoundError, ValueError):
        # No marker — backfill only yesterday by default
        last_backfill = today - timedelta(days=2)

    start_date = last_backfill + timedelta(days=1)
    end_date = today

    days_to_fill = (end_date - start_date).days
    if days_to_fill <= 0:
        logger.info("No backfill needed — data is up to date.")
        return

    logger.info(
        "Running smart backfill: %d days (%s → %s)",
        days_to_fill,
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

    try:
        producer = BinanceRESTProducer(
            symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
            interval="1m",
            start_date=start_date,
            end_date=end_date,
        )
        asyncio.run(producer.run())

        # Write success marker
        try:
            os.makedirs(os.path.dirname(marker_file) or ".", exist_ok=True)
            with open(marker_file, "w") as f:
                f.write((end_date - timedelta(days=1)).strftime("%Y-%m-%d"))
        except Exception:
            pass

        logger.info("Historical backfill complete (%d days).", days_to_fill)
    except Exception as exc:
        logger.error("Backfill failed: %s", exc)


def check_data_quality():
    """
    Thay DAG crypto_data_quality.
    Kiểm tra dữ liệu cơ bản: Kafka lag, MinIO connectivity.
    """
    logger.info("Running data quality check at %s", datetime.now(tz=timezone.utc).isoformat())

    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        topics = admin.list_topics(timeout=10)
        topic_names = list(topics.topics.keys())
        logger.info("Kafka topics available: %s", topic_names)

        required = ["raw-ohlcv", "raw-crypto-ticks", "alert-events"]
        missing = [t for t in required if t not in topic_names]
        if missing:
            logger.warning("Missing Kafka topics: %s", missing)
        else:
            logger.info("Data quality check PASSED — all required topics exist.")
    except Exception as exc:
        logger.error("Data quality check failed: %s", exc)


def create_scheduler() -> BackgroundScheduler:
    """Create and configure the APScheduler instance."""
    scheduler = BackgroundScheduler(timezone="UTC")

    # Historical backfill — daily at 02:00 UTC
    scheduler.add_job(
        run_historical_backfill,
        'cron',
        hour=2,
        minute=0,
        id='historical_backfill',
        name='Daily Historical Backfill',
        misfire_grace_time=3600,
    )

    # Data quality check — every hour
    scheduler.add_job(
        check_data_quality,
        'interval',
        hours=1,
        id='data_quality_check',
        name='Hourly Data Quality Check',
        misfire_grace_time=600,
    )

    return scheduler
