"""
alert-engine/consumer/alert_consumer.py
Kafka Consumer Bridge — reads alert events from the 'alert-events' topic
and dispatches them to the Alert Engine API for notification delivery.

This is the CRITICAL missing link between:
  Spark (produces → alert-events topic)  →  Alert API (POST /api/v1/notifications/dispatch)

Run:
    python -m alert_engine.consumer.alert_consumer
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from confluent_kafka import Consumer, KafkaError, KafkaException

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("AlertConsumer")

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALERT_TOPIC             = os.getenv("ALERT_TOPIC", "alert-events")
CONSUMER_GROUP_ID       = os.getenv("CONSUMER_GROUP_ID", "alert-consumer-group")
ALERT_API_URL           = os.getenv("ALERT_API_URL", "http://localhost:8000")
DISPATCH_ENDPOINT       = f"{ALERT_API_URL}/api/v1/notifications/dispatch"

# Retry configuration
MAX_RETRIES             = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS   = float(os.getenv("RETRY_BACKOFF_SECONDS", "2.0"))
POLL_TIMEOUT_SECONDS    = float(os.getenv("POLL_TIMEOUT_SECONDS", "1.0"))
BATCH_SIZE              = int(os.getenv("BATCH_SIZE", "10"))

# Dead letter logging
DLQ_LOG_DIR             = os.getenv("DLQ_LOG_DIR", "/tmp/alert-consumer-dlq")


# ─────────────────────────────────────────────
# Dead Letter Queue (file-based fallback)
# ─────────────────────────────────────────────
def log_dead_letter(event_data: dict, error: str):
    """Log failed events to a dead-letter file for manual review."""
    os.makedirs(DLQ_LOG_DIR, exist_ok=True)
    dlq_file = os.path.join(DLQ_LOG_DIR, f"dlq_{datetime.now(tz=timezone.utc).strftime('%Y%m%d')}.jsonl")
    record = {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "error": error,
        "event": event_data,
    }
    try:
        with open(dlq_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")
        logger.warning("Event logged to DLQ: %s", dlq_file)
    except Exception as exc:
        logger.error("Failed to write DLQ: %s", exc)


# ─────────────────────────────────────────────
# Alert Dispatcher
# ─────────────────────────────────────────────
class AlertDispatcher:
    """
    Dispatches alert events to the Alert Engine API
    with retry logic and dead-letter handling.
    """

    def __init__(self, dispatch_url: str = DISPATCH_ENDPOINT):
        self.dispatch_url = dispatch_url
        self._stats = {
            "dispatched": 0,
            "failed": 0,
            "retried": 0,
        }

    def dispatch(self, event_data: dict) -> bool:
        """
        Send a single alert event to the Alert API.
        Returns True on success, False on failure.
        """
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with httpx.Client(timeout=10) as client:
                    resp = client.post(
                        self.dispatch_url,
                        json=event_data,
                        headers={"Content-Type": "application/json"},
                    )

                if resp.status_code == 200:
                    self._stats["dispatched"] += 1
                    logger.info(
                        "Alert dispatched | alert_id=%s symbol=%s action=%s",
                        event_data.get("alert_id", "?"),
                        event_data.get("symbol", "?"),
                        event_data.get("action", "?"),
                    )
                    return True

                if resp.status_code == 404:
                    # Rule not found — no point retrying
                    logger.warning(
                        "Rule not found for alert %s (HTTP 404), skipping.",
                        event_data.get("alert_id"),
                    )
                    self._stats["failed"] += 1
                    return False

                logger.warning(
                    "Dispatch attempt %d/%d failed: HTTP %d — %s",
                    attempt, MAX_RETRIES, resp.status_code, resp.text[:200],
                )

            except httpx.ConnectError as exc:
                logger.warning(
                    "Dispatch attempt %d/%d: connection error — %s",
                    attempt, MAX_RETRIES, exc,
                )
            except httpx.TimeoutException as exc:
                logger.warning(
                    "Dispatch attempt %d/%d: timeout — %s",
                    attempt, MAX_RETRIES, exc,
                )
            except Exception as exc:
                logger.error(
                    "Dispatch attempt %d/%d: unexpected error — %s",
                    attempt, MAX_RETRIES, exc,
                )

            self._stats["retried"] += 1
            if attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.info("Retrying in %.1fs...", backoff)
                time.sleep(backoff)

        # All retries exhausted
        self._stats["failed"] += 1
        log_dead_letter(event_data, f"Failed after {MAX_RETRIES} attempts")
        return False

    @property
    def stats(self) -> dict:
        return dict(self._stats)


# ─────────────────────────────────────────────
# Kafka Consumer
# ─────────────────────────────────────────────
class AlertConsumerBridge:
    """
    Kafka consumer that reads from the 'alert-events' topic
    and forwards each event to the Alert Engine API.
    """

    def __init__(self):
        self._running = False
        self._consumer: Optional[Consumer] = None
        self._dispatcher = AlertDispatcher()

        self._consumer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id":          CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 30000,
        }

    def _create_consumer(self) -> Consumer:
        """Create and subscribe the Kafka consumer."""
        consumer = Consumer(self._consumer_config)
        consumer.subscribe([ALERT_TOPIC])
        logger.info(
            "Kafka consumer created | broker=%s topic=%s group=%s",
            KAFKA_BOOTSTRAP_SERVERS, ALERT_TOPIC, CONSUMER_GROUP_ID,
        )
        return consumer

    def _parse_message(self, msg_value: bytes) -> Optional[dict]:
        """Parse Kafka message value from JSON bytes."""
        try:
            data = json.loads(msg_value.decode("utf-8"))
            # Validate required fields
            required = ["alert_id", "rule_id", "user_id", "symbol"]
            missing = [f for f in required if f not in data]
            if missing:
                logger.warning("Message missing required fields: %s", missing)
                return None
            return data
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Failed to parse message: %s", exc)
            return None

    def run(self):
        """Main consumer loop — runs until stopped."""
        self._running = True
        self._consumer = self._create_consumer()
        messages_processed = 0

        logger.info("Alert Consumer Bridge started. Waiting for events...")

        try:
            while self._running:
                msg = self._consumer.poll(timeout=POLL_TIMEOUT_SECONDS)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            "Reached end of partition %s [%d] at offset %d",
                            msg.topic(), msg.partition(), msg.offset(),
                        )
                        continue
                    raise KafkaException(msg.error())

                # Parse and dispatch the alert event
                event_data = self._parse_message(msg.value())
                if event_data:
                    success = self._dispatcher.dispatch(event_data)
                    if success:
                        messages_processed += 1

                    # Commit offset regardless of dispatch success
                    # (failed events are logged to DLQ)
                    self._consumer.commit(asynchronous=False)

                    # Periodic stats logging
                    if messages_processed % 100 == 0 and messages_processed > 0:
                        logger.info(
                            "Progress: %d messages processed | Stats: %s",
                            messages_processed, self._dispatcher.stats,
                        )

        except KeyboardInterrupt:
            logger.info("Shutdown requested by KeyboardInterrupt.")
        except Exception as exc:
            logger.exception("Consumer loop fatal error: %s", exc)
        finally:
            self._shutdown()

        logger.info(
            "Alert Consumer Bridge stopped. Total processed: %d | Stats: %s",
            messages_processed, self._dispatcher.stats,
        )

    def stop(self):
        """Signal the consumer to stop gracefully."""
        logger.info("Stop signal received.")
        self._running = False

    def _shutdown(self):
        """Clean up Kafka consumer resources."""
        if self._consumer:
            try:
                self._consumer.close()
                logger.info("Kafka consumer closed.")
            except Exception as exc:
                logger.error("Error closing consumer: %s", exc)


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
def main():
    bridge = AlertConsumerBridge()

    # Graceful shutdown on SIGTERM/SIGINT
    def _handle_signal(signum, frame):
        logger.info("Received signal %d, stopping...", signum)
        bridge.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Retry loop — keep trying to connect to Kafka
    while True:
        try:
            bridge.run()
            break  # Clean exit
        except KafkaException as exc:
            logger.error("Kafka error: %s — retrying in 10s...", exc)
            time.sleep(10)
        except Exception as exc:
            logger.exception("Unexpected error: %s — retrying in 10s...", exc)
            time.sleep(10)


if __name__ == "__main__":
    main()
