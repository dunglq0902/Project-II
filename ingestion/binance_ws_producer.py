"""
binance_ws_producer.py
WebSocket streaming producer – thu thập tick data real-time từ Binance
và đẩy vào Kafka topics.

Chạy:
    python ingestion/binance_ws_producer.py --symbols BTCUSDT ETHUSDT --interval 1m
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional

import websockets
from confluent_kafka import Producer, KafkaException

from ingestion.kafka_config import (
    PRODUCER_CONFIG,
    TOPICS,
    BINANCE_WS_BASE_URL,
    BINANCE_WS_STREAMS,
    WS_RECONNECT_INITIAL_DELAY,
    WS_RECONNECT_MAX_DELAY,
    WS_RECONNECT_MULTIPLIER,
    WS_PING_INTERVAL,
    WS_PING_TIMEOUT,
    get_partition_for_symbol,
    SUPPORTED_SYMBOLS,
)

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("BinanceWSProducer")


# ─────────────────────────────────────────────
# Message Parsers
# ─────────────────────────────────────────────
def parse_kline_event(raw: dict) -> Optional[dict]:
    """Parse Binance kline/candlestick stream event."""
    try:
        k = raw["k"]
        return {
            "symbol":             k["s"],
            "event_time":         datetime.fromtimestamp(raw["E"] / 1000, tz=timezone.utc).isoformat(),
            "ingest_time":        datetime.now(tz=timezone.utc).isoformat(),
            "open_time":          datetime.fromtimestamp(k["t"] / 1000, tz=timezone.utc).isoformat(),
            "close_time":         datetime.fromtimestamp(k["T"] / 1000, tz=timezone.utc).isoformat(),
            "interval":           k["i"],
            "open":               float(k["o"]),
            "high":               float(k["h"]),
            "low":                float(k["l"]),
            "close":              float(k["c"]),
            "volume":             float(k["v"]),
            "quote_volume":       float(k["q"]),
            "trade_count":        int(k["n"]),
            "taker_buy_base_vol": float(k["V"]),
            "taker_buy_quote_vol":float(k["Q"]),
            "is_closed":          k["x"],           # True = candle closed
            "source":             "websocket",
            "partition_date":     datetime.fromtimestamp(k["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
        }
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("Failed to parse kline event: %s | raw=%s", exc, raw)
        return None


def parse_trade_event(raw: dict) -> Optional[dict]:
    """Parse Binance individual trade stream event.

    Return `None` when required fields are missing or invalid.
    """
    try:
        symbol = raw.get("s")
        price_raw = raw.get("p")
        qty_raw = raw.get("q")

        # Required fields: symbol, price, quantity
        if symbol is None or price_raw is None or qty_raw is None:
            logger.warning("Missing required trade fields | raw=%s", raw)
            return None

        event_time_ms = raw.get("E")
        trade_time_ms = raw.get("T") or event_time_ms

        price = float(price_raw)
        quantity = float(qty_raw)

        return {
            "symbol":         symbol,
            "event_time":     datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).isoformat() if event_time_ms is not None else None,
            "ingest_time":    datetime.now(tz=timezone.utc).isoformat(),
            "trade_id":       raw.get("t"),
            "price":          price,
            "quantity":       quantity,
            "buyer_order_id": raw.get("b"),   # may be absent
            "seller_order_id":raw.get("a"),   # may be absent
            "trade_time":     datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc).isoformat() if trade_time_ms is not None else None,
            "is_buyer_maker": raw.get("m"),
            "source":         "websocket",
        }
    except (TypeError, ValueError) as exc:
        logger.warning("Failed to parse trade event: %s | raw=%s", exc, raw)
        return None


def parse_book_ticker_event(raw: dict) -> Optional[dict]:
    """Parse Binance best bid/ask price event."""
    try:
        return {
            "symbol":       raw["s"],
            "ingest_time":  datetime.now(tz=timezone.utc).isoformat(),
            "update_id":    raw["u"],
            "best_bid":     float(raw["b"]),
            "best_bid_qty": float(raw["B"]),
            "best_ask":     float(raw["a"]),
            "best_ask_qty": float(raw["A"]),
            "source":       "websocket",
        }
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("Failed to parse book_ticker event: %s | raw=%s", exc, raw)
        return None


# ─────────────────────────────────────────────
# Delivery Callback
# ─────────────────────────────────────────────
def delivery_report(err, msg):
    """Called once for each produced message to indicate delivery result."""
    if err is not None:
        logger.error("Message delivery failed | topic=%s error=%s", msg.topic(), err)
    else:
        logger.debug(
            "Message delivered | topic=%s partition=%d offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ─────────────────────────────────────────────
# Producer
# ─────────────────────────────────────────────
class BinanceWebSocketProducer:
    """
    Subscribes to Binance WebSocket streams for the given symbols and interval,
    then pushes parsed messages to Kafka with exponential-backoff reconnect.
    """

    def __init__(self, symbols: List[str], interval: str = "1m"):
        self.symbols:   List[str] = [s.upper() for s in symbols]
        self.interval:  str       = interval
        self.producer:  Producer  = Producer(PRODUCER_CONFIG)
        self._stop:     bool      = False
        self._msg_count: int      = 0

        # Validate symbols
        invalid = [s for s in self.symbols if s not in SUPPORTED_SYMBOLS]
        if invalid:
            raise ValueError(f"Unsupported symbols: {invalid}. Supported: {SUPPORTED_SYMBOLS}")

    # ── Stream URL builder ────────────────────────────────────────────────────
    def _build_stream_url(self) -> str:
        """
        Build a combined stream URL for kline + trade + bookTicker.
        e.g.: wss://stream.binance.com:9443/ws/btcusdt@kline_1m/btcusdt@trade/...
        """
        streams = []
        for symbol in self.symbols:
            s = symbol.lower()
            streams.append(f"{s}@kline_{self.interval}")
            streams.append(f"{s}@trade")
            streams.append(f"{s}@bookTicker")
        combined = "/".join(streams)
        return f"{BINANCE_WS_BASE_URL}/{combined}"

    # ── Message dispatcher ────────────────────────────────────────────────────
    def _dispatch(self, raw: dict):
        """Route raw message to the correct topic based on stream type."""
        event_type = raw.get("e")

        if event_type == "kline":
            parsed = parse_kline_event(raw)
            topic  = TOPICS["raw_ohlcv"]
            key    = raw.get("k", {}).get("s", "UNKNOWN")
        elif event_type == "trade":
            parsed = parse_trade_event(raw)
            topic  = TOPICS["raw_ticks"]
            key    = raw.get("s", "UNKNOWN")
        elif event_type == "bookTicker" or "b" in raw and "a" in raw and "s" in raw:
            parsed = parse_book_ticker_event(raw)
            topic  = TOPICS["raw_orderbook"]
            key    = raw.get("s", "UNKNOWN")
        else:
            logger.debug("Unknown event type '%s', skipping.", event_type)
            return

        if parsed is None:
            return

        partition = get_partition_for_symbol(key)
        value_bytes = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
        key_bytes   = key.encode("utf-8")

        try:
            self.producer.produce(
                topic=topic,
                value=value_bytes,
                key=key_bytes,
                # partition=partition,
                callback=delivery_report,
            )
            self._msg_count += 1

            # Poll every 100 messages to trigger delivery callbacks
            if self._msg_count % 100 == 0:
                self.producer.poll(0)
                logger.info("Produced %d messages so far.", self._msg_count)

        except KafkaException as exc:
            logger.error("Kafka produce error: %s", exc)
        except BufferError:
            # Local buffer is full – flush and retry
            logger.warning("Kafka buffer full, flushing...")
            self.producer.flush(timeout=10)

    # ── WebSocket loop ────────────────────────────────────────────────────────
    async def _listen(self, url: str):
        """Connect and listen until stop signal or error."""
        logger.info("Connecting to: %s", url)
        async with websockets.connect(
            url,
            ping_interval=WS_PING_INTERVAL,
            ping_timeout=WS_PING_TIMEOUT,
        ) as ws:
            logger.info("WebSocket connected. Streaming %s ...", self.symbols)
            async for raw_msg in ws:
                if self._stop:
                    break
                try:
                    data = json.loads(raw_msg)
                    # Combined streams wrap data under "data" key
                    if "data" in data:
                        data = data["data"]
                    self._dispatch(data)
                except json.JSONDecodeError as exc:
                    logger.warning("JSON decode error: %s", exc)

    async def _run_with_backoff(self):
        """Run _listen() with exponential backoff on disconnection."""
        delay = WS_RECONNECT_INITIAL_DELAY
        url   = self._build_stream_url()

        while not self._stop:
            try:
                await self._listen(url)
                if self._stop:
                    break
                logger.warning("WebSocket closed unexpectedly. Reconnecting in %.1fs...", delay)
            except (
                websockets.ConnectionClosedError,
                websockets.ConnectionClosedOK,
                OSError,
            ) as exc:
                logger.error("WebSocket error: %s. Reconnecting in %.1fs...", exc, delay)
            except Exception as exc:                # pylint: disable=broad-except
                logger.exception("Unexpected error: %s. Reconnecting in %.1fs...", exc, delay)

            await asyncio.sleep(delay)
            delay = min(delay * WS_RECONNECT_MULTIPLIER, WS_RECONNECT_MAX_DELAY)

    # ── Public API ────────────────────────────────────────────────────────────
    def stop(self):
        """Signal the producer to stop."""
        logger.info("Stop signal received.")
        self._stop = True

    async def run(self):
        """Start the WebSocket listener."""
        await self._run_with_backoff()
        logger.info("Flushing remaining Kafka messages...")
        self.producer.flush(timeout=30)
        logger.info("Producer shut down. Total messages produced: %d", self._msg_count)


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance WebSocket → Kafka Producer")
    parser.add_argument(
        "--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
        help="List of trading pair symbols (e.g. BTCUSDT ETHUSDT)",
    )
    parser.add_argument(
        "--interval", default="1m",
        choices=["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"],
        help="Kline interval (default: 1m)",
    )
    return parser.parse_args()


async def _main():
    args = parse_args()
    producer = BinanceWebSocketProducer(symbols=args.symbols, interval=args.interval)

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_running_loop()

    def _shutdown():
        producer.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            # Fallback for Windows event loops that don't implement add_signal_handler
            try:
                signal.signal(sig, lambda _s, _f: _shutdown())
            except Exception:
                pass

    logger.info(
        "Starting Binance WebSocket Producer | symbols=%s interval=%s",
        args.symbols, args.interval,
    )
    await producer.run()


if __name__ == "__main__":
    asyncio.run(_main())
