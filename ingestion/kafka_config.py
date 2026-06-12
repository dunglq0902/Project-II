"""
kafka_config.py #Cấu hình 
Kafka connection configuration và topic definitions cho Cryptocurrency Analytics Platform.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any, List
from pathlib import Path
from dotenv import load_dotenv

# Tìm file .env ở thư mục gốc của dự án (Project Root)
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# ─────────────────────────────────────────────
# Kafka Broker Settings
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ─────────────────────────────────────────────
# Topic Definitions
# ─────────────────────────────────────────────
# kiểu Medallion Architecture (Bronze → Silver → Gold)
TOPICS = {
    "raw_ticks":        "raw-crypto-ticks",        # WebSocket tick data (real-time)    dữ liệu real-time
    "raw_ohlcv":        "raw-ohlcv",               # 1-min OHLCV candles    dữ liệu candle
    "raw_orderbook":    "raw-orderbook",            # Order book snapshots
    "processed_signals":"processed-signals",        # Processed trading signals
    "alert_events":     "alert-events",             # Alert trigger events
}

# Supported trading pairs
SUPPORTED_SYMBOLS: List[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
]

# ─────────────────────────────────────────────
# Producer Configuration
# ─────────────────────────────────────────────
PRODUCER_CONFIG: Dict[str, Any] = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id":         "binance-producer",
    # Keep a minimal set of keys compatible with librdkafka / confluent-kafka
    # Avoid Java-client-only keys like `buffer.memory`, `batch.size`, `linger.ms`.
    "compression.type":  "snappy",
    "enable.idempotence": True,
}

# ─────────────────────────────────────────────
# Consumer Configuration (for alert evaluator / test consumer)
# ─────────────────────────────────────────────
CONSUMER_CONFIG: Dict[str, Any] = {
    "bootstrap.servers":       KAFKA_BOOTSTRAP_SERVERS,
    "group.id":                "crypto-analytics-consumer",
    "auto.offset.reset":       "earliest",
    "enable.auto.commit":      False,              # Manual commit for exactly-once
    "max.poll.interval.ms":    300000,
    "session.timeout.ms":      45000,
    "fetch.min.bytes":         1,
    "fetch.max.wait.ms":       500,
}

# ─────────────────────────────────────────────
# Topic Partition Strategy
# ─────────────────────────────────────────────
SYMBOL_PARTITION_MAP: Dict[str, int] = {
    symbol: idx for idx, symbol in enumerate(SUPPORTED_SYMBOLS)
}

def get_partition_for_symbol(symbol: str) -> int:
    """Return deterministic partition index for a given symbol.(Trả về chỉ số phân vùng cố định cho một mã (symbol) nhất định)"""
    return SYMBOL_PARTITION_MAP.get(symbol, hash(symbol) % len(SUPPORTED_SYMBOLS))


# ─────────────────────────────────────────────
# Binance API Settings
# ─────────────────────────────────────────────
BINANCE_WS_BASE_URL:   str = "wss://stream.binance.com:9443/ws"
BINANCE_REST_BASE_URL: str = "https://api.binance.com"

# Private API: Dùng để:
# Đặt lệnh (buy/sell)
# Xem tài khoản
# Xem số dư
BINANCE_API_KEY:       str = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET:    str = os.getenv("BINANCE_API_SECRET", "")

# REST API endpoints
BINANCE_REST_ENDPOINTS = {
    "klines":        "/api/v3/klines",
    "exchange_info": "/api/v3/exchangeInfo",
    "order_book":    "/api/v3/depth",
    "ticker_24h":    "/api/v3/ticker/24hr",
}

# WebSocket stream types
BINANCE_WS_STREAMS = {
    "kline":       "{symbol}@kline_{interval}",
    "trade":       "{symbol}@trade",
    "book_ticker": "{symbol}@bookTicker",
    "mini_ticker": "{symbol}@miniTicker",
}

# Reconnect settings
WS_RECONNECT_INITIAL_DELAY: float = 1.0   # seconds
WS_RECONNECT_MAX_DELAY:     float = 60.0  # seconds
WS_RECONNECT_MULTIPLIER:    float = 2.0
WS_PING_INTERVAL:           int   = 20    # seconds
WS_PING_TIMEOUT:            int   = 10    # seconds

# REST rate-limit guard
REST_REQUEST_DELAY_MS: float = 100        # ms between REST requests
REST_MAX_LIMIT:        int   = 1000       # max rows per REST klines call
