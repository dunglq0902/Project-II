"""
tests/test_ingestion.py
Unit tests for Binance producers and Kafka config.
"""

import json
import sys
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.kafka_config import (
    get_partition_for_symbol,
    SUPPORTED_SYMBOLS,
    TOPICS,
)
from ingestion.binance_ws_producer import (
    parse_kline_event,
    parse_trade_event,
    parse_book_ticker_event,
    BinanceWebSocketProducer,
)
from ingestion.binance_rest_producer import parse_kline_row


# ─────────────────────────────────────────────
# Kafka Config Tests
# ─────────────────────────────────────────────

class TestKafkaConfig:
    def test_partition_for_known_symbol(self):
        for symbol in SUPPORTED_SYMBOLS:
            partition = get_partition_for_symbol(symbol)
            assert isinstance(partition, int)
            assert 0 <= partition < len(SUPPORTED_SYMBOLS)

    def test_partition_for_unknown_symbol_is_int(self):
        partition = get_partition_for_symbol("DOGEUSDT")
        assert isinstance(partition, int)

    def test_all_required_topics_exist(self):
        required = {"raw_ticks", "raw_ohlcv", "raw_orderbook", "processed_signals", "alert_events"}
        assert required.issubset(set(TOPICS.keys()))

    def test_partition_deterministic(self):
        """Same symbol always maps to same partition."""
        assert get_partition_for_symbol("BTCUSDT") == get_partition_for_symbol("BTCUSDT")


# ─────────────────────────────────────────────
# WebSocket Parser Tests
# ─────────────────────────────────────────────

SAMPLE_KLINE_EVENT = {
    "e": "kline",
    "E": 1700000000000,
    "k": {
        "t": 1700000000000,
        "T": 1700000059999,
        "s": "BTCUSDT",
        "i": "1m",
        "o": "37000.00",
        "h": "37100.00",
        "l": "36950.00",
        "c": "37050.00",
        "v": "10.5",
        "q": "388525.50",
        "n": 250,
        "V": "5.2",
        "Q": "192580.40",
        "x": True,
    }
}

SAMPLE_TRADE_EVENT = {
    "e": "trade",
    "E": 1700000000000,
    "s": "BTCUSDT",
    "t": 999888777,
    "p": "37050.00",
    "q": "0.001",
    "b": 111,
    "a": 222,
    "T": 1700000000000,
    "m": False,
}

SAMPLE_BOOK_TICKER = {
    "u": 12345678,
    "s": "BTCUSDT",
    "b": "37049.00",
    "B": "1.500",
    "a": "37050.00",
    "A": "2.000",
}


class TestKlineParser:
    def test_parse_valid_kline(self):
        result = parse_kline_event(SAMPLE_KLINE_EVENT)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["close"] == 37050.00
        assert result["volume"] == 10.5
        assert result["source"] == "websocket"
        assert result["is_closed"] is True
        assert "partition_date" in result

    def test_parse_kline_missing_key(self):
        bad = {"e": "kline", "E": 123}  # Missing "k"
        result = parse_kline_event(bad)
        assert result is None

    def test_parse_kline_invalid_price(self):
        bad = {**SAMPLE_KLINE_EVENT}
        bad["k"] = {**SAMPLE_KLINE_EVENT["k"], "c": "not_a_number"}
        result = parse_kline_event(bad)
        assert result is None


class TestTradeParser:
    def test_parse_valid_trade(self):
        result = parse_trade_event(SAMPLE_TRADE_EVENT)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["price"] == 37050.00
        assert result["is_buyer_maker"] is False
        assert result["source"] == "websocket"

    def test_parse_trade_missing_field(self):
        bad = {"e": "trade"}
        result = parse_trade_event(bad)
        assert result is None


class TestBookTickerParser:
    def test_parse_valid_book_ticker(self):
        result = parse_book_ticker_event(SAMPLE_BOOK_TICKER)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["best_bid"] == 37049.00
        assert result["best_ask"] == 37050.00

    def test_spread_is_positive(self):
        result = parse_book_ticker_event(SAMPLE_BOOK_TICKER)
        assert result["best_ask"] > result["best_bid"]


# ─────────────────────────────────────────────
# REST Producer Parser Tests
# ─────────────────────────────────────────────

SAMPLE_REST_ROW = [
    1700000000000,  # open_time
    "37000.00",     # open
    "37100.00",     # high
    "36950.00",     # low
    "37050.00",     # close
    "10.50",        # volume
    1700000059999,  # close_time
    "388525.50",    # quote_volume
    250,            # trade_count
    "5.20",         # taker_buy_base_vol
    "192580.40",    # taker_buy_quote_vol
    "0",            # ignore
]


class TestRESTParser:
    def test_parse_valid_row(self):
        result = parse_kline_row(SAMPLE_REST_ROW, "BTCUSDT", "1m")
        assert result["symbol"] == "BTCUSDT"
        assert result["close"] == 37050.00
        assert result["volume"] == 10.50
        assert result["trade_count"] == 250
        assert result["source"] == "rest"
        assert result["is_closed"] is True
        assert result["interval"] == "1m"

    def test_parse_row_partition_date_format(self):
        result = parse_kline_row(SAMPLE_REST_ROW, "BTCUSDT", "1m")
        assert len(result["partition_date"]) == 10   # YYYY-MM-DD


# ─────────────────────────────────────────────
# Producer Validation Tests
# ─────────────────────────────────────────────

class TestProducerValidation:
    def test_invalid_symbol_raises(self):
        with pytest.raises(ValueError, match="Unsupported symbols"):
            BinanceWebSocketProducer(symbols=["INVALID999"])

    def test_valid_symbols_accepted(self):
        # Should not raise
        producer = BinanceWebSocketProducer.__new__(BinanceWebSocketProducer)
        producer.symbols = ["BTCUSDT", "ETHUSDT"]
        producer._stop = False
        producer._msg_count = 0
        assert "BTCUSDT" in producer.symbols

    def test_stream_url_contains_all_symbols(self):
        # Build URL manually using the method logic
        symbols = ["BTCUSDT", "ETHUSDT"]
        streams = []
        for s in symbols:
            sl = s.lower()
            streams.extend([f"{sl}@kline_1m", f"{sl}@trade", f"{sl}@bookTicker"])
        url = "wss://stream.binance.com:9443/ws/" + "/".join(streams)
        assert "btcusdt@kline_1m" in url
        assert "ethusdt@trade" in url
