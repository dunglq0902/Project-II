"""
tests/conftest.py
Shared pytest fixtures for unit and integration tests.
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure project root is on sys.path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


# ─────────────────────────────────────────────
# Raw data fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def sample_ohlcv_row() -> Dict[str, Any]:
    """Single 1-min OHLCV row as it would appear in the Silver layer."""
    return {
        "symbol":           "BTCUSDT",
        "timeframe":        "1m",
        "open_time":        datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        "close_time":       datetime(2024, 1, 15, 12, 0, 59, tzinfo=timezone.utc),
        "interval":         "1m",
        "open":             37000.0,
        "high":             37100.0,
        "low":              36950.0,
        "close":            37050.0,
        "volume":           10.5,
        "quote_volume":     388525.5,
        "trade_count":      250,
        "taker_buy_base_vol": 5.2,
        "partition_date":   "2024-01-15",
        "ma7":              36900.0,
        "ma25":             37200.0,
        "ma99":             36500.0,
        "bb_upper":         38000.0,
        "bb_lower":         36000.0,
        "bb_middle":        37000.0,
        "rsi_14":           28.5,
        "macd":             -50.0,
        "macd_signal":      -45.0,
        "macd_hist":        -5.0,
        "atr_14":           150.0,
        "avg_volume_20":    5.0,
        "volume_ratio":     2.1,
        "candle_pattern":   "HAMMER",
        "price_change_pct": 0.14,
        "prev_close":       37000.0,
        "ingest_time":      datetime(2024, 1, 15, 12, 1, 5, tzinfo=timezone.utc),
        "processing_time":  datetime(2024, 1, 15, 12, 1, 10, tzinfo=timezone.utc),
    }


@pytest.fixture
def sample_gold_row(sample_ohlcv_row) -> Dict[str, Any]:
    """Gold layer aggregated row (1h timeframe)."""
    row = {**sample_ohlcv_row}
    row.update({
        "timeframe":    "1h",
        "window_start": datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        "window_end":   datetime(2024, 1, 15, 13, 0, 0, tzinfo=timezone.utc),
        "vwap":         37100.0,
        "price_rank":   1,
        "cumulative_volume": 630.0,
        "volume_dense_rank": 1,
        "aggregated_at": datetime(2024, 1, 15, 13, 5, 0, tzinfo=timezone.utc),
    })
    return row


@pytest.fixture
def sample_alert_rule() -> Dict[str, Any]:
    """A complete alert rule document as stored in MongoDB."""
    return {
        "rule_id":              "test-rule-001",
        "user_id":              "test-user-001",
        "symbol":               "BTCUSDT",
        "timeframe":            "1h",
        "logic":                "AND",
        "action":               "BUY",
        "is_active":            True,
        "cooldown_seconds":     300,
        "last_triggered_at":    None,
        "trigger_count":        0,
        "notification_channels":["email"],
        "email_address":        "luudungpkt922005@gmail.com",
        "created_at":           datetime(2024, 1, 1, tzinfo=timezone.utc),
        "updated_at":           datetime(2024, 1, 1, tzinfo=timezone.utc),
        "conditions": [
            {"field": "rsi_14",         "operator": "<",  "value": 30},
            {"field": "volume_ratio",   "operator": ">",  "value": 1.5},
            {"field": "candle_pattern", "operator": "==", "value": "HAMMER"},
        ],
    }


@pytest.fixture
def sample_alert_event() -> Dict[str, Any]:
    """A triggered alert event ready for dispatch."""
    return {
        "alert_id":       "alert-uuid-001",
        "rule_id":        "test-rule-001",
        "user_id":        "test-user-001",
        "symbol":         "BTCUSDT",
        "timeframe":      "1h",
        "action":         "BUY",
        "triggered_at":   datetime(2024, 1, 15, 12, 5, 0, tzinfo=timezone.utc),
        "close_price":    37050.0,
        "rsi_14":         28.5,
        "macd":           -50.0,
        "volume_ratio":   2.1,
        "candle_pattern": "HAMMER",
        "message":        json.dumps({
            "symbol": "BTCUSDT", "action": "BUY",
            "close_price": 37050.0, "rsi_14": 28.5,
            "signal_type": "STRONG",
        }),
    }


@pytest.fixture
def multi_symbol_rows(sample_ohlcv_row) -> List[Dict[str, Any]]:
    """Multiple rows across different symbols for batch evaluation tests."""
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    rows = []
    for symbol in symbols:
        row = {**sample_ohlcv_row, "symbol": symbol}
        rows.append(row)
    return rows


@pytest.fixture
def multi_rule_set(sample_alert_rule) -> List[Dict[str, Any]]:
    """Set of rules for different symbols and conditions."""
    return [
        sample_alert_rule,
        {
            **sample_alert_rule,
            "rule_id":   "test-rule-002",
            "symbol":    "ETHUSDT",
            "action":    "SELL",
            "conditions": [
                {"field": "rsi_14", "operator": ">", "value": 70},
            ],
        },
        {
            **sample_alert_rule,
            "rule_id":    "test-rule-003",
            "is_active":  False,    # Should be skipped
        },
    ]


# ─────────────────────────────────────────────
# Mock fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def mock_kafka_producer():
    """Mock confluent_kafka Producer."""
    with patch("confluent_kafka.Producer") as mock:
        producer = MagicMock()
        mock.return_value = producer
        producer.produce = MagicMock()
        producer.poll    = MagicMock(return_value=0)
        producer.flush   = MagicMock(return_value=0)
        yield producer


@pytest.fixture
def mock_mongo_collection():
    """Mock Motor async MongoDB collection."""
    collection = AsyncMock()
    collection.find_one        = AsyncMock(return_value=None)
    collection.insert_one      = AsyncMock(return_value=MagicMock(inserted_id="abc123"))
    collection.update_one      = AsyncMock(return_value=MagicMock(matched_count=1, modified_count=1))
    collection.delete_one      = AsyncMock(return_value=MagicMock(deleted_count=1))
    collection.count_documents = AsyncMock(return_value=0)
    return collection


@pytest.fixture
def mock_httpx_client():
    """Mock httpx.AsyncClient for notification tests."""
    with patch("httpx.AsyncClient") as mock_cls:
        client   = AsyncMock()
        response = MagicMock()
        response.status_code = 200
        response.text        = "OK"
        response.raise_for_status = MagicMock()
        client.__aenter__    = AsyncMock(return_value=client)
        client.__aexit__     = AsyncMock(return_value=None)
        client.post          = AsyncMock(return_value=response)
        mock_cls.return_value = client
        yield client, response


# ─────────────────────────────────────────────
# pytest configuration
# ─────────────────────────────────────────────

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (require running services)"
    )
    config.addinivalue_line(
        "markers", "spark: marks tests that require a SparkSession"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow-running"
    )
