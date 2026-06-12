"""
tests/test_spark_udfs.py
Unit tests for Spark UDF logic (pure Python functions, no SparkContext).
"""

import json
import sys
import os

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the underlying pure-Python functions (not the udf() wrappers)
from spark.udfs.indicator_udfs import (
    _classify_candle,
    _format_signal,
    _signal_strength,
    _detect_support_resistance,
    _volume_profile,
)


# ─────────────────────────────────────────────
# Candle Classification Tests
# ─────────────────────────────────────────────

class TestCandleClassifier:
    def test_doji_detection(self):
        # Very small body relative to range
        result = _classify_candle(100.0, 105.0, 95.0, 100.2)
        assert result == "DOJI"

    def test_hammer_detection(self):
        # Small body at top, long lower wick
        result = _classify_candle(100.0, 101.0, 90.0, 100.5)
        assert result == "HAMMER"

    def test_shooting_star_detection(self):
        # Small body at bottom, long upper wick, bearish close
        result = _classify_candle(100.0, 110.0, 99.5, 99.8)
        assert result == "SHOOTING_STAR"

    def test_strong_bullish(self):
        # Body > 80% of range
        result = _classify_candle(100.0, 110.0, 99.5, 109.5)
        assert result == "BULLISH"

    def test_strong_bearish(self):
        result = _classify_candle(110.0, 110.5, 100.0, 100.5)
        assert result == "BEARISH"

    def test_neutral_candle(self):
        # Medium-sized body
        result = _classify_candle(100.0, 105.0, 98.0, 103.0)
        assert result == "NEUTRAL"

    def test_none_input_returns_none(self):
        assert _classify_candle(None, 105.0, 95.0, 100.0) is None
        assert _classify_candle(100.0, None, 95.0, 100.0) is None
        assert _classify_candle(100.0, 105.0, None, 100.0) is None
        assert _classify_candle(100.0, 105.0, 95.0, None) is None

    def test_zero_open_returns_none(self):
        assert _classify_candle(0.0, 105.0, 95.0, 100.0) is None


# ─────────────────────────────────────────────
# Signal Strength Tests
# ─────────────────────────────────────────────

class TestSignalStrength:
    def test_strong_signal(self):
        # RSI oversold + MACD active + high volume
        result = _signal_strength(25.0, -100.0, 2.5)
        assert result == "STRONG"

    def test_moderate_signal(self):
        result = _signal_strength(38.0, 0.002, 1.2)
        assert result == "MODERATE"

    def test_weak_signal(self):
        result = _signal_strength(50.0, 0.0001, 1.0)
        assert result == "WEAK"

    def test_all_none_is_weak(self):
        result = _signal_strength(None, None, None)
        assert result == "WEAK"


# ─────────────────────────────────────────────
# Signal Formatter Tests
# ─────────────────────────────────────────────

class TestSignalFormatter:
    def test_format_signal_valid(self):
        result = _format_signal(
            symbol="BTCUSDT",
            action="BUY",
            close=37050.0,
            rsi=28.5,
            macd=-50.0,
            volume_ratio=2.1,
            candle_pattern="HAMMER",
            rule_id="rule-001",
        )
        assert result is not None
        parsed = json.loads(result)
        assert parsed["symbol"] == "BTCUSDT"
        assert parsed["action"] == "BUY"
        assert parsed["close_price"] == 37050.0
        assert parsed["rsi_14"] == 28.5
        assert "signal_type" in parsed

    def test_format_signal_missing_symbol_returns_none(self):
        result = _format_signal(None, "BUY", 37050.0, 28.5, -50.0, 2.1, "HAMMER", "r001")
        assert result is None

    def test_format_signal_none_optionals(self):
        result = _format_signal("BTCUSDT", "SELL", None, None, None, None, None, "r001")
        assert result is not None
        parsed = json.loads(result)
        assert parsed["close_price"] is None
        assert parsed["rsi_14"] is None


# ─────────────────────────────────────────────
# Support / Resistance Tests
# ─────────────────────────────────────────────

class TestSupportResistance:
    def test_pivot_calculation(self):
        result = _detect_support_resistance(38000.0, 36000.0, 37000.0)
        assert result is not None
        levels = json.loads(result)

        expected_pivot = (38000.0 + 36000.0 + 37000.0) / 3
        assert abs(levels["pivot"] - expected_pivot) < 0.01

    def test_r1_above_pivot(self):
        result = json.loads(_detect_support_resistance(38000.0, 36000.0, 37000.0))
        assert result["r1"] > result["pivot"]

    def test_s1_below_pivot(self):
        result = json.loads(_detect_support_resistance(38000.0, 36000.0, 37000.0))
        assert result["s1"] < result["pivot"]

    def test_none_input_returns_none(self):
        assert _detect_support_resistance(None, 36000.0, 37000.0) is None


# ─────────────────────────────────────────────
# Volume Profile Tests
# ─────────────────────────────────────────────

class TestVolumeProfile:
    def test_profile_has_correct_bins(self):
        result = _volume_profile(38000.0, 36000.0, 100.0, bins=10)
        assert result is not None
        profile = json.loads(result)
        assert len(profile) == 10

    def test_total_volume_preserved(self):
        result = json.loads(_volume_profile(38000.0, 36000.0, 100.0, bins=10))
        total  = sum(b["volume"] for b in result)
        assert abs(total - 100.0) < 0.001

    def test_price_levels_in_range(self):
        result = json.loads(_volume_profile(38000.0, 36000.0, 100.0, bins=10))
        for bucket in result:
            assert 36000.0 <= bucket["price_level"] <= 38000.0

    def test_equal_high_low_returns_none(self):
        assert _volume_profile(37000.0, 37000.0, 100.0) is None

    def test_none_input_returns_none(self):
        assert _volume_profile(None, 36000.0, 100.0) is None
