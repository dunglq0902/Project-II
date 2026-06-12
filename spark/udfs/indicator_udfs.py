"""
udfs/indicator_udfs.py
Custom UDFs và UDAFs cho technical indicators:
  - udf_classify_candle    : Phân loại mẫu nến (Doji, Hammer, Shooting Star, Engulfing)
  - udf_calculate_atr      : Average True Range (ATR-14)
  - udf_format_signal      : Format alert signal thành JSON chuẩn
  - rsi_udf                : Tính RSI-14 trên cửa sổ sliding (dùng pandas_udf)
  - macd_udf               : Tính MACD trên cửa sổ (dùng pandas_udf)
"""

import json
import math
from typing import Iterator, Optional

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DoubleType, BooleanType,
    StructType, StructField,
)
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType


# ═══════════════════════════════════════════════════════════════════════════════
# 1. CANDLE CLASSIFICATION UDF
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_candle(
    open_: Optional[float],
    high:  Optional[float],
    low:   Optional[float],
    close: Optional[float],
) -> Optional[str]:
    """
    Classify a single candlestick into a named pattern.

    Returns one of:
        DOJI, HAMMER, SHOOTING_STAR, BULLISH_ENGULFING, BEARISH_ENGULFING,
        BULLISH, BEARISH, NEUTRAL
    """
    if None in (open_, high, low, close):
        return None
    if open_ == 0:
        return None

    body        = abs(close - open_)
    total_range = high - low if high != low else 1e-9
    upper_wick  = high - max(open_, close)
    lower_wick  = min(open_, close) - low
    body_ratio  = body / total_range

    # Hammer – small body at top, long lower wick and relatively small upper wick
    if lower_wick >= 2 * body and upper_wick <= (0.5 * lower_wick) and close > open_:
        return "HAMMER"

    # Shooting Star – small body at bottom, long upper wick and relatively small lower wick
    if upper_wick >= 2 * body and lower_wick <= (0.5 * upper_wick) and close < open_:
        return "SHOOTING_STAR"

    # Doji – body is very small relative to range
    if body_ratio < 0.05:
        return "DOJI"

    # Marubozu-style strong candles (body > 80% of range)
    if body_ratio > 0.8:
        return "BULLISH" if close > open_ else "BEARISH"

    return "NEUTRAL"


udf_classify_candle = udf(_classify_candle, StringType())


# ═══════════════════════════════════════════════════════════════════════════════
# 2. ATR UDF  (uses pandas_udf on a window of rows)
# ═══════════════════════════════════════════════════════════════════════════════

@pandas_udf(DoubleType())
def udf_calculate_atr(
    high:      pd.Series,
    low:       pd.Series,
    prev_close:pd.Series,
) -> pd.Series:
    """
    Compute True Range = max(high-low, |high-prev_close|, |low-prev_close|).
    This UDF is applied per-row; ATR-14 rolling mean is applied via Window in the job.
    """
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr


# ═══════════════════════════════════════════════════════════════════════════════
# 3. RSI pandas_udf  (Grouped Map – per symbol window)
# ═══════════════════════════════════════════════════════════════════════════════

RSI_SCHEMA = StructType([
    StructField("symbol",    StringType(), nullable=False),
    StructField("open_time", StringType(), nullable=False),   # will be cast to timestamp
    StructField("rsi_14",    DoubleType(), nullable=True),
])


@pandas_udf(DoubleType())
def udf_rsi_14(close: pd.Series) -> pd.Series:
    """
    Wilder's RSI(14) over an ordered close-price series.
    Apply with:
        window = Window.partitionBy("symbol").orderBy("open_time")
        df.withColumn("rsi_14", udf_rsi_14(F.collect_list("close").over(window)))
    NOTE: For a proper rolling RSI use applyInPandas on a grouped DataFrame.
    """
    period = 14
    delta  = close.diff()
    gain   = delta.clip(lower=0)
    loss   = (-delta).clip(lower=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs  = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi


# ═══════════════════════════════════════════════════════════════════════════════
# 4. MACD pandas_udf
# ═══════════════════════════════════════════════════════════════════════════════

@pandas_udf(DoubleType())
def udf_macd_line(close: pd.Series) -> pd.Series:
    """MACD line = EMA(12) - EMA(26)."""
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    return ema12 - ema26


@pandas_udf(DoubleType())
def udf_macd_signal(macd: pd.Series) -> pd.Series:
    """Signal line = EMA(9) of MACD."""
    return macd.ewm(span=9, adjust=False).mean()


# ═══════════════════════════════════════════════════════════════════════════════
# 5. ALERT SIGNAL FORMATTER UDF
# ═══════════════════════════════════════════════════════════════════════════════

def _format_signal(
    symbol:        Optional[str],
    action:        Optional[str],
    close:         Optional[float],
    rsi:           Optional[float],
    macd:          Optional[float],
    volume_ratio:  Optional[float],
    candle_pattern:Optional[str],
    rule_id:       Optional[str],
) -> Optional[str]:
    """
    Serialize a trading signal into a standardized JSON string
    ready for the notification service to consume.
    """
    if not symbol or not action:
        return None

    payload = {
        "symbol":         symbol,
        "action":         action,
        "close_price":    round(close, 4)        if close         is not None else None,
        "rsi_14":         round(rsi, 2)          if rsi           is not None else None,
        "macd":           round(macd, 4)         if macd          is not None else None,
        "volume_ratio":   round(volume_ratio, 2) if volume_ratio  is not None else None,
        "candle_pattern": candle_pattern,
        "rule_id":        rule_id,
        "signal_type":    _signal_strength(rsi, macd, volume_ratio),
    }
    return json.dumps(payload, ensure_ascii=False)


def _signal_strength(rsi, macd, volume_ratio) -> str:
    """Heuristic: assign STRONG / MODERATE / WEAK."""
    score = 0
    if rsi is not None:
        if rsi < 30 or rsi > 70:
            score += 2
        elif rsi < 40 or rsi > 60:
            score += 1
    if macd is not None and abs(macd) > 0.001:
        score += 1
    if volume_ratio is not None and volume_ratio > 1.5:
        score += 1
    if score >= 3:
        return "STRONG"
    elif score >= 2:
        return "MODERATE"
    return "WEAK"


udf_format_signal = udf(_format_signal, StringType())


# ═══════════════════════════════════════════════════════════════════════════════
# 6. SUPPORT / RESISTANCE DETECTION UDF  (simplified pivot-point method)
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_support_resistance(
    high:  Optional[float],
    low:   Optional[float],
    close: Optional[float],
) -> Optional[str]:
    """
    Classic pivot point levels: P, R1, R2, S1, S2.
    Returns JSON string with pivot levels.
    """
    if None in (high, low, close):
        return None

    pivot = (high + low + close) / 3
    r1    = 2 * pivot - low
    r2    = pivot + (high - low)
    s1    = 2 * pivot - high
    s2    = pivot - (high - low)

    levels = {
        "pivot": round(pivot, 4),
        "r1":    round(r1, 4),
        "r2":    round(r2, 4),
        "s1":    round(s1, 4),
        "s2":    round(s2, 4),
    }
    return json.dumps(levels)


udf_detect_support_resistance = udf(_detect_support_resistance, StringType())


# ═══════════════════════════════════════════════════════════════════════════════
# 7. VOLUME PROFILE UDF  (price-level volume distribution)
# ═══════════════════════════════════════════════════════════════════════════════

def _volume_profile(
    high:   Optional[float],
    low:    Optional[float],
    volume: Optional[float],
    bins:   int = 10,
) -> Optional[str]:
    """
    Distribute the candle's volume uniformly across `bins` price buckets.
    Returns a JSON array of {price_level, volume} objects.
    """
    if None in (high, low, volume) or high == low:
        return None

    step    = (high - low) / bins
    profile = []
    vol_per_bin = volume / bins
    for i in range(bins):
        level = low + step * (i + 0.5)
        profile.append({"price_level": round(level, 4), "volume": round(vol_per_bin, 4)})

    return json.dumps(profile)


udf_volume_profile = udf(_volume_profile, StringType())
