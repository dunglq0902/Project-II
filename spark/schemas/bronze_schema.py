"""
schemas/bronze_schema.py
Spark DataFrame schemas cho Bronze, Silver, Gold layer.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, DateType,
    DoubleType, LongType, IntegerType, BooleanType,
    ArrayType, MapType,
)

# ─────────────────────────────────────────────
# BRONZE – Raw tick / OHLCV from Kafka
# ─────────────────────────────────────────────
BRONZE_OHLCV_SCHEMA = StructType([
    StructField("symbol",               StringType(),    nullable=False),
    StructField("event_time",           TimestampType(), nullable=False),
    StructField("ingest_time",          TimestampType(), nullable=False),
    StructField("open_time",            TimestampType(), nullable=False),
    StructField("close_time",           TimestampType(), nullable=True),
    StructField("interval",             StringType(),    nullable=True),
    StructField("open",                 DoubleType(),    nullable=True),
    StructField("high",                 DoubleType(),    nullable=True),
    StructField("low",                  DoubleType(),    nullable=True),
    StructField("close",                DoubleType(),    nullable=False),
    StructField("volume",               DoubleType(),    nullable=True),
    StructField("quote_volume",         DoubleType(),    nullable=True),
    StructField("trade_count",          IntegerType(),   nullable=True),
    StructField("taker_buy_base_vol",   DoubleType(),    nullable=True),
    StructField("taker_buy_quote_vol",  DoubleType(),    nullable=True),
    StructField("is_closed",            BooleanType(),   nullable=True),
    StructField("source",               StringType(),    nullable=True),
    StructField("partition_date",       DateType(),      nullable=True),
])

BRONZE_TICK_SCHEMA = StructType([
    StructField("symbol",          StringType(),    nullable=False),
    StructField("event_time",      TimestampType(), nullable=False),
    StructField("ingest_time",     TimestampType(), nullable=False),
    StructField("trade_id",        LongType(),      nullable=True),
    StructField("price",           DoubleType(),    nullable=False),
    StructField("quantity",        DoubleType(),    nullable=True),
    StructField("buyer_order_id",  LongType(),      nullable=True),
    StructField("seller_order_id", LongType(),      nullable=True),
    StructField("trade_time",      TimestampType(), nullable=True),
    StructField("is_buyer_maker",  BooleanType(),   nullable=True),
    StructField("source",          StringType(),    nullable=True),
])

BRONZE_ORDERBOOK_SCHEMA = StructType([
    StructField("symbol",       StringType(),    nullable=False),
    StructField("ingest_time",  TimestampType(), nullable=False),
    StructField("update_id",    LongType(),      nullable=True),
    StructField("best_bid",     DoubleType(),    nullable=True),
    StructField("best_bid_qty", DoubleType(),    nullable=True),
    StructField("best_ask",     DoubleType(),    nullable=True),
    StructField("best_ask_qty", DoubleType(),    nullable=True),
    StructField("source",       StringType(),    nullable=True),
])

#==============================================================================================
# All schema below are being unused. They are left for upgrade in the future.

# ─────────────────────────────────────────────
# SILVER – Cleaned + enriched with indicators
# ─────────────────────────────────────────────
SILVER_OHLCV_SCHEMA = StructType([
    # Inherited from Bronze
    StructField("symbol",              StringType(),    nullable=False),
    StructField("open_time",           TimestampType(), nullable=False),
    StructField("close_time",          TimestampType(), nullable=True),
    StructField("interval",            StringType(),    nullable=False),
    StructField("open",                DoubleType(),    nullable=False),
    StructField("high",                DoubleType(),    nullable=False),
    StructField("low",                 DoubleType(),    nullable=False),
    StructField("close",               DoubleType(),    nullable=False),
    StructField("volume",              DoubleType(),    nullable=False),
    StructField("quote_volume",        DoubleType(),    nullable=True),
    StructField("trade_count",         IntegerType(),   nullable=True),
    StructField("taker_buy_base_vol",  DoubleType(),    nullable=True),
    StructField("partition_date",      DateType(),      nullable=False),
    # Moving Averages
    StructField("ma7",                 DoubleType(),    nullable=True),
    StructField("ma25",                DoubleType(),    nullable=True),
    StructField("ma99",                DoubleType(),    nullable=True),
    # Bollinger Bands
    StructField("bb_upper",            DoubleType(),    nullable=True),
    StructField("bb_lower",            DoubleType(),    nullable=True),
    StructField("bb_middle",           DoubleType(),    nullable=True),
    # Momentum indicators
    StructField("rsi_14",              DoubleType(),    nullable=True),
    StructField("macd",                DoubleType(),    nullable=True),
    StructField("macd_signal",         DoubleType(),    nullable=True),
    StructField("macd_hist",           DoubleType(),    nullable=True),
    # ATR (volatility)
    StructField("atr_14",              DoubleType(),    nullable=True),
    # Volume indicators
    StructField("avg_volume_20",       DoubleType(),    nullable=True),
    StructField("volume_ratio",        DoubleType(),    nullable=True),   # volume / avg_volume_20
    # Candle classification
    StructField("candle_pattern",      StringType(),    nullable=True),
    # Price changes
    StructField("price_change_pct",    DoubleType(),    nullable=True),
    StructField("prev_close",          DoubleType(),    nullable=True),
    # Metadata
    StructField("ingest_time",         TimestampType(), nullable=True),
    StructField("processing_time",     TimestampType(), nullable=True),
])

# ─────────────────────────────────────────────
# GOLD – Aggregated, analysis-ready
# ─────────────────────────────────────────────
GOLD_OHLCV_SCHEMA = StructType([
    StructField("symbol",              StringType(),    nullable=False),
    StructField("timeframe",           StringType(),    nullable=False),   # 5m, 15m, 1h, 4h, 1d
    StructField("window_start",        TimestampType(), nullable=False),
    StructField("window_end",          TimestampType(), nullable=False),
    StructField("open",                DoubleType(),    nullable=False),
    StructField("high",                DoubleType(),    nullable=False),
    StructField("low",                 DoubleType(),    nullable=False),
    StructField("close",               DoubleType(),    nullable=False),
    StructField("volume",              DoubleType(),    nullable=False),
    StructField("quote_volume",        DoubleType(),    nullable=True),
    StructField("trade_count",         LongType(),      nullable=True),
    StructField("vwap",                DoubleType(),    nullable=True),    # volume-weighted avg price
    StructField("ma7",                 DoubleType(),    nullable=True),
    StructField("ma25",                DoubleType(),    nullable=True),
    StructField("ma99",                DoubleType(),    nullable=True),
    StructField("rsi_14",              DoubleType(),    nullable=True),
    StructField("macd",                DoubleType(),    nullable=True),
    StructField("macd_signal",         DoubleType(),    nullable=True),
    StructField("bb_upper",            DoubleType(),    nullable=True),
    StructField("bb_lower",            DoubleType(),    nullable=True),
    StructField("atr_14",              DoubleType(),    nullable=True),
    StructField("volume_ratio",        DoubleType(),    nullable=True),
    StructField("candle_pattern",      StringType(),    nullable=True),
    StructField("price_rank",          IntegerType(),   nullable=True),    # rank by volume among symbols
    StructField("partition_date",      DateType(),      nullable=False),
    StructField("aggregated_at",       TimestampType(), nullable=True),
])

# ─────────────────────────────────────────────
# ALERT EVENT schema (written to Kafka alert-events topic)
# ─────────────────────────────────────────────
ALERT_EVENT_SCHEMA = StructType([
    StructField("alert_id",       StringType(),    nullable=False),
    StructField("rule_id",        StringType(),    nullable=False),
    StructField("user_id",        StringType(),    nullable=False),
    StructField("symbol",         StringType(),    nullable=False),
    StructField("timeframe",      StringType(),    nullable=False),
    StructField("action",         StringType(),    nullable=False),   # BUY / SELL / WATCH
    StructField("triggered_at",   TimestampType(), nullable=False),
    StructField("close_price",    DoubleType(),    nullable=True),
    StructField("rsi_14",         DoubleType(),    nullable=True),
    StructField("macd",           DoubleType(),    nullable=True),
    StructField("volume_ratio",   DoubleType(),    nullable=True),
    StructField("candle_pattern", StringType(),    nullable=True),
    StructField("conditions_met", StringType(),    nullable=True),   # JSON string
    StructField("message",        StringType(),    nullable=True),
])
