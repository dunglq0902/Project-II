"""
jobs/unified_streaming.py
Unified Streaming Job — Kafka → Bronze → Silver → Gold → Alert (1 Spark driver)

Kiến trúc điều chỉnh: gộp 4 Spark jobs thành 1 foreachBatch pipeline.
Medallion Architecture vẫn giữ nguyên — Bronze/Silver/Gold layers tồn tại
đầy đủ trong Delta Lake trên MinIO.

Chạy:
    spark-submit spark/jobs/unified_streaming.py
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
import time

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, BooleanType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from spark.schemas.bronze_schema import BRONZE_OHLCV_SCHEMA
from spark.utils.spark_session import (
    create_spark_session,
    checkpoint_path,
    delta_path,
    KAFKA_BOOTSTRAP_SERVERS,
)
from spark.udfs.indicator_udfs import udf_classify_candle, udf_calculate_atr, udf_format_signal

logger = logging.getLogger("UnifiedStreaming")

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
KAFKA_TOPICS      = "raw-ohlcv"
TRIGGER_INTERVAL  = "2 minutes"
WATERMARK_DELAY   = "2 minutes"
ALERT_TOPIC       = "alert-events"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "crypto_analytics")

RESAMPLE_INTERVALS = {
    "5m":  5  * 60,
    "15m": 15 * 60,
    "1h":  60 * 60,
    "4h":  4  * 60 * 60,
    "1d":  24 * 60 * 60,
}

# Number of historical rows to load for accurate indicator calculation
INDICATOR_HISTORY_ROWS = 100

# ── Pipeline Startup Timestamp ────────────────────────────────────────────
# Set once in main(). Alerts are only evaluated for data with
# window_start >= this value so that backfilled (historical) data does NOT
# trigger notifications.
PIPELINE_START_TIME: datetime | None = None


def _load_history(spark: SparkSession, new_df: DataFrame) -> DataFrame:
    """Load recent historical data from Delta Bronze and union with new batch.

    Returns (combined_df, has_history). Caller should filter '_is_new' == True
    after computing indicators to only write genuinely new rows.
    """
    path = delta_path("bronze", "ohlcv")
    try:
        hist_df = spark.read.format("delta").load(path)
    except Exception:
        return new_df.withColumn("_is_new", F.lit(True)), False

    pairs = new_df.select("symbol", "interval").distinct().collect()
    if not pairs:
        return new_df.withColumn("_is_new", F.lit(True)), False

    from functools import reduce as _reduce
    frames = []
    for row in pairs:
        sym, intv = row["symbol"], row["interval"]
        subset = (
            hist_df
            .filter((F.col("symbol") == sym) & (F.col("interval") == intv))
            .orderBy(F.col("open_time").desc())
            .limit(INDICATOR_HISTORY_ROWS)
        )
        frames.append(subset)

    history = _reduce(DataFrame.unionByName, frames)
    history_clean = history.join(new_df, ["symbol", "interval", "open_time"], "left_anti")
    new_tagged = new_df.withColumn("_is_new", F.lit(True))
    hist_tagged = history_clean.withColumn("_is_new", F.lit(False))
    combined = new_tagged.unionByName(hist_tagged, allowMissingColumns=True)
    return combined, True

# ═══════════════════════════════════════════════════════════════════════════════
# BRONZE TRANSFORM
# ═══════════════════════════════════════════════════════════════════════════════

def transform_bronze(raw_df: DataFrame) -> DataFrame:
    """Parse Kafka JSON → validate → watermark → enrich partition_date."""
    parsed = (
        raw_df
        .select(
            F.col("key").cast(StringType()).alias("_kafka_key"),
            F.col("timestamp").alias("_kafka_ts"),
            F.from_json(
                F.col("value").cast(StringType()),
                BRONZE_OHLCV_SCHEMA
            ).alias("data")
        )
        .select("_kafka_key", "_kafka_ts", "data.*")
    )

    # Validate critical fields
    valid = parsed.filter(
        F.col("symbol").isNotNull()
        & F.col("open_time").isNotNull()
        & F.col("close").isNotNull()
        & F.col("close").cast("double").isNotNull()
    )

    # Enrich partition_date
    enriched = valid.withColumn(
        "partition_date",
        F.coalesce(F.col("partition_date"), F.to_date(F.col("open_time")))
    )

    return enriched


# ═══════════════════════════════════════════════════════════════════════════════
# SILVER TRANSFORM
# ═══════════════════════════════════════════════════════════════════════════════

def transform_silver(bronze_df: DataFrame) -> DataFrame:
    """Clean + dedup + technical indicators (MA, RSI, MACD, BB, ATR)."""
    # Column pruning
    cols = [
        "symbol", "open_time", "interval",
        "open", "high", "low", "close",
        "volume", "quote_volume", "trade_count",
        "taker_buy_base_vol", "partition_date", "ingest_time",
    ]
    if "close_time" in bronze_df.columns:
        cols.insert(2, "close_time")
    elif "event_time" in bronze_df.columns:
        bronze_df = bronze_df.withColumnRenamed("event_time", "close_time")
        cols.insert(2, "close_time")

    df = bronze_df.select(*[c for c in cols if c in bronze_df.columns])

    # Filter nulls & cast OHLCV to Double
    df = df.filter(
        F.col("symbol").isNotNull()
        & F.col("open_time").isNotNull()
        & F.col("close").isNotNull()
        & F.col("volume").isNotNull()
    )
    for col in ("open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base_vol"):
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # Sanity filters
    df = df.filter(
        (F.col("close") > 0) & (F.col("volume") >= 0) & (F.col("high") >= F.col("low"))
    )

    # Normalize partition_date
    df = df.withColumn("partition_date", F.to_date(F.col("open_time")))

    # ── Technical Indicators (Window-based) ───────────────────────────────
    sym_win = Window.partitionBy("symbol", "interval").orderBy("open_time")

    # Moving Averages
    df = (
        df
        .withColumn("ma7",  F.avg("close").over(sym_win.rowsBetween(-6, 0)))
        .withColumn("ma25", F.avg("close").over(sym_win.rowsBetween(-24, 0)))
        .withColumn("ma99", F.avg("close").over(sym_win.rowsBetween(-98, 0)))
    )

    # Bollinger Bands (20-period)
    bb_win = sym_win.rowsBetween(-19, 0)
    df = (
        df
        .withColumn("bb_middle", F.avg("close").over(bb_win))
        .withColumn("_std20", F.stddev_samp("close").over(bb_win))
        .withColumn("bb_upper", F.col("bb_middle") + 2 * F.col("_std20"))
        .withColumn("bb_lower", F.col("bb_middle") - 2 * F.col("_std20"))
        .drop("_std20")
    )

    # Prev close, ATR
    df = df.withColumn("prev_close", F.lag("close", 1).over(sym_win))
    df = df.withColumn("_tr", udf_calculate_atr(F.col("high"), F.col("low"), F.col("prev_close")))
    df = df.withColumn("atr_14", F.avg("_tr").over(sym_win.rowsBetween(-13, 0))).drop("_tr")

    # Price change %
    df = df.withColumn(
        "price_change_pct",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
            (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100,
        ).otherwise(None)
    )

    # RSI-14 (SMA approximation)
    delta_col = F.col("close") - F.col("prev_close")
    df = df.withColumn("_delta", delta_col)
    df = df.withColumn("_gain", F.when(F.col("_delta") > 0, F.col("_delta")).otherwise(0.0))
    df = df.withColumn("_loss", F.when(F.col("_delta") < 0, -F.col("_delta")).otherwise(0.0))
    rsi_win = sym_win.rowsBetween(-13, 0)
    df = df.withColumn("avg_gain_14", F.avg("_gain").over(rsi_win))
    df = df.withColumn("avg_loss_14", F.avg("_loss").over(rsi_win))
    df = df.withColumn(
        "rsi_14",
        F.when(F.col("avg_loss_14").isNull() | F.col("avg_gain_14").isNull(), None)
        .when(F.col("avg_loss_14") == 0, F.lit(100.0))
        .otherwise(100 - (100 / (1 + (F.col("avg_gain_14") / F.col("avg_loss_14")))))
    ).drop("_delta", "_gain", "_loss", "avg_gain_14", "avg_loss_14")

    # MACD (SMA approximation)
    df = df.withColumn("sma12", F.avg("close").over(sym_win.rowsBetween(-11, 0)))
    df = df.withColumn("sma26", F.avg("close").over(sym_win.rowsBetween(-25, 0)))
    df = df.withColumn("macd", F.col("sma12") - F.col("sma26"))
    df = df.withColumn("macd_signal", F.avg("macd").over(sym_win.rowsBetween(-8, 0)))
    df = df.withColumn("macd_hist", F.col("macd") - F.col("macd_signal")).drop("sma12", "sma26")

    # Volume ratio
    vol_win = sym_win.rowsBetween(-19, 0)
    df = df.withColumn("avg_volume_20", F.avg("volume").over(vol_win))
    df = df.withColumn(
        "volume_ratio",
        F.when(
            F.col("avg_volume_20").isNotNull() & (F.col("avg_volume_20") != 0),
            F.col("volume") / F.col("avg_volume_20"),
        ).otherwise(None)
    )

    # Candle pattern
    df = df.withColumn(
        "candle_pattern",
        udf_classify_candle(F.col("open"), F.col("high"), F.col("low"), F.col("close"))
    )
    df = df.withColumn("processing_time", F.current_timestamp())

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# GOLD TRANSFORM
# ═══════════════════════════════════════════════════════════════════════════════

def _load_gold_history(spark: SparkSession, new_df: DataFrame):
    """Load recent historical data from Delta Gold and union with new batch."""
    path = delta_path("gold", "ohlcv")
    try:
        hist_df = spark.read.format("delta").load(path)
    except Exception:
        return new_df.withColumn("_is_new", F.lit(True)), False

    pairs = new_df.select("symbol", "timeframe").distinct().collect()
    if not pairs:
        return new_df.withColumn("_is_new", F.lit(True)), False

    from functools import reduce as _reduce
    frames = []
    for row in pairs:
        sym, tf = row["symbol"], row["timeframe"]
        subset = (
            hist_df
            .filter((F.col("symbol") == sym) & (F.col("timeframe") == tf))
            .orderBy(F.col("window_start").desc())
            .limit(20)
        )
        frames.append(subset)

    if not frames:
        return new_df.withColumn("_is_new", F.lit(True)), False

    history = _reduce(DataFrame.unionByName, frames)
    # Strip metadata columns added by transform_gold() on prior writes
    # to avoid duplicates when they are re-joined later.
    for drop_col in ("name", "base_asset", "consensus", "cmc_rank", "aggregated_at", "partition_date"):
        if drop_col in history.columns:
            history = history.drop(drop_col)
    history_clean = history.join(new_df, ["symbol", "timeframe", "window_start"], "left_anti")
    new_tagged = new_df.withColumn("_is_new", F.lit(True))
    hist_tagged = history_clean.withColumn("_is_new", F.lit(False))
    combined = new_tagged.unionByName(hist_tagged, allowMissingColumns=True)
    return combined, True


def _seconds_to_duration(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600} hours"
    elif seconds % 60 == 0:
        return f"{seconds // 60} minutes"
    return f"{seconds} seconds"


def transform_gold(silver_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Resample to multi-timeframe + VWAP + window functions + metadata join."""
    from functools import reduce

    all_frames = []
    for name, secs in RESAMPLE_INTERVALS.items():
        dur = _seconds_to_duration(secs)
        agg = (
            silver_df
            .groupBy("symbol", F.window(F.col("open_time"), dur).alias("time_window"))
            .agg(
                F.first("open", ignorenulls=True).alias("open"),
                F.max("high").alias("high"),
                F.min("low").alias("low"),
                F.last("close", ignorenulls=True).alias("close"),
                F.sum("volume").alias("volume"),
                F.sum("quote_volume").alias("quote_volume"),
                F.sum("trade_count").alias("trade_count"),
                F.last("ma7", ignorenulls=True).alias("ma7"),
                F.last("ma25", ignorenulls=True).alias("ma25"),
                F.last("rsi_14", ignorenulls=True).alias("rsi_14"),
                F.last("macd", ignorenulls=True).alias("macd"),
                F.last("macd_signal", ignorenulls=True).alias("macd_signal"),
                F.last("bb_upper", ignorenulls=True).alias("bb_upper"),
                F.last("bb_lower", ignorenulls=True).alias("bb_lower"),
                F.last("atr_14", ignorenulls=True).alias("atr_14"),
                F.last("candle_pattern", ignorenulls=True).alias("candle_pattern"),
            )
            .select(
                "symbol",
                F.lit(name).alias("timeframe"),
                F.col("time_window.start").alias("window_start"),
                F.col("time_window.end").alias("window_end"),
                "open", "high", "low", "close", "volume", "quote_volume",
                "trade_count", "ma7", "ma25", "rsi_14", "macd",
                "macd_signal", "bb_upper", "bb_lower", "atr_14", "candle_pattern",
            )
        )
        all_frames.append(agg)

    if not all_frames:
        return spark.createDataFrame([], all_frames[0].schema) if all_frames else silver_df

    gold_df = reduce(DataFrame.unionByName, all_frames)

    # Load history for Gold layer indicators (VWAP, Volume Ratio)
    combined_gold, has_history = _load_gold_history(spark, gold_df)

    # VWAP
    sym_tf_win = Window.partitionBy("symbol", "timeframe").orderBy("window_start")
    vwap_win = sym_tf_win.rowsBetween(-20, 0)
    combined_gold = combined_gold.withColumn("_tp", (F.col("high") + F.col("low") + F.col("close")) / 3)
    combined_gold = combined_gold.withColumn("_tp_vol", F.col("_tp") * F.col("volume"))
    combined_gold = combined_gold.withColumn(
        "vwap", F.sum("_tp_vol").over(vwap_win) / F.sum("volume").over(vwap_win)
    ).drop("_tp", "_tp_vol")

    # Volume ratio
    vol_win = sym_tf_win.rowsBetween(-19, 0)
    combined_gold = combined_gold.withColumn("avg_volume_20", F.avg("volume").over(vol_win))
    combined_gold = combined_gold.withColumn(
        "volume_ratio",
        F.when(F.col("avg_volume_20").isNotNull() & (F.col("avg_volume_20") != 0),
               F.col("volume") / F.col("avg_volume_20")).otherwise(None)
    ).drop("avg_volume_20")

    # Filter out historical rows to only output the newly evaluated rows
    if has_history and "_is_new" in combined_gold.columns:
        gold_df = combined_gold.filter(F.col("_is_new") == True).drop("_is_new")  # noqa: E712
    else:
        gold_df = combined_gold.drop("_is_new") if "_is_new" in combined_gold.columns else combined_gold

    # Metadata
    gold_df = gold_df.withColumn("partition_date", F.to_date(F.col("window_start")))
    gold_df = gold_df.withColumn("aggregated_at", F.current_timestamp())

    # Broadcast join with symbol metadata
    # Drop pre-existing metadata columns to avoid duplicates when
    # _load_gold_history returns rows that already have these columns.
    metadata_cols = ["name", "base_asset", "consensus", "cmc_rank"]
    for mc in metadata_cols:
        if mc in gold_df.columns:
            gold_df = gold_df.drop(mc)

    metadata = spark.createDataFrame([
        ("BTCUSDT", "Bitcoin",  "BTC", "Proof-of-Work", 1),
        ("ETHUSDT", "Ethereum", "ETH", "Proof-of-Stake", 2),
        ("BNBUSDT", "BNB",     "BNB", "BNB Chain", 3),
        ("SOLUSDT", "Solana",  "SOL", "PoH+PoS", 4),
        ("XRPUSDT", "XRP",    "XRP", "Federated Consensus", 5),
    ], ["symbol", "name", "base_asset", "consensus", "cmc_rank"])

    gold_df = gold_df.join(F.broadcast(metadata), on="symbol", how="left")

    return gold_df


# ═══════════════════════════════════════════════════════════════════════════════
# ALERT EVALUATION
# ═══════════════════════════════════════════════════════════════════════════════

def _evaluate_single(row_data: dict, condition: dict) -> bool:
    """Evaluate one condition against a row dict.

    For crosses_above / crosses_below, row_data must contain 'prev_<field>'.
    """
    field = condition.get("field", "")
    operator = condition.get("operator", "")
    value = condition.get("value")
    current = row_data.get(field)
    if current is None:
        return False
    try:
        if operator == ">":   return float(current) > float(value)
        if operator == "<":   return float(current) < float(value)
        if operator == ">=":  return float(current) >= float(value)
        if operator == "<=":  return float(current) <= float(value)
        if operator == "==":  return str(current) == str(value)
        if operator == "!=":  return str(current) != str(value)
        if operator == "crosses_above":
            prev = row_data.get(f"prev_{field}")
            if prev is None:
                return False
            return float(prev) <= float(value) and float(current) > float(value)
        if operator == "crosses_below":
            prev = row_data.get(f"prev_{field}")
            if prev is None:
                return False
            return float(prev) >= float(value) and float(current) < float(value)
    except (ValueError, TypeError):
        return False
    return False


def evaluate_alerts(gold_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Load alert rules from MongoDB, evaluate against Gold data, return triggered alerts."""
    try:
        rules_df = (
            spark.read
            .format("mongodb")
            .option("spark.mongodb.read.connection.uri", MONGO_URI)
            .option("spark.mongodb.read.database", MONGO_DB)
            .option("spark.mongodb.read.collection", "alert_rules")
            .load()
            .filter(F.col("is_active") == True)  # noqa: E712
            .select(
                "rule_id", "user_id", "symbol", "timeframe", "action",
                "cooldown_seconds", "last_triggered_at",
                F.to_json(F.struct("conditions", "logic")).alias("rule_json"),
            )
        )
    except Exception as exc:
        logger.warning("Could not load alert rules from MongoDB: %s", exc)
        return spark.createDataFrame([], "alert_id STRING")

    if rules_df.isEmpty():
        return spark.createDataFrame([], "alert_id STRING")

    # ── Load previous Gold row for crosses_above / crosses_below ──────────
    try:
        combined, has_history = _load_gold_history(spark, gold_df)
        
        w = Window.partitionBy("symbol", "timeframe").orderBy("window_start")
        combined_with_prev = (
            combined
            .withColumn("prev_close_val", F.lag("close", 1).over(w))
            .withColumn("prev_rsi_14", F.lag("rsi_14", 1).over(w))
            .withColumn("prev_macd", F.lag("macd", 1).over(w))
            .withColumn("prev_macd_signal", F.lag("macd_signal", 1).over(w))
        )
        if has_history and "_is_new" in combined_with_prev.columns:
            gold_df = combined_with_prev.filter(F.col("_is_new") == True).drop("_is_new")
        else:
            gold_df = combined_with_prev.drop("_is_new") if "_is_new" in combined_with_prev.columns else combined_with_prev
    except Exception as exc:
        logger.warning("Could not calculate previous Gold values: %s", exc)
        # No Gold history yet — crosses operators will return False
        gold_df = (
            gold_df
            .withColumn("prev_close_val", F.lit(None).cast(DoubleType()))
            .withColumn("prev_rsi_14", F.lit(None).cast(DoubleType()))
            .withColumn("prev_macd", F.lit(None).cast(DoubleType()))
            .withColumn("prev_macd_signal", F.lit(None).cast(DoubleType()))
        )

    # Rule evaluation UDF — now includes prev_ fields for crosses operators
    def _eval_rule(rule_json, close, rsi, macd, macd_signal, volume_ratio, candle_pattern,
                   ma7, ma25, bb_upper, bb_lower, vwap,
                   prev_close_v, prev_rsi, prev_macd_v, prev_macd_sig):
        if not rule_json:
            return False
        try:
            rule = json.loads(rule_json)
        except json.JSONDecodeError:
            return False
        row_data = {
            "close": close, "rsi_14": rsi, "macd": macd, "macd_signal": macd_signal,
            "volume_ratio": volume_ratio, "candle_pattern": candle_pattern,
            "ma7": ma7, "ma25": ma25, "bb_upper": bb_upper, "bb_lower": bb_lower, "vwap": vwap,
            # prev_ fields for crosses_above / crosses_below
            "prev_close": prev_close_v, "prev_rsi_14": prev_rsi,
            "prev_macd": prev_macd_v, "prev_macd_signal": prev_macd_sig,
        }
        conditions = rule.get("conditions", [])
        logic = rule.get("logic", "AND").upper()
        if not conditions:
            return False
        results = [_evaluate_single(row_data, c) for c in conditions]
        return all(results) if logic == "AND" else any(results)

    rule_eval_udf = F.udf(_eval_rule, BooleanType())

    joined = gold_df.join(F.broadcast(rules_df), on=["symbol", "timeframe"], how="inner")
    evaluated = joined.withColumn(
        "condition_met",
        rule_eval_udf(
            "rule_json", "close", "rsi_14", "macd", "macd_signal",
            "volume_ratio", "candle_pattern", "ma7", "ma25", "bb_upper", "bb_lower", "vwap",
            "prev_close_val", "prev_rsi_14", "prev_macd", "prev_macd_signal"
        )
    ).filter(F.col("condition_met") == True)  # noqa: E712

    # Cooldown
    now_ts = F.current_timestamp()
    with_cooldown = evaluated.filter(
        F.col("last_triggered_at").isNull()
        | (F.unix_timestamp(now_ts) - F.unix_timestamp(F.col("last_triggered_at"))
           > F.col("cooldown_seconds"))
    )

    alert_df = with_cooldown.select(
        F.expr("uuid()").alias("alert_id"),
        "rule_id", "user_id", "symbol", "timeframe", "action",
        now_ts.alias("triggered_at"),
        F.col("close").alias("close_price"),
        "rsi_14", "macd", "volume_ratio", "candle_pattern",
        "window_start",
        udf_format_signal(
            "symbol", "action", "close", "rsi_14", "macd",
            "volume_ratio", "candle_pattern", "rule_id"
        ).alias("message"),
    )

    # Deduplicate alerts: if a single rule triggers multiple times in the same batch,
    # keep only the most recent signal (highest window_start).
    dedup_win = Window.partitionBy("rule_id").orderBy(F.col("window_start").desc())
    alert_df = (
        alert_df
        .withColumn("_rn", F.row_number().over(dedup_win))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "window_start")
    )

    return alert_df


# ═══════════════════════════════════════════════════════════════════════════════
# UNIFIED foreachBatch PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def process_batch(batch_df: DataFrame, batch_id: int):
    """Called for each micro-batch — runs full Medallion pipeline."""
    try:
        if batch_df.isEmpty():
            return

        spark = batch_df.sparkSession
        logger.info("Processing batch %d...", batch_id)

        # ── Bronze ────────────────────────────────────────────────────────────
        bronze_df = transform_bronze(batch_df)
        bronze_df.cache()
        bronze_df.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("symbol", "partition_date") \
            .save(delta_path("bronze", "ohlcv"))
        logger.info("Batch %d: Bronze written.", batch_id)

        # ── Silver (Fix #1: load history for accurate indicators) ─────────
        combined_df, has_history = _load_history(spark, bronze_df)
        silver_full = transform_silver(combined_df)
        # Only write NEW rows, not the historical ones we loaded for context
        if has_history and "_is_new" in silver_full.columns:
            silver_df = silver_full.filter(F.col("_is_new") == True).drop("_is_new")  # noqa: E712
        else:
            silver_df = silver_full.drop("_is_new") if "_is_new" in silver_full.columns else silver_full
        
        silver_df.cache()
        silver_df.write.format("delta").mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("symbol", "partition_date") \
            .save(delta_path("silver", "ohlcv"))
        logger.info("Batch %d: Silver written.", batch_id)

        # ── Gold (Fix #2: MERGE upsert instead of append) ────────────────
        gold_df = transform_gold(silver_df, spark)
        if gold_df is not None and not gold_df.isEmpty():
            gold_df.cache()
            gold_path = delta_path("gold", "ohlcv")
            # Use Delta MERGE to upsert — avoids duplicates for in-progress windows
            try:
                from delta.tables import DeltaTable
                gold_table = DeltaTable.forPath(spark, gold_path)
                gold_table.alias("tgt").merge(
                    gold_df.alias("src"),
                    """tgt.symbol = src.symbol
                       AND tgt.timeframe = src.timeframe
                       AND tgt.window_start = src.window_start"""
                ).whenMatchedUpdate(set={
                    "high":           "src.high",
                    "low":            "src.low",
                    "close":          "src.close",
                    "volume":         "src.volume",
                    "quote_volume":   "src.quote_volume",
                    "trade_count":    "src.trade_count",
                    "ma7":            "src.ma7",
                    "ma25":           "src.ma25",
                    "rsi_14":         "src.rsi_14",
                    "macd":           "src.macd",
                    "macd_signal":    "src.macd_signal",
                    "bb_upper":       "src.bb_upper",
                    "bb_lower":       "src.bb_lower",
                    "atr_14":         "src.atr_14",
                    "candle_pattern": "src.candle_pattern",
                    "vwap":           "src.vwap",
                    "volume_ratio":   "src.volume_ratio",
                    "aggregated_at":  "src.aggregated_at",
                }).whenNotMatchedInsertAll().execute()
            except Exception:
                # First run — table doesn't exist yet, fall back to write
                gold_df.write.format("delta").mode("append") \
                    .option("mergeSchema", "true") \
                    .partitionBy("symbol", "timeframe", "partition_date") \
                    .save(gold_path)
            logger.info("Batch %d: Gold written to Delta.", batch_id)

            # ── Sync Gold to MongoDB (upsert to avoid duplicates) ─────────
            try:
                (
                    gold_df.write.format("mongodb")
                    .mode("append")
                    .option("spark.mongodb.write.connection.uri", MONGO_URI)
                    .option("spark.mongodb.write.database", MONGO_DB)
                    .option("spark.mongodb.write.collection", "gold_ohlcv")
                    .option("spark.mongodb.write.operationType", "replace")
                    .option("spark.mongodb.write.replaceDocument.shardKey",
                            '{"symbol": 1, "timeframe": 1, "window_start": 1}')
                    .save()
                )
                logger.info("Batch %d: Gold written to MongoDB.", batch_id)
            except Exception as mongo_exc:
                logger.warning("Batch %d: Failed to sync Gold to MongoDB: %s", batch_id, mongo_exc)

            # ── Alert Evaluation (only for REAL-TIME data) ────────────────────
            # Backfill data (window_start < PIPELINE_START_TIME) is processed
            # through Bronze → Silver → Gold → MongoDB for dashboard display,
            # but alerts/notifications are suppressed to avoid stale triggers.
            global PIPELINE_START_TIME
            realtime_gold = gold_df
            if PIPELINE_START_TIME is not None:
                realtime_gold = gold_df.filter(
                    F.col("window_start") >= F.lit(PIPELINE_START_TIME)
                )

            try:
                if realtime_gold.isEmpty():
                    logger.info(
                        "Batch %d: All data is backfill (before %s). "
                        "Alerts skipped — data saved to dashboard only.",
                        batch_id,
                        PIPELINE_START_TIME.strftime("%Y-%m-%d %H:%M UTC") if PIPELINE_START_TIME else "N/A",
                    )
                else:
                    alert_df = evaluate_alerts(realtime_gold, spark)
                    if alert_df is not None and "alert_id" in alert_df.columns:
                        alert_df.cache()
                        if not alert_df.isEmpty():
                            alert_count = alert_df.count()
                            logger.info("Batch %d: %d alerts triggered.", batch_id, alert_count)

                        # Collect rule_ids for cooldown update BEFORE writing
                        triggered_rules = alert_df.select("rule_id").distinct().collect()

                        # Publish to Kafka
                        (
                            alert_df
                            .select(
                                F.col("symbol").alias("key"),
                                F.to_json(F.struct(
                                    "alert_id", "rule_id", "user_id", "symbol", "timeframe",
                                    "action", "triggered_at", "close_price",
                                    "rsi_14", "macd", "volume_ratio", "candle_pattern", "message",
                                )).alias("value"),
                            )
                            .write.format("kafka")
                            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                            .option("topic", ALERT_TOPIC)
                            .save()
                        )

                        # Audit trail (Delta)
                        alert_df.write.format("delta").mode("append") \
                            .save(delta_path("gold", "alert_events"))

                        # Sync alerts to MongoDB (for Dashboard History)
                        try:
                            (
                                alert_df.write.format("mongodb")
                                .mode("append")
                                .option("spark.mongodb.write.connection.uri", MONGO_URI)
                                .option("spark.mongodb.write.database", MONGO_DB)
                                .option("spark.mongodb.write.collection", "alert_events")
                                .save()
                            )
                            logger.info("Batch %d: Alerts synced to MongoDB.", batch_id)
                        except Exception as mongo_exc:
                            logger.warning("Batch %d: Failed to sync Alerts to MongoDB: %s", batch_id, mongo_exc)

                        # ── Fix #4: Update cooldown in MongoDB alert_rules ────
                        try:
                            from pymongo import MongoClient, UpdateOne
                            mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
                            db = mongo_client[MONGO_DB]
                            now = datetime.now(timezone.utc)
                            ops = [
                                UpdateOne(
                                    {"rule_id": r["rule_id"]},
                                    {"$set": {"last_triggered_at": now},
                                     "$inc": {"trigger_count": 1}},
                                )
                                for r in triggered_rules
                            ]
                            if ops:
                                result = db["alert_rules"].bulk_write(ops)
                                logger.info(
                                    "Batch %d: Updated cooldown for %d rules (%d modified).",
                                    batch_id, len(ops), result.modified_count,
                                )
                            mongo_client.close()
                        except Exception as cooldown_exc:
                            logger.warning("Batch %d: Failed to update cooldown: %s", batch_id, cooldown_exc)

                    if alert_df is not None:
                        alert_df.unpersist()

            except Exception as exc:
                logger.warning("Alert evaluation skipped: %s", exc)

        logger.info("Batch %d complete.", batch_id)

    except Exception as exc:  # Catch any error in a micro-batch to avoid stopping the stream
        logger.exception("Processing of batch %d failed and will be skipped: %s", batch_id, exc)
    finally:
        if 'bronze_df' in locals() and bronze_df is not None:
            try: bronze_df.unpersist()
            except Exception: pass
        if 'silver_df' in locals() and silver_df is not None:
            try: silver_df.unpersist()
            except Exception: pass
        if 'gold_df' in locals() and gold_df is not None:
            try: gold_df.unpersist()
            except Exception: pass


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    global PIPELINE_START_TIME
    PIPELINE_START_TIME = datetime.now(timezone.utc)

    spark = create_spark_session(
        app_name="CryptoAnalytics-Unified-Streaming",
        extra_configs={
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.mongodb.read.connection.uri": MONGO_URI,
        }
    )

    logger.info(
        "Starting Unified Streaming Pipeline (Bronze → Silver → Gold → Alerts)..."
    )
    logger.info(
        "PIPELINE_START_TIME = %s — alerts will only fire for data after this timestamp.",
        PIPELINE_START_TIME.strftime("%Y-%m-%d %H:%M:%S UTC"),
    )

    # Run the streaming query in a restart loop so transient errors don't stop the container
    while True:
        try:
            stream = (
                spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPICS)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", 50_000) # Spark kéo tối đa 50.000 tin nhắn (records) từ Kafka topic raw-ohlcv để nạp vào đối tượng batch_df
                .load()
            )

            query = (
                stream
                .writeStream
                .foreachBatch(process_batch)
                .option("checkpointLocation", checkpoint_path("unified_streaming"))
                .trigger(processingTime=TRIGGER_INTERVAL)
                .queryName("unified_medallion_pipeline")
                .start()
            )

            logger.info("Unified streaming query started: %s", query.name)
            query.awaitTermination()

        except KeyboardInterrupt:
            logger.info("Shutdown requested, stopping streaming pipeline.")
            try:
                if 'query' in locals() and query.isActive:
                    query.stop()
            except Exception:
                pass
            break
        except Exception as exc:
            logger.exception("Streaming query failed with error: %s", exc)
            # Nếu gặp lỗi nghiêm trọng (như SparkContext sập), thoát luôn
            # để Kubernetes tự động khởi tạo lại một Pod mới sạch sẽ.
            import sys
            sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    )
    main()
