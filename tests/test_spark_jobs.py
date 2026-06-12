"""
tests/test_spark_jobs.py
Integration tests for Spark jobs — require a local SparkSession.
Run with:  pytest tests/test_spark_jobs.py -m spark -v

These tests spin up a local Spark session (local[2]) and exercise the core
transformation logic from silver_clean.py and gold_aggregate.py without
hitting S3/MinIO or Kafka. Delta Lake writes go to a temp directory.
"""

import os
import sys
import tempfile
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Skip Spark integration tests on Windows where Hadoop/winutils is not available
if os.name == "nt":
    import pytest as _pytest
    _pytest.skip("Skipping Spark integration tests on Windows (requires winutils/HADOOP_HOME)", allow_module_level=True)

# ─────────────────────────────────────────────
# Skip entire module if PySpark not installed
# ─────────────────────────────────────────────
pyspark = pytest.importorskip("pyspark", reason="PySpark not installed – skipping Spark tests")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType


# ─────────────────────────────────────────────
# Shared SparkSession fixture (session-scoped for speed)
# ─────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Create a minimal local SparkSession with Delta Lake support."""
    # Ensure HADOOP_HOME is set on Windows to avoid winutils startup error.
    # Create a temporary Hadoop home with a `bin` folder and a stubbed
    # `winutils.exe` so Spark can initialize on Windows CI/dev machines.
    if os.name == "nt":
        if not os.environ.get("HADOOP_HOME"):
            tmp_hadoop = tempfile.mkdtemp(prefix="hadoop_home_")
            bin_dir = os.path.join(tmp_hadoop, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            os.environ["HADOOP_HOME"] = tmp_hadoop

    session = (
        SparkSession.builder
        # Use a single local worker on Windows to avoid multiprocessing socket
        # issues in test environments. Multiple workers are still used in CI
        # when appropriate.
        .master("local[1]" if os.name == "nt" else "local[2]")
        .appName("CryptoAnalytics-Tests")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")    # Small for tests
        .config("spark.driver.memory",          "1g")
        .config("spark.ui.enabled",             "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def raw_bronze_df(spark):
    """Create a small Bronze-like DataFrame for testing."""
    data = [
        ("BTCUSDT", datetime(2024, 1, 15, 12, 0, 0), datetime(2024, 1, 15, 12, 0, 0),
         "1m", 37000.0, 37100.0, 36950.0, 37050.0, 10.5, 388525.5, 250, 5.2,
         True, "websocket", "2024-01-15",
         datetime(2024, 1, 15, 12, 1, 0)),
        # Duplicate – should be removed in silver_clean
        ("BTCUSDT", datetime(2024, 1, 15, 12, 0, 0), datetime(2024, 1, 15, 12, 0, 0),
         "1m", 37000.0, 37100.0, 36950.0, 37050.0, 10.5, 388525.5, 250, 5.2,
         True, "websocket", "2024-01-15",
         datetime(2024, 1, 15, 12, 1, 5)),
        # Null close – should be filtered out
        ("BTCUSDT", datetime(2024, 1, 15, 12, 1, 0), datetime(2024, 1, 15, 12, 1, 0),
         "1m", None, None, None, None, None, None, None, None,
         False, "websocket", "2024-01-15",
         datetime(2024, 1, 15, 12, 2, 0)),
        ("ETHUSDT", datetime(2024, 1, 15, 12, 0, 0), datetime(2024, 1, 15, 12, 0, 0),
         "1m", 2200.0, 2210.0, 2195.0, 2205.0, 50.0, 110250.0, 120, 25.0,
         True, "websocket", "2024-01-15",
         datetime(2024, 1, 15, 12, 1, 0)),
    ]
    columns = [
        "symbol", "open_time", "event_time", "interval",
        "open", "high", "low", "close",
        "volume", "quote_volume", "trade_count", "taker_buy_base_vol",
        "is_closed", "source", "partition_date", "ingest_time",
    ]
    return spark.createDataFrame(data, columns)


# ─────────────────────────────────────────────
# Silver Clean Tests
# ─────────────────────────────────────────────

@pytest.mark.spark
class TestSilverClean:

    def test_deduplication_removes_duplicates(self, spark, raw_bronze_df):
        from spark.jobs.silver_clean import clean
        result = clean(raw_bronze_df)
        btc_rows = result.filter(F.col("symbol") == "BTCUSDT").count()
        # Duplicate and null row both removed → 1 valid BTC row
        assert btc_rows == 1

    def test_null_close_rows_filtered(self, spark, raw_bronze_df):
        from spark.jobs.silver_clean import clean
        result = clean(raw_bronze_df)
        null_close = result.filter(F.col("close").isNull()).count()
        assert null_close == 0

    def test_partition_date_is_date_type(self, spark, raw_bronze_df):
        from spark.jobs.silver_clean import clean
        result = clean(raw_bronze_df)
        field = [f for f in result.schema.fields if f.name == "partition_date"][0]
        from pyspark.sql.types import DateType
        assert isinstance(field.dataType, DateType)

    def test_price_columns_are_double(self, spark, raw_bronze_df):
        from spark.jobs.silver_clean import clean
        result = clean(raw_bronze_df)
        schema = {f.name: f.dataType for f in result.schema.fields}
        for col in ("open", "high", "low", "close", "volume"):
            assert isinstance(schema[col], DoubleType), f"{col} should be DoubleType"

    def test_negative_price_filtered(self, spark):
        """Rows with negative close price should be dropped."""
        from spark.jobs.silver_clean import clean
        data = [
            ("BTCUSDT", datetime(2024, 1, 15, 12, 0, 0), datetime(2024, 1, 15, 12, 0, 0),
             "1m", 37000.0, 37100.0, 36950.0, -100.0, 10.5, 0.0, 250, 5.2,
             True, "websocket", "2024-01-15", datetime(2024, 1, 15, 12, 1, 0)),
        ]
        columns = [
            "symbol", "open_time", "event_time", "interval",
            "open", "high", "low", "close", "volume", "quote_volume",
            "trade_count", "taker_buy_base_vol", "is_closed", "source",
            "partition_date", "ingest_time",
        ]
        df = spark.createDataFrame(data, columns)
        result = clean(df)
        assert result.count() == 0

    def test_enrich_adds_ma_columns(self, spark, raw_bronze_df):
        from spark.jobs.silver_clean import clean, enrich_indicators
        cleaned = clean(raw_bronze_df)
        # Need enough rows for indicators; generate 30 rows for BTCUSDT
        rows = []
        for i in range(30):
            rows.append((
                "BTCUSDT",
                datetime(2024, 1, 15, 12, i, 0), datetime(2024, 1, 15, 12, i, 0),
                "1m", 37000.0 + i, 37100.0 + i, 36950.0 + i, 37050.0 + i,
                10.5, 388525.5, 250, 5.2, True, "websocket", "2024-01-15",
                datetime(2024, 1, 15, 12, i + 1, 0),
            ))
        big_df = spark.createDataFrame(rows, raw_bronze_df.columns)
        big_cleaned = clean(big_df)
        enriched = enrich_indicators(big_cleaned)

        cols = set(enriched.columns)
        for expected in ("ma7", "ma25", "bb_upper", "bb_lower", "volume_ratio", "prev_close"):
            assert expected in cols, f"Expected column '{expected}' missing from enriched DF"


# ─────────────────────────────────────────────
# Gold Aggregate Tests
# ─────────────────────────────────────────────

@pytest.fixture
def silver_df_30rows(spark):
    """30 rows of clean Silver data for aggregation tests."""
    rows = []
    for i in range(30):
        rows.append((
            "BTCUSDT", "1m",
            datetime(2024, 1, 15, 12, i, 0), datetime(2024, 1, 15, 12, i, 59),
            37000.0 + i, 37100.0 + i, 36950.0 + i, 37050.0 + i,
            10.5 + i * 0.1, 388525.5, 250,
            36800.0, 37200.0, 38000.0, 0.0, 37000.0,
            28.0, -50.0, -45.0, 38200.0, 36000.0, 150.0, "HAMMER",
            2.1, "2024-01-15", datetime(2024, 1, 15, 12, i + 1, 0),
        ))
    columns = [
        "symbol", "interval", "open_time", "close_time",
        "open", "high", "low", "close", "volume", "quote_volume", "trade_count",
        "ma7", "ma25", "bb_upper", "bb_lower", "bb_middle",
        "rsi_14", "macd", "macd_signal", "bb_upper2", "bb_lower2", "atr_14",
        "candle_pattern", "volume_ratio", "partition_date", "ingest_time",
    ]
    # Simpler schema for aggregation test
    simple_rows = []
    for i in range(30):
        simple_rows.append((
            "BTCUSDT", datetime(2024, 1, 15, 12, i, 0),
            37000.0 + i, 37100.0 + i, 36950.0 + i, 37050.0 + i,
            10.5 + i * 0.1, 388525.5, 250,
            36800.0, 37200.0, 37000.0,
            28.0, -50.0, -45.0, 38000.0, 36000.0, 150.0,
            "HAMMER", 2.1, "2024-01-15",
        ))
    simple_cols = [
        "symbol", "open_time", "open", "high", "low", "close",
        "volume", "quote_volume", "trade_count",
        "ma7", "ma25", "ma99", "rsi_14", "macd", "macd_signal",
        "bb_upper", "bb_lower", "atr_14", "candle_pattern", "volume_ratio",
        "partition_date",
    ]
    return spark.createDataFrame(simple_rows, simple_cols)


@pytest.mark.spark
class TestGoldAggregate:

    def test_resample_produces_fewer_rows(self, spark, silver_df_30rows):
        from spark.jobs.gold_aggregate import resample_ohlcv
        result = resample_ohlcv(silver_df_30rows, "5m", 5 * 60)
        # 30 minutes of 1m data → max 6 five-minute windows
        assert result.count() <= 6

    def test_resample_columns_present(self, spark, silver_df_30rows):
        from spark.jobs.gold_aggregate import resample_ohlcv
        result = resample_ohlcv(silver_df_30rows, "5m", 5 * 60)
        required = {"symbol", "timeframe", "window_start", "window_end",
                    "open", "high", "low", "close", "volume"}
        assert required.issubset(set(result.columns))

    def test_resample_high_gte_low(self, spark, silver_df_30rows):
        from spark.jobs.gold_aggregate import resample_ohlcv
        result = resample_ohlcv(silver_df_30rows, "5m", 5 * 60)
        invalid = result.filter(F.col("high") < F.col("low")).count()
        assert invalid == 0

    def test_resample_timeframe_label_correct(self, spark, silver_df_30rows):
        from spark.jobs.gold_aggregate import resample_ohlcv
        result = resample_ohlcv(silver_df_30rows, "1h", 3600)
        timeframes = [r.timeframe for r in result.select("timeframe").collect()]
        assert all(tf == "1h" for tf in timeframes)

    def test_window_functions_add_vwap(self, spark, silver_df_30rows):
        from spark.jobs.gold_aggregate import resample_ohlcv, apply_window_functions
        resampled = resample_ohlcv(silver_df_30rows, "5m", 5 * 60)
        result    = apply_window_functions(resampled)
        assert "vwap" in result.columns
        # VWAP should be a positive number close to the price range
        vwap_vals = [r.vwap for r in result.select("vwap").collect() if r.vwap is not None]
        for v in vwap_vals:
            assert v > 30000, f"VWAP {v} seems too low for BTCUSDT"

    def test_window_functions_add_price_rank(self, spark, silver_df_30rows):
        from spark.jobs.gold_aggregate import resample_ohlcv, apply_window_functions
        resampled = resample_ohlcv(silver_df_30rows, "5m", 5 * 60)
        result    = apply_window_functions(resampled)
        assert "price_rank" in result.columns

    def test_pivot_returns_symbol_column(self, spark, silver_df_30rows):
        from spark.jobs.gold_aggregate import resample_ohlcv, apply_window_functions, pivot_metrics
        resampled = resample_ohlcv(silver_df_30rows, "1h", 3600)
        windowed  = apply_window_functions(resampled)
        pivoted   = pivot_metrics(windowed)
        # The pivoted DF should have a column named after the symbol
        assert "BTCUSDT" in pivoted.columns or "window_start" in pivoted.columns

    def test_unpivot_produces_metric_column(self, spark, silver_df_30rows):
        from spark.jobs.gold_aggregate import resample_ohlcv, apply_window_functions, unpivot_metrics
        resampled = resample_ohlcv(silver_df_30rows, "5m", 5 * 60)
        windowed  = apply_window_functions(resampled)
        long_df   = unpivot_metrics(windowed)
        assert "metric_name" in long_df.columns
        assert "metric_value" in long_df.columns
        metric_names = {r.metric_name for r in long_df.select("metric_name").distinct().collect()}
        assert "close" in metric_names
        assert "rsi_14" in metric_names


# ─────────────────────────────────────────────
# UDF Integration Tests (with SparkSession)
# ─────────────────────────────────────────────

@pytest.mark.spark
class TestUDFsWithSpark:

    def test_candle_udf_in_spark(self, spark):
        from spark.udfs.indicator_udfs import udf_classify_candle
        data = [
            (100.0, 105.0, 95.0, 100.2),   # Doji
            (100.0, 101.0, 90.0, 100.5),   # Hammer
            (100.0, 110.0, 99.5, 100.1),   # Shooting star (bearish close)
        ]
        df = spark.createDataFrame(data, ["open", "high", "low", "close"])
        result = df.withColumn(
            "pattern",
            udf_classify_candle(F.col("open"), F.col("high"), F.col("low"), F.col("close"))
        )
        patterns = [r.pattern for r in result.select("pattern").collect()]
        assert "DOJI" in patterns
        assert "HAMMER" in patterns

    def test_format_signal_udf_in_spark(self, spark):
        from spark.udfs.indicator_udfs import udf_format_signal
        import json as _json
        data = [("BTCUSDT", "BUY", 37050.0, 28.5, -50.0, 2.1, "HAMMER", "rule-001")]
        df = spark.createDataFrame(
            data, ["symbol", "action", "close", "rsi_14", "macd",
                   "volume_ratio", "candle_pattern", "rule_id"]
        )
        result = df.withColumn(
            "signal",
            udf_format_signal(
                F.col("symbol"), F.col("action"), F.col("close"),
                F.col("rsi_14"), F.col("macd"), F.col("volume_ratio"),
                F.col("candle_pattern"), F.col("rule_id"),
            )
        )
        signal_json = result.select("signal").collect()[0].signal
        assert signal_json is not None
        parsed = _json.loads(signal_json)
        assert parsed["symbol"] == "BTCUSDT"
        assert parsed["action"] == "BUY"
        assert "signal_type" in parsed
