"""
utils/spark_session.py
SparkSession factory và helper utilities.

Kiến trúc điều chỉnh:
  - Giảm RAM: driver 512m, executor 1g (phù hợp 16GB laptop)
  - shuffle.partitions=4 (thay vì 200) — giảm overhead dữ liệu nhỏ
  - AQE enabled + coalesce partitions
  - Dynamic allocation disabled (single worker)
"""

import os
import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

logger = logging.getLogger("SparkUtils")

# ─────────────────────────────────────────────
# Environment / config
# ─────────────────────────────────────────────
SPARK_MASTER:             str = os.getenv("SPARK_MASTER",              "")
S3_ENDPOINT:              str = os.getenv("S3_ENDPOINT",               "http://minio:9000")
KAFKA_BOOTSTRAP_SERVERS:  str = os.getenv("KAFKA_BOOTSTRAP_SERVERS",   "localhost:9092")
DELTA_LAKE_VERSION:       str = os.getenv("DELTA_LAKE_VERSION",        "3.2.0")
SCALA_VERSION:            str = os.getenv("SCALA_VERSION",             "2.12")
SPARK_VERSION:            str = os.getenv("SPARK_VERSION",             "3.5.2")

CHECKPOINT_BASE:          str = os.getenv("SPARK_CHECKPOINT_BASE",     "s3a://crypto-lake/checkpoints")
DELTA_BASE:               str = os.getenv("DELTA_BASE_PATH",           "s3a://crypto-lake/lakehouse")

# Delta table paths
BRONZE_PATH: str = f"{DELTA_BASE}/bronze"
SILVER_PATH: str = f"{DELTA_BASE}/silver"
GOLD_PATH:   str = f"{DELTA_BASE}/gold"


# ─────────────────────────────────────────────
# SparkSession Factory
# ─────────────────────────────────────────────
def create_spark_session(
    app_name: str,
    extra_configs: Optional[dict] = None,
    enable_hive: bool = False,
) -> SparkSession:
    """
    Create and return a configured SparkSession with:
      - Delta Lake support
      - Kafka connector
      - AQE enabled
      - Hadoop / S3 settings
      - Optimized memory for 16GB laptop
    """
    delta_pkg = f"io.delta:delta-spark_{SCALA_VERSION}:{DELTA_LAKE_VERSION}"
    kafka_pkg  = (
        f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
    )
    mongo_pkg  = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"

    conf = SparkConf()
    conf.setAppName(app_name)
    if SPARK_MASTER:
        conf.setMaster(SPARK_MASTER)

    # ── Packages ──────────────────────────────────────────────────────────
    # conf.set("spark.jars.packages", f"{delta_pkg},{kafka_pkg},{mongo_pkg}")

    # ── Delta Lake ────────────────────────────────────────────────────────
    conf.set("spark.sql.extensions",
             "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog",
             "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # ── Adaptive Query Execution ──────────────────────────────────────────
    conf.set("spark.sql.adaptive.enabled",                         "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled",      "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled",                "true")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled",      "true")

    # ── Shuffle & Serialization (optimized for small data) ────────────────
    conf.set("spark.serializer",                   "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.shuffle.partitions",       "4")      # was 200, critical for small data
    conf.set("spark.default.parallelism",          "4")      # was 200

    # ── Memory (optimized for 16GB laptop) ────────────────────────────────
    conf.set("spark.driver.memory",                "512m")   # was 2g
    conf.set("spark.executor.memory",              "1g")     # was 4g
    conf.set("spark.executor.cores",               "2")
    conf.set("spark.memory.fraction",              "0.8")
    conf.set("spark.memory.storageFraction",       "0.3")

    # ── Broadcast join threshold ──────────────────────────────────────────
    conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))   # 10 MB

    # ── S3 / MinIO ────────────────────────────────────────────────────────
    conf.set("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin"))
    conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "password"))
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # S3A / Delta helpers: use directory committer + S3SingleDriverLogStore for Delta on MinIO
    conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
    conf.set("spark.hadoop.fs.s3a.buffer.dir", "/tmp/s3a")
    conf.set("spark.hadoop.fs.s3a.committer.name", "directory")
    conf.set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
    conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    conf.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")

    # ── Kafka source ──────────────────────────────────────────────────────
    conf.set("spark.kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)

    # ── Extra configs ─────────────────────────────────────────────────────
    if extra_configs:
        for k, v in extra_configs.items():
            conf.set(k, v)

    builder = SparkSession.builder.config(conf=conf)
    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession created | app=%s master=%s", app_name, SPARK_MASTER)
    return spark


# ─────────────────────────────────────────────
# Helper: Explain Plan
# ─────────────────────────────────────────────
def explain_df(df, mode: str = "extended"):
    """Print the physical / logical plan for debugging."""
    df.explain(mode)


# ─────────────────────────────────────────────
# Helper: Checkpoint path builder
# ─────────────────────────────────────────────
def checkpoint_path(job_name: str) -> str:
    return f"{CHECKPOINT_BASE.rstrip('/')}/{job_name}"


# ─────────────────────────────────────────────
# Helper: Delta table path builder
# ─────────────────────────────────────────────
def delta_path(layer: str, table: str) -> str:
    """
    Return the Delta Lake path for a given layer and table name.
    e.g.: delta_path("silver", "ohlcv") → s3a://crypto-lake/lakehouse/silver/ohlcv
    """
    base = {
        "bronze": BRONZE_PATH,
        "silver": SILVER_PATH,
        "gold":   GOLD_PATH,
    }.get(layer, DELTA_BASE)
    return f"{base.rstrip('/')}/{table}"
