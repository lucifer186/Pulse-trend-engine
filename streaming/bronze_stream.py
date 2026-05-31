"""
Bronze Streaming Job
─────────────────────────────────────────────────────────────
Reads from 3 Kafka topics simultaneously using PySpark
Structured Streaming and writes raw JSON to Delta Lake
Bronze tables in MinIO.

Architecture:
  Kafka (hn-raw, news-raw, github-raw)
      ↓  PySpark readStream
  Parse JSON with explicit schemas
      ↓
  Delta Lake Bronze tables in MinIO
      pulse-bronze/hn/
      pulse-bronze/news/
      pulse-bronze/github/

Why explicit schemas?
  Kafka sends messages as raw bytes. PySpark needs a schema
  to parse the JSON value field into typed columns.
  Using schema inference on streaming data is unreliable
  and slow — always define schemas explicitly in production.

Why Delta Lake?
  Unlike plain Parquet, Delta gives us:
  - ACID transactions (no corrupted files if job crashes)
  - Time travel (query data as it was yesterday)
  - Schema evolution (add columns without breaking old data)
  - Compaction with OPTIMIZE command
"""

import os
import logging
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    ArrayType, BooleanType, TimestampType
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# ── Config from .env ──────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER      = os.getenv("MINIO_ROOT_USER", "pulseadmin")
MINIO_PASSWORD  = os.getenv("MINIO_ROOT_PASSWORD", "pulsepassword123")
MINIO_BUCKET    = os.getenv("MINIO_BUCKET", "pulse-bronze")

# ── MinIO S3 path builder ─────────────────────────
def bronze_path(source: str) -> str:
    """Build the S3 path for a Bronze table."""
    return f"s3a://{MINIO_BUCKET}/{source}/"

# ── Checkpoint path builder ───────────────────────
def checkpoint_path(source: str) -> str:
    """
    Checkpoints are how Spark Structured Streaming tracks
    exactly where it left off in Kafka.
    If the job restarts, it resumes from the checkpoint
    instead of reprocessing everything from the start.
    Stored locally in /tmp/pulse-checkpoints/
    """
    return f"/tmp/pulse-checkpoints/{source}/"


# ══════════════════════════════════════════════════
# SCHEMAS — explicit type definitions for each source
# ══════════════════════════════════════════════════

# HackerNews story schema
HN_SCHEMA = StructType([
    StructField("id",               LongType(),    True),
    StructField("type",             StringType(),  True),
    StructField("by",               StringType(),  True),  # author username
    StructField("title",            StringType(),  True),
    StructField("url",              StringType(),  True),
    StructField("score",            IntegerType(), True),  # upvotes
    StructField("descendants",      IntegerType(), True),  # comment count
    StructField("time",             LongType(),    True),  # unix timestamp
    StructField("text",             StringType(),  True),  # Ask HN body text
    StructField("ingested_at",      StringType(),  True),
    StructField("source",           StringType(),  True),
    StructField("pipeline_version", StringType(),  True),
])

# NewsAPI article schema
NEWS_SCHEMA = StructType([
    StructField("article_id",       StringType(),  True),
    StructField("title",            StringType(),  True),
    StructField("description",      StringType(),  True),
    StructField("url",              StringType(),  True),
    StructField("published_at",     StringType(),  True),
    StructField("author",           StringType(),  True),
    StructField("source_name",      StringType(),  True),
    StructField("query_tag",        StringType(),  True),
    StructField("ingested_at",      StringType(),  True),
    StructField("source",           StringType(),  True),
    StructField("pipeline_version", StringType(),  True),
])

# GitHub repo schema
# topics is an array of strings e.g. ["machine-learning","python"]
GITHUB_SCHEMA = StructType([
    StructField("repo_id",          LongType(),                        True),
    StructField("repo_name",        StringType(),                      True),
    StructField("repo_url",         StringType(),                      True),
    StructField("description",      StringType(),                      True),
    StructField("stars",            IntegerType(),                     True),
    StructField("forks",            IntegerType(),                     True),
    StructField("language",         StringType(),                      True),
    StructField("topics",           ArrayType(StringType()),           True),
    StructField("created_at",       StringType(),                      True),
    StructField("updated_at",       StringType(),                      True),
    StructField("owner",            StringType(),                      True),
    StructField("language_filter",  StringType(),                      True),
    StructField("ingested_at",      StringType(),                      True),
    StructField("source",           StringType(),                      True),
    StructField("pipeline_version", StringType(),                      True),
])


# ══════════════════════════════════════════════════
# SPARK SESSION
# ══════════════════════════════════════════════════

def create_spark_session() -> SparkSession:
    """
    Build and return a SparkSession configured for:
    - Delta Lake (delta extension + catalog)
    - MinIO via S3A connector
    - Kafka source

    Key configs explained:
    - spark.jars.packages: downloads Kafka + Delta + S3A JARs automatically
    - fs.s3a.endpoint: points S3A to MinIO instead of real AWS
    - fs.s3a.path.style.access: MinIO needs path-style, not virtual-hosted
    - delta.autoMerge: allows schema evolution without manual ALTER TABLE
    """
    log.info("Creating Spark session...")

    # These JARs are downloaded automatically by Spark on first run
    # kafka connector, delta core, hadoop S3A, AWS SDK
    packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.367",
    ])

    spark = (
        SparkSession.builder
        .appName("pulse-bronze-stream")
        .master("local[*]")         # use all CPU cores on your laptop

        # ── Delta Lake config ──────────────────
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # ── MinIO / S3A config ─────────────────
        .config("spark.jars.packages", packages)
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # ── Performance tuning ─────────────────
        .config("spark.sql.shuffle.partitions", "4")   # small dataset locally
        .config("spark.driver.memory",          "2g")
        .config("spark.executor.memory",        "2g")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")   # reduce noisy Spark output
    log.info("Spark session created successfully")
    return spark


# ══════════════════════════════════════════════════
# KAFKA READER
# ══════════════════════════════════════════════════

def read_kafka_topic(spark: SparkSession, topic: str):
    """
    Create a streaming DataFrame from a Kafka topic.

    Key options:
    - startingOffsets=latest: only process new messages (not history)
      Change to "earliest" if you want to reprocess everything
    - failOnDataLoss=false: don't crash if Kafka deletes old messages
    - maxOffsetsPerTrigger: limits how many messages per micro-batch
      Prevents memory overload on large backlogs

    The returned DataFrame has these Kafka columns:
      key (binary), value (binary), topic, partition, offset,
      timestamp, timestampType

    We only use 'value' — which contains our JSON string.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe",               topic)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .option("maxOffsetsPerTrigger",    "500")
        .load()
    )


# ══════════════════════════════════════════════════
# STREAM PROCESSORS
# One function per source — parse JSON + add metadata
# ══════════════════════════════════════════════════

def process_hn_stream(spark: SparkSession):
    """
    Read from hn-raw topic, parse JSON, add Bronze metadata,
    write to Delta Lake at s3a://pulse-bronze/hn/

    Processing steps:
    1. Read raw Kafka messages (value = JSON bytes)
    2. Cast value bytes → string
    3. Parse JSON string using HN_SCHEMA
    4. Add bronze_ingested_at column (when Spark processed it)
    5. Write to Delta Lake with append mode
    """
    log.info("Setting up HN stream...")

    raw_df = read_kafka_topic(spark, "hn-raw")

    # Step 1: cast binary value to string
    # Step 2: parse the JSON string into typed columns using schema
    parsed_df = (
        raw_df
        .select(
            F.col("value").cast("string").alias("json_str"),
            F.col("timestamp").alias("kafka_timestamp")
        )
        .select(
            F.from_json(F.col("json_str"), HN_SCHEMA).alias("data"),
            F.col("kafka_timestamp")
        )
        .select(
            "data.*",              # expand all schema fields as columns
            "kafka_timestamp",
            F.current_timestamp().alias("bronze_ingested_at"),
            F.lit("hn-raw").alias("kafka_topic")
        )
        # Drop rows where title is null (malformed messages)
        .filter(F.col("title").isNotNull())
    )

    # Write to Delta Lake — append mode adds new rows without deleting old ones
    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path("hn"))
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")   # micro-batch every 30s
        .start(bronze_path("hn"))
    )

    log.info(f"HN stream started → {bronze_path('hn')}")
    return query


def process_news_stream(spark: SparkSession):
    """
    Read from news-raw topic, parse JSON, write to Delta Bronze.
    Same pattern as HN but with NEWS_SCHEMA.
    """
    log.info("Setting up News stream...")

    raw_df = read_kafka_topic(spark, "news-raw")

    parsed_df = (
        raw_df
        .select(
            F.col("value").cast("string").alias("json_str"),
            F.col("timestamp").alias("kafka_timestamp")
        )
        .select(
            F.from_json(F.col("json_str"), NEWS_SCHEMA).alias("data"),
            F.col("kafka_timestamp")
        )
        .select(
            "data.*",
            "kafka_timestamp",
            F.current_timestamp().alias("bronze_ingested_at"),
            F.lit("news-raw").alias("kafka_topic")
        )
        .filter(F.col("title").isNotNull())
        .filter(F.col("article_id").isNotNull())
    )

    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path("news"))
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .start(bronze_path("news"))
    )

    log.info(f"News stream started → {bronze_path('news')}")
    return query


def process_github_stream(spark: SparkSession):
    """
    Read from github-raw topic, parse JSON, write to Delta Bronze.
    GitHub schema includes an ArrayType field (topics list)
    which Delta Lake handles natively.
    """
    log.info("Setting up GitHub stream...")

    raw_df = read_kafka_topic(spark, "github-raw")

    parsed_df = (
        raw_df
        .select(
            F.col("value").cast("string").alias("json_str"),
            F.col("timestamp").alias("kafka_timestamp")
        )
        .select(
            F.from_json(F.col("json_str"), GITHUB_SCHEMA).alias("data"),
            F.col("kafka_timestamp")
        )
        .select(
            "data.*",
            "kafka_timestamp",
            F.current_timestamp().alias("bronze_ingested_at"),
            F.lit("github-raw").alias("kafka_topic")
        )
        .filter(F.col("repo_id").isNotNull())
    )

    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path("github"))
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .start(bronze_path("github"))
    )

    log.info(f"GitHub stream started → {bronze_path('github')}")
    return query


# ══════════════════════════════════════════════════
# MAIN — start all 3 streams and keep running
# ══════════════════════════════════════════════════

def run_all_streams():
    """
    Start all 3 Bronze streaming queries simultaneously.

    awaitAnyTermination() blocks here and keeps all 3 streams
    running until one of them stops (error or Ctrl+C).
    All 3 write in parallel — Spark handles threading internally.
    """
    spark = create_spark_session()

    log.info("════════════════════════════════════════")
    log.info("  Pulse Bronze Streaming Job starting   ")
    log.info("════════════════════════════════════════")
    log.info(f"Kafka broker : {KAFKA_BROKER}")
    log.info(f"MinIO        : {MINIO_ENDPOINT}")
    log.info(f"Bronze bucket: s3a://{MINIO_BUCKET}/")
    log.info("════════════════════════════════════════")

    # Start all 3 streams
    hn_query     = process_hn_stream(spark)
    news_query   = process_news_stream(spark)
    github_query = process_github_stream(spark)

    log.info("All 3 Bronze streams running!")
    log.info("Open MinIO at http://localhost:9001 to watch data land")
    log.info("Press Ctrl+C to stop")

    try:
        # Block and keep all 3 streams alive
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.info("Stopping all streams...")
        hn_query.stop()
        news_query.stop()
        github_query.stop()
        spark.stop()
        log.info("All streams stopped cleanly")


if __name__ == "__main__":
    run_all_streams()