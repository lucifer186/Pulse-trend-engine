"""
Gold Batch Job
────────────────────────────────────────────────
Reads unified Silver Delta tables (hn + news + github)
Computes 4 Gold aggregation tables:
  1. trending_topics   → pulse-gold/trending_topics/
  2. language_momentum → pulse-gold/language_momentum/
  3. source_activity   → pulse-gold/source_activity/
  4. top_content       → pulse-gold/top_content/

Run manually : python transforms/gold_job.py
Scheduled by : Airflow DAG (gold_dag.py) every hour
"""

import os
import logging
from dotenv import load_dotenv
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from transforms.gold_transforms import (
    build_trending_topics,
    build_language_momentum,
    build_source_activity,
    build_top_content
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "pulseadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "pulsepassword123")
SILVER_BUCKET  = "pulse-silver"
GOLD_BUCKET    = "pulse-gold"

def s3(bucket, path=""):
    return f"s3a://{bucket}/{path}"


def create_spark() -> SparkSession:
    packages = ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.367",
    ])
    spark = (
        SparkSession.builder
        .appName("pulse-gold-job")
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",              packages)
        .config("spark.hadoop.fs.s3a.endpoint",     MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",   MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",   MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory",          "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_silver_combined(spark: SparkSession) -> DataFrame:
    """
    Read all 3 Silver tables and union them into
    one combined DataFrame.

    This is the single input for all Gold transforms.
    Because all 3 Silver tables share the same schema
    (our unified Silver design from Step 6), union works
    perfectly without any column mapping needed.
    """
    log.info("Loading Silver tables...")

    hn     = spark.read.format("delta").load(s3(SILVER_BUCKET, "hn/"))
    news   = spark.read.format("delta").load(s3(SILVER_BUCKET, "news/"))
    github = spark.read.format("delta").load(s3(SILVER_BUCKET, "github/"))

    combined = hn.union(news).union(github)
    window = Window.partitionBy("unified_id").orderBy(
        F.col("silver_processed_at").desc()
    )
    combined = (
        combined
        .withColumn("_rank", F.rank().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    enriched_path = s3(SILVER_BUCKET, "enriched/")
    try:
        enriched = (
            spark.read.format("delta")
            .load(enriched_path)
            .select(
                "unified_id",
                "sentiment",
                "ai_topics",
                "ai_entities",
                "enrichment_error"
            )
        )
        # Left join — rows without enrichment keep null sentiment
        combined = combined.join(enriched, on="unified_id", how="left")

        enriched_count = enriched.count()
        log.info(f"Enriched rows joined: {enriched_count:,}")

        # Show sentiment distribution
        combined.groupBy("source", "sentiment").count().show()

    except Exception as e:
        log.warning(
            f"No enriched Silver found — running without AI signals. "
            f"Run ai_enrichment.py first. Error: {e}"
        )

    total = combined.count()
    log.info(f"Combined Silver rows (with enrichment): {total:,}")
    combined.groupBy("source").count().show()
    combined.cache()

    return combined


def write_gold(df: DataFrame, table_name: str):
    """
    Write a Gold DataFrame to Delta Lake.
    Gold tables use overwrite mode — they are fully
    recomputed on every run (not incremental like Silver MERGE).

    Why overwrite for Gold?
    Gold = aggregations. Aggregations must be recomputed
    from scratch each run because old scores become stale.
    A trend that was #1 yesterday may be #20 today.
    """
    gold_path = s3(GOLD_BUCKET, f"{table_name}/")
    log.info(f"Writing Gold table: {table_name} → {gold_path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", "1=1")   # full overwrite, no old data kept
        .save(gold_path)
    )

    count = spark_count(df)
    log.info(f"  ✓ {table_name}: {count:,} rows written")
    return count


def spark_count(df: DataFrame) -> int:
    """Safe count — returns 0 if df is empty."""
    try:
        return df.count()
    except Exception:
        return 0


def run_gold_job():
    """
    Full Gold pipeline:
    1. Load + combine all 3 Silver tables
    2. Build all 4 Gold aggregations
    3. Write each to pulse-gold/ in MinIO
    4. Print summary with row counts
    """
    log.info("══════════════════════════════════════")
    log.info("  Pulse Gold Job starting             ")
    log.info("══════════════════════════════════════")

    spark    = create_spark()
    combined = load_silver_combined(spark)

    counts = {}

    # ── Gold Table 1: Trending Topics ───────────
    log.info("Building trending_topics...")
    trending = build_trending_topics(combined)
    counts["trending_topics"] = write_gold(trending, "trending_topics")

    # ── Gold Table 2: Language Momentum ─────────
    log.info("Building language_momentum...")
    momentum = build_language_momentum(combined)
    counts["language_momentum"] = write_gold(momentum, "language_momentum")

    # ── Gold Table 3: Source Activity ───────────
    log.info("Building source_activity...")
    activity = build_source_activity(combined)
    counts["source_activity"] = write_gold(activity, "source_activity")

    # ── Gold Table 4: Top Content ────────────────
    log.info("Building top_content...")
    top = build_top_content(combined)
    counts["top_content"] = write_gold(top, "top_content")

    # ── Summary ──────────────────────────────────
    log.info("══════════════════════════════════════")
    log.info("  Gold Job Complete — Row Counts      ")
    log.info("══════════════════════════════════════")
    for table, count in counts.items():
        log.info(f"  {table:<25}: {count:,} rows")
    log.info("══════════════════════════════════════")

    combined.unpersist()   # release cached df from memory
    spark.stop()
    log.info("Gold job finished successfully")


if __name__ == "__main__":
    run_gold_job()