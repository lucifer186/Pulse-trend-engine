"""
Silver Batch Job
────────────────────────────────────────────────
Reads Bronze Delta tables (hn, news, github)
Applies cleaning + normalisation transforms
Writes unified Silver Delta table to MinIO

Run manually: python transforms/silver_job.py
Later: Airflow DAG will call this on schedule
"""

import os
import logging
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from transforms.silver_transforms import (
    deduplicate,
    clean_text,
    parse_timestamp,
    make_unified_id,
    drop_empty_titles,
    clean_url
)

# from silver_transforms import (
#     deduplicate,
#     clean_text,
#     parse_timestamp,
#     make_unified_id,
#     drop_empty_titles,
#     clean_url
# )

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "pulseadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "pulsepassword123")

BRONZE_BUCKET  = "pulse-bronze"
SILVER_BUCKET  = "pulse-silver"

def s3(bucket, path=""):
    return f"s3a://{bucket}/{path}"


# ── Spark Session ─────────────────────────────────
def create_spark():
    packages = ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.367",
    ])
    spark = (
        SparkSession.builder
        .appName("pulse-silver-job")
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", packages)
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ══════════════════════════════════════════════════
# SOURCE-SPECIFIC TRANSFORMS
# Each function: reads Bronze → returns unified Silver df
# ══════════════════════════════════════════════════

def transform_hn(spark: SparkSession) -> DataFrame:
    """
    Read Bronze HN → map to unified Silver schema.

    HN-specific mappings:
    - id (long)          → unified_id via MD5
    - by                 → author
    - score              → popularity_score
    - descendants        → comment_count
    - time (unix epoch)  → published_at (UTC timestamp)
    - type               → tags as single-element array
    - text               → description (Ask HN posts have body text)
    """
    log.info("Transforming HN Bronze → Silver...")

    df = spark.read.format("delta").load(s3(BRONZE_BUCKET, "hn/"))
    log.info(f"  HN Bronze rows read: {df.count()}")

    transformed = (
        df
        .withColumn("unified_id",      make_unified_id("hackernews", "id"))
        .withColumn("title",           clean_text("title"))
        .withColumn("description",     clean_text("text"))        # Ask HN body
        .withColumn("url",             clean_url("url"))
        .withColumn("author",          clean_text("by"))
        .withColumn("popularity_score",F.col("score").cast("integer"))
        # .withColumn("comment_count",   F.col("descendants").cast("integer"))
        .withColumn("tags",            F.array(F.lit("hackernews"), F.col("type")))
        .withColumn("language",        F.lit(None).cast(StringType()))
        .withColumn("published_at",    parse_timestamp("time"))
        .withColumn("ingested_at",     F.col("ingested_at").cast("timestamp"))
        .withColumn("silver_processed_at", F.current_timestamp())
        .select(
            "unified_id", "source", "title", "description", "url",
            "author", "popularity_score", "tags",
            "language", "published_at", "ingested_at",
            "silver_processed_at", "pipeline_version"
        )
    )

    # Clean: drop nulls and deduplicate
    transformed = drop_empty_titles(transformed)
    transformed = deduplicate(transformed, "unified_id")

    log.info(f"  HN Silver rows after clean: {transformed.count()}")
    return transformed


def transform_news(spark: SparkSession) -> DataFrame:
    """
    Read Bronze News → map to unified Silver schema.

    NewsAPI-specific mappings:
    - article_id         → unified_id via MD5
    - author             → author (already a string)
    - source_name        → added to tags alongside query_tag
    - published_at       → parsed as ISO timestamp
    - 0                  → popularity_score (no score in NewsAPI)
    - description        → description
    """
    log.info("Transforming News Bronze → Silver...")

    df = spark.read.format("delta").load(s3(BRONZE_BUCKET, "news/"))
    log.info(f"  News Bronze rows read: {df.count()}")

    transformed = (
        df
        .withColumn("unified_id",      make_unified_id("newsapi", "article_id"))
        .withColumn("title",           clean_text("title"))
        .withColumn("description",     clean_text("description"))
        .withColumn("url",             clean_url("url"))
        .withColumn("author",          clean_text("author"))
        .withColumn("popularity_score",F.lit(0).cast("integer"))
        # .withColumn("comment_count",   F.lit(0).cast("integer"))
        # Build tags array: ["newsapi", source_name, query_tag]
        # filter_nulls ensures we don't get null elements in array
        .withColumn("tags",
            F.array_remove(
                F.array(
                    F.lit("newsapi"),
                    F.col("source_name"),
                    F.col("query_tag")
                ),
                ""          # remove empty string elements
            )
        )
        .withColumn("language",        F.lit(None).cast(StringType()))
        .withColumn("published_at",    parse_timestamp("published_at"))
        .withColumn("ingested_at",     F.col("ingested_at").cast("timestamp"))
        .withColumn("silver_processed_at", F.current_timestamp())
        .select(
            "unified_id", "source", "title", "description", "url",
            "author", "popularity_score", "tags",
            "language", "published_at", "ingested_at",
            "silver_processed_at", "pipeline_version"
        )
    )

    transformed = drop_empty_titles(transformed)
    transformed = deduplicate(transformed, "unified_id")

    log.info(f"  News Silver rows after clean: {transformed.count()}")
    return transformed


def transform_github(spark: SparkSession) -> DataFrame:
    """
    Read Bronze GitHub → map to unified Silver schema.

    GitHub-specific mappings:
    - repo_id            → unified_id via MD5
    - owner              → author
    - stars              → popularity_score
     - open_issues        → comment_count (closest proxy)
    - topics (array)     → tags (already array, just add "github" prefix)
    - language           → language (kept as-is)
    - description        → description
    - repo_name          → title (repo full_name as title)
    - created_at         → published_at (repo creation = publication)
    """
    log.info("Transforming GitHub Bronze → Silver...")

    df = spark.read.format("delta").load(s3(BRONZE_BUCKET, "github/"))
    log.info(f"  GitHub Bronze rows read: {df.count()}")

    transformed = (
        df
        .withColumn("unified_id",      make_unified_id("github", "repo_id"))
        .withColumn("title",           clean_text("repo_name"))
        .withColumn("description",     clean_text("description"))
        .withColumn("url",             F.col("repo_url"))
        .withColumn("author",          clean_text("owner"))
        .withColumn("popularity_score",F.col("stars").cast("integer"))
        # .withColumn("comment_count",   F.col("open_issues").cast("integer"))
        # Merge "github" tag with repo's own topics array
        .withColumn("tags",
            F.array_union(
                F.array(F.lit("github")),
                F.coalesce(
                    F.col("topics"),
                    F.array()     # empty array if topics is null
                )
            )
        )
        .withColumn("language",        clean_text("language"))
        .withColumn("published_at",    parse_timestamp("created_at"))
        .withColumn("ingested_at",     F.col("ingested_at").cast("timestamp"))
        .withColumn("silver_processed_at", F.current_timestamp())
        .select(
            "unified_id", "source", "title", "description", "url",
            "author", "popularity_score", "tags",
            "language", "published_at", "ingested_at",
            "silver_processed_at", "pipeline_version"
        )
    )

    transformed = drop_empty_titles(transformed)
    transformed = deduplicate(transformed, "unified_id")

    log.info(f"  GitHub Silver rows after clean: {transformed.count()}")
    return transformed


# ══════════════════════════════════════════════════
# WRITE TO SILVER
# ══════════════════════════════════════════════════

def write_silver(df: DataFrame, source: str):
    """
    Write a Silver DataFrame to Delta Lake using MERGE.

    Why MERGE instead of overwrite?
    MERGE (upsert) updates existing rows if unified_id matches
    and inserts new rows if not found.
    This is idempotent — running the job twice won't duplicate data.

    Delta Lake mergeBuilder syntax:
    .whenMatchedUpdateAll()  → update all columns if ID matches
    .whenNotMatchedInsertAll() → insert new row if ID not found
    """
    from delta.tables import DeltaTable

    silver_path = s3(SILVER_BUCKET, f"{source}/")
    log.info(f"Writing {source} Silver to {silver_path}...")

    # Check if Silver table already exists
    try:
        silver_table = DeltaTable.forPath(df.sparkSession, silver_path)
        # Table exists → MERGE (upsert)
        (
            silver_table.alias("silver")
            .merge(
                df.alias("updates"),
                "silver.unified_id = updates.unified_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info(f"  MERGE complete for {source}")

    except Exception:
        # Table doesn't exist yet → create with initial write
        log.info(f"  Silver table not found — creating new: {source}")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(silver_path)
        )
        log.info(f"  Silver table created for {source}")


# ══════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════

def run_silver_job():
    """
    Orchestrate the full Silver transform:
    1. Create Spark session
    2. Transform each Bronze source independently
    3. Write each to its Silver Delta table via MERGE
    4. Print row counts for verification
    """
    log.info("══════════════════════════════════════")
    log.info("  Pulse Silver Job starting           ")
    log.info("══════════════════════════════════════")

    spark = create_spark()

    # Transform all 3 sources
    hn_silver     = transform_hn(spark)
    news_silver   = transform_news(spark)
    github_silver = transform_github(spark)

    # Write each to Silver Delta tables
    write_silver(hn_silver,     "hn")
    write_silver(news_silver,   "news")
    write_silver(github_silver, "github")

    # ── Summary stats ──────────────────────────────
    log.info("══════════════════════════════════════")
    log.info("  Silver Job Complete — Row Counts    ")
    log.info("══════════════════════════════════════")

    for source in ["hn", "news", "github"]:
        count = (
            spark.read.format("delta")
            .load(s3(SILVER_BUCKET, f"{source}/"))
            .count()
        )
        log.info(f"  Silver {source:10s}: {count:,} rows")

    log.info("══════════════════════════════════════")
    spark.stop()
    log.info("Silver job finished successfully")


if __name__ == "__main__":
    run_silver_job()