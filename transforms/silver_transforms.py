"""
Silver Transform Helpers
────────────────────────────────────────────────
Pure PySpark functions for cleaning and normalising
Bronze data into the unified Silver schema.

Kept separate from the main job so they can be:
- Unit tested independently (no Spark session needed)
- Reused in future Gold layer transforms
- Versioned independently of the orchestration logic
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


# ── Deduplication ─────────────────────────────────
def deduplicate(df: DataFrame, id_col: str) -> DataFrame:
    """
    Remove duplicate rows keeping the latest record.

    Strategy: window function partitioned by id_col,
    ordered by ingested_at descending → keep rank=1.

    Why not just dropDuplicates()?
    dropDuplicates() keeps an arbitrary row.
    Our approach always keeps the NEWEST version,
    which matters when scores/stars get updated.
    """
    from pyspark.sql.window import Window

    window = (
        Window
        .partitionBy(id_col)
        .orderBy(F.col("ingested_at").desc())
    )
    return (
        df
        .withColumn("_rank", F.rank().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


# ── Text cleaning ─────────────────────────────────
def clean_text(col_name: str) -> F.Column:
    """
    Clean a text column:
    - trim whitespace from both ends
    - replace multiple spaces with single space
    - return null if result is empty string

    Using regexp_replace for space normalisation —
    handles tabs and non-breaking spaces too.
    """
    return F.when(
        F.trim(
            F.regexp_replace(F.col(col_name), r"\s+", " ")
        ) != "",
        F.trim(
            F.regexp_replace(F.col(col_name), r"\s+", " ")
        )
    ).otherwise(F.lit(None))


# ── Timestamp normalisation ───────────────────────
def parse_timestamp(col_name: str) -> F.Column:
    """
    Parse various timestamp formats into a unified UTC timestamp.

    HN uses Unix epoch integers (e.g. 1717000000)
    NewsAPI uses ISO strings (e.g. "2024-01-15T10:23:00Z")
    GitHub uses ISO strings (e.g. "2024-01-15T10:23:00Z")

    Strategy:
    1. Try casting directly as TimestampType (works for ISO strings)
    2. If null, try treating as Unix epoch seconds
    This handles all 3 formats in one expression.
    """
    return F.coalesce(
        F.col(col_name).cast(TimestampType()),
        F.to_timestamp(F.col(col_name)),
        F.from_unixtime(F.col(col_name)).cast(TimestampType())
    )


# ── Unified ID generation ─────────────────────────
def make_unified_id(source: str, id_col: str) -> F.Column:
    """
    Create a globally unique ID across all 3 sources.
    MD5 hash of source + original_id.

    Example: MD5("hackernews_42000123") → "a3f2..."
    This ensures no ID collisions between sources and
    gives a stable deduplication key for the Silver table.
    """
    return F.md5(
        F.concat(F.lit(source), F.lit("_"), F.col(id_col).cast("string"))
    )


# ── Null filter ───────────────────────────────────
def drop_empty_titles(df: DataFrame) -> DataFrame:
    """
    Drop rows where title is null or empty after cleaning.
    Title is mandatory — content without a title is useless
    for trend analysis and AI enrichment.
    """
    return df.filter(
        F.col("title").isNotNull() &
        (F.trim(F.col("title")) != "")
    )


# ── URL validator ─────────────────────────────────
def clean_url(col_name: str) -> F.Column:
    """
    Return null for invalid or placeholder URLs.
    NewsAPI returns "https://removed.com" for deleted articles.
    """
    return F.when(
        F.col(col_name).startswith("http") &
        ~F.col(col_name).contains("removed.com"),
        F.col(col_name)
    ).otherwise(F.lit(None))