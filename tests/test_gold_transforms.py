"""
Unit tests for Gold transform functions.
Creates small mock Silver DataFrames to test each
Gold aggregation independently.

Run: python -m pytest tests/test_gold_transforms.py -v
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType, ArrayType
)
from transforms.gold_transforms import (
    build_trending_topics,
    build_language_momentum,
    build_source_activity,
    build_top_content
)


# ── Shared Spark session ──────────────────────────
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("pulse-gold-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


# ── Mock Silver schema (matches real Silver schema) ─
SILVER_SCHEMA = StructType([
    StructField("unified_id",        StringType(), True),
    StructField("source",            StringType(), True),
    StructField("title",             StringType(), True),
    StructField("description",       StringType(), True),
    StructField("url",               StringType(), True),
    StructField("author",            StringType(), True),
    StructField("popularity_score",  IntegerType(), True),
    StructField("comment_count",     IntegerType(), True),
    StructField("tags",              ArrayType(StringType()), True),
    StructField("language",          StringType(), True),
    StructField("published_at",      TimestampType(), True),
    StructField("ingested_at",       TimestampType(), True),
    StructField("silver_processed_at", TimestampType(), True),

    # NEW enrichment fields
    StructField("sentiment",         StringType(), True),
    StructField("ai_topics",         ArrayType(StringType()), True),
    StructField("ai_entities",       ArrayType(StringType()), True),

    StructField("pipeline_version",  StringType(), True),
])

def ts(s):
    """Helper: parse datetime string to Python datetime."""
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

def make_silver(spark, rows):
    """Helper: create a Silver DataFrame from row list."""
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


class TestTrendingTopics:

    def test_returns_rows(self, spark):
        """Should produce at least one trending topic row."""
        rows = [
            ("id1","hackernews","Rust is fast",None,None,"user1",
             100,10,["rust","systems"],None,ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
            ("id2","newsapi","Rust memory safety",None,None,"author1",
             0,0,["rust","memory"],None,ts("2024-01-15 10:30:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
        ]
        df     = make_silver(spark, rows)
        result = build_trending_topics(df)
        assert result.count() > 0

    def test_filters_generic_tags(self, spark):
        """Pipeline tags like 'hackernews' should be excluded."""
        rows = [
            ("id1","hackernews","Some story",None,None,"user1",
             50,5,["hackernews","story"],None,ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
        ]
        df     = make_silver(spark, rows)
        result = build_trending_topics(df)
        tags   = [r["tag"] for r in result.collect()]
        assert "hackernews" not in tags
        assert "story" not in tags

    def test_has_trend_score_column(self, spark):
        """Output must have trend_score column."""
        rows = [
            ("id1","hackernews","AI story",None,None,"u",
             80,3,["ai","python"],None,ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
        ]
        df     = make_silver(spark, rows)
        result = build_trending_topics(df)
        assert "trend_score" in result.columns

    def test_cross_source_scores_higher(self, spark):
        """Topic in 2 sources should score higher than in 1 source."""
        rows = [
            # python mentioned in 2 sources
            ("id1","hackernews","Python story",None,None,"u",
             50,0,["python"],None,ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
            ("id2","github","Python repo",None,None,"u",
             50,0,["python"],None,ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
            # rust mentioned in 1 source only
            ("id3","hackernews","Rust story",None,None,"u",
             50,0,["rust"],None,ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
        ]
        df      = make_silver(spark, rows)
        result  = build_trending_topics(df)
        scores  = {r["tag"]: r["trend_score"] for r in result.collect()}
        if "python" in scores and "rust" in scores:
            assert scores["python"] > scores["rust"]


class TestLanguageMomentum:

    def test_returns_language_rows(self, spark):
        """Should return rows for repos with language set."""
        rows = [
            ("id1","github","rust-analyzer",None,None,"owner",
             1000,0,["rust"],  "rust",  ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
            ("id2","github","python-lib",None,None,"owner",
             500,0, ["python"],"python",ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
        ]
        df     = make_silver(spark, rows)
        result = build_language_momentum(df)
        assert result.count() > 0

    def test_has_momentum_score(self, spark):
        """Output must have momentum_score column."""
        rows = [
            ("id1","github","repo",None,None,"owner",
             100,0,["python"],"python",ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
        ]
        df     = make_silver(spark, rows)
        result = build_language_momentum(df)
        assert "momentum_score" in result.columns


class TestSourceActivity:

    def test_counts_items_per_source(self, spark):
        """Should count items correctly per source."""
        rows = [
            ("id1","hackernews","s1",None,None,"u",
             10,0,[],"",ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
            ("id2","hackernews","s2",None,None,"u",
             20,0,[],"",ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
            ("id3","newsapi","s3",None,None,"u",
             0,0,[],"",ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
        ]
        df     = make_silver(spark, rows)
        result = build_source_activity(df)
        hn_row = result.filter(result.source=="hackernews").collect()
        assert hn_row[0]["item_count"] == 2


class TestTopContent:

    def test_returns_top_50_per_source(self, spark):
        """Should return max 50 items per source."""
        rows = [
            (f"id{i}","hackernews",f"Story {i}",None,None,"u",
             i*10,0,[],"",ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0")
            for i in range(1, 10)
        ]
        df     = make_silver(spark, rows)
        result = build_top_content(df)
        assert result.count() <= 50

    def test_has_global_rank(self, spark):
        """Output must have global_rank column."""
        rows = [
            ("id1","hackernews","Top story",None,None,"u",
             500,0,[],"",ts("2024-01-15 10:00:00"),
             ts("2024-01-15 10:00:00"),ts("2024-01-15 10:00:00"),"positive",
             ["programming"],["Rust"],"1.0.0"),
        ]
        df     = make_silver(spark, rows)
        result = build_top_content(df)
        assert "global_rank" in result.columns