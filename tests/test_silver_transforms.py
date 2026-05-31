"""
Unit tests for Silver transform helpers.
Uses a local SparkSession — no MinIO or Kafka needed.
Run: python -m pytest tests/test_silver_transforms.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

from transforms.silver_transforms import (
    deduplicate, clean_text, make_unified_id,
    drop_empty_titles, clean_url
)


# ── Shared Spark session for all tests ───────────
@pytest.fixture(scope="session")
def spark():
    """
    Single SparkSession shared across all tests in this file.
    scope="session" means it's created once and reused —
    much faster than creating a new session per test.
    """
    return (
        SparkSession.builder
        .appName("pulse-silver-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


class TestDeduplicate:
    def test_removes_duplicate_ids(self, spark):
        """Should keep only one row per unified_id"""
        data = [
            ("id_001", "First version",  "2024-01-01T10:00:00"),
            ("id_001", "Second version", "2024-01-01T11:00:00"),  # newer
            ("id_002", "Unique row",     "2024-01-01T10:00:00"),
        ]
        schema = ["unified_id", "title", "ingested_at"]
        df     = spark.createDataFrame(data, schema)

        result = deduplicate(df, "unified_id")
        assert result.count() == 2   # 2 unique IDs

    def test_keeps_latest_version(self, spark):
        """Should keep the row with the latest ingested_at"""
        data = [
            ("id_001", "Old title",  "2024-01-01T09:00:00"),
            ("id_001", "New title",  "2024-01-01T12:00:00"),
        ]
        df     = spark.createDataFrame(data, ["unified_id","title","ingested_at"])
        result = deduplicate(df, "unified_id")
        kept   = result.collect()[0]["title"]
        assert kept == "New title"


class TestCleanText:
    def test_trims_whitespace(self, spark):
        """Should remove leading and trailing spaces"""
        data   = [("  hello world  ",)]
        df     = spark.createDataFrame(data, ["text"])
        result = df.select(clean_text("text").alias("clean")).collect()
        assert result[0]["clean"] == "hello world"

    def test_collapses_multiple_spaces(self, spark):
        """Should replace multiple spaces with single space"""
        data   = [("hello   world",)]
        df     = spark.createDataFrame(data, ["text"])
        result = df.select(clean_text("text").alias("clean")).collect()
        assert result[0]["clean"] == "hello world"

    def test_returns_null_for_empty_string(self, spark):
        """Empty string after trim should become null"""
        data   = [("   ",)]
        df     = spark.createDataFrame(data, ["text"])
        result = df.select(clean_text("text").alias("clean")).collect()
        assert result[0]["clean"] is None


class TestMakeUnifiedId:
    def test_produces_md5_string(self, spark):
        """Should return a 32-char hex string"""
        data   = [("42000000",)]
        df     = spark.createDataFrame(data, ["id"])
        result = df.select(make_unified_id("hackernews","id").alias("uid")).collect()
        uid    = result[0]["uid"]
        assert len(uid) == 32
        assert all(c in "0123456789abcdef" for c in uid)

    def test_same_input_same_output(self, spark):
        """MD5 must be deterministic — same input = same hash"""
        data   = [("42000000",), ("42000000",)]
        df     = spark.createDataFrame(data, ["id"])
        result = df.select(make_unified_id("hackernews","id").alias("uid")).collect()
        assert result[0]["uid"] == result[1]["uid"]

    def test_different_sources_different_ids(self, spark):
        """Same original ID from different sources → different unified_id"""
        data   = [("123",)]
        df     = spark.createDataFrame(data, ["id"])
        hn_id  = df.select(make_unified_id("hackernews","id").alias("uid")).collect()[0]["uid"]
        gh_id  = df.select(make_unified_id("github","id").alias("uid")).collect()[0]["uid"]
        assert hn_id != gh_id


class TestDropEmptyTitles:
    def test_drops_null_titles(self, spark):
        data = [
            ("Good title",),
            (None,),
            ("Another good title",),
        ]
        df     = spark.createDataFrame(data, ["title"])
        result = drop_empty_titles(df)
        assert result.count() == 2

    def test_drops_whitespace_only_titles(self, spark):
        data   = [("   ",), ("Real title",)]
        df     = spark.createDataFrame(data, ["title"])
        result = drop_empty_titles(df)
        assert result.count() == 1


class TestCleanUrl:
    def test_keeps_valid_url(self, spark):
        data   = [("https://example.com/article",)]
        df     = spark.createDataFrame(data, ["url"])
        result = df.select(clean_url("url").alias("clean")).collect()
        assert result[0]["clean"] == "https://example.com/article"

    def test_removes_removed_com(self, spark):
        """NewsAPI placeholder URL should become null"""
        data   = [("https://removed.com",)]
        df     = spark.createDataFrame(data, ["url"])
        result = df.select(clean_url("url").alias("clean")).collect()
        assert result[0]["clean"] is None