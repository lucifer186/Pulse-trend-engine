"""
Gold Transform Helpers — Updated with AI Enrichment
────────────────────────────────────────────────────
All 4 Gold tables now use sentiment + ai_topics + ai_entities
from pulse-silver/enriched/ joined in gold_job.py

Enrichment impact per table:
  trending_topics   → sentiment-weighted trend scores
  language_momentum → sentiment-weighted momentum scores
  source_activity   → sentiment breakdown per source per hour
  top_content       → sentiment + ai_topics added to leaderboard
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── 1. TRENDING TOPICS ────────────────────────────
def build_trending_topics(combined_df: DataFrame) -> DataFrame:
    """
    Compute trending topics by hour — sentiment weighted.

    Enrichment used:
    - sentiment_weight: positive=1.2x, negative=0.8x boost/penalty
    - positive_count / negative_count per topic
    - avg_sentiment_weight affects final trend_score
    """

    exploded = (
        combined_df
        .filter(F.col("published_at").isNotNull())
        .withColumn("tag", F.explode(F.col("tags")))
        .filter(F.col("tag").isNotNull())
        .filter(F.length(F.col("tag")) > 1)
        .filter(~F.col("tag").isin(
            "hackernews", "newsapi", "github",
            "story", "top_headlines"
        ))
    )

    windowed = exploded.withColumn(
        "hour_window",
        F.date_trunc("hour", F.col("published_at"))
    )

    # Sentiment weight — positive topics score higher
    weighted = windowed.withColumn(
        "sentiment_weight",
        F.when(F.col("sentiment") == "positive", 1.2)
        .when(F.col("sentiment") == "negative", 0.8)
        .otherwise(1.0)   # neutral or null = no change
    )

    aggregated = (
        weighted
        .groupBy("hour_window", "tag")
        .agg(
            F.count("*").alias("mention_count"),
            F.avg("popularity_score").alias("avg_popularity"),
            F.countDistinct("source").alias("source_diversity"),
            F.avg("sentiment_weight").alias("avg_sentiment_weight"),
            F.collect_list("title").alias("sample_titles"),
            # Sentiment breakdown per topic
            F.sum(
                F.when(F.col("sentiment") == "positive", 1).otherwise(0)
            ).alias("positive_count"),
            F.sum(
                F.when(F.col("sentiment") == "negative", 1).otherwise(0)
            ).alias("negative_count"),
            F.sum(
                F.when(F.col("sentiment") == "neutral", 1).otherwise(0)
            ).alias("neutral_count"),
        )
    )

    # Sentiment-weighted trend score
    scored = aggregated.withColumn(
        "trend_score",
        (
            (F.col("mention_count") * 1.0)
            + (F.col("avg_popularity") * 0.5)
            + (F.col("source_diversity") * 10.0)
        ) * F.col("avg_sentiment_weight")
    )

    window_rank = (
        Window
        .partitionBy("hour_window")
        .orderBy(F.col("trend_score").desc())
    )

    ranked = (
        scored
        .withColumn("rank_in_hour", F.rank().over(window_rank))
        .withColumn("sample_titles", F.slice(F.col("sample_titles"), 1, 3))
        .withColumn("gold_computed_at", F.current_timestamp())
        .select(
            "hour_window", "tag", "mention_count",
            "avg_popularity", "source_diversity",
            "avg_sentiment_weight",
            "positive_count", "negative_count", "neutral_count",
            "trend_score", "rank_in_hour",
            "sample_titles", "gold_computed_at"
        )
    )

    return ranked


# ── 2. LANGUAGE MOMENTUM ─────────────────────────
def build_language_momentum(combined_df: DataFrame) -> DataFrame:
    """
    Compute programming language trend velocity.

    Enrichment used:
    - sentiment_weight: positive sentiment about a language
      boosts its momentum score
    - dominant_sentiment: most common sentiment for
      that language today (positive/negative/neutral)
    - This answers: "Is Rust trending positively or
      are people frustrated with it?"
    """

    common_languages = [
        "python", "javascript", "typescript", "rust",
        "go", "java", "kotlin", "swift", "sql",
        "c++", "c#", "ruby", "scala", "r"
    ]

    # From explicit language field (GitHub repos)
    with_lang = (
        combined_df
        .filter(
            F.col("language").isNotNull() &
            (F.trim(F.col("language")) != "")
        )
        .withColumn(
            "day_window",
            F.date_trunc("day", F.col("published_at"))
        )
        # Sentiment weight for language momentum
        .withColumn(
            "sentiment_weight",
            F.when(F.col("sentiment") == "positive", 1.3)
            .when(F.col("sentiment") == "negative", 0.7)
            .otherwise(1.0)
        )
    )

    # From tags (HN + News mentioning language names)
    tag_langs = (
        combined_df
        .filter(F.col("published_at").isNotNull())
        .withColumn("tag", F.explode(F.col("tags")))
        .filter(F.lower(F.col("tag")).isin(common_languages))
        .withColumn("language", F.lower(F.col("tag")))
        .withColumn("day_window", F.date_trunc("day", F.col("published_at")))
        .withColumn(
            "sentiment_weight",
            F.when(F.col("sentiment") == "positive", 1.3)
            .when(F.col("sentiment") == "negative", 0.7)
            .otherwise(1.0)
        )
        .select(
            "day_window", "language",
            "popularity_score", "source",
            "sentiment", "sentiment_weight"
        )
    )

    lang_combined = (
        with_lang
        .select(
            "day_window", "language",
            "popularity_score", "source",
            "sentiment", "sentiment_weight"
        )
        .union(tag_langs)
    )

    momentum = (
        lang_combined
        .groupBy("day_window", "language")
        .agg(
            F.count("*").alias("mention_count"),
            F.sum("popularity_score").alias("total_stars"),
            F.avg("popularity_score").alias("avg_popularity"),
            F.countDistinct("source").alias("source_diversity"),
            F.avg("sentiment_weight").alias("avg_sentiment_weight"),

            # Sentiment breakdown per language
            F.sum(
                F.when(F.col("sentiment") == "positive", 1).otherwise(0)
            ).alias("positive_mentions"),
            F.sum(
                F.when(F.col("sentiment") == "negative", 1).otherwise(0)
            ).alias("negative_mentions"),

            # Dominant sentiment — most common one
            # We compute this below after aggregation
        )
        # Sentiment-weighted momentum score
        # Stars + mentions × sentiment boost
        .withColumn(
            "momentum_score",
            (
                F.col("total_stars")
                + (F.col("mention_count") * 5.0)
            ) * F.col("avg_sentiment_weight")
        )
        # Dominant sentiment label
        .withColumn(
            "dominant_sentiment",
            F.when(
                F.col("positive_mentions") > F.col("negative_mentions"),
                "positive"
            )
            .when(
                F.col("negative_mentions") > F.col("positive_mentions"),
                "negative"
            )
            .otherwise("neutral")
        )
        .withColumn(
            "rank_by_day",
            F.rank().over(
                Window.partitionBy("day_window")
                .orderBy(F.col("momentum_score").desc())
            )
        )
        .withColumn("gold_computed_at", F.current_timestamp())
        .select(
            "day_window", "language",
            "mention_count", "total_stars", "avg_popularity",
            "source_diversity", "momentum_score",
            "avg_sentiment_weight", "dominant_sentiment",
            "positive_mentions", "negative_mentions",
            "rank_by_day", "gold_computed_at"
        )
        .orderBy("day_window", "rank_by_day")
    )

    return momentum


# ── 3. SOURCE ACTIVITY ───────────────────────────
def build_source_activity(combined_df: DataFrame) -> DataFrame:
    """
    Compute publishing rate per source per hour.

    Enrichment used:
    - positive_rate: % of content that is positive per source/hour
    - negative_rate: % of content that is negative
    - sentiment_score: weighted sentiment score per hour
    - This answers: "Is HackerNews more positive or negative
      than NewsAPI in the last 24 hours?"
    """

    activity = (
        combined_df
        .filter(F.col("published_at").isNotNull())
        .withColumn(
            "hour_window",
            F.date_trunc("hour", F.col("published_at"))
        )
        # Numeric sentiment for averaging
        .withColumn(
            "sentiment_numeric",
            F.when(F.col("sentiment") == "positive",  1.0)
            .when(F.col("sentiment") == "negative", -1.0)
            .otherwise(0.0)
        )
        .groupBy("hour_window", "source")
        .agg(
            F.count("*").alias("item_count"),
            F.avg("popularity_score").alias("avg_popularity"),
            F.max("popularity_score").alias("peak_popularity"),
            F.sum("popularity_score").alias("total_popularity"),

            # Sentiment aggregations
            F.avg("sentiment_numeric").alias("avg_sentiment_score"),
            F.sum(
                F.when(F.col("sentiment") == "positive", 1).otherwise(0)
            ).alias("positive_items"),
            F.sum(
                F.when(F.col("sentiment") == "negative", 1).otherwise(0)
            ).alias("negative_items"),
            F.sum(
                F.when(F.col("sentiment") == "neutral", 1).otherwise(0)
            ).alias("neutral_items"),
            F.sum(
                F.when(F.col("sentiment").isNull(), 1).otherwise(0)
            ).alias("unenriched_items"),  # not yet processed by ai_enrichment
        )
        # Positive rate (%) — what % of this source's content is positive
        .withColumn(
            "positive_rate_pct",
            F.round(
                (F.col("positive_items") / F.col("item_count")) * 100, 1
            )
        )
        .withColumn(
            "negative_rate_pct",
            F.round(
                (F.col("negative_items") / F.col("item_count")) * 100, 1
            )
        )
        # Activity score — boosted by positive sentiment
        .withColumn(
            "activity_score",
            F.col("item_count") * F.col("avg_popularity") *
            (1.0 + (F.col("avg_sentiment_score") * 0.1))
        )
        .withColumn("gold_computed_at", F.current_timestamp())
        .select(
            "hour_window", "source",
            "item_count", "avg_popularity",
            "peak_popularity", "total_popularity",
            "activity_score",
            # Sentiment breakdown
            "avg_sentiment_score",
            "positive_items", "negative_items",
            "neutral_items", "unenriched_items",
            "positive_rate_pct", "negative_rate_pct",
            "gold_computed_at"
        )
        .orderBy("hour_window", F.col("item_count").desc())
    )

    return activity


# ── 4. TOP CONTENT ───────────────────────────────
def build_top_content(combined_df: DataFrame) -> DataFrame:
    """
    Unified leaderboard across all 3 sources — last 7 days.

    Enrichment used:
    - sentiment: shown per item in leaderboard
    - ai_topics: AI-classified topics shown alongside user tags
    - ai_entities: named entities (OpenAI, Python, etc.)
    - sentiment_boost: positive items get small score boost
    - This answers: "Show me only the positively trending
      top content" or "What are entities mentioned in top content?"
    """

    normalised = (
        combined_df
        .filter(F.col("published_at").isNotNull())
        .filter(
            F.col("published_at") >=
            F.date_sub(F.current_date(), 7)
        )
        # Normalise score across sources
        .withColumn(
            "normalised_score",
            F.when(
                F.col("source") == "github",
                F.col("popularity_score") / 10.0
            )
            .when(
                F.col("source") == "hackernews",
                F.col("popularity_score").cast("double")
            )
            .otherwise(
                F.unix_timestamp(
                    F.col("published_at")
                ).cast("double") / 1e9
            )
        )
        # Sentiment boost for leaderboard ranking
        # Positive content gets a small extra push
        .withColumn(
            "sentiment_boost",
            F.when(F.col("sentiment") == "positive", 1.1)
            .when(F.col("sentiment") == "negative", 0.95)
            .otherwise(1.0)
        )
        # Final score = normalised × sentiment boost
        .withColumn(
            "final_score",
            F.col("normalised_score") * F.col("sentiment_boost")
        )
    )

    global_window = Window.orderBy(F.col("final_score").desc())
    source_window = (
        Window
        .partitionBy("source")
        .orderBy(F.col("final_score").desc())
    )

    ranked = (
        normalised
        .withColumn("global_rank", F.rank().over(global_window))
        .withColumn("source_rank",  F.rank().over(source_window))
        .withColumn("gold_computed_at", F.current_timestamp())
        .filter(F.col("source_rank") <= 50)
        .select(
            "global_rank", "source_rank", "source",
            "title", "description", "url", "author",
            "popularity_score", "normalised_score",
            "sentiment_boost", "final_score",
            # Enrichment columns
            "sentiment",       # positive / negative / neutral / null
            "ai_topics",       # LLM-classified topics
            "ai_entities",     # named entities from Gemini
            "tags", "language",
            "published_at", "gold_computed_at"
        )
        .orderBy("global_rank")
    )

    return ranked