"""
AI Enrichment Job
────────────────────────────────────────────────
Reads unified Silver tables, calls OpenAI API
inside a Spark UDF to enrich each row with:
  - sentiment    : positive / negative / neutral
  - ai_topics    : AI-classified topic tags
  - entities     : named entities (tools, companies, people)

Writes enriched data to: pulse-silver/enriched/

Why gpt-4.1-mini?
  Cheapest current OpenAI model — perfect for $5 budget.
  Handles JSON structured extraction reliably.
  Fast enough for batch enrichment jobs.
"""

import os
import json
import time
import logging
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",       "http://localhost:9000")
MINIO_USER      = os.getenv("MINIO_ROOT_USER",       "pulseadmin")
MINIO_PASSWORD  = os.getenv("MINIO_ROOT_PASSWORD",   "pulsepassword123")
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY")        # ← changed from GEMINI
SILVER_BUCKET   = "pulse-silver"

def s3(bucket, path=""):
    return f"s3a://{bucket}/{path}"


# ══════════════════════════════════════════════════
# OPENAI UDF — replaces Gemini UDF
# ══════════════════════════════════════════════════

def make_openai_udf(api_key: str):
    """
    Factory function that returns a PySpark UDF.

    Why a factory function?
    We need to pass the API key into the UDF closure.
    Spark UDFs must be serializable — a factory function
    lets us bake the key into the closure safely.

    The UDF:
    1. Builds a structured prompt asking GPT to return JSON
    2. Calls gpt-4.1-mini (cheapest, fastest model)
    3. Parses the JSON response
    4. Returns result as JSON string
    5. On any error → returns fallback JSON with neutral values
    """

    def enrich_with_openai(title: str, description: str) -> str:
        # ── Imports INSIDE the UDF ───────────────────
        # Critical: must import inside UDF body so
        # Spark workers can serialize this function
        from openai import OpenAI
        import json
        import time

        # Handle null/empty input gracefully
        if not title or not title.strip():
            return json.dumps({
                "sentiment": "neutral",
                "ai_topics": [],
                "entities":  [],
                "error":     "empty_title"
            })

        # Cap text at 800 chars to save tokens on $5 budget
        text = f"{title}. {description or ''}"[:800]

        prompt = f"""Analyze this tech content and respond ONLY with valid JSON.
        No explanation, no markdown, just the JSON object.

        Content: {text}

        Required JSON format:
        {{
        "sentiment": "positive" or "negative" or "neutral",
        "ai_topics": ["topic1", "topic2"],
        "entities": ["entity1", "entity2"]
        }}

        Rules:
        - sentiment: overall tone of the content
        - ai_topics: 2-4 specific tech topics (e.g. "large language models", "rust programming", "kubernetes")
        - entities: tools/companies/people mentioned (e.g. "OpenAI", "Python", "Linus Torvalds")
        - Keep each list to max 4 items
        - Only output the JSON object, nothing else"""

        try:
            client   = OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model="gpt-4.1-mini",       # cheapest — good for $5 budget
                messages=[
                    {
                        "role":    "system",
                        "content": "You are a JSON-only response bot. "
                                   "Always respond with valid JSON only."
                    },
                    {
                        "role":    "user",
                        "content": prompt
                    }
                ],
                max_tokens=150,             # JSON response is small — save tokens
                temperature=0.1,            # low = consistent structured output
                response_format={"type": "json_object"}  # forces JSON output
            )

            raw = response.choices[0].message.content.strip()

            # Clean response just in case
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            parsed = json.loads(raw)

            result = {
                "sentiment": parsed.get("sentiment", "neutral"),
                "ai_topics": parsed.get("ai_topics", [])[:4],
                "entities":  parsed.get("entities",  [])[:4],
                "error":     None
            }

            # Small delay — be polite to the API
            time.sleep(0.5)
            return json.dumps(result)

        except json.JSONDecodeError:
            return json.dumps({
                "sentiment": "neutral",
                "ai_topics": [],
                "entities":  [],
                "error":     "json_parse_error"
            })
        except Exception as e:
            return json.dumps({
                "sentiment": "neutral",
                "ai_topics": [],
                "entities":  [],
                "error":     str(e)[:100]
            })

    # Register as Spark UDF returning StringType (JSON string)
    from pyspark.sql.functions import udf
    return udf(enrich_with_openai, StringType())


# ══════════════════════════════════════════════════
# AI ENRICHMENT SCHEMA
# ══════════════════════════════════════════════════

AI_RESULT_SCHEMA = StructType([
    StructField("sentiment", StringType(), True),
    StructField("ai_topics", StringType(), True),
    StructField("entities",  StringType(), True),
    StructField("error",     StringType(), True),
])


# ══════════════════════════════════════════════════
# SPARK SESSION
# ══════════════════════════════════════════════════

def create_spark() -> SparkSession:
    packages = ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.367",
    ])
    spark = (
        SparkSession.builder
        .appName("pulse-ai-enrichment")
        .master("local[2]")         # 2 cores — throttled for API rate limit
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",               packages)
        .config("spark.hadoop.fs.s3a.endpoint",      MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",    MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",    MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions",      "2")
        .config("spark.default.parallelism",         "2")
        .config("spark.driver.memory",               "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ══════════════════════════════════════════════════
# MAIN ENRICHMENT JOB
# ══════════════════════════════════════════════════

def run_enrichment_job(sample_size: int = 50):
    """
    Main enrichment pipeline:
    1. Load combined Silver (all 3 sources)
    2. Sample N rows (respects $5 API budget)
    3. Apply OpenAI UDF to each row
    4. Parse JSON result into typed columns
    5. Write enriched table to pulse-silver/enriched/

    sample_size: how many rows to enrich per run.
    Default 50 = safe for $5 OpenAI budget.
    Each row costs ~150 input + 150 output tokens.
    50 rows ≈ 15,000 tokens ≈ $0.01 on gpt-4.1-mini.
    """
    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set in .env — cannot run enrichment")
        return

    log.info("══════════════════════════════════════════")
    log.info("  Pulse AI Enrichment Job starting        ")
    log.info(f"  Model       : gpt-4.1-mini")
    log.info(f"  Enriching   : {sample_size} rows")
    log.info(f"  Est. cost   : ~${sample_size * 0.0002:.4f}")
    log.info("══════════════════════════════════════════")

    spark = create_spark()

    # ── Load Silver ───────────────────────────────
    hn     = spark.read.format("delta").load(s3(SILVER_BUCKET, "hn/"))
    news   = spark.read.format("delta").load(s3(SILVER_BUCKET, "news/"))
    github = spark.read.format("delta").load(s3(SILVER_BUCKET, "github/"))

    combined = hn.union(news).union(github)
    total    = combined.count()
    log.info(f"Total Silver rows available: {total:,}")

    if total == 0:
        log.error("Silver tables are empty — run silver_job.py first")
        spark.stop()
        return

    # ── Sample for enrichment ─────────────────────
    enriched_path = s3(SILVER_BUCKET, "enriched/")
    try:
        already_enriched = (
            spark.read.format("delta").load(enriched_path)
            .select("unified_id")
        )
        already_count = already_enriched.count()
        log.info(f"Rows already enriched: {already_count:,}")

        # Only process rows NOT already enriched
        to_enrich = (
            combined
            .join(already_enriched, on="unified_id", how="left_anti")
            .filter(F.col("title").isNotNull())
            .filter(F.trim(F.col("title")) != "")
            .orderBy(F.col("popularity_score").desc())
            .limit(sample_size)
        )
    except Exception:
        # First run — no enriched table yet
        log.info("First enrichment run — no existing enriched table")
        to_enrich = (
            combined
            .filter(F.col("title").isNotNull())
            .filter(F.trim(F.col("title")) != "")
            .orderBy(F.col("popularity_score").desc())
            .limit(sample_size)
        )

    row_count = to_enrich.count()
    log.info(f"Rows to enrich this run: {row_count}")

    if row_count == 0:
        log.info("Nothing new to enrich — all rows already processed")
        spark.stop()
        return

    # ── Register OpenAI UDF ───────────────────────
    openai_udf = make_openai_udf(OPENAI_API_KEY)   # ← was make_gemini_udf

    # ── Apply UDF ─────────────────────────────────
    log.info("Calling OpenAI API on each row...")
    log.info("This takes ~1 min per 50 rows (0.5s delay between calls)")

    enriched = (
        to_enrich
        .withColumn(
            "ai_result_json",
            openai_udf(F.col("title"), F.col("description"))  # ← updated UDF
        )
        .withColumn(
            "ai_result",
            F.from_json(F.col("ai_result_json"), AI_RESULT_SCHEMA)
        )
        .withColumn("sentiment",         F.col("ai_result.sentiment"))
        .withColumn("ai_topics",         F.col("ai_result.ai_topics"))
        .withColumn("ai_entities",       F.col("ai_result.entities"))
        .withColumn("enrichment_error",  F.col("ai_result.error"))
        .withColumn("enriched_at",       F.current_timestamp())
        .withColumn("llm_model",         F.lit("gpt-4.1-mini"))  # track which model
        .drop("ai_result_json", "ai_result")
    )

    # ── Write to Silver enriched table ────────────
    log.info(f"Writing {row_count} enriched rows to {enriched_path}...")
    (
        enriched.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(enriched_path)
    )

    # ── Summary ───────────────────────────────────
    final_count = spark.read.format("delta").load(enriched_path).count()

    log.info("══════════════════════════════════════════")
    log.info(f"  Enrichment complete!")
    log.info(f"  Total enriched rows: {final_count:,}")
    log.info("  Sentiment breakdown:")

    (
        spark.read.format("delta").load(enriched_path)
        .groupBy("source", "sentiment")
        .count()
        .orderBy("source", "sentiment")
        .show()
    )

    # Show error rate
    error_count = (
        spark.read.format("delta").load(enriched_path)
        .filter(F.col("enrichment_error").isNotNull())
        .count()
    )
    if error_count > 0:
        log.warning(f"  Rows with errors: {error_count} "
                    f"({error_count/final_count*100:.1f}%)")
    else:
        log.info("  No enrichment errors ✓")

    log.info("══════════════════════════════════════════")
    spark.stop()
    log.info("AI enrichment job finished")


if __name__ == "__main__":
    run_enrichment_job(sample_size=80)