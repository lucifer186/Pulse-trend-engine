"""
Data Loader — shared Gold table reader
────────────────────────────────────────────────
Reads Gold Delta Lake tables from MinIO using pandas.
All functions use @st.cache_data so data is loaded once
and cached in memory — pages feel instant.

Why pandas not PySpark?
  PySpark startup = 30s. Streamlit reruns on every click.
  Pandas reads parquet directly from S3 = under 2 seconds.
  Gold tables are small enough for pandas (< 100k rows).
"""

import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import s3fs    # S3-compatible filesystem for pandas

load_dotenv()

# ── MinIO connection config ───────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000") \
                   .replace("http://","").replace("https://","")
MINIO_USER     = os.getenv("MINIO_ROOT_USER",     "pulseadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "pulsepassword123")
GOLD_BUCKET    = "pulse-gold"
SILVER_BUCKET  = "pulse-silver"


def get_s3fs():
    """
    Create an s3fs filesystem pointing to MinIO.
    s3fs lets pandas read parquet files from S3/MinIO
    as if they were local files.
    """
    return s3fs.S3FileSystem(
        anon=False,
        key=MINIO_USER,
        secret=MINIO_PASSWORD,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}"}
    )


def read_delta_as_pandas(bucket: str, table: str) -> pd.DataFrame:
    """
    Read a Delta Lake table from MinIO into a pandas DataFrame.

    Delta Lake tables are just parquet files + _delta_log/.
    We skip _delta_log/ and read all .parquet files directly.
    This works because our tables don't have deletes/updates
    that would require the transaction log for correctness.
    (Silver MERGE tables need PySpark — Gold overwrite is fine.)
    """
    fs   = get_s3fs()
    path = f"{bucket}/{table}/"

    try:
        # List all parquet files in the table directory
        all_files = fs.glob(f"{path}**/*.parquet")
        if not all_files:
            return pd.DataFrame()

        # Read all parquet files and concatenate
        dfs = []
        for f in all_files:
            with fs.open(f, "rb") as fh:
                dfs.append(pd.read_parquet(fh))

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    except Exception as e:
        st.error(f"Error reading {bucket}/{table}: {e}")
        return pd.DataFrame()


# ══════════════════════════════════════════════════
# CACHED DATA LOADERS — one per Gold table
# @st.cache_data(ttl=300) = cache for 5 minutes
# After 5 min Streamlit re-fetches fresh data
# ══════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_trending_topics() -> pd.DataFrame:
    """Load trending_topics Gold table."""
    df = read_delta_as_pandas(GOLD_BUCKET, "trending_topics")
    if df.empty:
        return df
    # Parse hour_window as datetime for proper chart axes
    df["hour_window"] = pd.to_datetime(df["hour_window"])
    df["trend_score"] = df["trend_score"].round(2)
    return df.sort_values(["hour_window","rank_in_hour"],
                          ascending=[False, True])


@st.cache_data(ttl=300)
def load_language_momentum() -> pd.DataFrame:
    """Load language_momentum Gold table."""
    df = read_delta_as_pandas(GOLD_BUCKET, "language_momentum")
    if df.empty:
        return df
    df["day_window"]     = pd.to_datetime(df["day_window"])
    df["momentum_score"] = df["momentum_score"].round(0).astype(int)
    return df.sort_values(["day_window","rank_by_day"],
                          ascending=[False, True])


@st.cache_data(ttl=300)
def load_source_activity() -> pd.DataFrame:
    """Load source_activity Gold table."""
    df = read_delta_as_pandas(GOLD_BUCKET, "source_activity")
    if df.empty:
        return df
    df["hour_window"]    = pd.to_datetime(df["hour_window"])
    df["activity_score"] = df["activity_score"].round(1)
    return df.sort_values("hour_window", ascending=False)


@st.cache_data(ttl=300)
def load_top_content() -> pd.DataFrame:
    """Load top_content Gold table."""
    df = read_delta_as_pandas(GOLD_BUCKET, "top_content")
    if df.empty:
        return df
    df["published_at"] = pd.to_datetime(df["published_at"])
    # Convert tags list to comma-separated string for display
    if "tags" in df.columns:
        df["tags_str"] = df["tags"].apply(
            lambda t: ", ".join(t[:3]) if isinstance(t, list) else str(t)
        )
    return df.sort_values("global_rank")


@st.cache_data(ttl=600)
def load_enriched_silver() -> pd.DataFrame:
    """Load AI-enriched Silver table for sentiment analysis."""
    df = read_delta_as_pandas(SILVER_BUCKET, "enriched")
    if df.empty:
        return df
    df["enriched_at"] = pd.to_datetime(df["enriched_at"])
    return df


# ── Summary metrics helper ────────────────────────
@st.cache_data(ttl=300)
def get_pipeline_summary() -> dict:
    """
    Compute summary metrics for the dashboard home page.
    Returns dict with counts and latest timestamps.
    """
    trending  = load_trending_topics()
    content   = load_top_content()
    activity  = load_source_activity()
    enriched  = load_enriched_silver()

    return {
        "total_topics":    len(trending["tag"].unique()) if not trending.empty else 0,
        "total_content":   len(content) if not content.empty else 0,
        "total_enriched":  len(enriched) if not enriched.empty else 0,
        "sources_active":  len(activity["source"].unique()) if not activity.empty else 0,
        "latest_update":   trending["gold_computed_at"].max() if not trending.empty and "gold_computed_at" in trending.columns else "N/A",
    }
