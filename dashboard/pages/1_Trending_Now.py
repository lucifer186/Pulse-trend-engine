"""
Page 1 — Trending Now — Fixed version
"""
import sys; sys.path.insert(0, ".")
import streamlit as st
import plotly.express as px
import pandas as pd
from dashboard.data_loader import load_trending_topics, load_source_activity

st.set_page_config(page_title="Trending Now", page_icon="🔥", layout="wide")
st.title("🔥 Trending Now")
st.markdown("Topics trending across HackerNews, NewsAPI and GitHub right now.")

df = load_trending_topics()

# ── Debug info — shows in sidebar ─────────────────
with st.sidebar.expander("🔍 Debug info"):
    st.write(f"Total rows loaded: {len(df)}")
    if not df.empty:
        st.write(f"Columns: {df.columns.tolist()}")
        st.write(f"Date range: {df['hour_window'].min()} → {df['hour_window'].max()}")
        st.write(f"Unique tags: {df['tag'].nunique()}")
        st.write(f"Max source_diversity: {df['source_diversity'].max()}")
        st.write(f"Max mention_count: {df['mention_count'].max()}")

if df.empty:
    st.warning(
        "No trending data loaded from Gold table. "
        "Check debug info in sidebar to see what's happening."
    )
    st.info("Try clicking 🔄 Refresh Data in the sidebar.")
    st.stop()

# ── Sidebar filters — relaxed defaults ────────────
st.sidebar.header("Filters")

# Use 168h (7 days) default — not 12h — catches older data
hours_back = st.sidebar.slider(
    "Hours to look back", 1, 168, 168
)

# Min source diversity default = 1 — don't filter out single-source topics
min_sources = st.sidebar.slider(
    "Min source diversity", 1, 3, 1
)

# Min mentions default = 1 — don't filter with small dataset
min_mentions = st.sidebar.slider(
    "Min mentions", 1, 20, 1
)

# ── Apply filters ─────────────────────────────────
cutoff = df["hour_window"].max() - pd.Timedelta(hours=hours_back)

filtered = df[
    (df["hour_window"] >= cutoff) &
    (df["source_diversity"] >= min_sources) &
    (df["mention_count"] >= min_mentions)
]

if filtered.empty:
    st.warning(
        f"Filters removed all data. "
        f"Total rows before filter: {len(df)}. "
        f"Try reducing Min mentions or increasing Hours to look back."
    )
    st.stop()

st.caption(
    f"Showing {len(filtered)} topic records "
    f"from {filtered['hour_window'].min().strftime('%Y-%m-%d %H:%M')} "
    f"to {filtered['hour_window'].max().strftime('%Y-%m-%d %H:%M')}"
)

# ── Row 1: Top topics bar chart ────────────────────
st.subheader("Top Topics by Trend Score")

latest_hour = filtered["hour_window"].max()
latest      = filtered[filtered["hour_window"] == latest_hour]

# Fallback: if latest hour has no data use all filtered data
if latest.empty:
    latest = filtered

top_n = st.slider("Show top N topics", 5, 30, 15)
top   = latest.sort_values("trend_score", ascending=False).head(top_n)

if not top.empty:
    fig = px.bar(
        top,
        x="trend_score",
        y="tag",
        orientation="h",
        color="source_diversity",
        color_continuous_scale="Viridis",
        labels={
            "trend_score":      "Trend Score",
            "tag":              "Topic",
            "source_diversity": "Sources"
        },
        text="mention_count",
    )
    fig.update_traces(
        texttemplate="%{text} mentions",
        textposition="inside"
    )
    fig.update_layout(
        yaxis={"categoryorder": "total ascending"},
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        coloraxis_colorbar_title="Sources",
        height=max(400, top_n * 28)
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No topics found for the latest hour window.")

# ── Row 2: Trend score over time ───────────────────
st.subheader("Topic Trend Over Time")

all_tags = sorted(filtered["tag"].unique().tolist())

if not all_tags:
    st.info("No tags available with current filters.")
else:
    default_tags = all_tags[:5] if len(all_tags) >= 5 else all_tags
    sel_tags = st.multiselect(
        "Select topics to track",
        options=all_tags,
        default=default_tags
    )

    if sel_tags:
        time_df = filtered[filtered["tag"].isin(sel_tags)]
        if not time_df.empty:
            fig2 = px.line(
                time_df,
                x="hour_window",
                y="trend_score",
                color="tag",
                markers=True,
                labels={
                    "hour_window":  "Time",
                    "trend_score":  "Trend Score",
                    "tag":          "Topic"
                }
            )
            fig2.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                height=400
            )
            st.plotly_chart(fig2, use_container_width=True)

# ── Row 3: Cross-source topics ─────────────────────
st.subheader("🌐 Topics Trending Across Multiple Sources")

cross = (
    filtered[filtered["source_diversity"] >= 2]
    .sort_values("trend_score", ascending=False)
    .head(20)
)

if not cross.empty:
    # Build display columns — only include ones that exist
    base_cols = ["rank_in_hour", "tag", "mention_count",
                 "source_diversity", "trend_score"]

    # Add sentiment columns if they exist (post-enrichment)
    optional_cols = {
        "positive_count": "Positive",
        "negative_count": "Negative",
        "avg_sentiment_weight": "Sentiment Weight"
    }
    extra_cols = [c for c in optional_cols if c in cross.columns]
    display_cols = base_cols + extra_cols

    rename_map = {
        "rank_in_hour":         "Rank",
        "tag":                  "Topic",
        "mention_count":        "Mentions",
        "source_diversity":     "Sources",
        "trend_score":          "Score",
        **{k: v for k, v in optional_cols.items() if k in extra_cols}
    }

    st.dataframe(
        cross[display_cols].rename(columns=rename_map),
        use_container_width=True,
        hide_index=True
    )
else:
    st.info(
        "No cross-source topics yet. "
        "Your data currently comes from 1 source only (GitHub). "
        "Run all 3 producers to get cross-source signals."
    )

# ── Sentiment breakdown (if enrichment ran) ────────
if "positive_count" in filtered.columns:
    st.subheader("😊 Sentiment Breakdown of Trending Topics")
    sentiment_data = (
        filtered
        .groupby("tag")
        .agg(
            positive=("positive_count", "sum"),
            negative=("negative_count", "sum"),
            neutral=("neutral_count", "sum") if "neutral_count" in filtered.columns else ("mention_count", "count")
        )
        .reset_index()
        .sort_values("positive", ascending=False)
        .head(15)
    )
    fig3 = px.bar(
        sentiment_data,
        x="tag",
        y=["positive", "negative"],
        barmode="group",
        color_discrete_map={"positive": "#2ecc71", "negative": "#e74c3c"},
        labels={"tag": "Topic", "value": "Count", "variable": "Sentiment"}
    )
    fig3.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=400
    )
    st.plotly_chart(fig3, use_container_width=True)

# ── Sidebar refresh ────────────────────────────────
st.sidebar.divider()
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.caption("Data auto-refreshes every 5 minutes.")