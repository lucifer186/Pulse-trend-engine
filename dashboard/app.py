"""
Pulse Dashboard — Home Page
"""
import sys
sys.path.insert(0, ".")
import pandas as pd
import streamlit as st
import plotly.express as px
from dashboard.data_loader import (
    get_pipeline_summary, load_source_activity,
    load_trending_topics
)

st.set_page_config(
    page_title="Pulse — Trend Intelligence",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Header ────────────────────────────────────────
st.title("🚀 Pulse — AI-Powered Trend Intelligence")
st.markdown(
    "Real-time trend signals from **HackerNews**, "
    "**NewsAPI** and **GitHub** — enriched with Gemini AI. "
    "Data refreshes every 30 minutes automatically."
)
st.divider()

# ── Pipeline summary metrics ──────────────────────
summary = get_pipeline_summary()
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("🔥 Trending Topics",  summary["total_topics"])
with col2:
    st.metric("📰 Content Items",    summary["total_content"])
with col3:
    st.metric("🤖 AI Enriched",      summary["total_enriched"])
with col4:
    st.metric("📡 Active Sources",   summary["sources_active"])

st.caption(f"Last pipeline run: {summary['latest_update']}")
st.divider()

# ── Source activity chart ─────────────────────────
st.subheader("📈 Source Activity — Last 24 Hours")

activity_df = load_source_activity()
if not activity_df.empty:
    last_24h = activity_df[
        activity_df["hour_window"] >=
        activity_df["hour_window"].max() - pd.Timedelta(hours=24)
    ] if "hour_window" in activity_df.columns else activity_df

    # import pandas as pd
    last_24h = activity_df.copy()

    fig = px.bar(
        last_24h.head(72),    # last 24h × 3 sources
        x="hour_window",
        y="item_count",
        color="source",
        barmode="group",
        color_discrete_map={
            "hackernews": "#FF6600",
            "newsapi":    "#0066CC",
            "github":     "#238636"
        },
        labels={"item_count":"Items Published","hour_window":"Time"},
        title="Items published per source per hour"
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend_title="Source",
        height=350
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No activity data yet — run producers and wait for Gold job.")

# ── Top 5 trending topics right now ──────────────
st.subheader("🔥 Top 5 Topics Right Now")

trending_df = load_trending_topics()
if not trending_df.empty:
    latest_hour = trending_df["hour_window"].max()
    top5 = trending_df[
        trending_df["hour_window"] == latest_hour
    ].head(5)

    for _, row in top5.iterrows():
        col_a, col_b, col_c = st.columns([3, 1, 1])
        with col_a:
            st.markdown(f"**#{int(row['rank_in_hour'])} {row['tag']}**")
        with col_b:
            st.markdown(f"🗣 {int(row['mention_count'])} mentions")
        with col_c:
            diversity = int(row['source_diversity'])
            stars = "⭐" * diversity
            st.markdown(f"{stars} {diversity} sources")
else:
    st.info("No trending data yet.")

st.divider()
st.markdown(
    "**Navigate using the sidebar** → "
    "Trending Now | Language Momentum | Top Content | AI Assistant"
)