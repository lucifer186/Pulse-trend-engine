"""
Page 3 — Top Content
Global leaderboard across all 3 sources
"""
import sys; sys.path.insert(0, ".")
import streamlit as st
import plotly.express as px
import pandas as pd
from dashboard.data_loader import load_top_content, load_enriched_silver

st.set_page_config(page_title="Top Content",page_icon="⭐",layout="wide")
st.title("⭐ Top Content")
st.markdown("Highest-ranked content across all sources, unified by popularity score.")

df = load_top_content()

if df.empty:
    st.warning("No content data yet.")
    st.stop()

# ── Sidebar filters ───────────────────────────────
st.sidebar.header("Filters")
sources    = ["All"] + sorted(df["source"].unique().tolist())
sel_source = st.sidebar.selectbox("Filter by source", sources)
languages  = ["All"] + sorted(df["language"].dropna().unique().tolist())
sel_lang   = st.sidebar.selectbox("Filter by language", languages)
top_n      = st.sidebar.slider("Show top N items", 10, 150, 50)

filtered = df.copy()
if sel_source != "All":
    filtered = filtered[filtered["source"] == sel_source]
if sel_lang != "All":
    filtered = filtered[filtered["language"] == sel_lang]
filtered = filtered.head(top_n)

# ── Row 1: Score distribution ─────────────────────
col_left, col_right = st.columns([2,1])

with col_left:
    st.subheader("Popularity Score Distribution by Source")
    fig = px.box(
        df,
        x="source",
        y="popularity_score",
        color="source",
        color_discrete_map={
            "hackernews":"#FF6600",
            "newsapi":   "#0066CC",
            "github":    "#238636"
        },
        labels={"popularity_score":"Score","source":"Source"}
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        height=300
    )
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Items by Source")
    source_counts = df.groupby("source").size().reset_index(name="count")
    fig2 = px.pie(
        source_counts,
        names="source",
        values="count",
        color="source",
        color_discrete_map={
            "hackernews":"#FF6600",
            "newsapi":   "#0066CC",
            "github":    "#238636"
        }
    )
    fig2.update_layout(height=300, paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig2, use_container_width=True)

# ── Row 2: Content leaderboard ────────────────────
st.subheader(f"Content Leaderboard — Top {len(filtered)}")

filtered_unique = (
    filtered
    .drop_duplicates(subset=["title"])
)

for _, row in filtered_unique.iterrows():
    source_icon = {
        "hackernews": "🟠",
        "newsapi":    "🔵",
        "github":     "🟢"
    }.get(row["source"], "⚪")

    with st.container():
        col_rank, col_content, col_score = st.columns([1, 8, 2])

        with col_rank:
            st.markdown(f"### #{int(row['global_rank'])}")

        with col_content:
            title = row["title"] or "Untitled"
            url   = row.get("url","")
            if url and str(url).startswith("http"):
                st.markdown(f"**[{title}]({url})**")
            else:
                st.markdown(f"**{title}**")

            meta_parts = [source_icon + " " + row["source"].capitalize()]
            if row.get("author"):
                meta_parts.append(f"by {row['author']}")
            if row.get("language"):
                meta_parts.append(f"🔧 {row['language']}")
            if row.get("published_at"):
                meta_parts.append(
                    str(pd.to_datetime(row["published_at"]).date())
                )
            st.caption(" · ".join(meta_parts))

        with col_score:
            st.metric("Score", int(row["popularity_score"]))

        st.divider()

if st.sidebar.button("🔄 Refresh"):
    st.cache_data.clear(); st.rerun()