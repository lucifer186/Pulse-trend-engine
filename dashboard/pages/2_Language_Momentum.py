"""
Page 2 — Language Momentum
Programming language trend velocity from GitHub + HN
"""
import sys; sys.path.insert(0, ".")
import streamlit as st
import plotly.express as px
import pandas as pd
from dashboard.data_loader import load_language_momentum

st.set_page_config(page_title="Language Momentum",page_icon="💻",layout="wide")
st.title("💻 Language Momentum")
st.markdown(
    "Which programming languages are gaining momentum this week? "
    "Momentum = GitHub stars + HN/News mentions combined."
)

df = load_language_momentum()

if df.empty:
    st.warning("No language data yet.")
    st.stop()

# ── Sidebar ───────────────────────────────────────
st.sidebar.header("Filters")
days_back = st.sidebar.slider("Days to look back", 1, 14, 7)
cutoff    = df["day_window"].max() - pd.Timedelta(days=days_back)
filtered  = df[df["day_window"] >= cutoff]

# ── Row 1: Leaderboard metrics ────────────────────
st.subheader("Today's Language Leaderboard")

today   = filtered[filtered["day_window"] == filtered["day_window"].max()]
# top_today = today.head(9)
top_today = (
    today
    .sort_values(
        ["total_stars"],
        ascending=False
    )
    .drop_duplicates(subset=["language"])
    .head(9)
)

if not top_today.empty:
    cols = st.columns(min(len(top_today), 3))
    for i, (_, row) in enumerate(top_today.iterrows()):
        with cols[i % 3]:
            medal = ["🥇","🥈","🥉"][i] if i < 3 else f"#{i+1}"
            st.metric(
                label=f"{medal} {row['language']}",
                value=f"{int(row['momentum_score']):,}",
                delta=f"⭐ {int(row['total_stars']):,} stars"
            )

# ── Row 2: Momentum over time ─────────────────────
st.subheader("Momentum Trend Over Time")

all_langs = sorted(filtered["language"].unique().tolist())
sel_langs = st.multiselect(
    "Select languages",
    options=all_langs,
    default=all_langs[:6] if len(all_langs) >= 6 else all_langs
)

if sel_langs:
    lang_df = filtered[filtered["language"].isin(sel_langs)]
    fig = px.line(
        lang_df,
        x="day_window",
        y="momentum_score",
        color="language",
        markers=True,
        labels={
            "day_window":     "Date",
            "momentum_score": "Momentum Score",
            "language":       "Language"
        }
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Row 3: Stars vs Mentions scatter ─────────────
st.subheader("Stars vs Mentions — Signal Strength")
st.markdown("Languages in the top-right corner have both high stars AND high mentions — strongest signal.")

if not today.empty:
    fig2 = px.scatter(
        today,
        x="mention_count",
        y="total_stars",
        size="momentum_score",
        color="source_diversity",
        text="language",
        color_continuous_scale="Viridis",
        labels={
            "mention_count":   "Total Mentions",
            "total_stars":     "Total Stars",
            "momentum_score":  "Momentum",
            "source_diversity":"Sources"
        }
    )
    fig2.update_traces(textposition="top center")
    fig2.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=450
    )
    st.plotly_chart(fig2, use_container_width=True)

# ── Row 4: Full table ─────────────────────────────
st.subheader("Full Data Table")
with st.expander("Show full language momentum table"):
    st.dataframe(
        filtered.rename(columns={
            "day_window":     "Date",
            "language":       "Language",
            "momentum_score": "Momentum",
            "total_stars":    "Stars",
            "mention_count":  "Mentions",
            "source_diversity":"Sources",
            "rank_by_day":    "Rank"
        }),
        use_container_width=True,
        hide_index=True
    )

if st.sidebar.button("🔄 Refresh"):
    st.cache_data.clear(); st.rerun()