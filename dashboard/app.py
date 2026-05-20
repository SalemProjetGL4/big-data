import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import date, timedelta

# ── Config ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Social Media Trend Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

PLATFORM_COLORS = {
    "YouTube": "#FF0000",
    "Twitch": "#9146FF",
    "Reddit": "#FF4500",
    "Twitter": "#1DA1F2",
    "All": "#AAAAAA",
}
PLATFORM_IDS = {"YouTube": 1, "Twitch": 2, "Twitter": 3, "Reddit": 4}
ID_TO_PLATFORM = {v: k for k, v in PLATFORM_IDS.items()}

DB_CFG = dict(host="postgres", port=5432, dbname="trends_db",
              user="postgres", password="password")


# ── Database helpers ─────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CFG)


@st.cache_data(ttl=60, show_spinner=False)
def query(sql: str, params=None) -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql(sql, conn, params=params)


@st.cache_data(ttl=300, show_spinner=False)
def get_table_columns(table_name: str) -> set[str]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
    """
    df = query(sql, (table_name,))
    return set(df["column_name"].str.lower().tolist())


@st.cache_data(ttl=300, show_spinner=False)
def load_date_range() -> tuple[date, date]:
    df = query("SELECT MIN(window_start), MAX(window_start) FROM trending_topics")
    lo = df.iloc[0, 0]
    hi = df.iloc[0, 1]
    if lo is None or hi is None:
        lo, hi = date.today() - timedelta(days=30), date.today()
    return pd.Timestamp(lo).date(), pd.Timestamp(hi).date()


def platform_name(pid) -> str:
    return ID_TO_PLATFORM.get(int(pid), str(pid))


# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("Social Media\nTrend Analyzer")
st.sidebar.markdown("---")

platform_choice = st.sidebar.selectbox(
    "Platform", ["All", "YouTube", "Twitch", "Reddit", "Twitter"]
)

date_min, date_max = load_date_range()
date_range = st.sidebar.date_input(
    "Date range (window_start)",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = date_min, date_max

st.sidebar.markdown("---")
st.sidebar.caption("Data auto-refreshes every 60 s")

# ── Page tabs ─────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "Trending Topics",
    "Sentiment Analysis",
    "Engagement Metrics",
    "Cross-Platform Trends",
])


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Trending Topics
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.header("Trending Topics")

    # Build query filters
    plat_filter = (
        f"AND platform_id = {PLATFORM_IDS[platform_choice]}"
        if platform_choice != "All" else ""
    )

    @st.cache_data(ttl=60, show_spinner="Loading trending topics…")
    def load_trending(plat, sd, ed):
        sql = f"""
            SELECT keyword, platform_id, SUM(count) AS count,
                   MIN(window_start) AS window_start, MAX(window_end) AS window_end
            FROM trending_topics
            WHERE window_start BETWEEN %s AND %s
              {plat_filter}
            GROUP BY keyword, platform_id
            ORDER BY count DESC
            LIMIT 100
        """
        df = query(sql, (sd, ed))
        df["platform"] = df["platform_id"].apply(platform_name)
        return df

    df_trend = load_trending(platform_choice, start_date, end_date)

    if df_trend.empty:
        st.info("No data found for the selected filters.")
    else:
        top20 = df_trend.head(20).copy()
        top20["color"] = top20["platform"].map(PLATFORM_COLORS)

        # ── Bar chart ────────────────────────────────────────────────────────
        st.subheader("Top 20 Keywords by Count")
        fig_bar = px.bar(
            top20,
            x="count", y="keyword",
            color="platform",
            color_discrete_map=PLATFORM_COLORS,
            orientation="h",
            labels={"count": "Total Count", "keyword": "Keyword"},
            height=520,
        )
        fig_bar.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            yaxis={"categoryorder": "total ascending"},
            legend_title="Platform",
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # ── Word cloud ───────────────────────────────────────────────────────
        st.subheader("Keyword Word Cloud")
        try:
            from wordcloud import WordCloud
            import matplotlib.pyplot as plt
            import io

            freq = dict(zip(df_trend["keyword"], df_trend["count"]))
            wc = WordCloud(
                width=1200, height=400,
                background_color="#0E1117",
                colormap="RdYlGn",
                max_words=150,
            ).generate_from_frequencies(freq)

            fig_wc, ax = plt.subplots(figsize=(14, 4))
            fig_wc.patch.set_facecolor("#0E1117")
            ax.imshow(wc, interpolation="bilinear")
            ax.axis("off")
            buf = io.BytesIO()
            plt.savefig(buf, format="png", bbox_inches="tight",
                        facecolor="#0E1117")
            buf.seek(0)
            st.image(buf, use_container_width=True)
            plt.close()
        except ImportError:
            st.info("Install `wordcloud` and `matplotlib` to see the word cloud: `pip install wordcloud matplotlib`")

        # ── Table ────────────────────────────────────────────────────────────
        st.subheader("Keyword Details")
        display_cols = ["keyword", "platform", "count", "window_start", "window_end"]
        st.dataframe(
            df_trend[display_cols].rename(columns={
                "keyword": "Keyword",
                "platform": "Platform",
                "count": "Count",
                "window_start": "Window Start",
                "window_end": "Window End",
            }),
            use_container_width=True,
            hide_index=True,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Sentiment Analysis
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.header("Sentiment Analysis")

    plat_filter2 = (
        f"AND platform_id = {PLATFORM_IDS[platform_choice]}"
        if platform_choice != "All" else ""
    )

    @st.cache_data(ttl=60, show_spinner="Loading sentiment data…")
    def load_sentiment(plat, sd, ed):
        sql = f"""
            SELECT platform_id, time_bucket,
                   positive_pct, negative_pct, neutral_pct,
                   avg_compound, sample_count
            FROM sentiment_summary
            WHERE time_bucket BETWEEN %s AND %s
              {plat_filter2}
            ORDER BY time_bucket
        """
        df = query(sql, (sd, ed))
        df["platform"] = df["platform_id"].apply(platform_name)
        df["time_bucket"] = pd.to_datetime(df["time_bucket"])
        return df

    df_sent = load_sentiment(platform_choice, start_date, end_date)

    if df_sent.empty:
        st.info("No sentiment data found for the selected filters.")
    else:
        platforms_in_data = df_sent["platform"].unique().tolist()

        # ── Metric cards (current avg sentiment per platform) ─────────────
        st.subheader("Current Average Sentiment")
        latest = (
            df_sent.sort_values("time_bucket")
            .groupby("platform")
            .last()
            .reset_index()
        )
        cols = st.columns(len(latest))
        for col, (_, row) in zip(cols, latest.iterrows()):
            compound = float(row["avg_compound"])
            sentiment_label = (
                "Positive" if compound > 0.05
                else "Negative" if compound < -0.05
                else "Neutral"
            )
            delta_color = "normal" if compound >= 0 else "inverse"
            col.metric(
                label=row["platform"],
                value=f"{compound:+.3f}",
                delta=sentiment_label,
                delta_color=delta_color,
            )

        st.markdown("---")

        # ── Stacked area chart per platform ───────────────────────────────
        st.subheader("Sentiment Over Time")
        for plat in platforms_in_data:
            df_p = df_sent[df_sent["platform"] == plat].sort_values("time_bucket")
            if df_p.empty:
                continue
            color = PLATFORM_COLORS.get(plat, "#AAAAAA")
            fig_area = go.Figure()
            fig_area.add_trace(go.Scatter(
                x=df_p["time_bucket"], y=df_p["positive_pct"],
                name="Positive", stackgroup="one",
                fillcolor="rgba(0,200,100,0.4)",
                line=dict(color="rgba(0,200,100,0.8)"),
            ))
            fig_area.add_trace(go.Scatter(
                x=df_p["time_bucket"], y=df_p["neutral_pct"],
                name="Neutral", stackgroup="one",
                fillcolor="rgba(150,150,150,0.4)",
                line=dict(color="rgba(150,150,150,0.8)"),
            ))
            fig_area.add_trace(go.Scatter(
                x=df_p["time_bucket"], y=df_p["negative_pct"],
                name="Negative", stackgroup="one",
                fillcolor="rgba(220,50,50,0.4)",
                line=dict(color="rgba(220,50,50,0.8)"),
            ))
            fig_area.update_layout(
                title=dict(text=plat, font=dict(color=color)),
                yaxis=dict(title="Percentage (%)", range=[0, 100]),
                xaxis_title="Time",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                hovermode="x unified",
                height=320,
                margin=dict(t=50, b=30),
            )
            st.plotly_chart(fig_area, use_container_width=True)

        # ── Compound score over time (line chart) ─────────────────────────
        st.subheader("Average Compound Score Over Time")
        fig_line = px.line(
            df_sent, x="time_bucket", y="avg_compound",
            color="platform",
            color_discrete_map=PLATFORM_COLORS,
            labels={"avg_compound": "Avg Compound Score", "time_bucket": "Time"},
            height=350,
        )
        fig_line.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        fig_line.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_line, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Engagement Metrics
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.header("Engagement Metrics")

    plat_filter3 = (
        f"AND platform_id = {PLATFORM_IDS[platform_choice]}"
        if platform_choice != "All" else ""
    )

    @st.cache_data(ttl=60, show_spinner="Loading engagement data…")
    def load_engagement(plat):
        available = get_table_columns("engagement_metrics")
        preferred_cols = [
            "platform_id",
            "content_id",
            "title",
            "category",
            "views",
            "likes",
            "peak_viewers",
            "engagement_index",
        ]
        select_cols = [c for c in preferred_cols if c in available]
        if "platform_id" not in select_cols or "content_id" not in select_cols:
            return pd.DataFrame()
        order_by = "views" if "views" in select_cols else select_cols[0]
        sql = f"""
            SELECT {", ".join(select_cols)}
            FROM engagement_metrics
            WHERE 1=1 {plat_filter3}
            ORDER BY {order_by} DESC
            LIMIT 500
        """
        df = query(sql)
        if "likes" not in df.columns:
            df["likes"] = 0
        if "peak_viewers" not in df.columns:
            df["peak_viewers"] = None
        if "engagement_index" not in df.columns:
            df["engagement_index"] = None
        df["platform"] = df["platform_id"].apply(platform_name)
        return df

    df_eng = load_engagement(platform_choice)

    if df_eng.empty:
        st.info("No engagement data found for the selected filters.")
    else:
        top20_eng = df_eng.head(20).copy()

        # ── Horizontal bar — top 20 by views ─────────────────────────────
        st.subheader("Top 20 Content Items by Views")
        top20_eng["label"] = top20_eng["title"].str[:50] + "…"
        fig_hbar = px.bar(
            top20_eng,
            x="views", y="label",
            color="platform",
            color_discrete_map=PLATFORM_COLORS,
            orientation="h",
            hover_data=["title", "category", "likes", "peak_viewers"],
            labels={"views": "Views", "label": "Content"},
            height=560,
        )
        fig_hbar.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            yaxis={"categoryorder": "total ascending"},
        )
        st.plotly_chart(fig_hbar, use_container_width=True)

        # ── Scatter — views vs engagement_index ───────────────────────────
        st.subheader("Views vs Engagement Index")
        fig_scatter = px.scatter(
            df_eng,
            x="views", y="engagement_index",
            color="platform",
            color_discrete_map=PLATFORM_COLORS,
            size="likes",
            hover_data=["title", "category"],
            labels={
                "views": "Views",
                "engagement_index": "Engagement Index",
                "platform": "Platform",
            },
            height=480,
            opacity=0.75,
        )
        fig_scatter.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        # ── Summary metrics ───────────────────────────────────────────────
        st.subheader("Platform Engagement Summary")
        summary = (
            df_eng.groupby("platform")
            .agg(
                total_views=("views", "sum"),
                avg_engagement=("engagement_index", "mean"),
                total_items=("content_id", "count"),
            )
            .reset_index()
        )
        cols = st.columns(len(summary))
        for col, (_, row) in zip(cols, summary.iterrows()):
            col.metric(
                label=row["platform"],
                value=f"{row['total_views']:,.0f} views",
                delta=f"{row['total_items']} items · {row['avg_engagement']:.2f} avg eng.",
            )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Cross-Platform Trends
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.header("Cross-Platform Trends")

    @st.cache_data(ttl=60, show_spinner="Loading cross-platform data…")
    def load_cross(sd, ed):
        sql = """
            SELECT keyword, platform_count, total_count,
                   confidence_score, platforms_present, window_start
            FROM cross_platform_trends
            WHERE window_start BETWEEN %s AND %s
              AND platform_count >= 2
            ORDER BY confidence_score DESC, total_count DESC
        """
        df = query(sql, (sd, ed))
        df["window_start"] = pd.to_datetime(df["window_start"])
        return df

    df_cross = load_cross(start_date, end_date)

    if df_cross.empty:
        st.info("No cross-platform trends found for the selected date range.")
    else:
        col_left, col_right = st.columns([3, 2])

        with col_left:
            # ── Table ─────────────────────────────────────────────────────
            st.subheader("Keywords Trending on 2+ Platforms")
            st.dataframe(
                df_cross[[
                    "keyword", "platform_count", "total_count",
                    "confidence_score", "platforms_present", "window_start"
                ]].rename(columns={
                    "keyword": "Keyword",
                    "platform_count": "Platform Count",
                    "total_count": "Total Count",
                    "confidence_score": "Confidence Score",
                    "platforms_present": "Platforms",
                    "window_start": "Window Start",
                }).style.background_gradient(
                    subset=["Confidence Score"],
                    cmap="RdYlGn",
                    vmin=0, vmax=1,
                ),
                use_container_width=True,
                hide_index=True,
                height=420,
            )

        with col_right:
            # ── Bar chart — platform_count distribution ────────────────────
            st.subheader("Platform Count Distribution")
            dist = df_cross["platform_count"].value_counts().reset_index()
            dist.columns = ["platform_count", "num_keywords"]
            dist = dist.sort_values("platform_count")
            dist["label"] = dist["platform_count"].apply(
                lambda n: f"{n} platform{'s' if n > 1 else ''}"
            )
            fig_dist = px.bar(
                dist,
                x="label", y="num_keywords",
                color="num_keywords",
                color_continuous_scale=["#9146FF", "#FF0000"],
                labels={"label": "Platforms", "num_keywords": "# Keywords"},
                height=340,
            )
            fig_dist.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_dist, use_container_width=True)

            # ── Top confidence keywords (mini table) ───────────────────────
            st.subheader("Top Confidence Keywords")
            top_conf = df_cross.nlargest(10, "confidence_score")[
                ["keyword", "confidence_score", "platforms_present"]
            ].rename(columns={
                "keyword": "Keyword",
                "confidence_score": "Score",
                "platforms_present": "Platforms",
            })
            st.dataframe(top_conf, hide_index=True, use_container_width=True)

        # ── Confidence score histogram ─────────────────────────────────────
        st.subheader("Confidence Score Distribution")
        fig_hist = px.histogram(
            df_cross, x="confidence_score",
            nbins=20,
            color_discrete_sequence=["#9146FF"],
            labels={"confidence_score": "Confidence Score"},
            height=280,
        )
        fig_hist.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            bargap=0.05,
        )
        st.plotly_chart(fig_hist, use_container_width=True)
