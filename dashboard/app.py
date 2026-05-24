"""
Social Signal — Streamlit Dashboard
====================================
Real-time trend analysis across Twitch and YouTube.
Reads from PostgreSQL (analytics + NLP aggregates) and MongoDB (raw/enriched counts).

Run:
    streamlit run dashboard/app.py

Pages:
    📊  Trend Analysis     — sentiment over time, top subjects, volume
    🔬  NLP Insights       — subject × sentiment matrix, topic radar
    ⚙️  Pipeline Health    — batch run history, watermarks, source health
"""

import streamlit as st

st.set_page_config(
    page_title="Social Signal",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Shared CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Dark background */
    .stApp { background-color: #080B10; }
    section[data-testid="stSidebar"] { background-color: #0D1117; border-right: 1px solid #1C2333; }

    /* Typography */
    html, body, [class*="css"] { font-family: 'IBM Plex Sans', 'Segoe UI', sans-serif; color: #E8E8E8; }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 10px;
        padding: 14px 18px !important;
    }
    [data-testid="metric-container"] label { color: #888 !important; font-size: 11px !important; letter-spacing: 1px; text-transform: uppercase; }
    [data-testid="metric-container"] [data-testid="stMetricValue"] { color: #F0F0F0 !important; font-size: 26px !important; font-family: 'DM Mono', monospace !important; }
    [data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size: 11px !important; }

    /* Dataframe */
    [data-testid="stDataFrame"] { border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; overflow: hidden; }

    /* Section headers */
    .section-header {
        display: flex; align-items: center; gap: 8px;
        margin-bottom: 12px; margin-top: 4px;
    }
    .section-dot {
        width: 8px; height: 8px; border-radius: 50%;
        display: inline-block;
    }
    .section-title {
        font-size: 11px; font-weight: 700; letter-spacing: 2px;
        text-transform: uppercase; color: #CCC;
    }

    /* Status badges */
    .badge {
        display: inline-block; font-size: 10px; font-weight: 700;
        padding: 2px 8px; border-radius: 4px; letter-spacing: 1px;
        text-transform: uppercase;
    }
    .badge-success { background: rgba(0,229,160,0.1);  color: #00E5A0; border: 1px solid rgba(0,229,160,0.3); }
    .badge-failed  { background: rgba(255,71,87,0.1);  color: #FF4757; border: 1px solid rgba(255,71,87,0.3); }
    .badge-running { background: rgba(255,215,0,0.1);  color: #FFD700; border: 1px solid rgba(255,215,0,0.3); }

    /* Live dot */
    .live-indicator {
        display: inline-flex; align-items: center; gap: 6px;
        background: rgba(0,229,160,0.08); border: 1px solid rgba(0,229,160,0.2);
        border-radius: 20px; padding: 4px 12px; font-size: 11px;
    }
    .live-dot { width: 7px; height: 7px; border-radius: 50%; background: #00E5A0;
        animation: blink 1.2s infinite; display: inline-block; }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.2} }

    /* Hide default streamlit branding */
    #MainMenu, footer { visibility: hidden; }
    .block-container { padding-top: 1rem !important; }
</style>
""", unsafe_allow_html=True)

# ── Navigation ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ Social Signal")
    st.markdown("<div style='color:#555;font-size:11px;letter-spacing:1px;margin-bottom:20px'>BIG DATA TREND ANALYSIS</div>", unsafe_allow_html=True)

    page = st.radio(
        "Navigate",
        ["📊 Trend Analysis", "🔬 NLP Insights", "⚙️ Pipeline Health"],
        label_visibility="collapsed",
    )

    st.divider()

    st.markdown("**Filters**")
    sources = st.multiselect(
        "Sources",
        ["twitch", "youtube"],
        default=["twitch", "youtube"],
    )
    time_window = st.selectbox(
        "Time window",
        ["Last 1 hour", "Last 6 hours", "Last 24 hours", "Last 7 days"],
        index=2,
    )

    st.divider()
    st.markdown(
        "<div class='live-indicator'><span class='live-dot'></span>"
        "<span style='color:#00E5A0;font-weight:700'>LIVE</span>"
        "<span style='color:#555'>· auto-refresh 30s</span></div>",
        unsafe_allow_html=True,
    )

# ── Route pages ──────────────────────────────────────────────────────────────
if page == "📊 Trend Analysis":
    from dashboard.pages.trend_analysis import render
    render(sources, time_window)
elif page == "🔬 NLP Insights":
    from dashboard.pages.nlp_insights import render
    render(sources, time_window)
elif page == "⚙️ Pipeline Health":
    from dashboard.pages.pipeline_health import render
    render()
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
