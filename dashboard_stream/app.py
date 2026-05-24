"""
Social Signal — Streamlit Dashboard
=====================================
Real-time trend analysis across YouTube, Twitch, and Reddit.
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
import sys, os
# Make the dashboard directory importable regardless of how streamlit is invoked
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if page == "📊 Trend Analysis":
    from pages.trend_analysis import render
    render(sources, time_window)
elif page == "🔬 NLP Insights":
    from pages.nlp_insights import render
    render(sources, time_window)
elif page == "⚙️ Pipeline Health":
    from pages.pipeline_health import render
    render()
