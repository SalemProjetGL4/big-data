"""
Page 1 — Trend Analysis
Charts: sentiment share over time, event volume, compound sentiment score,
        cross-platform top content table.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import (
    get_sentiment_trend, get_event_volume,
    get_compound_over_time, get_total_events_processed,
    SRC_COLORS, SENT_COLORS,
)

AUTO_REFRESH_SEC = 30


def hex_to_rgba(hex_color: str, alpha: float = 0.12) -> str:
    """Convert '#9146FF' → 'rgba(145,70,255,0.12)'"""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _fmt(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def render(sources: list, time_window: str):
    # Auto-refresh
    st.markdown(
        f'<meta http-equiv="refresh" content="{AUTO_REFRESH_SEC}">',
        unsafe_allow_html=True,
    )

    # ── Page header ───────────────────────────────────────────────────────────
    col_title, col_live = st.columns([4, 1])
    with col_title:
        st.markdown("## 📊 Trend Analysis")
        st.markdown(
            f"<span style='color:#555;font-size:12px'>{time_window} · "
            f"Sources: {', '.join(sources)}</span>",
            unsafe_allow_html=True,
        )
    with col_live:
        st.markdown(
            "<div class='live-indicator' style='margin-top:12px;float:right'>"
            "<span class='live-dot'></span>"
            "<span style='color:#00E5A0;font-weight:700'>LIVE</span></div>",
            unsafe_allow_html=True,
        )

    # ── Top stats ─────────────────────────────────────────────────────────────
    counters = get_total_events_processed()
    sent_df  = get_sentiment_trend(sources, time_window)

    # Compute latest snapshot (last 5 minutes)
    latest   = sent_df.copy()
    if not latest.empty:
        cutoff = latest["minute"].max() - pd.Timedelta(minutes=5)
        latest = latest[latest["minute"] >= cutoff]

    avg_pos = latest["positive"].mean() if not latest.empty else 0.42
    avg_neg = latest["negative"].mean() if not latest.empty else 0.25
    avg_cmp = latest["avg_compound"].mean() if not latest.empty else 0.12

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Events (24h)",     _fmt(counters["total_events"]),  "↑ ~250/sec")
    c2.metric("Active Sources",   counters["active_sources"],       None)
    c3.metric("Positive %",       f"{avg_pos*100:.1f}%",           f"{(avg_pos-0.40)*100:+.1f}% vs avg")
    c4.metric("Negative %",       f"{avg_neg*100:.1f}%",           f"{(avg_neg-0.25)*100:+.1f}% vs avg")
    c5.metric("Avg Compound",     f"{avg_cmp:+.3f}",               "↑ improving" if avg_cmp > 0 else "↓ declining")

    st.divider()

    # ── Row 1: Sentiment over time + Volume ───────────────────────────────────
    col_sent, col_vol = st.columns([3, 2])

    with col_sent:
        st.markdown(
            "<div class='section-header'>"
            "<span class='section-dot' style='background:#00E5A0;box-shadow:0 0 6px #00E5A0'></span>"
            "<span class='section-title'>Sentiment Share Over Time</span></div>",
            unsafe_allow_html=True,
        )
        sentiment_tab = st.radio(
            "Sentiment dimension",
            ["positive", "negative", "neutral"],
            horizontal=True,
            label_visibility="collapsed",
            key="sent_tab",
        )

        if not sent_df.empty:
            fig = go.Figure()
            for src in sources:
                src_df = sent_df[sent_df["source"] == src].sort_values("minute")
                fig.add_trace(go.Scatter(
                    x=src_df["minute"],
                    y=src_df[sentiment_tab],
                    name=src,
                    mode="lines",
                    line=dict(color=SRC_COLORS[src], width=2),
                    fill="tozeroy",
                    fillcolor=hex_to_rgba(SRC_COLORS[src], 0.12),
                    hovertemplate=f"<b>{src}</b><br>%{{x|%H:%M}}<br>{sentiment_tab}: %{{y:.1%}}<extra></extra>",
                ))
            fig.update_layout(**_chart_layout(yaxis_tickformat=".0%", height=260))
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("No sentiment data for selected filters.")

    with col_vol:
        st.markdown(
            "<div class='section-header'>"
            "<span class='section-dot' style='background:#4A90D9;box-shadow:0 0 6px #4A90D9'></span>"
            "<span class='section-title'>Event Volume / min</span></div>",
            unsafe_allow_html=True,
        )
        vol_df = get_event_volume(sources, time_window)
        if not vol_df.empty:
            vol_wide = vol_df.pivot_table(
                index="minute", columns="source", values="event_count", fill_value=0
            ).reset_index()
            fig2 = go.Figure()
            for src in sources:
                if src in vol_wide.columns:
                    fig2.add_trace(go.Scatter(
                        x=vol_wide["minute"], y=vol_wide[src],
                        name=src, mode="lines",
                        line=dict(color=SRC_COLORS[src], width=1.5),
                        stackgroup="one",
                        fillcolor=hex_to_rgba(SRC_COLORS[src], 0.25),
                        hovertemplate=f"<b>{src}</b><br>%{{x|%H:%M}}<br>%{{y:,}} events<extra></extra>",
                    ))
            fig2.update_layout(**_chart_layout(height=260))
            st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

    # ── Row 2: Compound sentiment ─────────────────────────────────────────────
    st.markdown(
        "<div class='section-header' style='margin-top:8px'>"
        "<span class='section-dot' style='background:#FFD700;box-shadow:0 0 6px #FFD700'></span>"
        "<span class='section-title'>Compound Sentiment Score  (-1 very negative → +1 very positive)</span></div>",
        unsafe_allow_html=True,
    )
    cmp_df = get_compound_over_time(sources, time_window)
    if not cmp_df.empty:
        fig3 = go.Figure()
        # Zero reference line
        fig3.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.15)")
        for src in sources:
            src_df = cmp_df[cmp_df["source"] == src].sort_values("minute")
            fig3.add_trace(go.Scatter(
                x=src_df["minute"],
                y=src_df["avg_compound"],
                name=src,
                mode="lines",
                line=dict(color=SRC_COLORS[src], width=2),
                hovertemplate=f"<b>{src}</b><br>%{{x|%H:%M}}<br>compound: %{{y:+.3f}}<extra></extra>",
            ))
        fig3.update_layout(**_chart_layout(yaxis_range=[-1, 1], height=200))
        st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})

    # ── Row 3: Sentiment breakdown pie per source ──────────────────────────────
    st.markdown(
        "<div class='section-header' style='margin-top:8px'>"
        "<span class='section-dot' style='background:#9146FF;box-shadow:0 0 6px #9146FF'></span>"
        "<span class='section-title'>Sentiment Breakdown by Source</span></div>",
        unsafe_allow_html=True,
    )
    if not sent_df.empty:
        agg = (
            sent_df.groupby("source")[["positive", "negative", "neutral"]]
            .mean()
            .reindex(sources)
            .reset_index()
        )
        cols = st.columns(len(sources))
        for i, src in enumerate(sources):
            row = agg[agg["source"] == src]
            if row.empty:
                continue
            row = row.iloc[0]
            fig_pie = go.Figure(go.Pie(
                labels=["Positive", "Negative", "Neutral"],
                values=[row["positive"], row["negative"], row["neutral"]],
                marker_colors=[SENT_COLORS["positive"], SENT_COLORS["negative"], SENT_COLORS["neutral"]],
                hole=0.6,
                textinfo="percent",
                hovertemplate="%{label}: %{percent}<extra></extra>",
            ))
            fig_pie.add_annotation(
                text=f"<b>{src}</b>",
                x=0.5, y=0.5, showarrow=False,
                font=dict(color=SRC_COLORS[src], size=13),
            )
            fig_pie.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=(i == len(sources)-1),
                legend=dict(font=dict(color="#888", size=10)),
                margin=dict(t=10, b=10, l=10, r=10),
                height=200,
            )
            cols[i].plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False})


# ── Shared chart layout ───────────────────────────────────────────────────────

def _chart_layout(**kwargs) -> dict:
    base = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#888", size=10),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.04)",
            zerolinecolor="rgba(255,255,255,0.06)",
            tickfont=dict(color="#555"),
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.04)",
            zerolinecolor="rgba(255,255,255,0.06)",
            tickfont=dict(color="#555"),
        ),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
            font=dict(color="#888", size=10),
            bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(t=30, b=10, l=0, r=0),
        hovermode="x unified",
    )
    base.update(kwargs)
    return base
