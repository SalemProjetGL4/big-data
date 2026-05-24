"""
Page 2 — NLP Insights
Charts: top subjects stacked bar, topic radar, subject×sentiment matrix,
        trending topics with change %.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import (
    get_top_subjects, get_topic_distribution,
    SRC_COLORS, SENT_COLORS,
)


def hex_to_rgba(hex_color: str, alpha: float = 0.12) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _fmt(n: int) -> str:
    return f"{n/1000:.1f}K" if n >= 1000 else str(n)


def _sentiment_color(val: float) -> str:
    if val > 0.1:
        return "#00E5A0"
    if val < -0.1:
        return "#FF4757"
    return "#4A90D9"


def render(sources: list, time_window: str):
    st.markdown("## 🔬 NLP Insights")
    st.markdown(
        f"<span style='color:#555;font-size:12px'>{time_window} · "
        f"Sources: {', '.join(sources)}</span>",
        unsafe_allow_html=True,
    )
    st.divider()

    subjects_df = get_top_subjects(sources, time_window, limit=15)
    topics_df   = get_topic_distribution(sources, time_window)

    # ── Row 1: Top subjects bar + Topic radar ─────────────────────────────────
    col_bar, col_radar = st.columns([3, 2])

    with col_bar:
        st.markdown(
            "<div class='section-header'>"
            "<span class='section-dot' style='background:#FF6314'></span>"
            "<span class='section-title'>Top Subjects by Mentions</span></div>",
            unsafe_allow_html=True,
        )
        if not subjects_df.empty:
            top = subjects_df.head(10)
            fig = go.Figure()
            for src in sources:
                if src in top.columns:
                    fig.add_trace(go.Bar(
                        y=top["subject"],
                        x=top[src],
                        name=src,
                        orientation="h",
                        marker_color=SRC_COLORS[src],
                        hovertemplate=f"<b>{src}</b><br>%{{y}}: %{{x:,}}<extra></extra>",
                    ))
            fig.update_layout(
                barmode="stack",
                **_chart_layout(height=360, xaxis_title="Mentions"),
                yaxis=dict(
                    autorange="reversed",
                    tickfont=dict(color="#CCC", size=11),
                    gridcolor="rgba(255,255,255,0.04)",
                ),
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    with col_radar:
        st.markdown(
            "<div class='section-header'>"
            "<span class='section-dot' style='background:#9146FF'></span>"
            "<span class='section-title'>Topic Distribution by Platform</span></div>",
            unsafe_allow_html=True,
        )
        if not topics_df.empty:
            fig2 = go.Figure()
            cats = topics_df["topic"].tolist()
            cats_closed = cats + [cats[0]]   # close the polygon

            for src in sources:
                if src in topics_df.columns:
                    vals = topics_df[src].tolist()
                    vals_closed = vals + [vals[0]]
                    fig2.add_trace(go.Scatterpolar(
                        r=vals_closed,
                        theta=cats_closed,
                        fill="toself",
                        fillcolor=hex_to_rgba(SRC_COLORS[src], 0.12),
                        line=dict(color=SRC_COLORS[src], width=2),
                        name=src,
                    ))
            fig2.update_layout(
                polar=dict(
                    bgcolor="rgba(0,0,0,0)",
                    radialaxis=dict(
                        visible=True, tickfont=dict(color="#555", size=8),
                        gridcolor="rgba(255,255,255,0.08)",
                    ),
                    angularaxis=dict(
                        tickfont=dict(color="#888", size=9),
                        gridcolor="rgba(255,255,255,0.08)",
                    ),
                ),
                paper_bgcolor="rgba(0,0,0,0)",
                legend=dict(font=dict(color="#888", size=10), bgcolor="rgba(0,0,0,0)"),
                margin=dict(t=20, b=20, l=20, r=20),
                height=360,
            )
            st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

    # ── Row 2: Subject × Sentiment matrix table ────────────────────────────────
    st.markdown(
        "<div class='section-header' style='margin-top:8px'>"
        "<span class='section-dot' style='background:#4A90D9'></span>"
        "<span class='section-title'>Subject × Sentiment Matrix</span></div>",
        unsafe_allow_html=True,
    )

    if not subjects_df.empty:
        display = subjects_df.copy()
        display["total"] = display[["twitch", "youtube"]].sum(axis=1)

        # Dominant platform
        display["top_platform"] = display[["twitch", "youtube"]].idxmax(axis=1)

        # Sentiment bar HTML
        def sentiment_bar(val):
            pct = abs(val) * 100
            color = _sentiment_color(val)
            sign  = "+" if val > 0 else ""
            bar   = (
                f"<div style='display:flex;align-items:center;gap:8px'>"
                f"<div style='width:80px;height:4px;background:#1C2333;border-radius:2px;overflow:hidden'>"
                f"<div style='width:{pct:.0f}%;height:100%;background:{color};border-radius:2px'></div></div>"
                f"<span style='font-family:monospace;color:{color};font-size:11px'>{sign}{val:.3f}</span>"
                f"</div>"
            )
            return bar

        def platform_badge(src):
            c = SRC_COLORS.get(src, "#888")
            return (
                f"<span style='font-size:10px;font-weight:700;padding:2px 7px;"
                f"border-radius:4px;background:{c}22;color:{c};"
                f"border:1px solid {c}44;letter-spacing:1px;text-transform:uppercase'>{src}</span>"
            )

        rows_html = ""
        for i, row in display.head(12).iterrows():
            bg = "rgba(255,255,255,0.01)" if i % 2 == 0 else "transparent"
            rows_html += (
                f"<tr style='border-bottom:1px solid rgba(255,255,255,0.04);background:{bg}'>"
                f"<td style='padding:9px 12px;color:#555;font-family:monospace;font-size:10px'>#{i+1}</td>"
                f"<td style='padding:9px 12px;color:#EEE;font-weight:600'>{row['subject']}</td>"
                f"<td style='padding:9px 12px;color:{SRC_COLORS['twitch']};font-family:monospace'>{row['twitch']:,}</td>"
                f"<td style='padding:9px 12px;color:{SRC_COLORS['youtube']};font-family:monospace'>{row['youtube']:,}</td>"
                f"<td style='padding:9px 12px;color:#DDD;font-family:monospace;font-weight:700'>{_fmt(int(row['total']))}</td>"
                f"<td style='padding:9px 12px'>{sentiment_bar(row['avg_sentiment'])}</td>"
                f"<td style='padding:9px 12px'>{platform_badge(row['top_platform'])}</td>"
                f"</tr>"
            )

        table_html = f"""
        <div style='overflow-x:auto;border:1px solid rgba(255,255,255,0.06);border-radius:10px'>
        <table style='width:100%;border-collapse:collapse;font-size:12px'>
          <thead>
            <tr style='border-bottom:1px solid rgba(255,255,255,0.08)'>
              {"".join(f"<th style='padding:9px 12px;text-align:left;color:#555;font-size:10px;font-weight:700;letter-spacing:1px;text-transform:uppercase'>{h}</th>" for h in ["#","Subject","Twitch","YouTube","Total","Sentiment Score","Top Platform"])}
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
        </div>
        """
        st.markdown(table_html, unsafe_allow_html=True)

    # ── Row 3: Trending topics ────────────────────────────────────────────────
    st.markdown(
        "<div class='section-header' style='margin-top:20px'>"
        "<span class='section-dot' style='background:#FF4757'></span>"
        "<span class='section-title'>Trending Topics</span></div>",
        unsafe_allow_html=True,
    )

    if not topics_df.empty:
        rng = np.random.default_rng(77)
        topics_display = topics_df.copy()
        topics_display["total"] = topics_display[["twitch","youtube"]].sum(axis=1)
        topics_display["change_pct"] = rng.uniform(-25, 40, len(topics_display)).round(1)
        topics_display = topics_display.sort_values("total", ascending=False)

        cols = st.columns(2)
        for i, (_, row) in enumerate(topics_display.iterrows()):
            chg = row["change_pct"]
            chg_color = "#00E5A0" if chg > 0 else "#FF4757"
            chg_str = f"+{chg:.1f}%" if chg > 0 else f"{chg:.1f}%"
            with cols[i % 2]:
                st.markdown(
                    f"""<div style='display:flex;justify-content:space-between;align-items:center;
                    padding:8px 12px;margin-bottom:6px;border-radius:7px;
                    background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.05)'>
                    <div style='display:flex;align-items:center;gap:10px'>
                      <span style='color:#444;font-family:monospace;font-size:10px;width:18px'>#{i+1}</span>
                      <span style='font-size:12px;font-weight:600;color:#DDD'>{row["topic"]}</span>
                    </div>
                    <div style='display:flex;align-items:center;gap:10px'>
                      <span style='font-size:11px;font-family:monospace;color:#AAA'>{_fmt(int(row["total"]))}</span>
                      <span style='font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;
                        background:{chg_color}22;color:{chg_color}'>{chg_str}</span>
                    </div></div>""",
                    unsafe_allow_html=True,
                )


def _chart_layout(**kwargs) -> dict:
    base = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#888", size=10),
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickfont=dict(color="#555")),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickfont=dict(color="#555")),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            font=dict(color="#888", size=10), bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(t=30, b=10, l=0, r=0),
        hovermode="y unified",
    )
    base.update(kwargs)
    return base
