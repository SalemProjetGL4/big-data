"""
Page 3 — Pipeline Health
Shows: per-source health summary cards, batch run history table,
       next-batch countdown, watermark status, error log.
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timezone, timedelta

from dashboard.db import (
    get_batch_run_history, get_source_health,
    SRC_COLORS,
)

INTERVAL_MINUTES = 15   # must match NLP_INTERVAL_MINUTES env var


def _fmt_duration(sec) -> str:
    if sec is None or pd.isna(sec):
        return "—"
    sec = int(sec)
    return f"{sec//60}m {sec%60}s" if sec >= 60 else f"{sec}s"


def _fmt_time(dt) -> str:
    if dt is None or (isinstance(dt, float) and pd.isna(dt)):
        return "—"
    if isinstance(dt, str):
        return dt
    return dt.strftime("%H:%M:%S")


def _status_badge(status: str) -> str:
    meta = {
        "success": ("badge-success", "✓ SUCCESS"),
        "failed": ("badge-failed", "✗ FAILED"),
        "running": ("badge-running", "● RUNNING"),
    }
    cls, label = meta.get(status, ("badge-running", status.upper()))
    return f"<span class='badge {cls}'>{label}</span>"


def _src_badge(src: str) -> str:
    c = SRC_COLORS.get(src, "#888")
    return (
        f"<span style='font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;"
        f"background:{c}22;color:{c};border:1px solid {c}44;"
        f"letter-spacing:1px;text-transform:uppercase'>{src}</span>"
    )


def render():
    st.markdown("## ⚙️ Pipeline Health")
    st.markdown(
        "<span style='color:#555;font-size:12px'>NLP Batch Scheduler · APScheduler · Interval mode</span>",
        unsafe_allow_html=True,
    )
    st.divider()

    health_df = get_source_health(["twitch", "youtube"])
    runs_df = get_batch_run_history(limit=40)

    # ── Countdown to next batch ───────────────────────────────────────────────
    now = datetime.now(timezone.utc)
    last_run = runs_df["started_at"].max() if not runs_df.empty else now - timedelta(minutes=5)
    if hasattr(last_run, "tzinfo") and last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)
    elapsed_since = (now - last_run).total_seconds()
    next_in = max(0, INTERVAL_MINUTES * 60 - elapsed_since)
    mins, secs = int(next_in // 60), int(next_in % 60)

    col_cd, col_mode, col_batch = st.columns(3)
    with col_cd:
        st.markdown(
            f"<div style='background:rgba(255,215,0,0.06);border:1px solid rgba(255,215,0,0.2);"
            f"border-radius:10px;padding:16px 20px'>"
            f"<div style='font-size:10px;color:#888;letter-spacing:1px;text-transform:uppercase;margin-bottom:6px'>Next batch in</div>"
            f"<div style='font-family:monospace;font-size:32px;font-weight:700;color:#FFD700'>{mins:02d}:{secs:02d}</div>"
            f"<div style='font-size:11px;color:#555;margin-top:4px'>Interval: every {INTERVAL_MINUTES} minutes</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with col_mode:
        st.markdown(
            f"<div style='background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.07);"
            f"border-radius:10px;padding:16px 20px'>"
            f"<div style='font-size:10px;color:#888;letter-spacing:1px;text-transform:uppercase;margin-bottom:6px'>Schedule Mode</div>"
            f"<div style='font-family:monospace;font-size:20px;font-weight:700;color:#DDD'>INTERVAL</div>"
            f"<div style='font-size:11px;color:#555;margin-top:4px'>coalesce=True · max_instances=1</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with col_batch:
        total_processed = runs_df[runs_df["status"] == "success"]["rows_processed"].sum() if not runs_df.empty else 0
        st.markdown(
            f"<div style='background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.07);"
            f"border-radius:10px;padding:16px 20px'>"
            f"<div style='font-size:10px;color:#888;letter-spacing:1px;text-transform:uppercase;margin-bottom:6px'>Total Rows (history)</div>"
            f"<div style='font-family:monospace;font-size:24px;font-weight:700;color:#DDD'>{total_processed:,}</div>"
            f"<div style='font-size:11px;color:#555;margin-top:4px'>{len(runs_df)} runs tracked</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    st.markdown("<div style='margin-top:20px'></div>", unsafe_allow_html=True)

    # ── Per-source health cards ───────────────────────────────────────────────
    st.markdown(
        "<div class='section-header'>"
        "<span class='section-dot' style='background:#00E5A0'></span>"
        "<span class='section-title'>Source Health (Last 24h)</span></div>",
        unsafe_allow_html=True,
    )

    src_cols = st.columns(2)
    for i, src in enumerate(["twitch", "youtube"]):
        row = health_df[health_df["source"] == src]
        if row.empty:
            continue
        row = row.iloc[0]
        rate = float(row["success_rate"] or 0)
        color = SRC_COLORS[src]
        status = str(row.get("last_status", "success"))
        st_meta = {
            "success": ("#00E5A0", "✓ HEALTHY"),
            "failed": ("#FF4757", "✗ DEGRADED"),
            "running": ("#FFD700", "● RUNNING"),
        }
        st_color, st_label = st_meta.get(status, ("#888", "UNKNOWN"))

        with src_cols[i]:
            st.markdown(
                f"""<div style='background:rgba(255,255,255,0.02);
                border:1px solid {color}33;border-top:3px solid {color};
                border-radius:10px;padding:18px'>
                <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:14px'>
                  <span style='font-size:13px;font-weight:700;color:{color};text-transform:uppercase;letter-spacing:1px'>{src}</span>
                  <span style='font-size:10px;font-weight:700;padding:2px 8px;border-radius:4px;
                    background:{st_color}22;color:{st_color};border:1px solid {st_color}44'>{st_label}</span>
                </div>
                <div style='display:grid;grid-template-columns:1fr 1fr;gap:10px'>
                  <div><div style='font-size:10px;color:#555;margin-bottom:2px'>Success Rate</div>
                       <div style='font-family:monospace;font-size:18px;font-weight:700;color:#DDD'>{rate:.1f}%</div></div>
                  <div><div style='font-size:10px;color:#555;margin-bottom:2px'>Total Runs</div>
                       <div style='font-family:monospace;font-size:18px;font-weight:700;color:#DDD'>{int(row["total_runs"])}</div></div>
                  <div><div style='font-size:10px;color:#555;margin-bottom:2px'>Avg Elapsed</div>
                       <div style='font-family:monospace;font-size:14px;font-weight:700;color:#AAA'>{_fmt_duration(row["avg_elapsed_sec"])}</div></div>
                  <div><div style='font-size:10px;color:#555;margin-bottom:2px'>Avg Rows</div>
                       <div style='font-family:monospace;font-size:14px;font-weight:700;color:#AAA'>{int(row["avg_rows"]):,}</div></div>
                </div>
                <div style='margin-top:12px;height:3px;background:#1C2333;border-radius:2px'>
                  <div style='width:{rate:.0f}%;height:100%;background:{color};border-radius:2px'></div>
                </div>
                </div>""",
                unsafe_allow_html=True,
            )

    st.markdown("<div style='margin-top:20px'></div>", unsafe_allow_html=True)

    # ── Rows processed over time bar chart ────────────────────────────────────
    st.markdown(
        "<div class='section-header'>"
        "<span class='section-dot' style='background:#4A90D9'></span>"
        "<span class='section-title'>Rows Processed per Batch Run</span></div>",
        unsafe_allow_html=True,
    )
    success_runs = runs_df[runs_df["status"] == "success"].copy()
    if not success_runs.empty:
        success_runs["started_at"] = pd.to_datetime(success_runs["started_at"])
        fig = go.Figure()
        for src in ["twitch", "youtube"]:
            src_runs = success_runs[success_runs["source"] == src].sort_values("started_at")
            fig.add_trace(go.Bar(
                x=src_runs["started_at"],
                y=src_runs["rows_processed"],
                name=src,
                marker_color=SRC_COLORS[src],
                hovertemplate=f"<b>{src}</b><br>%{{x|%H:%M}}<br>%{{y:,}} rows<extra></extra>",
            ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            barmode="group",
            font=dict(color="#888", size=10),
            xaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickfont=dict(color="#555")),
            yaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickfont=dict(color="#555"), tickformat=","),
            legend=dict(font=dict(color="#888", size=10), bgcolor="rgba(0,0,0,0)"),
            margin=dict(t=10, b=10, l=0, r=0),
            height=220,
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── Run history table ─────────────────────────────────────────────────────
    st.markdown(
        "<div class='section-header' style='margin-top:8px'>"
        "<span class='section-dot' style='background:#FF6314'></span>"
        "<span class='section-title'>Batch Run History</span></div>",
        unsafe_allow_html=True,
    )

    # Filters
    f_col1, f_col2, f_col3 = st.columns([2, 2, 3])
    with f_col1:
        filter_src = st.multiselect("Source", ["twitch", "youtube"],
                                     default=["twitch", "youtube"], key="ph_src")
    with f_col2:
        filter_stat = st.multiselect("Status", ["success", "failed", "running"],
                                     default=["success", "failed", "running"], key="ph_stat")
    with f_col3:
        search = st.text_input("Search run ID", placeholder="e.g. 20240115T…", key="ph_search")

    filtered = runs_df.copy()
    if filter_src:
        filtered = filtered[filtered["source"].isin(filter_src)]
    if filter_stat:
        filtered = filtered[filtered["status"].isin(filter_stat)]
    if search:
        filtered = filtered[filtered["run_id"].str.contains(search, case=False, na=False)]

    if filtered.empty:
        st.info("No runs match the current filters.")
    else:
        rows_html = ""
        for i, row in filtered.head(30).iterrows():
            bg = "rgba(255,255,255,0.01)" if i % 2 == 0 else "transparent"
            rows_html += (
                f"<tr style='border-bottom:1px solid rgba(255,255,255,0.04);background:{bg}'>"
                f"<td style='padding:9px 14px;font-family:monospace;color:#666;font-size:11px'>{row['run_id']}</td>"
                f"<td style='padding:9px 14px'>{_src_badge(row['source'])}</td>"
                f"<td style='padding:9px 14px'>{_status_badge(row['status'])}</td>"
                f"<td style='padding:9px 14px;color:#888;font-family:monospace;font-size:11px'>{_fmt_time(row['started_at'])}</td>"
                f"<td style='padding:9px 14px;color:#AAA;font-family:monospace'>{_fmt_duration(row['elapsed_sec'])}</td>"
                f"<td style='padding:9px 14px;color:#DDD;font-family:monospace;font-weight:600'>"
                f"{'processing…' if row['status']=='running' else f'{int(row['rows_processed']):,}'}"</n+                f"</td>"
                f"<td style='padding:9px 14px;color:#FF4757;font-size:11px'>"
                f"{'⚠ ' + str(row['error_message'])[:60] + '…' if row['error_message'] else ''}</td>"
                f"</tr>"
            )

        table_html = f"""
        <div style='overflow-x:auto;border:1px solid rgba(255,255,255,0.06);
                    border-radius:10px;margin-top:8px'>
        <table style='width:100%;border-collapse:collapse;font-size:12px'>
          <thead>
            <tr style='border-bottom:1px solid rgba(255,255,255,0.08)'>
              {"".join(
                f"<th style='padding:9px 14px;text-align:left;color:#555;font-size:10px;"
                f"font-weight:700;letter-spacing:1px;text-transform:uppercase'>{h}</th>"
                for h in ["Run ID", "Source", "Status", "Started", "Duration", "Rows", "Error"]
              )}
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
        </div>
        """
        st.markdown(table_html, unsafe_allow_html=True)
        st.caption(f"Showing {min(30, len(filtered))} of {len(filtered)} runs")
