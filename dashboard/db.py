"""
Data access layer for the Streamlit dashboard.

All functions return pandas DataFrames.
If the database is unreachable (e.g. local dev without infra),
they fall back to realistic simulated data so the dashboard
is always runnable.

Real queries are designed for the tables created by:
  - scripts/init_postgres.sql
  - scripts/init_nlp_tables.sql
"""
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://pipeline:pipeline_password@localhost:5432/analytics",
)
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://root:rootpassword@localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DATABASE", "social_raw")
USE_REAL_DB = os.getenv("DASHBOARD_USE_DB", "false").lower() == "true"

SOURCES = ["twitch", "youtube"]
SRC_COLORS = {"twitch": "#9146FF", "youtube": "#FF0033"}
SENT_COLORS = {"positive": "#00E5A0", "negative": "#FF4757", "neutral": "#4A90D9"}

SUBJECTS_POOL = [
    "Fortnite", "Minecraft", "Valorant", "Elden Ring", "GTA VI", "CS2",
    "Apex Legends", "xQc", "Pokimane", "MrBeast", "Ludwig", "Shroud",
    "Ninja", "Asmongold", "AI", "ChatGPT", "Tesla", "Apple", "gaming",
    "esports", "speedrun", "world record", "patch notes", "new update",
    "drama", "banned", "sub goal", "Cyberpunk", "Palworld",
]
TOPICS_POOL = [
    "fortnite", "minecraft", "valorant", "elden_ring",
    "just_chatting", "speedrun", "cs2", "apex_legends", "irl", "music",
]


# ── Postgres connection (cached per session) ──────────────────────────────────

def _get_pg_engine():
    try:
        from sqlalchemy import create_engine
        return create_engine(POSTGRES_URL, pool_pre_ping=True)
    except Exception as exc:
        logger.warning("Could not create Postgres engine: %s", exc)
        return None


def _query_pg(sql: str, params: dict = None) -> Optional[pd.DataFrame]:
    if not USE_REAL_DB:
        return None
    engine = _get_pg_engine()
    if engine is None:
        return None
    try:
        return pd.read_sql(sql, engine, params=params)
    except Exception as exc:
        logger.warning("Postgres query failed: %s", exc)
        return None


# ── Simulation helpers ────────────────────────────────────────────────────────

def _sim_time_index(hours: int, freq: str = "1min") -> pd.DatetimeIndex:
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)
    return pd.date_range(start=start, end=end, freq=freq)


def _window_hours(time_window: str) -> int:
    mapping = {
        "Last 1 hour": 1, "Last 6 hours": 6,
        "Last 24 hours": 24, "Last 7 days": 168,
    }
    return mapping.get(time_window, 24)


# ── Public API ────────────────────────────────────────────────────────────────

def get_sentiment_trend(sources: list, time_window: str) -> pd.DataFrame:
    """
    Returns per-minute sentiment breakdown per source.
    Columns: minute, source, positive, negative, neutral, avg_compound
    """
    hours = _window_hours(time_window)
    sql = """
        SELECT
            date_trunc('minute', minute) AS minute,
            source,
            SUM(CASE WHEN sentiment='positive' THEN event_count ELSE 0 END) AS positive,
            SUM(CASE WHEN sentiment='negative' THEN event_count ELSE 0 END) AS negative,
            SUM(CASE WHEN sentiment='neutral'  THEN event_count ELSE 0 END) AS neutral,
            AVG(avg_compound) AS avg_compound
        FROM sentiment_trends
        WHERE minute > NOW() - make_interval(hours => :hours)
          AND source = ANY(:sources)
        GROUP BY 1, 2
        ORDER BY 1
    """
    df = _query_pg(sql, {"hours": hours, "sources": sources})
    if df is not None:
        return df

    # ── Simulation ────────────────────────────────────────────────────────────
    freq = "5min" if hours >= 24 else "1min"
    idx = _sim_time_index(hours, freq)
    rows = []
    for src in sources:
        rng = np.random.default_rng(abs(hash(src)) % (2**31))
        pos = np.clip(rng.normal(0.42, 0.06, len(idx)), 0.05, 0.95)
        neg = np.clip(rng.normal(0.25, 0.04, len(idx)), 0.02, 0.90)
        neu = np.clip(1 - pos - neg, 0.02, 0.90)
        cmp = (pos - neg).round(4)
        for i, ts in enumerate(idx):
            rows.append({
                "minute": ts, "source": src,
                "positive": round(pos[i], 4),
                "negative": round(neg[i], 4),
                "neutral": round(neu[i], 4),
                "avg_compound": round(float(cmp[i]), 4),
            })
    return pd.DataFrame(rows)


def get_event_volume(sources: list, time_window: str) -> pd.DataFrame:
    """
    Returns per-minute event volume per source.
    Columns: minute, source, event_count
    """
    hours = _window_hours(time_window)
    sql = """
        SELECT date_trunc('minute', minute) AS minute, source,
               SUM(event_count) AS event_count
        FROM sentiment_trends
        WHERE minute > NOW() - make_interval(hours => :hours)
          AND source = ANY(:sources)
        GROUP BY 1, 2
        ORDER BY 1
    """
    df = _query_pg(sql, {"hours": hours, "sources": sources})
    if df is not None:
        return df

    freq = "5min" if hours >= 24 else "1min"
    idx = _sim_time_index(hours, freq)
    base = {"twitch": 1200, "youtube": 850}
    rows = []
    for src in sources:
        rng = np.random.default_rng(abs(hash(src + "vol")) % (2**31))
        vals = np.clip(rng.normal(base.get(src, 800), 150, len(idx)), 50, 5000).astype(int)
        for i, ts in enumerate(idx):
            rows.append({"minute": ts, "source": src, "event_count": int(vals[i])})
    return pd.DataFrame(rows)


def get_top_subjects(sources: list, time_window: str, limit: int = 15) -> pd.DataFrame:
    """
    Returns top subjects by total mentions across sources.
    Columns: subject, twitch, youtube, total, avg_sentiment
    """
    hours = _window_hours(time_window)
    sql = """
        SELECT subject,
               SUM(CASE WHEN source='twitch'  THEN mention_count ELSE 0 END) AS twitch,
               SUM(CASE WHEN source='youtube' THEN mention_count ELSE 0 END) AS youtube,
               SUM(mention_count)   AS total,
               AVG(avg_sentiment)   AS avg_sentiment
        FROM subject_trends
        WHERE minute > NOW() - make_interval(hours => :hours)
          AND source = ANY(:sources)
        GROUP BY subject
        ORDER BY total DESC
        LIMIT :limit
    """
    df = _query_pg(sql, {"hours": hours, "sources": sources, "limit": limit})
    if df is not None:
        return df

    rng = np.random.default_rng(42)
    subjects = rng.choice(SUBJECTS_POOL, size=min(limit, len(SUBJECTS_POOL)), replace=False)
    rows = []
    for sub in subjects:
        tw = int(rng.integers(200, 2500)) if "twitch" in sources else 0
        yt = int(rng.integers(100, 3500)) if "youtube" in sources else 0
        rows.append({
            "subject": sub, "twitch": tw, "youtube": yt,
            "total": tw + yt,
            "avg_sentiment": round(float(rng.uniform(-0.6, 0.8)), 3),
        })
    return pd.DataFrame(rows).sort_values("total", ascending=False).reset_index(drop=True)


def get_topic_distribution(sources: list, time_window: str) -> pd.DataFrame:
    """
    Returns mention counts per topic per source for the radar chart.
    Columns: topic, twitch, youtube
    """
    hours = _window_hours(time_window)
    sql = """
        SELECT SPLIT_PART(subject, ':', 2) AS topic,
               SUM(CASE WHEN source='twitch'  THEN mention_count ELSE 0 END) AS twitch,
               SUM(CASE WHEN source='youtube' THEN mention_count ELSE 0 END) AS youtube
        FROM subject_trends
        WHERE minute > NOW() - make_interval(hours => :hours)
          AND subject LIKE 'gaming:%'
        GROUP BY 1
        ORDER BY (twitch+youtube) DESC
        LIMIT 8
    """
    df = _query_pg(sql, {"hours": hours})
    if df is not None:
        return df

    rng = np.random.default_rng(99)
    rows = []
    for topic in TOPICS_POOL:
        rows.append({
            "topic": topic.replace("_", " "),
            "twitch": int(rng.integers(10, 100)) if "twitch" in sources else 0,
            "youtube": int(rng.integers(10, 100)) if "youtube" in sources else 0,
        })
    return pd.DataFrame(rows)


def get_compound_over_time(sources: list, time_window: str) -> pd.DataFrame:
    """
    Returns avg compound sentiment score per source over time.
    Columns: minute, source, avg_compound
    """
    df = get_sentiment_trend(sources, time_window)
    return df[["minute", "source", "avg_compound"]]


def get_batch_run_history(limit: int = 50) -> pd.DataFrame:
    """
    Returns NLP batch run history from nlp_batch_runs table.
    Columns: run_id, source, status, started_at, finished_at,
             elapsed_sec, rows_processed, error_message
    """
    sql = """
        SELECT run_id, source, status, started_at, finished_at,
               elapsed_sec, rows_processed, error_message
        FROM nlp_batch_runs
        ORDER BY started_at DESC
        LIMIT :limit
    """
    df = _query_pg(sql, {"limit": limit})
    if df is not None:
        return df

    # Simulation
    rng = np.random.default_rng(7)
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(min(limit, 30)):
        src = SOURCES[i % len(SOURCES)]
        status = "running" if i == 0 else ("failed" if rng.random() < 0.08 else "success")
        start = now - timedelta(minutes=i * 15 + rng.integers(0, 5))
        elapsed = None if status == "running" else int(rng.integers(35, 280))
        rows.append({
            "run_id": start.strftime("%Y%m%dT%H%M%SZ"),
            "source": src,
            "status": status,
            "started_at": start,
            "finished_at": None if status == "running" else start + timedelta(seconds=elapsed),
            "elapsed_sec": elapsed,
            "rows_processed": 0 if status in ("failed", "running") else int(rng.integers(3000, 48000)),
            "error_message": "MongoTimeout: operation exceeded 30s" if status == "failed" else None,
        })
    return pd.DataFrame(rows)


def get_source_health(sources: list) -> pd.DataFrame:
    """
    Returns last-24h health summary per source.
    Columns: source, total_runs, success_runs, failed_runs,
             success_rate, avg_elapsed_sec, avg_rows, last_run_at, last_status
    """
    sql = """
        SELECT source,
               COUNT(*)                                    AS total_runs,
               SUM(CASE WHEN status='success' THEN 1 END) AS success_runs,
               SUM(CASE WHEN status='failed'  THEN 1 END) AS failed_runs,
               ROUND(AVG(CASE WHEN status='success' THEN 100.0 END), 1) AS success_rate,
               ROUND(AVG(elapsed_sec), 1)                 AS avg_elapsed_sec,
               ROUND(AVG(rows_processed), 0)              AS avg_rows,
               MAX(started_at)                            AS last_run_at
        FROM nlp_batch_runs
        WHERE started_at > NOW() - INTERVAL '24 hours'
          AND source = ANY(:sources)
        GROUP BY source
    """
    df = _query_pg(sql, {"sources": sources})
    if df is not None:
        return df

    rng = np.random.default_rng(13)
    rows = []
    for src in SOURCES:
        if src not in sources:
            continue
        total = int(rng.integers(8, 24))
        failed = int(rng.integers(0, 2))
        success = total - failed
        rows.append({
            "source": src,
            "total_runs": total,
            "success_runs": success,
            "failed_runs": failed,
            "success_rate": round(success / total * 100, 1),
            "avg_elapsed_sec": round(float(rng.uniform(45, 230)), 1),
            "avg_rows": int(rng.integers(8000, 45000)),
            "last_run_at": datetime.now(timezone.utc) - timedelta(minutes=int(rng.integers(1, 20))),
            "last_status": "failed" if failed > 0 and rng.random() < 0.3 else "success",
        })
    return pd.DataFrame(rows)


def get_total_events_processed() -> dict:
    """Returns aggregate counters for the top stat bar."""
    sql = """
        SELECT
            SUM(event_count)                      AS total_events,
            COUNT(DISTINCT source)                AS active_sources,
            MAX(minute)                           AS last_event_at
        FROM sentiment_trends
        WHERE minute > NOW() - INTERVAL '24 hours'
    """
    df = _query_pg(sql)
    if df is not None and not df.empty:
        row = df.iloc[0]
        return {
            "total_events": int(row["total_events"] or 0),
            "active_sources": int(row["active_sources"] or 0),
            "last_event_at": row["last_event_at"],
        }
    rng = np.random.default_rng()
    return {
        "total_events": 847_293 + int(rng.integers(0, 50000)),
        "active_sources": 2,
        "last_event_at": datetime.now(timezone.utc),
    }
