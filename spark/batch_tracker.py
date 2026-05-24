"""
Batch run tracker — persists every batch run's metadata to Postgres
for observability, retry logic, and the dashboard's pipeline health panel.

Table: nlp_batch_runs
  run_id, started_at, finished_at, status, source, rows_processed,
  error_message, watermark_from, watermark_to, elapsed_sec
"""
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

logger = logging.getLogger(__name__)


def _get_conn():
    url = settings.postgres.url.replace("jdbc:", "")
    parsed = urlparse(url)
    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/"),
        user=settings.postgres.user,
        password=settings.postgres.password,
    )


def init_batch_run_table() -> None:
    """Create the batch run tracking table if it doesn't exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS nlp_batch_runs (
        id               BIGSERIAL   PRIMARY KEY,
        run_id           TEXT        NOT NULL,
        source           TEXT        NOT NULL,
        status           TEXT        NOT NULL,   -- running | success | failed
        started_at       TIMESTAMPTZ NOT NULL,
        finished_at      TIMESTAMPTZ,
        rows_processed   BIGINT      DEFAULT 0,
        elapsed_sec      DOUBLE PRECISION,
        watermark_from   TEXT,
        watermark_to     TEXT,
        error_message    TEXT,
        extra            JSONB,
        UNIQUE (run_id, source)
    );
    CREATE INDEX IF NOT EXISTS idx_nbr_run_id    ON nlp_batch_runs (run_id);
    CREATE INDEX IF NOT EXISTS idx_nbr_source    ON nlp_batch_runs (source);
    CREATE INDEX IF NOT EXISTS idx_nbr_started   ON nlp_batch_runs (started_at DESC);
    CREATE INDEX IF NOT EXISTS idx_nbr_status    ON nlp_batch_runs (status);
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
        logger.info("nlp_batch_runs table ready")
    except Exception as exc:
        logger.error("Could not init nlp_batch_runs: %s", exc)


class BatchRunTracker:
    """
    Context manager that records batch run metadata to Postgres.

    Usage:
        with BatchRunTracker(run_id, source, watermark_from) as tracker:
            rows = do_work()
            tracker.rows_processed = rows
            tracker.watermark_to   = new_watermark
        # On exit: writes success/failed row to Postgres
    """

    def __init__(self, run_id: str, source: str, watermark_from: Optional[str] = None):
        self.run_id         = run_id
        self.source         = source
        self.watermark_from = watermark_from
        self.watermark_to:   Optional[str]   = None
        self.rows_processed: int             = 0
        self._started_at     = datetime.now(timezone.utc)
        self._status         = "running"
        self._error: Optional[str] = None
        self._insert_start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        finished_at = datetime.now(timezone.utc)
        elapsed     = (finished_at - self._started_at).total_seconds()
        if exc_type:
            self._status = "failed"
            self._error  = str(exc_val)[:2000]
        else:
            self._status = "success"
        self._update_finish(finished_at, elapsed)
        return False  # don't suppress exceptions

    def _insert_start(self) -> None:
        sql = """
        INSERT INTO nlp_batch_runs
            (run_id, source, status, started_at, watermark_from)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (run_id, source) DO NOTHING;
        """
        try:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        self.run_id, self.source, "running",
                        self._started_at.isoformat(), self.watermark_from,
                    ))
                conn.commit()
        except Exception as exc:
            logger.warning("Could not insert batch run start: %s", exc)

    def _update_finish(self, finished_at: datetime, elapsed: float) -> None:
        sql = """
        UPDATE nlp_batch_runs SET
            status          = %s,
            finished_at     = %s,
            elapsed_sec     = %s,
            rows_processed  = %s,
            watermark_to    = %s,
            error_message   = %s
        WHERE run_id = %s AND source = %s;
        """
        try:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        self._status, finished_at.isoformat(), elapsed,
                        self.rows_processed, self.watermark_to,
                        self._error, self.run_id, self.source,
                    ))
                conn.commit()
        except Exception as exc:
            logger.warning("Could not update batch run finish: %s", exc)


def get_failed_runs(max_age_hours: int = 24) -> list:
    """Return recent failed batch runs for retry logic."""
    sql = """
    SELECT run_id, source, watermark_from, started_at, error_message
    FROM nlp_batch_runs
    WHERE status = 'failed'
      AND started_at > NOW() - INTERVAL '%s hours'
    ORDER BY started_at DESC;
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (max_age_hours,))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("Could not query failed runs: %s", exc)
        return []


def get_run_history(limit: int = 50) -> list:
    """Return recent batch run history for the dashboard."""
    sql = """
    SELECT run_id, source, status, started_at, finished_at,
           rows_processed, elapsed_sec, error_message
    FROM nlp_batch_runs
    ORDER BY started_at DESC
    LIMIT %s;
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (limit,))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("Could not query run history: %s", exc)
        return []
