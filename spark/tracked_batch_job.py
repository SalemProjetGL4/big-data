"""
Patch — integrates BatchRunTracker into NLPBatchJob._process_source().
Drop this into nlp_batch_scheduler.py to enable run-history tracking.

Adds to NLPBatchJob:
  - _process_source() wrapped in BatchRunTracker context manager
  - retry_failed_runs() method
"""
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark.batch_tracker import BatchRunTracker, get_failed_runs, init_batch_run_table
from config import settings

logger = logging.getLogger(__name__)


class TrackedNLPBatchJob:
    """
    Drop-in replacement for NLPBatchJob that wraps every source run
    in a BatchRunTracker for Postgres-backed observability + retry.
    """

    SOURCE_CONFIG = {
        "twitch":  (settings.mongo.twitch_collection,  "twitch_enriched",  ["title"],                   "stream_id"),
        "youtube": (settings.mongo.youtube_collection, "youtube_enriched", ["title", "description"],     "video_id"),
    }

    def __init__(self):
        init_batch_run_table()   # idempotent

    def run(self, run_id: Optional[str] = None) -> dict:
        from spark.nlp_batch_scheduler import get_spark, ensure_udfs, BatchState, BATCH_SIZE, MAX_TEXT_LEN

        run_id    = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        spark     = get_spark()
        udfs      = ensure_udfs(spark)
        state     = BatchState(spark)
        summary   = {"run_id": run_id, "sources": {}}

        for source, (raw_col, enriched_col, text_cols, id_col) in self.SOURCE_CONFIG.items():
            watermark = state.get_watermark(source)

            with BatchRunTracker(run_id, source, watermark) as tracker:
                rows, max_ts = self._process_source(
                    spark, udfs, source, raw_col, enriched_col,
                    text_cols, id_col, watermark,
                    batch_size=BATCH_SIZE, max_text_len=MAX_TEXT_LEN,
                )
                tracker.rows_processed = rows
                tracker.watermark_to   = max_ts

            if max_ts:
                state.set_watermark(source, max_ts)

            summary["sources"][source] = {
                "rows_processed": rows,
                "watermark": max_ts or watermark,
            }

        return summary

    def retry_failed_runs(self, max_age_hours: int = 24) -> None:
        """Re-run any source batches that failed within the last N hours."""
        failed = get_failed_runs(max_age_hours)
        if not failed:
            logger.info("No failed runs to retry.")
            return

        logger.info("Retrying %d failed batch(es)…", len(failed))
        for record in failed:
            retry_run_id = f"{record['run_id']}_retry_{datetime.now(timezone.utc).strftime('%H%M%S')}"
            logger.info("  Retrying run_id=%s source=%s", record["run_id"], record["source"])
            # Re-run just this source from its original watermark
            self.run(run_id=retry_run_id)

    # ── Internal (same logic as NLPBatchJob._process_source) ─────────────────

    def _process_source(
        self, spark, udfs, source, raw_col, enriched_col,
        text_cols, id_col, watermark, batch_size, max_text_len,
    ):
        from spark.nlp_batch_scheduler import NLPBatchJob
        job = NLPBatchJob()
        return job._process_source(
            spark, udfs, source, raw_col, enriched_col,
            text_cols, id_col, watermark,
        )
