"""
NLP Batch Scheduler
====================
Replaces the streaming NLP job with a configurable periodic batch processor.

Architecture:
  APScheduler (cron / interval trigger)
    └─► NLPBatchJob.run()
          ├─► read unprocessed docs from MongoDB   (watermark via `nlp_processed_at IS NULL`)
          ├─► EmoteCleaner  (Spark UDF)
          ├─► SentimentAnalyser (VADER / Transformer)
          ├─► SubjectExtractor  (spaCy)
          ├─► write enriched docs → MongoDB  *_enriched collections
          └─► write aggregates   → PostgreSQL sentiment_trends / subject_trends

Schedule options (set via env vars):
  NLP_SCHEDULE_MODE=interval   NLP_INTERVAL_MINUTES=15      (default)
  NLP_SCHEDULE_MODE=cron       NLP_CRON_EXPR="0 * * * *"   (every hour)
  NLP_SCHEDULE_MODE=manual                                  (call /run endpoint)

Batch size is controlled by NLP_BATCH_SIZE (default 50 000 rows per source).

Run:
  python spark/nlp_batch_scheduler.py
"""

import logging
import os
import sys
import signal
import time
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config from env ───────────────────────────────────────────────────────────
SCHEDULE_MODE      = os.getenv("NLP_SCHEDULE_MODE", "interval")   # interval | cron | manual
INTERVAL_MINUTES   = int(os.getenv("NLP_INTERVAL_MINUTES", "15"))
CRON_EXPR          = os.getenv("NLP_CRON_EXPR", "0 * * * *")       # default: every hour
BATCH_SIZE         = int(os.getenv("NLP_BATCH_SIZE", "50000"))
SENTIMENT_BACKEND  = os.getenv("NLP_SENTIMENT_BACKEND", "vader")   # vader | transformer
SPACY_MODEL        = os.getenv("NLP_SPACY_MODEL", "en_core_web_sm")
MAX_TEXT_LEN       = int(os.getenv("NLP_MAX_TEXT_LEN", "512"))

# ── Spark singleton (initialised once, reused across batches) ─────────────────
_spark: Optional[SparkSession] = None

def get_spark() -> SparkSession:
    global _spark
    if _spark is None or _spark._sc._jsc is None:
        logger.info("Initialising SparkSession…")
        _spark = (
            SparkSession.builder
            .appName("SocialPipeline-NLP-Batch")
            .config("spark.mongodb.connection.uri", settings.mongo.uri)
            .config("spark.mongodb.database", settings.mongo.database)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))
            .config("spark.driver.memory",   os.getenv("SPARK_DRIVER_MEMORY", "2g"))
            # Adaptive query execution — helps with skewed NLP batches
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("WARN")
    return _spark


# ── UDF registry (lazy, one-time per SparkSession) ────────────────────────────
_udfs_registered = False

def ensure_udfs(spark: SparkSession) -> dict:
    global _udfs_registered
    from nlp.emote_cleaner import make_clean_udf
    from nlp.content_moderator import make_moderation_udfs
    from nlp.sentiment import make_sentiment_udf
    from nlp.subject_extractor import make_subject_udfs

    clean_udf                          = make_clean_udf()
    mod_text_udf, severity_udf, rejected_udf, flagged_udf = make_moderation_udfs()
    label_udf, score_udf, compound_udf = make_sentiment_udf(backend=SENTIMENT_BACKEND)
    primary_udf, subjects_udf, topics_udf = make_subject_udfs(model=SPACY_MODEL)

    if not _udfs_registered:
        spark.udf.register("clean_text",           clean_udf)
        spark.udf.register("moderated_text",      mod_text_udf)
        spark.udf.register("content_severity",    severity_udf)
        spark.udf.register("content_rejected",    rejected_udf)
        spark.udf.register("flagged_term_count",  flagged_udf)
        spark.udf.register("sentiment_label",      label_udf)
        spark.udf.register("sentiment_score",      score_udf)
        spark.udf.register("sentiment_compound",   compound_udf)
        spark.udf.register("primary_subject",      primary_udf)
        spark.udf.register("subjects",             subjects_udf)
        spark.udf.register("topics",               topics_udf)
        _udfs_registered = True
        logger.info("NLP UDFs registered on SparkSession")

    return {
        "clean": clean_udf, "label": label_udf,
        "score": score_udf, "compound": compound_udf,
        "moderated": mod_text_udf, "severity": severity_udf,
        "rejected": rejected_udf, "flagged": flagged_udf,
        "primary": primary_udf, "subjects": subjects_udf, "topics": topics_udf,
    }


# ── Batch state tracker ───────────────────────────────────────────────────────

class BatchState:
    """
    Tracks per-source watermarks so each batch only processes NEW documents.
    Persisted to MongoDB `nlp_batch_state` collection between runs.
    """

    COLLECTION = "nlp_batch_state"

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def get_watermark(self, source: str) -> Optional[str]:
        """Returns ISO timestamp of last processed doc for this source, or None."""
        try:
            df = (
                self._spark.read
                .format("mongodb")
                .option("spark.mongodb.database", settings.mongo.database)
                .option("spark.mongodb.collection", self.COLLECTION)
                .load()
                .filter(F.col("source") == source)
                .orderBy(F.col("updated_at").desc())
                .limit(1)
            )
            rows = df.collect()
            return rows[0]["last_ingested_at"] if rows else None
        except Exception as exc:
            logger.warning("Could not read watermark for %s: %s", source, exc)
            return None

    def set_watermark(self, source: str, last_ingested_at: str) -> None:
        """Upsert the watermark for this source."""
        try:
            import pymongo
            client = pymongo.MongoClient(settings.mongo.uri)
            db = client[settings.mongo.database]
            db[self.COLLECTION].update_one(
                {"source": source},
                {"$set": {
                    "source": source,
                    "last_ingested_at": last_ingested_at,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }},
                upsert=True,
            )
            client.close()
        except Exception as exc:
            logger.error("Could not persist watermark for %s: %s", source, exc)


# ── Core batch job ────────────────────────────────────────────────────────────

class NLPBatchJob:
    """
    Single-run NLP batch: reads unprocessed raw docs → enriches → writes.
    Designed to be called by the scheduler; safe to call manually too.
    """

    # Map source name → (raw collection, enriched collection, text columns, id column)
    SOURCE_CONFIG = {
        "twitch": (
            settings.mongo.twitch_collection,
            "twitch_enriched",
            ["title"],
            "stream_id",
        ),
        "youtube": (
            settings.mongo.youtube_collection,
            "youtube_enriched",
            ["title", "description"],
            "video_id",
        ),
    }

    def __init__(self):
        self._stats: dict = {}

    def run(self) -> dict:
        """
        Execute one full batch across all three sources.
        Returns a summary dict with row counts and timings.
        """
        run_start = time.time()
        run_id    = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        logger.info("▶ NLP batch run started  [run_id=%s]", run_id)

        spark = get_spark()
        udfs  = ensure_udfs(spark)
        state = BatchState(spark)
        summary = {"run_id": run_id, "sources": {}}

        for source, (raw_col, enriched_col, text_cols, id_col) in self.SOURCE_CONFIG.items():
            src_start  = time.time()
            watermark  = state.get_watermark(source)
            try:
                processed, max_ts = self._process_source(
                    spark, udfs, source, raw_col, enriched_col,
                    text_cols, id_col, watermark,
                )
                if max_ts:
                    state.set_watermark(source, max_ts)
                elapsed = round(time.time() - src_start, 2)
                summary["sources"][source] = {
                    "rows_processed": processed,
                    "elapsed_sec": elapsed,
                    "watermark": max_ts or watermark,
                }
                logger.info(
                    "  ✓ %-10s  rows=%-7d  elapsed=%ss",
                    source, processed, elapsed,
                )
            except Exception as exc:
                logger.error("  ✗ %s batch failed: %s", source, exc, exc_info=True)
                summary["sources"][source] = {"error": str(exc)}

        summary["total_elapsed_sec"] = round(time.time() - run_start, 2)
        logger.info(
            "▶ NLP batch run complete [run_id=%s]  total=%.1fs",
            run_id, summary["total_elapsed_sec"],
        )
        return summary

    # ── Per-source processing ─────────────────────────────────────────────────

    def _process_source(
        self,
        spark: SparkSession,
        udfs: dict,
        source: str,
        raw_col: str,
        enriched_col: str,
        text_cols: list,
        id_col: str,
        watermark: Optional[str],
    ):
        """
        Read → filter unprocessed → NLP → write enriched + aggregates.
        Returns (rows_processed, max_ingested_at_str).
        """
        # 1. Read raw docs
        raw_df = (
            spark.read
            .format("mongodb")
            .option("spark.mongodb.database", settings.mongo.database)
            .option("spark.mongodb.collection", raw_col)
            .load()
        )

        # 2. Filter: only docs ingested after the watermark
        if watermark:
            raw_df = raw_df.filter(F.col("ingested_at") > F.lit(watermark))

        # 3. Enforce batch size cap (FIFO — oldest first)
        raw_df = raw_df.orderBy(F.col("ingested_at").asc()).limit(BATCH_SIZE)

        count = raw_df.cache().count()
        if count == 0:
            logger.info("  ○ %s — no new docs since watermark %s", source, watermark)
            return 0, None

        # 4. Build combined text column
        text_expr = F.concat_ws(
            " ",
            *[
                F.coalesce(F.col(c).substr(1, MAX_TEXT_LEN), F.lit(""))
                for c in text_cols
                if c in raw_df.columns
            ],
        )
        df = raw_df.withColumn("_nlp_text", text_expr)

        moderated = (
            df
            .withColumn("moderated_text", udfs["moderated"](F.col("_nlp_text")))
            .withColumn("content_severity", udfs["severity"](F.col("_nlp_text")))
            .withColumn("content_rejected", udfs["rejected"](F.col("_nlp_text")))
            .withColumn("flagged_term_count", udfs["flagged"](F.col("_nlp_text")))
            .filter(~F.col("content_rejected"))
        )

        # 5. Apply NLP pipeline
        enriched = (
            moderated
            .withColumn("cleaned_text",       udfs["clean"](F.col("moderated_text")))
            .withColumn("sentiment",           udfs["label"](F.col("cleaned_text")))
            .withColumn("sentiment_score",     udfs["score"](F.col("cleaned_text")))
            .withColumn("sentiment_compound",  udfs["compound"](F.col("cleaned_text")))
            .withColumn("primary_subject",     udfs["primary"](F.col("cleaned_text")))
            .withColumn("subjects",            udfs["subjects"](F.col("cleaned_text")))
            .withColumn("topics",              udfs["topics"](F.col("cleaned_text")))
            .withColumn("nlp_processed_at",    F.current_timestamp())
            .withColumn("nlp_run_id",          F.lit(
                datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            ))
            .drop("_nlp_text")
        )

        enriched.cache()

        # 6. Write enriched docs to MongoDB
        (
            enriched.write
            .format("mongodb")
            .mode("append")
            .option("spark.mongodb.database", settings.mongo.database)
            .option("spark.mongodb.collection", enriched_col)
            .save()
        )

        # 7. Write aggregates to Postgres
        self._write_aggregates(enriched, source)

        # 8. Compute new watermark = max ingested_at in this batch
        max_row = enriched.agg(F.max("ingested_at").alias("max_ts")).collect()
        max_ts  = str(max_row[0]["max_ts"]) if max_row and max_row[0]["max_ts"] else None

        enriched.unpersist()
        raw_df.unpersist()
        return count, max_ts

    # ── Aggregate writers ─────────────────────────────────────────────────────

    def _write_aggregates(self, enriched_df: DataFrame, source: str) -> None:
        """Write per-minute sentiment + subject aggregates to Postgres."""
        pg_props = {
            "user":   settings.postgres.user,
            "password": settings.postgres.password,
            "driver": settings.postgres.driver,
            "batchsize": "1000",
        }

        # Sentiment trends (per minute)
        sentiment_agg = (
            enriched_df
            .withColumn("minute", F.date_trunc("minute", F.col("nlp_processed_at")))
            .withColumn("source_col", F.lit(source))
            .groupBy("minute", "source_col", "sentiment")
            .agg(
                F.count("*").alias("event_count"),
                F.avg("sentiment_compound").alias("avg_compound"),
            )
            .withColumnRenamed("source_col", "source")
        )
        try:
            sentiment_agg.write.jdbc(
                url=settings.postgres.url,
                table="sentiment_trends",
                mode="append",
                properties=pg_props,
            )
        except Exception as exc:
            logger.warning("Could not write sentiment_trends: %s", exc)

        # Subject trends (per minute, exploded)
        if "subjects" in enriched_df.columns:
            subject_agg = (
                enriched_df
                .withColumn("subject", F.explode(F.col("subjects")))
                .withColumn("minute", F.date_trunc("minute", F.col("nlp_processed_at")))
                .withColumn("source_col", F.lit(source))
                .groupBy("minute", "source_col", "subject")
                .agg(
                    F.count("*").alias("mention_count"),
                    F.avg("sentiment_compound").alias("avg_sentiment"),
                )
                .filter(
                    F.col("subject").isNotNull()
                    & (F.length(F.col("subject")) > 2)
                )
                .withColumnRenamed("source_col", "source")
            )
            try:
                subject_agg.write.jdbc(
                    url=settings.postgres.url,
                    table="subject_trends",
                    mode="append",
                    properties=pg_props,
                )
            except Exception as exc:
                logger.warning("Could not write subject_trends: %s", exc)


# ── Scheduler setup ───────────────────────────────────────────────────────────

class NLPBatchScheduler:
    def __init__(self):
        self._scheduler = BackgroundScheduler(timezone="UTC")
        self._job = NLPBatchJob()

    def _job_listener(self, event):
        if event.exception:
            logger.error("NLP job failed: %s", event.exception)
        else:
            logger.info("NLP job finished successfully")

    def start(self):
        if SCHEDULE_MODE == "cron":
            trigger = CronTrigger.from_crontab(CRON_EXPR)
        elif SCHEDULE_MODE == "manual":
            trigger = None
        else:
            trigger = IntervalTrigger(minutes=INTERVAL_MINUTES)

        if trigger:
            self._scheduler.add_job(self._job.run, trigger=trigger)
            self._scheduler.add_listener(
                self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
            )
            self._scheduler.start()
            logger.info("Scheduler started (%s)", SCHEDULE_MODE)
        else:
            logger.info("Manual mode — no schedule configured")

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler shutting down…")
            self._scheduler.shutdown()


def main():
    scheduler = NLPBatchScheduler()
    scheduler.start()


if __name__ == "__main__":
    main()
