"""
NLP Enrichment Job — Spark Structured Streaming stage that reads
processed events from MongoDB, applies the full NLP pipeline, and
writes enriched documents back to MongoDB + analytics to PostgreSQL.

Pipeline per document:
  raw text
    → EmoteCleaner   (strip emotes, emoji, spam)
    → SentimentAnalyser (VADER by default; swap for transformer)
    → SubjectExtractor  (spaCy NER + noun chunks)
    → write enriched doc

Can also run as a batch job against historical data by swapping
readStream → read and writeStream → write.
"""
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config import settings
from nlp.emote_cleaner import make_clean_udf
from nlp.content_moderator import make_moderation_udfs
from nlp.sentiment import make_sentiment_udf
from nlp.subject_extractor import make_subject_udfs

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SocialPipeline-NLP-Enrichment")
        .config("spark.mongodb.connection.uri", settings.mongo.uri)
        .config("spark.mongodb.database", settings.mongo.database)
        # Kryo serializer speeds up UDF-heavy jobs
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Larger task memory for NLP models
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def register_udfs(spark: SparkSession):
    """Register all NLP UDFs on the SparkSession."""
    clean_udf = make_clean_udf()
    mod_text_udf, severity_udf, rejected_udf, flagged_udf = make_moderation_udfs()
    label_udf, score_udf, compound_udf = make_sentiment_udf(backend="vader")
    primary_udf, subjects_udf, topics_udf = make_subject_udfs()

    spark.udf.register("clean_text",       clean_udf)
    spark.udf.register("moderated_text",      mod_text_udf)
    spark.udf.register("content_severity",    severity_udf)
    spark.udf.register("content_rejected",    rejected_udf)
    spark.udf.register("flagged_term_count",  flagged_udf)
    spark.udf.register("sentiment_label",  label_udf)
    spark.udf.register("sentiment_score",  score_udf)
    spark.udf.register("sentiment_compound", compound_udf)
    spark.udf.register("primary_subject",  primary_udf)
    spark.udf.register("subjects",         subjects_udf)
    spark.udf.register("topics",           topics_udf)

    return {
        "clean":    clean_udf,
        "moderated": mod_text_udf,
        "severity": severity_udf,
        "rejected": rejected_udf,
        "flagged": flagged_udf,
        "label":    label_udf,
        "score":    score_udf,
        "compound": compound_udf,
        "primary":  primary_udf,
        "subjects": subjects_udf,
        "topics":   topics_udf,
    }


def enrich_dataframe(df: DataFrame, udfs: dict, text_col: str = "text") -> DataFrame:
    """
    Apply the full NLP pipeline to a DataFrame that has a text column.
    Works for all sources; pass the appropriate text_col name.

    Adds columns:
      cleaned_text, sentiment, sentiment_score, sentiment_compound,
      primary_subject, subjects, topics
    """
    return (
        df
        # Step 1: Content moderation
        .withColumn("moderated_text", udfs["moderated"](F.col(text_col)))
        .withColumn("content_severity", udfs["severity"](F.col(text_col)))
        .withColumn("content_rejected", udfs["rejected"](F.col(text_col)))
        .withColumn("flagged_term_count", udfs["flagged"](F.col(text_col)))
        .filter(~F.col("content_rejected"))
        # Step 2: Clean
        .withColumn("cleaned_text", udfs["clean"](F.col("moderated_text")))
        # Step 3: Sentiment (operate on cleaned text)
        .withColumn("sentiment",          udfs["label"](F.col("cleaned_text")))
        .withColumn("sentiment_score",    udfs["score"](F.col("cleaned_text")))
        .withColumn("sentiment_compound", udfs["compound"](F.col("cleaned_text")))
        # Step 4: Subject extraction
        .withColumn("primary_subject", udfs["primary"](F.col("cleaned_text")))
        .withColumn("subjects",        udfs["subjects"](F.col("cleaned_text")))
        .withColumn("topics",          udfs["topics"](F.col("cleaned_text")))
        # Metadata
        .withColumn("nlp_processed_at", F.current_timestamp())
    )


# ── Per-source enrichment functions ──────────────────────────────────────────

def enrich_twitch(spark: SparkSession, udfs: dict):
    """
    Reads from twitch_raw MongoDB collection, enriches stream titles + chat
    (title is the primary text for now; extend for chat once you have chat logs).
    """
    raw_df = (
        spark.readStream
        .format("mongodb")
        .option("spark.mongodb.database", settings.mongo.database)
        .option("spark.mongodb.collection", settings.mongo.twitch_collection)
        .load()
    )
    enriched = enrich_dataframe(raw_df, udfs, text_col="title")
    return _write_enriched(enriched, "twitch_enriched", "stream_id")


def enrich_youtube(spark: SparkSession, udfs: dict):
    """Enriches YouTube video titles + descriptions."""
    raw_df = (
        spark.readStream
        .format("mongodb")
        .option("spark.mongodb.database", settings.mongo.database)
        .option("spark.mongodb.collection", settings.mongo.youtube_collection)
        .load()
    )
    # Concatenate title + first 500 chars of description for richer context
    with_text = raw_df.withColumn(
        "text",
        F.concat_ws(
            ". ",
            F.col("title"),
            F.substring(F.col("description"), 1, 500),
        ),
    )
    enriched = enrich_dataframe(with_text, udfs, text_col="text")
    return _write_enriched(enriched, "youtube_enriched", "video_id")


def _write_enriched(enriched_df: DataFrame, collection: str, id_col: str):
    """Write enriched stream to MongoDB enriched collection."""
    def write_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        (
            batch_df.write
            .format("mongodb")
            .mode("append")
            .option("spark.mongodb.database", settings.mongo.database)
            .option("spark.mongodb.collection", collection)
            .save()
        )
        # Also write NLP aggregates to Postgres for the dashboard
        _write_nlp_aggregates(batch_df, batch_id)

    return (
        enriched_df.writeStream
        .foreachBatch(write_batch)
        .option(
            "checkpointLocation",
            f"{settings.spark.checkpoint_location}/nlp/{collection}",
        )
        .trigger(processingTime=settings.spark.trigger_interval)
        .queryName(f"nlp_{collection}")
        .start()
    )


def _write_nlp_aggregates(batch_df: DataFrame, batch_id: int):
    """
    Write per-minute sentiment + subject aggregates to Postgres.
    These power the trend dashboard.
    """
    pg_props = {
        "user": settings.postgres.user,
        "password": settings.postgres.password,
        "driver": settings.postgres.driver,
    }

    # Sentiment trend: (minute, source, sentiment, count, avg_compound)
    sentiment_agg = (
        batch_df
        .withColumn("minute", F.date_trunc("minute", F.col("nlp_processed_at")))
        .groupBy("minute", "source", "sentiment")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("sentiment_compound").alias("avg_compound"),
        )
        .withColumn("batch_id", F.lit(batch_id))
    )

    sentiment_agg.write.jdbc(
        url=settings.postgres.url,
        table="sentiment_trends",
        mode="append",
        properties=pg_props,
    )

    # Subject trends: (minute, source, subject, count, avg_sentiment)
    if "subjects" in batch_df.columns:
        subject_agg = (
            batch_df
            .withColumn("subject", F.explode(F.col("subjects")))
            .withColumn("minute", F.date_trunc("minute", F.col("nlp_processed_at")))
            .groupBy("minute", "source", "subject")
            .agg(
                F.count("*").alias("mention_count"),
                F.avg("sentiment_compound").alias("avg_sentiment"),
            )
            .filter(F.col("subject").isNotNull() & (F.length("subject") > 2))
        )
        subject_agg.write.jdbc(
            url=settings.postgres.url,
            table="subject_trends",
            mode="append",
            properties=pg_props,
        )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    udfs = register_udfs(spark)

    queries = [
        enrich_twitch(spark, udfs),
        enrich_youtube(spark, udfs),
    ]

    logger.info("NLP enrichment pipeline active (%d queries)", len(queries))
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Shutting down NLP enrichment pipeline…")
    finally:
        for q in queries:
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
