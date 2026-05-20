"""
Main Spark Structured Streaming job.

Reads from Kafka topics in parallel, transforms each stream,
and writes to MongoDB (raw) + PostgreSQL (analytics) via foreachBatch.

Submit with:
  spark-submit \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\\
               org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,\\
               org.postgresql:postgresql:42.6.0 \\
    spark/streaming_job.py
"""
import logging
import os
import sys

# Allow imports from project root when running via spark-submit
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession

from config import settings
from spark.transformers import (
    transform_twitch,
    transform_twitch_chat,
    transform_youtube_comments,
    transform_youtube,
)
from spark.writers import make_mongo_writer, make_postgres_writer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    spark_cfg = settings.spark
    mongo_cfg = settings.mongo

    return (
        SparkSession.builder
        .appName(spark_cfg.app_name)
        # MongoDB global settings
        .config("spark.mongodb.connection.uri", mongo_cfg.uri)
        .config("spark.mongodb.database", mongo_cfg.database)
        # Kafka consumer tuning
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, topic: str):
    """Return a raw Kafka streaming DataFrame for a single topic."""
    kafka_cfg = settings.kafka
    spark_cfg = settings.spark
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_cfg.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", spark_cfg.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")   # continue on offset gaps
        .load()
    )


def build_youtube_stream(spark: SparkSession):
    kafka_cfg = settings.kafka
    mongo_cfg = settings.mongo

    kafka_df = read_kafka_stream(spark, kafka_cfg.youtube_topic)
    raw_df, analytics_df = transform_youtube(kafka_df)

    mongo_query = (
        raw_df.writeStream
        .foreachBatch(make_mongo_writer(mongo_cfg.youtube_collection))
        .option(
            "checkpointLocation",
            f"{settings.spark.checkpoint_location}/youtube/mongo",
        )
        .trigger(processingTime=settings.spark.trigger_interval)
        .queryName("youtube_mongo")
        .start()
    )

    pg_query = (
        analytics_df.writeStream
        .foreachBatch(make_postgres_writer("youtube_analytics", "video_id"))
        .option(
            "checkpointLocation",
            f"{settings.spark.checkpoint_location}/youtube/postgres",
        )
        .trigger(processingTime=settings.spark.trigger_interval)
        .queryName("youtube_postgres")
        .start()
    )

    return mongo_query, pg_query


def build_twitch_stream(spark: SparkSession):
    kafka_cfg = settings.kafka
    mongo_cfg = settings.mongo

    kafka_df = read_kafka_stream(spark, kafka_cfg.twitch_topic)
    raw_df, analytics_df = transform_twitch(kafka_df)

    mongo_query = (
        raw_df.writeStream
        .foreachBatch(make_mongo_writer(mongo_cfg.twitch_collection))
        .option(
            "checkpointLocation",
            f"{settings.spark.checkpoint_location}/twitch/mongo",
        )
        .trigger(processingTime=settings.spark.trigger_interval)
        .queryName("twitch_mongo")
        .start()
    )

    pg_query = (
        analytics_df.writeStream
        .foreachBatch(make_postgres_writer("twitch_analytics", "stream_id"))
        .option(
            "checkpointLocation",
            f"{settings.spark.checkpoint_location}/twitch/postgres",
        )
        .trigger(processingTime=settings.spark.trigger_interval)
        .queryName("twitch_postgres")
        .start()
    )

    return mongo_query, pg_query


def build_youtube_comments_stream(spark: SparkSession):
    kafka_cfg = settings.kafka
    mongo_cfg = settings.mongo

    kafka_df = read_kafka_stream(spark, kafka_cfg.youtube_comments_topic)
    raw_df, analytics_df = transform_youtube_comments(kafka_df)

    mongo_query = (
        raw_df.writeStream
        .foreachBatch(make_mongo_writer(mongo_cfg.youtube_comments_collection))
        .option(
            "checkpointLocation",
            f"{settings.spark.checkpoint_location}/youtube_comments/mongo",
        )
        .trigger(processingTime=settings.spark.trigger_interval)
        .queryName("youtube_comments_mongo")
        .start()
    )

    pg_query = (
        analytics_df.writeStream
        .foreachBatch(make_postgres_writer("youtube_comments_analytics", "comment_id"))
        .option(
            "checkpointLocation",
            f"{settings.spark.checkpoint_location}/youtube_comments/postgres",
        )
        .trigger(processingTime=settings.spark.trigger_interval)
        .queryName("youtube_comments_postgres")
        .start()
    )

    return mongo_query, pg_query


def build_twitch_chat_stream(spark: SparkSession):
    kafka_cfg = settings.kafka
    mongo_cfg = settings.mongo

    kafka_df = read_kafka_stream(spark, kafka_cfg.twitch_chat_topic)
    raw_df, analytics_df = transform_twitch_chat(kafka_df)

    mongo_query = (
        raw_df.writeStream
        .foreachBatch(make_mongo_writer(mongo_cfg.twitch_chat_collection))
        .option(
            "checkpointLocation",
            f"{settings.spark.checkpoint_location}/twitch_chat/mongo",
        )
        .trigger(processingTime=settings.spark.trigger_interval)
        .queryName("twitch_chat_mongo")
        .start()
    )

    pg_query = (
        analytics_df.writeStream
        .foreachBatch(make_postgres_writer("twitch_chat_analytics", "message_id"))
        .option(
            "checkpointLocation",
            f"{settings.spark.checkpoint_location}/twitch_chat/postgres",
        )
        .trigger(processingTime=settings.spark.trigger_interval)
        .queryName("twitch_chat_postgres")
        .start()
    )

    return mongo_query, pg_query


def main():
    logger.info("Starting Social Media Pipeline — Spark Structured Streaming")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Launch streaming queries for enabled sources (2 per source)
    queries = []
    queries.extend(build_youtube_stream(spark))
    if settings.features.enable_youtube_comments:
        queries.extend(build_youtube_comments_stream(spark))
    else:
        logger.info("YouTube comments disabled (ENABLE_YOUTUBE_COMMENTS=false)")
    queries.extend(build_twitch_stream(spark))
    if settings.features.enable_twitch_chat:
        queries.extend(build_twitch_chat_stream(spark))
    else:
        logger.info("Twitch chat stream disabled (ENABLE_TWITCH_CHAT=false)")

    logger.info("All %d streaming queries active", len(queries))
    for q in queries:
        logger.info("  Query: %s | status: %s", q.name, q.status)

    # Block until all queries terminate (or one fails)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Interrupt received — stopping all queries…")
    finally:
        for q in queries:
            q.stop()
        spark.stop()
        logger.info("Pipeline shut down cleanly.")


if __name__ == "__main__":
    main()
