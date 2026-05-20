"""
Transformers — parse raw Kafka JSON bytes into typed Spark DataFrames
and apply per-source normalization before writing to sinks.

Each transformer returns two DataFrames:
  - raw_df    : enriched payload for MongoDB (full fidelity)
  - analytics_df : flattened, typed rows for PostgreSQL
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType

from schemas.schemas import (
    TWITCH_CHAT_SCHEMA,
    TWITCH_SCHEMA,
    YOUTUBE_COMMENTS_SCHEMA,
    YOUTUBE_SCHEMA,
)


def _parse_kafka(kafka_df: DataFrame, schema) -> DataFrame:
    """
    Deserialize Kafka value bytes → typed DataFrame using the given schema.
    Adds `ingested_at` as a proper timestamp column.
    """
    return (
        kafka_df
        .select(
            F.from_json(F.col("value").cast("string"), schema).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "data.*",
            "kafka_timestamp",
        )
        .withColumn(
            "ingested_at",
            F.to_timestamp(F.col("ingested_at")),
        )
    )


# ── YouTube ──────────────────────────────────────────────────────────────────

def transform_youtube(kafka_df: DataFrame):
    """
    Returns (raw_df, analytics_df) for YouTube events.

    analytics_df columns:
      video_id, channel_id, title, published_at, view_count,
      like_count, comment_count, ingested_at, engagement_rate
    """
    parsed = _parse_kafka(kafka_df, YOUTUBE_SCHEMA)

    raw_df = (
        parsed
        .select(
            F.col("pipeline_id"),
            F.col("ingested_at"),
            F.col("kafka_timestamp"),
            F.col("payload.*"),
        )
        .withColumn(
            "published_at",
            F.to_timestamp(F.col("published_at")),
        )
    )

    analytics_df = (
        raw_df
        .select(
            "video_id",
            "channel_id",
            "title",
            "published_at",
            "ingested_at",
            F.col("view_count").cast(LongType()),
            F.col("like_count").cast(LongType()),
            F.col("comment_count").cast(LongType()),
            "duration",
            "category_id",
        )
        .withColumn(
            "engagement_rate",
            F.when(
                F.col("view_count") > 0,
                (F.col("like_count") + F.col("comment_count")) / F.col("view_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("source", F.lit("youtube"))
    )

    return raw_df, analytics_df


# ── YouTube Comments ───────────────────────────────────────────────────────

def transform_youtube_comments(kafka_df: DataFrame):
    """
    Returns (raw_df, analytics_df) for YouTube comment events.

    analytics_df columns:
      comment_id, video_id, thread_id, parent_id, author_display_name,
      like_count, published_at, updated_at, ingested_at, is_reply
    """
    parsed = _parse_kafka(kafka_df, YOUTUBE_COMMENTS_SCHEMA)

    raw_df = (
        parsed
        .select(
            F.col("pipeline_id"),
            F.col("ingested_at"),
            F.col("kafka_timestamp"),
            F.col("payload.*"),
        )
        .withColumn("published_at", F.to_timestamp(F.col("published_at")))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
    )

    analytics_df = (
        raw_df
        .select(
            "comment_id",
            "video_id",
            "thread_id",
            "parent_id",
            "author_channel_id",
            "author_display_name",
            "text",
            F.col("like_count").cast(LongType()),
            "published_at",
            "updated_at",
            "ingested_at",
            "is_reply",
        )
        .withColumn("source", F.lit("youtube_comments"))
    )

    return raw_df, analytics_df


# ── Twitch ───────────────────────────────────────────────────────────────────

def transform_twitch(kafka_df: DataFrame):
    """
    Returns (raw_df, analytics_df) for Twitch events.

    analytics_df columns:
      stream_id, user_login, game_name, viewer_count, started_at,
      ingested_at, language, is_live
    """
    parsed = _parse_kafka(kafka_df, TWITCH_SCHEMA)

    raw_df = (
        parsed
        .select(
            F.col("pipeline_id"),
            F.col("ingested_at"),
            F.col("kafka_timestamp"),
            F.col("payload.*"),
        )
        .withColumn(
            "started_at",
            F.to_timestamp(F.col("started_at")),
        )
    )

    analytics_df = (
        raw_df
        .withColumn(
            "is_live",
            F.col("type") == F.lit("live"),
        )
        .select(
            "stream_id",
            "user_id",
            "user_login",
            "game_id",
            "game_name",
            "title",
            F.col("viewer_count").cast(LongType()),
            "started_at",
            "ingested_at",
            "language",
            "is_mature",
            F.col("channel_total_views").cast(LongType()),
            "is_live",
        )
        .withColumn("source", F.lit("twitch"))
    )

    return raw_df, analytics_df


# ── Twitch Chat ─────────────────────────────────────────────────────────────

def transform_twitch_chat(kafka_df: DataFrame):
    """
    Returns (raw_df, analytics_df) for Twitch chat events.

    analytics_df columns:
      message_id, channel, user_login, display_name, message,
      message_length, sent_at, ingested_at
    """
    parsed = _parse_kafka(kafka_df, TWITCH_CHAT_SCHEMA)

    raw_df = (
        parsed
        .select(
            F.col("pipeline_id"),
            F.col("ingested_at"),
            F.col("kafka_timestamp"),
            F.col("payload.*"),
        )
        .withColumn(
            "sent_at",
            F.to_timestamp(F.col("sent_at")),
        )
    )

    analytics_df = (
        raw_df
        .select(
            "message_id",
            "channel",
            "user_id",
            "user_login",
            "display_name",
            "message",
            F.length("message").cast(IntegerType()).alias("message_length"),
            "sent_at",
            "ingested_at",
            "is_mod",
            "is_subscriber",
            "is_vip",
        )
        .withColumn("source", F.lit("twitch_chat"))
    )

    return raw_df, analytics_df
