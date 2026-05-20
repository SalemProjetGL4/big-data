"""
Spark schemas for deserializing Kafka JSON payloads.

Each module exposes:
  - ENVELOPE_SCHEMA  — outer wrapper added by BaseProducer
  - PAYLOAD_SCHEMA   — source-specific fields inside envelope.payload
"""
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Shared envelope ──────────────────────────────────────────────────────────

def envelope(payload_schema: StructType) -> StructType:
    """Wrap a payload schema in the standard pipeline envelope."""
    return StructType(
        [
            StructField("pipeline_id", StringType(), True),
            StructField("source", StringType(), True),
            StructField("ingested_at", StringType(), True),   # ISO string → cast later
            StructField("payload", payload_schema, True),
        ]
    )


# ── YouTube ──────────────────────────────────────────────────────────────────

YOUTUBE_PAYLOAD = StructType(
    [
        StructField("video_id", StringType(), False),
        StructField("channel_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("published_at", StringType(), True),
        StructField("thumbnail_url", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("category_id", StringType(), True),
        StructField("view_count", LongType(), True),
        StructField("like_count", LongType(), True),
        StructField("comment_count", LongType(), True),
        StructField("duration", StringType(), True),
        StructField("definition", StringType(), True),
    ]
)

YOUTUBE_SCHEMA = envelope(YOUTUBE_PAYLOAD)


# ── YouTube Comments ───────────────────────────────────────────────────────

YOUTUBE_COMMENTS_PAYLOAD = StructType(
    [
        StructField("comment_id", StringType(), True),
        StructField("video_id", StringType(), True),
        StructField("thread_id", StringType(), True),
        StructField("parent_id", StringType(), True),
        StructField("author_channel_id", StringType(), True),
        StructField("author_display_name", StringType(), True),
        StructField("author_profile_image_url", StringType(), True),
        StructField("text", StringType(), True),
        StructField("like_count", LongType(), True),
        StructField("published_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("is_reply", BooleanType(), True),
    ]
)

YOUTUBE_COMMENTS_SCHEMA = envelope(YOUTUBE_COMMENTS_PAYLOAD)


# ── Twitch ───────────────────────────────────────────────────────────────────

TWITCH_PAYLOAD = StructType(
    [
        StructField("stream_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_login", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("game_id", StringType(), True),
        StructField("game_name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("viewer_count", LongType(), True),
        StructField("started_at", StringType(), True),
        StructField("language", StringType(), True),
        StructField("thumbnail_url", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("is_mature", BooleanType(), True),
        StructField("channel_description", StringType(), True),
        StructField("channel_total_views", LongType(), True),
        StructField("channel_created_at", StringType(), True),
        StructField("profile_image_url", StringType(), True),
    ]
)

TWITCH_SCHEMA = envelope(TWITCH_PAYLOAD)


# ── Twitch Chat ─────────────────────────────────────────────────────────────

TWITCH_CHAT_PAYLOAD = StructType(
    [
        StructField("message_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_login", StringType(), True),
        StructField("display_name", StringType(), True),
        StructField("message", StringType(), True),
        StructField("badges", StringType(), True),
        StructField("color", StringType(), True),
        StructField("emotes", StringType(), True),
        StructField("is_mod", BooleanType(), True),
        StructField("is_subscriber", BooleanType(), True),
        StructField("is_vip", BooleanType(), True),
        StructField("sent_at", StringType(), True),
    ]
)

TWITCH_CHAT_SCHEMA = envelope(TWITCH_CHAT_PAYLOAD)


