"""
Central configuration — all values pulled from environment variables.
Copy .env.example to .env and fill in your credentials before running.
"""
import os
from dataclasses import dataclass, field
from typing import List


def _csv(key: str, default: str = "") -> List[str]:
    """Parse a comma-separated env var into a list."""
    raw = os.getenv(key, default)
    return [v.strip() for v in raw.split(",") if v.strip()]


def _bool(key: str, default: bool = True) -> bool:
    """Parse a boolean env var like true/false/1/0/yes/no."""
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    youtube_topic: str = "youtube-events"
    youtube_comments_topic: str = "youtube-comments"
    twitch_topic: str = "twitch-events"
    twitch_chat_topic: str = "twitch-chat"
    # Kafka producer settings
    acks: str = "all"                  # wait for all replicas
    retries: int = 3
    retry_backoff_ms: int = 500
    compression_type: str = "lz4"     # balance CPU vs network
    linger_ms: int = 50               # small batching window


@dataclass(frozen=True)
class YouTubeConfig:
    api_key: str = field(default_factory=lambda: os.getenv("YOUTUBE_API_KEY", ""))
    channel_ids: List[str] = field(default_factory=lambda: _csv("YOUTUBE_CHANNEL_IDS"))
    poll_interval_seconds: int = int(os.getenv("YOUTUBE_POLL_INTERVAL", "60"))
    max_results_per_channel: int = int(os.getenv("YOUTUBE_MAX_RESULTS", "50"))


@dataclass(frozen=True)
class YouTubeCommentsConfig:
    max_per_video: int = int(os.getenv("YOUTUBE_COMMENTS_MAX_PER_VIDEO", "0"))
    include_replies: bool = field(
        default_factory=lambda: _bool("YOUTUBE_COMMENTS_INCLUDE_REPLIES", True)
    )
    order: str = field(
        default_factory=lambda: os.getenv("YOUTUBE_COMMENTS_ORDER", "time")
    )


@dataclass(frozen=True)
class TwitchConfig:
    client_id: str = field(default_factory=lambda: os.getenv("TWITCH_CLIENT_ID", ""))
    client_secret: str = field(
        default_factory=lambda: os.getenv("TWITCH_CLIENT_SECRET", "")
    )
    streamer_logins: List[str] = field(
        default_factory=lambda: _csv("TWITCH_STREAMER_LOGINS")
    )
    poll_interval_seconds: int = int(os.getenv("TWITCH_POLL_INTERVAL", "30"))


@dataclass(frozen=True)
class TwitchChatConfig:
    bot_username: str = field(
        default_factory=lambda: os.getenv("TWITCH_BOT_USERNAME", "")
    )
    oauth_token: str = field(
        default_factory=lambda: os.getenv("TWITCH_CHAT_OAUTH_TOKEN", "")
    )
    channels: List[str] = field(
        default_factory=lambda: _csv("TWITCH_CHAT_CHANNELS")
    )
    server: str = field(
        default_factory=lambda: os.getenv("TWITCH_CHAT_SERVER", "irc.chat.twitch.tv")
    )
    port: int = int(os.getenv("TWITCH_CHAT_PORT", "6697"))
    use_tls: bool = field(default_factory=lambda: _bool("TWITCH_CHAT_USE_TLS", True))


@dataclass(frozen=True)
class MongoConfig:
    uri: str = field(
        default_factory=lambda: os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    )
    database: str = field(
        default_factory=lambda: os.getenv("MONGODB_DATABASE", "social_raw")
    )
    youtube_collection: str = "youtube_raw"
    youtube_comments_collection: str = "youtube_comments_raw"
    twitch_collection: str = "twitch_raw"
    twitch_chat_collection: str = "twitch_chat_raw"


@dataclass(frozen=True)
class PostgresConfig:
    url: str = field(
        default_factory=lambda: os.getenv(
            "POSTGRES_URL", "jdbc:postgresql://localhost:5432/analytics"
        )
    )
    user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "pipeline"))
    password: str = field(
        default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "")
    )
    driver: str = "org.postgresql.Driver"
    # Write batch size for JDBC sink
    batch_size: int = int(os.getenv("POSTGRES_BATCH_SIZE", "1000"))


@dataclass(frozen=True)
class SparkConfig:
    app_name: str = "SocialMediaPipeline"
    checkpoint_location: str = field(
        default_factory=lambda: os.getenv(
            "SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints/social-pipeline"
        )
    )
    trigger_interval: str = os.getenv("SPARK_TRIGGER_INTERVAL", "30 seconds")
    max_offsets_per_trigger: int = int(
        os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "10000")
    )


@dataclass(frozen=True)
class FeatureFlags:
    enable_twitch_chat: bool = field(
        default_factory=lambda: _bool("ENABLE_TWITCH_CHAT", False)
    )
    enable_youtube_comments: bool = field(
        default_factory=lambda: _bool("ENABLE_YOUTUBE_COMMENTS", False)
    )


# ── Singleton instances ──────────────────────────────────────────────────────
kafka = KafkaConfig()
youtube = YouTubeConfig()
youtube_comments = YouTubeCommentsConfig()
twitch = TwitchConfig()
twitch_chat = TwitchChatConfig()
mongo = MongoConfig()
postgres = PostgresConfig()
spark = SparkConfig()
features = FeatureFlags()
