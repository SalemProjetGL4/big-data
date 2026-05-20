# Social Media Data Pipeline

Real-time data pipeline ingesting from YouTube, YouTube Comments, Twitch, and Twitch Chat → Kafka → Spark Streaming → MongoDB + PostgreSQL.

## Architecture

```
YouTube API ─┐
YouTube Comments ─┤
Twitch API  ─┼─► Kafka Topics ─► Spark Structured Streaming ─┬─► MongoDB     (raw payloads)
Twitch Chat ─┤
                                                     └─► PostgreSQL  (analytics)
```

## Project Structure

```
social-pipeline/
├── producers/
│   ├── base_producer.py       # Shared Kafka producer logic
│   ├── youtube_producer.py    # YouTube Data API v3 ingestion
│   ├── twitch_producer.py     # Twitch Helix API ingestion
│   ├── twitch_chat_producer.py# Twitch IRC chat ingestion
│
├── spark/
│   ├── streaming_job.py       # Main Spark Structured Streaming job
│   ├── transformers.py        # Per-source data normalization
│   └── writers.py             # MongoDB + PostgreSQL sink logic
├── schemas/
│   ├── youtube.py             # Spark schema for YouTube events
│   ├── twitch.py              # Spark schema for Twitch events
│
├── config/
│   └── settings.py            # Centralized config via env vars
├── docker/
│   └── docker-compose.yml     # Kafka, Zookeeper, MongoDB, PostgreSQL
├── scripts/
│   ├── init_postgres.sql      # Analytics table definitions
│   └── create_kafka_topics.sh # Topic creation script
└── requirements.txt
```

## Quick Start

### 1. Start infrastructure
```bash
cd docker
docker-compose up -d
```

### 2. Create Kafka topics
```bash
bash scripts/create_kafka_topics.sh
```

### 3. Initialize PostgreSQL schema
```bash
psql -h localhost -U pipeline -d analytics -f scripts/init_postgres.sql
```

### 4. Set environment variables
```bash
cp .env.example .env
# Fill in your API keys
```

### 5. Start producers
```bash
python producers/youtube_producer.py &
python producers/twitch_producer.py &
```

### 6. Submit Spark job
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,\
org.postgresql:postgresql:42.6.0 \
  spark/streaming_job.py
```

## Environment Variables

| Variable | Description |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | e.g. `localhost:9092` |
| `YOUTUBE_API_KEY` | Google Cloud API key |
| `YOUTUBE_CHANNEL_IDS` | Comma-separated channel IDs |
| `ENABLE_YOUTUBE_COMMENTS` | `true` to enable YouTube comments ingestion |
| `YOUTUBE_COMMENTS_MAX_PER_VIDEO` | 0 for all comments, or a numeric cap |
| `YOUTUBE_COMMENTS_INCLUDE_REPLIES` | `true` to include replies |
| `YOUTUBE_COMMENTS_ORDER` | `time` or `relevance` |
| `TWITCH_CLIENT_ID` | Twitch app client ID |
| `TWITCH_CLIENT_SECRET` | Twitch app client secret |
| `TWITCH_STREAMER_LOGINS` | Comma-separated streamer logins |
| `ENABLE_TWITCH_CHAT` | `true` to enable Twitch chat ingestion |
| `TWITCH_BOT_USERNAME` | Twitch bot username for IRC |
| `TWITCH_CHAT_OAUTH_TOKEN` | OAuth token for chat (format `oauth:...`) |
| `TWITCH_CHAT_CHANNELS` | Comma-separated chat channels |
| `MONGODB_URI` | e.g. `mongodb://localhost:27017` |
| `MONGODB_DATABASE` | e.g. `social_raw` |
| `POSTGRES_URL` | JDBC URL |
| `POSTGRES_USER` | DB user |
| `POSTGRES_PASSWORD` | DB password |
