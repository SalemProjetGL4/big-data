# Social Media Data Pipeline

Real-time data pipeline ingesting from YouTube, YouTube Comments, Twitch, and Twitch Chat → Kafka → Spark Streaming → MongoDB + PostgreSQL, with an optional NLP batch job that enriches MongoDB raw docs and writes trend analytics to Postgres.

## Architecture

```
YouTube API ─┐
YouTube Comments ─┤
Twitch API  ─┼─► Kafka Topics ─► Spark Structured Streaming ─┬─► MongoDB     (raw payloads)
Twitch Chat ─┤
                                                     └─► PostgreSQL  (analytics)

MongoDB (raw) ─► NLP Batch Scheduler ─► MongoDB (enriched) + PostgreSQL (trends)
```

## Project Structure

```
.
├── config/
│   └── settings.py            # Centralized config via env vars
├── docker/
│   ├── docker-compose.yml     # Full stack (Kafka, DBs, producers, Spark, NLP)
│   └── Dockerfile.nlp         # NLP batch scheduler image
├── nlp/
│   ├── emote_cleaner.py
│   ├── sentiment.py
│   └── subject_extractor.py
├── producers/
│   ├── base_producer.py
│   ├── youtube_producer.py
│   ├── twitch_producer.py
│   └── twitch_chat_producer.py
├── spark/
│   ├── streaming_job.py       # Main Spark Structured Streaming job
│   ├── transformers.py        # Per-source data normalization
│   ├── writers.py             # MongoDB + PostgreSQL sink logic
│   ├── nlp_batch_scheduler.py # NLP batch processor
│   ├── nlp_enrichment_job.py  # Optional streaming NLP job
│   └── batch_tracker.py       # NLP batch run tracking
├── schemas/
│   └── schemas.py             # Spark schemas
├── scripts/
│   ├── init_postgres.sql      # Analytics + NLP tables
│   └── create_kafka_topics.sh # Topic creation script
├── requirements.txt
└── requirements_batch.txt
```

## Quick Start

### 1. Start infrastructure (and pipeline services)
```bash
cd docker
docker compose up -d
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

### 4b. Optional: NLP dependencies for local runs
```bash
pip install -r requirements.txt -r requirements_batch.txt
python -m spacy download en_core_web_sm
```

### 5. Optional: run locally instead of Docker
```bash
python producers/youtube_producer.py &
python producers/twitch_producer.py &
```

### 6. Optional: submit Spark job manually
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
| `NLP_SCHEDULE_MODE` | `interval`, `cron`, or `manual` |
| `NLP_INTERVAL_MINUTES` | Interval in minutes when schedule mode is `interval` |
| `NLP_CRON_EXPR` | Cron expression when schedule mode is `cron` |
| `NLP_BATCH_SIZE` | Max rows per source per NLP batch |
| `NLP_SENTIMENT_BACKEND` | `vader` or `transformer` |
| `NLP_SPACY_MODEL` | spaCy model (e.g. `en_core_web_sm`) |
| `NLP_MAX_TEXT_LEN` | Max text length passed to NLP | 
