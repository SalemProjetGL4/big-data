-- PostgreSQL analytics schema
-- Run once: psql -h localhost -U pipeline -d analytics -f scripts/init_postgres.sql

-- Enable TimescaleDB extension for efficient time-series queries (optional but recommended)
-- CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ── YouTube analytics ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS youtube_analytics (
    video_id         TEXT        PRIMARY KEY,
    channel_id       TEXT        NOT NULL,
    title            TEXT,
    category_id      TEXT,
    duration         TEXT,
    published_at     TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ NOT NULL,
    processed_at     TIMESTAMPTZ,
    view_count       BIGINT      DEFAULT 0,
    like_count       BIGINT      DEFAULT 0,
    comment_count    BIGINT      DEFAULT 0,
    engagement_rate  DOUBLE PRECISION DEFAULT 0.0,
    source           TEXT        DEFAULT 'youtube'
);

CREATE INDEX IF NOT EXISTS idx_yt_channel    ON youtube_analytics (channel_id);
CREATE INDEX IF NOT EXISTS idx_yt_published  ON youtube_analytics (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_yt_ingested   ON youtube_analytics (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_yt_engagement ON youtube_analytics (engagement_rate DESC);

-- Hypertable for TimescaleDB (uncomment if using TimescaleDB)
-- SELECT create_hypertable('youtube_analytics', 'ingested_at', if_not_exists => TRUE);


-- ── Twitch analytics ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS twitch_analytics (
    stream_id            TEXT        PRIMARY KEY,
    user_id              TEXT        NOT NULL,
    user_login           TEXT        NOT NULL,
    game_id              TEXT,
    game_name            TEXT,
    title                TEXT,
    language             TEXT,
    is_live              BOOLEAN     DEFAULT TRUE,
    is_mature            BOOLEAN     DEFAULT FALSE,
    viewer_count         BIGINT      DEFAULT 0,
    channel_total_views  BIGINT      DEFAULT 0,
    started_at           TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL,
    processed_at         TIMESTAMPTZ,
    source               TEXT        DEFAULT 'twitch'
);

CREATE INDEX IF NOT EXISTS idx_tw_user      ON twitch_analytics (user_login);
CREATE INDEX IF NOT EXISTS idx_tw_game      ON twitch_analytics (game_name);
CREATE INDEX IF NOT EXISTS idx_tw_started   ON twitch_analytics (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_tw_viewers   ON twitch_analytics (viewer_count DESC);
CREATE INDEX IF NOT EXISTS idx_tw_ingested  ON twitch_analytics (ingested_at DESC);

-- SELECT create_hypertable('twitch_analytics', 'ingested_at', if_not_exists => TRUE);


-- ── Twitch chat analytics ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS twitch_chat_analytics (
    message_id       TEXT        PRIMARY KEY,
    channel          TEXT        NOT NULL,
    user_id          TEXT,
    user_login       TEXT,
    display_name     TEXT,
    message          TEXT,
    message_length   INTEGER     DEFAULT 0,
    badges           TEXT,
    color            TEXT,
    emotes           TEXT,
    is_mod           BOOLEAN     DEFAULT FALSE,
    is_subscriber    BOOLEAN     DEFAULT FALSE,
    is_vip           BOOLEAN     DEFAULT FALSE,
    sent_at          TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ NOT NULL,
    processed_at     TIMESTAMPTZ,
    source           TEXT        DEFAULT 'twitch_chat'
);

CREATE INDEX IF NOT EXISTS idx_tc_channel   ON twitch_chat_analytics (channel);
CREATE INDEX IF NOT EXISTS idx_tc_user      ON twitch_chat_analytics (user_login);
CREATE INDEX IF NOT EXISTS idx_tc_sent_at   ON twitch_chat_analytics (sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_tc_ingested  ON twitch_chat_analytics (ingested_at DESC);


-- ── YouTube comments analytics ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS youtube_comments_analytics (
    comment_id         TEXT        PRIMARY KEY,
    video_id           TEXT        NOT NULL,
    thread_id          TEXT,
    parent_id          TEXT,
    author_channel_id  TEXT,
    author_display_name TEXT,
    text               TEXT,
    like_count         BIGINT      DEFAULT 0,
    published_at       TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ,
    ingested_at        TIMESTAMPTZ NOT NULL,
    processed_at       TIMESTAMPTZ,
    is_reply           BOOLEAN     DEFAULT FALSE,
    source             TEXT        DEFAULT 'youtube_comments'
);

CREATE INDEX IF NOT EXISTS idx_yc_video     ON youtube_comments_analytics (video_id);
CREATE INDEX IF NOT EXISTS idx_yc_thread    ON youtube_comments_analytics (thread_id);
CREATE INDEX IF NOT EXISTS idx_yc_parent    ON youtube_comments_analytics (parent_id);
CREATE INDEX IF NOT EXISTS idx_yc_published ON youtube_comments_analytics (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_yc_ingested  ON youtube_comments_analytics (ingested_at DESC);


-- ── Convenience views ────────────────────────────────────────────────────────

-- Cross-platform top content in the last 24 hours
CREATE OR REPLACE VIEW top_content_24h AS
    SELECT
        'youtube'        AS platform,
        video_id         AS content_id,
        title,
        channel_id       AS creator,
        view_count       AS reach,
        engagement_rate  AS engagement,
        ingested_at
    FROM youtube_analytics
    WHERE ingested_at > NOW() - INTERVAL '24 hours'

    UNION ALL

    SELECT
        'twitch'         AS platform,
        stream_id        AS content_id,
        title,
        user_login       AS creator,
        viewer_count     AS reach,
        NULL             AS engagement,
        ingested_at
    FROM twitch_analytics
    WHERE ingested_at > NOW() - INTERVAL '24 hours'
      AND is_live = TRUE

    ORDER BY reach DESC NULLS LAST;


-- ── NLP analytics ───────────────────────────────────────────────────────────

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

-- Sentiment trends (per minute, per source)
CREATE TABLE IF NOT EXISTS sentiment_trends (
    id            BIGSERIAL   PRIMARY KEY,
    minute        TIMESTAMPTZ NOT NULL,
    source        TEXT        NOT NULL,   -- youtube | twitch
    sentiment     TEXT        NOT NULL,   -- positive | negative | neutral
    event_count   BIGINT      DEFAULT 0,
    avg_compound  DOUBLE PRECISION DEFAULT 0.0,
    batch_id      INTEGER,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_st_minute  ON sentiment_trends (minute DESC);
CREATE INDEX IF NOT EXISTS idx_st_source  ON sentiment_trends (source);
CREATE INDEX IF NOT EXISTS idx_st_sent    ON sentiment_trends (sentiment);
CREATE UNIQUE INDEX IF NOT EXISTS idx_st_unique
    ON sentiment_trends (minute, source, sentiment);

-- Subject/topic trends (per minute, per source)
CREATE TABLE IF NOT EXISTS subject_trends (
    id             BIGSERIAL   PRIMARY KEY,
    minute         TIMESTAMPTZ NOT NULL,
    source         TEXT        NOT NULL,
    subject        TEXT        NOT NULL,
    mention_count  BIGINT      DEFAULT 0,
    avg_sentiment  DOUBLE PRECISION DEFAULT 0.0,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sut_minute  ON subject_trends (minute DESC);
CREATE INDEX IF NOT EXISTS idx_sut_source  ON subject_trends (source);
CREATE INDEX IF NOT EXISTS idx_sut_subject ON subject_trends (subject);
CREATE INDEX IF NOT EXISTS idx_sut_count   ON subject_trends (mention_count DESC);

-- Materialised view: top subjects last 1 hour
CREATE MATERIALIZED VIEW IF NOT EXISTS top_subjects_1h AS
SELECT
    source,
    subject,
    SUM(mention_count)              AS total_mentions,
    AVG(avg_sentiment)              AS avg_sentiment,
    COUNT(DISTINCT minute)          AS active_minutes
FROM subject_trends
WHERE minute > NOW() - INTERVAL '1 hour'
GROUP BY source, subject
ORDER BY total_mentions DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_top_subjects_1h
    ON top_subjects_1h (source, subject);

-- Refresh this view every minute via pg_cron or from the app:
-- SELECT cron.schedule('refresh-top-subjects', '* * * * *',
--   'REFRESH MATERIALIZED VIEW CONCURRENTLY top_subjects_1h');

-- Materialised view: sentiment summary last 24 hours
CREATE MATERIALIZED VIEW IF NOT EXISTS sentiment_summary_24h AS
SELECT
    source,
    sentiment,
    SUM(event_count)       AS total_events,
    AVG(avg_compound)      AS avg_compound,
    DATE_TRUNC('hour', minute) AS hour
FROM sentiment_trends
WHERE minute > NOW() - INTERVAL '24 hours'
GROUP BY source, sentiment, DATE_TRUNC('hour', minute)
ORDER BY hour DESC;
