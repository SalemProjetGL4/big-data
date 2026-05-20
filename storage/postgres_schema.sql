-- =============================================================
--  EXTENSIONS
-- =============================================================
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- fuzzy keyword search


-- =============================================================
--  1. PLATFORMS lookup
--     Single source of truth for platform names
-- =============================================================
CREATE TABLE IF NOT EXISTS platforms (
    id          SMALLINT    PRIMARY KEY,
    name        VARCHAR(20) NOT NULL UNIQUE   -- 'youtube' | 'twitch' | 'twitter' | 'reddit'
);

INSERT INTO platforms VALUES
    (1, 'youtube'),
    (2, 'twitch'),
    (3, 'twitter'),
    (4, 'reddit')
ON CONFLICT DO NOTHING;


-- =============================================================
--  2. TRENDING_TOPICS
--     One row per keyword × platform × time window
--     Populated by Spark Structured Streaming (real-time)
--     and Spark batch jobs (historical)
-- =============================================================
CREATE TABLE IF NOT EXISTS trending_topics (
    id              BIGSERIAL       PRIMARY KEY,
    keyword         VARCHAR(120)    NOT NULL,
    platform_id     SMALLINT        NOT NULL REFERENCES platforms(id),
    count           INTEGER         NOT NULL DEFAULT 0,   -- occurrences in window
    window_start    TIMESTAMPTZ     NOT NULL,
    window_end      TIMESTAMPTZ     NOT NULL,
    is_batch        BOOLEAN         NOT NULL DEFAULT FALSE, -- TRUE = from Kaggle batch
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tt_keyword
    ON trending_topics USING gin (keyword gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_tt_platform_window
    ON trending_topics (platform_id, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_tt_window_start
    ON trending_topics (window_start DESC);


-- =============================================================
--  3. SENTIMENT_SUMMARY
--     Aggregated sentiment scores per platform per time bucket
--     Populated by Spark after VADER scoring
-- =============================================================
CREATE TABLE IF NOT EXISTS sentiment_summary (
    id              BIGSERIAL       PRIMARY KEY,
    platform_id     SMALLINT        NOT NULL REFERENCES platforms(id),
    time_bucket     TIMESTAMPTZ     NOT NULL,   -- truncated to the hour
    bucket_size     VARCHAR(10)     NOT NULL DEFAULT '1h',
    positive_pct    NUMERIC(5,2)    NOT NULL DEFAULT 0,
    negative_pct    NUMERIC(5,2)    NOT NULL DEFAULT 0,
    neutral_pct     NUMERIC(5,2)    NOT NULL DEFAULT 0,
    sample_count    INTEGER         NOT NULL DEFAULT 0,   -- records in this bucket
    avg_compound    NUMERIC(6,4),                         -- VADER compound mean
    is_batch        BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_pct_sum CHECK (
        ABS((positive_pct + negative_pct + neutral_pct) - 100) < 1
    )
);

CREATE INDEX IF NOT EXISTS idx_ss_platform_bucket
    ON sentiment_summary (platform_id, time_bucket DESC);


-- =============================================================
--  4. ENGAGEMENT_METRICS
--     Per-content item engagement stats (one row per video /
--     stream / post after Spark batch processing)
-- =============================================================
CREATE TABLE IF NOT EXISTS engagement_metrics (
    id                  BIGSERIAL       PRIMARY KEY,
    platform_id         SMALLINT        NOT NULL REFERENCES platforms(id),
    content_id          VARCHAR(120)    NOT NULL,   -- original platform ID
    title               TEXT,
    category            VARCHAR(80),
    published_at        TIMESTAMPTZ,
    snapshot_at         TIMESTAMPTZ     NOT NULL,   -- when the metric was captured

    -- platform-agnostic normalized fields
    views               BIGINT          DEFAULT 0,
    likes               INTEGER         DEFAULT 0,
    comments            INTEGER         DEFAULT 0,
    shares              INTEGER         DEFAULT 0,

    -- platform-specific extras (nullable)
    upvote_ratio        NUMERIC(4,3),               -- Reddit
    peak_viewers        INTEGER,                    -- Twitch
    view_velocity       NUMERIC(12,2),              -- YouTube views/hour since publish
    retweet_count       INTEGER,                    -- Twitter

    -- composite engagement index (computed by Spark, 0–1 scale)
    engagement_index    NUMERIC(6,4),

    is_batch            BOOLEAN         NOT NULL DEFAULT TRUE,

    CONSTRAINT uq_content_snapshot UNIQUE (platform_id, content_id, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_em_platform_published
    ON engagement_metrics (platform_id, published_at DESC);

CREATE INDEX IF NOT EXISTS idx_em_engagement
    ON engagement_metrics (engagement_index DESC NULLS LAST);


-- =============================================================
--  5. CROSS_PLATFORM_TRENDS
--     Keywords detected trending on 2+ platforms in the same
--     time window — computed by Spark after each batch/stream
-- =============================================================
CREATE TABLE IF NOT EXISTS cross_platform_trends (
    id                  BIGSERIAL       PRIMARY KEY,
    keyword             VARCHAR(120)    NOT NULL,
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    platforms_present   SMALLINT[]      NOT NULL,   -- array of platform_ids
    platform_count      SMALLINT        NOT NULL,   -- length of above array
    total_count         INTEGER         NOT NULL DEFAULT 0,
    confidence_score    NUMERIC(5,4),               -- 0–1 based on engagement velocity
    is_batch            BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cpt_keyword_window
    ON cross_platform_trends (keyword, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_cpt_confidence
    ON cross_platform_trends (confidence_score DESC NULLS LAST);


-- =============================================================
--  6. SPARK_JOB_LOG
--     Tracks every batch/streaming job run for debugging
--     and re-run detection
-- =============================================================
CREATE TABLE IF NOT EXISTS spark_job_log (
    id              BIGSERIAL       PRIMARY KEY,
    job_name        VARCHAR(80)     NOT NULL,
    platform        VARCHAR(20),
    status          VARCHAR(10)     NOT NULL DEFAULT 'running',  -- running|done|failed
    rows_written    INTEGER,
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    error_message   TEXT,

    CONSTRAINT chk_status CHECK (status IN ('running', 'done', 'failed'))
);


-- =============================================================
--  VIEWS — convenience queries used by the dashboard
-- =============================================================

-- Top trending keywords in the last 24 hours (real-time)
CREATE OR REPLACE VIEW v_top_trends_24h AS
SELECT
    tt.keyword,
    p.name              AS platform,
    SUM(tt.count)       AS total_count,
    MIN(tt.window_start) AS first_seen,
    MAX(tt.window_end)   AS last_seen
FROM trending_topics tt
JOIN platforms p ON p.id = tt.platform_id
WHERE tt.window_start >= NOW() - INTERVAL '24 hours'
  AND tt.is_batch = FALSE
GROUP BY tt.keyword, p.name
ORDER BY total_count DESC;


-- Sentiment trend per platform over last 7 days (hourly buckets)
CREATE OR REPLACE VIEW v_sentiment_7d AS
SELECT
    p.name          AS platform,
    ss.time_bucket,
    ss.positive_pct,
    ss.negative_pct,
    ss.neutral_pct,
    ss.avg_compound,
    ss.sample_count
FROM sentiment_summary ss
JOIN platforms p ON p.id = ss.platform_id
WHERE ss.time_bucket >= NOW() - INTERVAL '7 days'
ORDER BY ss.time_bucket DESC;


-- Cross-platform trends with platform names expanded
CREATE OR REPLACE VIEW v_cross_platform AS
SELECT
    cpt.keyword,
    cpt.window_start,
    cpt.window_end,
    cpt.platform_count,
    cpt.total_count,
    cpt.confidence_score,
    ARRAY(
        SELECT name FROM platforms
        WHERE id = ANY(cpt.platforms_present)
        ORDER BY name
    ) AS platform_names
FROM cross_platform_trends cpt
ORDER BY cpt.confidence_score DESC NULLS LAST;