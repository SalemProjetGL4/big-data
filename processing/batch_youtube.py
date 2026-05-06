# =============================================================
#  processing/batch_youtube.py
#  Spark Batch Job — YouTube Trending Videos (113 countries)
#  Reads: /data/youtube/trending/*.csv  (HDFS, ~6.6 GB)
#  Writes: PostgreSQL → trending_topics, sentiment_summary,
#                        engagement_metrics, cross_platform_trends
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, sum, count, round, lit, desc,
    lower, array
)
from pyspark.sql.types import LongType

# ── Config ────────────────────────────────────────────────────
HDFS_MASTER   = "hdfs://hadoop-master:9000"
PG_URL        = "jdbc:postgresql://host.docker.internal:5432/trends_db"
PG_PROPERTIES = {
    "user":     "postgres",
    "password": "password",
    "driver":   "org.postgresql.Driver"
}

CATEGORY_MAP = {
    "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music",
    "15": "Pets & Animals", "17": "Sports", "19": "Travel & Events",
    "20": "Gaming", "22": "People & Blogs", "23": "Comedy",
    "24": "Entertainment", "25": "News & Politics", "26": "Howto & Style",
    "27": "Education", "28": "Science & Technology", "29": "Nonprofits"
}

def create_spark():
    return (SparkSession.builder
        .appName("YouTubeTrendingBatch")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate())

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("\n🚀  YouTube Trending Batch Job started (6.6 GB file)\n")

    # ── 1. Load CSV ───────────────────────────────────────────
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(f"{HDFS_MASTER}/data/youtube/trending/trending_yt_videos_113_countries.csv"))

    total = df.count()
    print(f"  ✅  Loaded YouTube CSV → {total:,} rows")
    df.printSchema()

    # ── 2. Clean & cast ───────────────────────────────────────
    for c_name in df.columns:
        clean = c_name.strip().lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(c_name, clean)

    # Detect column names that vary across dataset versions
    like_col     = next((c for c in df.columns if c in ["likes", "like_count"]), None)
    category_col = next((c for c in df.columns if c in ["category_id", "categoryid", "kind"]), None)

    numeric_cols = ["view_count", "comment_count"]
    if like_col:
        numeric_cols.append(like_col)
    for nc in numeric_cols:
        if nc in df.columns:
            df = df.withColumn(nc, col(nc).cast(LongType()))

    df = df.filter(col("title").isNotNull())

    # ── 3. Top trending videos by view count ──────────────────
    like_agg = max(like_col).alias("max_likes") if like_col else lit(0).alias("max_likes")
    top_videos = (df
        .groupBy("title", "country")
        .agg(
            max("view_count").alias("max_views"),
            like_agg,
            max("comment_count").alias("max_comments"),
            count("*").alias("countries_trending")
        )
        .orderBy(desc("max_views"))
        .limit(500)
    )

    # ── 4. Write → trending_topics ────────────────────────────
    trending = (top_videos.select(
        col("title").alias("keyword"),
        lit(1).cast("short").alias("platform_id"),
        col("max_views").cast("int").alias("count"),
        lit("2020-01-01 00:00:00").cast("timestamp").alias("window_start"),
        lit("2024-12-31 23:59:59").cast("timestamp").alias("window_end"),
        lit(True).alias("is_batch")
    ))

    (trending.write
        .mode("append")
        .jdbc(PG_URL, "trending_topics", properties=PG_PROPERTIES))
    print(f"  ✅  Written {trending.count()} rows → trending_topics")

    # ── 5. Top categories ─────────────────────────────────────
    if category_col:
        like_avg = round(avg(like_col), 0).alias("avg_likes") if like_col else lit(0).alias("avg_likes")
        category_agg = (df
            .groupBy(category_col)
            .agg(
                count("*").alias("video_count"),
                round(avg("view_count"), 0).alias("avg_views"),
                like_avg,
                round(avg("comment_count"), 0).alias("avg_comments")
            )
            .orderBy(desc("video_count"))
        )

        cat_trending = (category_agg.select(
            col(category_col).alias("keyword"),
            lit(1).cast("short").alias("platform_id"),
            col("video_count").cast("int").alias("count"),
            lit("2020-01-01 00:00:00").cast("timestamp").alias("window_start"),
            lit("2024-12-31 23:59:59").cast("timestamp").alias("window_end"),
            lit(True).alias("is_batch")
        ))

        (cat_trending.write
            .mode("append")
            .jdbc(PG_URL, "trending_topics", properties=PG_PROPERTIES))
        print(f"  ✅  Written {cat_trending.count()} category rows → trending_topics")

    # ── 6. Engagement metrics ─────────────────────────────────
    engagement = (top_videos.select(
        lit(1).cast("short").alias("platform_id"),
        col("title").alias("content_id"),
        col("title").alias("title"),
        lit("video").alias("category"),
        lit("2020-01-01 00:00:00").cast("timestamp").alias("published_at"),
        lit("2024-12-31 23:59:59").cast("timestamp").alias("snapshot_at"),
        col("max_views").cast("bigint").alias("views"),
        col("max_likes").cast("int").alias("likes"),
        col("max_comments").cast("int").alias("comments"),
        lit(True).alias("is_batch")
    ))

    pythonengagement = engagement.dropDuplicates(["platform_id", "content_id", "snapshot_at"])

    (pythonengagement.write
        .mode("append")
        .jdbc(PG_URL, "engagement_metrics", properties=PG_PROPERTIES))
    print(f"  ✅  Written {pythonengagement.count()} rows → engagement_metrics")

    # ── 7. Sentiment proxy (title-based keywords) ─────────────
    positive_words = ["amazing", "awesome", "best", "love", "great",
                      "funny", "incredible", "beautiful", "perfect", "top"]
    negative_words = ["worst", "fail", "bad", "hate", "terrible",
                      "awful", "boring", "stupid", "waste", "disappointing"]

    pos_pattern = "|".join(positive_words)
    neg_pattern = "|".join(negative_words)

    sentiment_df = (df
        .withColumn("title_lower", lower(col("title")))
        .withColumn("is_positive", col("title_lower").rlike(pos_pattern).cast("int"))
        .withColumn("is_negative", col("title_lower").rlike(neg_pattern).cast("int"))
    )

    sentiment_agg = sentiment_df.agg(
        count("*").alias("total"),
        round(avg("is_positive") * 100, 2).alias("positive_pct"),
        round(avg("is_negative") * 100, 2).alias("negative_pct"),
        round((1 - avg("is_positive") - avg("is_negative")) * 100, 2).alias("neutral_pct")
    ).collect()[0]

    sentiment_row = (spark.createDataFrame([{
        "platform_id":  1,
        "time_bucket":  "2024-01-01 00:00:00",
        "bucket_size":  "batch",
        "positive_pct": float(sentiment_agg["positive_pct"]),
        "negative_pct": float(sentiment_agg["negative_pct"]),
        "neutral_pct":  float(sentiment_agg["neutral_pct"]),
        "sample_count": int(sentiment_agg["total"]),
        "is_batch":     True
    }]).withColumn("time_bucket", col("time_bucket").cast("timestamp")))

    (sentiment_row.write
        .mode("append")
        .jdbc(PG_URL, "sentiment_summary", properties=PG_PROPERTIES))
    print(f"  ✅  Written sentiment → sentiment_summary")
    print(f"      positive: {sentiment_agg['positive_pct']}% | "
          f"negative: {sentiment_agg['negative_pct']}%")

    # ── 8. Category-level breakdown → cross_platform_trends ───
    if category_col:
        cross_agg = (df
            .groupBy(category_col)
            .agg(
                count("*").alias("video_count"),
                round(avg("view_count"), 0).alias("avg_views")
            )
            .orderBy(desc("video_count"))
            .limit(300)
        )

        cross = (cross_agg.select(
            col(category_col).alias("keyword"),
            lit("2020-01-01 00:00:00").cast("timestamp").alias("window_start"),
            lit("2024-12-31 23:59:59").cast("timestamp").alias("window_end"),
            array(lit(1).cast("short")).alias("platforms_present"),
            lit(1).cast("short").alias("platform_count"),
            col("video_count").cast("int").alias("total_count"),
            lit(True).alias("is_batch")
        ))

        (cross.write
            .mode("append")
            .jdbc(PG_URL, "cross_platform_trends", properties=PG_PROPERTIES))
        print(f"  ✅  Written {cross.count()} rows → cross_platform_trends")

    print("\n✅  YouTube Batch Job complete!\n")
    spark.stop()

if __name__ == "__main__":
    main()
