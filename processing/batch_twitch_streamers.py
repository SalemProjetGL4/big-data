# =============================================================
#  processing/batch_twitch_streamers.py
#  Spark Batch Job — Twitch Streamers + Chat
#  Reads: /data/twitch/top_streamers/datasetV2.csv  (HDFS)
#         /data/twitch/chat/*.parquet                (HDFS)
#  Writes: PostgreSQL → trending_topics, sentiment_summary
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, count, round, lit, desc,
    when, lower, length, trim
)
from datetime import datetime

# ── Config ────────────────────────────────────────────────────
HDFS_MASTER   = "hdfs://hadoop-master:9000"
PG_URL        = "jdbc:postgresql://host.docker.internal:5432/trends_db"
PG_PROPERTIES = {
    "user":     "postgres",
    "password": "password",
    "driver":   "org.postgresql.Driver"
}

def create_spark():
    return (SparkSession.builder
        .appName("TwitchStreamersBatch")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("\n🚀  Twitch Streamers & Chat Batch Job started\n")

    # ── 1. Load streamers CSV ─────────────────────────────────
    streamers_df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("escape", '"')
        .csv(f"{HDFS_MASTER}/data/twitch/top_streamers/datasetV2.csv"))

    print(f"  ✅  Loaded streamers CSV → {streamers_df.count():,} rows")
    streamers_df.printSchema()

    # Clean column names
    for c_name in streamers_df.columns:
        clean = c_name.strip().lower().replace(" ", "_").replace("-", "_")
        streamers_df = streamers_df.withColumnRenamed(c_name, clean)

    # ── 2. Top streamers by followers ────────────────────────
    top_streamers = (streamers_df
        .filter(col("name").isNotNull())
        .groupBy("name")
        .agg(
            max("total_followers").alias("followers"),
            max("total_views").alias("total_views"),
            max("avg_viewers_per_stream").alias("avg_viewers"),
            count("*").alias("data_points")
        )
        .orderBy(desc("followers"))
        .limit(200)
    )

    # Write → trending_topics (streamers as keywords)
    streamer_trending = (top_streamers.select(
        col("name").alias("keyword"),
        lit(2).cast("short").alias("platform_id"),
        col("followers").cast("int").alias("count"),
        lit("2020-01-01 00:00:00").cast("timestamp").alias("window_start"),
        lit("2024-12-31 23:59:59").cast("timestamp").alias("window_end"),
        lit(True).alias("is_batch")
    ))

    (streamer_trending.write
        .mode("append")
        .jdbc(PG_URL, "trending_topics", properties=PG_PROPERTIES))
    print(f"  ✅  Written {streamer_trending.count()} rows → trending_topics (streamers)")

    # ── 3. Load chat Parquet ──────────────────────────────────
    chat_df = (spark.read
        .parquet(f"{HDFS_MASTER}/data/twitch/chat/"))

    print(f"  ✅  Loaded chat parquet → {chat_df.count():,} rows")
    chat_df.printSchema()

    # ── 4. Basic sentiment proxy from chat ────────────────────
    # Use message length & common positive/negative words as proxy
    # (VADER runs in streaming; here we do keyword-based batch proxy)
    positive_words = ["lol", "gg", "pog", "poggers", "hype", "nice", 
                      "great", "amazing", "love", "good", "win", "yes"]
    negative_words = ["bad", "trash", "hate", "no", "lose", "worst",
                      "awful", "terrible", "boring", "stop", "die"]

    pos_pattern = "|".join(positive_words)
    neg_pattern = "|".join(negative_words)

    # Find the message column (may vary by dataset)
    msg_col = None
    for c in chat_df.columns:
        if "message" in c.lower() or "text" in c.lower() or "content" in c.lower() or "body" in c.lower():
            msg_col = c
            break

    if msg_col:
        scored = (chat_df
            .withColumn("msg_lower", lower(trim(col(msg_col))))
            .withColumn("is_positive", col("msg_lower").rlike(pos_pattern).cast("int"))
            .withColumn("is_negative", col("msg_lower").rlike(neg_pattern).cast("int"))
        )

        sentiment_agg = scored.agg(
            count("*").alias("total_messages"),
            round(avg("is_positive") * 100, 2).alias("positive_pct"),
            round(avg("is_negative") * 100, 2).alias("negative_pct"),
            round((1 - avg("is_positive") - avg("is_negative")) * 100, 2).alias("neutral_pct")
        ).collect()[0]

        # Write → sentiment_summary
        sentiment_row = spark.createDataFrame([{
            "platform_id":   2,
            "time_bucket":   datetime(2024, 1, 1, 0, 0, 0),
            "bucket_size":   "batch",
            "positive_pct":  float(sentiment_agg["positive_pct"]),
            "negative_pct":  float(sentiment_agg["negative_pct"]),
            "neutral_pct":   float(sentiment_agg["neutral_pct"]),
            "sample_count":  int(sentiment_agg["total_messages"]),
            "is_batch":      True
        }])

        (sentiment_row.write
            .mode("append")
            .jdbc(PG_URL, "sentiment_summary", properties=PG_PROPERTIES))
        print(f"  ✅  Written sentiment summary → sentiment_summary")
        print(f"      positive: {sentiment_agg['positive_pct']}% | "
              f"negative: {sentiment_agg['negative_pct']}% | "
              f"neutral: {sentiment_agg['neutral_pct']}%")
    else:
        print(f"  ⚠️   Could not find message column in chat data. Columns: {chat_df.columns}")

    print("\n✅  Twitch Streamers & Chat Batch Job complete!\n")
    spark.stop()

if __name__ == "__main__":
    main()
