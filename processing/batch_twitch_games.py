# =============================================================
#  processing/batch_twitch_games.py
#  Spark Batch Job — Twitch Top Games
#  Reads: /data/twitch/top_games/*.csv  (HDFS)
#  Writes: PostgreSQL → trending_topics, engagement_metrics
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, sum, count, round, lit,
    to_timestamp, year, month, desc, regexp_replace
)
import os

# ── Config ────────────────────────────────────────────────────
HDFS_MASTER   = "hdfs://hadoop-master:9000"
PG_URL = "jdbc:postgresql://host.docker.internal:5432/trend_analyzer"
PG_PROPERTIES = {
    "user":     "postgres",
    "password": "password",
    "driver":   "org.postgresql.Driver"
}

def create_spark():
    return (SparkSession.builder
        .appName("TwitchGamesBatch")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("\n🚀  Twitch Top Games Batch Job started\n")

    # ── 1. Load CSVs ──────────────────────────────────────────
    games_df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(f"{HDFS_MASTER}/data/twitch/top_games/Twitch_game_data.csv"))

    global_df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{HDFS_MASTER}/data/twitch/top_games/Twitch_global_data.csv"))

    print(f"  ✅  Loaded Twitch_game_data.csv   → {games_df.count():,} rows")
    print(f"  ✅  Loaded Twitch_global_data.csv → {global_df.count():,} rows")
    games_df.printSchema()

    # ── 2. Clean column names (strip spaces/special chars) ────
    for c_name in games_df.columns:
        clean = c_name.strip().lower().replace(" ", "_").replace("-", "_")
        games_df = games_df.withColumnRenamed(c_name, clean)

    # ── 3. Top games by average viewers ──────────────────────
    top_games = (games_df
        .groupBy("game")
        .agg(
            round(avg("avg_viewers"), 0).alias("avg_viewers"),
            round(max("peak_viewers"), 0).alias("peak_viewers"),
            round(avg("hours_watched"), 0).alias("avg_hours_watched"),
            round(avg("hours_streamed"), 0).alias("avg_hours_streamed"),
            count("*").alias("data_points")
        )
        .orderBy(desc("avg_viewers"))
        .limit(200)
    )

    # ── 4. Write → trending_topics ────────────────────────────
    trending = (top_games.select(
        col("game").alias("keyword"),
        lit(2).cast("short").alias("platform_id"),
        col("avg_viewers").cast("int").alias("count"),
        lit("2020-01-01 00:00:00").cast("timestamp").alias("window_start"),
        lit("2024-12-31 23:59:59").cast("timestamp").alias("window_end"),
        lit(True).alias("is_batch")
    ))

    (trending.write
        .mode("append")
        .jdbc(PG_URL, "trending_topics", properties=PG_PROPERTIES))
    print(f"  ✅  Written {trending.count()} rows → trending_topics")

    # ── 5. Write → engagement_metrics ────────────────────────
    engagement = (top_games.select(
        lit(2).cast("short").alias("platform_id"),
        col("game").alias("content_id"),
        col("game").alias("title"),
        lit("game").alias("category"),
        lit("2020-01-01 00:00:00").cast("timestamp").alias("published_at"),
        lit("2024-12-31 23:59:59").cast("timestamp").alias("snapshot_at"),
        col("avg_viewers").cast("bigint").alias("views"),
        col("peak_viewers").cast("int").alias("peak_viewers"),
        lit(True).alias("is_batch")
    ))

    pythonengagement = engagement.dropDuplicates(["platform_id", "content_id", "snapshot_at"])

    (pythonengagement.write
        .mode("overwrite")
        .jdbc(PG_URL, "engagement_metrics", properties=PG_PROPERTIES))
    print(f"  ✅  Written {pythonengagement.count()} rows → engagement_metrics")

    print("\n✅  Twitch Games Batch Job complete!\n")
    spark.stop()

if __name__ == "__main__":
    main()
