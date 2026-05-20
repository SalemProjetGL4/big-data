# =============================================================
#  processing/batch_twitter.py
#  Spark Batch Job — Twitter BlockMesh (JSON.GZ, ~12.4 GB)
#  Reads: /data/twitter/blockmesh/*.json.gz  (HDFS)
#  Writes: PostgreSQL → trending_topics, sentiment_summary
#
#  ⚠️  Large file — runs with increased memory settings
#      Run LAST after other batch jobs complete
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, round, lit, desc, lower,
    when, trim, length, max, sum
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

def create_spark():
    return (SparkSession.builder
        .appName("TwitterBlockMeshBatch")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
        .getOrCreate())

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("\n🚀  Twitter BlockMesh Batch Job started (~12.4 GB)\n")
    print("    ⏳ This will take several minutes — large dataset\n")

    # ── 1. Load JSON.GZ ───────────────────────────────────────
    # Spark reads gzipped JSON natively
    df = (spark.read
        .option("multiLine", False)   # one JSON object per line
        .json(f"{HDFS_MASTER}/data/twitter/blockmesh/"))

    print("  ✅  Schema detected:")
    df.printSchema()

    # Sample count (avoid full scan twice)
    df.cache()
    total = df.count()
    print(f"  ✅  Total tweets loaded: {total:,}\n")

    # ── 2. Find relevant columns ──────────────────────────────
    # BlockMesh schema varies — detect text/hashtag columns
    all_cols = [c.lower() for c in df.columns]

    text_col     = next((c for c in df.columns if c.lower() in ["text", "full_text", "content", "body"]), None)
    hashtag_col  = next((c for c in df.columns if "hashtag" in c.lower()), None)
    retweet_col  = next((c for c in df.columns if "retweet" in c.lower()), None)
    like_col     = next((c for c in df.columns if c.lower() in ["favorite_count", "likes", "like_count"]), None)
    lang_col     = next((c for c in df.columns if c.lower() in ["lang", "language"]), None)

    print(f"  Detected columns → text: {text_col} | hashtags: {hashtag_col} | "
          f"retweets: {retweet_col} | likes: {like_col} | lang: {lang_col}")

    # ── 3. Filter English tweets ──────────────────────────────
    if lang_col:
        df = df.filter(col(lang_col) == "en")
        print(f"  ✅  Filtered to English tweets: {df.count():,}")

    # ── 4. Sentiment proxy on tweet text ──────────────────────
    if text_col:
        positive_words = ["love", "great", "amazing", "awesome", "good",
                          "happy", "best", "lol", "win", "nice", "excited"]
        negative_words = ["hate", "bad", "terrible", "awful", "worst",
                          "sad", "angry", "fail", "disgust", "horrible"]

        pos_pattern = "|".join(positive_words)
        neg_pattern = "|".join(negative_words)

        scored = (df
            .withColumn("text_lower", lower(trim(col(text_col))))
            .withColumn("is_positive", col("text_lower").rlike(pos_pattern).cast("int"))
            .withColumn("is_negative", col("text_lower").rlike(neg_pattern).cast("int"))
        )

        sentiment_agg = scored.agg(
            count("*").alias("total"),
            round(avg("is_positive") * 100, 2).alias("positive_pct"),
            round(avg("is_negative") * 100, 2).alias("negative_pct"),
            round((1 - avg("is_positive") - avg("is_negative")) * 100, 2).alias("neutral_pct")
        ).collect()[0]

        sentiment_row = (spark.createDataFrame([{
            "platform_id":  3,
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

    # ── 5. Top hashtags → trending_topics ─────────────────────
    if hashtag_col:
        # If hashtags is an array column, explode it
        from pyspark.sql.types import ArrayType, StringType
        ht_type = df.schema[hashtag_col].dataType

        if isinstance(ht_type, ArrayType):
            from pyspark.sql.functions import explode
            hashtags_df = (df
                .select(explode(col(hashtag_col)).alias("hashtag"))
                .filter(col("hashtag").isNotNull())
                .withColumn("hashtag", lower(trim(col("hashtag"))))
                .groupBy("hashtag")
                .agg(count("*").alias("mention_count"))
                .orderBy(desc("mention_count"))
                .limit(300)
            )
        else:
            # String column — split by comma or space
            from pyspark.sql.functions import split, explode
            hashtags_df = (df
                .select(explode(split(col(hashtag_col), "[,\\s]+")).alias("hashtag"))
                .filter(length(col("hashtag")) > 1)
                .withColumn("hashtag", lower(trim(col("hashtag"))))
                .groupBy("hashtag")
                .agg(count("*").alias("mention_count"))
                .orderBy(desc("mention_count"))
                .limit(300)
            )

        trending = (hashtags_df.select(
            col("hashtag").alias("keyword"),
            lit(3).cast("short").alias("platform_id"),
            col("mention_count").cast("int").alias("count"),
            lit("2020-01-01 00:00:00").cast("timestamp").alias("window_start"),
            lit("2024-12-31 23:59:59").cast("timestamp").alias("window_end"),
            lit(True).alias("is_batch")
        ))

        (trending.write
            .mode("append")
            .jdbc(PG_URL, "trending_topics", properties=PG_PROPERTIES))
        print(f"  ✅  Written {trending.count()} hashtag rows → trending_topics")

    elif text_col:
        # Fallback: extract common words from text as keywords
        print("  ℹ️   No hashtag column — extracting keywords from tweet text")
        from pyspark.sql.functions import split, explode

        stop_words = {"the","a","an","in","on","at","is","it","to","of",
                      "and","or","for","with","that","this","was","are","be",
                      "i","you","he","she","we","they","my","your","his","her",
                      "rt","https","http","amp","via"}

        words_df = (df
            .select(explode(split(lower(col(text_col)), r"[\s\W]+")).alias("word"))
            .filter(length(col("word")) > 3)
            .filter(~col("word").isin(stop_words))
            .groupBy("word")
            .agg(count("*").alias("freq"))
            .orderBy(desc("freq"))
            .limit(300)
        )

        trending = (words_df.select(
            col("word").alias("keyword"),
            lit(3).cast("short").alias("platform_id"),
            col("freq").cast("int").alias("count"),
            lit("2020-01-01 00:00:00").cast("timestamp").alias("window_start"),
            lit("2024-12-31 23:59:59").cast("timestamp").alias("window_end"),
            lit(True).alias("is_batch")
        ))

        (trending.write
            .mode("append")
            .jdbc(PG_URL, "trending_topics", properties=PG_PROPERTIES))
        print(f"  ✅  Written {trending.count()} keyword rows → trending_topics")

    df.unpersist()
    print("\n✅  Twitter BlockMesh Batch Job complete!\n")
    spark.stop()

if __name__ == "__main__":
    main()
