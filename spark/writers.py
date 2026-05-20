"""
Writers — foreachBatch sink functions for MongoDB and PostgreSQL.

MongoDB: full raw payload documents (one collection per source)
PostgreSQL: structured analytics rows via JDBC upsert
"""
import logging
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import settings

logger = logging.getLogger(__name__)


# ── MongoDB ──────────────────────────────────────────────────────────────────

def make_mongo_writer(collection: str) -> Callable:
    """
    Returns a foreachBatch function that writes raw payload rows
    to the given MongoDB collection.

    Uses the MongoDB Spark Connector (mongo-spark-connector).
    The connector is added via --packages at spark-submit time.
    """
    mongo_cfg = settings.mongo

    def write_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        count = batch_df.count()
        logger.info(
            "MongoDB | batch=%d collection=%s rows=%d",
            batch_id,
            collection,
            count,
        )
        (
            batch_df
            .write
            .format("mongodb")
            .mode("append")
            .option("spark.mongodb.connection.uri", mongo_cfg.uri)
            .option("spark.mongodb.database", mongo_cfg.database)
            .option("spark.mongodb.collection", collection)
            .save()
        )

    return write_to_mongo


# ── PostgreSQL ───────────────────────────────────────────────────────────────

def make_postgres_writer(table: str, id_column: str) -> Callable:
    """
    Returns a foreachBatch function that upserts analytics rows
    into the given PostgreSQL table via JDBC.

    Strategy: write to a temp table, then run an INSERT … ON CONFLICT
    DO UPDATE via a JDBC execute call. For simpler pipelines, mode="append"
    with dedup downstream is also acceptable.
    """
    pg_cfg = settings.postgres
    jdbc_props = {
        "user": pg_cfg.user,
        "password": pg_cfg.password,
        "driver": pg_cfg.driver,
        "batchsize": str(pg_cfg.batch_size),
        "reWriteBatchedInserts": "true",   # PG JDBC batch optimization
    }

    def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        count = batch_df.count()
        logger.info(
            "PostgreSQL | batch=%d table=%s rows=%d",
            batch_id,
            table,
            count,
        )
        # Add a processing timestamp for audit
        enriched = batch_df.withColumn(
            "processed_at", F.current_timestamp()
        )

        # Write to a temp staging table, then upsert into target
        staging_table = f"{table}_staging_{batch_id}"
        (
            enriched
            .write
            .jdbc(
                url=pg_cfg.url,
                table=staging_table,
                mode="overwrite",
                properties=jdbc_props,
            )
        )

        # Run upsert via JDBC connection on the driver
        _jdbc_upsert(pg_cfg, table, staging_table, id_column, enriched.columns)

    return write_to_postgres


def _jdbc_upsert(
    pg_cfg,
    target_table: str,
    staging_table: str,
    id_col: str,
    columns: list,
) -> None:
    """
    INSERT INTO target SELECT * FROM staging
    ON CONFLICT (id_col) DO UPDATE SET col = EXCLUDED.col, ...
    """
    import psycopg2
    from urllib.parse import urlparse

    # Parse JDBC URL: jdbc:postgresql://host:port/dbname
    url = pg_cfg.url.replace("jdbc:", "")
    parsed = urlparse(url)

    set_clause = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c != id_col
    )
    col_list = ", ".join(columns)
    sql = f"""
        INSERT INTO {target_table} ({col_list})
        SELECT {col_list} FROM {staging_table}
        ON CONFLICT ({id_col}) DO UPDATE SET {set_clause};
        DROP TABLE IF EXISTS {staging_table};
    """

    try:
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            dbname=parsed.path.lstrip("/"),
            user=pg_cfg.user,
            password=pg_cfg.password,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.close()
    except Exception as exc:
        logger.error("JDBC upsert failed for table %s: %s", target_table, exc)
        raise
