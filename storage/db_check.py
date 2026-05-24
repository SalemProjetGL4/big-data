# =============================================================
#  DB Health Check — PostgreSQL + MongoDB
#  Social Media Trend Analyzer
# =============================================================

import sys

# ── PostgreSQL ────────────────────────────────────────────────
def check_postgres():
    print("\n" + "="*55)
    print("  PostgreSQL CHECK")
    print("="*55)
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="trends_db",
            user="postgres", password="password",
            connect_timeout=5
        )
        cur = conn.cursor()

        tables = [
            "platforms",
            "trending_topics",
            "sentiment_summary",
            "engagement_metrics",
            "cross_platform_trends",
            "spark_job_log"
        ]

        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            status = "✅" if count > 0 else "⚠️ EMPTY"
            print(f"  {status}  {table:<30} {count:>10} rows")

        # Check spark job log for batch jobs
        print("\n  --- Spark Batch Jobs ---")
        cur.execute("""
            SELECT job_name, platform, status, rows_written, started_at
            FROM spark_job_log
            WHERE status IN ('done', 'failed')
            ORDER BY started_at DESC
            LIMIT 10;
        """)
        rows = cur.fetchall()
        if rows:
            for r in rows:
                icon = "✅" if r[2] == "done" else "❌"
                print(f"  {icon}  {r[0]:<30} platform={r[1] or 'all':<10} rows={r[3] or 0}")
        else:
            print("  ⚠️  No spark job logs found")

        # Sample from trending_topics
        print("\n  --- Sample: trending_topics (top 5 batch) ---")
        cur.execute("""
            SELECT tt.keyword, p.name, tt.count, tt.window_start
            FROM trending_topics tt
            JOIN platforms p ON p.id = tt.platform_id
            WHERE tt.is_batch = TRUE
            ORDER BY tt.count DESC
            LIMIT 5;
        """)
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"  keyword={r[0]:<20} platform={r[1]:<10} count={r[2]:<8} window={str(r[3])[:16]}")
        else:
            print("  ⚠️  No batch trending topics found")

        cur.close()
        conn.close()
        print("\n  ✅  PostgreSQL connection OK\n")

    except ImportError:
        print("  ❌  psycopg2 not installed — run: pip install psycopg2-binary")
    except Exception as e:
        print(f"  ❌  PostgreSQL error: {e}")


# ── MongoDB ───────────────────────────────────────────────────
def check_mongo():
    print("="*55)
    print("  MongoDB CHECK")
    print("="*55)
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["trend_analyzer"]

        collections = [
            "twitch_streams",
            "youtube_videos",
            "trend_events"
        ]

        for col_name in collections:
            count = db[col_name].count_documents({})
            status = "✅" if count > 0 else "⚠️ EMPTY"
            print(f"  {status}  {col_name:<30} {count:>10} documents")

        # Sample document from each collection
        print("\n  --- Sample documents ---")
        for col_name in collections:
            doc = db[col_name].find_one({}, {"_id": 0})
            if doc:
                keys = list(doc.keys())[:5]
                print(f"  {col_name}: fields → {keys}")
            else:
                print(f"  {col_name}: no documents yet")

        client.close()
        print("\n  ✅  MongoDB connection OK\n")

    except ImportError:
        print("  ❌  pymongo not installed — run: pip install pymongo")
    except Exception as e:
        print(f"  ❌  MongoDB error: {e}")


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🔍  Social Media Trend Analyzer — DB Health Check")
    check_postgres()
    check_mongo()
    print("="*55)
    print("  Done.")
    print("="*55 + "\n")