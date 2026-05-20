# =============================================================
#  storage/init_postgres.py
#  Applies postgres_schema.sql to the running postgres container.
#  Run from VSCode on the host — connects via localhost:5432.
# =============================================================

import sys
import os

def main():
    try:
        import psycopg2
    except ImportError:
        print("  ❌  psycopg2 not installed — run: pip install psycopg2-binary")
        sys.exit(1)

    schema_path = os.path.join(os.path.dirname(__file__), "postgres_schema.sql")
    if not os.path.exists(schema_path):
        print(f"  ❌  Schema file not found: {schema_path}")
        sys.exit(1)

    print("\n🚀  Initializing PostgreSQL for Social Media Trend Analyzer")
    print("    Connecting to localhost:5432 (trends_db)...\n")

    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="trends_db",
            user="postgres", password="password",
            connect_timeout=5
        )
        conn.autocommit = True
        cur = conn.cursor()

        with open(schema_path, "r", encoding="utf-8") as f:
            sql = f.read()

        cur.execute(sql)
        print("  ✅  Schema applied successfully")

        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' ORDER BY table_name;
        """)
        tables = [r[0] for r in cur.fetchall()]
        print(f"\n  Tables created: {', '.join(tables)}")

        cur.close()
        conn.close()
        print("\n✅  PostgreSQL is ready.\n")

    except Exception as e:
        print(f"  ❌  Error: {e}")
        print("      Make sure the postgres container is running: docker ps")
        sys.exit(1)

if __name__ == "__main__":
    main()
