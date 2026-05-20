# =============================================================
#  processing/run_all_batch.py
#  Master script — runs all Spark batch jobs in order
#  Usage: python processing/run_all_batch.py
# =============================================================

import subprocess
import sys
import time
from pathlib import Path

SPARK_SUBMIT = "spark-submit"   # must be in PATH inside Spark container
PG_JAR       = "/opt/spark/jars/postgresql-42.7.3.jar"

JOBS = [
    {
        "name":   "Twitch Top Games",
        "script": "processing/batch_twitch_games.py",
        "memory": "1g",
        "note":   "Small CSV — fast"
    },
    {
        "name":   "Twitch Streamers & Chat",
        "script": "processing/batch_twitch_streamers.py",
        "memory": "2g",
        "note":   "CSV + Parquet — medium"
    },
    {
        "name":   "YouTube Trending",
        "script": "processing/batch_youtube.py",
        "memory": "3g",
        "note":   "6.6 GB CSV — slow, be patient"
    },
    {
        "name":   "Twitter BlockMesh",
        "script": "processing/batch_twitter.py",
        "memory": "4g",
        "note":   "12.4 GB JSON.GZ — run last, takes longest"
    },
]

def run_job(job):
    print(f"\n{'='*60}")
    print(f"  ▶  {job['name']}")
    print(f"     {job['note']}")
    print(f"{'='*60}\n")

    cmd = [
        SPARK_SUBMIT,
        "--master", "local[*]",
        "--driver-memory", job["memory"],
        "--jars", PG_JAR,
        job["script"]
    ]

    start = time.time()
    result = subprocess.run(cmd, capture_output=False, text=True)
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"\n  ✅  {job['name']} done in {elapsed:.1f}s")
        return True
    else:
        print(f"\n  ❌  {job['name']} FAILED (exit {result.returncode})")
        return False

def main():
    print("\n" + "="*60)
    print("  🚀  Social Media Trend Analyzer — Spark Batch Runner")
    print("="*60)
    print(f"  Jobs to run: {len(JOBS)}")
    print(f"  Order: light → heavy\n")

    results = []
    for job in JOBS:
        ok = run_job(job)
        results.append((job["name"], ok))
        if not ok:
            print(f"\n  ⚠️  Skipping remaining jobs due to failure in: {job['name']}")
            break
        time.sleep(2)   # brief pause between jobs

    print("\n" + "="*60)
    print("  📊  Summary")
    print("="*60)
    for name, ok in results:
        status = "✅  DONE" if ok else "❌  FAILED"
        print(f"  {status}  {name}")
    print()

if __name__ == "__main__":
    main()
