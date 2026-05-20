# =============================================================
#  processing/run_in_docker.py
#  Copies a Spark script into hadoop-master and runs it.
#  Usage from VSCode terminal (project root):
#    python processing/run_in_docker.py batch_youtube
#    python processing/run_in_docker.py all
# =============================================================

import subprocess
import sys
import os
import time

CONTAINER  = "hadoop-master"
SPARK_JAR  = "/usr/local/spark/jars/postgresql-42.7.3.jar"
REMOTE_DIR = "/tmp/spark_jobs"

JOBS = [
    ("batch_twitch_games",    "1g", "Twitch Top Games      (small CSV)"),
    ("batch_twitch_streamers","2g", "Twitch Streamers+Chat (CSV + Parquet)"),
    ("batch_youtube",         "3g", "YouTube Trending      (6.6 GB CSV)"),
    ("batch_twitter",         "4g", "Twitter BlockMesh     (12.4 GB JSON.GZ — run last)"),
]

def docker_exec(args, **kwargs):
    return subprocess.run(["docker", "exec", CONTAINER] + args, **kwargs)

def ensure_remote_dir():
    docker_exec(["mkdir", "-p", REMOTE_DIR], check=True, capture_output=True)

def copy_and_run(job_name, memory="2g"):
    local_path  = os.path.join(os.path.dirname(__file__), f"{job_name}.py")
    remote_path = f"{REMOTE_DIR}/{job_name}.py"

    if not os.path.exists(local_path):
        print(f"  ❌  Script not found: {local_path}")
        return False

    print(f"\n  Copying {job_name}.py → {CONTAINER}:{remote_path}")
    subprocess.run(
        ["docker", "cp", local_path, f"{CONTAINER}:{remote_path}"],
        check=True
    )

    print(f"  Running spark-submit (driver-memory={memory})...\n")
    cmd = [
        "spark-submit",
        "--master", "local[*]",
        "--driver-memory", memory,
        "--jars", SPARK_JAR,
        remote_path,
    ]
    start = time.time()
    result = docker_exec(cmd)
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"\n  ✅  {job_name} finished in {elapsed:.1f}s")
        return True
    else:
        print(f"\n  ❌  {job_name} failed (exit {result.returncode})")
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python processing/run_in_docker.py <job_name|all>")
        print("  job_name: batch_twitch_games | batch_twitch_streamers | batch_youtube | batch_twitter")
        print("  all:      runs all jobs in order (light → heavy)")
        sys.exit(1)

    target = sys.argv[1].lower()

    # Verify container is running
    check = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER],
        capture_output=True, text=True
    )
    if check.returncode != 0 or check.stdout.strip() != "true":
        print(f"  ❌  Container '{CONTAINER}' is not running. Start your Hadoop cluster first.")
        sys.exit(1)

    ensure_remote_dir()

    if target == "all":
        print(f"\n{'='*60}")
        print("  Running ALL Spark batch jobs (light → heavy)")
        print(f"{'='*60}")
        for job_name, memory, label in JOBS:
            print(f"\n{'─'*60}")
            print(f"  ▶  {label}")
            print(f"{'─'*60}")
            ok = copy_and_run(job_name, memory)
            if not ok:
                print(f"\n  ⚠️  Stopping — job failed: {job_name}")
                sys.exit(1)
            time.sleep(2)
        print(f"\n{'='*60}")
        print("  ✅  All batch jobs complete!")
        print(f"{'='*60}\n")
    else:
        job_map = {name: mem for name, mem, _ in JOBS}
        if target not in job_map:
            print(f"  ❌  Unknown job '{target}'. Choose from: {list(job_map)}")
            sys.exit(1)
        copy_and_run(target, job_map[target])

if __name__ == "__main__":
    main()
