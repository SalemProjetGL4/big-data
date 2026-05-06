#!/usr/bin/env python3
"""
hdfs_setup.py  —  Social Media Trend Analyzer
Diagnoses Hadoop, starts services if needed, and uploads datasets to HDFS.

Twitter special handling:
  - sentiment140  : zip is copied into the container and unzipped there,
                    then the extracted CSV is pushed into HDFS.
  - blockmesh     : 13.3 GB Parquet files — skipped by default, set
                    UPLOAD_BLOCKMESH = True below to include them.

Usage (run from your project root on Windows):
    python hdfs_setup.py

Requirements:
    pip install docker
"""

import subprocess
import sys
import time
import os

try:
    import docker
except ImportError:
    print("[!] 'docker' package not found. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "docker"])
    import docker

# ──────────────────────────────────────────────
# CONFIG — adjust if your container name differs
# ──────────────────────────────────────────────
MASTER_CONTAINER  = "hadoop-master"
DATASETS_BASE_DIR = r"datasets"

# Set to True only if you have disk space and time for 13 GB
UPLOAD_BLOCKMESH  = True

HDFS_DIRS = [
    "/data/twitter/sentiment140",
    "/data/twitter/blockmesh",
    "/data/youtube/trending",
    "/data/twitch/top_games",
    "/data/twitch/top_streamers",
    "/data/twitch/chat",
    "/data/reddit",
]

# local_path (relative) → hdfs_path
# Twitter is handled separately by upload_twitter() below
UPLOAD_MAP = [
    (r"datasets\youtube\trending_113_countries", "/data/youtube/trending"),
    (r"datasets\twitch\top_games_2016_2023",     "/data/twitch/top_games"),
    (r"datasets\twitch\top_streamers_2024",      "/data/twitch/top_streamers"),
    (r"datasets\twitch\twitch_chat_2025",        "/data/twitch/chat"),
    # Reddit: uncomment once you have the dump
    # (r"datasets\reddit",                       "/data/reddit"),
]

# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def banner(text):
    width = 60
    print("\n" + "═" * width)
    print(f"  {text}")
    print("═" * width)

def ok(msg):  print(f"  ✓  {msg}")
def err(msg): print(f"  ✗  {msg}")
def info(msg):print(f"  ℹ  {msg}")
def warn(msg):print(f"  ⚠  {msg}")

def exec_in_container(container, cmd, stream=False):
    """Run a shell command inside the container and return (exit_code, output)."""
    result = container.exec_run(f"bash -c \"{cmd}\"", stream=stream)
    if stream:
        return result
    return result.exit_code, result.output.decode("utf-8", errors="replace").strip()

# ──────────────────────────────────────────────
# STEP 1 — Connect to Docker
# ──────────────────────────────────────────────

def connect_docker():
    banner("STEP 1 — Connecting to Docker")
    try:
        client = docker.from_env()
        client.ping()
        ok("Docker daemon is reachable")
        return client
    except Exception as e:
        err(f"Cannot connect to Docker: {e}")
        info("Make sure Docker Desktop is running, then retry.")
        sys.exit(1)

# ──────────────────────────────────────────────
# STEP 2 — Find master container
# ──────────────────────────────────────────────

def get_master(client):
    banner("STEP 2 — Locating hadoop-master container")
    try:
        c = client.containers.get(MASTER_CONTAINER)
        ok(f"Found container '{c.name}'  (status: {c.status})")
        if c.status != "running":
            info("Container is not running — starting it...")
            c.start()
            time.sleep(5)
            c.reload()
            if c.status != "running":
                err("Container failed to start.")
                sys.exit(1)
            ok("Container started.")
        return c
    except docker.errors.NotFound:
        err(f"Container '{MASTER_CONTAINER}' not found.")
        info("Available containers:")
        for c in client.containers.list(all=True):
            print(f"     {c.name}  ({c.status})")
        sys.exit(1)

# ──────────────────────────────────────────────
# STEP 3 — Diagnose Hadoop services (jps)
# ──────────────────────────────────────────────

def diagnose_hadoop(master):
    banner("STEP 3 — Diagnosing Hadoop services (jps)")
    _, output = exec_in_container(master, "jps")
    print(f"  jps output:\n    {output.replace(chr(10), chr(10)+'    ')}")

    running = output.lower()
    namenode_up   = "namenode"   in running
    datanode_up   = "datanode"   in running

    if namenode_up:
        ok("NameNode is running")
    else:
        warn("NameNode is NOT running")

    if datanode_up:
        ok("DataNode is running (on master)")
    else:
        info("DataNode not on master (may be on worker containers — that's fine)")

    return namenode_up

# ──────────────────────────────────────────────
# STEP 4 — Fix SSH + start Hadoop if needed
# ──────────────────────────────────────────────

def fix_ssh(master):
    info("Checking SSH service...")
    _, out = exec_in_container(master, "service ssh status 2>&1 || true")
    if "running" in out.lower() or "active" in out.lower():
        ok("SSH is running")
    else:
        info("Starting SSH...")
        exec_in_container(master, "service ssh start")
        time.sleep(2)
        ok("SSH started")

    info("Ensuring passwordless SSH to localhost...")
    exec_in_container(master,
        "mkdir -p ~/.ssh && "
        "[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && "
        "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && "
        "chmod 600 ~/.ssh/authorized_keys && "
        "ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null && "
        "ssh-keyscan -H hadoop-master >> ~/.ssh/known_hosts 2>/dev/null || true"
    )
    ok("SSH keys configured")

def check_hdfs_formatted(master):
    """Return True if HDFS has already been formatted (current dir exists)."""
    _, out = exec_in_container(master,
        "[ -d /tmp/hadoop-root/dfs/name/current ] && echo YES || echo NO")
    return "YES" in out

def start_hadoop(master):
    banner("STEP 4 — Starting Hadoop services")
    fix_ssh(master)

    if not check_hdfs_formatted(master):
        warn("HDFS not formatted. Formatting now (this erases existing HDFS data)...")
        _, out = exec_in_container(master,
            "echo Y | hdfs namenode -format 2>&1")
        if "successfully formatted" in out.lower() or "re-format" in out.lower() or out == "":
            ok("HDFS formatted")
        else:
            # Some setups print differently — just proceed
            info(f"Format output: {out[:300]}")
    else:
        ok("HDFS already formatted — skipping format")

    info("Running start-dfs.sh ...")
    _, out = exec_in_container(master, "start-dfs.sh 2>&1")
    print(f"    {out[:500]}")
    time.sleep(8)

    # Verify
    _, jps = exec_in_container(master, "jps")
    if "namenode" in jps.lower():
        ok("NameNode is now running")
    else:
        err("NameNode still not running after start-dfs.sh")
        err("Check your Hadoop config (core-site.xml / hdfs-site.xml).")
        info(f"jps: {jps}")
        sys.exit(1)

    info("Running start-yarn.sh ...")
    exec_in_container(master, "start-yarn.sh 2>&1")
    time.sleep(4)
    ok("YARN started")

# ──────────────────────────────────────────────
# STEP 5 — Create HDFS directories
# ──────────────────────────────────────────────

def create_hdfs_dirs(master):
    banner("STEP 5 — Creating HDFS directory structure")
    for d in HDFS_DIRS:
        code, out = exec_in_container(master, f"hdfs dfs -mkdir -p {d} 2>&1")
        if code == 0:
            ok(f"hdfs:{d}")
        else:
            warn(f"mkdir {d}: {out}")

    # Show final structure
    _, listing = exec_in_container(master, "hdfs dfs -ls /data/")
    print(f"\n  HDFS /data/ contents:\n    {listing.replace(chr(10), chr(10)+'    ')}")

# ──────────────────────────────────────────────
# STEP 6 — Copy datasets → container → HDFS
# ──────────────────────────────────────────────

def get_files_to_upload(local_dir, include_zips=False):
    """Return list of file paths under local_dir (recursive)."""
    if not os.path.isdir(local_dir):
        return []
    files = []
    for root, _, filenames in os.walk(local_dir):
        for f in filenames:
            if not include_zips and f.endswith(".zip"):
                continue
            files.append(os.path.join(root, f))
    return files

def copy_file_to_container(master, local_file, container_dir="/tmp/hdfs_staging"):
    """
    Stream a local file into the container at container_dir/<filename>.
    Returns the full container path on success, None on failure.
    """
    import tarfile, io
    filename = os.path.basename(local_file)
    try:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            tar.add(local_file, arcname=filename)
        buf.seek(0)
        exec_in_container(master, f"mkdir -p {container_dir}")
        master.put_archive(container_dir, buf)
        return f"{container_dir}/{filename}"
    except Exception as e:
        err(f"  docker cp failed for {filename}: {e}")
        return None

def hdfs_file_exists(master, hdfs_path):
    """Return True if a file already exists in HDFS."""
    code, _ = exec_in_container(master, f"hdfs dfs -test -e {hdfs_path} 2>/dev/null")
    return code == 0

def copy_file_to_hdfs(master, container_path, hdfs_dir):
    """Push a file already inside the container into HDFS, skip if it exists."""
    filename  = os.path.basename(container_path)
    hdfs_dest = f"{hdfs_dir}/{filename}"

    if hdfs_file_exists(master, hdfs_dest):
        info(f"  SKIP (already in HDFS): {filename}")
        exec_in_container(master, f"rm -f {container_path}")
        return True

    code, out = exec_in_container(master,
        f"hdfs dfs -put {container_path} {hdfs_dir}/ 2>&1")
    exec_in_container(master, f"rm -f {container_path}")
    if code == 0:
        ok(f"  {filename}  →  hdfs:{hdfs_dir}/")
        return True
    err(f"  hdfs put failed: {filename}: {out[:200]}")
    return False

# ── Twitter special handling ───────────────────

def upload_twitter(master):
    """
    sentiment140 : copy zip → container, unzip there, put CSV → HDFS.
    blockmesh    : copy Parquet files directly (only if UPLOAD_BLOCKMESH=True).
    """
    banner("STEP 6a — Twitter datasets")

    # ── sentiment140 ──────────────────────────
    sentiment_dir = os.path.abspath(r"datasets\twitter\sentiment140")
    zip_files = [f for f in os.listdir(sentiment_dir) if f.endswith(".zip")] \
                if os.path.isdir(sentiment_dir) else []

    if not zip_files:
        warn("sentiment140 zip not found — skipping")
    else:
        zip_path = os.path.join(sentiment_dir, zip_files[0])
        size_mb  = os.path.getsize(zip_path) / (1024 * 1024)
        info(f"sentiment140: uploading zip ({size_mb:.1f} MB) → unzipping in container → HDFS")

        # Install unzip if missing
        exec_in_container(master,
            "which unzip >/dev/null 2>&1 || apt-get install -y unzip -qq")

        # Copy zip into container
        container_zip = copy_file_to_container(master, zip_path, "/tmp/twitter_staging")
        if container_zip is None:
            err("  Failed to copy sentiment140 zip into container")
        else:
            # Unzip inside container
            info("  Unzipping inside container...")
            code, out = exec_in_container(master,
                f"unzip -o {container_zip} -d /tmp/twitter_staging/ 2>&1")
            if code != 0:
                err(f"  unzip failed: {out[:300]}")
            else:
                ok("  Unzipped successfully")
                # Find extracted CSVs and push to HDFS
                _, ls_out = exec_in_container(master,
                    "find /tmp/twitter_staging/ -name '*.csv' 2>/dev/null")
                csv_files = [f.strip() for f in ls_out.splitlines() if f.strip()]
                if not csv_files:
                    warn("  No CSV found after unzip — check zip contents")
                else:
                    for csv_path in csv_files:
                        filename  = os.path.basename(csv_path)
                        hdfs_dest = f"/data/twitter/sentiment140/{filename}"
                        if hdfs_file_exists(master, hdfs_dest):
                            info(f"  SKIP (already in HDFS): {filename}")
                        else:
                            copy_file_to_hdfs(master, csv_path, "/data/twitter/sentiment140")
            # Clean up zip
            exec_in_container(master, f"rm -f {container_zip}")

    # ── blockmesh ─────────────────────────────
    blockmesh_dir = os.path.abspath(r"datasets\twitter\blockmesh_2024")
    if not os.path.isdir(blockmesh_dir):
        warn("blockmesh_2024 folder not found — skipping")
        return

    parquet_files = get_files_to_upload(blockmesh_dir)
    total_gb = sum(os.path.getsize(f) for f in parquet_files) / (1024 ** 3)

    if not UPLOAD_BLOCKMESH:
        warn(
            f"blockmesh: {len(parquet_files)} Parquet files ({total_gb:.1f} GB) — "
            f"SKIPPED (set UPLOAD_BLOCKMESH = True to include)"
        )
        return

    info(f"blockmesh: uploading {len(parquet_files)} Parquet files ({total_gb:.1f} GB)...")
    for f in parquet_files:
        size_mb = os.path.getsize(f) / (1024 * 1024)
        info(f"  {os.path.basename(f)}  ({size_mb:.1f} MB)...")
        cpath = copy_file_to_container(master, f, "/tmp/hdfs_staging")
        if cpath:
            copy_file_to_hdfs(master, cpath, "/data/twitter/blockmesh")

# ── Generic datasets ──────────────────────────

def upload_datasets(master):
    banner("STEP 6b — Remaining datasets")

    total_files = 0
    uploaded    = 0
    skipped     = 0

    for local_dir, hdfs_dir in UPLOAD_MAP:
        abs_local = os.path.abspath(local_dir)
        files     = get_files_to_upload(abs_local)  # skips zips

        if not files:
            warn(f"No files found in {abs_local} — skipping")
            skipped += 1
            continue

        total_gb = sum(os.path.getsize(f) for f in files) / (1024 ** 3)
        info(f"\n  {local_dir}  ({len(files)} files, {total_gb:.2f} GB)  →  hdfs:{hdfs_dir}")

        for f in files:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            info(f"  Uploading {os.path.basename(f)}  ({size_mb:.1f} MB)...")
            cpath = copy_file_to_container(master, f)
            if cpath:
                success = copy_file_to_hdfs(master, cpath, hdfs_dir)
                total_files += 1
                if success:
                    uploaded += 1
            else:
                total_files += 1

    print()
    ok(f"Upload complete: {uploaded}/{total_files} files uploaded")
    if skipped:
        warn(f"{skipped} dataset folder(s) not found — check UPLOAD_MAP paths")

# ──────────────────────────────────────────────
# STEP 7 — Final HDFS report
# ──────────────────────────────────────────────

def hdfs_report(master):
    banner("STEP 7 — HDFS Size Report")
    _, out = exec_in_container(master, "hdfs dfs -du -s -h /data/* 2>&1")
    print(f"  {out.replace(chr(10), chr(10)+'  ')}")

    _, out2 = exec_in_container(master, "hdfs dfsadmin -report 2>&1 | head -30")
    print(f"\n  Cluster summary:\n  {out2.replace(chr(10), chr(10)+'  ')}")

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    print("""
╔══════════════════════════════════════════════════════════╗
║   Social Media Trend Analyzer — HDFS Setup & Uploader   ║
╚══════════════════════════════════════════════════════════╝
""")

    client  = connect_docker()
    master  = get_master(client)
    nn_up   = diagnose_hadoop(master)

    if not nn_up:
        start_hadoop(master)

    create_hdfs_dirs(master)
    upload_twitter(master)
    upload_datasets(master)
    hdfs_report(master)

    print("""
╔══════════════════════════════════════════════════════════╗
║   ✅  All done! Your datasets are in HDFS.              ║
║                                                          ║
║   Next steps:                                            ║
║   1. Open http://localhost:9870  (NameNode UI)           ║
║   2. Run your PySpark batch jobs against hdfs:///data/   ║
║   3. Start Kafka producers for live API ingestion        ║
╚══════════════════════════════════════════════════════════╝
""")

if __name__ == "__main__":
    main()