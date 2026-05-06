#!/usr/bin/env python3
"""
hdfs_cleanup.py  —  Social Media Trend Analyzer
Removes junk files from HDFS (junk uploaded by HuggingFace cache).

Run once to fix the previous upload:
    python hdfs_cleanup.py

Requirements:
    pip install docker
"""

import sys
import subprocess

try:
    import docker
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "docker"])
    import docker

MASTER_CONTAINER = "hadoop-master"

# ── Junk files to remove ──────────────────────────────────────────────────────
# These are HuggingFace cache artifacts that should never be in HDFS
JUNK_NAMES = {".gitignore", "cachedir.tag"}
JUNK_EXTENSIONS = {".metadata", ".png", ".jpg", ".jpeg", ".gif"}

# ── Blockmesh: delete entire folder (was uploaded but UPLOAD_BLOCKMESH=False) ─


def banner(t): print(f"\n{'═'*60}\n  {t}\n{'═'*60}")
def ok(m):     print(f"  ✓  {m}")
def warn(m):   print(f"  ⚠  {m}")
def info(m):   print(f"  ℹ  {m}")
def err(m):    print(f"  ✗  {m}")

def run(container, cmd):
    r = container.exec_run(f'bash -c "{cmd}"')
    return r.exit_code, r.output.decode("utf-8", errors="replace").strip()


def connect():
    client = docker.from_env()
    c = client.containers.get(MASTER_CONTAINER)
    ok(f"Connected to '{c.name}' ({c.status})")
    return c


def list_all_hdfs_files(c):
    _, out = run(c, "hdfs dfs -ls -R /data/ 2>/dev/null")
    paths = []
    for line in out.splitlines():
        parts = line.split()
        # hdfs ls -R lines: permissions replicas owner group size date time PATH
        if len(parts) >= 8 and parts[0].startswith("-"):  # files only, not dirs
            paths.append(parts[-1])
    return paths


def is_junk(path):
    name = path.split("/")[-1].lower()
    if name in JUNK_NAMES:
        return True
    for junk_ext in JUNK_EXTENSIONS:
        if name.endswith(junk_ext):
            return True
    return False


def hdfs_remove(c, path):
    code, out = run(c, f"hdfs dfs -rm {path} 2>&1")
    if code == 0:
        ok(f"Deleted: {path}")
    else:
        err(f"Failed to delete {path}: {out[:100]}")


def main():
    print("""
╔══════════════════════════════════════════════════════════╗
║   Social Media Trend Analyzer — HDFS Cleanup            ║
╚══════════════════════════════════════════════════════════╝
""")

    c = connect()

    # ── 2. Remove junk files everywhere under /data ──────────────────────────
    banner("STEP 1 — Remove junk files (.gitignore, CACHEDIR.TAG, *.metadata, images)")
    all_files = list_all_hdfs_files(c)
    junk = [p for p in all_files if is_junk(p)]

    if not junk:
        ok("No junk files found")
    else:
        info(f"Found {len(junk)} junk file(s):")
        for path in junk:
            hdfs_remove(c, path)

    # ── 3. Final state report ────────────────────────────────────────────────
    banner("STEP 2 — Current HDFS contents after cleanup")
    _, out = run(c, "hdfs dfs -du -s -h /data/* 2>&1")
    print(f"  {out.replace(chr(10), chr(10)+'  ')}")

    print("""
╔══════════════════════════════════════════════════════════╗
║   ✅  Cleanup done!                                     ║
╚══════════════════════════════════════════════════════════╝
""")


if __name__ == "__main__":
    main()