"""
Social Media Trend Analyzer — Dataset Download Script
======================================================
Downloads datasets for all 3 platforms:
    - YouTube  (Global Trending 2022–2025, Illinois Data Bank)
    - Twitter  (Sentiment140 + BlockMesh 2024, Kaggle / HuggingFace)
    - Twitch   (Chat 2025 + Top Games 2024, HuggingFace / Kaggle)

Requirements:
    pip install kaggle requests tqdm datasets huggingface_hub

Kaggle setup:
    1. Go to https://www.kaggle.com/settings → API → Create New Token
    2. Place the downloaded kaggle.json in:
       - Windows: C:/Users/<YourName>/.kaggle/kaggle.json
       - Linux/Mac: ~/.kaggle/kaggle.json
"""

import os
import subprocess
import sys
import zipfile
import time
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIG — change BASE_DIR if needed
# ─────────────────────────────────────────────
BASE_DIR = Path("./datasets")

DIRS = {
    "twitter":  BASE_DIR / "twitter",
    "youtube":  BASE_DIR / "youtube",
    "twitch":   BASE_DIR / "twitch",
}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def create_dirs():
    for d in DIRS.values():
        d.mkdir(parents=True, exist_ok=True)

def install(package: str):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])

def ensure_packages():
    packages = ["kaggle", "requests", "tqdm", "datasets", "huggingface_hub"]
    for pkg in packages:
        try:
            __import__(pkg.replace("-", "_"))
        except ImportError:
            print(f"  Installing {pkg}...")
            install(pkg)

def print_header(title: str):
    print("\n" + "═" * 55)
    print(f"  {title}")
    print("═" * 55)

def kaggle_download(dataset_slug: str, dest: Path):
    """Download a Kaggle dataset and unzip it."""
    import kaggle  # noqa — imported after install check
    print(f"  Downloading kaggle:{dataset_slug} → {dest}")
    try:
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(
            dataset_slug,
            path=str(dest),
            unzip=True,
            quiet=False,
        )
        print(f"  ✓ Done: {dataset_slug}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        print("    → Make sure kaggle.json is in place and your account accepted the dataset terms.")

def hf_download(repo_id: str, dest: Path, repo_type: str = "dataset"):
    """Download a HuggingFace dataset snapshot."""
    from huggingface_hub import snapshot_download
    print(f"  Downloading HuggingFace:{repo_id} → {dest}")
    try:
        snapshot_download(
            repo_id=repo_id,
            repo_type=repo_type,
            local_dir=str(dest),
            ignore_patterns=["*.md", "*.gitattributes"],
        )
        print(f"  ✓ Done: {repo_id}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")

def http_download(url: str, dest_file: Path):
    """Download a single file over HTTP with a progress bar."""
    import requests
    from tqdm import tqdm

    print(f"  Downloading {url}")
    print(f"  → {dest_file}")
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        with open(dest_file, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, unit_divisor=1024
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
        print(f"  ✓ Done: {dest_file.name}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")


# ─────────────────────────────────────────────
# PLATFORM DOWNLOADERS
# ─────────────────────────────────────────────

def download_twitter():
    print_header("🐦  TWITTER / X")
    dest = DIRS["twitter"]

    # 1) Sentiment140 — 1.6M labeled tweets (Kaggle)
    print("\n[1/2] Sentiment140 — 1.6M tweets with sentiment labels")
    kaggle_download("kazanova/sentiment140", dest / "sentiment140")

    # 2) BlockMesh tweets 2024 — recent tweets (HuggingFace)
    print("\n[2/2] BlockMesh Tweets — recent tweets Dec 2024")
    hf_download("blockmesh/tweets", dest / "blockmesh_2024")

    print("""
  ℹ️  Twitter note:
     Since X discontinued free API access in 2023, no large free
     dataset from 2024–2025 exists. Sentiment140 provides the bulk
     volume for Spark batch jobs; BlockMesh provides recent examples.
     Acknowledge this in your report — it is a known research issue.
    """)


def download_youtube():
    print_header("📺  YOUTUBE (Global Trending 2022–2025)")

    dest = DIRS["youtube"]

    # Primary: Illinois Data Bank — most complete 2022–2025 dataset
    print("\n[1/2] Global YouTube Trending 2022–2025 (Illinois Data Bank)")
    print("""
  This dataset (446K snapshots, 78.4M entries, 104 countries) is hosted
  on the University of Illinois Data Bank and requires a browser download.

  Manual steps:
  ─────────────
  1. Go to: https://databank.illinois.edu/datasets/IDB-9307654
  2. Click "Download Dataset"
  3. Save the zip to: ./datasets/youtube/
  4. This script will unzip it automatically if placed there.
    """)

    # Check if manually downloaded zip exists and unzip it
    zips = list(dest.glob("*.zip"))
    if zips:
        for z in zips:
            print(f"  Found zip: {z.name} — unzipping...")
            with zipfile.ZipFile(z, "r") as zf:
                zf.extractall(dest)
            print(f"  ✓ Unzipped: {z.name}")
    else:
        print("  ⏳ Waiting for manual download — skipping unzip for now.")

    # Fallback: Kaggle YouTube Trending 113 countries (updated 2024)
    print("\n[2/2] YouTube Trending 113 Countries — Kaggle fallback (2024)")
    kaggle_download(
        "asaniczka/trending-youtube-videos-113-countries",
        dest / "trending_113_countries",
    )


def download_twitch():
    print_header("🟣  TWITCH (2024–2025)")

    dest = DIRS["twitch"]

    # 1) Twitch Chat — HuggingFace (2025)
    print("\n[1/3] Twitch Chat Dataset (HuggingFace, 2025)")
    hf_download("lparkourer10/twitch_chat", dest / "twitch_chat_2025")

    # 2) Top Games on Twitch 2016–2023 (Kaggle)
    print("\n[2/3] Top Games on Twitch 2016–2023 (Kaggle)")
    kaggle_download(
        "rankirsh/evolution-of-top-games-on-twitch",
        dest / "top_games_2016_2023",
    )

    # 3) Top 1000 Streamers May 2024 (Kaggle)
    print("\n[3/3] Top 1000 Twitch Streamers — May 2024 (Kaggle)")
    kaggle_download(
        "hibrahimag1/top-1000-twitch-streamers-data-may-2024",
        dest / "top_streamers_2024",
    )


# ─────────────────────────────────────────────
# SIZE REPORT
# ─────────────────────────────────────────────

def size_report():
    print_header("📊  DATASET SIZE REPORT")
    total = 0
    for platform, path in DIRS.items():
        if path.exists():
            size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
            size_gb = size / (1024 ** 3)
            total += size
            print(f"  {platform:<10} {size_gb:>6.2f} GB   ({path})")
    print(f"  {'─'*40}")
    print(f"  {'TOTAL':<10} {total / (1024**3):>6.2f} GB")
    print()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("\n" + "█" * 55)
    print("  Social Media Trend Analyzer — Dataset Downloader")
    print("  Platforms: Twitter · YouTube · Twitch")
    print("█" * 55)

    print("\nChecking/installing required packages...")
    ensure_packages()
    create_dirs()

    # Menu
    print("""
  Which datasets do you want to download?
  ────────────────────────────────────────
  1) Twitter only
    2) YouTube only
    3) Twitch only
    4) ALL platforms
    5) Show size report only
    """)

        choice = input("  Enter choice [1-5]: ").strip()

    start = time.time()

    if choice == "1":
        download_twitter()
    elif choice == "2":
        download_youtube()
    elif choice == "3":
        download_twitch()
    elif choice == "4":
        download_twitter()
        download_youtube()
        download_twitch()
    elif choice == "5":
        pass
    else:
        print("  Invalid choice. Exiting.")
        return

    size_report()
    elapsed = time.time() - start
    print(f"  Total time: {elapsed/60:.1f} minutes")
    print("\n  ✅ Done! Check ./datasets/ for your files.\n")


if __name__ == "__main__":
    main()