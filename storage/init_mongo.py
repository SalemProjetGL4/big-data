# =============================================================
#  Social Media Trend Analyzer — MongoDB Initialization
#  storage/init_mongo.py
#  Run once to create collections and indexes
# =============================================================

from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import CollectionInvalid
import sys

# ── Connection ────────────────────────────────────────────────
MONGO_URI = "mongodb://localhost:27017"   # change if different
DB_NAME   = "trend_analyzer"

def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

# ── Helpers ───────────────────────────────────────────────────
def create_collection(db, name):
    try:
        db.create_collection(name)
        print(f"  ✅  Created collection: {name}")
    except CollectionInvalid:
        print(f"  ⚠️   Collection already exists: {name} — skipping")

# ── Collections & Indexes ─────────────────────────────────────
def init_twitch(db):
    create_collection(db, "twitch_streams")
    col = db["twitch_streams"]
    col.create_index([("stream_id", ASCENDING)],  unique=True,  name="idx_stream_id")
    col.create_index([("fetched_at", DESCENDING)],              name="idx_fetched_at")
    col.create_index([("game_name",  ASCENDING)],               name="idx_game_name")
    col.create_index([("viewer_count", DESCENDING)],            name="idx_viewer_count")
    col.create_index([("title", TEXT)],                         name="idx_title_text")
    print("     Indexes ready: twitch_streams")

def init_youtube(db):
    create_collection(db, "youtube_videos")
    col = db["youtube_videos"]
    col.create_index([("video_id",   ASCENDING)],  unique=True, name="idx_video_id")
    col.create_index([("fetched_at", DESCENDING)],              name="idx_fetched_at")
    col.create_index([("category_id",ASCENDING)],               name="idx_category")
    col.create_index([("view_count", DESCENDING)],              name="idx_view_count")
    col.create_index([("title", TEXT), ("description", TEXT)],  name="idx_text_search")
    print("     Indexes ready: youtube_videos")

def init_reddit(db):
    create_collection(db, "reddit_posts")
    col = db["reddit_posts"]
    col.create_index([("post_id",    ASCENDING)],  unique=True, name="idx_post_id")
    col.create_index([("fetched_at", DESCENDING)],              name="idx_fetched_at")
    col.create_index([("subreddit",  ASCENDING)],               name="idx_subreddit")
    col.create_index([("score",      DESCENDING)],              name="idx_score")
    col.create_index([("title", TEXT), ("selftext", TEXT)],     name="idx_text_search")
    print("     Indexes ready: reddit_posts")

def init_trend_events(db):
    create_collection(db, "trend_events")
    col = db["trend_events"]
    col.create_index([("keyword",    ASCENDING)],               name="idx_keyword")
    col.create_index([("platform",   ASCENDING)],               name="idx_platform")
    col.create_index([("detected_at",DESCENDING)],              name="idx_detected_at")
    col.create_index([("keyword", ASCENDING), ("platform", ASCENDING)], name="idx_kw_platform")
    print("     Indexes ready: trend_events")

# ── Sample document shapes (for reference) ───────────────────
SAMPLE_TWITCH = {
    "stream_id":    "123456789",
    "user_id":      "987654321",
    "user_name":    "streamer_name",
    "game_id":      "27471",
    "game_name":    "Minecraft",
    "title":        "Stream title here",
    "viewer_count": 12500,
    "language":     "en",
    "tags":         ["gaming", "minecraft"],
    "started_at":   "2025-01-01T12:00:00Z",
    "fetched_at":   "2025-01-01T12:05:00Z",
    "sentiment":    {"compound": 0.4, "pos": 0.3, "neg": 0.0, "neu": 0.7}
}

SAMPLE_YOUTUBE = {
    "video_id":      "dQw4w9WgXcQ",
    "title":         "Video title",
    "description":   "Video description...",
    "channel_id":    "UC...",
    "channel_title": "Channel Name",
    "category_id":   "20",
    "tags":          ["gaming", "trending"],
    "view_count":    1000000,
    "like_count":    50000,
    "comment_count": 3000,
    "published_at":  "2025-01-01T10:00:00Z",
    "fetched_at":    "2025-01-01T12:00:00Z",
    "sentiment":     {"compound": 0.6, "pos": 0.5, "neg": 0.05, "neu": 0.45}
}

SAMPLE_REDDIT = {
    "post_id":       "abc123",
    "title":         "Post title here",
    "selftext":      "Post body...",
    "subreddit":     "gaming",
    "author":        "username",
    "score":         4200,
    "upvote_ratio":  0.97,
    "num_comments":  312,
    "flair":         "News",
    "created_utc":   1735689600,
    "fetched_at":    "2025-01-01T12:00:00Z",
    "sentiment":     {"compound": 0.3, "pos": 0.2, "neg": 0.1, "neu": 0.7}
}

SAMPLE_TREND_EVENT = {
    "keyword":      "Minecraft",
    "platform":     "twitch",
    "window_start": "2025-01-01T12:00:00Z",
    "window_end":   "2025-01-01T12:10:00Z",
    "count":        342,
    "spike_ratio":  3.1,
    "detected_at":  "2025-01-01T12:10:05Z",
    "cross_platform": True
}

# ── Main ──────────────────────────────────────────────────────
def main():
    print("\n🚀  Initializing MongoDB for Social Media Trend Analyzer")
    print(f"    URI: {MONGO_URI}")
    print(f"    DB:  {DB_NAME}\n")

    try:
        db = get_db()
        # Quick connection check
        db.command("ping")
        print("  ✅  Connected to MongoDB\n")
    except Exception as e:
        print(f"  ❌  Cannot connect to MongoDB: {e}")
        print("      Make sure MongoDB container is running: docker ps")
        sys.exit(1)

    init_twitch(db)
    init_youtube(db)
    init_reddit(db)
    init_trend_events(db)

    print("\n✅  All collections and indexes created successfully!")
    print(f"    Database '{DB_NAME}' is ready.\n")

    # Show summary
    print("  Collections in database:")
    for name in db.list_collection_names():
        count = db[name].count_documents({})
        print(f"    • {name:<25} ({count} documents)")
    print()

if __name__ == "__main__":
    main()