"""
YouTube producer — polls the YouTube Data API v3 for recent videos
and their statistics, then publishes each video as a Kafka event.

Tracks published video IDs in memory to avoid duplicate messages
across poll cycles. For production, swap the in-memory seen_ids set
for a Redis set or a Kafka-compacted dedup topic.
"""
import logging
import time
from typing import Any, Dict, List, Optional

import requests

from config import settings
from producers.base_producer import BaseProducer

logger = logging.getLogger(__name__)


class YouTubeProducer(BaseProducer):

    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(self):
        super().__init__()
        self._cfg = settings.youtube
        self._comments_cfg = settings.youtube_comments
        if not self._cfg.api_key:
            raise ValueError("YOUTUBE_API_KEY env var is required")
        self._seen_ids: set = set()   # dedup within a process lifetime

    # ── BaseProducer interface ───────────────────────────────────────────────

    @property
    def source_name(self) -> str:
        return "youtube"

    @property
    def topic(self) -> str:
        return settings.kafka.youtube_topic

    def fetch_events(self) -> List[Dict[str, Any]]:
        events = []
        for channel_id in self._cfg.channel_ids:
            try:
                events.extend(self._fetch_channel_videos(channel_id))
            except Exception as exc:
                logger.error("Failed to fetch YouTube channel %s: %s", channel_id, exc)
        return events

    def run_loop(self) -> None:
        logger.info(
            "YouTube producer started | channels=%s interval=%ds",
            self._cfg.channel_ids,
            self._cfg.poll_interval_seconds,
        )
        while self._running:
            events = self.fetch_events()
            new_events = [e for e in events if e["video_id"] not in self._seen_ids]
            logger.info("Fetched %d new YouTube events", len(new_events))

            for event in new_events:
                self.publish(event, key=event["video_id"])
                self._seen_ids.add(event["video_id"])

            if settings.features.enable_youtube_comments and new_events:
                self._publish_comments_for_new_videos(new_events)

            # Bound memory: keep only the last 10 000 seen IDs
            if len(self._seen_ids) > 10_000:
                self._seen_ids = set(list(self._seen_ids)[-5_000:])

            time.sleep(self._cfg.poll_interval_seconds)

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _fetch_channel_videos(self, channel_id: str) -> List[Dict[str, Any]]:
        """
        1. Search for recent uploads on the channel.
        2. Batch-fetch statistics for all returned video IDs.
        3. Merge and return enriched video dicts.
        """
        video_ids = self._search_recent_videos(channel_id)
        if not video_ids:
            return []

        stats = self._fetch_video_stats(video_ids)
        return [
            {
                "video_id": vid_id,
                "channel_id": channel_id,
                **stats.get(vid_id, {}),
            }
            for vid_id in video_ids
        ]

    def _search_recent_videos(self, channel_id: str) -> List[str]:
        params = {
            "part": "id",
            "channelId": channel_id,
            "order": "date",
            "type": "video",
            "maxResults": self._cfg.max_results_per_channel,
            "key": self._cfg.api_key,
        }
        resp = self._get(f"{self.BASE_URL}/search", params)
        return [item["id"]["videoId"] for item in resp.get("items", [])]

    def _fetch_video_stats(self, video_ids: List[str]) -> Dict[str, Any]:
        """Returns a dict keyed by video_id with title, stats, and snippet."""
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(video_ids),
            "key": self._cfg.api_key,
        }
        resp = self._get(f"{self.BASE_URL}/videos", params)
        result = {}
        for item in resp.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            details = item.get("contentDetails", {})
            result[item["id"]] = {
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "published_at": snippet.get("publishedAt"),
                "thumbnail_url": snippet.get("thumbnails", {})
                .get("high", {})
                .get("url"),
                "tags": snippet.get("tags", []),
                "category_id": snippet.get("categoryId"),
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
                "duration": details.get("duration"),   # ISO 8601 e.g. PT4M13S
                "definition": details.get("definition"),
            }
        return result

    # ── Comments ──────────────────────────────────────────────────────────

    def _publish_comments_for_new_videos(self, new_events: List[Dict[str, Any]]) -> None:
        for event in new_events:
            video_id = event.get("video_id")
            if not video_id:
                continue
            try:
                comments = self._fetch_comments_for_video(video_id)
                logger.info(
                    "Fetched %d comments for video %s",
                    len(comments),
                    video_id,
                )
                for comment in comments:
                    self.publish(
                        comment,
                        key=comment.get("comment_id"),
                        topic=settings.kafka.youtube_comments_topic,
                    )
            except Exception as exc:
                logger.error("Failed to fetch comments for %s: %s", video_id, exc)

    def _fetch_comments_for_video(self, video_id: str) -> List[Dict[str, Any]]:
        max_per_video = self._comments_cfg.max_per_video
        include_replies = self._comments_cfg.include_replies
        order = self._comments_cfg.order
        comments: List[Dict[str, Any]] = []
        seen_ids = set()

        page_token = None
        while True:
            params = {
                "part": "snippet,replies" if include_replies else "snippet",
                "videoId": video_id,
                "maxResults": 100,
                "textFormat": "plainText",
                "order": order,
                "key": self._cfg.api_key,
            }
            if page_token:
                params["pageToken"] = page_token

            resp = self._get(f"{self.BASE_URL}/commentThreads", params)
            items = resp.get("items", [])
            for item in items:
                top = item.get("snippet", {}).get("topLevelComment", {})
                top_id = top.get("id")
                thread_id = item.get("id")
                top_event = self._comment_to_event(
                    top,
                    video_id=video_id,
                    thread_id=thread_id,
                    parent_id=None,
                    is_reply=False,
                )
                if top_id and top_id not in seen_ids:
                    comments.append(top_event)
                    seen_ids.add(top_id)

                if include_replies:
                    replies = item.get("replies", {}).get("comments", [])
                    for reply in replies:
                        reply_id = reply.get("id")
                        if reply_id and reply_id not in seen_ids:
                            comments.append(
                                self._comment_to_event(
                                    reply,
                                    video_id=video_id,
                                    thread_id=thread_id,
                                    parent_id=reply.get("snippet", {}).get("parentId"),
                                    is_reply=True,
                                )
                            )
                            seen_ids.add(reply_id)

                    total_replies = item.get("snippet", {}).get("totalReplyCount", 0)
                    if total_replies and len(replies) < total_replies and top_id:
                        extra = self._fetch_replies(top_id, video_id, thread_id, seen_ids)
                        comments.extend(extra)

                if max_per_video > 0 and len(comments) >= max_per_video:
                    return comments[:max_per_video]

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        return comments

    def _fetch_replies(
        self,
        parent_id: str,
        video_id: str,
        thread_id: str,
        seen_ids: set,
    ) -> List[Dict[str, Any]]:
        replies: List[Dict[str, Any]] = []
        page_token = None
        while True:
            params = {
                "part": "snippet",
                "parentId": parent_id,
                "maxResults": 100,
                "textFormat": "plainText",
                "key": self._cfg.api_key,
            }
            if page_token:
                params["pageToken"] = page_token

            resp = self._get(f"{self.BASE_URL}/comments", params)
            items = resp.get("items", [])
            for reply in items:
                reply_id = reply.get("id")
                if reply_id and reply_id not in seen_ids:
                    replies.append(
                        self._comment_to_event(
                            reply,
                            video_id=video_id,
                            thread_id=thread_id,
                            parent_id=parent_id,
                            is_reply=True,
                        )
                    )
                    seen_ids.add(reply_id)

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        return replies

    def _comment_to_event(
        self,
        comment: Dict[str, Any],
        video_id: str,
        thread_id: str,
        parent_id: Optional[str],
        is_reply: bool,
    ) -> Dict[str, Any]:
        snippet = comment.get("snippet", {})
        author_channel = snippet.get("authorChannelId", {})
        return {
            "comment_id": comment.get("id"),
            "video_id": video_id,
            "thread_id": thread_id,
            "parent_id": parent_id,
            "author_channel_id": author_channel.get("value"),
            "author_display_name": snippet.get("authorDisplayName"),
            "author_profile_image_url": snippet.get("authorProfileImageUrl"),
            "text": snippet.get("textOriginal") or snippet.get("textDisplay"),
            "like_count": snippet.get("likeCount", 0),
            "published_at": snippet.get("publishedAt"),
            "updated_at": snippet.get("updatedAt"),
            "is_reply": is_reply,
        }

    def _get(self, url: str, params: Dict) -> Dict:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    YouTubeProducer().start()
