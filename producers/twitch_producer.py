"""
Twitch producer — polls the Twitch Helix API for live stream data
and channel information, publishing each stream snapshot as a Kafka event.

Handles OAuth2 client-credentials token refresh automatically.
"""
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

from config import settings
from producers.base_producer import BaseProducer

logger = logging.getLogger(__name__)


class TwitchProducer(BaseProducer):

    HELIX_URL = "https://api.twitch.tv/helix"
    TOKEN_URL = "https://id.twitch.tv/oauth2/token"

    def __init__(self):
        super().__init__()
        self._cfg = settings.twitch
        if not self._cfg.client_id:
            raise ValueError("TWITCH_CLIENT_ID is required")
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._use_static_token = False

        access_token = os.getenv("TWITCH_ACCESS_TOKEN", "").strip()
        if access_token:
            self._access_token = access_token
            self._token_expires_at = float("inf")
            self._use_static_token = True
        else:
            if not self._cfg.client_secret:
                raise ValueError(
                    "TWITCH_CLIENT_SECRET is required unless TWITCH_ACCESS_TOKEN is set"
                )
            self._refresh_token()

    # ── BaseProducer interface ───────────────────────────────────────────────

    @property
    def source_name(self) -> str:
        return "twitch"

    @property
    def topic(self) -> str:
        return settings.kafka.twitch_topic

    def fetch_events(self) -> List[Dict[str, Any]]:
        self._refresh_token_if_needed()
        events = []
        try:
            live_streams = self._fetch_live_streams()
            channel_info = self._fetch_channel_info()
            events = self._merge_stream_and_channel(live_streams, channel_info)
        except Exception as exc:
            logger.error("Failed to fetch Twitch events: %s", exc)
        return events

    def run_loop(self) -> None:
        logger.info(
            "Twitch producer started | streamers=%s interval=%ds",
            self._cfg.streamer_logins,
            self._cfg.poll_interval_seconds,
        )
        while self._running:
            events = self.fetch_events()
            logger.info("Fetched %d Twitch stream events", len(events))
            for event in events:
                self.publish(event, key=event.get("user_login"))
            time.sleep(self._cfg.poll_interval_seconds)

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _fetch_live_streams(self) -> List[Dict[str, Any]]:
        """
        Returns live stream objects for the configured streamer list.
        Offline streamers are simply absent from the response — that's fine.
        """
        params = [("user_login", login) for login in self._cfg.streamer_logins]
        params.append(("first", "100"))
        resp = self._get(f"{self.HELIX_URL}/streams", params=params)
        return resp.get("data", [])

    def _fetch_channel_info(self) -> Dict[str, Dict]:
        """Returns channel metadata keyed by broadcaster_login."""
        # First resolve logins → broadcaster_ids via /users
        params = [("login", login) for login in self._cfg.streamer_logins]
        users_resp = self._get(f"{self.HELIX_URL}/users", params=params)
        users = {u["login"]: u for u in users_resp.get("data", [])}

        broadcaster_ids = [u["id"] for u in users.values()]
        if not broadcaster_ids:
            return {}

        # Then fetch channel info for those IDs
        params = [("broadcaster_id", bid) for bid in broadcaster_ids]
        channels_resp = self._get(f"{self.HELIX_URL}/channels", params=params)

        channel_map = {}
        for ch in channels_resp.get("data", []):
            login = ch.get("broadcaster_login")
            user = users.get(login, {})
            channel_map[login] = {
                "broadcaster_id": ch.get("broadcaster_id"),
                "broadcaster_name": ch.get("broadcaster_name"),
                "broadcaster_login": login,
                "game_name": ch.get("game_name"),
                "game_id": ch.get("game_id"),
                "title": ch.get("title"),
                "delay": ch.get("delay"),
                "tags": ch.get("tags", []),
                "profile_image_url": user.get("profile_image_url"),
                "description": user.get("description"),
                "view_count": user.get("view_count", 0),
                "created_at": user.get("created_at"),
            }
        return channel_map

    def _merge_stream_and_channel(
        self,
        streams: List[Dict],
        channels: Dict[str, Dict],
    ) -> List[Dict[str, Any]]:
        events = []
        for stream in streams:
            login = stream.get("user_login", "")
            channel = channels.get(login, {})
            events.append(
                {
                    "stream_id": stream.get("id"),
                    "user_id": stream.get("user_id"),
                    "user_login": login,
                    "user_name": stream.get("user_name"),
                    "game_id": stream.get("game_id"),
                    "game_name": stream.get("game_name"),
                    "type": stream.get("type"),               # "live" or ""
                    "title": stream.get("title"),
                    "viewer_count": stream.get("viewer_count", 0),
                    "started_at": stream.get("started_at"),
                    "language": stream.get("language"),
                    "thumbnail_url": stream.get("thumbnail_url"),
                    "tags": stream.get("tags", []),
                    "is_mature": stream.get("is_mature", False),
                    # Channel-level fields
                    "channel_description": channel.get("description"),
                    "channel_total_views": channel.get("view_count"),
                    "channel_created_at": channel.get("created_at"),
                    "profile_image_url": channel.get("profile_image_url"),
                }
            )
        return events

    # ── OAuth2 ───────────────────────────────────────────────────────────────

    def _refresh_token(self) -> None:
        resp = requests.post(
            self.TOKEN_URL,
            data={
                "client_id": self._cfg.client_id,
                "client_secret": self._cfg.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        # Buffer 60 s before actual expiry
        self._token_expires_at = time.time() + data.get("expires_in", 3600) - 60
        logger.info("Twitch OAuth token refreshed (expires in ~%ds)", data.get("expires_in", 3600))

    def _refresh_token_if_needed(self) -> None:
        if self._use_static_token:
            return
        if time.time() >= self._token_expires_at:
            self._refresh_token()

    def _get(self, url: str, params=None) -> Dict:
        headers = {
            "Client-ID": self._cfg.client_id,
            "Authorization": f"Bearer {self._access_token}",
        }
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 401:
            # Token may have been revoked; try once more after refresh
            self._refresh_token()
            headers["Authorization"] = f"Bearer {self._access_token}"
            response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    TwitchProducer().start()
