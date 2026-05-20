"""
Twitch chat producer — connects to Twitch IRC, streams chat messages,
and publishes each message as a Kafka event.

Requires a bot account and OAuth token with chat:read.
"""
import logging
import socket
import ssl
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

from config import settings
from producers.base_producer import BaseProducer

logger = logging.getLogger(__name__)


class TwitchChatProducer(BaseProducer):

    def __init__(self):
        super().__init__()
        self._cfg = settings.twitch_chat
        self._enabled = settings.features.enable_twitch_chat
        self._sock: Optional[socket.socket] = None
        self._buffer = ""

    # ── BaseProducer interface ───────────────────────────────────────────────

    @property
    def source_name(self) -> str:
        return "twitch_chat"

    @property
    def topic(self) -> str:
        return settings.kafka.twitch_chat_topic

    def fetch_events(self):
        raise NotImplementedError("Twitch chat uses a streaming loop")

    def run_loop(self) -> None:
        if not self._enabled:
            logger.info(
                "Twitch chat disabled (ENABLE_TWITCH_CHAT=false); idle mode enabled."
            )
            while self._running:
                time.sleep(30)
            return

        self._validate_config()
        while self._running:
            try:
                self._connect()
                self._read_loop()
            except Exception as exc:
                logger.error("Twitch chat connection error: %s", exc)
            finally:
                self._close()
            time.sleep(5)

    # ── Connection handling ─────────────────────────────────────────────────

    def _validate_config(self) -> None:
        missing = []
        if not self._cfg.bot_username:
            missing.append("TWITCH_BOT_USERNAME")
        if not self._cfg.oauth_token:
            missing.append("TWITCH_CHAT_OAUTH_TOKEN")
        if not self._cfg.channels:
            missing.append("TWITCH_CHAT_CHANNELS")
        if missing:
            raise ValueError("Missing required env vars: " + ", ".join(missing))

    def _connect(self) -> None:
        logger.info("Connecting to Twitch IRC at %s:%d", self._cfg.server, self._cfg.port)
        sock = socket.create_connection((self._cfg.server, self._cfg.port), timeout=10)
        if self._cfg.use_tls:
            context = ssl.create_default_context()
            sock = context.wrap_socket(sock, server_hostname=self._cfg.server)
        sock.settimeout(1.0)
        self._sock = sock
        self._buffer = ""

        token = self._cfg.oauth_token.strip()
        if not token.startswith("oauth:"):
            token = f"oauth:{token}"

        self._send_line(f"PASS {token}")
        self._send_line(f"NICK {self._cfg.bot_username}")
        self._send_line("CAP REQ :twitch.tv/tags twitch.tv/commands")
        for channel in self._cfg.channels:
            self._send_line(f"JOIN #{channel}")

    def _close(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        self._sock = None
        self._buffer = ""

    def _send_line(self, line: str) -> None:
        if not self._sock:
            return
        payload = (line + "\r\n").encode("utf-8")
        self._sock.sendall(payload)

    def _read_loop(self) -> None:
        if not self._sock:
            return
        while self._running:
            try:
                data = self._sock.recv(4096)
                if not data:
                    raise ConnectionError("Twitch IRC socket closed")
                self._buffer += data.decode("utf-8", errors="ignore")
                while "\r\n" in self._buffer:
                    line, self._buffer = self._buffer.split("\r\n", 1)
                    self._handle_line(line)
            except socket.timeout:
                continue

    # ── Line parsing ─────────────────────────────────────────────────────────

    def _handle_line(self, line: str) -> None:
        if line.startswith("PING"):
            self._send_line(line.replace("PING", "PONG", 1))
            return

        event = self._parse_privmsg(line)
        if event:
            self.publish(event, key=event["message_id"])

    def _parse_privmsg(self, line: str) -> Optional[Dict]:
        tags = {}
        rest = line
        if line.startswith("@"):
            tag_part, rest = line.split(" ", 1)
            tags = self._parse_tags(tag_part[1:])

        if " PRIVMSG " not in rest:
            return None

        prefix, trailing = rest.split(" PRIVMSG ", 1)
        if not prefix.startswith(":"):
            return None

        user_login = prefix[1:].split("!", 1)[0]
        if " :" in trailing:
            channel_part, message = trailing.split(" :", 1)
        else:
            channel_part, message = trailing, ""

        channel = channel_part.strip().lstrip("#")
        message_id = tags.get("id") or str(uuid.uuid4())
        sent_at = self._parse_sent_at(tags.get("tmi-sent-ts"))

        return {
            "message_id": message_id,
            "channel": channel,
            "user_id": tags.get("user-id"),
            "user_login": user_login,
            "display_name": tags.get("display-name"),
            "message": message,
            "badges": tags.get("badges"),
            "color": tags.get("color"),
            "emotes": tags.get("emotes"),
            "is_mod": tags.get("mod") == "1",
            "is_subscriber": tags.get("subscriber") == "1",
            "is_vip": tags.get("vip") == "1",
            "sent_at": sent_at,
        }

    def _parse_tags(self, raw: str) -> Dict[str, str]:
        tags: Dict[str, str] = {}
        for part in raw.split(";"):
            if "=" in part:
                key, value = part.split("=", 1)
            else:
                key, value = part, ""
            tags[key] = self._unescape_tag(value)
        return tags

    def _unescape_tag(self, value: str) -> str:
        return (
            value
            .replace("\\s", " ")
            .replace("\\:", ";")
            .replace("\\r", "\r")
            .replace("\\n", "\n")
            .replace("\\\\", "\\")
        )

    def _parse_sent_at(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        if raw.isdigit():
            ts = int(raw) / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        return None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    TwitchChatProducer().start()
