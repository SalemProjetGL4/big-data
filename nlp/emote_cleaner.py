"""
EmoteCleaner — strips Twitch emotes, emoticons, spam patterns, and
platform noise from raw chat messages before NLP processing.

Handles:
  - Twitch global emotes (hardcoded common set + API-loaded full set)
  - BTTV / FFZ third-party emotes
  - Unicode emoji
  - Repeated characters (Kappa x500 spam)
  - URLs, @mentions, #hashtags
  - ASCII art / copypasta noise
"""
import re
import unicodedata
from typing import Optional, Set

import requests


# ── Static emote lists (most common, always available offline) ────────────────

TWITCH_GLOBAL_EMOTES: Set[str] = {
    # Facial expressions
    "Kappa", "KappaPride", "KappaHD", "KappaRoss", "KappaClaus",
    "PogChamp", "Pog", "POGGERS", "PogO",
    "LUL", "LULW", "LuL",
    "TriHard", "TriHard7",
    "4Head", "4HEad",
    "BibleThump", "BibleThump1",
    "ResidentSleeper", "ResidentSleeper2",
    "FeelsBadMan", "FeelsGoodMan", "FeelsWeirdMan",
    "monkaS", "monkaW", "monkaHMM", "monkaOMEGA",
    "Pepega", "PepeLaugh", "PepeHands", "PeepoHappy", "PeepoSad",
    "OMEGALUL", "OMEGADANCE",
    "Clap", "ClapClap",
    "HeyGuys", "VoHiYo",
    "DansGame", "DansGame2",
    "SwiftRage", "HassaanChop",
    "NotLikeThis", "NotATK",
    "WutFace", "DatSheffy",
    "EleGiggle", "GachiGASM", "HYPERS",
    "PauseChamp", "PauseMan",
    "Sadge", "Wokege", "Okayge", "Okege",
    "5Head", "BrainSlug",
    "KEKW", "KEKWait",
    "xqcL", "xqcW",
    # Actions / reactions
    "GG", "GGs", "GGWP", "GLHF", "FF", "EZ", "EZY", "EZEZ",
    "PJSalt", "PJSugar", "PJSub",
    "HolidayPresent", "HolidaySanta",
    "SeemsGood", "VoteYea", "VoteNay",
    "TPFufun", "TehePelo",
    # Subscriber / channel emotes structure (t1-t3)
    "hype", "HYPE", "Hype",
    "PogU", "PogBones",
    "WideHardo",
    # BetterTTV common
    "OMEGALUL", "NODDERS", "YESYES", "NOPERS",
    "PauseChamp", "BASED",
    "catJAM", "pepeD", "pepeJAM",
    "WAYTOODANK", "BATCHEST",
    "docSpin", "forsenCD",
    "Pog", "PogChampion",
}

# Regex patterns for various noise types
_URL_RE          = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_MENTION_RE      = re.compile(r"@\w+")
_HASHTAG_RE      = re.compile(r"#\w+")
_REPEATED_WORD_RE = re.compile(r"\b(\w+)(?:\s+\1){3,}\b", re.IGNORECASE)  # word x4+
_CHAR_SPAM_RE    = re.compile(r"(.)\1{4,}")        # aaaaa → a
_EMOTE_COUNT_RE  = re.compile(r"\b(\w+)\s+x\d+\b") # "Kappa x500"
_NON_ASCII_JUNK  = re.compile(r"[^\x00-\x7F\u00C0-\u024F\u0400-\u04FF]+")  # keep latin/cyrillic
_WHITESPACE_RE   = re.compile(r"\s{2,}")


class EmoteCleaner:
    """
    Stateful cleaner that can optionally fetch the live Twitch
    global emote list via the Helix API.
    """

    def __init__(
        self,
        extra_emotes: Optional[Set[str]] = None,
        strip_emoji: bool = True,
        strip_mentions: bool = True,
        strip_urls: bool = True,
        min_token_length: int = 2,
    ):
        self._emotes: Set[str] = TWITCH_GLOBAL_EMOTES.copy()
        if extra_emotes:
            self._emotes.update(extra_emotes)

        self._strip_emoji    = strip_emoji
        self._strip_mentions = strip_mentions
        self._strip_urls     = strip_urls
        self._min_token_len  = min_token_length

        # Build a single fast regex from the emote set
        self._emote_re = self._build_emote_regex()

    # ── Public API ────────────────────────────────────────────────────────────

    def clean(self, text: str) -> str:
        """
        Full cleaning pipeline. Returns cleaned text or empty string
        if nothing meaningful remains.
        """
        if not text or not text.strip():
            return ""

        t = text

        # 1. Remove URLs
        if self._strip_urls:
            t = _URL_RE.sub(" ", t)

        # 2. Remove @mentions
        if self._strip_mentions:
            t = _MENTION_RE.sub(" ", t)

        # 3. Remove #hashtags (keep the word, drop the #)
        t = _HASHTAG_RE.sub(lambda m: m.group(0)[1:], t)

        # 4. Remove "EmoteName x500" patterns
        t = _EMOTE_COUNT_RE.sub(" ", t)

        # 5. Remove known Twitch / BTTV / FFZ emotes
        t = self._emote_re.sub(" ", t)

        # 6. Collapse character spam: "AAAAAAA" → "A"
        t = _CHAR_SPAM_RE.sub(r"\1", t)

        # 7. Remove repeated word spam: "lol lol lol lol lol" → "lol"
        t = _REPEATED_WORD_RE.sub(r"\1", t)

        # 8. Strip unicode emoji
        if self._strip_emoji:
            t = self._remove_emoji(t)

        # 9. Remove non-latin/cyrillic junk characters
        t = _NON_ASCII_JUNK.sub(" ", t)

        # 10. Normalise whitespace
        t = _WHITESPACE_RE.sub(" ", t).strip()

        # 11. Drop if too short after cleaning
        tokens = [tok for tok in t.split() if len(tok) >= self._min_token_len]
        return " ".join(tokens)

    def add_emotes(self, emotes: Set[str]) -> None:
        """Add emotes at runtime (e.g. from channel-specific emote fetch)."""
        self._emotes.update(emotes)
        self._emote_re = self._build_emote_regex()

    def fetch_twitch_global_emotes(self, access_token: str, client_id: str) -> None:
        """
        Hit the Twitch Helix emotes endpoint and add all global emotes.
        Call once at startup with a valid app access token.
        """
        try:
            resp = requests.get(
                "https://api.twitch.tv/helix/chat/emotes/global",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Client-Id": client_id,
                },
                timeout=10,
            )
            resp.raise_for_status()
            names = {e["name"] for e in resp.json().get("data", [])}
            self.add_emotes(names)
        except Exception as exc:
            # Non-fatal — fall back to hardcoded set
            import logging
            logging.getLogger(__name__).warning(
                "Could not fetch live Twitch emotes: %s", exc
            )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _build_emote_regex(self) -> re.Pattern:
        """
        Compile a single alternation regex for all known emotes.
        Uses word boundaries to avoid matching substrings.
        Sorted by length descending so longer emotes match first.
        """
        sorted_emotes = sorted(self._emotes, key=len, reverse=True)
        pattern = r"\b(?:" + "|".join(re.escape(e) for e in sorted_emotes) + r")\b"
        return re.compile(pattern)

    @staticmethod
    def _remove_emoji(text: str) -> str:
        """Remove Unicode emoji and pictographic characters."""
        return "".join(
            ch for ch in text
            if not (
                unicodedata.category(ch) in ("So", "Sm")
                or 0x1F300 <= ord(ch) <= 0x1FAFF
                or 0x2600  <= ord(ch) <= 0x27BF
                or 0xFE00  <= ord(ch) <= 0xFE0F
            )
        )


# ── Spark UDF wrapper ─────────────────────────────────────────────────────────

def make_clean_udf():
    """
    Returns a Spark UDF backed by a broadcast-friendly EmoteCleaner instance.
    Register with: spark.udf.register("clean_text", make_clean_udf())
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    cleaner = EmoteCleaner()

    @udf(returnType=StringType())
    def clean_text_udf(text: str) -> str:
        if text is None:
            return None
        return cleaner.clean(text)

    return clean_text_udf
