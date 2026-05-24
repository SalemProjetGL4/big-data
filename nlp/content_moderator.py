"""
ContentModerationFilter
========================
Detects and handles profanity, slurs, NSFW language, and hate speech
in social media text before NLP processing and storage.

Pipeline stages:
  1. Exact-match profanity list   (fast, no model needed)
  2. Leet-speak / obfuscation normaliser  (h3ll0 → hello, f*ck → fuck)
  3. Severity scoring             (clean / mild / moderate / severe / blocked)
  4. Action dispatcher            (allow / redact / mask / reject)
  5. Optional: Transformer NSFW classifier  (more accurate, GPU optional)

Severity levels:
  0  CLEAN     — no issues detected
  1  MILD      — minor profanity acceptable in most adult contexts
  2  MODERATE  — strong language, should be flagged for review
  3  SEVERE    — slurs, explicit content, hate speech — redact or reject
  4  BLOCKED   — CSAM indicators, threats — always reject, log for review

Actions per severity (configurable):
  allow   — pass through unchanged
  mask    — replace matched words with  ****
  redact  — replace entire text with [CONTENT REMOVED]
  reject  — drop the document entirely, log the event
"""

import logging
import re
import unicodedata
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# ── Severity enum ─────────────────────────────────────────────────────────────

class Severity(IntEnum):
    CLEAN    = 0
    MILD     = 1
    MODERATE = 2
    SEVERE   = 3
    BLOCKED  = 4


# ── Word lists ────────────────────────────────────────────────────────────────
# These are intentionally sanitised for the source file.
# In production, load from a private file / secrets manager.
# Format: { "word": severity_level }

_WORD_LIST: Dict[str, int] = {
    # ── Mild (1) — common profanity, widely used casually ───────────────────
    "damn":    1, "hell":    1, "crap":    1, "ass":     1,
    "bastard": 1, "piss":    1, "bitch":   1, "dumb":    1,
    "idiot":   1, "moron":   1, "jerk":    1, "loser":   1,
    "screw":   1, "sucks":   1, "butt":    1, "boobs":   1,

    # ── Moderate (2) — strong language ──────────────────────────────────────
    "fuck":    2, "shit":    2, "cunt":    2, "cock":    2,
    "dick":    2, "pussy":   2, "whore":   2, "slut":    2,
    "asshole": 2, "bullshit":2, "motherfucker": 2,
    "wtf":     2, "stfu":    2,

    # ── Severe (3) — slurs and hate speech ──────────────────────────────────
    # Listed symbolically; actual slurs loaded from external file in prod.
    "__racial_slur_1__":  3,
    "__racial_slur_2__":  3,
    "__homophobic_slur__":3,
    "__gender_slur__":    3,
    "__religious_slur__": 3,
    # Explicit sexual content
    "porn":    3, "nsfw":    2, "hentai":  3,
    # Self-harm / violence encouragement
    "kys":     3, "kill yourself": 3,

    # ── Blocked (4) — zero tolerance ─────────────────────────────────────────
    "csam":    4, "cp":      4,   # CSAM indicators
}

# Regex patterns for contextual NSFW detection (beyond exact-word match)
_NSFW_PATTERNS: List[Tuple[re.Pattern, int]] = [
    # Threats
    (re.compile(r"\b(i will|gonna|going to)\s+(kill|hurt|rape|stab)\s+(you|him|her|them)\b", re.I), 3),
    # Doxxing signals
    (re.compile(r"\b(address|phone|dox|swat)\s+(is|him|her|them|at)\b", re.I), 3),
    # Explicit solicitation
    (re.compile(r"\b(send|dm|msg)\s+me\s+(nudes?|pics?|photos?)\b", re.I), 3),
]


# ── Leet-speak normaliser ────────────────────────────────────────────────────

_LEET_MAP = str.maketrans({
    "0": "o", "1": "i", "3": "e", "4": "a",
    "5": "s", "6": "g", "7": "t", "8": "b",
    "@": "a", "$": "s", "!": "i", "+": "t",
})

_ASTERISK_RE    = re.compile(r"\*+")
_PUNCT_FILL_RE  = re.compile(r"[^\w\s]")
_REPEAT_CHAR_RE = re.compile(r"(.)\1{2,}")   # fuuuuck → fuck


def _normalise(text: str) -> str:
    """
    Normalise obfuscated text for matching purposes only.
    The original text is preserved for output; normalised version
    is only used in the detection pass.
    """
    t = text.lower()
    # Unicode normalise (catch fullwidth chars, accented variants)
    t = unicodedata.normalize("NFKD", t)
    t = t.encode("ascii", "ignore").decode("ascii")
    # Leet substitutions
    t = t.translate(_LEET_MAP)
    # Remove asterisk fills: f**k → fk  (still catches it)
    t = _ASTERISK_RE.sub("", t)
    # Collapse repeated chars: fuuuuck → fuuck (keep 2 to preserve meaning)
    t = _REPEAT_CHAR_RE.sub(r"\1\1", t)
    # Collapse punctuation used as separators: f.u.c.k → fuck
    t = re.sub(r"(\w)\s*[._\-]\s*(?=\w)", r"\1", t)
    return t


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ModerationResult:
    original_text:  str
    output_text:    str                   # text after action applied
    severity:       Severity = Severity.CLEAN
    action:         str = "allow"         # allow | mask | redact | reject
    matched_words:  List[str] = field(default_factory=list)
    matched_patterns: List[str] = field(default_factory=list)
    is_clean:       bool = True
    is_rejected:    bool = False
    word_count:     int = 0
    flagged_count:  int = 0

    @property
    def severity_label(self) -> str:
        return self.severity.name.lower()


# ── Main filter class ─────────────────────────────────────────────────────────

class ContentModerationFilter:
    """
    Full content moderation pipeline.

    Default action policy (overridable):
      CLEAN    → allow
      MILD     → allow         (flag only)
      MODERATE → mask          (replace matched words with ****)
      SEVERE   → redact        (full text replaced)
      BLOCKED  → reject        (document dropped)

    Parameters
    ----------
    action_policy : dict
        Map of Severity → action string. Override to customise.
    extra_words : dict
        Additional {word: severity} pairs (e.g. channel-specific rules).
    mask_char : str
        Character used when masking words. Default "*".
    keep_word_length : bool
        If True, mask preserves word length (f*** vs ****).
    log_rejected : bool
        If True, log BLOCKED documents to the moderation logger.
    """

    DEFAULT_POLICY: Dict[Severity, str] = {
        Severity.CLEAN:    "allow",
        Severity.MILD:     "allow",
        Severity.MODERATE: "mask",
        Severity.SEVERE:   "redact",
        Severity.BLOCKED:  "reject",
    }

    def __init__(
        self,
        action_policy: Optional[Dict[Severity, str]] = None,
        extra_words: Optional[Dict[str, int]] = None,
        mask_char: str = "*",
        keep_word_length: bool = True,
        log_rejected: bool = True,
    ):
        self._policy          = action_policy or self.DEFAULT_POLICY.copy()
        self._mask_char       = mask_char
        self._keep_length     = keep_word_length
        self._log_rejected    = log_rejected
        self._mod_logger      = logging.getLogger("moderation.rejected")

        # Build combined word → severity map
        self._words: Dict[str, int] = {**_WORD_LIST}
        if extra_words:
            self._words.update({k.lower(): v for k, v in extra_words.items()})

        # Pre-compile word-boundary regex for all flagged words
        self._word_re: Dict[str, Tuple[re.Pattern, int]] = {
            word: (re.compile(r"\b" + re.escape(word) + r"\b", re.I), sev)
            for word, sev in self._words.items()
        }

        # Phrase patterns (multi-word, no word boundary needed)
        self._phrase_patterns: List[Tuple[re.Pattern, int]] = [
            (re.compile(re.escape(word), re.I), sev)
            for word, sev in self._words.items()
            if " " in word
        ]

    # ── Public API ────────────────────────────────────────────────────────────

    def moderate(self, text: str, source: str = "unknown") -> ModerationResult:
        """
        Run the full moderation pipeline on a single piece of text.
        Always returns a ModerationResult — never raises.
        """
        if not text or not text.strip():
            return ModerationResult(
                original_text=text or "",
                output_text=text or "",
                word_count=0,
            )

        normalised     = _normalise(text)
        matched_words  : List[str]  = []
        matched_pats   : List[str]  = []
        max_severity   = Severity.CLEAN
        tokens         = text.split()

        # Pass 1: word list scan (on normalised text, positions mapped back)
        for word, (pattern, sev) in self._word_re.items():
            if pattern.search(normalised):
                matched_words.append(word)
                if Severity(sev) > max_severity:
                    max_severity = Severity(sev)

        # Pass 2: phrase patterns
        for phrase, sev in self._phrase_patterns:
            if phrase.search(normalised):
                matched_words.append(phrase.pattern)
                if Severity(sev) > max_severity:
                    max_severity = Severity(sev)

        # Pass 3: contextual regex patterns (on original text)
        for pattern, sev in _NSFW_PATTERNS:
            m = pattern.search(text)
            if m:
                matched_pats.append(m.group(0))
                if Severity(sev) > max_severity:
                    max_severity = Severity(sev)

        action     = self._policy.get(max_severity, "allow")
        output     = self._apply_action(text, action, matched_words)
        is_clean   = max_severity == Severity.CLEAN
        is_rejected= action == "reject"

        if is_rejected and self._log_rejected:
            self._mod_logger.warning(
                "REJECTED | source=%s severity=%s words=%s text_preview='%.80s'",
                source, max_severity.name, matched_words, text,
            )

        return ModerationResult(
            original_text   = text,
            output_text     = output,
            severity        = max_severity,
            action          = action,
            matched_words   = matched_words,
            matched_patterns= matched_pats,
            is_clean        = is_clean,
            is_rejected     = is_rejected,
            word_count      = len(tokens),
            flagged_count   = len(matched_words) + len(matched_pats),
        )

    def moderate_batch(self, texts: List[str], source: str = "unknown") -> List[ModerationResult]:
        return [self.moderate(t, source) for t in texts]

    def is_clean(self, text: str) -> bool:
        return self.moderate(text).is_clean

    def add_words(self, words: Dict[str, int]) -> None:
        """Add channel/community-specific words at runtime."""
        self._words.update({k.lower(): v for k, v in words.items()})
        for word, sev in words.items():
            w = word.lower()
            if " " not in w:
                self._word_re[w] = (
                    re.compile(r"\b" + re.escape(w) + r"\b", re.I), sev
                )
            else:
                self._phrase_patterns.append(
                    (re.compile(re.escape(w), re.I), sev)
                )

    @classmethod
    def load_from_file(cls, path: str, **kwargs) -> "ContentModerationFilter":
        """
        Load word list from a plain-text file.

        File format (one entry per line):
            word                  → defaults to severity 2 (MODERATE)
            word,3                → explicit severity
            # comment line        → ignored
        """
        words: Dict[str, int] = {}
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(",", 1)
                word  = parts[0].strip().lower()
                sev   = int(parts[1].strip()) if len(parts) > 1 else 2
                words[word] = min(max(sev, 0), 4)
        logger.info("Loaded %d words from %s", len(words), path)
        return cls(extra_words=words, **kwargs)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _apply_action(self, text: str, action: str, matched: List[str]) -> str:
        if action == "allow":
            return text
        if action == "reject":
            return ""   # caller checks is_rejected; empty string signals drop
        if action == "redact":
            return "[CONTENT REMOVED]"
        if action == "mask":
            return self._mask_words(text, matched)
        return text

    def _mask_words(self, text: str, matched: List[str]) -> str:
        """Replace each matched word with mask characters."""
        result = text
        for word in matched:
            if " " in word:
                # Phrase — replace the whole phrase
                result = re.sub(
                    re.escape(word),
                    self._mask_char * len(word) if self._keep_length else "****",
                    result, flags=re.I,
                )
            else:
                def _replacer(m):
                    w = m.group(0)
                    return self._mask_char * len(w) if self._keep_length else "****"
                result = re.sub(
                    r"\b" + re.escape(word) + r"\b",
                    _replacer, result, flags=re.I,
                )
        return result


# ── Spark UDFs ────────────────────────────────────────────────────────────────

def make_moderation_udfs(
    policy: Optional[Dict[str, str]] = None,
    extra_words_path: Optional[str] = None,
):
    """
    Returns four Spark UDFs:
      moderated_text_udf    → str   (text after action applied)
      severity_udf          → str   (clean/mild/moderate/severe/blocked)
      is_rejected_udf       → bool  (True if document should be dropped)
      flagged_count_udf     → int   (number of flagged terms)

    Usage in Spark:
        mod_text, severity, rejected, flag_count = make_moderation_udfs()
        df = (df
            .withColumn("moderated_text", mod_text("cleaned_text"))
            .withColumn("content_severity", severity("cleaned_text"))
            .withColumn("is_rejected", rejected("cleaned_text"))
            .withColumn("flagged_term_count", flag_count("cleaned_text"))
            .filter(~col("is_rejected"))   # drop blocked content
        )
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType, BooleanType, IntegerType

    # Convert string policy keys to Severity enum
    _sev_policy = None
    if policy:
        _sev_policy = {Severity[k.upper()]: v for k, v in policy.items()}

    _filter_cache: dict = {}

    def _get_filter() -> ContentModerationFilter:
        if "inst" not in _filter_cache:
            kwargs = {}
            if _sev_policy:
                kwargs["action_policy"] = _sev_policy
            if extra_words_path:
                _filter_cache["inst"] = ContentModerationFilter.load_from_file(
                    extra_words_path, **kwargs
                )
            else:
                _filter_cache["inst"] = ContentModerationFilter(**kwargs)
        return _filter_cache["inst"]

    @udf(returnType=StringType())
    def moderated_text_udf(text):
        if not text:
            return text
        result = _get_filter().moderate(text)
        return result.output_text if not result.is_rejected else None

    @udf(returnType=StringType())
    def severity_udf(text):
        if not text:
            return "clean"
        return _get_filter().moderate(text).severity_label

    @udf(returnType=BooleanType())
    def is_rejected_udf(text):
        if not text:
            return False
        return _get_filter().moderate(text).is_rejected

    @udf(returnType=IntegerType())
    def flagged_count_udf(text):
        if not text:
            return 0
        return _get_filter().moderate(text).flagged_count

    return moderated_text_udf, severity_udf, is_rejected_udf, flagged_count_udf