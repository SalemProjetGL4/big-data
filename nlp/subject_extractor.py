"""
Subject extractor — identifies the main topics and named entities
from cleaned social media text.

Three extraction strategies:
  1. spaCy NER  — named entities (people, games, orgs, products)
  2. Noun chunks — grammatical subject/object phrases
  3. KeyBERT    — keyword extraction via sentence embeddings (optional)

Output per document:
  subjects      : List[str]   — top noun phrases / entities
  entities      : List[dict]  — {text, label} named entity dicts
  primary_subject: str        — single most salient subject
  topics        : List[str]   — normalized topic tags
"""
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Domain-specific topic normalisation map ───────────────────────────────────
# Maps surface forms → canonical topic tags for analytics grouping

TOPIC_NORMALISER: Dict[str, str] = {
    # Games (expand for your use case)
    "fortnite": "gaming:fortnite",
    "minecraft": "gaming:minecraft",
    "valorant": "gaming:valorant",
    "league of legends": "gaming:league_of_legends",
    "lol": "gaming:league_of_legends",
    "dota": "gaming:dota2",
    "dota 2": "gaming:dota2",
    "apex": "gaming:apex_legends",
    "cs2": "gaming:cs2",
    "csgo": "gaming:cs2",
    "overwatch": "gaming:overwatch",
    "elden ring": "gaming:elden_ring",
    "gta": "gaming:gta",
    "rust": "gaming:rust",
    # Content categories
    "speedrun": "content:speedrun",
    "irl": "content:irl",
    "just chatting": "content:just_chatting",
    "art": "content:art",
    "music": "content:music",
    # Sentiment signals (remove from subjects, handled by sentiment layer)
    "lmao": None,
    "lol": None,
    "omg": None,
}

# Tokens to always exclude from subjects
STOPWORDS_EXTRA = {
    "stream", "streamer", "chat", "viewers", "guys", "everyone",
    "people", "thing", "things", "way", "lot", "time", "day",
    "video", "clip", "highlight", "sub", "subs",
    "twitch", "youtube",
}


@dataclass
class ExtractionResult:
    primary_subject: Optional[str]
    subjects: List[str] = field(default_factory=list)
    entities: List[Dict[str, str]] = field(default_factory=list)
    topics: List[str] = field(default_factory=list)


class SubjectExtractor:
    """
    Extracts subjects and named entities using spaCy.
    Loads the model once per process (expensive to initialise).

    Requires: pip install spacy && python -m spacy download en_core_web_sm
    For better accuracy: python -m spacy download en_core_web_trf
    """

    # Entity types we care about for social media analytics
    RELEVANT_ENT_TYPES = {
        "PERSON", "ORG", "PRODUCT", "WORK_OF_ART", "EVENT",
        "GPE",   # geopolitical (countries, cities)
        "LOC",   # non-GPE locations
        "NORP",  # nationalities, groups
    }

    def __init__(self, model: str = "en_core_web_sm", max_subjects: int = 5):
        try:
            import spacy
            self._nlp = spacy.load(model)
            # Add sentencizer for basic sentence boundary detection
            if "sentencizer" not in self._nlp.pipe_names:
                self._nlp.add_pipe("sentencizer")
            self._max_subjects = max_subjects
            logger.info("spaCy model '%s' loaded", model)
        except ImportError:
            raise ImportError("Install spacy: pip install spacy")
        except OSError:
            raise OSError(
                f"spaCy model '{model}' not found. "
                f"Run: python -m spacy download {model}"
            )

    def extract(self, text: str) -> "ExtractionResult":
        if not text or not text.strip():
            return ExtractionResult(primary_subject=None)

        doc = self._nlp(text[:512])  # cap at 512 chars for speed

        # 1. Named entities
        entities = [
            {"text": ent.text.strip(), "label": ent.label_}
            for ent in doc.ents
            if ent.label_ in self.RELEVANT_ENT_TYPES
            and len(ent.text.strip()) > 1
        ]

        # 2. Noun chunks as candidate subjects
        noun_chunks = []
        if "parser" in self._nlp.pipe_names:
            noun_chunks = [
                chunk.text.strip().lower()
                for chunk in doc.noun_chunks
                if len(chunk.text.strip()) > 2
                and chunk.text.strip().lower() not in STOPWORDS_EXTRA
            ]

        # 3. Merge and deduplicate
        all_candidates = list({
            ent["text"].lower() for ent in entities
        } | set(noun_chunks))

        # 4. Filter out noise
        filtered = [
            s for s in all_candidates
            if len(s) > 2
            and not re.fullmatch(r"[\d\s\W]+", s)
            and s not in STOPWORDS_EXTRA
        ]

        # 5. Normalise to topic tags
        topics = []
        for candidate in filtered:
            tag = TOPIC_NORMALISER.get(candidate.lower())
            if tag is not None:
                topics.append(tag)

        # 6. Rank subjects: named entities first, then noun chunks
        entity_texts = [e["text"].lower() for e in entities]
        ranked = sorted(
            filtered,
            key=lambda s: (0 if s in entity_texts else 1, len(s)),
        )[: self._max_subjects]

        primary = ranked[0] if ranked else None

        return ExtractionResult(
            primary_subject=primary,
            subjects=ranked,
            entities=entities,
            topics=list(set(topics)),
        )

    def extract_batch(self, texts: List[str]) -> List["ExtractionResult"]:
        """Use spaCy's nlp.pipe() for efficient batch processing."""
        capped = [t[:512] if t else "" for t in texts]
        results = []
        for doc in self._nlp.pipe(capped, batch_size=64):
            entities = [
                {"text": ent.text.strip(), "label": ent.label_}
                for ent in doc.ents
                if ent.label_ in self.RELEVANT_ENT_TYPES
                and len(ent.text.strip()) > 1
            ]
            noun_chunks = []
            if "parser" in self._nlp.pipe_names:
                noun_chunks = [
                    chunk.text.strip().lower()
                    for chunk in doc.noun_chunks
                    if len(chunk.text.strip()) > 2
                    and chunk.text.strip().lower() not in STOPWORDS_EXTRA
                ]
            all_candidates = list({e["text"].lower() for e in entities} | set(noun_chunks))
            filtered = [
                s for s in all_candidates
                if len(s) > 2 and not re.fullmatch(r"[\d\s\W]+", s)
                and s not in STOPWORDS_EXTRA
            ][: self._max_subjects]
            topics = [
                TOPIC_NORMALISER[s.lower()]
                for s in filtered
                if s.lower() in TOPIC_NORMALISER and TOPIC_NORMALISER[s.lower()] is not None
            ]
            primary = filtered[0] if filtered else None
            results.append(ExtractionResult(
                primary_subject=primary,
                subjects=filtered,
                entities=entities,
                topics=list(set(topics)),
            ))
        return results


# ── Spark UDFs ────────────────────────────────────────────────────────────────

def make_subject_udfs(model: str = "en_core_web_sm"):
    """
    Returns Spark UDFs for subject extraction.
    The spaCy model is loaded once per executor via a lazy singleton.

    Usage:
        primary_udf, subjects_udf, topics_udf = make_subject_udfs()
        df = df.withColumn("primary_subject", primary_udf("cleaned_text"))
               .withColumn("subjects", subjects_udf("cleaned_text"))
               .withColumn("topics", topics_udf("cleaned_text"))
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType, ArrayType

    _extractor_cache = {}

    def _get_extractor():
        if "inst" not in _extractor_cache:
            _extractor_cache["inst"] = SubjectExtractor(model=model)
        return _extractor_cache["inst"]

    @udf(returnType=StringType())
    def primary_subject_udf(text):
        if not text:
            return None
        return _get_extractor().extract(text).primary_subject

    @udf(returnType=ArrayType(StringType()))
    def subjects_udf(text):
        if not text:
            return []
        return _get_extractor().extract(text).subjects

    @udf(returnType=ArrayType(StringType()))
    def topics_udf(text):
        if not text:
            return []
        return _get_extractor().extract(text).topics

    return primary_subject_udf, subjects_udf, topics_udf
