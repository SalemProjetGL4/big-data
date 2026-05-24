"""
Sentiment analyser for social media text.

Two backends — pick based on your throughput / accuracy trade-off:

  SentimentBackend.VADER      — rule-based, ~500k rows/s on a single core,
                                 great for short social media text, no GPU needed.

  SentimentBackend.TRANSFORMER — cardiffnlp/twitter-roberta-base-sentiment-latest
                                  fine-tuned on tweets/social text, ~1-5k rows/s
                                  on CPU, much faster with GPU. Best accuracy.

Both return a SentimentResult with:
  label    : "positive" | "negative" | "neutral"
  score    : float 0-1 (confidence)
  compound : float -1 to +1 (signed intensity, VADER native; derived for transformer)
"""
import enum
import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


class SentimentBackend(enum.Enum):
    VADER       = "vader"
    TRANSFORMER = "transformer"


@dataclass
class SentimentResult:
    label: str          # positive | negative | neutral
    score: float        # confidence 0-1
    compound: float     # signed intensity -1 to +1
    backend: str


# ── VADER backend ─────────────────────────────────────────────────────────────

class VADERSentimentAnalyser:
    """
    Uses NLTK VADER (Valence Aware Dictionary and sEntiment Reasoner).
    Best for short, informal text with slang and punctuation signals.
    Thread-safe — one instance per process is sufficient.
    """

    POSITIVE_THRESHOLD  =  0.05
    NEGATIVE_THRESHOLD  = -0.05

    def __init__(self):
        try:
            from nltk.sentiment.vader import SentimentIntensityAnalyzer
            import nltk
            nltk.download("vader_lexicon", quiet=True)
            self._sia = SentimentIntensityAnalyzer()
            # Extend with social media slang
            self._sia.lexicon.update({
                "gg":       2.0,
                "ggs":      2.0,
                "ez":      -1.0,   # often condescending
                "poggers":  2.5,
                "sadge":   -2.0,
                "based":    2.0,
                "cringe":  -2.0,
                "goated":   3.0,
                "slaps":    2.5,
                "bussin":   2.5,
                "ratio":   -1.5,
                "cope":    -1.5,
                "seethe":  -2.0,
                "rent free":-1.0,
                "W":        2.0,
                "L":       -2.0,
                "mid":     -1.0,
                "fire":     2.0,
            })
        except ImportError:
            raise ImportError("Install nltk: pip install nltk")

    def analyse(self, text: str) -> "SentimentResult":
        if not text or not text.strip():
            return SentimentResult("neutral", 0.0, 0.0, "vader")

        scores = self._sia.polarity_scores(text)
        compound = scores["compound"]

        if compound >= self.POSITIVE_THRESHOLD:
            label = "positive"
            score = scores["pos"]
        elif compound <= self.NEGATIVE_THRESHOLD:
            label = "negative"
            score = scores["neg"]
        else:
            label = "neutral"
            score = scores["neu"]

        return SentimentResult(label, round(score, 4), round(compound, 4), "vader")

    def analyse_batch(self, texts: List[str]) -> List["SentimentResult"]:
        return [self.analyse(t) for t in texts]


# ── Transformer backend ───────────────────────────────────────────────────────

class TransformerSentimentAnalyser:
    """
    Uses cardiffnlp/twitter-roberta-base-sentiment-latest via HuggingFace.
    Requires: pip install transformers torch  (or transformers[torch])

    Batched inference is used for throughput. Set batch_size based on
    your available memory (GPU: 64-128, CPU: 16-32).
    """

    MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    LABEL_MAP  = {
        "positive": "positive",
        "negative": "negative",
        "neutral":  "neutral",
        # model outputs
        "LABEL_0":  "negative",
        "LABEL_1":  "neutral",
        "LABEL_2":  "positive",
    }

    def __init__(self, batch_size: int = 32, device: Optional[str] = None):
        try:
            from transformers import pipeline as hf_pipeline
            import torch
            _device = device or ("cuda" if torch.cuda.is_available() else "cpu")
            logger.info("Loading sentiment model on %s …", _device)
            self._pipe = hf_pipeline(
                "sentiment-analysis",
                model=self.MODEL_NAME,
                tokenizer=self.MODEL_NAME,
                device=0 if _device == "cuda" else -1,
                truncation=True,
                max_length=128,
            )
            self._batch_size = batch_size
            logger.info("Transformer sentiment model ready")
        except ImportError:
            raise ImportError("Install: pip install transformers torch")

    def analyse(self, text: str) -> "SentimentResult":
        results = self.analyse_batch([text])
        return results[0]

    def analyse_batch(self, texts: List[str]) -> List["SentimentResult"]:
        clean_texts = [t if t and t.strip() else " " for t in texts]
        raw = self._pipe(clean_texts, batch_size=self._batch_size)
        results = []
        for r in raw:
            label = self.LABEL_MAP.get(r["label"].lower(), "neutral")
            score = round(r["score"], 4)
            # Derive compound: positive→+score, negative→-score, neutral→0
            if label == "positive":
                compound = score
            elif label == "negative":
                compound = -score
            else:
                compound = 0.0
            results.append(SentimentResult(label, score, compound, "transformer"))
        return results


# ── Unified analyser ──────────────────────────────────────────────────────────

class SentimentAnalyser:
    """
    Facade that picks the configured backend.
    Use VADER for maximum throughput (Spark workers, no GPU).
    Use TRANSFORMER for best accuracy (smaller enrichment job, GPU available).
    """

    def __init__(self, backend: SentimentBackend = SentimentBackend.VADER, **kwargs):
        if backend == SentimentBackend.VADER:
            self._impl = VADERSentimentAnalyser()
        else:
            self._impl = TransformerSentimentAnalyser(**kwargs)
        self._backend = backend

    def analyse(self, text: str) -> "SentimentResult":
        return self._impl.analyse(text)

    def analyse_batch(self, texts: List[str]) -> List["SentimentResult"]:
        return self._impl.analyse_batch(texts)


# ── Spark UDF ─────────────────────────────────────────────────────────────────

def make_sentiment_udf(backend: str = "vader"):
    """
    Returns three Spark UDFs: sentiment_label, sentiment_score, sentiment_compound.
    Instantiates the analyser once per executor via a closure.

    Usage:
        label_udf, score_udf, compound_udf = make_sentiment_udf()
        df = df.withColumn("sentiment", label_udf("cleaned_text"))
               .withColumn("sentiment_score", score_udf("cleaned_text"))
               .withColumn("compound", compound_udf("cleaned_text"))
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType, FloatType

    # Lazy init — one instance per executor process
    _analyser_cache = {}

    def _get_analyser():
        if "inst" not in _analyser_cache:
            _analyser_cache["inst"] = SentimentAnalyser(
                SentimentBackend(backend)
            )
        return _analyser_cache["inst"]

    @udf(returnType=StringType())
    def label_udf(text):
        if not text:
            return "neutral"
        return _get_analyser().analyse(text).label

    @udf(returnType=FloatType())
    def score_udf(text):
        if not text:
            return 0.0
        return float(_get_analyser().analyse(text).score)

    @udf(returnType=FloatType())
    def compound_udf(text):
        if not text:
            return 0.0
        return float(_get_analyser().analyse(text).compound)

    return label_udf, score_udf, compound_udf
