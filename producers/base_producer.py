"""
Base Kafka producer — shared by all three source producers.

Features:
  - JSON serialization with UTC timestamps
  - Delivery confirmation callback
  - Exponential backoff on transient failures
  - Graceful shutdown on SIGTERM/SIGINT
"""
import json
import logging
import signal
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from confluent_kafka import Producer, KafkaException

from config import settings

logger = logging.getLogger(__name__)


def _serialize(obj: Any) -> str:
    """JSON serializer that handles datetimes."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class BaseProducer(ABC):
    """
    Abstract base for all Kafka producers.

    Subclasses must implement:
      - topic (property) → str
      - fetch_events()   → List[Dict]
      - run_loop()       → drives fetch → publish cycle
    """

    def __init__(self):
        cfg = settings.kafka
        self._producer = Producer(
            {
                "bootstrap.servers": cfg.bootstrap_servers,
                "acks": cfg.acks,
                "retries": cfg.retries,
                "retry.backoff.ms": cfg.retry_backoff_ms,
                "compression.type": cfg.compression_type,
                "linger.ms": cfg.linger_ms,
                "enable.idempotence": True,   # exactly-once producer semantics
                "client.id": f"{self.source_name}-producer",
            }
        )
        self._running = True
        self._published = 0
        self._errors = 0

        # Graceful shutdown on SIGTERM / SIGINT
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(
            "Producer initialised",
            extra={"source": self.source_name, "topic": self.topic},
        )

    # ── Abstract interface ───────────────────────────────────────────────────

    @property
    @abstractmethod
    def source_name(self) -> str:
        """e.g. 'youtube', 'twitch'"""

    @property
    @abstractmethod
    def topic(self) -> str:
        """Kafka topic to publish to."""

    @abstractmethod
    def fetch_events(self) -> list:
        """Fetch a batch of raw events from the source API."""

    @abstractmethod
    def run_loop(self) -> None:
        """Main polling loop — call self.publish() for each event."""

    # ── Publishing ───────────────────────────────────────────────────────────

    def publish(
        self,
        event: Dict[str, Any],
        key: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> None:
        """
        Serialize and publish a single event to Kafka.

        Wraps the payload in a standard envelope:
          {
            "pipeline_id": <uuid>,
            "source":      <source_name>,
            "ingested_at": <utc iso>,
            "payload":     <original event>
          }
        """
        envelope = {
            "pipeline_id": str(uuid.uuid4()),
            "source": self.source_name,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload": event,
        }
        value_bytes = json.dumps(envelope, default=_serialize).encode("utf-8")
        key_bytes = (key or str(uuid.uuid4())).encode("utf-8")

        self._produce_with_backoff(key_bytes, value_bytes, topic=topic or self.topic)

    def _produce_with_backoff(
        self,
        key: bytes,
        value: bytes,
        topic: str,
        max_retries: int = 3,
    ) -> None:
        delay = 0.5
        for attempt in range(1, max_retries + 1):
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=self._delivery_callback,
                )
                self._producer.poll(0)   # non-blocking flush of callbacks
                return
            except BufferError:
                # Local queue is full — flush and retry
                logger.warning("Producer buffer full, flushing (attempt %d)", attempt)
                self._producer.flush(timeout=5)
            except KafkaException as exc:
                logger.error("Kafka error on attempt %d: %s", attempt, exc)
                if attempt == max_retries:
                    self._errors += 1
                    raise
                time.sleep(delay)
                delay *= 2

    def _delivery_callback(self, err, msg) -> None:
        if err:
            logger.error(
                "Delivery failed | topic=%s partition=%s error=%s",
                msg.topic(),
                msg.partition(),
                err,
            )
            self._errors += 1
        else:
            self._published += 1
            if self._published % 100 == 0:
                logger.info(
                    "Published %d messages to %s (errors: %d)",
                    self._published,
                    self.topic,
                    self._errors,
                )

    def flush(self, timeout: float = 10.0) -> None:
        """Block until all in-flight messages are delivered."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining:
            logger.warning("%d messages were NOT delivered before flush timeout", remaining)

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def _shutdown(self, signum, frame) -> None:
        logger.info("Shutdown signal received, stopping %s producer…", self.source_name)
        self._running = False

    def start(self) -> None:
        """Entry point — starts the polling loop and flushes on exit."""
        try:
            self.run_loop()
        finally:
            logger.info("Flushing final messages for %s…", self.source_name)
            self.flush()
            logger.info(
                "Producer stopped | published=%d errors=%d",
                self._published,
                self._errors,
            )
