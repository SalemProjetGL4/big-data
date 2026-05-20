#!/usr/bin/env bash
# Creates Kafka topics for the social media pipeline.
# Run this once after docker-compose up.

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka:29092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION="${REPLICATION_FACTOR:-1}"    # bump to 3 in production
RETENTION_MS="${RETENTION_MS:-172800000}" # 48 hours

TOPICS=(
  "youtube-events"
  "youtube-comments"
  "twitch-events"
  "twitch-chat"
)

echo "Creating Kafka topics on $BOOTSTRAP ..."

for TOPIC in "${TOPICS[@]}"; do
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION" \
    --config retention.ms="$RETENTION_MS" \
    --config compression.type=lz4

  echo "  ✓ $TOPIC"
done

echo ""
echo "Topic list:"
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP" \
  --list
