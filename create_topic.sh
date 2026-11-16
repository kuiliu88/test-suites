
#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Create Kafka/Redpanda Topic
# Execute rpk inside broker1 container
# ============================================================

CONTAINER="${CONTAINER:-broker1}"
TOPIC="${TOPIC:-proton_window_test_events}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"

# Redpanda broker address inside the container
BROKERS="${BROKERS:-localhost:9092}"

echo "=========================================="
echo "Creating Redpanda topic"
echo "=========================================="
echo "Container:          $CONTAINER"
echo "Broker:             $BROKERS"
echo "Topic:              $TOPIC"
echo "Partitions:         $PARTITIONS"
echo "Replication factor: $REPLICATION_FACTOR"
echo

# Check Docker availability
if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: Docker is not installed."
    exit 1
fi

# Check container status
if [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || true)" != "true" ]]; then
    echo "ERROR: Container '$CONTAINER' is not running."
    exit 1
fi

# Check rpk availability inside container
if ! docker exec "$CONTAINER" sh -c 'command -v rpk >/dev/null 2>&1'; then
    echo "ERROR: rpk is not installed inside container '$CONTAINER'."
    exit 1
fi

# Create topic (ignore already-exists errors)
docker exec "$CONTAINER" \
    rpk topic create "$TOPIC" \
    --brokers "$BROKERS" \
    --partitions "$PARTITIONS" \
    --replicas "$REPLICATION_FACTOR" \
    || {
        echo "Topic creation returned an error."
        echo "Checking whether the topic already exists..."

        docker exec "$CONTAINER" \
            rpk topic describe "$TOPIC" \
            --brokers "$BROKERS" || exit 1
    }

echo
echo "=========================================="
echo "Topic verification"
echo "=========================================="

docker exec "$CONTAINER" \
    rpk topic describe "$TOPIC" \
    --brokers "$BROKERS"

echo
echo "Topic '$TOPIC' is ready."

