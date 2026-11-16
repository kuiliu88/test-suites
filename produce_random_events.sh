
#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Proton Streaming Window Memory Stability Benchmark
# Kafka / Redpanda Random Event Producer
# ============================================================

# Kafka configuration
BROKERS="${BROKERS:-localhost:9092}"
TOPIC="${TOPIC:-proton_window_test_events}"

# Benchmark configuration
DURATION_HOURS="${DURATION_HOURS:-24}"
PROFILE="${PROFILE:-mixed}"

# Events per minute for sparse / continuous profiles
MIN_EVENTS="${MIN_EVENTS:-1}"
MAX_EVENTS="${MAX_EVENTS:-5}"

# Number of possible device IDs
DEVICE_COUNT="${DEVICE_COUNT:-100}"

# Number of possible metric names
METRIC_COUNT="${METRIC_COUNT:-5}"

# Maximum event-time lateness in seconds
# 0 = no intentional out-of-order timestamps
MAX_LATENESS_SECONDS="${MAX_LATENESS_SECONDS:-0}"

# Burst configuration
BURST_MAX_EVENTS="${BURST_MAX_EVENTS:-20}"
BURST_SPAN_SECONDS="${BURST_SPAN_SECONDS:-30}"

# Reproducibility
SEED="${SEED:-20261116}"

# Logging
LOG_EVERY="${LOG_EVERY:-100}"

# ============================================================
# Validation
# ============================================================

if ! python3 -c "import confluent_kafka" >/dev/null 2>&1; then
    echo "ERROR: confluent-kafka is not installed."
    echo "Run:"
    echo "  python3 -m venv .venv"
    echo "  source .venv/bin/activate"
    echo "  pip install confluent-kafka"
    exit 1
fi

if [[ ! "$DURATION_HOURS" =~ ^[0-9]+([.][0-9]+)?$ ]] ||
   ! python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) > 0 else 1)" "$DURATION_HOURS"; then
    echo "ERROR: DURATION_HOURS must be positive."
    exit 1
fi

if [[ "$PROFILE" != "sparse" &&
      "$PROFILE" != "continuous" &&
      "$PROFILE" != "bursty" &&
      "$PROFILE" != "mixed" ]]; then
    echo "ERROR: PROFILE must be sparse, continuous, bursty, or mixed."
    exit 1
fi

export BROKERS TOPIC DURATION_HOURS PROFILE
export MIN_EVENTS MAX_EVENTS DEVICE_COUNT METRIC_COUNT
export MAX_LATENESS_SECONDS BURST_MAX_EVENTS BURST_SPAN_SECONDS
export SEED LOG_EVERY

# ============================================================
# Producer
# ============================================================

python3 - <<'PY'
import os
import sys
import json
import time
import random
import signal
from datetime import datetime, timezone
from confluent_kafka import Producer

BROKERS = os.environ["BROKERS"]
TOPIC = os.environ["TOPIC"]

DURATION = float(os.environ["DURATION_HOURS"]) * 3600
PROFILE = os.environ["PROFILE"]

MIN_EVENTS = int(os.environ["MIN_EVENTS"])
MAX_EVENTS = int(os.environ["MAX_EVENTS"])

DEVICE_COUNT = int(os.environ["DEVICE_COUNT"])
METRIC_COUNT = int(os.environ["METRIC_COUNT"])

MAX_LATENESS = int(os.environ["MAX_LATENESS_SECONDS"])

BURST_MAX = int(os.environ["BURST_MAX_EVENTS"])
BURST_SPAN = int(os.environ["BURST_SPAN_SECONDS"])

SEED = int(os.environ["SEED"])
LOG_EVERY = int(os.environ["LOG_EVERY"])

if MIN_EVENTS < 0 or MAX_EVENTS < MIN_EVENTS:
    sys.exit("Invalid MIN_EVENTS / MAX_EVENTS")

if DEVICE_COUNT < 1 or METRIC_COUNT < 1:
    sys.exit("DEVICE_COUNT and METRIC_COUNT must be positive")

if MAX_LATENESS < 0 or BURST_MAX < 1 or not 1 <= BURST_SPAN <= 60:
    sys.exit("Invalid lateness or burst configuration")

rng = random.Random(SEED)

metrics = [
    "cpu_usage",
    "memory_usage",
    "request_latency",
    "network_bytes",
    "disk_usage",
    "queue_depth",
    "temperature",
    "error_count",
    "request_rate",
    "active_connections"
][:METRIC_COUNT]

producer = Producer({
    "bootstrap.servers": BROKERS,
    "client.id": "proton-window-benchmark",
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "lz4",
    "linger.ms": 100,
    "batch.num.messages": 100,
    "queue.buffering.max.messages": 10000
})

sent = 0
delivered = 0
failed = 0
running = True
event_number = 0

def delivery_callback(err, msg):
    global delivered, failed
    if err:
        failed += 1
        print(f"DELIVERY ERROR: {err}", flush=True)
    else:
        delivered += 1

def stop_handler(signum, frame):
    global running
    running = False
    print("\nShutdown requested. Flushing producer...", flush=True)

signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)

def make_event():
    global event_number

    event_number += 1

    now = datetime.now(timezone.utc)

    # Intentional event-time disorder.
    lateness = rng.uniform(0, MAX_LATENESS) if MAX_LATENESS else 0
    event_time = now.timestamp() - lateness

    # Proton-compatible DateTime64(3) timestamp.
    # Keep UTC; remove ISO 8601 T separator and trailing Z.
    timestamp = datetime.fromtimestamp(
        event_time, timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    return {
        "event_id": f"evt-{event_number:012d}",
        "event_time": timestamp,
        "device_id": f"device-{rng.randrange(DEVICE_COUNT):05d}",
        "metric_name": rng.choice(metrics),
        "metric_value": round(rng.uniform(0, 100), 4),
        "event_type": rng.choice([
            "metric", "metric", "metric", "status", "event"
        ])
    }

def events_for_minute():
    if PROFILE == "sparse":
        return rng.randint(MIN_EVENTS, MAX_EVENTS), 60

    if PROFILE == "continuous":
        return rng.randint(
            max(5, MIN_EVENTS), max(20, MAX_EVENTS)
        ), 60

    if PROFILE == "bursty":
        if rng.random() < 0.25:
            return 0, 60
        return rng.randint(1, BURST_MAX), BURST_SPAN

    # Mixed: sparse, continuous, burst, and idle periods.
    mode = rng.random()

    if mode < 0.30:
        return rng.randint(MIN_EVENTS, MAX_EVENTS), 60

    if mode < 0.55:
        return rng.randint(5, 20), 60

    if mode < 0.80:
        return rng.randint(1, BURST_MAX), BURST_SPAN

    return 0, 60

print("=" * 65)
print("Proton Window Benchmark - Random Kafka Event Producer")
print("=" * 65)
print(f"Broker:             {BROKERS}")
print(f"Topic:              {TOPIC}")
print(f"Profile:            {PROFILE}")
print(f"Duration:           {DURATION / 3600:.2f} hours")
print(f"Device cardinality: {DEVICE_COUNT}")
print(f"Metric cardinality: {METRIC_COUNT}")
print(f"Event-time lateness:{MAX_LATENESS}s")
print(f"Random seed:        {SEED}")
print("=" * 65)

start = time.monotonic()
end = start + DURATION
minute = 0

try:
    while running:
        minute_start = start + minute * 60

        if minute_start >= end:
            break

        count, span = events_for_minute()

        # Random arrival offsets within the minute.
        offsets = sorted(
            rng.uniform(0, min(span, max(0, end-minute_start)))
            for _ in range(count)
        )

        for offset in offsets:
            if not running:
                break

            target = minute_start + offset

            while running:
                remaining = target - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(remaining, 0.5))

            if not running or time.monotonic() >= end:
                break

            event = make_event()

            try:
                producer.produce(
                    TOPIC,
                    key=event["device_id"].encode("utf-8"),
                    value=json.dumps(
                        event, separators=(",", ":")
                    ).encode("utf-8"),
                    callback=delivery_callback
                )
                sent += 1
                producer.poll(0)

            except BufferError:
                producer.poll(1)
                producer.produce(
                    TOPIC,
                    key=event["device_id"].encode("utf-8"),
                    value=json.dumps(
                        event, separators=(",", ":")
                    ).encode("utf-8"),
                    callback=delivery_callback
                )
                sent += 1

            if sent % LOG_EVERY == 0:
                elapsed = time.monotonic() - start
                print(
                    f"[{elapsed/3600:.2f}h] "
                    f"sent={sent} delivered={delivered} "
                    f"failed={failed}",
                    flush=True
                )

        minute += 1

except KeyboardInterrupt:
    pass

finally:
    print("\nFlushing outstanding messages...", flush=True)
    remaining = producer.flush(30)

    elapsed = time.monotonic() - start

    print("\n" + "=" * 65)
    print("PRODUCER SUMMARY")
    print("=" * 65)
    print(f"Elapsed seconds: {elapsed:.1f}")
    print(f"Sent:            {sent}")
    print(f"Delivered:       {delivered}")
    print(f"Failed:          {failed}")
    print(f"Pending:         {remaining}")
    print("=" * 65)

    if remaining:
        sys.exit(1)

PY

