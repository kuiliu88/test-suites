
#!/usr/bin/env bash
set -uo pipefail

CONTAINER="${CONTAINER:-timeplusd}"
INTERVAL="${INTERVAL:-10}"
OUTPUT="${OUTPUT:-timeplusd_resources.csv}"
ERROR_LOG="${ERROR_LOG:-timeplusd_monitor_errors.log}"

# Create CSV header if the file does not exist or is empty.
if [[ ! -s "$OUTPUT" ]]; then
    echo 'timestamp,container,container_id,status,cpu_percent,memory_usage,memory_percent,network_io,block_io,pids' > "$OUTPUT"
fi

csv_quote() {
    local value="${1//\"/\"\"}"
    printf '"%s"' "$value"
}

echo "Monitoring container: $CONTAINER"
echo "Interval: ${INTERVAL}s"
echo "CSV output: $OUTPUT"
echo "Error log: $ERROR_LOG"

while true; do
    TIMESTAMP=$(date '+%Y-%m-%dT%H:%M:%S%z')

    INSPECT=$(docker inspect \
        --format '{{.Id}}|{{.State.Status}}' \
        "$CONTAINER" 2>> "$ERROR_LOG") || {
        echo "$TIMESTAMP ERROR: Cannot inspect container $CONTAINER" >> "$ERROR_LOG"
        sleep "$INTERVAL"
        continue
    }

    CONTAINER_ID="${INSPECT%%|*}"
    STATUS="${INSPECT#*|}"

    STATS=$(docker stats --no-stream \
        --format '{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' \
        "$CONTAINER" 2>> "$ERROR_LOG") || {
        echo "$TIMESTAMP ERROR: Cannot collect docker stats" >> "$ERROR_LOG"
        sleep "$INTERVAL"
        continue
    }

    IFS=',' read -r CPU MEM_USAGE MEM_PERCENT NET_IO BLOCK_IO PIDS <<< "$STATS"

    {
        csv_quote "$TIMESTAMP"; printf ','
        csv_quote "$CONTAINER"; printf ','
        csv_quote "$CONTAINER_ID"; printf ','
        csv_quote "$STATUS"; printf ','
        csv_quote "$CPU"; printf ','
        csv_quote "$MEM_USAGE"; printf ','
        csv_quote "$MEM_PERCENT"; printf ','
        csv_quote "$NET_IO"; printf ','
        csv_quote "$BLOCK_IO"; printf ','
        csv_quote "$PIDS"; printf '\n'
    } >> "$OUTPUT"

    sleep "$INTERVAL"
done

