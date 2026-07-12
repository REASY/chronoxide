#!/usr/bin/env bash

set -euo pipefail
export LC_ALL=C

OUTPUT_DIR="${OUTPUT_DIR:-}"
if [[ -z "$OUTPUT_DIR" || "$OUTPUT_DIR" != /* || -e "$OUTPUT_DIR" ]]; then
    echo "OUTPUT_DIR must be a new absolute path" >&2
    exit 2
fi
for command in curl jq comm; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command is missing: $command" >&2
        exit 2
    fi
done

mkdir -p "$OUTPUT_DIR"
prometheus="http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1"
greptime="http://127.0.0.1:${GREPTIME_PORT:-4000}/v1/prometheus/api/v1"

curl --fail --silent --show-error "$prometheus/label/__name__/values" \
    | tee "$OUTPUT_DIR/prometheus-metrics.json" \
    | jq -r '.data[]' | sort -u >"$OUTPUT_DIR/prometheus-metrics.txt"
curl --fail --silent --show-error "$greptime/label/__name__/values?db=public" \
    | tee "$OUTPUT_DIR/greptime-metrics.json" \
    | jq -r '.data[]' | sort -u >"$OUTPUT_DIR/greptime-metrics.txt"
curl --fail --silent --show-error "$prometheus/labels" \
    | tee "$OUTPUT_DIR/prometheus-labels.json" \
    | jq -r '.data[]' | sort -u >"$OUTPUT_DIR/prometheus-labels.txt"
curl --fail --silent --show-error "$greptime/labels?db=public" \
    | tee "$OUTPUT_DIR/greptime-labels.json" \
    | jq -r '.data[]' | sort -u >"$OUTPUT_DIR/greptime-labels.txt"

comm -12 "$OUTPUT_DIR/prometheus-metrics.txt" "$OUTPUT_DIR/greptime-metrics.txt" \
    >"$OUTPUT_DIR/common-metrics.txt"
comm -12 "$OUTPUT_DIR/prometheus-labels.txt" "$OUTPUT_DIR/greptime-labels.txt" \
    >"$OUTPUT_DIR/common-labels.txt"

echo "metric and label inventories: $OUTPUT_DIR"
echo "verify the translated names in queries.example.json before benchmarking"
