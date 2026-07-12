#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ROOT="${RUN_ROOT:?RUN_ROOT is required}"
QUERY_RESULT_DIR="${QUERY_RESULT_DIR:?QUERY_RESULT_DIR is required}"
LOG="$RUN_ROOT/pipeline.log"

exec > >(tee -a "$LOG") 2>&1

echo "pipeline_started_at=$(date --iso-8601=seconds)"
RESULT_DIR="$RUN_ROOT" TARGET=greptime BUILD=0 \
    MAX_BATCH_BYTES=16777216 MAX_BATCH_MESSAGES=2048 \
    "$SCRIPT_DIR/replay_capture.sh"

echo "replay_finished_at=$(date --iso-8601=seconds)"
sleep 60
OUTPUT_DIR="$RUN_ROOT/schema" "$SCRIPT_DIR/discover_metrics.sh"
docker stats --no-stream >"$RUN_ROOT/docker-stats-after-replay.txt"
du -sb "$RUN_ROOT/prometheus-data" "$RUN_ROOT/greptime-data" \
    >"$RUN_ROOT/disk-usage-after-replay.txt"
df -h "$RUN_ROOT" >"$RUN_ROOT/filesystem-after-replay.txt"

RESULT_DIR="$QUERY_RESULT_DIR" \
    QUERIES="$SCRIPT_DIR/queries.greptime.json" \
    BACKENDS=greptime REPEATS=9 WARMUPS=1 BUILD=1 \
    "$SCRIPT_DIR/compare_promql.sh"

echo "pipeline_finished_at=$(date --iso-8601=seconds)"
touch "$RUN_ROOT/PIPELINE_COMPLETE"
