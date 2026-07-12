#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CAPTURE="${CAPTURE:-/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001}"
RESULT_DIR="${RESULT_DIR:-}"
TARGET="${TARGET:-}"
BUILD="${BUILD:-1}"
MAX_SOURCE_MESSAGES="${MAX_SOURCE_MESSAGES:-}"
START_SOURCE_MESSAGE="${START_SOURCE_MESSAGE:-}"
MAX_BATCH_BYTES="${MAX_BATCH_BYTES:-4194304}"
MAX_BATCH_MESSAGES="${MAX_BATCH_MESSAGES:-512}"
DROP_MISSING_NUMBER_VALUES="${DROP_MISSING_NUMBER_VALUES:-1}"
MAX_EVENT_AGE_SECS="${MAX_EVENT_AGE_SECS:-3600}"
MAX_EVENT_LEAD_SECS="${MAX_EVENT_LEAD_SECS:-5}"
BIN="$REPO_ROOT/target/release/chronoxide-otlp-http-replay"

if [[ -z "$RESULT_DIR" || "$RESULT_DIR" != /* || ! -d "$RESULT_DIR" ]]; then
    echo "RESULT_DIR must be the absolute run root created by stack_up.sh" >&2
    exit 2
fi
if [[ ! -d "$CAPTURE" ]]; then
    echo "capture does not exist: $CAPTURE" >&2
    exit 2
fi
if [[ "$TARGET" != "prometheus" && "$TARGET" != "greptime" ]]; then
    echo "TARGET must be prometheus or greptime" >&2
    exit 2
fi
if [[ "$BUILD" == "1" ]]; then
    (cd "$REPO_ROOT" && cargo build --release -p chronoxide-ingester --bin chronoxide-otlp-http-replay)
elif [[ "$BUILD" != "0" ]]; then
    echo "BUILD must be 0 or 1" >&2
    exit 2
fi

report="$RESULT_DIR/${TARGET}-replay.json"
log="$RESULT_DIR/${TARGET}-replay.log"
if [[ -e "$report" || -e "$log" ]]; then
    echo "replay output already exists for $TARGET in $RESULT_DIR" >&2
    exit 2
fi

case "$TARGET" in
    prometheus)
        endpoint="http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1/otlp/v1/metrics"
        headers=()
        ;;
    greptime)
        endpoint="http://127.0.0.1:${GREPTIME_PORT:-4000}/v1/otlp/v1/metrics"
        headers=(
            --header "X-Greptime-DB-Name=public"
            --header "x-greptime-otlp-metric-promote-all-resource-attrs=true"
        )
        ;;
esac

limit_args=()
if [[ "$DROP_MISSING_NUMBER_VALUES" == "1" ]]; then
    limit_args+=(--drop-missing-number-values)
elif [[ "$DROP_MISSING_NUMBER_VALUES" != "0" ]]; then
    echo "DROP_MISSING_NUMBER_VALUES must be 0 or 1" >&2
    exit 2
fi
if ! [[ "$MAX_EVENT_AGE_SECS" =~ ^[0-9]+$ && "$MAX_EVENT_LEAD_SECS" =~ ^[0-9]+$ ]]; then
    echo "MAX_EVENT_AGE_SECS and MAX_EVENT_LEAD_SECS must be non-negative integers" >&2
    exit 2
fi
limit_args+=(
    --max-event-age-secs "$MAX_EVENT_AGE_SECS"
    --max-event-lead-secs "$MAX_EVENT_LEAD_SECS"
)
if [[ -n "$START_SOURCE_MESSAGE" ]]; then
    limit_args+=(--start-source-message "$START_SOURCE_MESSAGE")
fi
if [[ -n "$MAX_SOURCE_MESSAGES" ]]; then
    limit_args+=(--max-source-messages "$MAX_SOURCE_MESSAGES")
fi

sha256sum "$BIN" >"$RESULT_DIR/${TARGET}-replay-binary.sha256"
git -C "$REPO_ROOT" rev-parse HEAD >"$RESULT_DIR/${TARGET}-git-commit.txt"
git -C "$REPO_ROOT" status --short >"$RESULT_DIR/${TARGET}-git-status.txt"

set -o pipefail
"$BIN" \
    --capture "$CAPTURE" \
    --endpoint "$endpoint" \
    --max-batch-bytes "$MAX_BATCH_BYTES" \
    --max-batch-messages "$MAX_BATCH_MESSAGES" \
    "${headers[@]}" \
    "${limit_args[@]}" \
    --report "$report" \
    2>&1 | tee "$log"
