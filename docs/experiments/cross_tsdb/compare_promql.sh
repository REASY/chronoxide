#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SEGMENTS_DIR="${SEGMENTS_DIR:-}"
QUERIES="${QUERIES:-$SCRIPT_DIR/queries.example.json}"
RESULT_DIR="${RESULT_DIR:-}"
REPEATS="${REPEATS:-9}"
WARMUPS="${WARMUPS:-1}"
BUILD="${BUILD:-1}"
BACKENDS="${BACKENDS:-chronoxide prometheus greptime}"
QUERY_BIN="${QUERY_BIN:-$REPO_ROOT/target/release/chronoxide-query}"
HTTP_BIN="${HTTP_BIN:-$REPO_ROOT/target/release/chronoxide-promql-http-bench}"
API_BIN="${API_BIN:-$REPO_ROOT/target/release/chronoxide-api}"
CHRONOXIDE_PORT="${CHRONOXIDE_PORT:-9091}"
CHRONOXIDE_STORAGE_SCHEMA="${CHRONOXIDE_STORAGE_SCHEMA:-schema8}"
CHRONOXIDE_CHUNK_READ_MODE="${CHRONOXIDE_CHUNK_READ_MODE:-pread}"
CHRONOXIDE_CHUNK_READ_QUEUE_DEPTH="${CHRONOXIDE_CHUNK_READ_QUEUE_DEPTH:-256}"
CHRONOXIDE_RANGE_SCALAR_CACHE_MAX_BYTES="${CHRONOXIDE_RANGE_SCALAR_CACHE_MAX_BYTES:-0}"
CHRONOXIDE_MAX_CONCURRENT_QUERIES="${CHRONOXIDE_MAX_CONCURRENT_QUERIES:-1}"
CHRONOXIDE_EXPERIMENTAL_CROSS_SEGMENT_READS="${CHRONOXIDE_EXPERIMENTAL_CROSS_SEGMENT_READS:-0}"

for command in cargo curl git jq realpath sha256sum; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command is missing: $command" >&2
        exit 2
    fi
done
if [[ -z "$RESULT_DIR" || "$RESULT_DIR" != /* || -e "$RESULT_DIR" ]]; then
    echo "RESULT_DIR must be a new absolute output path" >&2
    exit 2
fi
if [[ -z "$SEGMENTS_DIR" || ! -d "$SEGMENTS_DIR" ]]; then
    echo "SEGMENTS_DIR must name an explicit existing corpus" >&2
    exit 2
fi
if [[ ! -f "$QUERIES" ]]; then
    echo "missing QUERIES input: $QUERIES" >&2
    exit 2
fi
if ! [[ "$REPEATS" =~ ^[1-9][0-9]*$ && "$WARMUPS" =~ ^[0-9]+$ ]]; then
    echo "REPEATS must be positive and WARMUPS non-negative" >&2
    exit 2
fi
case "$CHRONOXIDE_STORAGE_SCHEMA" in
    schema7|schema8)
        ;;
    *)
        echo "CHRONOXIDE_STORAGE_SCHEMA must be schema7 or schema8" >&2
        exit 2
        ;;
esac
jq -e 'type == "array" and length > 0' "$QUERIES" >/dev/null
for backend in $BACKENDS; do
    case "$backend" in
        chronoxide)
            ;;
        prometheus)
            curl --fail --silent "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/ready" >/dev/null
            ;;
        greptime)
            curl --fail --silent "http://127.0.0.1:${GREPTIME_PORT:-4000}/health" >/dev/null
            ;;
        *)
            echo "BACKENDS contains unsupported backend: $backend" >&2
            exit 2
            ;;
    esac
done

if [[ "$BUILD" == "1" ]]; then
    (cd "$REPO_ROOT" && cargo build --release \
        -p chronoxide-ingester -p chronoxide-query-cli \
        --bin chronoxide-query --bin chronoxide-promql-http-bench)
    if [[ " $BACKENDS " == *" chronoxide "* ]]; then
        (cd "$REPO_ROOT" && cargo build --release -p chronoxide-api --features io_uring)
    fi
elif [[ "$BUILD" != "0" ]]; then
    echo "BUILD must be 0 or 1" >&2
    exit 2
fi

resolve_binary() {
    local variable_name="$1"
    local path="$2"
    if [[ ! -f "$path" || ! -x "$path" ]]; then
        echo "$variable_name must name an executable regular file: $path" >&2
        return 2
    fi
    realpath -e -- "$path"
}

QUERY_BIN="$(resolve_binary QUERY_BIN "$QUERY_BIN")"
HTTP_BIN="$(resolve_binary HTTP_BIN "$HTTP_BIN")"
if [[ " $BACKENDS " == *" chronoxide "* ]]; then
    API_BIN="$(resolve_binary API_BIN "$API_BIN")"
fi

mkdir -p "$RESULT_DIR"
cp "$QUERIES" "$RESULT_DIR/queries.json"
{
    printf 'storage_schema=%s\n' "$CHRONOXIDE_STORAGE_SCHEMA"
    printf 'segments_dir=%s\n' "$SEGMENTS_DIR"
    printf 'query_bin=%s\n' "$QUERY_BIN"
    printf 'http_bin=%s\n' "$HTTP_BIN"
    if [[ " $BACKENDS " == *" chronoxide "* ]]; then
        printf 'api_bin=%s\n' "$API_BIN"
    fi
} >"$RESULT_DIR/chronoxide-config.txt"
binary_paths=("$QUERY_BIN" "$HTTP_BIN")
if [[ " $BACKENDS " == *" chronoxide "* ]]; then
    binary_paths+=("$API_BIN")
fi
sha256sum "${binary_paths[@]}" >"$RESULT_DIR/binaries.sha256"
git -C "$REPO_ROOT" rev-parse HEAD >"$RESULT_DIR/git-commit.txt"
git -C "$REPO_ROOT" status --short >"$RESULT_DIR/git-status.txt"
git -C "$REPO_ROOT" diff >"$RESULT_DIR/working-tree.patch"
printf 'name\tbackend\tmedian_duration_ns\tresult_series\tresult_samples\tportable_fingerprint\n' \
    >"$RESULT_DIR/summary.tsv"

api_pid=""
cleanup() {
    if [[ -n "$api_pid" ]] && kill -0 "$api_pid" 2>/dev/null; then
        kill "$api_pid" 2>/dev/null || true
        wait "$api_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

if [[ " $BACKENDS " == *" chronoxide "* ]]; then
    api_args=(--segments-dir "$SEGMENTS_DIR" --listen "127.0.0.1:$CHRONOXIDE_PORT"
        --storage-schema "$CHRONOXIDE_STORAGE_SCHEMA"
        --chunk-read-mode "$CHRONOXIDE_CHUNK_READ_MODE"
        --chunk-read-queue-depth "$CHRONOXIDE_CHUNK_READ_QUEUE_DEPTH"
        --range-scalar-cache-max-bytes "$CHRONOXIDE_RANGE_SCALAR_CACHE_MAX_BYTES"
        --max-concurrent-queries "$CHRONOXIDE_MAX_CONCURRENT_QUERIES")
    if [[ "$CHRONOXIDE_EXPERIMENTAL_CROSS_SEGMENT_READS" == "1" ]]; then
        api_args+=(--experimental-cross-segment-chunk-reads)
    elif [[ "$CHRONOXIDE_EXPERIMENTAL_CROSS_SEGMENT_READS" != "0" ]]; then
        echo "CHRONOXIDE_EXPERIMENTAL_CROSS_SEGMENT_READS must be 0 or 1" >&2
        exit 2
    fi
    {
        printf 'storage_schema=%s\n' "$CHRONOXIDE_STORAGE_SCHEMA"
        printf 'chunk_read_mode=%s\n' "$CHRONOXIDE_CHUNK_READ_MODE"
        printf 'chunk_read_queue_depth=%s\n' "$CHRONOXIDE_CHUNK_READ_QUEUE_DEPTH"
        printf 'range_scalar_cache_max_bytes=%s\n' "$CHRONOXIDE_RANGE_SCALAR_CACHE_MAX_BYTES"
        printf 'max_concurrent_queries=%s\n' "$CHRONOXIDE_MAX_CONCURRENT_QUERIES"
        printf 'experimental_cross_segment_reads=%s\n' "$CHRONOXIDE_EXPERIMENTAL_CROSS_SEGMENT_READS"
    } >"$RESULT_DIR/chronoxide-api-config.txt"
    "$API_BIN" "${api_args[@]}" >"$RESULT_DIR/chronoxide-api.log" 2>&1 &
    api_pid=$!
    ready=0
    for _ in $(seq 1 120); do
        if curl --fail --silent "http://127.0.0.1:$CHRONOXIDE_PORT/-/ready" >/dev/null 2>&1; then
            ready=1
            break
        fi
        if ! kill -0 "$api_pid" 2>/dev/null; then
            break
        fi
        sleep 0.25
    done
    if [[ "$ready" != "1" ]]; then
        echo "Chronoxide API did not become ready" >&2
        tail -n 50 "$RESULT_DIR/chronoxide-api.log" >&2
        exit 1
    fi
fi

median_last_runs() {
    local count="$1"
    jq -r --argjson count "$count" \
        '[.runs[-$count:][] | .duration_ns] | sort | .[length / 2 | floor]'
}

while IFS= read -r query_spec; do
    name="$(jq -r '.name' <<<"$query_spec")"
    mode="$(jq -r '.mode' <<<"$query_spec")"
    time_ms="$(jq -r '.time_ms // .end_ms' <<<"$query_spec")"
    chronoxide_query="$(jq -r '.chronoxide_query' <<<"$query_spec")"
    chronoxide_raw="$RESULT_DIR/$name-chronoxide-core.json"
    chronoxide_markdown="$RESULT_DIR/$name-chronoxide-core.md"
    chronoxide_repeats=$((WARMUPS + REPEATS))
    chronoxide_args=(--segments-dir "$SEGMENTS_DIR" --query "$chronoxide_query" \
        --storage-layout "$CHRONOXIDE_STORAGE_SCHEMA" \
        --benchmark-repeats "$chronoxide_repeats" --end-ms "$time_ms" \
        --output "$chronoxide_markdown" --raw-output "$chronoxide_raw")
    if [[ "$mode" == "range" ]]; then
        chronoxide_args+=(--start-ms "$(jq -r '.start_ms' <<<"$query_spec")" \
            --step-ms "$(jq -r '.step_ms' <<<"$query_spec")")
    elif [[ "$mode" != "instant" ]]; then
        echo "unsupported query mode for $name: $mode" >&2
        exit 2
    fi
    echo "running $name on chronoxide"
    "$QUERY_BIN" "${chronoxide_args[@]}" >"$RESULT_DIR/$name-chronoxide-core.log" 2>&1

    case "$(jq -r '.schema' "$chronoxide_raw")" in
        chronoxide.query-benchmark.raw/v3|chronoxide.query-benchmark.raw/v4|chronoxide.query-benchmark.raw/v5|chronoxide.query-benchmark.raw/v6|chronoxide.query-benchmark.raw/v7|chronoxide.query-benchmark.raw/v8|chronoxide.query-benchmark.raw/v9|chronoxide.query-benchmark.raw/v10) ;;
        *)
            echo "Chronoxide query binary lacks portable fingerprints; rerun with BUILD=1" >&2
            exit 1
            ;;
    esac
    fingerprint="$(jq -r '.runs[-1].portable_semantic_fingerprint_sha256' "$chronoxide_raw")"
    series="$(jq -r '.runs[-1].result_series' "$chronoxide_raw")"
    samples="$(jq -r '.runs[-1].result_samples' "$chronoxide_raw")"
    if [[ "$(jq -r '[.runs[].portable_semantic_fingerprint_sha256] | unique | length' "$chronoxide_raw")" != 1 ]]; then
        echo "Chronoxide result changed across repetitions for $name" >&2
        exit 1
    fi
    chronoxide_median="$(median_last_runs "$REPEATS" <"$chronoxide_raw")"
    printf '%s\tchronoxide-core\t%s\t%s\t%s\t%s\n' \
        "$name" "$chronoxide_median" "$series" "$samples" "$fingerprint" \
        >>"$RESULT_DIR/summary.tsv"

    for backend in $BACKENDS; do
        backend_query="$(jq -r --arg backend "$backend" '.[$backend + "_query"]' <<<"$query_spec")"
        if [[ "$backend" == "chronoxide" ]]; then
            base="http://127.0.0.1:$CHRONOXIDE_PORT/api/v1"
        elif [[ "$backend" == "prometheus" ]]; then
            base="http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1"
        else
            base="http://127.0.0.1:${GREPTIME_PORT:-4000}/v1/prometheus/api/v1"
        fi
        if [[ "$mode" == "instant" ]]; then
            endpoint="$base/query"
            time_args=(--time-ms "$time_ms")
        else
            endpoint="$base/query_range"
            time_args=(--start-ms "$(jq -r '.start_ms' <<<"$query_spec")" \
                --end-ms "$(jq -r '.end_ms' <<<"$query_spec")" \
                --step-ms "$(jq -r '.step_ms' <<<"$query_spec")")
        fi
        if [[ "$backend" == "greptime" ]]; then
            endpoint="$endpoint?db=public"
        fi
        transform_args=()
        while IFS= read -r mapping; do
            transform_args+=(--label-rename "$mapping")
        done < <(jq -r --arg backend "$backend" \
            '.label_renames[$backend] // {} | to_entries[] | "\(.key)=\(.value)"' <<<"$query_spec")
        while IFS= read -r label; do
            transform_args+=(--drop-label "$label")
        done < <(jq -r --arg backend "$backend" \
            '.drop_labels[$backend] // [] | .[]' <<<"$query_spec")

        report="$RESULT_DIR/$name-$backend.json"
        echo "running $name on $backend"
        "$HTTP_BIN" --name "$name-$backend" --endpoint "$endpoint" \
            --query "$backend_query" --mode "$mode" "${time_args[@]}" \
            --warmups "$WARMUPS" --repeats "$REPEATS" \
            --expected-fingerprint "$fingerprint" --expected-series "$series" \
            --expected-samples "$samples" "${transform_args[@]}" --report "$report"
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$backend" \
            "$(jq -r '.median_duration_ns' "$report")" \
            "$(jq -r '.result_series' "$report")" \
            "$(jq -r '.result_samples' "$report")" \
            "$(jq -r '.portable_semantic_fingerprint_sha256' "$report")" \
            >>"$RESULT_DIR/summary.tsv"
    done
done < <(jq -c '.[]' "$QUERIES")

column -t -s $'\t' "$RESULT_DIR/summary.tsv"
echo "raw results: $RESULT_DIR"
