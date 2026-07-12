#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SEGMENTS_DIR="${SEGMENTS_DIR:-/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/segments-replay-20260711-141105}"
QUERIES="${QUERIES:-$SCRIPT_DIR/queries.example.json}"
RESULT_DIR="${RESULT_DIR:-}"
REPEATS="${REPEATS:-9}"
WARMUPS="${WARMUPS:-1}"
BUILD="${BUILD:-1}"
BACKENDS="${BACKENDS:-prometheus greptime}"
QUERY_BIN="$REPO_ROOT/target/release/chronoxide-query"
HTTP_BIN="$REPO_ROOT/target/release/chronoxide-promql-http-bench"

for command in cargo curl git jq sha256sum; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command is missing: $command" >&2
        exit 2
    fi
done
if [[ -z "$RESULT_DIR" || "$RESULT_DIR" != /* || -e "$RESULT_DIR" ]]; then
    echo "RESULT_DIR must be a new absolute output path" >&2
    exit 2
fi
if [[ ! -d "$SEGMENTS_DIR" || ! -f "$QUERIES" ]]; then
    echo "missing SEGMENTS_DIR or QUERIES input" >&2
    exit 2
fi
if ! [[ "$REPEATS" =~ ^[1-9][0-9]*$ && "$WARMUPS" =~ ^[0-9]+$ ]]; then
    echo "REPEATS must be positive and WARMUPS non-negative" >&2
    exit 2
fi
jq -e 'type == "array" and length > 0' "$QUERIES" >/dev/null
for backend in $BACKENDS; do
    case "$backend" in
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
    (cd "$REPO_ROOT" && cargo build --release -p chronoxide-ingester \
        --bin chronoxide-query --bin chronoxide-promql-http-bench)
elif [[ "$BUILD" != "0" ]]; then
    echo "BUILD must be 0 or 1" >&2
    exit 2
fi

mkdir -p "$RESULT_DIR"
cp "$QUERIES" "$RESULT_DIR/queries.json"
sha256sum "$QUERY_BIN" "$HTTP_BIN" >"$RESULT_DIR/binaries.sha256"
git -C "$REPO_ROOT" rev-parse HEAD >"$RESULT_DIR/git-commit.txt"
git -C "$REPO_ROOT" status --short >"$RESULT_DIR/git-status.txt"
git -C "$REPO_ROOT" diff >"$RESULT_DIR/working-tree.patch"
printf 'name\tbackend\tmedian_duration_ns\tresult_series\tresult_samples\tportable_fingerprint\n' \
    >"$RESULT_DIR/summary.tsv"

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
    chronoxide_raw="$RESULT_DIR/$name-chronoxide.json"
    chronoxide_markdown="$RESULT_DIR/$name-chronoxide.md"
    chronoxide_repeats=$((WARMUPS + REPEATS))
    chronoxide_args=(--segments-dir "$SEGMENTS_DIR" --query "$chronoxide_query" \
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
    "$QUERY_BIN" "${chronoxide_args[@]}" >"$RESULT_DIR/$name-chronoxide.log" 2>&1

    if [[ "$(jq -r '.schema' "$chronoxide_raw")" != "chronoxide.query-benchmark.raw/v3" ]]; then
        echo "Chronoxide query binary lacks portable fingerprints; rerun with BUILD=1" >&2
        exit 1
    fi
    fingerprint="$(jq -r '.runs[-1].portable_semantic_fingerprint_sha256' "$chronoxide_raw")"
    series="$(jq -r '.runs[-1].result_series' "$chronoxide_raw")"
    samples="$(jq -r '.runs[-1].result_samples' "$chronoxide_raw")"
    if [[ "$(jq -r '[.runs[].portable_semantic_fingerprint_sha256] | unique | length' "$chronoxide_raw")" != 1 ]]; then
        echo "Chronoxide result changed across repetitions for $name" >&2
        exit 1
    fi
    chronoxide_median="$(median_last_runs "$REPEATS" <"$chronoxide_raw")"
    printf '%s\tchronoxide\t%s\t%s\t%s\t%s\n' \
        "$name" "$chronoxide_median" "$series" "$samples" "$fingerprint" \
        >>"$RESULT_DIR/summary.tsv"

    for backend in $BACKENDS; do
        backend_query="$(jq -r --arg backend "$backend" '.[$backend + "_query"]' <<<"$query_spec")"
        if [[ "$backend" == "prometheus" ]]; then
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
