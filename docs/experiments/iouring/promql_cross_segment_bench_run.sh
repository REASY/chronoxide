#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

CORPUS="${CORPUS:-/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/segments-replay-20260711-141105}"
RESULT_DIR="${RESULT_DIR:-$(dirname "$CORPUS")/io-uring-promql-shapes-$(date +%Y%m%d-%H%M%S)}"
END_MS="${END_MS:-1782985800000}"
REPEATS="${REPEATS:-3}"
QUEUE_DEPTH="${QUEUE_DEPTH:-8}"
RING_TEARDOWN_SECS="${RING_TEARDOWN_SECS:-1}"
MIN_MEMLOCK_KIB="${MIN_MEMLOCK_KIB:-65536}"
BUILD="${BUILD:-1}"

METRIC="${METRIC:-http_client_duration_xf5f33b0f6bbd8257}"
GROUP_LABEL="${GROUP_LABEL:-service_name_x55e50a58f9befba7}"

SOURCE_BIN="$REPO_ROOT/target/release/chronoxide-query"
RUN_BIN="$RESULT_DIR/chronoxide-query"
FADVISE_BIN="$RESULT_DIR/fadvise-dontneed"
SUMMARY="$RESULT_DIR/summary.tsv"

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "required command is missing: $1" >&2
        exit 2
    fi
}

for command in awk cargo cc find git jq prlimit ps rg rustc sha256sum sort stat wc xargs; do
    require_command "$command"
done

if [[ ! -d "$CORPUS" ]]; then
    echo "corpus does not exist: $CORPUS" >&2
    exit 2
fi
if ! [[ "$REPEATS" =~ ^[1-9][0-9]*$ ]]; then
    echo "REPEATS must be a positive integer" >&2
    exit 2
fi
if ! [[ "$QUEUE_DEPTH" =~ ^[1-9][0-9]*$ ]]; then
    echo "QUEUE_DEPTH must be a positive integer" >&2
    exit 2
fi

memlock_kib="$(ulimit -l)"
if [[ "$memlock_kib" != "unlimited" ]] \
    && { ! [[ "$memlock_kib" =~ ^[0-9]+$ ]] || (( memlock_kib < MIN_MEMLOCK_KIB )); }; then
    cat >&2 <<EOF
memlock is ${memlock_kib} KiB; this benchmark requires at least ${MIN_MEMLOCK_KIB} KiB.
Raise the parent shell with:
  sudo prlimit --pid "\$\$" --memlock=67108864:67108864
Then enter a fresh shell and rerun:
  zsh
EOF
    exit 2
fi

mkdir -p "$RESULT_DIR"

if [[ "$BUILD" == "1" ]]; then
    (
        cd "$REPO_ROOT"
        cargo build --release \
            -p chronoxide-ingester \
            --bin chronoxide-query \
            --features chronoxide-core/io_uring
    )
elif [[ "$BUILD" != "0" ]]; then
    echo "BUILD must be 0 or 1" >&2
    exit 2
fi

if [[ ! -x "$SOURCE_BIN" ]]; then
    echo "release binary does not exist: $SOURCE_BIN" >&2
    exit 2
fi

cp "$SOURCE_BIN" "$RUN_BIN"
cc -O2 -Wall -Wextra -o "$FADVISE_BIN" "$SCRIPT_DIR/fadvise_dontneed.c"

sha256sum "$RUN_BIN" >"$RESULT_DIR/binary.sha256"
git -C "$REPO_ROOT" rev-parse HEAD >"$RESULT_DIR/git-commit.txt"
git -C "$REPO_ROOT" status --short >"$RESULT_DIR/git-status.txt"
git -C "$REPO_ROOT" diff >"$RESULT_DIR/working-tree.patch"
uname -a >"$RESULT_DIR/uname.txt"
rustc --version --verbose >"$RESULT_DIR/rustc.txt"
prlimit --pid "$$" --memlock >"$RESULT_DIR/memlock.txt"
stat -f -c 'filesystem_type=%T mount=%m' "$CORPUS" >"$RESULT_DIR/filesystem.txt"

cat >"$RESULT_DIR/configuration.txt" <<EOF
corpus=$CORPUS
end_ms=$END_MS
repeats=$REPEATS
queue_depth=$QUEUE_DEPTH
ring_teardown_secs=$RING_TEARDOWN_SECS
metric=$METRIC
group_label=$GROUP_LABEL
EOF

declare -A QUERIES
QUERIES[count]="histogram_count(sum by ($GROUP_LABEL)(rate(${METRIC}[6h])))"
QUERIES[sum]="histogram_sum(sum by ($GROUP_LABEL)(rate(${METRIC}[6h])))"
QUERIES[fraction]="histogram_fraction(0.1, 1.0, sum by ($GROUP_LABEL)(rate(${METRIC}[6h])))"
QUERIES[quantile]="histogram_quantile(0.95, sum by ($GROUP_LABEL)(rate(${METRIC}[6h])))"
QUERY_NAMES=(count sum fraction quantile)

for name in "${QUERY_NAMES[@]}"; do
    printf '%s\t%s\n' "$name" "${QUERIES[$name]}" >>"$RESULT_DIR/queries.tsv"
done

check_host_idle() {
    local conflicts
    conflicts="$(
        ps -eo pid=,comm=,args= | awk '
            $2 == "perf" ||
            $2 == "cargo" ||
            $2 == "rustc" ||
            $2 == "chronoxide-ing" ||
            $2 == "chronoxide-quer" ||
            $2 == "codehop-server" ||
            $2 == "codehop-index-w" { print }
        '
    )"
    if [[ -n "$conflicts" ]]; then
        echo "measurement conflict detected:" >&2
        echo "$conflicts" >&2
        exit 70
    fi
}

evict_chunk_pages() {
    find "$CORPUS" -name chunks.bin -print0 | xargs -0 -r "$FADVISE_BIN"
}

duration_to_ns() {
    awk -v duration="$1" 'BEGIN {
        if (duration ~ /ns$/) {
            sub(/ns$/, "", duration); multiplier = 1
        } else if (duration ~ /µs$/) {
            sub(/µs$/, "", duration); multiplier = 1000
        } else if (duration ~ /ms$/) {
            sub(/ms$/, "", duration); multiplier = 1000000
        } else if (duration ~ /s$/) {
            sub(/s$/, "", duration); multiplier = 1000000000
        } else {
            exit 1
        }
        printf "%.0f\n", duration * multiplier
    }'
}

printf 'query_name\tcase\trepetition\tduration_ns\tpayload_duration_ns\tpayload_used_bytes\tpayload_physical_reads\tpayload_physical_bytes\tmax_rss_kib\tresult_series\tresult_samples\tsemantic_fingerprint\n' >"$SUMMARY"

run_case() {
    local query_name="$1"
    local case_name="$2"
    local repetition="$3"
    local mode=pread
    local extra=()
    local label="$query_name-$case_name-$repetition"
    local markdown="$RESULT_DIR/$label.md"
    local raw="$RESULT_DIR/$label.json"
    local log="$RESULT_DIR/$label.log"
    local time_file="$RESULT_DIR/$label.time.txt"

    if [[ "$case_name" == *uring ]]; then
        mode=io-uring
    fi
    if [[ "$case_name" == cross-* ]]; then
        extra+=(--experimental-cross-segment-chunk-reads)
    fi

    check_host_idle
    evict_chunk_pages

    echo "running $label"
    if ! /usr/bin/time -v -o "$time_file" "$RUN_BIN" \
        --segments-dir "$CORPUS" \
        --query "${QUERIES[$query_name]}" \
        --end-ms "$END_MS" \
        --benchmark-repeats 1 \
        --chunk-read-mode "$mode" \
        --chunk-read-queue-depth "$QUEUE_DEPTH" \
        "${extra[@]}" \
        --output "$markdown" \
        --raw-output "$raw" \
        >"$log" 2>&1; then
        tail -20 "$log" >&2
        exit 1
    fi

    local duration_ns payload_row payload_duration payload_duration_ns
    local payload_used_bytes payload_physical_reads payload_physical_bytes
    local max_rss_kib result_series result_samples fingerprint
    duration_ns="$(jq -r '.runs[0].duration_ns' "$raw")"
    result_series="$(jq -r '.runs[0].result_series' "$raw")"
    result_samples="$(jq -r '.runs[0].result_samples' "$raw")"
    fingerprint="$(jq -r '.runs[0].semantic_fingerprint_sha256' "$raw")"
    payload_row="$(rg '^\| Chunk Payload Spans \|' "$markdown")"
    payload_duration="$(awk -F'|' '{gsub(/^ +| +$/, "", $3); print $3}' <<<"$payload_row")"
    payload_duration_ns="$(duration_to_ns "$payload_duration")"
    payload_physical_bytes="$(awk -F'|' '{gsub(/^ +| +$/, "", $4); print $4}' <<<"$payload_row")"
    payload_physical_reads="$(awk -F'|' '{gsub(/^ +| +$/, "", $5); print $5}' <<<"$payload_row")"
    payload_used_bytes="$(jq -r '.runs[0].stats.bytes_read' "$raw")"
    max_rss_kib="$(awk -F: '/Maximum resident set size/ {gsub(/^[[:space:]]+/, "", $2); print $2}' "$time_file")"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$query_name" "$case_name" "$repetition" "$duration_ns" \
        "$payload_duration_ns" "$payload_used_bytes" "$payload_physical_reads" \
        "$payload_physical_bytes" "$max_rss_kib" "$result_series" "$result_samples" \
        "$fingerprint" >>"$SUMMARY"

    sleep "$RING_TEARDOWN_SECS"
}

case_order_for_repetition() {
    case $(( ($1 - 1) % 3 )) in
        0) echo 'default-pread cross-uring cross-pread default-uring' ;;
        1) echo 'cross-uring default-pread default-uring cross-pread' ;;
        2) echo 'cross-pread default-uring default-pread cross-uring' ;;
    esac
}

check_host_idle
for query_name in "${QUERY_NAMES[@]}"; do
    for ((repetition = 1; repetition <= REPEATS; repetition++)); do
        read -r -a cases <<<"$(case_order_for_repetition "$repetition")"
        for case_name in "${cases[@]}"; do
            run_case "$query_name" "$case_name" "$repetition"
        done
    done
done

for query_name in "${QUERY_NAMES[@]}"; do
    mapfile -t raw_files < <(find "$RESULT_DIR" -maxdepth 1 -name "$query_name-*.json" -print | sort)
    semantic_variants="$(
        jq -cS '[.runs[0].semantic_fingerprint_sha256, .runs[0].result_series, .runs[0].result_samples, .runs[0].stats]' \
            "${raw_files[@]}" | sort -u | wc -l
    )"
    if [[ "$semantic_variants" != "1" ]]; then
        echo "semantic or QueryStats mismatch for $query_name" >&2
        exit 1
    fi
done

echo "all semantic fingerprints and QueryStats match"
echo "raw summary: $SUMMARY"
if command -v column >/dev/null 2>&1; then
    column -t -s $'\t' "$SUMMARY"
else
    cat "$SUMMARY"
fi
