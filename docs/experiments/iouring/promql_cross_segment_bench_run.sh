#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

CORPUS="${CORPUS:-/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/segments-replay-20260711-141105}"
RESULT_DIR="${RESULT_DIR:-$(dirname "$CORPUS")/io-uring-promql-shapes-$(date +%Y%m%d-%H%M%S)}"
END_MS="${END_MS:-1782985800000}"
SPARSE_END_MS="${SPARSE_END_MS:-1782980100000}"
REPEATS="${REPEATS:-9}"
QUEUE_DEPTH="${QUEUE_DEPTH:-8}"
RING_TEARDOWN_SECS="${RING_TEARDOWN_SECS:-1}"
MIN_MEMLOCK_KIB="${MIN_MEMLOCK_KIB:-65536}"
BUILD="${BUILD:-1}"
MAX_RESIDENT_BYTES_AFTER_EVICT="${MAX_RESIDENT_BYTES_AFTER_EVICT:-0}"
CACHE_STATES="${CACHE_STATES:-evicted warm}"
INCLUDE_SPARSE="${INCLUDE_SPARSE:-1}"
QUERY_NAMES_OVERRIDE="${QUERY_NAMES_OVERRIDE:-}"
CASE_NAMES_OVERRIDE="${CASE_NAMES_OVERRIDE:-}"

METRIC="${METRIC:-http_client_duration_xf5f33b0f6bbd8257}"
GROUP_LABEL="${GROUP_LABEL:-service_name_x55e50a58f9befba7}"

SOURCE_BIN="${SOURCE_BIN:-$REPO_ROOT/target/release/chronoxide-query}"
RUN_BIN="$RESULT_DIR/chronoxide-query"
FADVISE_BIN="$RESULT_DIR/fadvise-dontneed"
SUMMARY="$RESULT_DIR/summary.tsv"
RESIDENCY="$RESULT_DIR/payload-residency.tsv"

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "required command is missing: $1" >&2
        exit 2
    fi
}

for command in awk cargo cc fincore find git jq prlimit ps rg rustc sha256sum sort stat wc xargs; do
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
if ! [[ "$MAX_RESIDENT_BYTES_AFTER_EVICT" =~ ^[0-9]+$ ]]; then
    echo "MAX_RESIDENT_BYTES_AFTER_EVICT must be a non-negative integer" >&2
    exit 2
fi
if ! [[ "$SPARSE_END_MS" =~ ^[0-9]+$ ]]; then
    echo "SPARSE_END_MS must be a non-negative integer" >&2
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
source_bin=$SOURCE_BIN
end_ms=$END_MS
sparse_end_ms=$SPARSE_END_MS
repeats=$REPEATS
queue_depth=$QUEUE_DEPTH
ring_teardown_secs=$RING_TEARDOWN_SECS
metric=$METRIC
group_label=$GROUP_LABEL
max_resident_bytes_after_evict=$MAX_RESIDENT_BYTES_AFTER_EVICT
cache_states=$CACHE_STATES
include_sparse=$INCLUDE_SPARSE
query_names_override=$QUERY_NAMES_OVERRIDE
case_names_override=$CASE_NAMES_OVERRIDE
cache_note=POSIX_FADV_DONTNEED and fincore cover Linux page-cache residency only; they do not flush NVMe/controller cache.
EOF

declare -A QUERIES
QUERIES[count]="histogram_count(sum by ($GROUP_LABEL)(rate(${METRIC}[6h])))"
QUERIES[sum]="histogram_sum(sum by ($GROUP_LABEL)(rate(${METRIC}[6h])))"
QUERIES[fraction]="histogram_fraction(0.1, 1.0, sum by ($GROUP_LABEL)(rate(${METRIC}[6h])))"
QUERIES[quantile]="histogram_quantile(0.95, sum by ($GROUP_LABEL)(rate(${METRIC}[6h])))"
QUERIES[scalar_count_rate]="sum by ($GROUP_LABEL)(rate(${METRIC}_count[6h]))"
QUERIES[shallow]="histogram_quantile(0.95, sum by ($GROUP_LABEL)(rate(${METRIC}[15m])))"
QUERIES[sparse_scalar]='{__name__=~".*[02468]_count"}'
QUERY_NAMES=(count sum fraction quantile scalar_count_rate shallow)
if [[ "$INCLUDE_SPARSE" == "1" ]]; then
    QUERY_NAMES+=(sparse_scalar)
elif [[ "$INCLUDE_SPARSE" != "0" ]]; then
    echo "INCLUDE_SPARSE must be 0 or 1" >&2
    exit 2
fi
if [[ -n "$QUERY_NAMES_OVERRIDE" ]]; then
    read -r -a QUERY_NAMES <<<"$QUERY_NAMES_OVERRIDE"
    for name in "${QUERY_NAMES[@]}"; do
        if [[ -z "${QUERIES[$name]+configured}" ]]; then
            echo "unknown query name in QUERY_NAMES_OVERRIDE: $name" >&2
            exit 2
        fi
    done
fi

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

snapshot_chunk_residency() {
    local label="$1"
    local phase="$2"
    local details="$RESULT_DIR/$label.$phase.fincore.txt"
    local totals

    find "$CORPUS" -name chunks.bin -print0 \
        | xargs -0 -r fincore --bytes --noheadings --output PAGES,RES,SIZE,FILE \
        >"$details"
    totals="$(awk '{pages += $1; resident += $2; size += $3} END {printf "%d\t%d\t%d", pages, resident, size}' "$details")"
    printf '%s\t%s\t%s\n' "$label" "$phase" "$totals" >>"$RESIDENCY"
    awk '{resident += $2} END {printf "%d\n", resident}' "$details"
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

printf 'query_name\tcase\tcache_state\trepetition\tduration_ns\tpayload_duration_ns\tpayload_used_bytes\tpayload_physical_reads\tpayload_physical_bytes\tscheduler_executions\tscheduler_pread_decisions\tscheduler_io_uring_decisions\tscheduler_submissions\tscheduler_sqes\tscheduler_max_depth\tscheduler_peak_in_flight_bytes\tmax_rss_kib\tresult_series\tresult_samples\tsemantic_fingerprint\n' >"$SUMMARY"
printf 'run_label\tphase\tresident_pages\tresident_bytes\tpayload_file_bytes\n' >"$RESIDENCY"

run_case() {
    local query_name="$1"
    local case_name="$2"
    local cache_state="$3"
    local repetition="$4"
    local mode=pread
    local extra=()
    local query_end_ms="$END_MS"
    local label="$query_name-$case_name-$cache_state-$repetition"
    local markdown="$RESULT_DIR/$label.md"
    local raw="$RESULT_DIR/$label.json"
    local log="$RESULT_DIR/$label.log"
    local time_file="$RESULT_DIR/$label.time.txt"

    case "$case_name" in
        default-pread|cross-pread|default-uring|cross-uring|default-auto|cross-auto) ;;
        *)
            echo "unsupported benchmark case: $case_name" >&2
            exit 2
            ;;
    esac

    if [[ "$case_name" == *uring ]]; then
        mode=io-uring
    elif [[ "$case_name" == *auto ]]; then
        mode=auto
    fi
    if [[ "$case_name" == cross-* ]]; then
        extra+=(--experimental-cross-segment-chunk-reads)
    fi
    if [[ "$query_name" == sparse_scalar ]]; then
        query_end_ms="$SPARSE_END_MS"
        extra+=(--regex-max-expanded-values 1000000)
    fi
    if [[ "$query_name" == shallow ]]; then
        extra+=(--start-ms "$((END_MS - 3600000))" --step-ms 60000)
    fi

    check_host_idle
    case "$cache_state" in
        evicted)
            evict_chunk_pages
            local resident_after_evict
            resident_after_evict="$(snapshot_chunk_residency "$label" after-evict)"
            if (( resident_after_evict > MAX_RESIDENT_BYTES_AFTER_EVICT )); then
                echo "payload residency remained ${resident_after_evict} bytes after eviction for $label (limit ${MAX_RESIDENT_BYTES_AFTER_EVICT})" >&2
                exit 71
            fi
            ;;
        warm)
            snapshot_chunk_residency "$label" before-run >/dev/null
            ;;
        *)
            echo "unsupported cache state: $cache_state" >&2
            exit 2
            ;;
    esac

    echo "running $label"
    if ! /usr/bin/time -v -o "$time_file" "$RUN_BIN" \
        --segments-dir "$CORPUS" \
        --query "${QUERIES[$query_name]}" \
        --end-ms "$query_end_ms" \
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
    check_host_idle
    snapshot_chunk_residency "$label" after-run >/dev/null

    local duration_ns payload_row payload_duration payload_duration_ns
    local payload_used_bytes payload_physical_reads payload_physical_bytes
    local scheduler_executions scheduler_pread scheduler_uring scheduler_submissions
    local scheduler_sqes scheduler_max_depth scheduler_peak_bytes
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
    scheduler_executions="$(awk -F'|' '/^\| Executions \|/ {gsub(/^ +| +$/, "", $3); print $3; exit}' "$markdown")"
    scheduler_pread="$(awk -F'|' '/^\| Pread Decisions \|/ {gsub(/^ +| +$/, "", $3); print $3; exit}' "$markdown")"
    scheduler_uring="$(awk -F'|' '/^\| io_uring Decisions \|/ {gsub(/^ +| +$/, "", $3); print $3; exit}' "$markdown")"
    scheduler_submissions="$(awk -F'|' '/^\| Backend Submissions \|/ {gsub(/^ +| +$/, "", $3); print $3; exit}' "$markdown")"
    scheduler_sqes="$(awk -F'|' '/^\| SQEs Submitted \|/ {gsub(/^ +| +$/, "", $3); print $3; exit}' "$markdown")"
    scheduler_max_depth="$(awk -F'|' '/^\| Maximum Submission Depth \|/ {gsub(/^ +| +$/, "", $3); print $3; exit}' "$markdown")"
    scheduler_peak_bytes="$(awk -F'|' '/^\| Peak In-Flight Bytes \|/ {gsub(/^ +| +$/, "", $3); print $3; exit}' "$markdown")"
    max_rss_kib="$(awk -F: '/Maximum resident set size/ {gsub(/^[[:space:]]+/, "", $2); print $2}' "$time_file")"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$query_name" "$case_name" "$cache_state" "$repetition" "$duration_ns" \
        "$payload_duration_ns" "$payload_used_bytes" "$payload_physical_reads" \
        "$payload_physical_bytes" "$scheduler_executions" "$scheduler_pread" \
        "$scheduler_uring" "$scheduler_submissions" "$scheduler_sqes" \
        "$scheduler_max_depth" "$scheduler_peak_bytes" "$max_rss_kib" \
        "$result_series" "$result_samples" \
        "$fingerprint" >>"$SUMMARY"

    sleep "$RING_TEARDOWN_SECS"
}

case_order_for_repetition() {
    if [[ -n "$CASE_NAMES_OVERRIDE" ]]; then
        echo "$CASE_NAMES_OVERRIDE"
        return
    fi
    case $(( ($1 - 1) % 3 )) in
        0) echo 'default-pread cross-uring cross-auto cross-pread default-uring default-auto' ;;
        1) echo 'cross-auto default-pread default-auto cross-uring cross-pread default-uring' ;;
        2) echo 'cross-pread default-uring cross-uring default-auto default-pread cross-auto' ;;
    esac
}

check_host_idle
for query_name in "${QUERY_NAMES[@]}"; do
    for ((repetition = 1; repetition <= REPEATS; repetition++)); do
        read -r -a cases <<<"$(case_order_for_repetition "$repetition")"
        for case_name in "${cases[@]}"; do
            read -r -a cache_states <<<"$CACHE_STATES"
            for cache_state in "${cache_states[@]}"; do
                run_case "$query_name" "$case_name" "$cache_state" "$repetition"
            done
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
    payload_accounting_variants="$(
        awk -F'\t' -v query="$query_name" \
            'NR > 1 && $1 == query { print $7 "\t" $8 "\t" $9 }' "$SUMMARY" \
            | sort -u | wc -l
    )"
    if [[ "$payload_accounting_variants" != "1" ]]; then
        echo "logical or physical payload accounting mismatch for $query_name" >&2
        exit 1
    fi
done

echo "all semantic fingerprints, QueryStats, and payload accounting match"
echo "raw summary: $SUMMARY"
if command -v column >/dev/null 2>&1; then
    column -t -s $'\t' "$SUMMARY"
else
    cat "$SUMMARY"
fi
