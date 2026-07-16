#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE_TOOL="$SCRIPT_DIR/query_ab_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DRY_RUN="${DRY_RUN:-0}"
REPEATS="${REPEATS:-10}"
START_MS="${START_MS:-0}"
END_MS="${END_MS:-}"
STEP_MS="${STEP_MS:-}"
RANGE_SCALAR_CACHE_MAX_BYTES="${RANGE_SCALAR_CACHE_MAX_BYTES:-0}"
CHUNK_READ_QUEUE_DEPTH="${CHUNK_READ_QUEUE_DEPTH:-128}"
MAX_RESIDENT_BYTES_AFTER_EVICT="${MAX_RESIDENT_BYTES_AFTER_EVICT:-0}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
RUN_NOTE="${RUN_NOTE:-}"
QUERY_NAMES_OVERRIDE="${QUERY_NAMES_OVERRIDE:-}"

METRIC="${METRIC:-http_client_duration_xf5f33b0f6bbd8257}"
GROUP_LABEL="${GROUP_LABEL:-service_name_x55e50a58f9befba7}"
SCALAR_COUNT_QUERY="${SCALAR_COUNT_QUERY:-sum by ($GROUP_LABEL)(rate(${METRIC}_count[15m]))}"
NATIVE_QUANTILE_QUERY="${NATIVE_QUANTILE_QUERY:-histogram_quantile(0.95, sum by ($GROUP_LABEL)(rate(${METRIC}[15m])))}"
EXPONENTIAL_QUANTILE_QUERY="${EXPONENTIAL_QUANTILE_QUERY:-}"
METRIC_REGEX_QUERY="${METRIC_REGEX_QUERY:-{__name__=~\"${METRIC}_count\"}}"

QUERY_MAX_SERIES_MATCHED=1000000
QUERY_MAX_PROJECTED_SERIES=2000000
QUERY_MAX_CHUNKS_READ=5000000
QUERY_MAX_BYTES_READ=2147483648
QUERY_MAX_SAMPLES=50000000
REGEX_MAX_EXPANDED_VALUES=100000

usage() {
    cat <<'EOF'
Usage:
  AB_ROOT=/absolute/completed-storage-ab \
  QUERY_BIN=/absolute/vnext/chronoxide-query \
  END_MS=1782980413585 \
  RESULT_DIR=/absolute/new-query-ab-result \
    docs/experiments/storage_vnext/query_ab_run.sh [--dry-run]

Real measurements additionally require QUIET_HOST_CONFIRMED=1 and a non-empty
RUN_NOTE. The runner uses one copied vNext query binary for both formats,
enables --experimental-storage-layout-ab only for v7-a, evicts and verifies
every corpus file before each process, and runs exactly cold then warm in that
one process.
EOF
}

die() {
    echo "storage query A/B: $*" >&2
    exit 2
}

note() {
    echo "storage query A/B: $*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

require_env() {
    local name="$1"
    [[ -n "${!name:-}" ]] || die "$name is required"
}

require_bool() {
    local name="$1"
    local value="$2"
    [[ "$value" == "0" || "$value" == "1" ]] \
        || die "$name must be 0 or 1; got $value"
}

for argument in "$@"; do
    case "$argument" in
        --dry-run)
            DRY_RUN=1
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            usage >&2
            die "unknown argument: $argument"
            ;;
    esac
done

for command in awk cc cmp cp date df find fincore git grep ps python3 realpath sha256sum sort stat /usr/bin/time; do
    require_command "$command"
done
[[ -f "$GATE_TOOL" ]] || die "query A/B gate helper is missing: $GATE_TOOL"
[[ -f "$FADVISE_SOURCE" ]] || die "safe fadvise helper is missing: $FADVISE_SOURCE"

require_env AB_ROOT
require_env QUERY_BIN
require_env RESULT_DIR
require_env END_MS
require_bool DRY_RUN "$DRY_RUN"
require_bool QUIET_HOST_CONFIRMED "$QUIET_HOST_CONFIRMED"
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || die "REPEATS must be a positive integer"
(( REPEATS % 2 == 0 )) || die "REPEATS must be even to balance format order"
[[ "$START_MS" =~ ^[0-9]+$ ]] || die "START_MS must be a non-negative integer"
[[ "$END_MS" =~ ^[0-9]+$ ]] || die "END_MS must be a non-negative integer"
(( END_MS >= START_MS )) || die "END_MS must be greater than or equal to START_MS"
if [[ -n "$STEP_MS" ]]; then
    [[ "$STEP_MS" =~ ^[1-9][0-9]*$ ]] || die "STEP_MS must be empty or positive"
fi
[[ "$RANGE_SCALAR_CACHE_MAX_BYTES" =~ ^[0-9]+$ ]] \
    || die "RANGE_SCALAR_CACHE_MAX_BYTES must be non-negative"
[[ "$CHUNK_READ_QUEUE_DEPTH" =~ ^[1-9][0-9]*$ ]] \
    || die "CHUNK_READ_QUEUE_DEPTH must be positive"
[[ "$MAX_RESIDENT_BYTES_AFTER_EVICT" =~ ^[0-9]+$ ]] \
    || die "MAX_RESIDENT_BYTES_AFTER_EVICT must be non-negative"
[[ "$RUN_NOTE" != *$'\n'* ]] || die "RUN_NOTE must not contain newlines"

if [[ "$DRY_RUN" != "1" ]]; then
    [[ "$QUIET_HOST_CONFIRMED" == "1" ]] \
        || die "real measurement requires QUIET_HOST_CONFIRMED=1"
    [[ -n "$RUN_NOTE" ]] || die "real measurement requires a non-empty RUN_NOTE"
fi

[[ "$AB_ROOT" == /* && -d "$AB_ROOT" ]] || die "AB_ROOT must be an absolute directory"
AB_ROOT="$(realpath -e -- "$AB_ROOT")"
if [[ ! -f "$AB_ROOT/COMPLETE" && ! -f "$AB_ROOT/COMPLETE_WITH_COVERAGE_GAPS" ]]; then
    die "AB_ROOT has no completion marker"
fi
V7_CORPUS="$AB_ROOT/runs/v7-a/segments"
VNEXT_CORPUS="$AB_ROOT/runs/vnext-a/segments"
[[ -d "$V7_CORPUS" ]] || die "v7-a corpus is missing: $V7_CORPUS"
[[ -d "$VNEXT_CORPUS" ]] || die "vnext-a corpus is missing: $VNEXT_CORPUS"
V7_CORPUS="$(realpath -e -- "$V7_CORPUS")"
VNEXT_CORPUS="$(realpath -e -- "$VNEXT_CORPUS")"

[[ "$QUERY_BIN" == /* && -f "$QUERY_BIN" && -x "$QUERY_BIN" ]] \
    || die "QUERY_BIN must be an absolute executable regular file"
QUERY_BIN="$(realpath -e -- "$QUERY_BIN")"

[[ "$RESULT_DIR" == /* ]] || die "RESULT_DIR must be absolute"
result_name="$(basename "$RESULT_DIR")"
[[ -n "$result_name" && "$result_name" != "." && "$result_name" != ".." ]] \
    || die "RESULT_DIR must name a new child of an existing directory"
result_parent_input="$(dirname "$RESULT_DIR")"
[[ -d "$result_parent_input" ]] || die "RESULT_DIR parent does not exist"
result_parent="$(realpath -e -- "$result_parent_input")"
RESULT_DIR="$result_parent/$result_name"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists; outputs are never reused"
case "$RESULT_DIR/" in
    "$AB_ROOT/"*) die "RESULT_DIR must not be inside AB_ROOT" ;;
esac

declare -A QUERIES=(
    [scalar_count]="$SCALAR_COUNT_QUERY"
    [native_quantile]="$NATIVE_QUANTILE_QUERY"
    [exponential_quantile]="$EXPONENTIAL_QUANTILE_QUERY"
    [metric_regex]="$METRIC_REGEX_QUERY"
)
QUERY_NAMES=(scalar_count native_quantile)
if [[ -n "$QUERY_NAMES_OVERRIDE" ]]; then
    read -r -a QUERY_NAMES <<<"$QUERY_NAMES_OVERRIDE"
fi
(( ${#QUERY_NAMES[@]} > 0 )) || die "at least one query name is required"
for query_name in "${QUERY_NAMES[@]}"; do
    [[ -n "${QUERIES[$query_name]+configured}" ]] \
        || die "unknown query name: $query_name"
    query="${QUERIES[$query_name]}"
    [[ -n "$query" && "$query" != *$'\t'* && "$query" != *$'\n'* ]] \
        || die "query $query_name must be non-empty and contain no tabs/newlines"
done

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/metadata" "$RESULT_DIR/inventory" "$RESULT_DIR/runs" \
    "$RESULT_DIR/comparisons"
METADATA_DIR="$RESULT_DIR/metadata"
INVENTORY_DIR="$RESULT_DIR/inventory"
RUNS_DIR="$RESULT_DIR/runs"
COMPARISONS_DIR="$RESULT_DIR/comparisons"
RUN_BIN="$METADATA_DIR/chronoxide-query"
FADVISE_BIN="$METADATA_DIR/fadvise-regular-dontneed"
RESIDENCY_SUMMARY="$RESULT_DIR/residency-summary.tsv"

cp --reflink=auto --preserve=mode,timestamps -- "$QUERY_BIN" "$RUN_BIN"
cmp -s -- "$QUERY_BIN" "$RUN_BIN" || die "copied query binary differs from source"
[[ -x "$RUN_BIN" ]] || die "copied query binary is not executable"
"$RUN_BIN" --help 2>&1 | grep -Fq -- '--experimental-storage-layout-ab' \
    || die "query binary does not expose --experimental-storage-layout-ab"
sha256sum -- "$RUN_BIN" >"$METADATA_DIR/query-binary.sha256"
printf 'source=%s\npreserved=%s\n' "$QUERY_BIN" "$RUN_BIN" \
    >"$METADATA_DIR/query-binary-paths.txt"

for harness_file in \
    query_ab_run.sh query_ab_gate.py test_query_ab_gate.py \
    fadvise_regular_dontneed.c README.md; do
    [[ -f "$SCRIPT_DIR/$harness_file" ]] \
        || die "harness provenance file is missing: $SCRIPT_DIR/$harness_file"
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$METADATA_DIR/$harness_file"
done
sha256sum \
    "$METADATA_DIR/query_ab_run.sh" \
    "$METADATA_DIR/query_ab_gate.py" \
    "$METADATA_DIR/test_query_ab_gate.py" \
    "$METADATA_DIR/fadvise_regular_dontneed.c" \
    "$METADATA_DIR/README.md" >"$METADATA_DIR/harness.sha256"

git -C "$REPO_ROOT" rev-parse HEAD >"$METADATA_DIR/git-commit.txt"
git -C "$REPO_ROOT" status --porcelain=v2 --branch >"$METADATA_DIR/git-status.txt"
git -C "$REPO_ROOT" diff --binary --full-index HEAD -- \
    >"$METADATA_DIR/tracked-source.patch"

python3 "$GATE_TOOL" inventory \
    --corpus "$V7_CORPUS" \
    --output "$INVENTORY_DIR/v7.json" \
    --paths-output "$INVENTORY_DIR/v7-files.nul"
python3 "$GATE_TOOL" inventory \
    --corpus "$VNEXT_CORPUS" \
    --output "$INVENTORY_DIR/vnext.json" \
    --paths-output "$INVENTORY_DIR/vnext-files.nul"
python3 "$GATE_TOOL" compare-corpora \
    --baseline "$INVENTORY_DIR/v7.json" \
    --candidate "$INVENTORY_DIR/vnext.json" \
    --output "$COMPARISONS_DIR/corpus-artifact-equivalence.json"
sha256sum "$INVENTORY_DIR/v7.json" "$INVENTORY_DIR/vnext.json" \
    "$COMPARISONS_DIR/corpus-artifact-equivalence.json" \
    >"$INVENTORY_DIR/inventory.sha256"

cc -O2 -Wall -Wextra -Werror -o "$FADVISE_BIN" "$FADVISE_SOURCE"
sha256sum -- "$FADVISE_BIN" >"$METADATA_DIR/fadvise.sha256"

{
    printf 'query_name\texpression\n'
    for query_name in "${QUERY_NAMES[@]}"; do
        printf '%s\t%s\n' "$query_name" "${QUERIES[$query_name]}"
    done
} >"$RESULT_DIR/queries.tsv"

format_order_for_repetition() {
    if (( $1 % 2 == 1 )); then
        printf 'v7 vnext\n'
    else
        printf 'vnext v7\n'
    fi
}

{
    printf 'process_label\tquery_name\trepetition\torder_index\tformat\tcorpus\texperimental_storage_layout_ab\n'
    for ((repetition = 1; repetition <= REPEATS; repetition++)); do
        read -r -a formats <<<"$(format_order_for_repetition "$repetition")"
        for query_name in "${QUERY_NAMES[@]}"; do
            order_index=0
            for format_name in "${formats[@]}"; do
                ((order_index += 1))
                if [[ "$format_name" == "v7" ]]; then
                    corpus="$V7_CORPUS"
                    experimental=true
                else
                    corpus="$VNEXT_CORPUS"
                    experimental=false
                fi
                process_label="$(printf 'r%02d-%s-%02d-%s' \
                    "$repetition" "$query_name" "$order_index" "$format_name")"
                printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                    "$process_label" "$query_name" "$repetition" "$order_index" \
                    "$format_name" "$corpus" "$experimental"
            done
        done
    done
} >"$RESULT_DIR/run-plan.tsv"

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'ab_root=%s\n' "$AB_ROOT"
    printf 'v7_corpus=%s\n' "$V7_CORPUS"
    printf 'vnext_corpus=%s\n' "$VNEXT_CORPUS"
    printf 'query_binary=%s\n' "$RUN_BIN"
    printf 'repeats=%s\n' "$REPEATS"
    printf 'start_ms=%s\n' "$START_MS"
    printf 'end_ms=%s\n' "$END_MS"
    printf 'step_ms=%s\n' "$STEP_MS"
    printf 'range_scalar_cache_max_bytes=%s\n' "$RANGE_SCALAR_CACHE_MAX_BYTES"
    printf 'chunk_read_mode=pread\n'
    printf 'chunk_read_queue_depth=%s\n' "$CHUNK_READ_QUEUE_DEPTH"
    printf 'max_resident_bytes_after_evict=%s\n' "$MAX_RESIDENT_BYTES_AFTER_EVICT"
    printf 'quiet_host_confirmed=%s\n' "$QUIET_HOST_CONFIRMED"
    printf 'run_note=%s\n' "$RUN_NOTE"
    printf 'cache_note=POSIX_FADV_DONTNEED and fincore cover Linux page-cache residency only; they do not flush device/controller caches\n'
    uname -a || true
    command -v rustc >/dev/null 2>&1 && rustc --version --verbose || true
    command -v cargo >/dev/null 2>&1 && cargo --version --verbose || true
    command -v lscpu >/dev/null 2>&1 && lscpu || true
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$AB_ROOT" || true
    stat -f -c 'ab_filesystem_type=%T ab_mount=%m' "$AB_ROOT" || true
    stat -f -c 'result_filesystem_type=%T result_mount=%m' "$RESULT_DIR" || true
    df -B1 "$RESULT_DIR" || true
    ulimit -a || true
    [[ -r /proc/meminfo ]] && cat /proc/meminfo || true
} >"$METADATA_DIR/environment.txt" 2>&1
printf '%s\n' "$RUN_NOTE" >"$METADATA_DIR/run-note.txt"

if [[ "$DRY_RUN" == "1" ]]; then
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    note "dry run complete; no query process or cache eviction was launched: $RESULT_DIR"
    exit 0
fi

check_measurement_conflicts() {
    local conflicts
    conflicts="$(
        ps -eo pid=,comm=,args= | awk -v own="$$" '
            $1 != own && (
                $2 == "cargo" ||
                $2 == "rustc" ||
                $2 == "perf" ||
                $2 ~ /^chronoxide-/ ||
                $2 ~ /^greptime/ ||
                $2 == "prometheus" ||
                $2 == "codehop-server" ||
                $2 == "codehop-index-w"
            ) { print }
        '
    )"
    [[ -z "$conflicts" ]] || {
        printf 'measurement conflict detected:\n%s\n' "$conflicts" >&2
        exit 70
    }
}

evict_all_files() {
    local paths_file="$1"
    local file
    while IFS= read -r -d '' file; do
        "$FADVISE_BIN" "$file"
    done <"$paths_file"
}

snapshot_residency() {
    local process_label="$1"
    local format_name="$2"
    local query_name="$3"
    local repetition="$4"
    local phase="$5"
    local paths_file="$6"
    local output="$7"
    local file
    local line
    local resident
    local size
    local file_count=0
    local total_resident=0
    local total_size=0

    [[ ! -e "$output" ]] || die "refusing to reuse residency detail: $output"
    : >"$output"
    while IFS= read -r -d '' file; do
        [[ -f "$file" && ! -L "$file" ]] \
            || die "corpus entry changed type after inventory: $file"
        line="$(fincore --bytes --noheadings --output RES,SIZE -- "$file")"
        read -r resident size <<<"$line"
        [[ "$resident" =~ ^[0-9]+$ && "$size" =~ ^[0-9]+$ ]] \
            || die "could not parse fincore output for $file: $line"
        printf '%s\0%s\0%s\0' "$resident" "$size" "$file" >>"$output"
        ((file_count += 1))
        ((total_resident += resident))
        ((total_size += size))
    done <"$paths_file"
    (( file_count > 0 )) || die "residency snapshot saw no corpus files"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$format_name" "$query_name" "$repetition" "$phase" \
        "$file_count" "$total_resident" "$total_size" >>"$RESIDENCY_SUMMARY"
    printf '%s\n' "$total_resident"
}

printf 'process_label\tformat\tquery_name\trepetition\tphase\tfile_count\tresident_bytes\tcorpus_file_bytes\n' \
    >"$RESIDENCY_SUMMARY"

declare -a PARSED_RESULTS=()

run_process() {
    local query_name="$1"
    local repetition="$2"
    local order_index="$3"
    local format_name="$4"
    local corpus
    local paths_file
    local process_label
    local run_dir
    local raw
    local markdown
    local log
    local time_file
    local parsed
    local resident_after_evict
    local max_rss_kib
    local status
    local -a args
    local -a parser_args

    if [[ "$format_name" == "v7" ]]; then
        corpus="$V7_CORPUS"
        paths_file="$INVENTORY_DIR/v7-files.nul"
    else
        corpus="$VNEXT_CORPUS"
        paths_file="$INVENTORY_DIR/vnext-files.nul"
    fi
    process_label="$(printf 'r%02d-%s-%02d-%s' \
        "$repetition" "$query_name" "$order_index" "$format_name")"
    run_dir="$RUNS_DIR/$process_label"
    [[ ! -e "$run_dir" ]] || die "refusing to reuse process directory: $run_dir"
    mkdir "$run_dir"
    raw="$run_dir/raw.json"
    markdown="$run_dir/report.md"
    log="$run_dir/query.log"
    time_file="$run_dir/time.txt"
    parsed="$run_dir/parsed.json"

    check_measurement_conflicts
    evict_all_files "$paths_file"
    resident_after_evict="$(snapshot_residency \
        "$process_label" "$format_name" "$query_name" "$repetition" \
        after-evict "$paths_file" "$run_dir/residency-after-evict.nul")"
    if (( resident_after_evict > MAX_RESIDENT_BYTES_AFTER_EVICT )); then
        die "resident corpus bytes after eviction are $resident_after_evict for $process_label; limit is $MAX_RESIDENT_BYTES_AFTER_EVICT"
    fi

    args=(
        --segments-dir "$corpus"
        --query "${QUERIES[$query_name]}"
        --start-ms "$START_MS"
        --end-ms "$END_MS"
        --benchmark-repeats 2
        --chunk-read-mode pread
        --chunk-read-queue-depth "$CHUNK_READ_QUEUE_DEPTH"
        --query-max-series-matched "$QUERY_MAX_SERIES_MATCHED"
        --query-max-projected-series "$QUERY_MAX_PROJECTED_SERIES"
        --query-max-chunks-read "$QUERY_MAX_CHUNKS_READ"
        --query-max-bytes-read "$QUERY_MAX_BYTES_READ"
        --query-max-samples "$QUERY_MAX_SAMPLES"
        --regex-max-expanded-values "$REGEX_MAX_EXPANDED_VALUES"
        --output "$markdown"
        --raw-output "$raw"
    )
    if [[ -n "$STEP_MS" ]]; then
        args+=(
            --step-ms "$STEP_MS"
            --range-scalar-cache-max-bytes "$RANGE_SCALAR_CACHE_MAX_BYTES"
        )
    fi
    if [[ "$format_name" == "v7" ]]; then
        args+=(--experimental-storage-layout-ab)
    fi

    note "running $process_label"
    set +e
    /usr/bin/time -v -o "$time_file" \
        "$RUN_BIN" "${args[@]}" >"$log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$run_dir/exit-status"
    if (( status != 0 )); then
        tail -n 50 "$log" >&2 || true
        die "$process_label query failed with status $status; partial output was preserved"
    fi
    check_measurement_conflicts
    snapshot_residency \
        "$process_label" "$format_name" "$query_name" "$repetition" \
        after-run "$paths_file" "$run_dir/residency-after-run.nul" >/dev/null

    max_rss_kib="$(awk -F: '/Maximum resident set size/ {
        gsub(/^[[:space:]]+/, "", $2); print $2
    }' "$time_file")"
    [[ "$max_rss_kib" =~ ^[0-9]+$ ]] \
        || die "could not parse maximum RSS for $process_label"
    parser_args=(
        parse-raw
        --raw "$raw"
        --output "$parsed"
        --process-label "$process_label"
        --format "$format_name"
        --repetition "$repetition"
        --order-index "$order_index"
        --query-name "$query_name"
        --query "${QUERIES[$query_name]}"
        --corpus "$corpus"
        --max-rss-kib "$max_rss_kib"
        --start-ms "$START_MS"
        --end-ms "$END_MS"
        --queue-depth "$CHUNK_READ_QUEUE_DEPTH"
        --range-scalar-cache-max-bytes "$RANGE_SCALAR_CACHE_MAX_BYTES"
    )
    if [[ -n "$STEP_MS" ]]; then
        parser_args+=(--step-ms "$STEP_MS")
    fi
    python3 "$GATE_TOOL" "${parser_args[@]}" \
        || die "$process_label raw output failed validation"
    PARSED_RESULTS+=("$parsed")
}

for ((repetition = 1; repetition <= REPEATS; repetition++)); do
    read -r -a formats <<<"$(format_order_for_repetition "$repetition")"
    for query_name in "${QUERY_NAMES[@]}"; do
        order_index=0
        for format_name in "${formats[@]}"; do
            ((order_index += 1))
            run_process "$query_name" "$repetition" "$order_index" "$format_name"
        done
    done
done

compare_args=(
    compare-results
    --repeats "$REPEATS"
    --summary "$RESULT_DIR/summary.tsv"
    --output "$COMPARISONS_DIR/query-equivalence.json"
)
for query_name in "${QUERY_NAMES[@]}"; do
    compare_args+=(--query-name "$query_name")
done
for parsed in "${PARSED_RESULTS[@]}"; do
    compare_args+=(--input "$parsed")
done
python3 "$GATE_TOOL" "${compare_args[@]}" \
    || die "query result equivalence gate failed"

(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        sha256sum -- "${artifact#./}"
    done < <(find runs comparisons -type f -print0 | sort -z)
) >"$METADATA_DIR/result-artifacts.sha256"

touch "$RESULT_DIR/COMPLETE"
note "complete: $RESULT_DIR"
