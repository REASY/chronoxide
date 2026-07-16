#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE_TOOL="$SCRIPT_DIR/schema7_query_ab_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DRY_RUN="${DRY_RUN:-0}"
REPEATS="${REPEATS:-4}"
BENCHMARK_REPEATS="${BENCHMARK_REPEATS:-1}"
START_MS="${START_MS:-0}"
END_MS="${END_MS:-}"
CHUNK_READ_QUEUE_DEPTH="${CHUNK_READ_QUEUE_DEPTH:-128}"
MAX_RESIDENT_BYTES_AFTER_EVICT="${MAX_RESIDENT_BYTES_AFTER_EVICT:-0}"
ALLOW_NOISY_HOST="${ALLOW_NOISY_HOST:-0}"
RUN_NOTE="${RUN_NOTE:-}"

METRIC="${METRIC:-http_client_duration_xf5f33b0f6bbd8257}"
GROUP_LABEL="${GROUP_LABEL:-service_name_x55e50a58f9befba7}"
SCALAR_COUNT_QUERY="${SCALAR_COUNT_QUERY:-sum by ($GROUP_LABEL)(rate(${METRIC}_count[15m]))}"
NATIVE_QUANTILE_QUERY="${NATIVE_QUANTILE_QUERY:-histogram_quantile(0.95, sum by ($GROUP_LABEL)(rate(${METRIC}[15m])))}"
METRIC_REGEX_QUERY="${METRIC_REGEX_QUERY:-}"
if [[ -z "$METRIC_REGEX_QUERY" ]]; then
    METRIC_REGEX_QUERY="{__name__=~\"${METRIC}_count\"}"
fi
EXPONENTIAL_QUANTILE_QUERY="${EXPONENTIAL_QUANTILE_QUERY:-histogram_quantile(0.95, sum by ($GROUP_LABEL)(rate(ag_consul_request_x0f4a28dca7d2d184[15m])))}"
INCLUDE_EXPONENTIAL_QUERY="${INCLUDE_EXPONENTIAL_QUERY:-1}"

QUERY_MAX_SERIES_MATCHED="${QUERY_MAX_SERIES_MATCHED:-1000000}"
QUERY_MAX_PROJECTED_SERIES="${QUERY_MAX_PROJECTED_SERIES:-2000000}"
QUERY_MAX_CHUNKS_READ="${QUERY_MAX_CHUNKS_READ:-5000000}"
QUERY_MAX_BYTES_READ="${QUERY_MAX_BYTES_READ:-2147483648}"
QUERY_MAX_SAMPLES="${QUERY_MAX_SAMPLES:-50000000}"
REGEX_MAX_EXPANDED_VALUES="${REGEX_MAX_EXPANDED_VALUES:-100000}"

usage() {
    cat <<'EOF'
Usage:
  SCHEMA6_DIR=/absolute/schema6/segments \
  SCHEMA7_DIR=/absolute/schema7/segments \
  QUERY_BIN=/absolute/chronoxide-query \
  END_MS=1782980413585 \
  RESULT_DIR=/absolute/new-result \
  RUN_NOTE='exploratory run on a noisy host' \
  ALLOW_NOISY_HOST=1 \
    docs/experiments/storage_vnext/schema7_query_ab_run.sh [--dry-run]

The runner copies one query binary, inventories both immutable corpora, validates
the Schema 7 footers once before measurement, evicts each corpus before its
process, then fully validates both layouts during store open before query timing.
This gives both layouts the same validation-warmed page-cache condition. Layout
order alternates by outer repetition and all selected query shapes run in one
process per layout/repetition.

Defaults: REPEATS=4, BENCHMARK_REPEATS=1, INCLUDE_EXPONENTIAL_QUERY=1.
REPEATS may be odd for exploratory noisy-host runs, although even values balance
layout order. Set ALLOW_NOISY_HOST=1 only when concurrent-workload noise is an
accepted limitation and describe it in RUN_NOTE.
EOF
}

die() {
    echo "Schema 6/7 query A/B: $*" >&2
    exit 2
}

note() {
    echo "Schema 6/7 query A/B: $*"
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
[[ -f "$GATE_TOOL" ]] || die "equivalence gate is missing: $GATE_TOOL"
[[ -f "$FADVISE_SOURCE" ]] || die "safe fadvise helper is missing: $FADVISE_SOURCE"

require_env SCHEMA6_DIR
require_env SCHEMA7_DIR
require_env QUERY_BIN
require_env RESULT_DIR
require_env END_MS
require_bool DRY_RUN "$DRY_RUN"
require_bool ALLOW_NOISY_HOST "$ALLOW_NOISY_HOST"
require_bool INCLUDE_EXPONENTIAL_QUERY "$INCLUDE_EXPONENTIAL_QUERY"
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || die "REPEATS must be a positive integer"
[[ "$BENCHMARK_REPEATS" =~ ^[1-9][0-9]*$ ]] \
    || die "BENCHMARK_REPEATS must be a positive integer"
[[ "$START_MS" =~ ^[0-9]+$ ]] || die "START_MS must be a non-negative integer"
[[ "$END_MS" =~ ^[0-9]+$ ]] || die "END_MS must be a non-negative integer"
(( END_MS >= START_MS )) || die "END_MS must be greater than or equal to START_MS"
[[ "$CHUNK_READ_QUEUE_DEPTH" =~ ^[1-9][0-9]*$ ]] \
    || die "CHUNK_READ_QUEUE_DEPTH must be positive"
[[ "$MAX_RESIDENT_BYTES_AFTER_EVICT" =~ ^[0-9]+$ ]] \
    || die "MAX_RESIDENT_BYTES_AFTER_EVICT must be non-negative"
for limit_name in \
    QUERY_MAX_SERIES_MATCHED QUERY_MAX_PROJECTED_SERIES QUERY_MAX_CHUNKS_READ \
    QUERY_MAX_BYTES_READ QUERY_MAX_SAMPLES REGEX_MAX_EXPANDED_VALUES; do
    [[ "${!limit_name}" =~ ^[1-9][0-9]*$ ]] \
        || die "$limit_name must be a positive integer"
done
[[ "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
    || die "RUN_NOTE must contain no tabs or newlines"
if [[ "$DRY_RUN" != "1" && -z "$RUN_NOTE" ]]; then
    die "real measurement requires a non-empty RUN_NOTE"
fi

for corpus_name in SCHEMA6_DIR SCHEMA7_DIR; do
    corpus="${!corpus_name}"
    [[ "$corpus" == /* && -d "$corpus" ]] || die "$corpus_name must be an absolute directory"
    corpus="$(realpath -e -- "$corpus")"
    [[ "$corpus" != *$'\n'* && "$corpus" != *$'\t'* ]] \
        || die "$corpus_name must contain no tabs or newlines"
    printf -v "$corpus_name" '%s' "$corpus"
done
[[ "$SCHEMA6_DIR" != "$SCHEMA7_DIR" ]] || die "Schema 6 and Schema 7 corpora must differ"

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
for corpus in "$SCHEMA6_DIR" "$SCHEMA7_DIR"; do
    case "$RESULT_DIR/" in
        "$corpus/"*) die "RESULT_DIR must not be inside a corpus" ;;
    esac
done

declare -a QUERY_NAMES=(scalar_count native_quantile metric_regex)
declare -a QUERY_EXPRESSIONS=(
    "$SCALAR_COUNT_QUERY"
    "$NATIVE_QUANTILE_QUERY"
    "$METRIC_REGEX_QUERY"
)
if [[ "$INCLUDE_EXPONENTIAL_QUERY" == "1" ]]; then
    [[ -n "$EXPONENTIAL_QUANTILE_QUERY" ]] \
        || die "EXPONENTIAL_QUANTILE_QUERY must be non-empty when included"
    QUERY_NAMES+=(exponential_quantile)
    QUERY_EXPRESSIONS+=("$EXPONENTIAL_QUANTILE_QUERY")
fi
for query in "${QUERY_EXPRESSIONS[@]}"; do
    [[ -n "$query" && "$query" != *$'\t'* && "$query" != *$'\n'* ]] \
        || die "query expressions must be non-empty and contain no tabs/newlines"
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
RAW_INDEX="$RESULT_DIR/raw-index.tsv"

cp --reflink=auto --preserve=mode,timestamps -- "$QUERY_BIN" "$RUN_BIN"
cmp -s -- "$QUERY_BIN" "$RUN_BIN" || die "copied query binary differs from source"
[[ -x "$RUN_BIN" ]] || die "copied query binary is not executable"
help_text="$($RUN_BIN --help 2>&1)"
grep -Fq -- '--storage-layout' <<<"$help_text" \
    || die "query binary does not expose --storage-layout"
grep -Fq -- '--label-materialization' <<<"$help_text" \
    || die "query binary does not expose --label-materialization"
grep -Fq -- '--query-label-storage' <<<"$help_text" \
    || die "query binary does not expose --query-label-storage"
grep -Fq -- 'owned-strings' <<<"$help_text" \
    || die "query binary does not expose owned-strings label storage"
grep -Fq -- 'schema6-ab' <<<"$help_text" \
    || die "query binary does not expose the schema6-ab layout"
grep -Fq -- 'schema7' <<<"$help_text" \
    || die "query binary does not expose the schema7 layout"
sha256sum -- "$RUN_BIN" >"$METADATA_DIR/query-binary.sha256"
printf 'source=%s\npreserved=%s\n' "$QUERY_BIN" "$RUN_BIN" \
    >"$METADATA_DIR/query-binary-paths.txt"

for harness_file in \
    schema7_query_ab_run.sh schema7_query_ab_gate.py fadvise_regular_dontneed.c; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$METADATA_DIR/$harness_file"
done
FROZEN_GATE_TOOL="$METADATA_DIR/schema7_query_ab_gate.py"
sha256sum \
    "$METADATA_DIR/schema7_query_ab_run.sh" \
    "$METADATA_DIR/schema7_query_ab_gate.py" \
    "$METADATA_DIR/fadvise_regular_dontneed.c" \
    >"$METADATA_DIR/harness.sha256"

git -C "$REPO_ROOT" rev-parse HEAD >"$METADATA_DIR/git-commit.txt"
git -C "$REPO_ROOT" status --porcelain=v2 --branch >"$METADATA_DIR/git-status.txt"
git -C "$REPO_ROOT" diff --binary --full-index HEAD -- \
    >"$METADATA_DIR/tracked-source.patch"

python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SCHEMA6_DIR" \
    --output "$INVENTORY_DIR/schema6.json" \
    --paths-output "$INVENTORY_DIR/schema6-files.nul"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SCHEMA7_DIR" \
    --output "$INVENTORY_DIR/schema7.json" \
    --paths-output "$INVENTORY_DIR/schema7-files.nul"
sha256sum "$INVENTORY_DIR/schema6.json" "$INVENTORY_DIR/schema7.json" \
    >"$INVENTORY_DIR/inventory.sha256"

cc -O2 -Wall -Wextra -Werror -o "$FADVISE_BIN" "$FADVISE_SOURCE"
sha256sum -- "$FADVISE_BIN" >"$METADATA_DIR/fadvise.sha256"

{
    printf 'query_name\texpression\n'
    for ((query_index = 0; query_index < ${#QUERY_NAMES[@]}; query_index++)); do
        printf '%s\t%s\n' "${QUERY_NAMES[$query_index]}" "${QUERY_EXPRESSIONS[$query_index]}"
    done
} >"$RESULT_DIR/queries.tsv"

format_order_for_repetition() {
    if (( $1 % 2 == 1 )); then
        printf 'schema6-ab schema7\n'
    else
        printf 'schema7 schema6-ab\n'
    fi
}

{
    printf 'process_label\trepetition\torder_index\tstorage_layout\tcorpus\n'
    for ((repetition = 1; repetition <= REPEATS; repetition++)); do
        read -r -a layouts <<<"$(format_order_for_repetition "$repetition")"
        for ((order_index = 0; order_index < 2; order_index++)); do
            layout="${layouts[$order_index]}"
            if [[ "$layout" == "schema6-ab" ]]; then
                corpus="$SCHEMA6_DIR"
            else
                corpus="$SCHEMA7_DIR"
            fi
            process_label="$(printf 'r%02d-%02d-%s' "$repetition" "$((order_index + 1))" "$layout")"
            printf '%s\t%s\t%s\t%s\t%s\n' \
                "$process_label" "$repetition" "$((order_index + 1))" "$layout" "$corpus"
        done
    done
} >"$RESULT_DIR/run-plan.tsv"

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'schema6_corpus=%s\n' "$SCHEMA6_DIR"
    printf 'schema7_corpus=%s\n' "$SCHEMA7_DIR"
    printf 'query_binary=%s\n' "$RUN_BIN"
    printf 'repeats=%s\n' "$REPEATS"
    printf 'benchmark_repeats=%s\n' "$BENCHMARK_REPEATS"
    printf 'start_ms=%s\n' "$START_MS"
    printf 'end_ms=%s\n' "$END_MS"
    printf 'chunk_read_mode=pread\n'
    printf 'chunk_read_queue_depth=%s\n' "$CHUNK_READ_QUEUE_DEPTH"
    printf 'label_materialization=full\n'
    printf 'max_resident_bytes_after_evict=%s\n' "$MAX_RESIDENT_BYTES_AFTER_EVICT"
    printf 'allow_noisy_host=%s\n' "$ALLOW_NOISY_HOST"
    printf 'run_note=%s\n' "$RUN_NOTE"
    printf 'schema7_footer_validation=one separate pre-measurement smoke pass plus complete validation during every benchmark store open\n'
    printf 'schema6_footer_validation=complete validation during every benchmark store open, both explicitly requested and required by schema6-ab\n'
    printf 'cache_note=POSIX_FADV_DONTNEED and fincore cover Linux page-cache residency only; every benchmark store then validates and warms all tracked corpus files before query timing; device/controller caches are not flushed\n'
    uname -a || true
    command -v rustc >/dev/null 2>&1 && rustc --version --verbose || true
    command -v cargo >/dev/null 2>&1 && cargo --version --verbose || true
    command -v lscpu >/dev/null 2>&1 && lscpu || true
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$SCHEMA6_DIR" || true
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$SCHEMA7_DIR" || true
    stat -f -c 'schema6_filesystem_type=%T schema6_mount=%m' "$SCHEMA6_DIR" || true
    stat -f -c 'schema7_filesystem_type=%T schema7_mount=%m' "$SCHEMA7_DIR" || true
    stat -f -c 'result_filesystem_type=%T result_mount=%m' "$RESULT_DIR" || true
    df -B1 "$RESULT_DIR" || true
    ulimit -a || true
    [[ -r /proc/meminfo ]] && cat /proc/meminfo || true
    ps -eo pid=,ppid=,comm=,args= || true
} >"$METADATA_DIR/environment.txt" 2>&1
printf '%s\n' "$RUN_NOTE" >"$METADATA_DIR/run-note.txt"

if [[ "$DRY_RUN" == "1" ]]; then
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    note "dry run complete; no footer validation, cache eviction, or query process was launched: $RESULT_DIR"
    exit 0
fi

check_measurement_conflicts() {
    local snapshot="$1"
    local conflicts
    ps -eo pid=,ppid=,comm=,args= >"$snapshot"
    conflicts="$(
        awk -v own="$$" '
            $1 != own && ($3 == "cargo" || $3 == "rustc" || $3 == "perf" ||
                $3 ~ /^chronoxide-/ || $3 ~ /^greptime/ || $3 == "prometheus") { print }
        ' "$snapshot"
    )"
    if [[ -n "$conflicts" && "$ALLOW_NOISY_HOST" != "1" ]]; then
        printf 'measurement conflict detected:\n%s\n' "$conflicts" >&2
        exit 70
    fi
    if [[ -n "$conflicts" ]]; then
        printf 'accepted measurement conflicts:\n%s\n' "$conflicts" >>"$snapshot"
    fi
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
    local layout="$2"
    local repetition="$3"
    local phase="$4"
    local paths_file="$5"
    local output="$6"
    local file line resident size
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
        file_count=$((file_count + 1))
        total_resident=$((total_resident + resident))
        total_size=$((total_size + size))
    done <"$paths_file"
    (( file_count > 0 )) || die "residency snapshot saw no corpus files"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$layout" "$repetition" "$phase" "$file_count" \
        "$total_resident" "$total_size" >>"$RESIDENCY_SUMMARY"
    printf '%s\n' "$total_resident"
}

note "validating all Schema 7 segment footers outside timed query processes"
check_measurement_conflicts "$METADATA_DIR/processes-before-schema7-footer-validation.txt"
set +e
/usr/bin/time -v -o "$METADATA_DIR/schema7-footer-validation.time.txt" \
    "$RUN_BIN" \
        --segments-dir "$SCHEMA7_DIR" \
        --storage-layout schema7 \
        --sample-limit-per-kind 0 \
        --validate-segment-footers \
        --output "$METADATA_DIR/schema7-footer-validation.md" \
        >"$METADATA_DIR/schema7-footer-validation.log" 2>&1
validation_status=$?
set -e
printf '%s\n' "$validation_status" >"$METADATA_DIR/schema7-footer-validation.exit-status"
(( validation_status == 0 )) \
    || die "Schema 7 footer validation failed; query measurements were not started"

printf 'process_label\tstorage_layout\trepetition\tphase\tfile_count\tresident_bytes\tcorpus_file_bytes\n' \
    >"$RESIDENCY_SUMMARY"
printf 'process_label\trepetition\torder_index\tstorage_layout\tcorpus\traw_output\tmax_rss_kib\n' \
    >"$RAW_INDEX"

run_process() {
    local repetition="$1"
    local order_index="$2"
    local layout="$3"
    local corpus paths_file process_label run_dir raw markdown log time_file
    local resident_after_evict max_rss_kib status
    local -a args

    if [[ "$layout" == "schema6-ab" ]]; then
        corpus="$SCHEMA6_DIR"
        paths_file="$INVENTORY_DIR/schema6-files.nul"
    else
        corpus="$SCHEMA7_DIR"
        paths_file="$INVENTORY_DIR/schema7-files.nul"
    fi
    process_label="$(printf 'r%02d-%02d-%s' "$repetition" "$order_index" "$layout")"
    run_dir="$RUNS_DIR/$process_label"
    [[ ! -e "$run_dir" ]] || die "refusing to reuse process directory: $run_dir"
    mkdir "$run_dir"
    raw="$run_dir/raw.json"
    markdown="$run_dir/report.md"
    log="$run_dir/query.log"
    time_file="$run_dir/time.txt"

    check_measurement_conflicts "$run_dir/processes-before.txt"
    evict_all_files "$paths_file"
    resident_after_evict="$(snapshot_residency \
        "$process_label" "$layout" "$repetition" after-evict \
        "$paths_file" "$run_dir/residency-after-evict.nul")"
    if (( resident_after_evict > MAX_RESIDENT_BYTES_AFTER_EVICT )); then
        die "resident corpus bytes after eviction are $resident_after_evict for $process_label; limit is $MAX_RESIDENT_BYTES_AFTER_EVICT"
    fi

    args=(
        --segments-dir "$corpus"
        --storage-layout "$layout"
        --label-materialization full
        --start-ms "$START_MS"
        --end-ms "$END_MS"
        --benchmark-repeats "$BENCHMARK_REPEATS"
        --chunk-read-mode pread
        --chunk-read-queue-depth "$CHUNK_READ_QUEUE_DEPTH"
        --query-label-storage owned-strings
        --query-max-series-matched "$QUERY_MAX_SERIES_MATCHED"
        --query-max-projected-series "$QUERY_MAX_PROJECTED_SERIES"
        --query-max-chunks-read "$QUERY_MAX_CHUNKS_READ"
        --query-max-bytes-read "$QUERY_MAX_BYTES_READ"
        --query-max-samples "$QUERY_MAX_SAMPLES"
        --regex-max-expanded-values "$REGEX_MAX_EXPANDED_VALUES"
        --validate-segment-footers
        --output "$markdown"
        --raw-output "$raw"
    )
    for query in "${QUERY_EXPRESSIONS[@]}"; do
        args+=(--query "$query")
    done

    note "running $process_label"
    set +e
    /usr/bin/time -v -o "$time_file" \
        "$RUN_BIN" "${args[@]}" >"$log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$run_dir/exit-status"
    if (( status != 0 )); then
        tail -n 50 "$log" >&2 || true
        die "$process_label failed with status $status; partial output was preserved"
    fi
    check_measurement_conflicts "$run_dir/processes-after.txt"
    snapshot_residency \
        "$process_label" "$layout" "$repetition" after-run \
        "$paths_file" "$run_dir/residency-after-run.nul" >/dev/null

    max_rss_kib="$(awk -F: '/Maximum resident set size/ {
        gsub(/^[[:space:]]+/, "", $2); print $2
    }' "$time_file")"
    [[ "$max_rss_kib" =~ ^[0-9]+$ ]] \
        || die "could not parse maximum RSS for $process_label"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$repetition" "$order_index" "$layout" "$corpus" \
        "$raw" "$max_rss_kib" >>"$RAW_INDEX"
}

for ((repetition = 1; repetition <= REPEATS; repetition++)); do
    read -r -a layouts <<<"$(format_order_for_repetition "$repetition")"
    for ((order_index = 0; order_index < 2; order_index++)); do
        run_process "$repetition" "$((order_index + 1))" "${layouts[$order_index]}"
    done
done

python3 "$FROZEN_GATE_TOOL" compare-results \
    --index "$RAW_INDEX" \
    --queries "$RESULT_DIR/queries.tsv" \
    --summary "$RESULT_DIR/summary.tsv" \
    --output "$COMPARISONS_DIR/query-equivalence.json" \
    --repeats "$REPEATS" \
    --benchmark-repeats "$BENCHMARK_REPEATS" \
    --start-ms "$START_MS" \
    --end-ms "$END_MS" \
    --queue-depth "$CHUNK_READ_QUEUE_DEPTH" \
    --max-matched-series "$QUERY_MAX_SERIES_MATCHED" \
    --max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
    --max-chunk-reads "$QUERY_MAX_CHUNKS_READ" \
    --max-bytes-read "$QUERY_MAX_BYTES_READ" \
    --max-samples-decoded "$QUERY_MAX_SAMPLES" \
    --max-regex-values-examined "$REGEX_MAX_EXPANDED_VALUES" \
    || die "query semantic fingerprint or QueryStats equivalence gate failed"

(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        sha256sum -- "${artifact#./}"
    done < <(find runs comparisons -type f -print0 | sort -z)
) >"$METADATA_DIR/result-artifacts.sha256"

touch "$RESULT_DIR/COMPLETE"
note "complete: $RESULT_DIR"
