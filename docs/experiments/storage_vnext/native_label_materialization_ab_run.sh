#!/usr/bin/env bash

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE_TOOL="$SCRIPT_DIR/native_label_materialization_ab_gate.py"
SCHEMA8_GATE_TOOL="$SCRIPT_DIR/schema8_query_ab_gate.py"
COMMON_GATE_TOOL="$SCRIPT_DIR/schema7_query_ab_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DEFAULT_SCHEMA8_DIR="/run/media/user/8c0c2e73-2c76-4cfb-bc59-36559b9bfb10/data/chronoxide/storage-schema8-perf-4m-20260715-005902/segments"
DEFAULT_RESULT_PARENT="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide"
DEFAULT_QUERY_MANIFEST="$SCRIPT_DIR/native_label_materialization_queries.json"

DRY_RUN="${DRY_RUN:-0}"
REPEATS="${REPEATS:-4}"
BENCHMARK_REPEATS="${BENCHMARK_REPEATS:-2}"
DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES="${DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES:-0}"
CHUNK_READ_QUEUE_DEPTH="${CHUNK_READ_QUEUE_DEPTH:-128}"
READBACK_SAMPLE_LIMIT_PER_KIND="${READBACK_SAMPLE_LIMIT_PER_KIND:-2}"
MAX_RESIDENT_BYTES_AFTER_EVICT="${MAX_RESIDENT_BYTES_AFTER_EVICT:-0}"
ALLOW_NOISY_HOST="${ALLOW_NOISY_HOST:-0}"
RUN_NOTE="${RUN_NOTE:-}"

QUERY_MAX_SERIES_MATCHED="${QUERY_MAX_SERIES_MATCHED:-1000000}"
QUERY_MAX_PROJECTED_SERIES="${QUERY_MAX_PROJECTED_SERIES:-2000000}"
QUERY_MAX_CHUNKS_READ="${QUERY_MAX_CHUNKS_READ:-5000000}"
QUERY_MAX_BYTES_READ="${QUERY_MAX_BYTES_READ:-2147483648}"
QUERY_MAX_SAMPLES="${QUERY_MAX_SAMPLES:-50000000}"
REGEX_MAX_EXPANDED_VALUES="${REGEX_MAX_EXPANDED_VALUES:-100000}"

usage() {
    cat <<EOF
Usage:
  RUN_NOTE='controlled run; host otherwise quiet' \\
    docs/experiments/storage_vnext/native_label_materialization_ab_run.sh

Optional overrides:
  SCHEMA8_DIR=/absolute/schema8/segments
  QUERY_BIN=/absolute/chronoxide-query
  QUERY_MANIFEST=/absolute/native-query-matrix.json
  RESULT_DIR=/absolute/new-run-directory
  RESULT_PARENT=/absolute/existing-parent
  REPEATS=4 BENCHMARK_REPEATS=2

The verified host defaults are:
  SCHEMA8_DIR=$DEFAULT_SCHEMA8_DIR
  QUERY_BIN=$REPO_ROOT/target/release/chronoxide-query
  QUERY_MANIFEST=$DEFAULT_QUERY_MANIFEST
  RESULT_PARENT=$DEFAULT_RESULT_PARENT

Each query runs in a fresh process. Full and DemandDriven order alternates by
repetition, and each process performs exactly two evaluations (cold, warm).
The runner copies one binary and uses it for both policies. If the binary
exposes --query-label-storage, it is pinned to owned-strings; older binaries
implicitly use the same pre-interning representation.

Footer validation and independent readback verification run before timing.
Timed processes never validate footers. POSIX_FADV_DONTNEED plus fincore cover
Linux page-cache residency only; they do not flush device/controller caches.

Set ALLOW_NOISY_HOST=1 only intentionally, and include "noisy" in RUN_NOTE.
Outputs are always created in a new run-specific directory and never reused.
EOF
}

die() {
    echo "native label-materialization A/B: $*" >&2
    exit 2
}

note() {
    echo "native label-materialization A/B: $*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
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

for command in awk bash cc cmp cp date df find fincore git grep ps python3 \
    realpath sha256sum sort stat uname /usr/bin/time; do
    require_command "$command"
done
for harness_file in \
    "$GATE_TOOL" "$SCHEMA8_GATE_TOOL" "$COMMON_GATE_TOOL" "$FADVISE_SOURCE"; do
    [[ -f "$harness_file" ]] || die "required harness file is missing: $harness_file"
done

require_bool DRY_RUN "$DRY_RUN"
require_bool ALLOW_NOISY_HOST "$ALLOW_NOISY_HOST"
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || die "REPEATS must be positive"
[[ "$BENCHMARK_REPEATS" == "2" ]] \
    || die "BENCHMARK_REPEATS must be exactly 2 (one cold and one warm run)"
[[ "$DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES" =~ ^[0-9]+$ ]] \
    || die "DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES must be non-negative"
[[ "$CHUNK_READ_QUEUE_DEPTH" =~ ^[1-9][0-9]*$ ]] \
    || die "CHUNK_READ_QUEUE_DEPTH must be positive"
[[ "$READBACK_SAMPLE_LIMIT_PER_KIND" =~ ^[1-9][0-9]*$ ]] \
    || die "READBACK_SAMPLE_LIMIT_PER_KIND must be positive"
[[ "$MAX_RESIDENT_BYTES_AFTER_EVICT" =~ ^[0-9]+$ ]] \
    || die "MAX_RESIDENT_BYTES_AFTER_EVICT must be non-negative"
for limit_name in \
    QUERY_MAX_SERIES_MATCHED QUERY_MAX_PROJECTED_SERIES QUERY_MAX_CHUNKS_READ \
    QUERY_MAX_BYTES_READ QUERY_MAX_SAMPLES REGEX_MAX_EXPANDED_VALUES; do
    [[ "${!limit_name}" =~ ^[1-9][0-9]*$ ]] \
        || die "$limit_name must be positive"
done
[[ -n "$RUN_NOTE" && "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
    || die "RUN_NOTE is required and must contain no tabs or newlines"
if [[ "$ALLOW_NOISY_HOST" == "1" && "$RUN_NOTE" != *[Nn][Oo][Ii][Ss][Yy]* ]]; then
    die "ALLOW_NOISY_HOST=1 requires RUN_NOTE to contain the word noisy"
fi

SCHEMA8_DIR="${SCHEMA8_DIR:-$DEFAULT_SCHEMA8_DIR}"
QUERY_BIN="${QUERY_BIN:-$REPO_ROOT/target/release/chronoxide-query}"
QUERY_MANIFEST="${QUERY_MANIFEST:-$DEFAULT_QUERY_MANIFEST}"
RESULT_PARENT="${RESULT_PARENT:-$DEFAULT_RESULT_PARENT}"

[[ "$SCHEMA8_DIR" == /* && -d "$SCHEMA8_DIR" ]] \
    || die "SCHEMA8_DIR must be an absolute directory"
SCHEMA8_DIR="$(realpath -e -- "$SCHEMA8_DIR")"
[[ "$SCHEMA8_DIR" != *$'\n'* && "$SCHEMA8_DIR" != *$'\t'* ]] \
    || die "SCHEMA8_DIR must contain no tabs or newlines"
[[ "$QUERY_BIN" == /* && -f "$QUERY_BIN" && -x "$QUERY_BIN" ]] \
    || die "QUERY_BIN must be an absolute executable regular file"
QUERY_BIN="$(realpath -e -- "$QUERY_BIN")"
[[ "$QUERY_MANIFEST" == /* && -f "$QUERY_MANIFEST" ]] \
    || die "QUERY_MANIFEST must be an absolute regular file"
QUERY_MANIFEST="$(realpath -e -- "$QUERY_MANIFEST")"

if [[ -z "${RESULT_DIR:-}" ]]; then
    [[ "$RESULT_PARENT" == /* && -d "$RESULT_PARENT" ]] \
        || die "RESULT_PARENT must be an absolute existing directory"
    RESULT_PARENT="$(realpath -e -- "$RESULT_PARENT")"
    RESULT_DIR="$RESULT_PARENT/native-label-materialization-ab-$(date +%Y%m%d-%H%M%S)"
fi
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
    "$SCHEMA8_DIR/"*) die "RESULT_DIR must not be inside the corpus" ;;
esac

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/metadata" "$RESULT_DIR/inventory" "$RESULT_DIR/validation" \
    "$RESULT_DIR/runs" "$RESULT_DIR/comparisons"
METADATA_DIR="$RESULT_DIR/metadata"
INVENTORY_DIR="$RESULT_DIR/inventory"
VALIDATION_DIR="$RESULT_DIR/validation"
RUNS_DIR="$RESULT_DIR/runs"
COMPARISONS_DIR="$RESULT_DIR/comparisons"
RUN_BIN="$METADATA_DIR/chronoxide-query"
FADVISE_BIN="$METADATA_DIR/fadvise-regular-dontneed"
FROZEN_GATE_TOOL="$METADATA_DIR/native_label_materialization_ab_gate.py"
NORMALIZED_TSV="$RESULT_DIR/queries.tsv"
NORMALIZED_JSON="$RESULT_DIR/queries.normalized.json"
RAW_INDEX="$RESULT_DIR/raw-index.tsv"
RESIDENCY_SUMMARY="$RESULT_DIR/residency-summary.tsv"

cp --reflink=auto --preserve=mode,timestamps -- "$QUERY_BIN" "$RUN_BIN"
cmp -s -- "$QUERY_BIN" "$RUN_BIN" || die "copied query binary differs from source"
[[ -x "$RUN_BIN" ]] || die "copied query binary is not executable"
help_text="$($RUN_BIN --help 2>&1)"
for required_help in \
    '--storage-layout' '--label-materialization' '--range-scalar-cache-max-bytes' \
    '--verify-readbacks' '--validate-segment-footers' 'schema8'; do
    grep -Fq -- "$required_help" <<<"$help_text" \
        || die "query binary help is missing $required_help"
done
LABEL_STORAGE_MODE="legacy-owned-strings"
LABEL_STORAGE_ARGS=()
if grep -Fq -- '--query-label-storage' <<<"$help_text"; then
    grep -Fq -- 'owned-strings' <<<"$help_text" \
        || die "query binary exposes --query-label-storage without owned-strings"
    LABEL_STORAGE_MODE="owned-strings"
    LABEL_STORAGE_ARGS=(--query-label-storage owned-strings)
fi
sha256sum -- "$RUN_BIN" >"$METADATA_DIR/query-binary.sha256"
printf 'source=%s\npreserved=%s\n' "$QUERY_BIN" "$RUN_BIN" \
    >"$METADATA_DIR/query-binary-paths.txt"

for harness_file in \
    native_label_materialization_ab_run.sh \
    native_label_materialization_ab_gate.py \
    schema8_query_ab_gate.py schema7_query_ab_gate.py \
    fadvise_regular_dontneed.c; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$METADATA_DIR/$harness_file"
done
cp --preserve=mode,timestamps -- "$QUERY_MANIFEST" \
    "$METADATA_DIR/query-manifest.input.json"
sha256sum \
    "$METADATA_DIR/native_label_materialization_ab_run.sh" \
    "$METADATA_DIR/native_label_materialization_ab_gate.py" \
    "$METADATA_DIR/schema8_query_ab_gate.py" \
    "$METADATA_DIR/schema7_query_ab_gate.py" \
    "$METADATA_DIR/fadvise_regular_dontneed.c" \
    "$METADATA_DIR/query-manifest.input.json" \
    >"$METADATA_DIR/harness.sha256"

python3 "$FROZEN_GATE_TOOL" normalize-manifest \
    --input "$METADATA_DIR/query-manifest.input.json" \
    --output-tsv "$NORMALIZED_TSV" \
    --output-json "$NORMALIZED_JSON" \
    --default-range-cache-bytes "$DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SCHEMA8_DIR" \
    --output "$INVENTORY_DIR/schema8.json" \
    --paths-output "$INVENTORY_DIR/schema8-files.nul"
sha256sum "$INVENTORY_DIR/schema8.json" >"$INVENTORY_DIR/inventory.sha256"

cc -O2 -Wall -Wextra -Werror -o "$FADVISE_BIN" "$FADVISE_SOURCE"
sha256sum -- "$FADVISE_BIN" >"$METADATA_DIR/fadvise.sha256"

git -C "$REPO_ROOT" rev-parse HEAD >"$METADATA_DIR/git-commit.txt"
git -C "$REPO_ROOT" status --porcelain=v2 --branch >"$METADATA_DIR/git-status.txt"
git -C "$REPO_ROOT" diff --binary --full-index HEAD -- \
    >"$METADATA_DIR/tracked-source.patch"

policy_order_for_repetition() {
    if (( $1 % 2 == 1 )); then
        printf 'full demand-driven\n'
    else
        printf 'demand-driven full\n'
    fi
}

{
    printf 'process_label\tquery_name\tcategory\tmode\trepetition\torder_index\tlabel_materialization\tcorpus\n'
    while IFS=$'\t' read -r query_name category mode _start _end _step _cache \
        _boundaries _expression; do
        [[ "$query_name" != "query_name" ]] || continue
        for ((repetition = 1; repetition <= REPEATS; repetition++)); do
            read -r -a policies <<<"$(policy_order_for_repetition "$repetition")"
            for ((order_index = 0; order_index < 2; order_index++)); do
                policy="${policies[$order_index]}"
                process_label="$(printf '%s-r%02d-%02d-%s' \
                    "$query_name" "$repetition" "$((order_index + 1))" "$policy")"
                printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                    "$process_label" "$query_name" "$category" "$mode" \
                    "$repetition" "$((order_index + 1))" "$policy" "$SCHEMA8_DIR"
            done
        done
    done <"$NORMALIZED_TSV"
} >"$RESULT_DIR/run-plan.tsv"

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'schema8_corpus=%s\n' "$SCHEMA8_DIR"
    printf 'query_binary=%s\n' "$RUN_BIN"
    printf 'query_manifest=%s\n' "$QUERY_MANIFEST"
    printf 'repeats=%s\n' "$REPEATS"
    printf 'benchmark_repeats=%s\n' "$BENCHMARK_REPEATS"
    printf 'chunk_read_mode=pread\n'
    printf 'chunk_read_queue_depth=%s\n' "$CHUNK_READ_QUEUE_DEPTH"
    printf 'query_label_storage=%s\n' "$LABEL_STORAGE_MODE"
    printf 'default_range_scalar_cache_max_bytes=%s\n' \
        "$DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES"
    printf 'max_resident_bytes_after_evict=%s\n' "$MAX_RESIDENT_BYTES_AFTER_EVICT"
    printf 'allow_noisy_host=%s\n' "$ALLOW_NOISY_HOST"
    printf 'run_note=%s\n' "$RUN_NOTE"
    printf 'footer_validation=separate pre-measurement pass\n'
    printf 'readback_validation=separate pre-measurement pass\n'
    printf 'timed_footer_validation=forbidden and enforced by raw-output gate\n'
    printf 'cache_note=POSIX_FADV_DONTNEED and fincore cover Linux page-cache residency only; device/controller caches are not flushed\n'
} >"$METADATA_DIR/settings.txt"
printf '%s\n' "$RUN_NOTE" >"$METADATA_DIR/run-note.txt"

{
    date --iso-8601=seconds
    uname -a || true
    command -v rustc >/dev/null 2>&1 && rustc --version --verbose || true
    command -v cargo >/dev/null 2>&1 && cargo --version --verbose || true
    command -v lscpu >/dev/null 2>&1 && lscpu || true
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$SCHEMA8_DIR" || true
    stat -f -c 'schema8_filesystem_type=%T schema8_mount=%m' "$SCHEMA8_DIR" || true
    stat -f -c 'result_filesystem_type=%T result_mount=%m' "$RESULT_DIR" || true
    df -B1 "$RESULT_DIR" || true
    ulimit -a || true
    [[ -r /proc/meminfo ]] && cat /proc/meminfo || true
    for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
        [[ -r "$pressure" ]] && { printf '%s\n' "$pressure"; cat "$pressure"; }
    done
    ps -eo pid=,ppid=,comm=,args= || true
} >"$METADATA_DIR/environment.txt" 2>&1

if [[ "$DRY_RUN" == "1" ]]; then
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    note "dry run complete; validation, eviction, and queries were not launched: $RESULT_DIR"
    exit 0
fi

check_measurement_conflicts() {
    local snapshot="$1"
    local conflicts
    ps -eo pid=,ppid=,comm=,args= >"$snapshot"
    conflicts="$(
        awk -v own="$$" '
            $1 != own && ($3 == "cargo" || $3 == "rustc" ||
                $3 == "rustdoc" || $3 == "clippy-driver" ||
                $3 == "nextest" || $3 == "perf" ||
                $3 ~ /^chronoxide-/ || $3 ~ /^greptime/ ||
                $3 == "prometheus") { print }
        ' "$snapshot"
    )"
    if [[ -n "$conflicts" && "$ALLOW_NOISY_HOST" != "1" ]]; then
        printf 'measurement conflict detected:\n%s\n' "$conflicts" >&2
        exit 70
    fi
    if [[ -n "$conflicts" ]]; then
        printf 'accepted noisy-host conflicts:\n%s\n' "$conflicts" >>"$snapshot"
    fi
}

snapshot_pressure() {
    local output="$1"
    {
        date --iso-8601=ns
        cat /proc/loadavg 2>/dev/null || true
        for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
            [[ -r "$pressure" ]] && { printf '%s\n' "$pressure"; cat "$pressure"; }
        done
    } >"$output"
}

evict_all_files() {
    local file
    while IFS= read -r -d '' file; do
        "$FADVISE_BIN" "$file"
    done <"$INVENTORY_DIR/schema8-files.nul"
}

snapshot_residency() {
    local process_label="$1"
    local repetition="$2"
    local policy="$3"
    local phase="$4"
    local output="$5"
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
    done <"$INVENTORY_DIR/schema8-files.nul"
    (( file_count > 0 )) || die "residency snapshot saw no corpus files"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$repetition" "$policy" "$phase" "$file_count" \
        "$total_resident" "$total_size" >>"$RESIDENCY_SUMMARY"
    printf '%s\n' "$total_resident"
}

run_validation_passes() {
    local status
    note "validating every Schema 8 footer outside timed query processes"
    check_measurement_conflicts "$VALIDATION_DIR/processes-before-footer.txt"
    set +e
    /usr/bin/time -v -o "$VALIDATION_DIR/footer.time.txt" \
        "$RUN_BIN" \
            --segments-dir "$SCHEMA8_DIR" \
            --storage-layout schema8 \
            --sample-limit-per-kind 0 \
            --validate-segment-footers \
            --output "$VALIDATION_DIR/footer.md" \
            >"$VALIDATION_DIR/footer.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$VALIDATION_DIR/footer.exit-status"
    (( status == 0 )) || die "Schema 8 footer validation failed"

    note "running independent Schema 8 readbacks outside timed query processes"
    check_measurement_conflicts "$VALIDATION_DIR/processes-before-readbacks.txt"
    set +e
    /usr/bin/time -v -o "$VALIDATION_DIR/readbacks.time.txt" \
        "$RUN_BIN" \
            --segments-dir "$SCHEMA8_DIR" \
            --storage-layout schema8 \
            --sample-limit-per-kind "$READBACK_SAMPLE_LIMIT_PER_KIND" \
            --verify-readbacks \
            --output "$VALIDATION_DIR/readbacks.md" \
            >"$VALIDATION_DIR/readbacks.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$VALIDATION_DIR/readbacks.exit-status"
    (( status == 0 )) || die "Schema 8 independent readback verification failed"
}

run_validation_passes

printf 'process_label\trepetition\tlabel_materialization\tphase\tfile_count\tresident_bytes\tcorpus_file_bytes\n' \
    >"$RESIDENCY_SUMMARY"
printf 'process_label\tquery_name\tcategory\tmode\trepetition\torder_index\tlabel_materialization\tcorpus\traw_output\tprocess_wall_seconds\tprocess_user_seconds\tprocess_system_seconds\tmax_rss_kib\n' \
    >"$RAW_INDEX"

read_time_value() {
    local key="$1"
    local file="$2"
    awk -F '\t' -v key="$key" '$1 == key { print $2 }' "$file"
}

run_process() {
    local query_name="$1"
    local category="$2"
    local mode="$3"
    local start_ms="$4"
    local end_ms="$5"
    local step_ms="$6"
    local cache_bytes="$7"
    local boundaries_csv="$8"
    local expression="$9"
    local repetition="${10}"
    local order_index="${11}"
    local policy="${12}"
    local process_label run_dir raw markdown log time_file
    local resident_after_evict status boundary
    local wall_seconds user_seconds system_seconds max_rss_kib
    local -a args boundaries

    process_label="$(printf '%s-r%02d-%02d-%s' \
        "$query_name" "$repetition" "$order_index" "$policy")"
    run_dir="$RUNS_DIR/$process_label"
    [[ ! -e "$run_dir" ]] || die "refusing to reuse process directory: $run_dir"
    mkdir "$run_dir"
    raw="$run_dir/raw.json"
    markdown="$run_dir/report.md"
    log="$run_dir/query.log"
    time_file="$run_dir/time.tsv"

    check_measurement_conflicts "$run_dir/processes-before.txt"
    snapshot_pressure "$run_dir/pressure-before.txt"
    evict_all_files
    resident_after_evict="$(snapshot_residency \
        "$process_label" "$repetition" "$policy" after-evict \
        "$run_dir/residency-after-evict.nul")"
    if (( resident_after_evict > MAX_RESIDENT_BYTES_AFTER_EVICT )); then
        die "resident bytes after eviction are $resident_after_evict for $process_label; limit is $MAX_RESIDENT_BYTES_AFTER_EVICT"
    fi

    args=(
        --segments-dir "$SCHEMA8_DIR"
        --storage-layout schema8
        --label-materialization "$policy"
        "${LABEL_STORAGE_ARGS[@]}"
        --start-ms "$start_ms"
        --end-ms "$end_ms"
        --benchmark-repeats "$BENCHMARK_REPEATS"
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
        --query "$expression"
    )
    if [[ "$mode" == "range" ]]; then
        [[ "$step_ms" != "-" && "$cache_bytes" != "-" ]] \
            || die "normalized range query lacks step/cache values"
        args+=(--step-ms "$step_ms" --range-scalar-cache-max-bytes "$cache_bytes")
    else
        [[ "$step_ms" == "-" && "$cache_bytes" == "-" ]] \
            || die "normalized instant query unexpectedly has step/cache values"
    fi
    if [[ "$boundaries_csv" != "-" ]]; then
        IFS=',' read -r -a boundaries <<<"$boundaries_csv"
        for boundary in "${boundaries[@]}"; do
            args+=(--exponential-histogram-bucket-boundary "$boundary")
        done
    fi
    for argument in "${args[@]}"; do
        [[ "$argument" != "--validate-segment-footers" ]] \
            || die "internal error: footer validation entered timed arguments"
    done

    note "running $process_label"
    set +e
    /usr/bin/time \
        -f $'process_wall_seconds\t%e\nprocess_user_seconds\t%U\nprocess_system_seconds\t%S\nmax_rss_kib\t%M\nexit_status\t%x' \
        -o "$time_file" \
        "$RUN_BIN" "${args[@]}" >"$log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$run_dir/exit-status"
    if (( status != 0 )); then
        tail -n 50 "$log" >&2 || true
        die "$process_label failed with status $status; partial output was preserved"
    fi
    snapshot_pressure "$run_dir/pressure-after.txt"
    check_measurement_conflicts "$run_dir/processes-after.txt"
    snapshot_residency \
        "$process_label" "$repetition" "$policy" after-run \
        "$run_dir/residency-after-run.nul" >/dev/null

    wall_seconds="$(read_time_value process_wall_seconds "$time_file")"
    user_seconds="$(read_time_value process_user_seconds "$time_file")"
    system_seconds="$(read_time_value process_system_seconds "$time_file")"
    max_rss_kib="$(read_time_value max_rss_kib "$time_file")"
    [[ "$wall_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || die "could not parse wall time for $process_label"
    [[ "$user_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || die "could not parse user time for $process_label"
    [[ "$system_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || die "could not parse system time for $process_label"
    [[ "$max_rss_kib" =~ ^[0-9]+$ ]] \
        || die "could not parse maximum RSS for $process_label"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$query_name" "$category" "$mode" "$repetition" \
        "$order_index" "$policy" "$SCHEMA8_DIR" "$raw" "$wall_seconds" \
        "$user_seconds" "$system_seconds" "$max_rss_kib" >>"$RAW_INDEX"
}

while IFS=$'\t' read -r query_name category mode start_ms end_ms step_ms \
    cache_bytes boundaries_csv expression; do
    [[ "$query_name" != "query_name" ]] || continue
    for ((repetition = 1; repetition <= REPEATS; repetition++)); do
        read -r -a policies <<<"$(policy_order_for_repetition "$repetition")"
        for ((order_index = 0; order_index < 2; order_index++)); do
            run_process \
                "$query_name" "$category" "$mode" "$start_ms" "$end_ms" \
                "$step_ms" "$cache_bytes" "$boundaries_csv" "$expression" \
                "$repetition" "$((order_index + 1))" "${policies[$order_index]}"
        done
    done
done <"$NORMALIZED_TSV"

python3 "$FROZEN_GATE_TOOL" compare-results \
    --index "$RAW_INDEX" \
    --manifest "$NORMALIZED_JSON" \
    --summary "$RESULT_DIR/summary.tsv" \
    --output "$COMPARISONS_DIR/query-equivalence.json" \
    --repeats "$REPEATS" \
    --benchmark-repeats "$BENCHMARK_REPEATS" \
    --queue-depth "$CHUNK_READ_QUEUE_DEPTH" \
    --max-matched-series "$QUERY_MAX_SERIES_MATCHED" \
    --max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
    --max-chunk-reads "$QUERY_MAX_CHUNKS_READ" \
    --max-bytes-read "$QUERY_MAX_BYTES_READ" \
    --max-samples-decoded "$QUERY_MAX_SAMPLES" \
    --max-regex-values-examined "$REGEX_MAX_EXPANDED_VALUES" \
    || die "semantic, QueryStats, counter, or materialization gate failed"

note "re-inventorying the Schema 8 corpus to prove it stayed immutable"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SCHEMA8_DIR" \
    --output "$INVENTORY_DIR/schema8-after.json" \
    --paths-output "$INVENTORY_DIR/schema8-files-after.nul"
cmp -s "$INVENTORY_DIR/schema8.json" "$INVENTORY_DIR/schema8-after.json" \
    || die "Schema 8 corpus changed during the benchmark"
cmp -s "$INVENTORY_DIR/schema8-files.nul" \
    "$INVENTORY_DIR/schema8-files-after.nul" \
    || die "Schema 8 corpus path set changed during the benchmark"
sha256sum "$INVENTORY_DIR/schema8-after.json" \
    >"$INVENTORY_DIR/inventory-after.sha256"

(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        sha256sum -- "${artifact#./}"
    done < <(find validation runs comparisons -type f -print0 | sort -z)
    sha256sum -- summary.tsv raw-index.tsv residency-summary.tsv \
        queries.tsv queries.normalized.json run-plan.tsv
) >"$METADATA_DIR/result-artifacts.sha256"

touch "$RESULT_DIR/COMPLETE"
note "complete: $RESULT_DIR"
