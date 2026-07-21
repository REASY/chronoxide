#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE_TOOL="$SCRIPT_DIR/phase1_query_gate.py"
COMMON_GATE_TOOL="$SCRIPT_DIR/schema7_query_ab_gate.py"
QUERY_MANIFEST="${QUERY_MANIFEST:-$SCRIPT_DIR/phase1_query_matrix.json}"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DRY_RUN="${DRY_RUN:-0}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
RUN_NOTE="${RUN_NOTE:-}"
CHUNK_READ_QUEUE_DEPTH="${CHUNK_READ_QUEUE_DEPTH:-128}"
READBACK_SAMPLE_LIMIT_PER_KIND=2
BENCHMARK_REPEATS=3
MAX_RESIDENT_BYTES_AFTER_EVICT="${MAX_RESIDENT_BYTES_AFTER_EVICT:-0}"

QUERY_MAX_SERIES_MATCHED="${QUERY_MAX_SERIES_MATCHED:-1000000}"
QUERY_MAX_PROJECTED_SERIES="${QUERY_MAX_PROJECTED_SERIES:-2000000}"
QUERY_MAX_CHUNKS_READ="${QUERY_MAX_CHUNKS_READ:-5000000}"
QUERY_MAX_BYTES_READ="${QUERY_MAX_BYTES_READ:-2147483648}"
QUERY_MAX_SAMPLES="${QUERY_MAX_SAMPLES:-50000000}"
REGEX_MAX_EXPANDED_VALUES="${REGEX_MAX_EXPANDED_VALUES:-100000}"

usage() {
    cat <<'EOF'
Usage:
  SEGMENTS_DIR=/absolute/schema8/segments \
  QUERY_BIN=/absolute/chronoxide-query \
  RESULT_DIR=/absolute/new-result \
  RUN_NOTE='quiet host; no builds, replay, profiler, or unrelated DB work' \
  QUIET_HOST_CONFIRMED=1 \
    docs/experiments/storage_vnext/phase1_query_run.sh [--dry-run]

The runner accepts only the sealed Phase 1 matrix and deterministic 4M Schema 8
corpus identity. It copies one query binary, runs separate footer/readback
validation, then executes each matrix row in the fixed instrumentation schedule:

  off,detailed,detailed,off
  detailed,off,off,detailed
  off,detailed,detailed,off

Every fresh process performs exactly three evaluations: one CLI-cold run followed
by two warm runs. Before each process, all inventoried corpus files receive
POSIX_FADV_DONTNEED and fincore must report no more than the configured residency
threshold. This proves process-start residency, not that startup leaves every
file cold at the exact timed-query boundary.

--dry-run freezes provenance, validates and inventories the corpus, and writes
the complete plan. It launches neither smoke validation nor measured queries.
EOF
}

die() {
    echo "Phase 1 query baseline: $*" >&2
    exit 2
}

note() {
    echo "Phase 1 query baseline: $*"
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
    [[ "$value" == "0" || "$value" == "1" ]] || die "$name must be 0 or 1"
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

for command in awk bash cc cmp cp date df find fincore git grep ps python3 realpath sha256sum sort stat /usr/bin/time; do
    require_command "$command"
done
for harness_file in "$GATE_TOOL" "$COMMON_GATE_TOOL" "$QUERY_MANIFEST" "$FADVISE_SOURCE"; do
    [[ -f "$harness_file" ]] || die "required harness file is missing: $harness_file"
done
require_env SEGMENTS_DIR
require_env QUERY_BIN
require_env RESULT_DIR
require_bool DRY_RUN "$DRY_RUN"
require_bool QUIET_HOST_CONFIRMED "$QUIET_HOST_CONFIRMED"
[[ "$DRY_RUN" == "1" || "$QUIET_HOST_CONFIRMED" == "1" ]] \
    || die "non-dry measurement requires QUIET_HOST_CONFIRMED=1"
[[ -n "$RUN_NOTE" && "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
    || die "RUN_NOTE is required and must contain no tabs or newlines"
[[ "$CHUNK_READ_QUEUE_DEPTH" =~ ^[1-9][0-9]*$ ]] \
    || die "CHUNK_READ_QUEUE_DEPTH must be positive"
[[ "$MAX_RESIDENT_BYTES_AFTER_EVICT" =~ ^[0-9]+$ ]] \
    || die "MAX_RESIDENT_BYTES_AFTER_EVICT must be non-negative"
for limit_name in \
    QUERY_MAX_SERIES_MATCHED QUERY_MAX_PROJECTED_SERIES QUERY_MAX_CHUNKS_READ \
    QUERY_MAX_BYTES_READ QUERY_MAX_SAMPLES REGEX_MAX_EXPANDED_VALUES; do
    [[ "${!limit_name}" =~ ^[1-9][0-9]*$ ]] || die "$limit_name must be positive"
done

[[ "$SEGMENTS_DIR" == /* && -d "$SEGMENTS_DIR" ]] \
    || die "SEGMENTS_DIR must be an absolute directory"
SEGMENTS_DIR="$(realpath -e -- "$SEGMENTS_DIR")"
[[ "$SEGMENTS_DIR" != *$'\n'* && "$SEGMENTS_DIR" != *$'\t'* ]] \
    || die "SEGMENTS_DIR must contain no tabs or newlines"
[[ "$QUERY_BIN" == /* && -f "$QUERY_BIN" && -x "$QUERY_BIN" ]] \
    || die "QUERY_BIN must be an absolute executable regular file"
QUERY_BIN="$(realpath -e -- "$QUERY_BIN")"
QUERY_MANIFEST="$(realpath -e -- "$QUERY_MANIFEST")"

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
    "$SEGMENTS_DIR/"*) die "RESULT_DIR must not be inside the corpus" ;;
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
FROZEN_GATE_TOOL="$METADATA_DIR/phase1_query_gate.py"
NORMALIZED_JSON="$RESULT_DIR/queries.normalized.json"
NORMALIZED_TSV="$RESULT_DIR/queries.tsv"
RUN_PLAN="$RESULT_DIR/run-plan.tsv"
RAW_INDEX="$RESULT_DIR/raw-index.tsv"
RESIDENCY_SUMMARY="$RESULT_DIR/residency-summary.tsv"

cp --reflink=auto --preserve=mode,timestamps -- "$QUERY_BIN" "$RUN_BIN"
cmp -s -- "$QUERY_BIN" "$RUN_BIN" || die "copied query binary differs from source"
[[ -x "$RUN_BIN" ]] || die "copied query binary is not executable"
help_text="$($RUN_BIN --help 2>&1)"
for required_help in \
    '--storage-layout' '--label-materialization' '--range-scalar-cache-max-bytes' \
    '--query-label-storage' '--query-instrumentation' 'owned-strings' 'demand-driven' \
    'schema8' 'detailed' 'off'; do
    grep -Fq -- "$required_help" <<<"$help_text" \
        || die "query binary help is missing $required_help"
done
sha256sum -- "$RUN_BIN" >"$METADATA_DIR/query-binary.sha256"
printf 'source=%s\npreserved=%s\n' "$QUERY_BIN" "$RUN_BIN" \
    >"$METADATA_DIR/query-binary-paths.txt"

for harness_file in \
    phase1_query_run.sh phase1_query_gate.py schema7_query_ab_gate.py \
    fadvise_regular_dontneed.c phase1_query_matrix.json; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$METADATA_DIR/$harness_file"
done
cp --preserve=mode,timestamps -- "$QUERY_MANIFEST" \
    "$METADATA_DIR/query-manifest.input.json"
sha256sum \
    "$METADATA_DIR/phase1_query_run.sh" \
    "$METADATA_DIR/phase1_query_gate.py" \
    "$METADATA_DIR/schema7_query_ab_gate.py" \
    "$METADATA_DIR/fadvise_regular_dontneed.c" \
    "$METADATA_DIR/phase1_query_matrix.json" \
    "$METADATA_DIR/query-manifest.input.json" \
    >"$METADATA_DIR/harness.sha256"

python3 "$FROZEN_GATE_TOOL" normalize-manifest \
    --input "$METADATA_DIR/query-manifest.input.json" \
    --output-json "$NORMALIZED_JSON" \
    --output-tsv "$NORMALIZED_TSV"
python3 "$FROZEN_GATE_TOOL" write-plan \
    --manifest "$NORMALIZED_JSON" \
    --output "$RUN_PLAN"

note "hashing and validating the complete fixed Schema 8 corpus"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SEGMENTS_DIR" \
    --manifest "$NORMALIZED_JSON" \
    --output "$INVENTORY_DIR/before.json" \
    --paths-output "$INVENTORY_DIR/files.nul"
sha256sum "$INVENTORY_DIR/before.json" >"$INVENTORY_DIR/before.sha256"

cc -O2 -Wall -Wextra -Werror -o "$FADVISE_BIN" "$FADVISE_SOURCE"
sha256sum -- "$FADVISE_BIN" >"$METADATA_DIR/fadvise.sha256"

git -C "$REPO_ROOT" rev-parse HEAD >"$METADATA_DIR/git-commit.txt"
git -C "$REPO_ROOT" status --porcelain=v2 --branch >"$METADATA_DIR/git-status.txt"
git -C "$REPO_ROOT" diff --binary --full-index HEAD -- \
    >"$METADATA_DIR/tracked-source.patch"

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'quiet_host_confirmed=%s\n' "$QUIET_HOST_CONFIRMED"
    printf 'segments_dir=%s\n' "$SEGMENTS_DIR"
    printf 'query_binary=%s\n' "$RUN_BIN"
    printf 'query_manifest=%s\n' "$QUERY_MANIFEST"
    printf 'abba_schedule=off,detailed,detailed,off / detailed,off,off,detailed / off,detailed,detailed,off\n'
    printf 'benchmark_repeats=%s\n' "$BENCHMARK_REPEATS"
    printf 'run_kinds=cold,warm,warm\n'
    printf 'chunk_read_mode=pread\n'
    printf 'chunk_read_queue_depth=%s\n' "$CHUNK_READ_QUEUE_DEPTH"
    printf 'query_label_storage=owned-strings\n'
    printf 'prewarm_query_contexts=false\n'
    printf 'prefetch_query_data=false\n'
    printf 'max_resident_bytes_after_evict=%s\n' "$MAX_RESIDENT_BYTES_AFTER_EVICT"
    printf 'range_cache_budgets=fixed per matrix; 0 means disabled\n'
    printf 'footer_validation=separate untimed pass\n'
    printf 'readback_validation=38 expected/executed; zero skips/mismatches required\n'
    printf 'timed_footer_validation=forbidden by gate\n'
    printf 'cache_note=POSIX_FADV_DONTNEED plus fincore prove process-start Linux page-cache residency for all inventoried files; startup metadata/fingerprint work can touch files before the timed query; device/controller caches are not flushed\n'
    printf 'run_note=%s\n' "$RUN_NOTE"
} >"$METADATA_DIR/settings.txt"
printf '%s\n' "$RUN_NOTE" >"$METADATA_DIR/run-note.txt"

{
    date --iso-8601=seconds
    uname -a || true
    command -v rustc >/dev/null 2>&1 && rustc --version --verbose || true
    command -v cargo >/dev/null 2>&1 && cargo --version --verbose || true
    command -v lscpu >/dev/null 2>&1 && lscpu || true
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$SEGMENTS_DIR" || true
    stat -f -c 'corpus_filesystem_type=%T corpus_mount=%m' "$SEGMENTS_DIR" || true
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
    note "dry run complete; validation, eviction, and query processes were not launched: $RESULT_DIR"
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
    [[ -z "$conflicts" ]] || {
        printf 'measurement conflict detected:\n%s\n' "$conflicts" >&2
        exit 70
    }
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
    done <"$INVENTORY_DIR/files.nul"
}

snapshot_residency() {
    local process_label="$1"
    local block="$2"
    local instrumentation="$3"
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
    done <"$INVENTORY_DIR/files.nul"
    (( file_count > 0 )) || die "residency snapshot saw no corpus files"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$block" "$instrumentation" "$phase" "$file_count" \
        "$total_resident" "$total_size" >>"$RESIDENCY_SUMMARY"
    printf '%s\n' "$total_resident"
}

run_validation_passes() {
    local status
    note "validating every segment footer outside timed query processes"
    check_measurement_conflicts "$VALIDATION_DIR/processes-before-footer.txt"
    set +e
    /usr/bin/time -v -o "$VALIDATION_DIR/footer.time.txt" \
        "$RUN_BIN" \
            --segments-dir "$SEGMENTS_DIR" \
            --storage-layout schema8 \
            --sample-limit-per-kind 0 \
            --validate-segment-footers \
            --output "$VALIDATION_DIR/footer.md" \
            >"$VALIDATION_DIR/footer.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$VALIDATION_DIR/footer.exit-status"
    (( status == 0 )) || die "footer validation failed"
    python3 "$FROZEN_GATE_TOOL" validate-smoke-report \
        --kind footer \
        --report "$VALIDATION_DIR/footer.md" \
        --output "$VALIDATION_DIR/footer.json"

    note "running independent readback oracle outside timed query processes"
    check_measurement_conflicts "$VALIDATION_DIR/processes-before-readbacks.txt"
    set +e
    /usr/bin/time -v -o "$VALIDATION_DIR/readbacks.time.txt" \
        "$RUN_BIN" \
            --segments-dir "$SEGMENTS_DIR" \
            --storage-layout schema8 \
            --sample-limit-per-kind "$READBACK_SAMPLE_LIMIT_PER_KIND" \
            --verify-readbacks \
            --validate-segment-footers \
            --output "$VALIDATION_DIR/readbacks.md" \
            >"$VALIDATION_DIR/readbacks.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$VALIDATION_DIR/readbacks.exit-status"
    (( status == 0 )) || die "independent readback verification failed"
    python3 "$FROZEN_GATE_TOOL" validate-smoke-report \
        --kind readback \
        --report "$VALIDATION_DIR/readbacks.md" \
        --output "$VALIDATION_DIR/readbacks.json"
}

run_validation_passes

printf 'process_label\tabba_block\tquery_instrumentation\tphase\tfile_count\tresident_bytes\tcorpus_file_bytes\n' \
    >"$RESIDENCY_SUMMARY"
printf 'process_label\tquery_name\tcategory\tmode\tlabel_materialization\tabba_block\torder_index\tquery_instrumentation\tcorpus\traw_output\tmax_rss_kib\n' \
    >"$RAW_INDEX"

declare -A QUERY_START QUERY_END QUERY_STEP QUERY_CACHE QUERY_EXPRESSION
while IFS=$'\t' read -r query_name _category _mode start_ms end_ms step_ms \
    cache_bytes _materialization expression; do
    [[ "$query_name" != "query_name" ]] || continue
    QUERY_START["$query_name"]="$start_ms"
    QUERY_END["$query_name"]="$end_ms"
    QUERY_STEP["$query_name"]="$step_ms"
    QUERY_CACHE["$query_name"]="$cache_bytes"
    QUERY_EXPRESSION["$query_name"]="$expression"
done <"$NORMALIZED_TSV"

run_process() {
    local process_label="$1"
    local query_name="$2"
    local category="$3"
    local mode="$4"
    local materialization="$5"
    local block="$6"
    local order_index="$7"
    local instrumentation="$8"
    local run_dir raw markdown log time_file resident_after_evict max_rss_kib status
    local start_ms end_ms step_ms cache_bytes expression
    local -a args

    start_ms="${QUERY_START[$query_name]}"
    end_ms="${QUERY_END[$query_name]}"
    step_ms="${QUERY_STEP[$query_name]}"
    cache_bytes="${QUERY_CACHE[$query_name]}"
    expression="${QUERY_EXPRESSION[$query_name]}"
    run_dir="$RUNS_DIR/$process_label"
    [[ ! -e "$run_dir" ]] || die "refusing to reuse process directory: $run_dir"
    mkdir "$run_dir"
    raw="$run_dir/raw.json"
    markdown="$run_dir/report.md"
    log="$run_dir/query.log"
    time_file="$run_dir/time.txt"

    check_measurement_conflicts "$run_dir/processes-before.txt"
    snapshot_pressure "$run_dir/pressure-before.txt"
    evict_all_files
    resident_after_evict="$(snapshot_residency \
        "$process_label" "$block" "$instrumentation" after-evict \
        "$run_dir/residency-after-evict.nul")"
    if (( resident_after_evict > MAX_RESIDENT_BYTES_AFTER_EVICT )); then
        die "resident bytes after eviction are $resident_after_evict for $process_label; limit is $MAX_RESIDENT_BYTES_AFTER_EVICT"
    fi

    args=(
        --segments-dir "$SEGMENTS_DIR"
        --storage-layout schema8
        --label-materialization "$materialization"
        --query-label-storage owned-strings
        --query-instrumentation "$instrumentation"
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
    for argument in "${args[@]}"; do
        [[ "$argument" != "--validate-segment-footers" ]] \
            || die "internal error: footer validation entered timed arguments"
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
    snapshot_pressure "$run_dir/pressure-after.txt"
    check_measurement_conflicts "$run_dir/processes-after.txt"
    snapshot_residency \
        "$process_label" "$block" "$instrumentation" after-run \
        "$run_dir/residency-after-run.nul" >/dev/null

    max_rss_kib="$(awk -F: '/Maximum resident set size/ {
        gsub(/^[[:space:]]+/, "", $2); print $2
    }' "$time_file")"
    [[ "$max_rss_kib" =~ ^[1-9][0-9]*$ ]] \
        || die "could not parse positive maximum RSS for $process_label"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$query_name" "$category" "$mode" "$materialization" \
        "$block" "$order_index" "$instrumentation" "$SEGMENTS_DIR" "$raw" \
        "$max_rss_kib" >>"$RAW_INDEX"
}

while IFS=$'\t' read -r process_label query_name category mode materialization \
    block order_index instrumentation; do
    [[ "$process_label" != "process_label" ]] || continue
    run_process "$process_label" "$query_name" "$category" "$mode" \
        "$materialization" "$block" "$order_index" "$instrumentation"
done <"$RUN_PLAN"

note "re-inventorying the corpus to prove it remained immutable"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SEGMENTS_DIR" \
    --manifest "$NORMALIZED_JSON" \
    --output "$INVENTORY_DIR/after.json" \
    --paths-output "$INVENTORY_DIR/files-after.nul"
cmp -s "$INVENTORY_DIR/before.json" "$INVENTORY_DIR/after.json" \
    || die "Schema 8 corpus changed during the benchmark"
cmp -s "$INVENTORY_DIR/files.nul" "$INVENTORY_DIR/files-after.nul" \
    || die "Schema 8 corpus path set changed during the benchmark"
sha256sum "$INVENTORY_DIR/after.json" >"$INVENTORY_DIR/after.sha256"

python3 "$FROZEN_GATE_TOOL" compare-results \
    --index "$RAW_INDEX" \
    --manifest "$NORMALIZED_JSON" \
    --inventory-before "$INVENTORY_DIR/before.json" \
    --inventory-after "$INVENTORY_DIR/after.json" \
    --residency "$RESIDENCY_SUMMARY" \
    --footer-validation "$VALIDATION_DIR/footer.json" \
    --readback-validation "$VALIDATION_DIR/readbacks.json" \
    --summary "$RESULT_DIR/summary.tsv" \
    --output "$COMPARISONS_DIR/result-gate.json" \
    --queue-depth "$CHUNK_READ_QUEUE_DEPTH" \
    --max-resident-bytes-after-evict "$MAX_RESIDENT_BYTES_AFTER_EVICT" \
    --max-matched-series "$QUERY_MAX_SERIES_MATCHED" \
    --max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
    --max-chunk-reads "$QUERY_MAX_CHUNKS_READ" \
    --max-bytes-read "$QUERY_MAX_BYTES_READ" \
    --max-samples-decoded "$QUERY_MAX_SAMPLES" \
    --max-regex-values-examined "$REGEX_MAX_EXPANDED_VALUES" \
    || die "strict Phase 1 validation gate failed"

(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        sha256sum -- "${artifact#./}"
    done < <(find validation runs comparisons -type f -print0 | sort -z)
) >"$METADATA_DIR/result-artifacts.sha256"

touch "$RESULT_DIR/COMPLETE"
note "complete: $RESULT_DIR"
