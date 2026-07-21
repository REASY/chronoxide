#!/usr/bin/env bash

# Controlled real-corpus, same-binary CompactIds/OwnedStrings Phase 2 A/B.
# Odd query blocks use Owned-Compact-Compact-Owned and even blocks use the
# inverse Compact-Owned-Owned-Compact schedule. Each process owns a fresh query
# session and records one cold plus two warm evaluations.

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE_TOOL="$SCRIPT_DIR/phase2_compact_ids_ab_gate.py"
MANIFEST_TOOL="$SCRIPT_DIR/schema8_query_ab_gate.py"
COMMON_GATE_TOOL="$SCRIPT_DIR/schema7_query_ab_gate.py"
PHASE1_GATE_TOOL="$SCRIPT_DIR/phase1_query_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DEFAULT_SEGMENTS_DIR="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase1-4m-20260721T051609Z/runs/replay-01/segments"
DEFAULT_RESULT_PARENT="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide"
DEFAULT_QUERY_MANIFEST="$SCRIPT_DIR/phase2_compact_ids_queries.json"

DRY_RUN="${DRY_RUN:-0}"
BLOCKS="${BLOCKS:-4}"
BENCHMARK_REPEATS=3
QUERY_LABEL_ARENA_MAX_BYTES=536870912
CHUNK_READ_QUEUE_DEPTH="${CHUNK_READ_QUEUE_DEPTH:-128}"
READBACK_SAMPLE_LIMIT_PER_KIND="${READBACK_SAMPLE_LIMIT_PER_KIND:-2}"
MAX_RESIDENT_BYTES_AFTER_EVICT="${MAX_RESIDENT_BYTES_AFTER_EVICT:-0}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
ALLOW_NOISY_HOST="${ALLOW_NOISY_HOST:-0}"
RUN_NOTE="${RUN_NOTE:-}"

BROAD_QUERY_NAME="${BROAD_QUERY_NAME:-broad_raw_count_selector}"
BROAD_MIN_IMPROVEMENT_PCT="${BROAD_MIN_IMPROVEMENT_PCT:-5}"
BROAD_MIN_RSS_IMPROVEMENT_PCT="${BROAD_MIN_RSS_IMPROVEMENT_PCT:-5}"
CONTROL_MAX_REGRESSION_PCT="${CONTROL_MAX_REGRESSION_PCT:-3}"
CONTROL_MIN_MATERIAL_REGRESSION_NS="${CONTROL_MIN_MATERIAL_REGRESSION_NS:-1000000}"
RSS_MAX_REGRESSION_PCT="${RSS_MAX_REGRESSION_PCT:-3}"
RSS_MIN_MATERIAL_REGRESSION_KIB="${RSS_MIN_MATERIAL_REGRESSION_KIB:-16384}"

QUERY_MAX_SERIES_MATCHED="${QUERY_MAX_SERIES_MATCHED:-1000000}"
QUERY_MAX_PROJECTED_SERIES="${QUERY_MAX_PROJECTED_SERIES:-2000000}"
QUERY_MAX_CHUNKS_READ="${QUERY_MAX_CHUNKS_READ:-5000000}"
QUERY_MAX_BYTES_READ="${QUERY_MAX_BYTES_READ:-2147483648}"
QUERY_MAX_SAMPLES="${QUERY_MAX_SAMPLES:-50000000}"
REGEX_MAX_EXPANDED_VALUES="${REGEX_MAX_EXPANDED_VALUES:-100000}"

usage() {
    cat <<EOF
Usage:
  RUN_NOTE='quiet host; no build, replay, profiler, or unrelated DB work' \\
  QUIET_HOST_CONFIRMED=1 \\
    docs/experiments/storage_vnext/phase2_compact_ids_ab_run.sh [--dry-run]

Optional overrides:
  SEGMENTS_DIR=/absolute/schema8/segments
  QUERY_BIN=/absolute/release/chronoxide-query
  QUERY_MANIFEST=/absolute/representative-manifest.json
  RESULT_DIR=/absolute/new-output-directory
  RESULT_PARENT=/absolute/existing-parent
  BLOCKS=4

Defaults use the accepted Phase 1 4M corpus and the Phase 2 representative
manifest. Odd blocks use Owned-Compact-Compact-Owned and even blocks use
Compact-Owned-Owned-Compact, so each arm occupies each schedule position
equally. The default records eight fresh processes per arm. Every process runs
exactly one cold and two warm evaluations with:

  --label-materialization demand-driven
  --query-label-storage owned-strings|compact-ids
  --query-label-arena-max-bytes 536870912
  --query-instrumentation off
  --chunk-read-mode pread

All range scalar caches are disabled. One copied raw-v11 release binary serves
both arms. Footer validation and independent readbacks run before, and outside,
timed query processes. POSIX_FADV_DONTNEED plus fincore records Linux page-cache
residency; it does not flush device/controller caches.

The promotion gate requires at least 5% broad cold/warm latency and broad RSS
improvement by default. A control regression is material only when it exceeds
both 3% and 1 ms for latency, or both 3% and 16 MiB for RSS. Thresholds are
configurable through the corresponding environment variables.
Every output directory is new and is never reused.
EOF
}

die() {
    echo "Phase 2 CompactIds A/B: $*" >&2
    exit 2
}

note() {
    echo "Phase 2 CompactIds A/B: $*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

require_bool() {
    local name="$1" value="$2"
    [[ "$value" == "0" || "$value" == "1" ]] || die "$name must be 0 or 1"
}

require_single_line() {
    local name="$1" value="$2"
    [[ -n "$value" && "$value" != *$'\n'* && "$value" != *$'\t'* ]] \
        || die "$name is required and must contain no tabs or newlines"
}

for argument in "$@"; do
    case "$argument" in
        --dry-run) DRY_RUN=1 ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; die "unknown argument: $argument" ;;
    esac
done

for command in awk bash cc cmp cp date df find fincore git grep ps python3 \
    realpath sha256sum sort stat uname /usr/bin/time; do
    require_command "$command"
done
for harness_file in "$GATE_TOOL" "$MANIFEST_TOOL" "$COMMON_GATE_TOOL" \
    "$PHASE1_GATE_TOOL" "$FADVISE_SOURCE"; do
    [[ -f "$harness_file" ]] || die "required harness file is missing: $harness_file"
done

require_bool DRY_RUN "$DRY_RUN"
require_bool QUIET_HOST_CONFIRMED "$QUIET_HOST_CONFIRMED"
require_bool ALLOW_NOISY_HOST "$ALLOW_NOISY_HOST"
[[ "$DRY_RUN" == "1" || "$QUIET_HOST_CONFIRMED" == "1" ]] \
    || die "non-dry measurement requires QUIET_HOST_CONFIRMED=1"
require_single_line RUN_NOTE "$RUN_NOTE"
require_single_line BROAD_QUERY_NAME "$BROAD_QUERY_NAME"
if [[ "$ALLOW_NOISY_HOST" == "1" && "$RUN_NOTE" != *[Nn][Oo][Ii][Ss][Yy]* ]]; then
    die "ALLOW_NOISY_HOST=1 requires RUN_NOTE to contain the word noisy"
fi
[[ "$BLOCKS" =~ ^[1-9][0-9]*$ ]] || die "BLOCKS must be positive"
(( BLOCKS % 2 == 0 )) || die "BLOCKS must be even for position counterbalancing"
[[ "$CHUNK_READ_QUEUE_DEPTH" =~ ^[1-9][0-9]*$ ]] \
    || die "CHUNK_READ_QUEUE_DEPTH must be positive"
[[ "$READBACK_SAMPLE_LIMIT_PER_KIND" =~ ^[1-9][0-9]*$ ]] \
    || die "READBACK_SAMPLE_LIMIT_PER_KIND must be positive"
[[ "$MAX_RESIDENT_BYTES_AFTER_EVICT" =~ ^[0-9]+$ ]] \
    || die "MAX_RESIDENT_BYTES_AFTER_EVICT must be non-negative"
for threshold_name in BROAD_MIN_IMPROVEMENT_PCT BROAD_MIN_RSS_IMPROVEMENT_PCT \
    CONTROL_MAX_REGRESSION_PCT RSS_MAX_REGRESSION_PCT; do
    [[ "${!threshold_name}" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || die "$threshold_name must be a finite non-negative decimal"
done
for threshold_name in CONTROL_MIN_MATERIAL_REGRESSION_NS \
    RSS_MIN_MATERIAL_REGRESSION_KIB; do
    [[ "${!threshold_name}" =~ ^[0-9]+$ ]] \
        || die "$threshold_name must be a non-negative integer"
done
for limit_name in QUERY_MAX_SERIES_MATCHED QUERY_MAX_PROJECTED_SERIES \
    QUERY_MAX_CHUNKS_READ QUERY_MAX_BYTES_READ QUERY_MAX_SAMPLES \
    REGEX_MAX_EXPANDED_VALUES; do
    [[ "${!limit_name}" =~ ^[1-9][0-9]*$ ]] || die "$limit_name must be positive"
done

SEGMENTS_DIR="${SEGMENTS_DIR:-$DEFAULT_SEGMENTS_DIR}"
QUERY_BIN="${QUERY_BIN:-$REPO_ROOT/target/release/chronoxide-query}"
QUERY_MANIFEST="${QUERY_MANIFEST:-$DEFAULT_QUERY_MANIFEST}"
RESULT_PARENT="${RESULT_PARENT:-$DEFAULT_RESULT_PARENT}"

[[ "$SEGMENTS_DIR" == /* && -d "$SEGMENTS_DIR" ]] \
    || die "SEGMENTS_DIR must be an absolute existing directory"
SEGMENTS_DIR="$(realpath -e -- "$SEGMENTS_DIR")"
[[ "$SEGMENTS_DIR" != *$'\n'* && "$SEGMENTS_DIR" != *$'\t'* ]] \
    || die "SEGMENTS_DIR must contain no tabs or newlines"
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
    RESULT_DIR="$RESULT_PARENT/storage-vnext-phase2-compact-ids-ab-$(date +%Y%m%d-%H%M%S)"
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
FROZEN_GATE_TOOL="$METADATA_DIR/phase2_compact_ids_ab_gate.py"
NORMALIZED_TSV="$RESULT_DIR/queries.tsv"
NORMALIZED_JSON="$RESULT_DIR/queries.normalized.json"
RUN_PLAN="$RESULT_DIR/run-plan.tsv"
RAW_INDEX="$RESULT_DIR/raw-index.tsv"
RESIDENCY_SUMMARY="$RESULT_DIR/residency-summary.tsv"

cp --reflink=auto --preserve=mode,timestamps -- "$QUERY_BIN" "$RUN_BIN"
cmp -s -- "$QUERY_BIN" "$RUN_BIN" || die "copied query binary differs from source"
[[ -x "$RUN_BIN" ]] || die "copied query binary is not executable"
help_text="$($RUN_BIN --help 2>&1)"
for required_help in '--storage-layout' '--label-materialization' \
    '--query-label-storage' '--query-label-arena-max-bytes' \
    '--query-instrumentation' '--range-scalar-cache-max-bytes' \
    '--verify-readbacks' '--validate-segment-footers' 'schema8' \
    'owned-strings' 'compact-ids'; do
    grep -Fq -- "$required_help" <<<"$help_text" \
        || die "query binary help is missing $required_help"
done
printf '%s\n' "$help_text" >"$METADATA_DIR/query-help.txt"
sha256sum -- "$RUN_BIN" >"$METADATA_DIR/query-binary.sha256"
BINARY_SHA256="$(awk '{print $1}' "$METADATA_DIR/query-binary.sha256")"
[[ "$BINARY_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "could not hash copied query binary"
stat --printf='size_bytes=%s\nmtime=%y\ninode=%i\ndevice=%d\n' -- "$RUN_BIN" \
    >"$METADATA_DIR/query-binary.stat.txt"
printf 'source=%s\npreserved=%s\n' "$QUERY_BIN" "$RUN_BIN" \
    >"$METADATA_DIR/query-binary-paths.txt"

for harness_file in phase2_compact_ids_ab_run.sh phase2_compact_ids_ab_gate.py \
    schema8_query_ab_gate.py schema7_query_ab_gate.py phase1_query_gate.py \
    fadvise_regular_dontneed.c; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$METADATA_DIR/$harness_file"
done
cp --preserve=mode,timestamps -- "$QUERY_MANIFEST" \
    "$METADATA_DIR/query-manifest.input.json"
sha256sum \
    "$METADATA_DIR/phase2_compact_ids_ab_run.sh" \
    "$METADATA_DIR/phase2_compact_ids_ab_gate.py" \
    "$METADATA_DIR/schema8_query_ab_gate.py" \
    "$METADATA_DIR/schema7_query_ab_gate.py" \
    "$METADATA_DIR/phase1_query_gate.py" \
    "$METADATA_DIR/fadvise_regular_dontneed.c" \
    "$METADATA_DIR/query-manifest.input.json" \
    >"$METADATA_DIR/harness.sha256"

python3 "$FROZEN_GATE_TOOL" normalize-manifest \
    --input "$METADATA_DIR/query-manifest.input.json" \
    --output-tsv "$NORMALIZED_TSV" \
    --output-json "$NORMALIZED_JSON"
python3 "$FROZEN_GATE_TOOL" write-plan \
    --manifest "$NORMALIZED_JSON" \
    --output "$RUN_PLAN" \
    --blocks "$BLOCKS"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SEGMENTS_DIR" \
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
    printf 'corpus=%s\nquery_binary=%s\n' "$SEGMENTS_DIR" "$RUN_BIN"
    printf 'query_manifest=%s\n' "$QUERY_MANIFEST"
    printf 'blocks=%s\nbenchmark_repeats=%s\n' "$BLOCKS" "$BENCHMARK_REPEATS"
    printf 'schedule_odd=owned-strings,compact-ids,compact-ids,owned-strings\n'
    printf 'schedule_even=compact-ids,owned-strings,owned-strings,compact-ids\n'
    printf 'processes_per_arm_per_query=%s\n' "$((BLOCKS * 2))"
    printf 'query_label_arena_max_bytes=%s\n' "$QUERY_LABEL_ARENA_MAX_BYTES"
    printf 'storage_layout=schema8\nchunk_read_mode=pread\n'
    printf 'chunk_read_queue_depth=%s\n' "$CHUNK_READ_QUEUE_DEPTH"
    printf 'label_materialization=demand-driven\nquery_instrumentation=off\n'
    printf 'range_scalar_cache_max_bytes=0\n'
    printf 'max_resident_bytes_after_evict=%s\n' "$MAX_RESIDENT_BYTES_AFTER_EVICT"
    printf 'broad_query_name=%s\n' "$BROAD_QUERY_NAME"
    printf 'broad_min_improvement_pct=%s\n' "$BROAD_MIN_IMPROVEMENT_PCT"
    printf 'broad_min_rss_improvement_pct=%s\n' "$BROAD_MIN_RSS_IMPROVEMENT_PCT"
    printf 'control_max_regression_pct=%s\n' "$CONTROL_MAX_REGRESSION_PCT"
    printf 'control_min_material_regression_ns=%s\n' \
        "$CONTROL_MIN_MATERIAL_REGRESSION_NS"
    printf 'rss_max_regression_pct=%s\n' "$RSS_MAX_REGRESSION_PCT"
    printf 'rss_min_material_regression_kib=%s\n' \
        "$RSS_MIN_MATERIAL_REGRESSION_KIB"
    printf 'quiet_host_confirmed=%s\nallow_noisy_host=%s\n' \
        "$QUIET_HOST_CONFIRMED" "$ALLOW_NOISY_HOST"
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
    note "dry run complete; validation, eviction, and queries were not launched: $RESULT_DIR"
    exit 0
fi

check_measurement_conflicts() {
    local snapshot="$1" conflicts
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
    done <"$INVENTORY_DIR/files.nul"
}

snapshot_residency() {
    local process_label="$1" block="$2" policy="$3" phase="$4" output="$5"
    local file line resident size
    local file_count=0 total_resident=0 total_size=0
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
        "$process_label" "$block" "$policy" "$phase" "$file_count" \
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

printf 'process_label\tblock\tquery_label_storage\tphase\tfile_count\tresident_bytes\tcorpus_file_bytes\n' \
    >"$RESIDENCY_SUMMARY"
printf 'process_label\tquery_name\tcategory\tmode\tblock\torder_index\tquery_label_storage\tbinary_sha256\tcorpus\traw_output\tprocess_wall_seconds\tprocess_user_seconds\tprocess_system_seconds\tmax_rss_kib\n' \
    >"$RAW_INDEX"

declare -A QUERY_START QUERY_END QUERY_STEP QUERY_CACHE QUERY_BOUNDARIES QUERY_EXPRESSION
while IFS=$'\t' read -r query_name _category _mode start_ms end_ms step_ms \
    cache_bytes boundaries_csv expression; do
    [[ "$query_name" != "query_name" ]] || continue
    QUERY_START["$query_name"]="$start_ms"
    QUERY_END["$query_name"]="$end_ms"
    QUERY_STEP["$query_name"]="$step_ms"
    QUERY_CACHE["$query_name"]="$cache_bytes"
    QUERY_BOUNDARIES["$query_name"]="$boundaries_csv"
    QUERY_EXPRESSION["$query_name"]="$expression"
done <"$NORMALIZED_TSV"

read_time_value() {
    local key="$1" file="$2"
    awk -F '\t' -v key="$key" '$1 == key { print $2 }' "$file"
}

run_process() {
    local process_label="$1" query_name="$2" category="$3" mode="$4"
    local block="$5" order_index="$6" policy="$7"
    local start_ms end_ms step_ms cache_bytes boundaries_csv expression
    local run_dir raw markdown log time_file resident_after_evict status
    local wall_seconds user_seconds system_seconds max_rss_kib boundary
    local -a args boundaries

    start_ms="${QUERY_START[$query_name]}"
    end_ms="${QUERY_END[$query_name]}"
    step_ms="${QUERY_STEP[$query_name]}"
    cache_bytes="${QUERY_CACHE[$query_name]}"
    boundaries_csv="${QUERY_BOUNDARIES[$query_name]}"
    expression="${QUERY_EXPRESSION[$query_name]}"
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
        "$process_label" "$block" "$policy" after-evict \
        "$run_dir/residency-after-evict.nul")"
    if (( resident_after_evict > MAX_RESIDENT_BYTES_AFTER_EVICT )); then
        die "resident bytes after eviction are $resident_after_evict for $process_label; limit is $MAX_RESIDENT_BYTES_AFTER_EVICT"
    fi

    args=(
        --segments-dir "$SEGMENTS_DIR"
        --storage-layout schema8
        --label-materialization demand-driven
        --query-label-storage "$policy"
        --query-label-arena-max-bytes "$QUERY_LABEL_ARENA_MAX_BYTES"
        --query-instrumentation off
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
        [[ "$step_ms" != "-" && "$cache_bytes" == "0" ]] \
            || die "normalized range query must have a step and zero-byte cache"
        args+=(--step-ms "$step_ms" --range-scalar-cache-max-bytes 0)
    else
        [[ "$step_ms" == "-" && "$cache_bytes" == "-" ]] \
            || die "normalized instant query unexpectedly has range settings"
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
    printf '%s\0' "$RUN_BIN" "${args[@]}" >"$run_dir/argv.nul"

    note "running $process_label"
    set +e
    /usr/bin/time \
        -f $'process_wall_seconds\t%e\nprocess_user_seconds\t%U\nprocess_system_seconds\t%S\nmax_rss_kib\t%M\nexit_status\t%x' \
        -o "$time_file" "$RUN_BIN" "${args[@]}" >"$log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$run_dir/exit-status"
    if (( status != 0 )); then
        tail -n 50 "$log" >&2 || true
        die "$process_label failed with status $status; partial output was preserved"
    fi
    snapshot_pressure "$run_dir/pressure-after.txt"
    check_measurement_conflicts "$run_dir/processes-after.txt"
    snapshot_residency "$process_label" "$block" "$policy" after-run \
        "$run_dir/residency-after-run.nul" >/dev/null

    wall_seconds="$(read_time_value process_wall_seconds "$time_file")"
    user_seconds="$(read_time_value process_user_seconds "$time_file")"
    system_seconds="$(read_time_value process_system_seconds "$time_file")"
    max_rss_kib="$(read_time_value max_rss_kib "$time_file")"
    [[ "$wall_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not parse wall time"
    [[ "$user_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not parse user time"
    [[ "$system_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not parse system time"
    [[ "$max_rss_kib" =~ ^[1-9][0-9]*$ ]] || die "could not parse maximum RSS"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$query_name" "$category" "$mode" "$block" \
        "$order_index" "$policy" "$BINARY_SHA256" "$SEGMENTS_DIR" "$raw" \
        "$wall_seconds" "$user_seconds" "$system_seconds" "$max_rss_kib" \
        >>"$RAW_INDEX"
}

while IFS=$'\t' read -r process_label query_name category mode block order_index policy; do
    [[ "$process_label" != "process_label" ]] || continue
    run_process "$process_label" "$query_name" "$category" "$mode" \
        "$block" "$order_index" "$policy"
done <"$RUN_PLAN"

note "re-inventorying the corpus to prove it remained immutable"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SEGMENTS_DIR" \
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
    --binary "$RUN_BIN" \
    --corpus "$SEGMENTS_DIR" \
    --broad-query-name "$BROAD_QUERY_NAME" \
    --blocks "$BLOCKS" \
    --benchmark-repeats "$BENCHMARK_REPEATS" \
    --arena-bytes "$QUERY_LABEL_ARENA_MAX_BYTES" \
    --queue-depth "$CHUNK_READ_QUEUE_DEPTH" \
    --max-resident-bytes-after-evict "$MAX_RESIDENT_BYTES_AFTER_EVICT" \
    --max-matched-series "$QUERY_MAX_SERIES_MATCHED" \
    --max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
    --max-chunk-reads "$QUERY_MAX_CHUNKS_READ" \
    --max-bytes-read "$QUERY_MAX_BYTES_READ" \
    --max-samples-decoded "$QUERY_MAX_SAMPLES" \
    --max-regex-values-examined "$REGEX_MAX_EXPANDED_VALUES" \
    --broad-min-improvement-pct "$BROAD_MIN_IMPROVEMENT_PCT" \
    --broad-min-rss-improvement-pct "$BROAD_MIN_RSS_IMPROVEMENT_PCT" \
    --control-max-regression-pct "$CONTROL_MAX_REGRESSION_PCT" \
    --control-min-material-regression-ns "$CONTROL_MIN_MATERIAL_REGRESSION_NS" \
    --rss-max-regression-pct "$RSS_MAX_REGRESSION_PCT" \
    --rss-min-material-regression-kib "$RSS_MIN_MATERIAL_REGRESSION_KIB" \
    || die "strict Phase 2 correctness/accounting/performance gate failed"

(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        sha256sum -- "${artifact#./}"
    done < <(find validation runs comparisons inventory -type f -print0 | sort -z)
    sha256sum -- summary.tsv raw-index.tsv residency-summary.tsv \
        queries.tsv queries.normalized.json run-plan.tsv
) >"$METADATA_DIR/result-artifacts.sha256"

touch "$RESULT_DIR/COMPLETE"
note "complete: $RESULT_DIR"
