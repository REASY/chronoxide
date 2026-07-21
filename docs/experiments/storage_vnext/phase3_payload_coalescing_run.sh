#!/usr/bin/env bash

# Controlled real-corpus Phase 3 payload-coalescing matrix. One artifact forces
# one backend and executes the gate-generated eight-block Williams schedule for
# gaps 0, 256, 1024, and 4096. Every process records one cold and two warm runs.

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE_TOOL="$SCRIPT_DIR/phase3_payload_coalescing_gate.py"
PHASE2_GATE_TOOL="$SCRIPT_DIR/phase2_compact_ids_ab_gate.py"
MANIFEST_TOOL="$SCRIPT_DIR/schema8_query_ab_gate.py"
COMMON_GATE_TOOL="$SCRIPT_DIR/schema7_query_ab_gate.py"
PHASE1_GATE_TOOL="$SCRIPT_DIR/phase1_query_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DEFAULT_SEGMENTS_DIR="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase1-4m-20260721T051609Z/runs/replay-01/segments"
DEFAULT_RESULT_PARENT="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide"
DEFAULT_QUERY_MANIFEST="$SCRIPT_DIR/phase2_compact_ids_queries.json"

DRY_RUN="${DRY_RUN:-0}"
BACKEND="${BACKEND:-}"
BENCHMARK_REPEATS=3
QUERY_LABEL_ARENA_MAX_BYTES=536870912
PREFLIGHT_QUEUE_DEPTH=8
RECOMMENDED_MEMLOCK_KIB=65536
CHUNK_READ_QUEUE_DEPTH="${CHUNK_READ_QUEUE_DEPTH:-}"
READBACK_SAMPLE_LIMIT_PER_KIND="${READBACK_SAMPLE_LIMIT_PER_KIND:-2}"
MAX_RESIDENT_BYTES_AFTER_EVICT="${MAX_RESIDENT_BYTES_AFTER_EVICT:-0}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
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
Usage (BACKEND is required):
  RUN_NOTE='quiet host; no build, replay, profiler, or unrelated DB work' \\
  QUIET_HOST_CONFIRMED=1 \\
    docs/experiments/storage_vnext/phase3_payload_coalescing_run.sh [--dry-run]

Required environment: BACKEND=pread or BACKEND=io-uring

Optional overrides:
  SEGMENTS_DIR=/absolute/schema8/segments
  QUERY_BIN=/absolute/release/chronoxide-query
  QUERY_MANIFEST=/absolute/representative-manifest.json
  RESULT_DIR=/absolute/new-output-directory
  RESULT_PARENT=/absolute/existing-parent
  CHUNK_READ_QUEUE_DEPTH=128|8

Defaults use the accepted Phase 1 4M corpus and the Phase 2 representative
manifest, sealed by SHA-256 in the gate. The fixed eight-block Williams plan
balances all four gaps at every order position and repeats the square twice.
One backend artifact contains 352 fresh processes, each with exactly one cold
and two warm evaluations using:

  --label-materialization demand-driven
  --query-label-storage compact-ids
  --query-label-arena-max-bytes 536870912
  --query-instrumentation off
  --chunk-read-mode pread|io-uring
  --chunk-payload-coalesce-max-gap-bytes 0|256|1024|4096

All range scalar caches are disabled. One copied raw-v13 release binary serves
both backend artifacts. Footer validation, independent readbacks, and a real
forced-io_uring setup preflight run before and outside timed query processes.
POSIX_FADV_DONTNEED plus fincore enforces the configured Linux page-cache
residency bound; it does not flush device/controller caches. Every output
directory is new and is never reused.
EOF
}

die() {
    echo "Phase 3 payload coalescing: $*" >&2
    exit 2
}

note() {
    echo "Phase 3 payload coalescing: $*"
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

for command in awk bash cc cmp cp date df find fincore git grep prlimit ps \
    python3 realpath sha256sum sort stat uname /usr/bin/time; do
    require_command "$command"
done
for harness_file in "$GATE_TOOL" "$PHASE2_GATE_TOOL" "$MANIFEST_TOOL" "$COMMON_GATE_TOOL" \
    "$PHASE1_GATE_TOOL" "$FADVISE_SOURCE"; do
    [[ -f "$harness_file" ]] || die "required harness file is missing: $harness_file"
done

require_bool DRY_RUN "$DRY_RUN"
require_bool QUIET_HOST_CONFIRMED "$QUIET_HOST_CONFIRMED"
require_bool ALLOW_NOISY_HOST "$ALLOW_NOISY_HOST"
[[ "$DRY_RUN" == "1" || "$QUIET_HOST_CONFIRMED" == "1" ]] \
    || die "non-dry measurement requires QUIET_HOST_CONFIRMED=1"
require_single_line RUN_NOTE "$RUN_NOTE"
if [[ "$ALLOW_NOISY_HOST" == "1" && "$RUN_NOTE" != *[Nn][Oo][Ii][Ss][Yy]* ]]; then
    die "ALLOW_NOISY_HOST=1 requires RUN_NOTE to contain the word noisy"
fi
[[ "$BACKEND" == "pread" || "$BACKEND" == "io-uring" ]] \
    || die "BACKEND must be pread or io-uring"
if [[ -z "$CHUNK_READ_QUEUE_DEPTH" ]]; then
    if [[ "$BACKEND" == "pread" ]]; then
        CHUNK_READ_QUEUE_DEPTH=128
    else
        CHUNK_READ_QUEUE_DEPTH=8
    fi
fi
[[ "$CHUNK_READ_QUEUE_DEPTH" =~ ^[1-9][0-9]*$ ]] \
    || die "CHUNK_READ_QUEUE_DEPTH must be positive"
[[ "$READBACK_SAMPLE_LIMIT_PER_KIND" =~ ^[1-9][0-9]*$ ]] \
    || die "READBACK_SAMPLE_LIMIT_PER_KIND must be positive"
[[ "$MAX_RESIDENT_BYTES_AFTER_EVICT" =~ ^[0-9]+$ ]] \
    || die "MAX_RESIDENT_BYTES_AFTER_EVICT must be non-negative"
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
    RESULT_DIR="$RESULT_PARENT/storage-vnext-phase3-payload-coalescing-$BACKEND-$(date +%Y%m%d-%H%M%S)"
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
FROZEN_GATE_TOOL="$METADATA_DIR/phase3_payload_coalescing_gate.py"
NORMALIZED_TSV="$RESULT_DIR/queries.tsv"
NORMALIZED_JSON="$RESULT_DIR/queries.normalized.json"
RUN_PLAN="$RESULT_DIR/run-plan.tsv"
RAW_INDEX="$RESULT_DIR/raw-index.tsv"
RESIDENCY_SUMMARY="$RESULT_DIR/residency-summary.tsv"

MEMLOCK_SOFT_KIB="$(ulimit -S -l)"
MEMLOCK_HARD_KIB="$(ulimit -H -l)"
[[ -n "$MEMLOCK_SOFT_KIB" && "$MEMLOCK_SOFT_KIB" != *$'\n'* ]] \
    || die "could not read the soft RLIMIT_MEMLOCK"
[[ -n "$MEMLOCK_HARD_KIB" && "$MEMLOCK_HARD_KIB" != *$'\n'* ]] \
    || die "could not read the hard RLIMIT_MEMLOCK"
{
    printf 'recommended_soft_kib=%s\n' "$RECOMMENDED_MEMLOCK_KIB"
    printf 'observed_soft_kib=%s\nobserved_hard_kib=%s\n' \
        "$MEMLOCK_SOFT_KIB" "$MEMLOCK_HARD_KIB"
    prlimit --pid "$$" --memlock
} >"$METADATA_DIR/memlock.txt"
if [[ "$MEMLOCK_SOFT_KIB" =~ ^[0-9]+$ ]] \
    && (( MEMLOCK_SOFT_KIB < RECOMMENDED_MEMLOCK_KIB )); then
    {
        printf 'coverage_warning=soft RLIMIT_MEMLOCK is below the 64 MiB benchmark recommendation\n'
        printf 'observed_soft_kib=%s\nrecommended_soft_kib=%s\n' \
            "$MEMLOCK_SOFT_KIB" "$RECOMMENDED_MEMLOCK_KIB"
        printf 'disposition=the real forced-io_uring queue-depth-8 preflight remains authoritative; this warning is not a setup failure\n'
    } >"$METADATA_DIR/io-uring-memlock-coverage-warning.txt"
    note "coverage warning: soft RLIMIT_MEMLOCK is ${MEMLOCK_SOFT_KIB} KiB; 65536 KiB is recommended, so the real forced-io_uring preflight is required"
fi

cp --reflink=auto --preserve=mode,timestamps -- "$QUERY_BIN" "$RUN_BIN"
cmp -s -- "$QUERY_BIN" "$RUN_BIN" || die "copied query binary differs from source"
[[ -x "$RUN_BIN" ]] || die "copied query binary is not executable"
help_text="$($RUN_BIN --help 2>&1)"
for required_help in '--storage-layout' '--label-materialization' \
    '--query-label-storage' '--query-label-arena-max-bytes' \
    '--chunk-payload-coalesce-max-gap-bytes' \
    '--query-instrumentation' '--range-scalar-cache-max-bytes' \
    '--verify-readbacks' '--validate-segment-footers' 'schema8' \
    'io-uring' 'compact-ids'; do
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

for harness_file in phase3_payload_coalescing_run.sh phase3_payload_coalescing_gate.py \
    phase2_compact_ids_ab_gate.py schema8_query_ab_gate.py \
    schema7_query_ab_gate.py phase1_query_gate.py \
    fadvise_regular_dontneed.c; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$METADATA_DIR/$harness_file"
done
cp --preserve=mode,timestamps -- "$QUERY_MANIFEST" \
    "$METADATA_DIR/query-manifest.input.json"
sha256sum \
    "$METADATA_DIR/phase3_payload_coalescing_run.sh" \
    "$METADATA_DIR/phase3_payload_coalescing_gate.py" \
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
    --source-manifest "$METADATA_DIR/query-manifest.input.json" \
    --output "$RUN_PLAN" \
    --backend "$BACKEND"
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
    printf 'blocks=8\nbenchmark_repeats=%s\n' "$BENCHMARK_REPEATS"
    printf 'williams_square=0,256,4096,1024|256,1024,0,4096|1024,4096,256,0|4096,0,1024,256\n'
    printf 'schedule_repetitions=2\nprocesses_per_gap_per_query=8\n'
    printf 'query_label_arena_max_bytes=%s\n' "$QUERY_LABEL_ARENA_MAX_BYTES"
    printf 'storage_layout=schema8\nchunk_read_mode=%s\n' "$BACKEND"
    printf 'chunk_read_queue_depth=%s\n' "$CHUNK_READ_QUEUE_DEPTH"
    printf 'label_materialization=demand-driven\nquery_instrumentation=off\n'
    printf 'range_scalar_cache_max_bytes=0\n'
    printf 'max_resident_bytes_after_evict=%s\n' "$MAX_RESIDENT_BYTES_AFTER_EVICT"
    printf 'payload_coalesce_gaps=0,256,1024,4096\n'
    printf 'io_uring_preflight_queue_depth=%s\n' "$PREFLIGHT_QUEUE_DEPTH"
    printf 'recommended_memlock_kib=%s\nobserved_soft_memlock_kib=%s\nobserved_hard_memlock_kib=%s\n' \
        "$RECOMMENDED_MEMLOCK_KIB" "$MEMLOCK_SOFT_KIB" "$MEMLOCK_HARD_KIB"
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
    local process_label="$1" block="$2" backend="$3" gap="$4" phase="$5" output="$6"
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
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$block" "$backend" "$gap" "$phase" "$file_count" \
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

printf 'process_label\tblock\tchunk_read_backend\tpayload_coalesce_max_gap_bytes\tphase\tfile_count\tresident_bytes\tcorpus_file_bytes\n' \
    >"$RESIDENCY_SUMMARY"
printf 'process_label\tquery_name\tcategory\tmode\tblock\torder_index\tchunk_read_backend\tpayload_coalesce_max_gap_bytes\tbinary_sha256\tcorpus\traw_output\tprocess_wall_seconds\tprocess_user_seconds\tprocess_system_seconds\tmax_rss_kib\n' \
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

run_io_uring_preflight() {
    local query_name=no_result
    local preflight_dir="$VALIDATION_DIR/io-uring-preflight"
    local raw="$preflight_dir/raw.json" report="$preflight_dir/report.md"
    local log="$preflight_dir/query.log" time_file="$preflight_dir/time.txt"
    local status
    local -a args

    [[ ! -e "$preflight_dir" ]] || die "refusing to reuse io_uring preflight directory"
    mkdir "$preflight_dir"
    args=(
        --segments-dir "$SEGMENTS_DIR"
        --storage-layout schema8
        --label-materialization demand-driven
        --query-label-storage compact-ids
        --query-label-arena-max-bytes "$QUERY_LABEL_ARENA_MAX_BYTES"
        --query-instrumentation off
        --start-ms "${QUERY_START[$query_name]}"
        --end-ms "${QUERY_END[$query_name]}"
        --benchmark-repeats 1
        --chunk-read-mode io-uring
        --chunk-read-queue-depth "$PREFLIGHT_QUEUE_DEPTH"
        --chunk-payload-coalesce-max-gap-bytes 0
        --query-max-series-matched "$QUERY_MAX_SERIES_MATCHED"
        --query-max-projected-series "$QUERY_MAX_PROJECTED_SERIES"
        --query-max-chunks-read "$QUERY_MAX_CHUNKS_READ"
        --query-max-bytes-read "$QUERY_MAX_BYTES_READ"
        --query-max-samples "$QUERY_MAX_SAMPLES"
        --regex-max-expanded-values "$REGEX_MAX_EXPANDED_VALUES"
        --output "$report"
        --raw-output "$raw"
        --query "${QUERY_EXPRESSION[$query_name]}"
    )
    [[ "${QUERY_STEP[$query_name]}" == "-" && "${QUERY_CACHE[$query_name]}" == "-" ]] \
        || die "sealed no-result preflight query is not instant"
    printf '%s\0' "$RUN_BIN" "${args[@]}" >"$preflight_dir/argv.nul"
    check_measurement_conflicts "$preflight_dir/processes-before.txt"
    note "running real forced-io_uring setup preflight"
    set +e
    /usr/bin/time -v -o "$time_file" \
        "$RUN_BIN" "${args[@]}" >"$log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$preflight_dir/exit-status"
    (( status == 0 )) || die "forced io_uring setup preflight failed"
    sha256sum -c "$METADATA_DIR/query-binary.sha256" >/dev/null \
        || die "query binary changed during io_uring preflight"
    python3 "$FROZEN_GATE_TOOL" validate-io-uring-preflight \
        --raw "$raw" \
        --binary "$RUN_BIN" \
        --corpus "$SEGMENTS_DIR" \
        --manifest "$NORMALIZED_JSON" \
        --source-manifest "$METADATA_DIR/query-manifest.input.json" \
        --query-name "$query_name" \
        --expected-queue-depth "$PREFLIGHT_QUEUE_DEPTH" \
        --arena-bytes "$QUERY_LABEL_ARENA_MAX_BYTES" \
        --max-matched-series "$QUERY_MAX_SERIES_MATCHED" \
        --max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
        --max-chunk-reads "$QUERY_MAX_CHUNKS_READ" \
        --max-bytes-read "$QUERY_MAX_BYTES_READ" \
        --max-samples-decoded "$QUERY_MAX_SAMPLES" \
        --max-regex-values-examined "$REGEX_MAX_EXPANDED_VALUES" \
        --output "$VALIDATION_DIR/io-uring-preflight.json"
}

run_io_uring_preflight

read_time_value() {
    local key="$1" file="$2"
    awk -F '\t' -v key="$key" '$1 == key { print $2 }' "$file"
}

run_process() {
    local process_label="$1" query_name="$2" category="$3" mode="$4"
    local block="$5" order_index="$6" backend="$7" gap="$8"
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
        "$process_label" "$block" "$backend" "$gap" after-evict \
        "$run_dir/residency-after-evict.nul")"
    if (( resident_after_evict > MAX_RESIDENT_BYTES_AFTER_EVICT )); then
        die "resident bytes after eviction are $resident_after_evict for $process_label; limit is $MAX_RESIDENT_BYTES_AFTER_EVICT"
    fi

    args=(
        --segments-dir "$SEGMENTS_DIR"
        --storage-layout schema8
        --label-materialization demand-driven
        --query-label-storage compact-ids
        --query-label-arena-max-bytes "$QUERY_LABEL_ARENA_MAX_BYTES"
        --query-instrumentation off
        --start-ms "$start_ms"
        --end-ms "$end_ms"
        --benchmark-repeats "$BENCHMARK_REPEATS"
        --chunk-read-mode "$backend"
        --chunk-read-queue-depth "$CHUNK_READ_QUEUE_DEPTH"
        --chunk-payload-coalesce-max-gap-bytes "$gap"
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
    snapshot_residency "$process_label" "$block" "$backend" "$gap" after-run \
        "$run_dir/residency-after-run.nul" >/dev/null

    wall_seconds="$(read_time_value process_wall_seconds "$time_file")"
    user_seconds="$(read_time_value process_user_seconds "$time_file")"
    system_seconds="$(read_time_value process_system_seconds "$time_file")"
    max_rss_kib="$(read_time_value max_rss_kib "$time_file")"
    [[ "$wall_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not parse wall time"
    [[ "$user_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not parse user time"
    [[ "$system_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not parse system time"
    [[ "$max_rss_kib" =~ ^[1-9][0-9]*$ ]] || die "could not parse maximum RSS"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$query_name" "$category" "$mode" "$block" \
        "$order_index" "$backend" "$gap" "$BINARY_SHA256" "$SEGMENTS_DIR" "$raw" \
        "$wall_seconds" "$user_seconds" "$system_seconds" "$max_rss_kib" \
        >>"$RAW_INDEX"
}

while IFS=$'\t' read -r process_label query_name category mode block order_index backend gap; do
    [[ "$process_label" != "process_label" ]] || continue
    run_process "$process_label" "$query_name" "$category" "$mode" \
        "$block" "$order_index" "$backend" "$gap"
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
    --source-manifest "$METADATA_DIR/query-manifest.input.json" \
    --inventory-before "$INVENTORY_DIR/before.json" \
    --inventory-after "$INVENTORY_DIR/after.json" \
    --residency "$RESIDENCY_SUMMARY" \
    --footer-validation "$VALIDATION_DIR/footer.json" \
    --readback-validation "$VALIDATION_DIR/readbacks.json" \
    --io-uring-preflight "$VALIDATION_DIR/io-uring-preflight.json" \
    --summary "$RESULT_DIR/summary.tsv" \
    --output "$COMPARISONS_DIR/result-gate.json" \
    --binary "$RUN_BIN" \
    --corpus "$SEGMENTS_DIR" \
    --runs-dir "$RUNS_DIR" \
    --backend "$BACKEND" \
    --arena-bytes "$QUERY_LABEL_ARENA_MAX_BYTES" \
    --queue-depth "$CHUNK_READ_QUEUE_DEPTH" \
    --preflight-queue-depth "$PREFLIGHT_QUEUE_DEPTH" \
    --max-resident-bytes-after-evict "$MAX_RESIDENT_BYTES_AFTER_EVICT" \
    --max-matched-series "$QUERY_MAX_SERIES_MATCHED" \
    --max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
    --max-chunk-reads "$QUERY_MAX_CHUNKS_READ" \
    --max-bytes-read "$QUERY_MAX_BYTES_READ" \
    --max-samples-decoded "$QUERY_MAX_SAMPLES" \
    --max-regex-values-examined "$REGEX_MAX_EXPANDED_VALUES" \
    || die "strict Phase 3 correctness/accounting gate failed"

(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        sha256sum -- "${artifact#./}"
    done < <(find validation runs comparisons inventory -type f -print0 | sort -z)
    sha256sum -- summary.tsv raw-index.tsv residency-summary.tsv \
        queries.tsv queries.normalized.json run-plan.tsv
) >"$METADATA_DIR/result-artifacts.sha256"

sha256sum -c "$METADATA_DIR/query-binary.sha256" >/dev/null
sha256sum -c "$METADATA_DIR/harness.sha256" >/dev/null
sha256sum -c "$METADATA_DIR/fadvise.sha256" >/dev/null
(
    cd "$RESULT_DIR"
    sha256sum -c metadata/result-artifacts.sha256 >/dev/null
)

touch "$RESULT_DIR/COMPLETE"
note "complete: $RESULT_DIR"
