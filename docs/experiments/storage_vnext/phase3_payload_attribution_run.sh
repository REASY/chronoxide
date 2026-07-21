#!/usr/bin/env bash

# Observer-heavy Phase 3 support run. This is diagnostic stage attribution,
# not a headline latency benchmark: Detailed timers perturb hot query paths.

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE_TOOL="$SCRIPT_DIR/phase3_payload_attribution_gate.py"
PHASE3_GATE_TOOL="$SCRIPT_DIR/phase3_payload_coalescing_gate.py"
PHASE2_GATE_TOOL="$SCRIPT_DIR/phase2_compact_ids_ab_gate.py"
MANIFEST_TOOL="$SCRIPT_DIR/schema8_query_ab_gate.py"
COMMON_GATE_TOOL="$SCRIPT_DIR/schema7_query_ab_gate.py"
PHASE1_GATE_TOOL="$SCRIPT_DIR/phase1_query_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DEFAULT_SEGMENTS_DIR="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase1-4m-20260721T051609Z/runs/replay-01/segments"
DEFAULT_RESULT_PARENT="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide"
DEFAULT_QUERY_MANIFEST="$SCRIPT_DIR/phase2_compact_ids_queries.json"

DRY_RUN="${DRY_RUN:-0}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
ALLOW_NOISY_HOST="${ALLOW_NOISY_HOST:-0}"
RUN_NOTE="${RUN_NOTE:-}"
BENCHMARK_REPEATS=2
QUERY_LABEL_ARENA_MAX_BYTES=536870912
PREAD_QUEUE_DEPTH=128
IO_URING_QUEUE_DEPTH=8
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
  RUN_NOTE='quiet diagnostic host; Detailed wall is non-comparable' \
  QUIET_HOST_CONFIRMED=1 \
    docs/experiments/storage_vnext/phase3_payload_attribution_run.sh [--dry-run]

Optional overrides:
  SEGMENTS_DIR=/absolute/schema8/segments
  QUERY_BIN=/absolute/release/chronoxide-query
  QUERY_MANIFEST=/absolute/phase2_compact_ids_queries.json
  RESULT_DIR=/absolute/new-output-directory
  RESULT_PARENT=/absolute/existing-parent
  MAX_RESIDENT_BYTES_AFTER_EVICT=0

This supporting diagnostic runs 24 fresh processes: four sealed representative
queries x forced pread/io-uring x gaps 0/1024/4096. Each process records one
cold and one warm evaluation with Detailed instrumentation. Detailed query and
process wall times MUST NOT be compared with instrumentation-off headline
latency. The gate reports only observer-heavy stage attribution.

No build, footer validation, readback validation, or profiler runs inside a
timed query process. The already-built final binary is copied once and used by
every backend/gap arm. Page-cache eviction evidence covers corpus files only.
EOF
}

die() {
    echo "Phase 3 payload attribution: $*" >&2
    exit 2
}

note() {
    echo "Phase 3 payload attribution: $*"
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
for harness_file in "$GATE_TOOL" "$PHASE3_GATE_TOOL" "$PHASE2_GATE_TOOL" \
    "$MANIFEST_TOOL" "$COMMON_GATE_TOOL" "$PHASE1_GATE_TOOL" \
    "$FADVISE_SOURCE"; do
    [[ -f "$harness_file" ]] || die "required harness file is missing: $harness_file"
done

require_bool DRY_RUN "$DRY_RUN"
require_bool QUIET_HOST_CONFIRMED "$QUIET_HOST_CONFIRMED"
require_bool ALLOW_NOISY_HOST "$ALLOW_NOISY_HOST"
[[ "$DRY_RUN" == "1" || "$QUIET_HOST_CONFIRMED" == "1" ]] \
    || die "non-dry attribution requires QUIET_HOST_CONFIRMED=1"
require_single_line RUN_NOTE "$RUN_NOTE"
if [[ "$ALLOW_NOISY_HOST" == "1" && "$RUN_NOTE" != *[Nn][Oo][Ii][Ss][Yy]* ]]; then
    die "ALLOW_NOISY_HOST=1 requires RUN_NOTE to contain the word noisy"
fi
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
    RESULT_DIR="$RESULT_PARENT/storage-vnext-phase3-payload-attribution-$(date +%Y%m%d-%H%M%S)"
fi
[[ "$RESULT_DIR" == /* ]] || die "RESULT_DIR must be absolute"
result_name="$(basename "$RESULT_DIR")"
[[ -n "$result_name" && "$result_name" != "." && "$result_name" != ".." ]] \
    || die "RESULT_DIR must name a new child of an existing directory"
result_parent="$(realpath -e -- "$(dirname "$RESULT_DIR")")"
RESULT_DIR="$result_parent/$result_name"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists; outputs are never reused"
case "$RESULT_DIR/" in
    "$SEGMENTS_DIR/"*) die "RESULT_DIR must not be inside the corpus" ;;
esac

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/metadata" "$RESULT_DIR/inventory" \
    "$RESULT_DIR/runs" "$RESULT_DIR/comparisons"
METADATA_DIR="$RESULT_DIR/metadata"
INVENTORY_DIR="$RESULT_DIR/inventory"
RUNS_DIR="$RESULT_DIR/runs"
COMPARISONS_DIR="$RESULT_DIR/comparisons"
RUN_BIN="$METADATA_DIR/chronoxide-query"
FADVISE_BIN="$METADATA_DIR/fadvise-regular-dontneed"
FROZEN_GATE_TOOL="$METADATA_DIR/phase3_payload_attribution_gate.py"
NORMALIZED_TSV="$RESULT_DIR/queries.tsv"
NORMALIZED_JSON="$RESULT_DIR/queries.normalized.json"
RUN_PLAN="$RESULT_DIR/run-plan.tsv"
RAW_INDEX="$RESULT_DIR/raw-index.tsv"
RESIDENCY_SUMMARY="$RESULT_DIR/residency-summary.tsv"

cp --reflink=auto --preserve=mode,timestamps -- "$QUERY_BIN" "$RUN_BIN"
cmp -s -- "$QUERY_BIN" "$RUN_BIN" || die "copied query binary differs from source"
[[ -x "$RUN_BIN" ]] || die "copied query binary is not executable"
help_text="$($RUN_BIN --help 2>&1)"
for required_help in '--storage-layout' '--query-instrumentation' 'detailed' \
    '--query-label-storage' 'compact-ids' '--chunk-read-mode' 'io-uring' \
    '--chunk-payload-coalesce-max-gap-bytes' '--range-scalar-cache-max-bytes'; do
    grep -Fq -- "$required_help" <<<"$help_text" \
        || die "query binary help is missing $required_help"
done
printf '%s\n' "$help_text" >"$METADATA_DIR/query-help.txt"
sha256sum -- "$RUN_BIN" >"$METADATA_DIR/query-binary.sha256"
BINARY_SHA256="$(awk '{print $1}' "$METADATA_DIR/query-binary.sha256")"
[[ "$BINARY_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "could not hash copied binary"
stat --printf='size_bytes=%s\nmtime=%y\ninode=%i\ndevice=%d\n' -- "$RUN_BIN" \
    >"$METADATA_DIR/query-binary.stat.txt"

for harness_file in phase3_payload_attribution_run.sh \
    phase3_payload_attribution_gate.py phase3_payload_coalescing_gate.py \
    phase2_compact_ids_ab_gate.py schema8_query_ab_gate.py \
    schema7_query_ab_gate.py phase1_query_gate.py fadvise_regular_dontneed.c; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$METADATA_DIR/$harness_file"
done
cp --preserve=mode,timestamps -- "$QUERY_MANIFEST" \
    "$METADATA_DIR/query-manifest.input.json"
sha256sum \
    "$METADATA_DIR/phase3_payload_attribution_run.sh" \
    "$METADATA_DIR/phase3_payload_attribution_gate.py" \
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
    --output "$RUN_PLAN"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SEGMENTS_DIR" \
    --output "$INVENTORY_DIR/before.json" \
    --paths-output "$INVENTORY_DIR/files.nul"

# This helper build is setup-only and completes before any timed query process.
cc -O2 -Wall -Wextra -Werror -o "$FADVISE_BIN" "$FADVISE_SOURCE"
sha256sum -- "$FADVISE_BIN" >"$METADATA_DIR/fadvise.sha256"

git -C "$REPO_ROOT" rev-parse HEAD >"$METADATA_DIR/git-commit.txt"
git -C "$REPO_ROOT" status --porcelain=v2 --branch >"$METADATA_DIR/git-status.txt"
git -C "$REPO_ROOT" diff --binary --full-index HEAD -- \
    >"$METADATA_DIR/tracked-source.patch"

memlock_kib="$(ulimit -l)"
{
    printf 'ulimit_memlock_kib=%s\n' "$memlock_kib"
    printf 'recommended_memlock_kib=65536\n'
    printf 'gate=forced io_uring payload-bearing process must succeed; recommendation is not a hard limit\n'
    prlimit --pid "$$" --memlock || true
} >"$METADATA_DIR/memlock.txt" 2>&1
if [[ "$memlock_kib" =~ ^[0-9]+$ ]] && (( memlock_kib < 65536 )); then
    note "coverage warning: RLIMIT_MEMLOCK ${memlock_kib} KiB is below the 64 MiB recommendation; forced io_uring execution remains the evidence gate"
    printf 'coverage_warning=below_64_mib_recommendation\n' \
        >>"$METADATA_DIR/memlock.txt"
else
    printf 'coverage_warning=none\n' >>"$METADATA_DIR/memlock.txt"
fi

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\ncorpus=%s\nquery_binary=%s\n' \
        "$DRY_RUN" "$SEGMENTS_DIR" "$RUN_BIN"
    printf 'query_manifest=%s\n' "$QUERY_MANIFEST"
    printf 'queries=broad_raw_count_selector,scalar_rate_sum_instant,scalar_rate_sum_range,native_hist_count_range\n'
    printf 'backends=pread,io-uring\ngaps=0,1024,4096\n'
    printf 'processes=24\nbenchmark_repeats=%s\n' "$BENCHMARK_REPEATS"
    printf 'pread_queue_depth=%s\nio_uring_queue_depth=%s\n' \
        "$PREAD_QUEUE_DEPTH" "$IO_URING_QUEUE_DEPTH"
    printf 'storage_layout=schema8\nquery_label_storage=compact-ids\n'
    printf 'label_materialization=demand-driven\nquery_instrumentation=detailed\n'
    printf 'range_scalar_cache_max_bytes=0 for range queries\n'
    printf 'timing_comparability=Detailed query/process wall is observer-heavy diagnostic evidence and MUST NOT be used as headline latency\n'
    printf 'footer_validation=not run by this supporting attribution harness\n'
    printf 'readback_validation=not run by this supporting attribution harness\n'
    printf 'profiler=not run by this harness\n'
    printf 'max_resident_bytes_after_evict=%s\n' "$MAX_RESIDENT_BYTES_AFTER_EVICT"
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
    [[ -r /proc/meminfo ]] && sed -n '1,80p' /proc/meminfo || true
    ps -eo pid=,ppid=,comm=,args= || true
} >"$METADATA_DIR/environment.txt" 2>&1

if [[ "$DRY_RUN" == "1" ]]; then
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    note "dry run complete; no query, eviction, footer, readback, or profiler process ran: $RESULT_DIR"
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
        sed -n '1p' /proc/loadavg 2>/dev/null || true
        for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
            [[ -r "$pressure" ]] && { printf '%s\n' "$pressure"; sed -n '1,20p' "$pressure"; }
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
    local process_label="$1" backend="$2" gap="$3" phase="$4" output="$5"
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
        "$process_label" "$backend" "$gap" "$phase" "$file_count" \
        "$total_resident" "$total_size" >>"$RESIDENCY_SUMMARY"
    printf '%s\n' "$total_resident"
}

printf 'process_label\tchunk_read_backend\tpayload_coalesce_max_gap_bytes\tphase\tfile_count\tresident_bytes\tcorpus_file_bytes\n' \
    >"$RESIDENCY_SUMMARY"
printf 'process_label\tquery_name\tcategory\tmode\torder_index\tchunk_read_backend\tpayload_coalesce_max_gap_bytes\tbinary_sha256\tcorpus\traw_output\tprocess_wall_seconds\tprocess_user_seconds\tprocess_system_seconds\tmax_rss_kib\n' \
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
    local order_index="$5" backend="$6" gap="$7"
    local start_ms end_ms step_ms cache_bytes boundaries_csv expression queue_depth
    local run_dir raw report log time_file resident_after_evict status boundary
    local wall_seconds user_seconds system_seconds max_rss_kib
    local -a args boundaries

    start_ms="${QUERY_START[$query_name]}"
    end_ms="${QUERY_END[$query_name]}"
    step_ms="${QUERY_STEP[$query_name]}"
    cache_bytes="${QUERY_CACHE[$query_name]}"
    boundaries_csv="${QUERY_BOUNDARIES[$query_name]}"
    expression="${QUERY_EXPRESSION[$query_name]}"
    if [[ "$backend" == "pread" ]]; then
        queue_depth="$PREAD_QUEUE_DEPTH"
    elif [[ "$backend" == "io-uring" ]]; then
        queue_depth="$IO_URING_QUEUE_DEPTH"
    else
        die "run plan contains an unsupported backend: $backend"
    fi
    run_dir="$RUNS_DIR/$process_label"
    [[ ! -e "$run_dir" ]] || die "refusing to reuse process directory: $run_dir"
    mkdir "$run_dir"
    raw="$run_dir/raw.json"
    report="$run_dir/report.md"
    log="$run_dir/query.log"
    time_file="$run_dir/time.tsv"

    check_measurement_conflicts "$run_dir/processes-before.txt"
    snapshot_pressure "$run_dir/pressure-before.txt"
    evict_all_files
    resident_after_evict="$(snapshot_residency \
        "$process_label" "$backend" "$gap" after-evict \
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
        --query-instrumentation detailed
        --start-ms "$start_ms"
        --end-ms "$end_ms"
        --benchmark-repeats "$BENCHMARK_REPEATS"
        --chunk-read-mode "$backend"
        --chunk-read-queue-depth "$queue_depth"
        --chunk-payload-coalesce-max-gap-bytes "$gap"
        --query-max-series-matched "$QUERY_MAX_SERIES_MATCHED"
        --query-max-projected-series "$QUERY_MAX_PROJECTED_SERIES"
        --query-max-chunks-read "$QUERY_MAX_CHUNKS_READ"
        --query-max-bytes-read "$QUERY_MAX_BYTES_READ"
        --query-max-samples "$QUERY_MAX_SAMPLES"
        --regex-max-expanded-values "$REGEX_MAX_EXPANDED_VALUES"
        --output "$report"
        --raw-output "$raw"
        --query "$expression"
    )
    if [[ "$mode" == "range" ]]; then
        [[ "$step_ms" != "-" && "$cache_bytes" == "0" ]] \
            || die "normalized range query must have a step and zero cache"
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
        case "$argument" in
            --validate-segment-footers|--verify-readbacks|perf|cargo|rustc)
                die "forbidden timed-process argument: $argument" ;;
        esac
    done
    printf '%s\0' "$RUN_BIN" "${args[@]}" >"$run_dir/argv.nul"

    note "running observer-heavy diagnostic $process_label (wall non-comparable)"
    set +e
    /usr/bin/time \
        -f $'process_wall_seconds\t%e\nprocess_user_seconds\t%U\nprocess_system_seconds\t%S\nmax_rss_kib\t%M\nexit_status\t%x' \
        -o "$time_file" "$RUN_BIN" "${args[@]}" >"$log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$run_dir/exit-status"
    if (( status != 0 )); then
        tail -n 50 "$log" >&2 || true
        die "$process_label failed with status $status"
    fi
    snapshot_pressure "$run_dir/pressure-after.txt"
    check_measurement_conflicts "$run_dir/processes-after.txt"
    snapshot_residency "$process_label" "$backend" "$gap" after-run \
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
        "$process_label" "$query_name" "$category" "$mode" "$order_index" \
        "$backend" "$gap" "$BINARY_SHA256" "$SEGMENTS_DIR" "$raw" \
        "$wall_seconds" "$user_seconds" "$system_seconds" "$max_rss_kib" \
        >>"$RAW_INDEX"
}

while IFS=$'\t' read -r process_label query_name category mode order_index \
    backend gap; do
    [[ "$process_label" != "process_label" ]] || continue
    run_process "$process_label" "$query_name" "$category" "$mode" \
        "$order_index" "$backend" "$gap"
done <"$RUN_PLAN"

note "re-inventorying the corpus"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$SEGMENTS_DIR" \
    --output "$INVENTORY_DIR/after.json" \
    --paths-output "$INVENTORY_DIR/files-after.nul"
cmp -s "$INVENTORY_DIR/before.json" "$INVENTORY_DIR/after.json" \
    || die "Schema 8 corpus changed during attribution"
cmp -s "$INVENTORY_DIR/files.nul" "$INVENTORY_DIR/files-after.nul" \
    || die "Schema 8 corpus path set changed during attribution"

python3 "$FROZEN_GATE_TOOL" compare-results \
    --index "$RAW_INDEX" \
    --manifest "$NORMALIZED_JSON" \
    --source-manifest "$METADATA_DIR/query-manifest.input.json" \
    --inventory-before "$INVENTORY_DIR/before.json" \
    --inventory-after "$INVENTORY_DIR/after.json" \
    --residency "$RESIDENCY_SUMMARY" \
    --summary "$RESULT_DIR/summary.tsv" \
    --output "$COMPARISONS_DIR/result-gate.json" \
    --binary "$RUN_BIN" \
    --corpus "$SEGMENTS_DIR" \
    --runs-dir "$RUNS_DIR" \
    --max-resident-bytes-after-evict "$MAX_RESIDENT_BYTES_AFTER_EVICT" \
    --max-matched-series "$QUERY_MAX_SERIES_MATCHED" \
    --max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
    --max-chunk-reads "$QUERY_MAX_CHUNKS_READ" \
    --max-bytes-read "$QUERY_MAX_BYTES_READ" \
    --max-samples-decoded "$QUERY_MAX_SAMPLES" \
    --max-regex-values-examined "$REGEX_MAX_EXPANDED_VALUES" \
    || die "strict attribution correctness/stage gate failed"

(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        sha256sum -- "${artifact#./}"
    done < <(find runs comparisons inventory -type f -print0 | sort -z)
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
note "complete observer-heavy attribution artifact: $RESULT_DIR"
