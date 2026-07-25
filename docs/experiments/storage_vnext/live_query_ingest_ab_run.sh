#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE="$SCRIPT_DIR/live_query_ingest_ab_gate.py"
PHASE1_GATE="$SCRIPT_DIR/phase1_replay_gate.py"
REPORT_GATE="$SCRIPT_DIR/ab_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DEFAULT_CAPTURE="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001"
DEFAULT_TEMPLATE="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/post-adaptive-head-profile-20260716-223717/config.toml"

CAPTURE="${CAPTURE:-$DEFAULT_CAPTURE}"
CONFIG_TEMPLATE="${CONFIG_TEMPLATE:-$DEFAULT_TEMPLATE}"
EXPECTATIONS="${EXPECTATIONS:-$SCRIPT_DIR/phase1_4m_expectations.json}"
QUERY_WORKLOAD="${QUERY_WORKLOAD:-$SCRIPT_DIR/live_query_ingest_queries.json}"
REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
RESULT_DIR="${RESULT_DIR:-}"
INGESTER_BIN="${INGESTER_BIN:-}"
API_BIN="${API_BIN:-}"
QUERY_BIN="${QUERY_BIN:-}"
STORAGE_VERIFY_BIN="${STORAGE_VERIFY_BIN:-}"
INGEST_CPUSET="${INGEST_CPUSET:-}"
CLIENT_CPUSET="${CLIENT_CPUSET:-}"
LIVE_MEMORY_ADMISSION_BYTES="${LIVE_MEMORY_ADMISSION_BYTES:-}"
STOP_AFTER_MESSAGES="${STOP_AFTER_MESSAGES:-250000}"
RUN_ORDER="${RUN_ORDER:-D,P,Q}"
API_LISTEN="${API_LISTEN:-127.0.0.1:19091}"
PUBLISH_INTERVAL_MS="${PUBLISH_INTERVAL_MS:-1000}"
MAX_VIEW_STALENESS_MS="${MAX_VIEW_STALENESS_MS:-10000}"
MAX_CONCURRENT_QUERIES="${MAX_CONCURRENT_QUERIES:-4}"
RANGE_SCALAR_CACHE_MAX_BYTES="${RANGE_SCALAR_CACHE_MAX_BYTES:-0}"
RSS_INTERVAL_MS="${RSS_INTERVAL_MS:-100}"
HOST_PROCESS_SAMPLE_INTERVAL_MS="${HOST_PROCESS_SAMPLE_INTERVAL_MS:-250}"
PERF_STAT_MODE="${PERF_STAT_MODE:-required}"
EVICT_CAPTURE="${EVICT_CAPTURE:-0}"
MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT="${MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT:-0}"
MIN_FREE_BYTES="${MIN_FREE_BYTES:-5368709120}"
READBACK_SAMPLE_LIMIT_PER_KIND="${READBACK_SAMPLE_LIMIT_PER_KIND:-2}"
ALLOW_NOISY_HOST="${ALLOW_NOISY_HOST:-0}"
MAX_HOST_LOAD_PER_CPU="${MAX_HOST_LOAD_PER_CPU:-1.0}"
MAX_CPU_PSI_AVG10="${MAX_CPU_PSI_AVG10:-10.0}"
MAX_IO_PSI_AVG10="${MAX_IO_PSI_AVG10:-5.0}"
MAX_MEMORY_PSI_AVG10="${MAX_MEMORY_PSI_AVG10:-2.0}"
PRESSURE_SETTLE_TIMEOUT_SECS="${PRESSURE_SETTLE_TIMEOUT_SECS:-120}"
RUN_NOTE="${RUN_NOTE:-}"
RUST_LOG_DISABLED="${RUST_LOG_DISABLED:-chronoxide_ingester=info,chronoxide_core=warn}"
RUST_LOG_LIVE="${RUST_LOG_LIVE:-chronoxide_ingester=info,chronoxide_core=warn,chronoxide_live_metrics=debug}"
DIAGNOSTIC_P_ONLY="${DIAGNOSTIC_P_ONLY:-0}"
DRY_RUN=0

usage() {
    cat <<'EOF'
Usage:
  RESULT_DIR=/absolute/fresh/external/root \
  INGESTER_BIN=/absolute/chronoxide-ingester \
  API_BIN=/absolute/chronoxide-api \
  QUERY_BIN=/absolute/chronoxide-query \
  STORAGE_VERIFY_BIN=/absolute/chronoxide-storage-verify \
  INGEST_CPUSET=2-5 CLIENT_CPUSET=6-7 \
  LIVE_MEMORY_ADMISSION_BYTES=<measured nonzero budget> \
  RUN_NOTE='quiet host; no build, replay, profiler, or database overlap' \
    docs/experiments/storage_vnext/live_query_ingest_ab_run.sh [--dry-run]

The default 250,000-message screen runs D (live disabled), P (publication
enabled, no client), and Q (publication plus moderate HTTP query load) from one
preserved ingester binary. RUN_ORDER may be any comma-separated permutation of
D,P,Q. Use three fresh roots with the cyclic D,P,Q / P,Q,D / Q,D,P orders, or
all six permutations, for position counterbalancing; roots are never reused or
cleaned automatically.

Set DIAGNOSTIC_P_ONLY=1 and RUN_ORDER=P for one publication-only diagnostic
arm. This preserves all per-arm capture eviction, perf/RSS, replay, tree,
footer/postings, and readback checks, but deliberately skips the D/P/Q
cross-variant gate. Use separate fresh roots for counterbalanced code-version
comparisons.

PERF_STAT_MODE=required|off (default required). EVICT_CAPTURE=0|1 (default 0);
disabled controls are recorded as coverage gaps. A formal Phase1 4M run
requires EVICT_CAPTURE=1. Footer/exact-postings and independent readback
validation run after all measured processes.
EOF
}

die() {
    echo "live-query ingestion A/B: $*" >&2
    exit 2
}

note() {
    echo "live-query ingestion A/B: $*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

require_executable() {
    local name="$1"
    local path="$2"
    [[ "$path" == /* && -f "$path" && ! -L "$path" && -x "$path" ]] \
        || die "$name must be an absolute executable regular file: $path"
}

require_uint() {
    local name="$1"
    local value="$2"
    [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be a non-negative integer"
}

for argument in "$@"; do
    case "$argument" in
        --dry-run) DRY_RUN=1 ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; die "unknown argument: $argument" ;;
    esac
done

for command in awk bash cc cp date df diff file find git mkdir perf ps \
    nproc python3 realpath setsid sha256sum sleep sort stat sync taskset uname \
    /usr/bin/time; do
    require_command "$command"
done
(( BASH_VERSINFO[0] > 5 \
    || (BASH_VERSINFO[0] == 5 && BASH_VERSINFO[1] >= 1) )) \
    || die "the supervised measurement lifecycle requires Bash 5.1 or newer"
for path in "$GATE" "$PHASE1_GATE" "$REPORT_GATE" "$FADVISE_SOURCE" \
    "$EXPECTATIONS" "$QUERY_WORKLOAD"; do
    [[ -f "$path" && ! -L "$path" ]] || die "required harness file is missing: $path"
done
require_executable INGESTER_BIN "$INGESTER_BIN"
require_executable API_BIN "$API_BIN"
require_executable QUERY_BIN "$QUERY_BIN"
require_executable STORAGE_VERIFY_BIN "$STORAGE_VERIFY_BIN"
for item in STOP_AFTER_MESSAGES LIVE_MEMORY_ADMISSION_BYTES PUBLISH_INTERVAL_MS \
    MAX_VIEW_STALENESS_MS MAX_CONCURRENT_QUERIES RANGE_SCALAR_CACHE_MAX_BYTES \
    RSS_INTERVAL_MS MIN_FREE_BYTES MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT \
    HOST_PROCESS_SAMPLE_INTERVAL_MS PRESSURE_SETTLE_TIMEOUT_SECS; do
    require_uint "$item" "${!item}"
done
(( STOP_AFTER_MESSAGES > 0 )) || die "STOP_AFTER_MESSAGES must be nonzero"
(( LIVE_MEMORY_ADMISSION_BYTES > 0 )) \
    || die "LIVE_MEMORY_ADMISSION_BYTES must be explicitly nonzero"
(( PUBLISH_INTERVAL_MS > 0 && MAX_VIEW_STALENESS_MS >= PUBLISH_INTERVAL_MS )) \
    || die "publication/staleness settings are invalid"
(( MAX_CONCURRENT_QUERIES > 0 )) || die "MAX_CONCURRENT_QUERIES must be nonzero"
(( RSS_INTERVAL_MS >= 10 )) || die "RSS_INTERVAL_MS must be at least 10"
(( HOST_PROCESS_SAMPLE_INTERVAL_MS > 0 )) \
    || die "HOST_PROCESS_SAMPLE_INTERVAL_MS must be nonzero"
if (( STOP_AFTER_MESSAGES == 250000 )); then
    (( HOST_PROCESS_SAMPLE_INTERVAL_MS == 250 )) \
        || die "the mandatory 250k host-process sample interval must be 250ms"
fi
[[ "$PERF_STAT_MODE" == "required" || "$PERF_STAT_MODE" == "off" ]] \
    || die "PERF_STAT_MODE must be required or off"
[[ "$EVICT_CAPTURE" == "0" || "$EVICT_CAPTURE" == "1" ]] \
    || die "EVICT_CAPTURE must be 0 or 1"
[[ "$DIAGNOSTIC_P_ONLY" == "0" || "$DIAGNOSTIC_P_ONLY" == "1" ]] \
    || die "DIAGNOSTIC_P_ONLY must be 0 or 1"
[[ "$ALLOW_NOISY_HOST" == "0" || "$ALLOW_NOISY_HOST" == "1" ]] \
    || die "ALLOW_NOISY_HOST must be 0 or 1"
for item in "$MAX_HOST_LOAD_PER_CPU" "$MAX_CPU_PSI_AVG10" "$MAX_IO_PSI_AVG10" \
    "$MAX_MEMORY_PSI_AVG10"; do
    [[ "$item" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "host pressure thresholds must be numeric"
done
[[ -n "$INGEST_CPUSET" && -n "$CLIENT_CPUSET" ]] \
    || die "INGEST_CPUSET and CLIENT_CPUSET are required"
[[ "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
    || die "RUN_NOTE must contain no tab or newline"
if [[ "$DRY_RUN" != "1" ]]; then
    [[ -n "$RUN_NOTE" ]] || die "RUN_NOTE is required for a measured run"
fi
if [[ "$ALLOW_NOISY_HOST" == "1" && "$RUN_NOTE" != *[Nn][Oo][Ii][Ss][Yy]* ]]; then
    die "ALLOW_NOISY_HOST=1 requires RUN_NOTE to contain the word noisy"
fi

IFS=',' read -r -a RUN_VARIANTS <<<"$RUN_ORDER"
if [[ "$DIAGNOSTIC_P_ONLY" == "1" ]]; then
    (( ${#RUN_VARIANTS[@]} == 1 )) && [[ "${RUN_VARIANTS[0]}" == "P" ]] \
        || die "DIAGNOSTIC_P_ONLY=1 requires RUN_ORDER=P"
else
    (( ${#RUN_VARIANTS[@]} == 3 )) \
        || die "RUN_ORDER must contain exactly one D, P, and Q"
    ordered="$(printf '%s\n' "${RUN_VARIANTS[@]}" | sort | tr -d '\n')"
    [[ "$ordered" == "DPQ" ]] || die "RUN_ORDER must be a permutation of D,P,Q"
fi

[[ "$CAPTURE" == /* && -d "$CAPTURE" && ! -L "$CAPTURE" ]] \
    || die "CAPTURE must be an absolute non-symlink directory"
[[ "$CONFIG_TEMPLATE" == /* && -f "$CONFIG_TEMPLATE" && ! -L "$CONFIG_TEMPLATE" ]] \
    || die "CONFIG_TEMPLATE must be an absolute regular file"
[[ "$REPO_ROOT" == /* && -d "$REPO_ROOT" ]] || die "REPO_ROOT must be absolute"
CAPTURE="$(realpath -e -- "$CAPTURE")"
CONFIG_TEMPLATE="$(realpath -e -- "$CONFIG_TEMPLATE")"
EXPECTATIONS="$(realpath -e -- "$EXPECTATIONS")"
QUERY_WORKLOAD="$(realpath -e -- "$QUERY_WORKLOAD")"
REPO_ROOT="$(realpath -e -- "$REPO_ROOT")"
INGESTER_BIN="$(realpath -e -- "$INGESTER_BIN")"
API_BIN="$(realpath -e -- "$API_BIN")"
QUERY_BIN="$(realpath -e -- "$QUERY_BIN")"
STORAGE_VERIFY_BIN="$(realpath -e -- "$STORAGE_VERIFY_BIN")"
[[ "$(git -C "$REPO_ROOT" rev-parse --show-toplevel)" == "$REPO_ROOT" ]] \
    || die "REPO_ROOT must be the worktree root"

[[ -n "$RESULT_DIR" && "$RESULT_DIR" == /* ]] \
    || die "RESULT_DIR must be a fresh absolute external path"
result_parent_input="$(dirname "$RESULT_DIR")"
result_name="$(basename "$RESULT_DIR")"
[[ -d "$result_parent_input" && "$result_name" != "." && "$result_name" != ".." ]] \
    || die "RESULT_DIR parent must already exist"
RESULT_DIR="$(realpath -e -- "$result_parent_input")/$result_name"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists and will not be reused"
case "$RESULT_DIR/" in
    "$REPO_ROOT/"*|"$CAPTURE/"*) die "RESULT_DIR must be outside source and capture roots" ;;
esac

query_help="$("$QUERY_BIN" --help 2>&1)"
api_help="$("$API_BIN" --help 2>&1)"
verify_help="$("$STORAGE_VERIFY_BIN" --help 2>&1)"
for flag in --segments-dir --storage-layout --sample-limit-per-kind --verify-readbacks --output; do
    [[ "$query_help" == *"$flag"* ]] || die "query binary help is missing $flag"
done
for flag in --segments-dir --listen --storage-schema --validate-segment-footers; do
    [[ "$api_help" == *"$flag"* ]] || die "API binary help is missing $flag"
done
for flag in --segments-dir --schema --validate-segment-footers --verify-exact-postings; do
    [[ "$verify_help" == *"$flag"* ]] || die "storage verifier help is missing $flag"
done

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/configs" "$RESULT_DIR/metadata" "$RESULT_DIR/runs" \
    "$RESULT_DIR/validation" "$RESULT_DIR/comparisons"
CONFIG_DIR="$RESULT_DIR/configs"
METADATA_DIR="$RESULT_DIR/metadata"
RUNS_DIR="$RESULT_DIR/runs"
VALIDATION_DIR="$RESULT_DIR/validation"
COMPARISONS_DIR="$RESULT_DIR/comparisons"
mkdir "$METADATA_DIR/binaries" "$METADATA_DIR/harness" "$METADATA_DIR/source" \
    "$METADATA_DIR/tools"

for file in live_query_ingest_ab_run.sh live_query_ingest_ab_gate.py \
    live_query_scale_validator_bootstrap.py test_live_query_ingest_ab_gate.py \
    phase1_replay_gate.py ab_gate.py fadvise_regular_dontneed.c; do
    [[ -f "$SCRIPT_DIR/$file" ]] || die "harness provenance file is missing: $file"
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$file" "$METADATA_DIR/harness/$file"
done
cp --preserve=mode,timestamps -- "$QUERY_WORKLOAD" \
    "$METADATA_DIR/harness/live_query_ingest_queries.json"
cp --preserve=mode,timestamps -- "$EXPECTATIONS" \
    "$METADATA_DIR/harness/phase1_4m_expectations.json"
FROZEN_GATE="$METADATA_DIR/harness/live_query_ingest_ab_gate.py"
FROZEN_PHASE1="$METADATA_DIR/harness/phase1_replay_gate.py"
FROZEN_REPORT="$METADATA_DIR/harness/ab_gate.py"
FROZEN_WORKLOAD="$METADATA_DIR/harness/live_query_ingest_queries.json"
FROZEN_EXPECTATIONS="$METADATA_DIR/harness/phase1_4m_expectations.json"

preserve_binary() {
    local role="$1"
    local source="$2"
    local destination="$METADATA_DIR/binaries/$role"
    cp --reflink=auto --preserve=mode,timestamps -- "$source" "$destination"
    cmp -s -- "$source" "$destination" || die "preserved $role differs from source"
    sha256sum -- "$destination" >>"$METADATA_DIR/binaries.sha256"
}
: >"$METADATA_DIR/binaries.sha256"
preserve_binary chronoxide-ingester "$INGESTER_BIN"
preserve_binary chronoxide-api "$API_BIN"
preserve_binary chronoxide-query "$QUERY_BIN"
preserve_binary chronoxide-storage-verify "$STORAGE_VERIFY_BIN"
RUN_INGESTER="$METADATA_DIR/binaries/chronoxide-ingester"
RUN_QUERY="$METADATA_DIR/binaries/chronoxide-query"
RUN_VERIFY="$METADATA_DIR/binaries/chronoxide-storage-verify"

python3 "$FROZEN_PHASE1" validate-inputs \
    --capture "$CAPTURE" --template "$CONFIG_TEMPLATE" --expectations "$FROZEN_EXPECTATIONS" \
    --output "$METADATA_DIR/capture-capacity.json"
python3 "$FROZEN_GATE" bind-selected-input-prefix \
    --validated-capacity "$METADATA_DIR/capture-capacity.json" \
    --stop-after-messages "$STOP_AFTER_MESSAGES" \
    --output "$METADATA_DIR/validated-inputs.json"
phase1_messages="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["stop_after_messages"])' "$METADATA_DIR/capture-capacity.json")"
if [[ "$STOP_AFTER_MESSAGES" == "$phase1_messages" && "$EVICT_CAPTURE" != "1" ]]; then
    die "formal Phase1-message-count runs require EVICT_CAPTURE=1"
fi
python3 "$FROZEN_GATE" validate-workload \
    --workload "$FROZEN_WORKLOAD" --output "$METADATA_DIR/workload.json"
python3 "$FROZEN_GATE" validate-cpusets \
    --ingest "$INGEST_CPUSET" --client "$CLIENT_CPUSET" \
    --output "$METADATA_DIR/cpusets.json"

git -C "$REPO_ROOT" rev-parse HEAD >"$METADATA_DIR/source/git-head.txt"
git -C "$REPO_ROOT" status --porcelain=v2 --branch >"$METADATA_DIR/source/git-status.txt"
git -C "$REPO_ROOT" diff --binary --full-index HEAD -- \
    >"$METADATA_DIR/source/working-tree.patch"
git -C "$REPO_ROOT" diff --cached --binary --full-index -- \
    >"$METADATA_DIR/source/index.patch"
git -C "$REPO_ROOT" ls-files --others --exclude-standard \
    >"$METADATA_DIR/source/untracked-paths.txt"
mkdir "$METADATA_DIR/source/untracked"
(
    cd "$REPO_ROOT"
    while IFS= read -r -d '' path; do
        case "$path" in
            *.rs|*.toml|Cargo.lock|docs/superpowers/specs/*.md)
                if [[ -f "$path" && ! -L "$path" ]]; then
                    mkdir -p "$METADATA_DIR/source/untracked/$(dirname "$path")"
                    cp --preserve=mode,timestamps -- "$path" \
                        "$METADATA_DIR/source/untracked/$path"
                fi
                ;;
        esac
    done < <(git ls-files --others --exclude-standard -z)
)
(
    cd "$METADATA_DIR/source/untracked"
    find . -type f -print0 | sort -z | xargs -0 -r sha256sum -z
) >"$METADATA_DIR/source/untracked-task-sources.sha256.nul"
cp --preserve=mode,timestamps -- "$CONFIG_TEMPLATE" "$METADATA_DIR/config-template.toml"
cp --preserve=mode,timestamps -- "$CAPTURE/manifest.json" "$METADATA_DIR/capture-manifest.json"
{
    printf 'recorded_at=%s\n' "$(date --iso-8601=ns)"
    printf 'result_dir=%s\ncapture=%s\nconfig_template=%s\n' \
        "$RESULT_DIR" "$CAPTURE" "$CONFIG_TEMPLATE"
    printf 'stop_after_messages=%s\nrun_order=%s\n' "$STOP_AFTER_MESSAGES" "$RUN_ORDER"
    printf 'diagnostic_p_only=%s\n' "$DIAGNOSTIC_P_ONLY"
    printf 'ingest_cpuset=%s\nclient_cpuset=%s\napi_listen=%s\n' \
        "$INGEST_CPUSET" "$CLIENT_CPUSET" "$API_LISTEN"
    printf 'live_memory_admission_bytes=%s\npublish_interval_ms=%s\nmax_view_staleness_ms=%s\n' \
        "$LIVE_MEMORY_ADMISSION_BYTES" "$PUBLISH_INTERVAL_MS" "$MAX_VIEW_STALENESS_MS"
    printf 'max_concurrent_queries=%s\nrange_scalar_cache_max_bytes=%s\n' \
        "$MAX_CONCURRENT_QUERIES" "$RANGE_SCALAR_CACHE_MAX_BYTES"
    printf 'rss_interval_ms=%s\nperf_stat_mode=%s\nevict_capture=%s\n' \
        "$RSS_INTERVAL_MS" "$PERF_STAT_MODE" "$EVICT_CAPTURE"
    printf 'host_process_sample_interval_ms=%s\n' \
        "$HOST_PROCESS_SAMPLE_INTERVAL_MS"
    printf 'allow_noisy_host=%s\nreadback_sample_limit_per_kind=%s\n' \
        "$ALLOW_NOISY_HOST" "$READBACK_SAMPLE_LIMIT_PER_KIND"
    printf 'max_host_load_per_cpu=%s\nmax_cpu_psi_avg10=%s\nmax_io_psi_avg10=%s\nmax_memory_psi_avg10=%s\n' \
        "$MAX_HOST_LOAD_PER_CPU" "$MAX_CPU_PSI_AVG10" "$MAX_IO_PSI_AVG10" \
        "$MAX_MEMORY_PSI_AVG10"
    printf 'pressure_settle_timeout_secs=%s\n' "$PRESSURE_SETTLE_TIMEOUT_SECS"
    printf 'rust_log_disabled=%s\nrust_log_live=%s\nrun_note=%s\n' \
        "$RUST_LOG_DISABLED" "$RUST_LOG_LIVE" "$RUN_NOTE"
    printf 'client_schedule=parallelism 2; 500ms delay after each query pair; fixed workload order\n'
    printf 'client_model=closed-loop moderate load; achieved request rate is measured, not prescribed; latency is subject to coordinated-omission bias\n'
    printf 'publication_observer=the P/Q cost includes chronoxide_live_metrics DEBUG event construction and output; no uninstrumented publication arm is claimed\n'
    printf 'ingestion_pause_scope=publication-due message boundaries only, excluding the event log emission itself\n'
    printf 'topology_assumption=CPU sets are disjoint and validated against process affinity; SMT sibling isolation and exclusivity from arbitrary tasks remain operator responsibilities\n'
    if [[ "$DIAGNOSTIC_P_ONLY" == "1" ]]; then
        printf 'validation=separate footer/exact-postings verifier and independent readbacks on P after the measured run\n'
    else
        printf 'validation=separate footer/exact-postings verifier and independent readbacks on D after all measured runs\n'
    fi
} >"$METADATA_DIR/settings.txt"
{
    date --iso-8601=ns
    uname -a
    taskset -pc "$$"
    command -v lscpu >/dev/null && lscpu || true
    perf --version
    df -B1 "$RESULT_DIR"
    cat /proc/loadavg
    for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
        [[ -r "$pressure" ]] && { echo "$pressure"; cat "$pressure"; }
    done
} >"$METADATA_DIR/environment.txt" 2>&1

printf 'order\tvariant\tconfig\tsegments_dir\n' >"$RESULT_DIR/run-plan.tsv"
for index in "${!RUN_VARIANTS[@]}"; do
    variant="${RUN_VARIANTS[$index]}"
    run_dir="$RUNS_DIR/$variant"
    segments_dir="$run_dir/segments"
    mkdir "$run_dir"
    python3 "$FROZEN_GATE" render-config \
        --template "$METADATA_DIR/config-template.toml" \
        --output "$CONFIG_DIR/$variant.toml" \
        --capture "$CAPTURE" \
        --segments-dir "$segments_dir" \
        --stop-after-messages "$STOP_AFTER_MESSAGES" \
        --variant "$variant" \
        --listen "$API_LISTEN" \
        --publish-interval-ms "$PUBLISH_INTERVAL_MS" \
        --max-staleness-ms "$MAX_VIEW_STALENESS_MS" \
        --memory-admission-bytes "$LIVE_MEMORY_ADMISSION_BYTES" \
        --max-concurrent-queries "$MAX_CONCURRENT_QUERIES" \
        --range-cache-bytes "$RANGE_SCALAR_CACHE_MAX_BYTES" \
        >"$run_dir/config-render.json"
    printf '%s\t%s\t%s\t%s\n' "$((index + 1))" "$variant" \
        "$CONFIG_DIR/$variant.toml" "$segments_dir" >>"$RESULT_DIR/run-plan.tsv"
done

if [[ "$DRY_RUN" == "1" ]]; then
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    note "dry-run plan complete: $RESULT_DIR"
    exit 0
fi

available_bytes="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 {print $1}')"
[[ "$available_bytes" =~ ^[0-9]+$ && "$available_bytes" -ge "$MIN_FREE_BYTES" ]] \
    || die "result filesystem does not satisfy MIN_FREE_BYTES=$MIN_FREE_BYTES"

PERF_EVENTS="task-clock,cycles,instructions,cache-misses,context-switches,cpu-migrations,page-faults"
if [[ "$PERF_STAT_MODE" == "required" ]]; then
    set +e
    perf stat --event "$PERF_EVENTS" --output "$METADATA_DIR/perf-preflight.txt" \
        -- true >/dev/null 2>&1
    perf_status=$?
    set -e
    (( perf_status == 0 )) || die "perf stat preflight failed"
else
    printf 'perf stat disabled\n' >"$METADATA_DIR/PERF_COVERAGE_GAP"
fi

if [[ "$EVICT_CAPTURE" == "1" ]]; then
    require_command fincore
    cc -O2 -Wall -Wextra -Werror "$FADVISE_SOURCE" \
        -o "$METADATA_DIR/tools/fadvise-regular-dontneed"
else
    printf 'capture page-cache eviction disabled\n' >"$METADATA_DIR/CAPTURE_CACHE_COVERAGE_GAP"
fi

check_conflicts() {
    local output="$1"
    ps -eo pid=,ppid=,pcpu=,rss=,stat=,comm=,args= >"$output"
    local conflicts
    conflicts="$(awk -v own="$$" '
        $1 != own && ($6 == "cargo" || $6 == "rustc" || $6 == "perf" ||
            $6 == "ninja" || $6 == "ninja-build" || $6 == "cmake" ||
            $6 == "make" || $6 == "gmake" || $6 == "clang" ||
            $6 == "clang++" || $6 == "clang++.real" || $6 == "cc" ||
            $6 == "cc1" || $6 == "cc1plus" || $6 == "collect2" ||
            $6 == "c++" || $6 == "gcc" || $6 == "g++" || $6 == "ld" ||
            $6 == "ld.gold" || $6 == "ld.lld" || $6 == "lld" ||
            $6 == "mold" || $6 == "java" || $6 == "javac" ||
            $6 == "mvn" || $6 == "mvnw" || $6 == "gradle" ||
            $6 == "gradlew" || $6 == "soong_ui" || $6 == "bazel" ||
            $6 == "docker" || $6 == "podman" || $6 == "buildah" ||
            $6 == "dd" || $6 == "fio" || $6 == "rsync" ||
            $6 == "stress" || $6 == "stress-ng" || $6 == "sysbench" ||
            $6 ~ /^qemu-system/ || $6 ~ /^chronoxide-/ ||
            $6 ~ /^greptime/ || $6 == "prometheus" ||
            $7 ~ /(^|\/)([[:alnum:]_]+-)*(cc|c\+\+|clang|clang\+\+|gcc|g\+\+)([.-][[:alnum:]_.+-]+)?$/) {print}
    ' "$output")"
    if [[ -n "$conflicts" && "$ALLOW_NOISY_HOST" != "1" ]]; then
        printf '%s\n' "$conflicts" >&2
        exit 70
    fi
}

snapshot_pressure() {
    local output="$1"
    {
        date --iso-8601=ns
        cat /proc/loadavg
        for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
            [[ -r "$pressure" ]] && { echo "$pressure"; cat "$pressure"; }
        done
    } >"$output"
}

gate_pressure() {
    local snapshot="$1"
    local attempt=0 load_one cpu_count cpu_psi io_psi memory_psi
    while :; do
        snapshot_pressure "$snapshot"
        [[ "$ALLOW_NOISY_HOST" == "0" ]] || return 0
        load_one="$(awk '{print $1}' /proc/loadavg)"
        cpu_count="$(nproc)"
        cpu_psi="$(awk '$1 == "some" {for (i=1;i<=NF;i++) if ($i ~ /^avg10=/) {sub(/^avg10=/,"",$i); print $i; exit}}' /proc/pressure/cpu)"
        io_psi="$(awk '$1 == "full" {for (i=1;i<=NF;i++) if ($i ~ /^avg10=/) {sub(/^avg10=/,"",$i); print $i; exit}}' /proc/pressure/io)"
        memory_psi="$(awk '$1 == "full" {for (i=1;i<=NF;i++) if ($i ~ /^avg10=/) {sub(/^avg10=/,"",$i); print $i; exit}}' /proc/pressure/memory)"
        if awk \
            -v load_one="$load_one" \
            -v cpus="$cpu_count" \
            -v load_limit="$MAX_HOST_LOAD_PER_CPU" \
            -v cpu_psi="$cpu_psi" \
            -v cpu_limit="$MAX_CPU_PSI_AVG10" \
            -v io_psi="$io_psi" \
            -v io_limit="$MAX_IO_PSI_AVG10" \
            -v memory_psi="$memory_psi" \
            -v memory_limit="$MAX_MEMORY_PSI_AVG10" \
            'BEGIN { exit !(load_one <= cpus * load_limit && cpu_psi <= cpu_limit && io_psi <= io_limit && memory_psi <= memory_limit) }'
        then
            return 0
        fi
        (( attempt < PRESSURE_SETTLE_TIMEOUT_SECS )) \
            || die "host pressure did not settle: load=$load_one cpu_psi=$cpu_psi io_psi=$io_psi memory_psi=$memory_psi"
        attempt=$((attempt + 1))
        sleep 1
    done
}

prepare_capture_cache() {
    local run_dir="$1"
    [[ "$EVICT_CAPTURE" == "1" ]] || return 0
    local file
    while IFS= read -r -d '' file; do
        "$METADATA_DIR/tools/fadvise-regular-dontneed" "$file"
    done < <(find "$CAPTURE" -maxdepth 1 -type f -name '*.capture' -print0 | sort -z)
    : >"$run_dir/capture-residency-before.tsv"
    while IFS= read -r -d '' file; do
        fincore --bytes --noheadings --output RES,SIZE,FILE -- "$file" \
            >>"$run_dir/capture-residency-before.tsv"
    done < <(find "$CAPTURE" -maxdepth 1 -type f -name '*.capture' -print0 | sort -z)
    local resident
    resident="$(awk '{sum += $1} END {printf "%.0f", sum}' \
        "$run_dir/capture-residency-before.tsv")"
    (( resident <= MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT )) \
        || die "capture retained $resident resident bytes after eviction"
}

printf 'variant\telapsed\tuser_seconds\tsystem_seconds\ttime_max_rss_kib\tproc_peak_rss_kib\tcorpus_files\tcorpus_bytes\tmanifest_sha256\tperf\n' \
    >"$RESULT_DIR/run-summary.tsv"

ACTIVE_LAUNCHER_PID=""
ACTIVE_LAUNCHER_PGID=""
ACTIVE_RSS_PID=""
ACTIVE_CLIENT_PID=""
ACTIVE_STOP_FILE=""
ACTIVE_HOST_MONITOR_PID=""
ACTIVE_HOST_MONITOR_PGID=""
ACTIVE_HOST_MONITOR_STOP_FILE=""
ACTIVE_HOST_MONITOR_STATUS_FILE=""
cleanup_children() {
    local status=$?
    trap - EXIT INT TERM
    set +e
    if [[ -n "$ACTIVE_STOP_FILE" ]]; then
        : >"$ACTIVE_STOP_FILE"
    fi
    if [[ -n "$ACTIVE_HOST_MONITOR_STOP_FILE" ]]; then
        : >"$ACTIVE_HOST_MONITOR_STOP_FILE"
    fi
    local pid
    for pid in "$ACTIVE_CLIENT_PID" "$ACTIVE_RSS_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    if [[ -n "$ACTIVE_LAUNCHER_PID" ]] && kill -0 "$ACTIVE_LAUNCHER_PID" 2>/dev/null; then
        local observed_sid
        observed_sid="$(ps -o sid= -p "$ACTIVE_LAUNCHER_PID" | tr -d ' ')"
        if [[ -n "$ACTIVE_LAUNCHER_PGID" && "$observed_sid" == "$ACTIVE_LAUNCHER_PGID" ]]; then
            kill -TERM -- "-$ACTIVE_LAUNCHER_PGID" 2>/dev/null || true
            kill -CONT -- "-$ACTIVE_LAUNCHER_PGID" 2>/dev/null || true
        else
            kill -TERM "$ACTIVE_LAUNCHER_PID" 2>/dev/null || true
            kill -CONT "$ACTIVE_LAUNCHER_PID" 2>/dev/null || true
        fi
    fi
    for pid in "$ACTIVE_CLIENT_PID" "$ACTIVE_RSS_PID" "$ACTIVE_LAUNCHER_PID"; do
        if [[ -n "$pid" ]]; then
            wait "$pid" 2>/dev/null || true
        fi
    done
    local monitor_status=0
    if [[ -n "$ACTIVE_HOST_MONITOR_PID" ]]; then
        local attempt
        for ((attempt = 0; attempt < 50; attempt++)); do
            kill -0 "$ACTIVE_HOST_MONITOR_PID" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$ACTIVE_HOST_MONITOR_PID" 2>/dev/null; then
            if [[ -n "$ACTIVE_HOST_MONITOR_PGID" ]]; then
                kill -TERM -- "-$ACTIVE_HOST_MONITOR_PGID" 2>/dev/null || true
            else
                kill -TERM "$ACTIVE_HOST_MONITOR_PID" 2>/dev/null || true
            fi
            for ((attempt = 0; attempt < 20; attempt++)); do
                kill -0 "$ACTIVE_HOST_MONITOR_PID" 2>/dev/null || break
                sleep 0.1
            done
        fi
        if kill -0 "$ACTIVE_HOST_MONITOR_PID" 2>/dev/null; then
            if [[ -n "$ACTIVE_HOST_MONITOR_PGID" ]]; then
                kill -KILL -- "-$ACTIVE_HOST_MONITOR_PGID" 2>/dev/null || true
            else
                kill -KILL "$ACTIVE_HOST_MONITOR_PID" 2>/dev/null || true
            fi
        fi
        wait "$ACTIVE_HOST_MONITOR_PID" 2>/dev/null
        monitor_status=$?
        if [[ -n "$ACTIVE_HOST_MONITOR_STATUS_FILE" \
            && ! -e "$ACTIVE_HOST_MONITOR_STATUS_FILE" ]]; then
            printf '%s\n' "$monitor_status" \
                >"$ACTIVE_HOST_MONITOR_STATUS_FILE"
        fi
    fi
    exit "$status"
}
trap cleanup_children EXIT INT TERM

run_variant() {
    local variant="$1"
    local run_dir="$RUNS_DIR/$variant"
    local config="$CONFIG_DIR/$variant.toml"
    local segments="$run_dir/segments"
    local rust_log="$RUST_LOG_DISABLED"
    [[ "$variant" == "D" ]] || rust_log="$RUST_LOG_LIVE"
    local -a command=(
        taskset --cpu-list "$INGEST_CPUSET"
        env
        -u OTEL_EXPORTER_OTLP_ENDPOINT
        -u OTEL_EXPORTER_OTLP_LOGS_ENDPOINT
        -u OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
        "CONFIG_FILE=$config"
        "RUST_LOG=$rust_log"
        "$RUN_INGESTER"
    )
    if [[ "$PERF_STAT_MODE" == "required" ]]; then
        command=(
            perf stat --no-big-num --field-separator $'\t'
            --event "$PERF_EVENTS" --output "$run_dir/perf-stat.tsv" --
            "${command[@]}"
        )
    fi
    check_conflicts "$run_dir/processes-before.txt"
    gate_pressure "$run_dir/pressure-before.txt"
    prepare_capture_cache "$run_dir"
    note "running $variant"
    set +e
    (
        cd "$run_dir"
        exec setsid /bin/sh -c 'kill -STOP "$$"; exec "$@"' \
            chronoxide-measured \
            /usr/bin/time -v -o "$run_dir/replay.time.txt" \
                env LC_ALL=C TZ=UTC \
                "${command[@]}" >"$run_dir/ingester.log" 2>&1
    ) &
    local launcher_pid=$!
    ACTIVE_LAUNCHER_PID="$launcher_pid"
    ACTIVE_LAUNCHER_PGID="$launcher_pid"
    ACTIVE_STOP_FILE="$run_dir/ingestion-complete"
    ACTIVE_HOST_MONITOR_STOP_FILE="$run_dir/host-process-monitor-stop"
    local launcher_stopped=0
    local attempt observed_sid observed_state
    for ((attempt = 0; attempt < 100; attempt++)); do
        kill -0 "$launcher_pid" 2>/dev/null \
            || die "$variant measured launcher exited before its start barrier"
        read -r observed_sid observed_state \
            < <(ps -o sid=,stat= -p "$launcher_pid")
        if [[ "$observed_sid" == "$launcher_pid" && "$observed_state" == T* ]]; then
            launcher_stopped=1
            break
        fi
        sleep 0.05
    done
    (( launcher_stopped == 1 )) \
        || die "$variant measured launcher did not reach its start barrier"
    local -a host_monitor_conflict_args=()
    if [[ "$ALLOW_NOISY_HOST" == "0" ]]; then
        host_monitor_conflict_args+=(--abort-on-conflict)
    fi
    (
        exec setsid taskset --cpu-list "$CLIENT_CPUSET" \
            /usr/bin/time -v -o "$run_dir/host-process-monitor.time.txt" \
                python3 "$FROZEN_GATE" monitor-host-processes \
                    --expected-session-id "$launcher_pid" \
                    --interval-ms "$HOST_PROCESS_SAMPLE_INTERVAL_MS" \
                    "${host_monitor_conflict_args[@]}" \
                    --stop-file "$run_dir/host-process-monitor-stop" \
                    --ready-file "$run_dir/host-process-monitor-ready.json" \
                    --output "$run_dir/host-process-samples.jsonl" \
                >"$run_dir/host-process-monitor.log" 2>&1
    ) &
    local host_monitor_pid=$!
    ACTIVE_HOST_MONITOR_PID="$host_monitor_pid"
    ACTIVE_HOST_MONITOR_PGID="$host_monitor_pid"
    ACTIVE_HOST_MONITOR_STATUS_FILE="$run_dir/host-process-monitor.exit-status"
    local host_monitor_ready=0
    for ((attempt = 0; attempt < 200; attempt++)); do
        if [[ -f "$run_dir/host-process-monitor-ready.json" ]]; then
            host_monitor_ready=1
            break
        fi
        kill -0 "$host_monitor_pid" 2>/dev/null \
            || die "$variant host-process monitor exited before readiness"
        sleep 0.05
    done
    (( host_monitor_ready == 1 )) \
        || die "$variant host-process monitor did not become ready"
    local monitor_sid monitor_pgid monitor_state
    read -r monitor_sid monitor_pgid monitor_state \
        < <(ps -o sid=,pgid=,stat= -p "$host_monitor_pid")
    if [[ "$monitor_sid" != "$host_monitor_pid" \
        || "$monitor_pgid" != "$host_monitor_pid" \
        || -z "$monitor_state" ]]; then
        die "$variant host-process monitor is not an isolated session"
    fi
    taskset --cpu-list "$CLIENT_CPUSET" python3 "$FROZEN_PHASE1" monitor-rss \
        --pid "$launcher_pid" \
        --output "$run_dir/rss-samples.tsv" \
        --summary "$run_dir/rss-summary.json" \
        --interval-ms "$RSS_INTERVAL_MS" \
        >"$run_dir/rss-monitor.log" 2>&1 &
    local rss_pid=$!
    ACTIVE_RSS_PID="$rss_pid"
    local client_pid=""
    if [[ "$variant" == "Q" ]]; then
        taskset --cpu-list "$CLIENT_CPUSET" \
            python3 "$FROZEN_GATE" client \
                --base-url "http://$API_LISTEN" \
                --workload "$FROZEN_WORKLOAD" \
                --records "$run_dir/client-records.jsonl" \
                --summary "$run_dir/client-summary.json" \
                --stop-file "$run_dir/ingestion-complete" \
                >"$run_dir/client.log" 2>&1 &
        client_pid=$!
        ACTIVE_CLIENT_PID="$client_pid"
    fi
    python3 "$FROZEN_GATE" record-host-process-boundary \
        --phase start \
        --expected-leader-pid "$launcher_pid" \
        --output "$run_dir/host-process-start.json" >/dev/null \
        || die "$variant could not record the measured start boundary"
    kill -CONT "$launcher_pid" \
        || die "$variant could not release the measured start barrier"
    local completed_pid=""
    wait -n -p completed_pid "$launcher_pid" "$host_monitor_pid"
    local status=$?
    if [[ "$completed_pid" == "$host_monitor_pid" ]]; then
        printf '%s\n' "$status" \
            >"$run_dir/host-process-monitor.exit-status"
        if kill -0 "$launcher_pid" 2>/dev/null; then
            kill -KILL -- "-$launcher_pid" 2>/dev/null || true
            wait "$launcher_pid" 2>/dev/null || true
        fi
        ACTIVE_LAUNCHER_PID=""
        ACTIVE_LAUNCHER_PGID=""
        ACTIVE_HOST_MONITOR_PID=""
        ACTIVE_HOST_MONITOR_PGID=""
        ACTIVE_HOST_MONITOR_STOP_FILE=""
        ACTIVE_HOST_MONITOR_STATUS_FILE=""
        die "$variant host-process monitor exited during measurement; partial root preserved"
    fi
    [[ "$completed_pid" == "$launcher_pid" ]] \
        || die "$variant wait supervisor returned an unknown child"
    python3 "$FROZEN_GATE" record-host-process-boundary \
        --phase end \
        --expected-leader-pid "$launcher_pid" \
        --start-boundary "$run_dir/host-process-start.json" \
        --output "$run_dir/host-process-end.json" >/dev/null \
        || die "$variant could not record the measured end boundary"
    ACTIVE_LAUNCHER_PID=""
    ACTIVE_LAUNCHER_PGID=""
    : >"$run_dir/ingestion-complete"
    : >"$run_dir/host-process-monitor-stop"
    local client_status=0
    if [[ -n "$client_pid" ]]; then
        wait "$client_pid"
        client_status=$?
        ACTIVE_CLIENT_PID=""
    fi
    wait "$rss_pid"
    local rss_status=$?
    ACTIVE_RSS_PID=""
    wait "$host_monitor_pid"
    local host_monitor_status=$?
    printf '%s\n' "$host_monitor_status" \
        >"$run_dir/host-process-monitor.exit-status"
    ACTIVE_HOST_MONITOR_PID=""
    ACTIVE_HOST_MONITOR_PGID=""
    ACTIVE_HOST_MONITOR_STOP_FILE=""
    ACTIVE_HOST_MONITOR_STATUS_FILE=""
    ACTIVE_STOP_FILE=""
    set -e
    printf '%s\n' "$status" >"$run_dir/ingester.exit-status"
    printf '%s\n' "$rss_status" >"$run_dir/rss-monitor.exit-status"
    [[ "$variant" != "Q" ]] || printf '%s\n' "$client_status" >"$run_dir/client.exit-status"
    (( status == 0 )) || die "$variant ingester failed; partial root preserved"
    (( rss_status == 0 )) || die "$variant RSS monitor failed; partial root preserved"
    (( host_monitor_status == 0 )) \
        || die "$variant host-process monitor failed; partial root preserved"
    (( client_status == 0 )) || die "Q client failed; partial root preserved"
    [[ -d "$segments" ]] || die "$variant produced no segment corpus"
    sync -f "$segments"
    python3 "$FROZEN_PHASE1" parse-time \
        --input "$run_dir/replay.time.txt" --output "$run_dir/replay.time.json" >/dev/null
    if [[ "$PERF_STAT_MODE" == "required" ]]; then
        local -a required=()
        local event
        IFS=',' read -r -a perf_events <<<"$PERF_EVENTS"
        for event in "${perf_events[@]}"; do required+=(--require-event "$event"); done
        python3 "$FROZEN_PHASE1" parse-perf-stat \
            --input "$run_dir/perf-stat.tsv" --output "$run_dir/perf-stat.json" \
            "${required[@]}" >/dev/null
    fi
    mapfile -d '' -t reports \
        < <(find "$run_dir" -maxdepth 1 -type f -name 'ingestion_stats_*.md' -print0)
    (( ${#reports[@]} == 1 )) || die "$variant must produce exactly one ingestion report"
    python3 "$FROZEN_REPORT" replay-report \
        --report "${reports[0]}" --output "$run_dir/replay-correctness.json"
    python3 "$FROZEN_PHASE1" tree-manifest \
        --corpus "$segments" \
        --manifest "$run_dir/segments.sha256" \
        --inventory "$run_dir/segments.tsv" \
        --summary "$run_dir/corpus-summary.json" >/dev/null
    if [[ "$variant" != "D" ]]; then
        python3 "$FROZEN_GATE" parse-live-log \
            --log "$run_dir/ingester.log" \
            --expected-messages "$STOP_AFTER_MESSAGES" \
            --output "$run_dir/live-log-summary.json" >/dev/null
    fi
    check_conflicts "$run_dir/processes-after.txt"
    snapshot_pressure "$run_dir/pressure-after.txt"
    python3 - "$variant" "$run_dir/replay.time.json" "$run_dir/rss-summary.json" \
        "$run_dir/corpus-summary.json" "$PERF_STAT_MODE" >>"$RESULT_DIR/run-summary.tsv" <<'PY'
import json, sys
variant, timing_path, rss_path, corpus_path, perf = sys.argv[1:]
timing = json.load(open(timing_path))
rss = json.load(open(rss_path))
corpus = json.load(open(corpus_path))
print("\t".join(map(str, (
    variant, timing["elapsed"], timing["user_seconds"], timing["system_seconds"],
    timing["max_rss_kib"], rss["aggregate_rss_kib"], corpus["file_count"],
    corpus["size_bytes"], corpus["manifest_sha256"], perf,
))))
PY
}

for variant in "${RUN_VARIANTS[@]}"; do
    run_variant "$variant"
done

if [[ "$DIAGNOSTIC_P_ONLY" == "1" ]]; then
    printf '%s\n' \
        'Publication-only diagnostic arm: the D/P/Q cross-variant gate intentionally did not run.' \
        >"$COMPARISONS_DIR/DPQ_GATE_NOT_APPLICABLE"
    VALIDATION_VARIANT=P
else
    gate_args=(
        --runs-root "$RUNS_DIR"
        --workload "$FROZEN_WORKLOAD"
        --expected-messages "$STOP_AFTER_MESSAGES"
        --phase1-expectations "$FROZEN_EXPECTATIONS"
        --output "$COMPARISONS_DIR/dpq-gate.json"
    )
    [[ "$PERF_STAT_MODE" != "required" ]] || gate_args+=(--perf-required)
    python3 "$FROZEN_GATE" gate-run-set "${gate_args[@]}" >/dev/null
    VALIDATION_VARIANT=D
fi

note "running separate exhaustive storage validation on $VALIDATION_VARIANT corpus"
/usr/bin/time -v -o "$VALIDATION_DIR/storage-verify.time.txt" \
    "$RUN_VERIFY" \
        --segments-dir "$RUNS_DIR/$VALIDATION_VARIANT/segments" \
        --schema schema8 \
        --validate-segment-footers \
        --verify-exact-postings \
        >"$VALIDATION_DIR/storage-verify.json" \
        2>"$VALIDATION_DIR/storage-verify.log"
storage_gate_args=(
    --report "$VALIDATION_DIR/storage-verify.json"
    --replay-correctness "$RUNS_DIR/$VALIDATION_VARIANT/replay-correctness.json"
    --ingester-log "$RUNS_DIR/$VALIDATION_VARIANT/ingester.log"
    --output "$VALIDATION_DIR/storage-verify-gate.json"
)
[[ "$DIAGNOSTIC_P_ONLY" != "1" ]] || storage_gate_args+=(--live-handoff)
python3 "$FROZEN_GATE" gate-storage "${storage_gate_args[@]}" >/dev/null
if [[ "$STOP_AFTER_MESSAGES" == "$phase1_messages" ]]; then
    python3 "$FROZEN_PHASE1" gate-verifier \
        --actual "$VALIDATION_DIR/storage-verify.json" \
        --expectations "$FROZEN_EXPECTATIONS"
    touch "$METADATA_DIR/CAPTURE_LEVEL_PHYSICAL_SAMPLE_GOLDEN_GATED"
elif [[ "$DIAGNOSTIC_P_ONLY" == "1" ]]; then
    printf '%s\n' \
        'No version-matched capture-level physical-row golden exists for this prefix. P-only live-handoff validation has no independent per-window writer-row reconciliation; the exhaustive Schema 8 footer/postings verifier and independent readbacks remain authoritative for the persisted corpus.' \
        >"$METADATA_DIR/PHYSICAL_SAMPLE_COUNT_COVERAGE_GAP"
else
    printf \
        'No version-matched capture-level physical-row golden exists for this prefix; %s writer inputs/outputs and the exhaustive verifier reconcile exactly.\n' \
        "$VALIDATION_VARIANT" \
        >"$METADATA_DIR/PHYSICAL_SAMPLE_COUNT_COVERAGE_GAP"
fi

note "running separate independent readbacks on $VALIDATION_VARIANT corpus"
/usr/bin/time -v -o "$VALIDATION_DIR/readbacks.time.txt" \
    "$RUN_QUERY" \
        --segments-dir "$RUNS_DIR/$VALIDATION_VARIANT/segments" \
        --storage-layout schema8 \
        --sample-limit-per-kind "$READBACK_SAMPLE_LIMIT_PER_KIND" \
        --verify-readbacks \
        --output "$VALIDATION_DIR/readbacks.md" \
        >"$VALIDATION_DIR/readbacks.log" 2>&1
python3 "$FROZEN_GATE" gate-readbacks \
    --report "$VALIDATION_DIR/readbacks.md" \
    --output "$VALIDATION_DIR/readbacks-gate.json" >/dev/null
if [[ "$STOP_AFTER_MESSAGES" == "$phase1_messages" ]]; then
    python3 "$FROZEN_PHASE1" gate-readbacks \
        --report "$VALIDATION_DIR/readbacks.md" \
        --expectations "$FROZEN_EXPECTATIONS" >/dev/null
fi

(
    cd "$RESULT_DIR"
    find configs metadata validation comparisons runs run-plan.tsv run-summary.tsv \
        -type f \
        ! -path 'runs/*/segments/*' \
        ! -path 'metadata/result-artifacts.sha256' \
        -print0 | sort -z | xargs -0 sha256sum
) >"$METADATA_DIR/result-artifacts.sha256"
touch "$RESULT_DIR/COMPLETE"
note "complete: $RESULT_DIR"
