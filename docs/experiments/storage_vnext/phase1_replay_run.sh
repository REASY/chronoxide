#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE_TOOL="$SCRIPT_DIR/phase1_replay_gate.py"
REPLAY_REPORT_TOOL="$SCRIPT_DIR/ab_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DEFAULT_CAPTURE="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001"
DEFAULT_CONFIG_TEMPLATE="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/post-adaptive-head-profile-20260716-223717/config.toml"

CAPTURE="${CAPTURE:-$DEFAULT_CAPTURE}"
CONFIG_TEMPLATE="${CONFIG_TEMPLATE:-$DEFAULT_CONFIG_TEMPLATE}"
EXPECTATIONS="${EXPECTATIONS:-$SCRIPT_DIR/phase1_4m_expectations.json}"
REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
RESULT_DIR="${RESULT_DIR:-}"
INGESTER_BIN="${INGESTER_BIN:-}"
QUERY_BIN="${QUERY_BIN:-}"
STORAGE_VERIFY_BIN="${STORAGE_VERIFY_BIN:-}"
BUILD_COMMAND="${BUILD_COMMAND:-cargo build --locked --release --no-default-features -p chronoxide-ingester --bin chronoxide-ingester --bin chronoxide-query --bin chronoxide-storage-verify}"
RUST_LOG_VALUE="${RUST_LOG_VALUE:-chronoxide_ingester=info,chronoxide_core=warn}"
RUN_NOTE="${RUN_NOTE:-}"
PERF_STAT_MODE="${PERF_STAT_MODE:-required}"
EVICT_CAPTURE="${EVICT_CAPTURE:-1}"
MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT="${MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT:-0}"
RSS_INTERVAL_MS="${RSS_INTERVAL_MS:-100}"
READBACK_SAMPLE_LIMIT_PER_KIND="${READBACK_SAMPLE_LIMIT_PER_KIND:-2}"
ALLOW_NOISY_HOST="${ALLOW_NOISY_HOST:-0}"
WITH_PROFILE="${WITH_PROFILE:-0}"
DRY_RUN=0
VALIDATE_ONLY=0

usage() {
    cat <<'EOF'
Usage:
  RESULT_DIR=/absolute/new/external/result-root \
  INGESTER_BIN=/absolute/chronoxide-ingester \
  QUERY_BIN=/absolute/chronoxide-query \
  STORAGE_VERIFY_BIN=/absolute/chronoxide-storage-verify \
  REPO_ROOT=/absolute/chronoxide-worktree \
  RUN_NOTE='quiet host; no builds, profilers, replay, or other databases active' \
    docs/experiments/storage_vnext/phase1_replay_run.sh [--dry-run] [--with-profile]

Modes:
  --validate-only  Hash and validate the pinned capture/template and binary interfaces.
                   RESULT_DIR is checked for freshness but is not created.
  --dry-run        Create a new provenance root and all rendered configs, but launch
                   no replay, cache eviction, verifier, readback, or profiler.
  --with-profile   Add one fresh, correctness-gated perf-record corpus after the
                   three measured replay corpora. It is excluded from latency data.

The default workload is the pinned four-million-message prefix. A real run
always creates replay-01, replay-02, and replay-03, never deletes output, and
refuses an existing RESULT_DIR. The capture and template paths may be overridden,
but their hashes and workload semantics must still match phase1_4m_expectations.json.

PERF_STAT_MODE is required (default), auto, or off. `auto` records an explicit
coverage gap if the exact event-set preflight fails. EVICT_CAPTURE=1 is the
default; fincore evidence must be at or below
MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT (default 0) before every replay.
EOF
}

die() {
    echo "Phase 1 replay: $*" >&2
    exit 2
}

note() {
    echo "Phase 1 replay: $*"
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

require_executable() {
    local name="$1"
    local path="$2"
    [[ "$path" == /* && -f "$path" && -x "$path" ]] \
        || die "$name must be an absolute executable regular file: $path"
}

for argument in "$@"; do
    case "$argument" in
        --dry-run)
            DRY_RUN=1
            ;;
        --validate-only)
            VALIDATE_ONLY=1
            ;;
        --with-profile)
            WITH_PROFILE=1
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

(( DRY_RUN + VALIDATE_ONLY <= 1 )) \
    || die "--dry-run and --validate-only are mutually exclusive"
require_bool WITH_PROFILE "$WITH_PROFILE"
require_bool EVICT_CAPTURE "$EVICT_CAPTURE"
require_bool ALLOW_NOISY_HOST "$ALLOW_NOISY_HOST"
[[ "$PERF_STAT_MODE" == "required" || "$PERF_STAT_MODE" == "auto" \
    || "$PERF_STAT_MODE" == "off" ]] \
    || die "PERF_STAT_MODE must be required, auto, or off"
[[ "$RSS_INTERVAL_MS" =~ ^[1-9][0-9]*$ && "$RSS_INTERVAL_MS" -ge 10 ]] \
    || die "RSS_INTERVAL_MS must be an integer of at least 10"
[[ "$READBACK_SAMPLE_LIMIT_PER_KIND" =~ ^[1-9][0-9]*$ ]] \
    || die "READBACK_SAMPLE_LIMIT_PER_KIND must be a positive integer"
[[ "$READBACK_SAMPLE_LIMIT_PER_KIND" == "2" ]] \
    || die "the pinned 38-query readback contract requires READBACK_SAMPLE_LIMIT_PER_KIND=2"
[[ "$MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT" =~ ^[0-9]+$ ]] \
    || die "MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT must be non-negative"
[[ "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
    || die "RUN_NOTE must not contain tabs or newlines"
if [[ "$DRY_RUN" != "1" && "$VALIDATE_ONLY" != "1" ]]; then
    [[ -n "$RUN_NOTE" ]] || die "RUN_NOTE is required for a measured run"
fi
if [[ "$ALLOW_NOISY_HOST" == "1" && "$RUN_NOTE" != *[Nn][Oo][Ii][Ss][Yy]* ]]; then
    die "ALLOW_NOISY_HOST=1 requires RUN_NOTE to contain the word noisy"
fi

for command in awk bash cmp cp date df diff find git grep mkdir ps python3 realpath sha256sum sort stat tail touch xargs uname /usr/bin/time; do
    require_command "$command"
done
[[ -f "$GATE_TOOL" ]] || die "Phase 1 gate helper is missing: $GATE_TOOL"
[[ -f "$REPLAY_REPORT_TOOL" ]] || die "replay report parser is missing: $REPLAY_REPORT_TOOL"
[[ -f "$FADVISE_SOURCE" ]] || die "cache-eviction helper source is missing: $FADVISE_SOURCE"
[[ -f "$EXPECTATIONS" ]] || die "expectations file is missing: $EXPECTATIONS"

[[ "$CAPTURE" == /* && -d "$CAPTURE" ]] || die "CAPTURE must be an absolute directory"
[[ "$CONFIG_TEMPLATE" == /* && -f "$CONFIG_TEMPLATE" ]] \
    || die "CONFIG_TEMPLATE must be an absolute regular file"
[[ "$REPO_ROOT" == /* && -d "$REPO_ROOT" ]] || die "REPO_ROOT must be absolute"
CAPTURE="$(realpath -e -- "$CAPTURE")"
CONFIG_TEMPLATE="$(realpath -e -- "$CONFIG_TEMPLATE")"
EXPECTATIONS="$(realpath -e -- "$EXPECTATIONS")"
REPO_ROOT="$(realpath -e -- "$REPO_ROOT")"
[[ "$(git -C "$REPO_ROOT" rev-parse --show-toplevel)" == "$REPO_ROOT" ]] \
    || die "REPO_ROOT must be the Git worktree root"

require_executable INGESTER_BIN "$INGESTER_BIN"
require_executable QUERY_BIN "$QUERY_BIN"
require_executable STORAGE_VERIFY_BIN "$STORAGE_VERIFY_BIN"
INGESTER_BIN="$(realpath -e -- "$INGESTER_BIN")"
QUERY_BIN="$(realpath -e -- "$QUERY_BIN")"
STORAGE_VERIFY_BIN="$(realpath -e -- "$STORAGE_VERIFY_BIN")"

query_help="$($QUERY_BIN --help 2>&1)"
for flag in --segments-dir --storage-layout --sample-limit-per-kind --verify-readbacks --output; do
    grep -Fq -- "$flag" <<<"$query_help" || die "query binary help is missing $flag"
done
grep -Fq -- schema8 <<<"$query_help" || die "query binary does not expose schema8"
verify_help="$($STORAGE_VERIFY_BIN --help 2>&1)"
for flag in --segments-dir --schema --validate-segment-footers --verify-exact-postings; do
    grep -Fq -- "$flag" <<<"$verify_help" \
        || die "storage verifier help is missing $flag"
done

[[ -n "$RESULT_DIR" && "$RESULT_DIR" == /* ]] \
    || die "RESULT_DIR must be a new absolute external path"
result_name="$(basename "$RESULT_DIR")"
[[ -n "$result_name" && "$result_name" != "." && "$result_name" != ".." ]] \
    || die "RESULT_DIR must name a new child of an existing directory"
result_parent_input="$(dirname "$RESULT_DIR")"
[[ -d "$result_parent_input" ]] || die "RESULT_DIR parent does not exist"
result_parent="$(realpath -e -- "$result_parent_input")"
RESULT_DIR="$result_parent/$result_name"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists; output is never reused: $RESULT_DIR"
for checked_path in "$CAPTURE" "$CONFIG_TEMPLATE" "$EXPECTATIONS" "$REPO_ROOT" \
        "$INGESTER_BIN" "$QUERY_BIN" "$STORAGE_VERIFY_BIN" "$RESULT_DIR"; do
    [[ "$checked_path" != *$'\t'* && "$checked_path" != *$'\n'* ]] \
        || die "input and output paths must not contain tabs or newlines"
done
case "$RESULT_DIR/" in
    "$REPO_ROOT/"*) die "RESULT_DIR must be outside the source worktree" ;;
    "$CAPTURE/"*) die "RESULT_DIR must not be inside the capture" ;;
esac

note "validating and hashing the pinned capture and configuration template"
validated_inputs_json="$(python3 "$GATE_TOOL" validate-inputs \
    --capture "$CAPTURE" \
    --template "$CONFIG_TEMPLATE" \
    --expectations "$EXPECTATIONS")"

if [[ "$VALIDATE_ONLY" == "1" ]]; then
    note "validation complete; RESULT_DIR was not created: $RESULT_DIR"
    exit 0
fi

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/configs" "$RESULT_DIR/metadata" "$RESULT_DIR/runs" \
    "$RESULT_DIR/validation" "$RESULT_DIR/comparisons"
CONFIG_DIR="$RESULT_DIR/configs"
METADATA_DIR="$RESULT_DIR/metadata"
RUNS_DIR="$RESULT_DIR/runs"
VALIDATION_DIR="$RESULT_DIR/validation"
COMPARISONS_DIR="$RESULT_DIR/comparisons"
BINARY_DIR="$METADATA_DIR/binaries"
HARNESS_DIR="$METADATA_DIR/harness"
SOURCE_DIR="$METADATA_DIR/source"
TOOLS_DIR="$METADATA_DIR/tools"
mkdir "$BINARY_DIR" "$HARNESS_DIR" "$SOURCE_DIR" "$TOOLS_DIR"

for harness_file in \
    phase1_replay_run.sh phase1_replay_gate.py phase1_4m_expectations.json \
    test_phase1_replay_gate.py ab_gate.py fadvise_regular_dontneed.c README.md; do
    [[ -f "$SCRIPT_DIR/$harness_file" ]] \
        || die "harness provenance file is missing: $harness_file"
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" "$HARNESS_DIR/$harness_file"
done
FROZEN_GATE="$HARNESS_DIR/phase1_replay_gate.py"
FROZEN_REPLAY_REPORT_TOOL="$HARNESS_DIR/ab_gate.py"
FROZEN_EXPECTATIONS="$HARNESS_DIR/phase1_4m_expectations.json"

printf 'role\tsource_path\tpreserved_path\tsha256\n' >"$METADATA_DIR/binaries.tsv"
preserve_binary() {
    local role="$1"
    local source="$2"
    local destination="$BINARY_DIR/$role"
    local source_hash
    local destination_hash

    [[ ! -e "$destination" ]] || die "refusing to reuse preserved binary: $destination"
    cp --reflink=auto --preserve=mode,timestamps -- "$source" "$destination"
    source_hash="$(sha256sum -- "$source")"
    source_hash="${source_hash%% *}"
    destination_hash="$(sha256sum -- "$destination")"
    destination_hash="${destination_hash%% *}"
    [[ "$source_hash" == "$destination_hash" ]] \
        || die "preserved binary differs from source: $role"
    printf '%s\t%s\t%s\t%s\n' "$role" "$source" "$destination" "$destination_hash" \
        >>"$METADATA_DIR/binaries.tsv"
}

preserve_binary chronoxide-ingester "$INGESTER_BIN"
preserve_binary chronoxide-query "$QUERY_BIN"
preserve_binary chronoxide-storage-verify "$STORAGE_VERIFY_BIN"
RUN_INGESTER="$BINARY_DIR/chronoxide-ingester"
RUN_QUERY="$BINARY_DIR/chronoxide-query"
RUN_STORAGE_VERIFY="$BINARY_DIR/chronoxide-storage-verify"

for binary in "$RUN_INGESTER" "$RUN_QUERY" "$RUN_STORAGE_VERIFY"; do
    role="$(basename "$binary")"
    file -- "$binary" >"$METADATA_DIR/$role.file.txt" 2>&1 || true
    if command -v readelf >/dev/null 2>&1; then
        readelf -n -- "$binary" >"$METADATA_DIR/$role.elf-notes.txt" 2>&1 || true
    fi
done

record_source_state() {
    git -C "$REPO_ROOT" rev-parse HEAD >"$SOURCE_DIR/git-head.txt"
    git -C "$REPO_ROOT" status --porcelain=v2 --branch >"$SOURCE_DIR/git-status.txt"
    git -C "$REPO_ROOT" remote -v >"$SOURCE_DIR/git-remotes.txt"
    git -C "$REPO_ROOT" ls-files -s >"$SOURCE_DIR/tracked-index.txt"
    git -C "$REPO_ROOT" diff --binary --full-index HEAD -- >"$SOURCE_DIR/tracked-combined.patch"
    git -C "$REPO_ROOT" diff --cached --binary --full-index -- >"$SOURCE_DIR/tracked-index.patch"
    git -C "$REPO_ROOT" diff --binary --full-index -- >"$SOURCE_DIR/tracked-worktree.patch"
    git -C "$REPO_ROOT" ls-files --others --exclude-standard \
        >"$SOURCE_DIR/untracked-paths.txt"
    (
        cd "$REPO_ROOT"
        while IFS= read -r -d '' path; do
            if [[ -f "$path" && ! -L "$path" ]]; then
                sha256sum -z -- "$path"
            fi
        done < <(git ls-files -z)
    ) >"$SOURCE_DIR/tracked-working-tree.sha256.nul"
    (
        cd "$REPO_ROOT"
        while IFS= read -r -d '' path; do
            if [[ -f "$path" && ! -L "$path" ]]; then
                sha256sum -z -- "$path"
            fi
        done < <(git ls-files --others --exclude-standard -z)
    ) >"$SOURCE_DIR/untracked-working-tree.sha256.nul"
    sha256sum \
        "$SOURCE_DIR/git-head.txt" \
        "$SOURCE_DIR/tracked-index.txt" \
        "$SOURCE_DIR/tracked-combined.patch" \
        "$SOURCE_DIR/tracked-working-tree.sha256.nul" \
        "$SOURCE_DIR/untracked-working-tree.sha256.nul" \
        >"$SOURCE_DIR/source-state.sha256"
}
record_source_state
(
    cd "$REPO_ROOT"
    while IFS= read -r -d '' build_input; do
        sha256sum -- "${build_input#./}"
    done < <(
        find . -type f \
            \( -name Cargo.toml -o -name Cargo.lock -o -name rust-toolchain \
                -o -name rust-toolchain.toml -o -path './.cargo/config' \
                -o -path './.cargo/config.toml' \) \
            ! -path './target/*' -print0 | sort -z
    )
) >"$SOURCE_DIR/build-inputs.sha256"

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'with_profile=%s\n' "$WITH_PROFILE"
    printf 'capture=%s\n' "$CAPTURE"
    printf 'config_template=%s\n' "$CONFIG_TEMPLATE"
    printf 'expectations=%s\n' "$EXPECTATIONS"
    printf 'repo_root=%s\n' "$REPO_ROOT"
    printf 'result_dir=%s\n' "$RESULT_DIR"
    printf 'build_command=%s\n' "$BUILD_COMMAND"
    printf 'rustflags=%s\n' "${RUSTFLAGS:-}"
    printf 'cargo_target_dir=%s\n' "${CARGO_TARGET_DIR:-}"
    printf 'cc=%s\n' "${CC:-}"
    printf 'cflags=%s\n' "${CFLAGS:-}"
    printf 'rust_log=%s\n' "$RUST_LOG_VALUE"
    printf 'perf_stat_mode=%s\n' "$PERF_STAT_MODE"
    printf 'evict_capture=%s\n' "$EVICT_CAPTURE"
    printf 'max_capture_resident_bytes_after_evict=%s\n' \
        "$MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT"
    printf 'rss_interval_ms=%s\n' "$RSS_INTERVAL_MS"
    printf 'readback_sample_limit_per_kind=%s\n' "$READBACK_SAMPLE_LIMIT_PER_KIND"
    printf 'allow_noisy_host=%s\n' "$ALLOW_NOISY_HOST"
    printf 'run_note=%s\n' "$RUN_NOTE"
    printf 'normal_head_note=segment_writer.enabled makes the effective normal head duration equal the 900-second segment duration; the configured 3600-second head duration is not effective\n'
    printf 'footer_validation=separate untimed exhaustive storage-verifier pass on replay-01\n'
    printf 'readback_validation=separate untimed independent query-oracle pass on replay-01\n'
    printf 'profile_note=profile is a separate fourth replay and never contributes measured latency\n'
} >"$METADATA_DIR/settings.txt"
printf '%s\n' "$RUN_NOTE" >"$METADATA_DIR/run-note.txt"

printf '%s\n' "$validated_inputs_json" >"$METADATA_DIR/validated-inputs.json"
cp --preserve=mode,timestamps -- "$CONFIG_TEMPLATE" "$METADATA_DIR/config-template.toml"
cp --preserve=mode,timestamps -- "$CAPTURE/manifest.json" "$METADATA_DIR/capture-manifest.json"

{
    date --iso-8601=seconds
    uname -a || true
    command -v lscpu >/dev/null 2>&1 && lscpu || true
    command -v rustc >/dev/null 2>&1 && rustc --version --verbose || true
    command -v cargo >/dev/null 2>&1 && cargo --version --verbose || true
    command -v rustup >/dev/null 2>&1 && rustup show active-toolchain || true
    command -v ld >/dev/null 2>&1 && ld --version || true
    command -v clang >/dev/null 2>&1 && clang --version || true
    command -v perf >/dev/null 2>&1 && perf --version || true
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$CAPTURE" || true
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$RESULT_DIR" || true
    stat -f -c 'capture_filesystem_type=%T capture_mount=%m' "$CAPTURE" || true
    stat -f -c 'result_filesystem_type=%T result_mount=%m' "$RESULT_DIR" || true
    df -B1 "$RESULT_DIR" || true
    ulimit -a || true
    [[ -r /proc/meminfo ]] && cat /proc/meminfo || true
    for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
        [[ -r "$pressure" ]] && { printf '%s\n' "$pressure"; cat "$pressure"; }
    done
} >"$METADATA_DIR/environment.txt" 2>&1
ps -eo pid=,ppid=,pcpu=,pmem=,rss=,etime=,stat=,comm=,args= \
    >"$METADATA_DIR/processes-at-plan.txt"

declare -a RUN_LABELS=(replay-01 replay-02 replay-03)
declare -A RUN_KIND=(
    [replay-01]=measured
    [replay-02]=measured
    [replay-03]=measured
)
if [[ "$WITH_PROFILE" == "1" ]]; then
    RUN_LABELS+=(profile)
    RUN_KIND[profile]=profile
fi

stop_after_messages="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["stop_after_messages"])' "$FROZEN_EXPECTATIONS")"
printf 'order\tlabel\tkind\tconfig\tsegments_dir\n' >"$RESULT_DIR/run-plan.tsv"
for index in "${!RUN_LABELS[@]}"; do
    label="${RUN_LABELS[$index]}"
    kind="${RUN_KIND[$label]}"
    run_dir="$RUNS_DIR/$label"
    segments_dir="$run_dir/segments"
    mkdir "$run_dir"
    python3 "$FROZEN_GATE" render-config \
        --template "$METADATA_DIR/config-template.toml" \
        --output "$CONFIG_DIR/$label.toml" \
        --capture "$CAPTURE" \
        --segments-dir "$segments_dir" \
        --stop-after-messages "$stop_after_messages" \
        >"$run_dir/config-render.json"
    printf '%s\t%s\t%s\t%s\t%s\n' "$((index + 1))" "$label" "$kind" \
        "$CONFIG_DIR/$label.toml" "$segments_dir" >>"$RESULT_DIR/run-plan.tsv"
done
sha256sum "$CONFIG_DIR"/*.toml >"$METADATA_DIR/rendered-configs.sha256"
(
    cd "$HARNESS_DIR"
    find . -type f -print0 | sort -z | xargs -0 sha256sum
) >"$METADATA_DIR/harness.sha256"

if [[ "$DRY_RUN" == "1" ]]; then
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    note "dry run complete; no replay or validation process was launched: $RESULT_DIR"
    exit 0
fi

COVERAGE_GAPS=0
printf 'kind\tdetail\n' >"$METADATA_DIR/coverage-gaps.tsv"
record_coverage_gap() {
    local kind="$1"
    local detail="$2"
    COVERAGE_GAPS=1
    printf '%s\t%s\n' "$kind" "$detail" >>"$METADATA_DIR/coverage-gaps.tsv"
}
if [[ "$EVICT_CAPTURE" != "1" ]]; then
    record_coverage_gap capture_cache 'capture eviction and residency proof disabled'
fi
if [[ "$PERF_STAT_MODE" == "off" ]]; then
    record_coverage_gap perf_stat 'perf stat collection explicitly disabled'
fi
if [[ "$ALLOW_NOISY_HOST" == "1" ]]; then
    record_coverage_gap host_noise 'noisy-host execution explicitly allowed'
fi

if [[ "$EVICT_CAPTURE" == "1" ]]; then
    for command in cc fincore; do
        require_command "$command"
    done
    cc -O2 -Wall -Wextra -Werror -o "$TOOLS_DIR/fadvise-regular-dontneed" \
        "$HARNESS_DIR/fadvise_regular_dontneed.c"
    sha256sum "$TOOLS_DIR/fadvise-regular-dontneed" \
        >"$TOOLS_DIR/fadvise-regular-dontneed.sha256"
fi

PERF_STAT_EVENTS="task-clock,cycles,instructions,branches,branch-misses,cache-references,cache-misses,page-faults,context-switches,cpu-migrations"
IFS=, read -r -a perf_stat_event_names <<<"$PERF_STAT_EVENTS"
declare -a PERF_STAT_REQUIRED_ARGS=()
for perf_event in "${perf_stat_event_names[@]}"; do
    PERF_STAT_REQUIRED_ARGS+=(--require-event "$perf_event")
done
PERF_STAT_EFFECTIVE=off
if [[ "$PERF_STAT_MODE" != "off" ]]; then
    require_command perf
    set +e
    perf stat --no-big-num --field-separator $'\t' \
        --event "$PERF_STAT_EVENTS" \
        --output "$METADATA_DIR/perf-stat-preflight.tsv" -- \
        python3 -c 'sum(range(10000000))' \
        >"$METADATA_DIR/perf-stat-preflight.log" 2>&1
    perf_preflight_status=$?
    set -e
    printf '%s\n' "$perf_preflight_status" >"$METADATA_DIR/perf-stat-preflight.exit-status"
    if (( perf_preflight_status == 0 )); then
        set +e
        python3 "$FROZEN_GATE" parse-perf-stat \
            --input "$METADATA_DIR/perf-stat-preflight.tsv" \
            --output "$METADATA_DIR/perf-stat-preflight.json" \
            "${PERF_STAT_REQUIRED_ARGS[@]}" \
            >"$METADATA_DIR/perf-stat-preflight-parse.log" 2>&1
        perf_preflight_parse_status=$?
        set -e
        printf '%s\n' "$perf_preflight_parse_status" \
            >"$METADATA_DIR/perf-stat-preflight-parse.exit-status"
    else
        perf_preflight_parse_status=1
    fi
    if (( perf_preflight_status == 0 && perf_preflight_parse_status == 0 )); then
        PERF_STAT_EFFECTIVE=on
    elif [[ "$PERF_STAT_MODE" == "required" ]]; then
        die "perf stat preflight failed; preserved output under metadata/"
    else
        printf 'perf stat auto-mode preflight failed; measured replays use GNU time and /proc RSS only\n' \
            >"$METADATA_DIR/PERF_STAT_COVERAGE_GAP"
        record_coverage_gap perf_stat 'auto-mode preflight failed; required counters absent'
    fi
fi
if [[ "$WITH_PROFILE" == "1" ]]; then
    require_command perf
    set +e
    perf record --output "$METADATA_DIR/perf-record-preflight.data" \
        --freq 49 --event cpu-clock --call-graph "dwarf,32768" -- true \
        >"$METADATA_DIR/perf-record-preflight.log" 2>&1
    profile_preflight_status=$?
    set -e
    printf '%s\n' "$profile_preflight_status" \
        >"$METADATA_DIR/perf-record-preflight.exit-status"
    (( profile_preflight_status == 0 )) \
        || die "perf record preflight failed; the profile run was not launched"
fi

expected_corpus_bytes="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["corpus"]["size_bytes"])' "$FROZEN_EXPECTATIONS")"
run_count="${#RUN_LABELS[@]}"
minimum_free_bytes=$((expected_corpus_bytes * run_count + 2147483648))
available_bytes="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 { print $1 }')"
[[ "$available_bytes" =~ ^[0-9]+$ ]] || die "could not determine free result-filesystem bytes"
(( available_bytes >= minimum_free_bytes )) \
    || die "result filesystem has $available_bytes bytes free; at least $minimum_free_bytes are required"

check_measurement_conflicts() {
    local snapshot="$1"
    local conflicts
    ps -eo pid=,ppid=,pcpu=,pmem=,rss=,etime=,stat=,comm=,args= >"$snapshot"
    conflicts="$(
        awk -v own="$$" '
            $1 != own && ($8 == "cargo" || $8 == "rustc" || $8 == "perf" ||
                $8 ~ /^chronoxide-/ || $8 ~ /^greptime/ || $8 == "prometheus") { print }
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

snapshot_capture_residency() {
    local output="$1"
    local file
    : >"$output"
    while IFS= read -r -d '' file; do
        fincore --bytes --noheadings --output RES,SIZE,FILE -- "$file" >>"$output"
    done < <(find "$CAPTURE" -maxdepth 1 -type f -name '*.capture' -print0 | sort -z)
}

prepare_capture_cache() {
    local run_dir="$1"
    local resident_bytes
    local file
    [[ "$EVICT_CAPTURE" == "1" ]] || {
        printf 'capture eviction disabled; OS cache state is uncontrolled\n' \
            >"$run_dir/CAPTURE_CACHE_COVERAGE_GAP"
        return
    }
    while IFS= read -r -d '' file; do
        "$TOOLS_DIR/fadvise-regular-dontneed" "$file"
    done < <(find "$CAPTURE" -maxdepth 1 -type f -name '*.capture' -print0 | sort -z)
    snapshot_capture_residency "$run_dir/capture-residency-before.tsv"
    resident_bytes="$(awk '{ total += $1 } END { printf "%.0f", total }' \
        "$run_dir/capture-residency-before.tsv")"
    [[ "$resident_bytes" =~ ^[0-9]+$ ]] \
        || die "could not parse capture residency before $run_dir"
    (( resident_bytes <= MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT )) \
        || die "capture retained $resident_bytes bytes after eviction; limit is $MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT"
    if (( resident_bytes > 0 )); then
        record_coverage_gap capture_residency \
            "$run_dir began with $resident_bytes resident capture bytes"
    fi
}

printf 'label\tkind\telapsed\tuser_seconds\tsystem_seconds\ttime_max_rss_kib\tproc_peak_aggregate_rss_kib\tproc_peak_aggregate_anon_kib\tproc_peak_aggregate_file_kib\tproc_peak_aggregate_swap_kib\tcorpus_files\tcorpus_bytes\tcorpus_manifest_sha256\tperf_stat\n' \
    >"$RESULT_DIR/replay-summary.tsv"

run_replay() {
    local label="$1"
    local kind="${RUN_KIND[$label]}"
    local run_dir="$RUNS_DIR/$label"
    local config="$CONFIG_DIR/$label.toml"
    local segments_dir="$run_dir/segments"
    local launcher_pid
    local monitor_pid
    local status
    local monitor_status
    local report
    local -a command

    [[ ! -e "$segments_dir" ]] || die "refusing to reuse segment output: $segments_dir"
    check_measurement_conflicts "$run_dir/processes-before.txt"
    snapshot_pressure "$run_dir/pressure-before.txt"
    prepare_capture_cache "$run_dir"
    note "running $label ($kind)"

    command=(
        env
        -u OTEL_EXPORTER_OTLP_ENDPOINT
        -u OTEL_EXPORTER_OTLP_LOGS_ENDPOINT
        -u OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
        "CONFIG_FILE=$config"
        "RUST_LOG=$RUST_LOG_VALUE"
        "$RUN_INGESTER"
    )
    if [[ "$kind" == "profile" ]]; then
        command=(
            perf record
            --output "$run_dir/perf.data"
            --freq 49
            --event cpu-clock
            --call-graph "dwarf,32768"
            --
            "${command[@]}"
        )
    elif [[ "$PERF_STAT_EFFECTIVE" == "on" ]]; then
        command=(
            perf stat
            --no-big-num
            --field-separator $'\t'
            --event "$PERF_STAT_EVENTS"
            --output "$run_dir/perf-stat.tsv"
            --
            "${command[@]}"
        )
    fi

    set +e
    (
        cd "$run_dir"
        LC_ALL=C /usr/bin/time -v -o "$run_dir/replay.time.txt" \
            "${command[@]}" >"$run_dir/replay.log" 2>&1
    ) &
    launcher_pid=$!
    python3 "$FROZEN_GATE" monitor-rss \
        --pid "$launcher_pid" \
        --output "$run_dir/rss-samples.tsv" \
        --summary "$run_dir/rss-summary.json" \
        --interval-ms "$RSS_INTERVAL_MS" \
        >"$run_dir/rss-monitor.log" 2>&1 &
    monitor_pid=$!
    wait "$launcher_pid"
    status=$?
    wait "$monitor_pid"
    monitor_status=$?
    set -e
    printf '%s\n' "$status" >"$run_dir/replay.exit-status"
    printf '%s\n' "$monitor_status" >"$run_dir/rss-monitor.exit-status"
    (( monitor_status == 0 )) || die "$label RSS monitor failed; partial output is preserved"
    if (( status != 0 )); then
        tail -n 80 "$run_dir/replay.log" >&2 || true
        die "$label failed with status $status; partial output is preserved"
    fi
    [[ -d "$segments_dir" ]] || die "$label completed without creating a segment corpus"
    python3 "$FROZEN_GATE" parse-time \
        --input "$run_dir/replay.time.txt" --output "$run_dir/replay.time.json" >/dev/null
    if [[ "$kind" == "measured" && "$PERF_STAT_EFFECTIVE" == "on" ]]; then
        python3 "$FROZEN_GATE" parse-perf-stat \
            --input "$run_dir/perf-stat.tsv" --output "$run_dir/perf-stat.json" \
            "${PERF_STAT_REQUIRED_ARGS[@]}" >/dev/null
    fi
    if [[ "$kind" == "profile" ]]; then
        perf report --stdio --no-children --percent-limit 0.05 \
            --input "$run_dir/perf.data" >"$run_dir/perf-report-self.txt" 2>"$run_dir/perf-report-self.log"
        perf report --stdio --children --percent-limit 0.05 \
            --input "$run_dir/perf.data" >"$run_dir/perf-report-children.txt" 2>"$run_dir/perf-report-children.log"
    fi
    if [[ "$EVICT_CAPTURE" == "1" ]]; then
        snapshot_capture_residency "$run_dir/capture-residency-after.tsv"
    fi
    snapshot_pressure "$run_dir/pressure-after.txt"
    check_measurement_conflicts "$run_dir/processes-after.txt"

    mapfile -d '' -t reports \
        < <(find "$run_dir" -maxdepth 1 -type f -name 'ingestion_stats_*.md' -print0)
    (( ${#reports[@]} == 1 )) \
        || die "$label must produce exactly one ingestion report; found ${#reports[@]}"
    report="${reports[0]}"
    python3 "$FROZEN_REPLAY_REPORT_TOOL" replay-report \
        --report "$report" --output "$run_dir/replay-correctness.json"
    python3 "$FROZEN_GATE" gate-correctness \
        --actual "$run_dir/replay-correctness.json" \
        --expectations "$FROZEN_EXPECTATIONS"
    python3 "$FROZEN_GATE" tree-manifest \
        --corpus "$segments_dir" \
        --manifest "$run_dir/segments.sha256" \
        --inventory "$run_dir/segments.tsv" \
        --summary "$run_dir/corpus-summary.json" >/dev/null
    python3 "$FROZEN_GATE" gate-corpus \
        --actual "$run_dir/corpus-summary.json" \
        --expectations "$FROZEN_EXPECTATIONS"
    python3 "$FROZEN_GATE" run-summary \
        --label "$label" --kind "$kind" \
        --time "$run_dir/replay.time.json" \
        --rss "$run_dir/rss-summary.json" \
        --corpus "$run_dir/corpus-summary.json" \
        --perf-status "$([[ "$kind" == "measured" ]] && printf '%s' "$PERF_STAT_EFFECTIVE" || printf 'profile')" \
        >>"$RESULT_DIR/replay-summary.tsv"
}

for label in "${RUN_LABELS[@]}"; do
    run_replay "$label"
done

for label in "${RUN_LABELS[@]:1}"; do
    if ! cmp -s "$RUNS_DIR/replay-01/segments.sha256" "$RUNS_DIR/$label/segments.sha256"; then
        diff -u "$RUNS_DIR/replay-01/segments.sha256" "$RUNS_DIR/$label/segments.sha256" \
            >"$COMPARISONS_DIR/replay-01-vs-$label.manifest.diff" || true
        die "$label corpus is not byte-identical to replay-01"
    fi
    if ! cmp -s "$RUNS_DIR/replay-01/replay-correctness.json" \
            "$RUNS_DIR/$label/replay-correctness.json"; then
        diff -u "$RUNS_DIR/replay-01/replay-correctness.json" \
            "$RUNS_DIR/$label/replay-correctness.json" \
            >"$COMPARISONS_DIR/replay-01-vs-$label.correctness.diff" || true
        die "$label replay counters differ from replay-01"
    fi
done
printf 'all planned corpora are byte-identical and match the pinned 66-file, 5,569,314,896-byte manifest; replay correctness documents are identical\n' \
    >"$COMPARISONS_DIR/determinism.txt"

note "running exhaustive footer/series/exact-postings verification outside measured replay"
check_measurement_conflicts "$VALIDATION_DIR/processes-before-storage-verify.txt"
set +e
/usr/bin/time -v -o "$VALIDATION_DIR/storage-verify.time.txt" \
    "$RUN_STORAGE_VERIFY" \
        --segments-dir "$RUNS_DIR/replay-01/segments" \
        --schema schema8 \
        --validate-segment-footers \
        --verify-exact-postings \
        >"$VALIDATION_DIR/storage-verify.json" \
        2>"$VALIDATION_DIR/storage-verify.log"
verify_status=$?
set -e
printf '%s\n' "$verify_status" >"$VALIDATION_DIR/storage-verify.exit-status"
(( verify_status == 0 )) || die "exhaustive storage verification failed"
python3 "$FROZEN_GATE" gate-verifier \
    --actual "$VALIDATION_DIR/storage-verify.json" \
    --expectations "$FROZEN_EXPECTATIONS"

note "running independent readbacks outside measured replay"
check_measurement_conflicts "$VALIDATION_DIR/processes-before-readbacks.txt"
set +e
/usr/bin/time -v -o "$VALIDATION_DIR/readbacks.time.txt" \
    "$RUN_QUERY" \
        --segments-dir "$RUNS_DIR/replay-01/segments" \
        --storage-layout schema8 \
        --sample-limit-per-kind "$READBACK_SAMPLE_LIMIT_PER_KIND" \
        --verify-readbacks \
        --output "$VALIDATION_DIR/readbacks.md" \
        >"$VALIDATION_DIR/readbacks.log" 2>&1
readback_status=$?
set -e
printf '%s\n' "$readback_status" >"$VALIDATION_DIR/readbacks.exit-status"
(( readback_status == 0 )) || die "independent readback verification failed"
python3 "$FROZEN_GATE" gate-readbacks \
    --report "$VALIDATION_DIR/readbacks.md" \
    --expectations "$FROZEN_EXPECTATIONS" \
    --output "$VALIDATION_DIR/readbacks.json" >/dev/null

check_measurement_conflicts "$VALIDATION_DIR/processes-after.txt"
(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        case "$artifact" in
            runs/*/segments/*|./runs/*/segments/*) continue ;;
        esac
        sha256sum -- "${artifact#./}"
    done < <(find metadata configs validation comparisons runs -type f -print0 | sort -z)
) >"$METADATA_DIR/result-artifacts.sha256"

if [[ "$COVERAGE_GAPS" == "0" ]]; then
    touch "$RESULT_DIR/COMPLETE"
    note "complete: $RESULT_DIR"
else
    touch "$RESULT_DIR/COMPLETE_WITH_COVERAGE_GAPS"
    note "correctness complete with measurement coverage gaps: $RESULT_DIR"
fi
