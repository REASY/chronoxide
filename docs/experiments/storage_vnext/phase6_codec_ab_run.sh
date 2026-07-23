#!/usr/bin/env bash

set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1
export PYTHONNOUSERSITE=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE_TOOL="$SCRIPT_DIR/phase6_codec_ab_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"
EXPECTATIONS="$SCRIPT_DIR/phase1_4m_expectations.json"

CAPTURE="${CAPTURE:-}"
CONFIG_TEMPLATE="${CONFIG_TEMPLATE:-}"
STOP_AFTER_MESSAGES="${STOP_AFTER_MESSAGES:-4000000}"
REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
RESULT_DIR="${RESULT_DIR:-}"
INGESTER_BIN="${INGESTER_BIN:-}"
QUERY_BIN="${QUERY_BIN:-}"
STORAGE_VERIFY_BIN="${STORAGE_VERIFY_BIN:-}"
DEFAULT_QUERY_MANIFEST="$SCRIPT_DIR/phase6_codec_queries.json"
QUERY_MANIFEST="${QUERY_MANIFEST:-$DEFAULT_QUERY_MANIFEST}"
BINARY_PROVENANCE_MODE="${BINARY_PROVENANCE_MODE:-internal}"
RUN_NOTE="${RUN_NOTE:-}"
RUST_LOG_VALUE="${RUST_LOG_VALUE:-chronoxide_ingester=info,chronoxide_core=warn,chronoxide_core::storage::segment::writer=info}"
REPLAY_BLOCKS="${REPLAY_BLOCKS:-2}"
QUERY_BLOCKS="${QUERY_BLOCKS:-2}"
BENCHMARK_REPEATS="${BENCHMARK_REPEATS:-3}"
PERF_STAT_MODE="${PERF_STAT_MODE:-required}"
PERF_BIN=-
PERF_BINARY_SHA256=-
PERF_VERSION=-
RSS_INTERVAL_MS="${RSS_INTERVAL_MS:-100}"
GUARD_INTERVAL_MS="${GUARD_INTERVAL_MS:-100}"
CAPACITY_MONITOR_INTERVAL_MS="${CAPACITY_MONITOR_INTERVAL_MS:-100}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT="${MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT:-0}"
MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT="${MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT:-0}"
MAX_DIRTY_WRITEBACK_BYTES="${MAX_DIRTY_WRITEBACK_BYTES:-67108864}"
READBACK_SAMPLE_LIMIT_PER_KIND="${READBACK_SAMPLE_LIMIT_PER_KIND:-2}"
CHUNK_READ_QUEUE_DEPTH="${CHUNK_READ_QUEUE_DEPTH:-128}"
QUERY_LABEL_ARENA_MAX_BYTES="${QUERY_LABEL_ARENA_MAX_BYTES:-536870912}"
QUERY_MAX_SERIES_MATCHED="${QUERY_MAX_SERIES_MATCHED:-1000000}"
QUERY_MAX_PROJECTED_SERIES="${QUERY_MAX_PROJECTED_SERIES:-2000000}"
QUERY_MAX_CHUNKS_READ="${QUERY_MAX_CHUNKS_READ:-5000000}"
QUERY_MAX_BYTES_READ="${QUERY_MAX_BYTES_READ:-2147483648}"
QUERY_MAX_SAMPLES="${QUERY_MAX_SAMPLES:-50000000}"
REGEX_MAX_EXPANDED_VALUES="${REGEX_MAX_EXPANDED_VALUES:-100000}"
DRY_RUN=0

usage() {
    cat <<'EOF'
Usage:
  CAPTURE=/absolute/capture \
  CONFIG_TEMPLATE=/absolute/production-schema8-config.toml \
  RESULT_DIR=/absolute/new/external/result-root \
  RUN_NOTE='quiet host; no builds, Android/Docker, profiler, or database workload' \
  QUIET_HOST_CONFIRMED=1 \
    docs/experiments/storage_vnext/phase6_codec_ab_run.sh [--dry-run]

The default `BINARY_PROVENANCE_MODE=internal` performs one clean-tree,
source-sealed `cargo build --locked --release --no-default-features` from an
exact read-only `git archive HEAD` snapshot, in a fresh result-local target
directory under a sanitized environment. It preserves and hash-locks the three
resulting binaries before any help probe or experiment.

`BINARY_PROVENANCE_MODE=external-exploratory` additionally requires
INGESTER_BIN, QUERY_BIN, and STORAGE_VERIFY_BIN. That mode is explicitly
non-promotable and cannot emit the formal completion marker.

One preserved release binary is used for both variants. The only codec control
is ingestion.head_buffer.float_encoding plus the matching segment-writer field.
Every replay gets a fresh isolated segment root. Odd blocks use
Raw,Gorilla,Gorilla,Raw; even blocks reverse the order. Query processes use the
same counterbalanced schedule and record one cold plus two warm evaluations.

The internal mode's sole build completes before measurement, and the runner
never reuses output. It performs exhaustive storage verification, footer
validation, exact-postings verification, readbacks, and semantic gates outside
replay/query timing. Timestamp codec candidates are inventoried, but the result
is deliberately marked blocked because the current writer and reader expose no
versioned timestamp-codec selector.
EOF
}

die() {
    echo "Phase 6 codec A/B: $*" >&2
    exit 2
}

note() {
    echo "Phase 6 codec A/B: $*"
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

require_positive() {
    local name="$1"
    local value="$2"
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "$name must be a positive integer"
}

require_nonnegative() {
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

PYTHON_BIN="$(type -P python3 || true)"
[[ "$PYTHON_BIN" == /* && -x "$PYTHON_BIN" ]] \
    || die "python3 must resolve to an absolute executable path"
python3() {
    "$PYTHON_BIN" -I -S -B "$@"
}
python3_background() {
    exec "$PYTHON_BIN" -I -S -B "$@"
}
verify_background_python_pid_binding() {
    local probe observed_pid bound_pid probe_status
    probe="$({
        python3_background -c \
            'import os,sys; sys.stdout.write(str(os.getpid())); sys.stdout.flush()' &
        bound_pid=$!
        if wait "$bound_pid"; then probe_status=0; else probe_status=$?; fi
        printf '\t%s\t%s\n' "$bound_pid" "$probe_status"
    })"
    IFS=$'\t' read -r observed_pid bound_pid probe_status <<<"$probe"
    [[ "$observed_pid" =~ ^[1-9][0-9]*$ \
        && "$bound_pid" =~ ^[1-9][0-9]*$ \
        && "$probe_status" == 0 \
        && "$observed_pid" == "$bound_pid" ]] \
        || die "background Python PID binding probe failed: observed=$observed_pid bound=$bound_pid status=$probe_status"
}

for command in awk bash cc cmp cp date df find fincore getconf git grep mkdir ps python3 realpath sha256sum sleep sort stat sync tail tar touch uname /usr/bin/time; do
    require_command "$command"
done
verify_background_python_pid_binding
PAGE_SIZE_BYTES="$(getconf PAGESIZE)" \
    || die "could not determine the producer page size"
require_positive PAGE_SIZE_BYTES "$PAGE_SIZE_BYTES"
for file in "$GATE_TOOL" "$FADVISE_SOURCE" "$QUERY_MANIFEST" "$EXPECTATIONS"; do
    [[ -f "$file" ]] || die "required harness file is missing: $file"
done
PYTHONDONTWRITEBYTECODE=1 python3 "$GATE_TOOL" check-ambient-env >/dev/null \
    || die "ambient build/runtime environment violates the sanitized contract"
require_env CAPTURE
require_env CONFIG_TEMPLATE
require_env RESULT_DIR
case "$BINARY_PROVENANCE_MODE" in
    internal)
        [[ -z "$INGESTER_BIN" && -z "$QUERY_BIN" && -z "$STORAGE_VERIFY_BIN" ]] \
            || die "internal mode rejects external binary paths"
        require_command cargo
        require_command rustc
        ;;
    external-exploratory)
        require_env INGESTER_BIN
        require_env QUERY_BIN
        require_env STORAGE_VERIFY_BIN
        ;;
    *) die "BINARY_PROVENANCE_MODE must be internal or external-exploratory" ;;
esac
require_positive STOP_AFTER_MESSAGES "$STOP_AFTER_MESSAGES"
[[ "$STOP_AFTER_MESSAGES" == "4000000" ]] \
    || die "the capacity proof requires STOP_AFTER_MESSAGES=4000000"
require_positive REPLAY_BLOCKS "$REPLAY_BLOCKS"
[[ "$REPLAY_BLOCKS" == "2" ]] || die "the capacity proof requires REPLAY_BLOCKS=2"
require_positive QUERY_BLOCKS "$QUERY_BLOCKS"
[[ "$QUERY_BLOCKS" == "2" ]] || die "the formal gate requires QUERY_BLOCKS=2"
require_positive BENCHMARK_REPEATS "$BENCHMARK_REPEATS"
[[ "$BENCHMARK_REPEATS" == "3" ]] || die "current raw-v13 gate requires BENCHMARK_REPEATS=3"
require_positive RSS_INTERVAL_MS "$RSS_INTERVAL_MS"
[[ "$RSS_INTERVAL_MS" == "100" ]] || die "RSS_INTERVAL_MS must be exactly 100"
require_positive GUARD_INTERVAL_MS "$GUARD_INTERVAL_MS"
[[ "$GUARD_INTERVAL_MS" == "100" ]] || die "GUARD_INTERVAL_MS must be exactly 100"
require_positive CAPACITY_MONITOR_INTERVAL_MS "$CAPACITY_MONITOR_INTERVAL_MS"
[[ "$CAPACITY_MONITOR_INTERVAL_MS" == "100" ]] \
    || die "CAPACITY_MONITOR_INTERVAL_MS must be exactly 100"
require_bool QUIET_HOST_CONFIRMED "$QUIET_HOST_CONFIRMED"
require_nonnegative MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT "$MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT"
require_nonnegative MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT "$MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT"
require_nonnegative MAX_DIRTY_WRITEBACK_BYTES "$MAX_DIRTY_WRITEBACK_BYTES"
[[ "$MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT" == "0" ]] \
    || die "MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT must be exactly 0"
[[ "$MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT" == "0" ]] \
    || die "MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT must be exactly 0"
[[ "$MAX_DIRTY_WRITEBACK_BYTES" == "67108864" ]] \
    || die "MAX_DIRTY_WRITEBACK_BYTES must be exactly 67108864"
require_positive READBACK_SAMPLE_LIMIT_PER_KIND "$READBACK_SAMPLE_LIMIT_PER_KIND"
require_positive CHUNK_READ_QUEUE_DEPTH "$CHUNK_READ_QUEUE_DEPTH"
require_positive QUERY_LABEL_ARENA_MAX_BYTES "$QUERY_LABEL_ARENA_MAX_BYTES"
[[ "$QUERY_LABEL_ARENA_MAX_BYTES" == "536870912" ]] || die "Phase 6 fixes the CompactIds arena at 512 MiB"
for name in QUERY_MAX_SERIES_MATCHED QUERY_MAX_PROJECTED_SERIES QUERY_MAX_CHUNKS_READ QUERY_MAX_BYTES_READ QUERY_MAX_SAMPLES REGEX_MAX_EXPANDED_VALUES; do
    require_positive "$name" "${!name}"
done
[[ "$PERF_STAT_MODE" == "required" || "$PERF_STAT_MODE" == "auto" || "$PERF_STAT_MODE" == "off" ]] \
    || die "PERF_STAT_MODE must be required, auto, or off"
if [[ "$BINARY_PROVENANCE_MODE" == "internal" && "$PERF_STAT_MODE" != "required" ]]; then
    die "formal internal runs require PERF_STAT_MODE=required"
fi
if [[ "$PERF_STAT_MODE" != "off" ]]; then
    require_command perf
    PERF_BIN="$(type -P perf)"
    PERF_BIN="$(realpath -e -- "$PERF_BIN")"
    [[ "$PERF_BIN" == /* && -f "$PERF_BIN" && ! -L "$PERF_BIN" && -x "$PERF_BIN" ]] \
        || die "perf must resolve to a canonical absolute executable"
    PERF_BINARY_SHA256="$(sha256sum "$PERF_BIN" | awk '{print $1}')"
    PERF_VERSION="$(env -i LC_ALL=C TZ=UTC "$PERF_BIN" --version)" \
        || die "could not read the perf version"
    [[ -n "$PERF_VERSION" && "$PERF_VERSION" != *$'\n'* \
        && "$PERF_VERSION" != *$'\r'* && "$PERF_VERSION" != *$'\t'* ]] \
        || die "perf --version must produce one non-empty safe line"
fi
[[ "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] || die "RUN_NOTE must contain no tabs or newlines"
[[ -n "$RUST_LOG_VALUE" && "$RUST_LOG_VALUE" != *$'\n'* \
    && "$RUST_LOG_VALUE" != *$'\t'* ]] \
    || die "RUST_LOG_VALUE must be non-empty and contain no tabs or newlines"
if [[ "$DRY_RUN" != "1" ]]; then
    [[ "$QUIET_HOST_CONFIRMED" == "1" ]] || die "measured runs require QUIET_HOST_CONFIRMED=1"
    [[ -n "$RUN_NOTE" ]] || die "measured runs require RUN_NOTE"
fi

[[ "$CAPTURE" == /* && -d "$CAPTURE" && ! -L "$CAPTURE" ]] \
    || die "CAPTURE must be an absolute non-symlink directory"
[[ "$CONFIG_TEMPLATE" == /* && -f "$CONFIG_TEMPLATE" && ! -L "$CONFIG_TEMPLATE" ]] \
    || die "CONFIG_TEMPLATE must be an absolute non-symlink file"
[[ "$QUERY_MANIFEST" == /* && -f "$QUERY_MANIFEST" && ! -L "$QUERY_MANIFEST" ]] \
    || die "QUERY_MANIFEST must be an absolute non-symlink file"
[[ "$EXPECTATIONS" == /* && -f "$EXPECTATIONS" && ! -L "$EXPECTATIONS" ]] \
    || die "EXPECTATIONS must be an absolute non-symlink file"
[[ "$REPO_ROOT" == /* && -d "$REPO_ROOT" && ! -L "$REPO_ROOT" ]] \
    || die "REPO_ROOT must be an absolute non-symlink directory"
if [[ "$BINARY_PROVENANCE_MODE" == "external-exploratory" ]]; then
    for name in INGESTER_BIN QUERY_BIN STORAGE_VERIFY_BIN; do
        path="${!name}"
        [[ "$path" == /* && -f "$path" && -x "$path" ]] || die "$name must be an absolute executable file"
        printf -v "$name" '%s' "$(realpath -e -- "$path")"
    done
fi
CAPTURE="$(realpath -e -- "$CAPTURE")"
CONFIG_TEMPLATE="$(realpath -e -- "$CONFIG_TEMPLATE")"
QUERY_MANIFEST="$(realpath -e -- "$QUERY_MANIFEST")"
DEFAULT_QUERY_MANIFEST="$(realpath -e -- "$DEFAULT_QUERY_MANIFEST")"
[[ "$QUERY_MANIFEST" == "$DEFAULT_QUERY_MANIFEST" ]] \
    || die "QUERY_MANIFEST must be the committed Phase 6 manifest"
EXPECTATIONS="$(realpath -e -- "$EXPECTATIONS")"
REPO_ROOT="$(realpath -e -- "$REPO_ROOT")"
[[ "$(git -C "$REPO_ROOT" rev-parse --show-toplevel)" == "$REPO_ROOT" ]] || die "REPO_ROOT is not a Git worktree root"

[[ "$RESULT_DIR" == /* ]] || die "RESULT_DIR must be absolute"
result_name="$(basename "$RESULT_DIR")"
[[ -n "$result_name" && "$result_name" != "." && "$result_name" != ".." ]] || die "RESULT_DIR must name a fresh child"
result_parent_input="$(dirname "$RESULT_DIR")"
[[ -d "$result_parent_input" ]] || die "RESULT_DIR parent does not exist"
RESULT_DIR="$(realpath -e -- "$result_parent_input")/$result_name"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists; output is never reused"
case "$RESULT_DIR/" in
    "$REPO_ROOT/"*|"$CAPTURE/"*) die "RESULT_DIR must be external to source and capture" ;;
esac

for checked_path in "$CAPTURE" "$CONFIG_TEMPLATE" "$EXPECTATIONS" "$REPO_ROOT" \
        "$RESULT_DIR"; do
    [[ "$checked_path" != *$'\t'* && "$checked_path" != *$'\n'* ]] \
        || die "input and output paths must not contain tabs or newlines"
done

note "validating and hashing the pinned capture and configuration before allocating output"
validated_inputs_json="$(python3 "$SCRIPT_DIR/phase1_replay_gate.py" validate-inputs \
    --capture "$CAPTURE" \
    --template "$CONFIG_TEMPLATE" \
    --expectations "$EXPECTATIONS")"
SOURCE_HEAD="$(git -C "$REPO_ROOT" rev-parse HEAD)"
capacity_contract_json="$(python3 "$GATE_TOOL" capacity-contract \
    --expectations "$EXPECTATIONS" \
    --repo "$REPO_ROOT" \
    --source-head "$SOURCE_HEAD" \
    --replay-blocks "$REPLAY_BLOCKS")"
CAPACITY_INITIAL_REQUIRED_BYTES="$(python3 -c \
    'import json,sys; print(json.load(sys.stdin)["initial_required_free_bytes"])' \
    <<<"$capacity_contract_json")"
CAPACITY_POSTBUILD_REQUIRED_BYTES="$(python3 -c \
    'import json,sys; print(json.load(sys.stdin)["postbuild_required_free_bytes"])' \
    <<<"$capacity_contract_json")"
CAPACITY_OPERATIONAL_FLOOR_BYTES="$(python3 -c \
    'import json,sys; print(json.load(sys.stdin)["operational_floor_bytes"])' \
    <<<"$capacity_contract_json")"
capacity_prebuild_json="$(python3 "$GATE_TOOL" capacity-snapshot \
    --filesystem "$result_parent_input" \
    --minimum-free-bytes "$CAPACITY_INITIAL_REQUIRED_BYTES" \
    --phase prebuild)"

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/configs" "$RESULT_DIR/metadata" "$RESULT_DIR/replays" \
    "$RESULT_DIR/validation" "$RESULT_DIR/query-runs" "$RESULT_DIR/inventory" \
    "$RESULT_DIR/comparisons"
CONFIG_DIR="$RESULT_DIR/configs"
METADATA_DIR="$RESULT_DIR/metadata"
REPLAY_DIR="$RESULT_DIR/replays"
VALIDATION_DIR="$RESULT_DIR/validation"
QUERY_RUN_DIR="$RESULT_DIR/query-runs"
INVENTORY_DIR="$RESULT_DIR/inventory"
COMPARISON_DIR="$RESULT_DIR/comparisons"
NORMALIZED_TSV="$RESULT_DIR/queries.tsv"
NORMALIZED_JSON="$RESULT_DIR/queries.normalized.json"
ADMISSION_PLAN="$METADATA_DIR/admission-plan.json"
HARNESS_DIR="$METADATA_DIR/harness"
BINARY_DIR="$METADATA_DIR/binaries"
SOURCE_DIR="$METADATA_DIR/source"
mkdir "$HARNESS_DIR" "$BINARY_DIR" "$SOURCE_DIR"

HARNESS_FILES=(
    phase6_codec_ab_run.sh phase6_codec_ab_gate.py phase6_codec_queries.json
    test_phase6_codec_ab_gate.py phase1_4m_expectations.json
    phase1_replay_gate.py ab_gate.py schema7_query_ab_gate.py
    schema8_query_ab_gate.py phase1_query_gate.py phase2_compact_ids_ab_gate.py
    phase3_payload_coalescing_gate.py fadvise_regular_dontneed.c
    2026-07-22-phase6-codec-results.md
)
for harness in "${HARNESS_FILES[@]}"; do
    [[ -f "$SCRIPT_DIR/$harness" ]] || die "harness provenance file is missing: $harness"
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness" "$HARNESS_DIR/$harness"
done
chmod -R a-w -- "$HARNESS_DIR"
FROZEN_GATE="$HARNESS_DIR/phase6_codec_ab_gate.py"
FROZEN_MANIFEST="$HARNESS_DIR/phase6_codec_queries.json"
FROZEN_EXPECTATIONS="$HARNESS_DIR/phase1_4m_expectations.json"
FROZEN_FADVISE_SOURCE="$HARNESS_DIR/fadvise_regular_dontneed.c"
FADVISE_BIN="$METADATA_DIR/fadvise-regular-dontneed"
sha256sum "${HARNESS_FILES[@]/#/$HARNESS_DIR/}" >"$METADATA_DIR/harness.sha256"
chmod 0444 -- "$METADATA_DIR/harness.sha256"
HARNESS_MANIFEST_SHA256="$(sha256sum "$METADATA_DIR/harness.sha256" | awk '{print $1}')"
printf '%s\n' "$validated_inputs_json" >"$METADATA_DIR/validated-inputs.json"
printf '%s\n' "$capacity_contract_json" >"$METADATA_DIR/capacity-contract.json"
printf '%s\n' "$capacity_prebuild_json" >"$METADATA_DIR/capacity-prebuild.json"
chmod 0444 -- "$METADATA_DIR/validated-inputs.json" \
    "$METADATA_DIR/capacity-contract.json" "$METADATA_DIR/capacity-prebuild.json"
[[ "$(python3 "$FROZEN_GATE" capacity-contract \
    --expectations "$FROZEN_EXPECTATIONS" \
    --repo "$REPO_ROOT" \
    --source-head "$SOURCE_HEAD" \
    --replay-blocks "$REPLAY_BLOCKS")" == "$capacity_contract_json" ]] \
    || die "frozen capacity contract differs from the pre-allocation proof"

assert_harness_seal() {
    local harness mode
    [[ "$(stat -c '%a' -- "$HARNESS_DIR")" == "555" ]] \
        || die "frozen harness directory mode changed"
    [[ "$(stat -c '%a' -- "$METADATA_DIR/harness.sha256")" == "444" ]] \
        || die "harness checksum authority mode changed"
    [[ "$(sha256sum "$METADATA_DIR/harness.sha256" | awk '{print $1}')" == "$HARNESS_MANIFEST_SHA256" ]] \
        || die "harness checksum authority changed"
    sha256sum --check --strict "$METADATA_DIR/harness.sha256" >/dev/null \
        || die "frozen harness changed"
    for harness in "${HARNESS_FILES[@]}"; do
        [[ -f "$HARNESS_DIR/$harness" && ! -L "$HARNESS_DIR/$harness" ]] \
            || die "frozen harness file changed type: $harness"
        mode="$(stat -c '%a' -- "$HARNESS_DIR/$harness")"
        [[ "$mode" == "444" || "$mode" == "555" ]] \
            || die "frozen harness file is writable or has an unexpected mode: $harness"
    done
}

assert_harness_seal

preserve_binary() {
    local role="$1"
    local source="$2"
    local destination="$BINARY_DIR/$role"
    cp --reflink=auto --preserve=mode,timestamps -- "$source" "$destination"
    cmp -s -- "$source" "$destination" || die "preserved $role differs from source"
    chmod 0555 -- "$destination"
    [[ -x "$destination" ]] || die "preserved $role is not executable"
    printf '%s\t%s\t%s\t%s\n' "$role" "$source" "$destination" "$(sha256sum "$destination" | awk '{print $1}')" \
        >>"$METADATA_DIR/binaries.tsv"
}

printf 'role\tsource\tpreserved\tsha256\n' >"$METADATA_DIR/binaries.tsv"
PROMOTION_ELIGIBILITY="exploratory_non_promotable_external_binaries"
SOURCE_SEAL="$SOURCE_DIR/formal-source-seal.json"
SOURCE_SNAPSHOT_SEAL="$SOURCE_DIR/source-snapshot-seal.json"
SOURCE_ARCHIVE="$SOURCE_DIR/source-head.tar"
SOURCE_ARCHIVE_SHA256="$SOURCE_DIR/source-head.tar.sha256"
BUILD_SOURCE_DIR="$RESULT_DIR/build-source"
SOURCE_SEAL_SHA256=""
SOURCE_SNAPSHOT_SEAL_SHA256=""
SOURCE_ARCHIVE_AUTHORITY_SHA256=""
SEALED_HEAD=""
if [[ "$BINARY_PROVENANCE_MODE" == "internal" ]]; then
    PROMOTION_ELIGIBILITY="formal_source_bound"
    assert_harness_seal
    python3 "$FROZEN_GATE" source-seal --repo "$REPO_ROOT" --output "$SOURCE_SEAL"
    chmod 0444 -- "$SOURCE_SEAL"
    SOURCE_SEAL_SHA256="$(sha256sum "$SOURCE_SEAL" | awk '{print $1}')"
    SEALED_HEAD="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["head"])' "$SOURCE_SEAL")"
    [[ "$SEALED_HEAD" =~ ^[0-9a-f]{40,64}$ ]] || die "formal source seal has an invalid HEAD"
    git -C "$REPO_ROOT" archive --format=tar --output="$SOURCE_ARCHIVE" "$SEALED_HEAD"
    chmod 0444 -- "$SOURCE_ARCHIVE"
    sha256sum "$SOURCE_ARCHIVE" >"$SOURCE_ARCHIVE_SHA256"
    chmod 0444 -- "$SOURCE_ARCHIVE_SHA256"
    SOURCE_ARCHIVE_AUTHORITY_SHA256="$(sha256sum "$SOURCE_ARCHIVE_SHA256" | awk '{print $1}')"
    mkdir "$BUILD_SOURCE_DIR"
    tar -xf "$SOURCE_ARCHIVE" -C "$BUILD_SOURCE_DIR"
    chmod -R a-w -- "$BUILD_SOURCE_DIR"
    python3 "$FROZEN_GATE" source-snapshot-seal \
        --repo "$REPO_ROOT" \
        --snapshot "$BUILD_SOURCE_DIR" \
        --source-seal "$SOURCE_SEAL" \
        --output "$SOURCE_SNAPSHOT_SEAL"
    chmod 0444 -- "$SOURCE_SNAPSHOT_SEAL"
    SOURCE_SNAPSHOT_SEAL_SHA256="$(sha256sum "$SOURCE_SNAPSHOT_SEAL" | awk '{print $1}')"
    for harness in "${HARNESS_FILES[@]}"; do
        snapshot_harness="$BUILD_SOURCE_DIR/docs/experiments/storage_vnext/$harness"
        [[ -f "$snapshot_harness" && ! -L "$snapshot_harness" ]] \
            || die "formal source snapshot is missing harness file: $harness"
        cmp -s -- "$HARNESS_DIR/$harness" "$snapshot_harness" \
            || die "frozen harness differs from sealed HEAD: $harness"
    done
    BUILD_DIR="$METADATA_DIR/build"
    BUILD_HOME="$BUILD_DIR/home"
    BUILD_CARGO_HOME="$BUILD_DIR/cargo-home"
    BUILD_TARGET_DIR="$RESULT_DIR/build-target"
    mkdir "$BUILD_DIR" "$BUILD_HOME" "$BUILD_CARGO_HOME" "$BUILD_TARGET_DIR"
    python3 "$FROZEN_GATE" check-cargo-config-isolation \
        --snapshot "$BUILD_SOURCE_DIR" \
        --cargo-home "$BUILD_CARGO_HOME" \
        >"$BUILD_DIR/cargo-config-isolation-before-metadata.json"
    CARGO_BIN="$(type -P cargo)"
    RUSTC_BIN="$(type -P rustc)"
    [[ "$CARGO_BIN" == /* && -x "$CARGO_BIN" && "$RUSTC_BIN" == /* && -x "$RUSTC_BIN" ]] \
        || die "cargo and rustc must resolve to absolute executable paths"
    RUSTUP_HOME_EFFECTIVE="${RUSTUP_HOME:-$HOME/.rustup}"
    [[ -d "$RUSTUP_HOME_EFFECTIVE" ]] || die "RUSTUP_HOME is unavailable: $RUSTUP_HOME_EFFECTIVE"
    BUILD_TARGET_TRIPLE="$(env -i \
        PATH="$PATH" HOME="$BUILD_HOME" RUSTUP_HOME="$RUSTUP_HOME_EFFECTIVE" \
        CARGO_HOME="$BUILD_CARGO_HOME" LC_ALL=C TZ=UTC \
        "$RUSTC_BIN" --version --verbose | awk -F': ' '$1 == "host" {print $2}')"
    [[ "$BUILD_TARGET_TRIPLE" =~ ^[A-Za-z0-9_.-]+$ ]] \
        || die "could not determine the formal build target triple"
    SOURCE_DATE_EPOCH="$(git -C "$REPO_ROOT" show -s --format=%ct "$SEALED_HEAD")"
    BUILD_ENV=(
        "PATH=$PATH"
        "HOME=$BUILD_HOME"
        "RUSTUP_HOME=$RUSTUP_HOME_EFFECTIVE"
        "CARGO_HOME=$BUILD_CARGO_HOME"
        "CARGO_TARGET_DIR=$BUILD_TARGET_DIR"
        "CARGO_INCREMENTAL=0"
        "CARGO_TERM_COLOR=never"
        "LC_ALL=C"
        "TZ=UTC"
        "SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH"
    )
    BUILD_COMMAND=(
        "$CARGO_BIN" build --locked --release --no-default-features
        --target "$BUILD_TARGET_TRIPLE"
        -p chronoxide-ingester
        --bin chronoxide-ingester
        --bin chronoxide-query
        --bin chronoxide-storage-verify
    )
    {
        printf 'name\tvalue\n'
        for assignment in "${BUILD_ENV[@]}"; do
            printf '%s\t%s\n' "${assignment%%=*}" "${assignment#*=}"
        done
        printf 'RUSTFLAGS\t<unset>\n'
        printf 'RUSTDOCFLAGS\t<unset>\n'
        printf 'features\tno-default-features\n'
        printf 'profile\trelease\n'
        printf 'target\t%s\n' "$BUILD_TARGET_TRIPLE"
    } >"$BUILD_DIR/build-environment.tsv"
    printf '%q ' "${BUILD_COMMAND[@]}" >"$BUILD_DIR/build-command.txt"
    printf '\n' >>"$BUILD_DIR/build-command.txt"
    env -i "${BUILD_ENV[@]}" "$RUSTC_BIN" --version --verbose >"$BUILD_DIR/rustc-version.txt"
    env -i "${BUILD_ENV[@]}" "$CARGO_BIN" --version --verbose >"$BUILD_DIR/cargo-version.txt"
    printf 'cargo\t%s\nrustc\t%s\n' "$CARGO_BIN" "$RUSTC_BIN" >"$BUILD_DIR/tool-paths.tsv"
    sha256sum "$RUSTC_BIN" "$CARGO_BIN" >"$BUILD_DIR/tool-binaries.sha256"
    printf '%s\n' \
        'unavailable: rustup is outside the pinned formal build tool contract' \
        >"$BUILD_DIR/rustup-active-toolchain.txt"
    (
        cd "$BUILD_SOURCE_DIR"
        env -i "${BUILD_ENV[@]}" "$CARGO_BIN" metadata --locked --no-deps \
            --format-version 1
    ) >"$BUILD_DIR/cargo-metadata.json"
    python3 "$FROZEN_GATE" check-cargo-config-isolation \
        --snapshot "$BUILD_SOURCE_DIR" \
        --cargo-home "$BUILD_CARGO_HOME" \
        >"$BUILD_DIR/cargo-config-isolation-after-metadata.json"
    python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
        >"$BUILD_DIR/source-check-before-build.json"
    sha256sum --check --strict "$SOURCE_ARCHIVE_SHA256" \
        >"$BUILD_DIR/source-archive-check-before-build.txt"
    python3 "$FROZEN_GATE" check-source-snapshot-seal \
        --repo "$REPO_ROOT" \
        --snapshot "$BUILD_SOURCE_DIR" \
        --source-seal "$SOURCE_SEAL" \
        --seal "$SOURCE_SNAPSHOT_SEAL" \
        >"$BUILD_DIR/source-snapshot-check-before-build.json"
    note "performing one formal source-bound release build"
    set +e
    (
        cd "$BUILD_SOURCE_DIR"
        env -i "${BUILD_ENV[@]}" "${BUILD_COMMAND[@]}"
    ) >"$BUILD_DIR/build.log" 2>&1
    build_status=$?
    set -e
    printf '%s\n' "$build_status" >"$BUILD_DIR/build.exit-status"
    (( build_status == 0 )) || die "formal source-bound release build failed"
    python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
        >"$BUILD_DIR/source-check-after-build.json"
    sha256sum --check --strict "$SOURCE_ARCHIVE_SHA256" \
        >"$BUILD_DIR/source-archive-check-after-build.txt"
    python3 "$FROZEN_GATE" check-source-snapshot-seal \
        --repo "$REPO_ROOT" \
        --snapshot "$BUILD_SOURCE_DIR" \
        --source-seal "$SOURCE_SEAL" \
        --seal "$SOURCE_SNAPSHOT_SEAL" \
        >"$BUILD_DIR/source-snapshot-check-after-build.json"
    python3 "$FROZEN_GATE" check-cargo-config-isolation \
        --snapshot "$BUILD_SOURCE_DIR" \
        --cargo-home "$BUILD_CARGO_HOME" \
        >"$BUILD_DIR/cargo-config-isolation-after-build.json"
    BUILT_RELEASE_DIR="$BUILD_TARGET_DIR/$BUILD_TARGET_TRIPLE/release"
    preserve_binary chronoxide-ingester "$BUILT_RELEASE_DIR/chronoxide-ingester"
    preserve_binary chronoxide-query "$BUILT_RELEASE_DIR/chronoxide-query"
    preserve_binary chronoxide-storage-verify "$BUILT_RELEASE_DIR/chronoxide-storage-verify"
else
    preserve_binary chronoxide-ingester "$INGESTER_BIN"
    preserve_binary chronoxide-query "$QUERY_BIN"
    preserve_binary chronoxide-storage-verify "$STORAGE_VERIFY_BIN"
fi
RUN_INGESTER="$BINARY_DIR/chronoxide-ingester"
RUN_QUERY="$BINARY_DIR/chronoxide-query"
RUN_STORAGE_VERIFY="$BINARY_DIR/chronoxide-storage-verify"
sha256sum "$RUN_INGESTER" "$RUN_QUERY" "$RUN_STORAGE_VERIFY" \
    >"$METADATA_DIR/preserved-binaries.sha256"
chmod 0444 -- "$METADATA_DIR/preserved-binaries.sha256"
BINARY_MANIFEST_SHA256="$(sha256sum "$METADATA_DIR/preserved-binaries.sha256" | awk '{print $1}')"
python3 "$FROZEN_GATE" capacity-snapshot \
    --filesystem "$RESULT_DIR/.." \
    --minimum-free-bytes "$CAPACITY_POSTBUILD_REQUIRED_BYTES" \
    --phase postbuild \
    --output "$METADATA_DIR/capacity-postbuild.json"

assert_binary_seal() {
    local binary
    [[ "$(stat -c '%a' -- "$METADATA_DIR/preserved-binaries.sha256")" == "444" ]] \
        || die "preserved binary checksum authority mode changed"
    [[ "$(sha256sum "$METADATA_DIR/preserved-binaries.sha256" | awk '{print $1}')" == "$BINARY_MANIFEST_SHA256" ]] \
        || die "preserved binary checksum authority changed"
    sha256sum --check --strict "$METADATA_DIR/preserved-binaries.sha256" >/dev/null \
        || die "preserved binary seal changed"
    for binary in "$RUN_INGESTER" "$RUN_QUERY" "$RUN_STORAGE_VERIFY"; do
        [[ -f "$binary" && ! -L "$binary" && "$(stat -c '%a' -- "$binary")" == "555" ]] \
            || die "preserved binary type or mode changed: $binary"
    done
}

assert_harness_snapshot_binding() {
    local harness snapshot_harness
    [[ "$BINARY_PROVENANCE_MODE" == "internal" ]] || return 0
    for harness in "${HARNESS_FILES[@]}"; do
        snapshot_harness="$BUILD_SOURCE_DIR/docs/experiments/storage_vnext/$harness"
        [[ -f "$snapshot_harness" && ! -L "$snapshot_harness" ]] \
            || die "formal source snapshot is missing harness file: $harness"
        cmp -s -- "$HARNESS_DIR/$harness" "$snapshot_harness" \
            || die "frozen harness differs from sealed HEAD: $harness"
    done
}

assert_source_seal() {
    if [[ "$BINARY_PROVENANCE_MODE" == "internal" ]]; then
        [[ "$(stat -c '%a' -- "$SOURCE_SEAL")" == "444" ]] \
            || die "formal source seal mode changed"
        [[ "$(sha256sum "$SOURCE_SEAL" | awk '{print $1}')" == "$SOURCE_SEAL_SHA256" ]] \
            || die "formal source seal authority changed"
        [[ "$(stat -c '%a' -- "$SOURCE_SNAPSHOT_SEAL")" == "444" ]] \
            || die "formal source snapshot seal mode changed"
        [[ "$(sha256sum "$SOURCE_SNAPSHOT_SEAL" | awk '{print $1}')" == "$SOURCE_SNAPSHOT_SEAL_SHA256" ]] \
            || die "formal source snapshot seal authority changed"
        [[ "$(sha256sum "$SOURCE_ARCHIVE_SHA256" | awk '{print $1}')" == "$SOURCE_ARCHIVE_AUTHORITY_SHA256" ]] \
            || die "formal source archive checksum authority changed"
        [[ "$(stat -c '%a' -- "$SOURCE_ARCHIVE_SHA256")" == "444" ]] \
            || die "formal source archive checksum authority mode changed"
        python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
            >/dev/null || die "formal source seal changed"
        [[ "$(stat -c '%a' -- "$SOURCE_ARCHIVE")" == "444" ]] \
            || die "formal source archive mode changed"
        sha256sum --check --strict "$SOURCE_ARCHIVE_SHA256" >/dev/null \
            || die "formal source archive changed"
        python3 "$FROZEN_GATE" check-source-snapshot-seal \
            --repo "$REPO_ROOT" \
            --snapshot "$BUILD_SOURCE_DIR" \
            --source-seal "$SOURCE_SEAL" \
            --seal "$SOURCE_SNAPSHOT_SEAL" \
            >/dev/null || die "formal read-only source snapshot changed"
        python3 "$FROZEN_GATE" check-cargo-config-isolation \
            --snapshot "$BUILD_SOURCE_DIR" \
            --cargo-home "$BUILD_CARGO_HOME" \
            >/dev/null || die "formal Cargo configuration isolation changed"
        assert_harness_snapshot_binding
    fi
}

CONTROL_INPUTS_READY=0
CONTROL_INPUTS_MANIFEST_SHA256=""
CONTROLLED_INPUT_FILES=()

assert_control_inputs_seal() {
    local input expected_mode
    [[ "$CONTROL_INPUTS_READY" == "1" ]] || return 0
    [[ "$(stat -c '%a' -- "$METADATA_DIR/controlled-inputs.sha256")" == "444" ]] \
        || die "controlled input checksum authority mode changed"
    [[ "$(sha256sum "$METADATA_DIR/controlled-inputs.sha256" | awk '{print $1}')" == "$CONTROL_INPUTS_MANIFEST_SHA256" ]] \
        || die "controlled input checksum authority changed"
    sha256sum --check --strict "$METADATA_DIR/controlled-inputs.sha256" >/dev/null \
        || die "controlled experiment input changed"
    for input in "${CONTROLLED_INPUT_FILES[@]}"; do
        expected_mode=444
        [[ "$input" != "$FADVISE_BIN" ]] || expected_mode=555
        [[ -f "$input" && ! -L "$input" && "$(stat -c '%a' -- "$input")" == "$expected_mode" ]] \
            || die "controlled experiment input type or mode changed: $input"
    done
}

assert_perf_identity() {
    local observed_version
    if [[ "$PERF_STAT_MODE" == "off" ]]; then
        [[ "$PERF_BIN" == "-" && "$PERF_BINARY_SHA256" == "-" \
            && "$PERF_VERSION" == "-" ]] \
            || die "disabled perf tool identity changed"
        return 0
    fi
    [[ -f "$PERF_BIN" && ! -L "$PERF_BIN" && -x "$PERF_BIN" ]] \
        || die "perf binary type changed"
    [[ "$(sha256sum "$PERF_BIN" | awk '{print $1}')" == "$PERF_BINARY_SHA256" ]] \
        || die "perf binary digest changed"
    observed_version="$(env -i LC_ALL=C TZ=UTC "$PERF_BIN" --version)" \
        || die "perf binary version probe failed"
    [[ "$observed_version" == "$PERF_VERSION" ]] \
        || die "perf binary version changed"
}

assert_experiment_seals() {
    local context="$1"
    assert_harness_seal
    assert_source_seal
    assert_harness_seal
    assert_binary_seal
    assert_control_inputs_seal
    assert_perf_identity
    printf '%s\t%s\t%s\n' "$(date --iso-8601=ns)" "$context" "$PROMOTION_ELIGIBILITY" \
        >>"$METADATA_DIR/seal-checks.tsv"
}

record_ingester_runtime_identity() {
    local output="$1" config="$2"
    python3 "$FROZEN_GATE" runtime-identity \
        --binary "$RUN_INGESTER" --role ingester \
        --env "LC_ALL=C" --env "TZ=UTC" \
        --env "CONFIG_FILE=$config" --env "RUST_LOG=$RUST_LOG_VALUE" \
        --normalize-env CONFIG_FILE --output "$output"
}

record_query_runtime_identity() {
    local output="$1"
    python3 "$FROZEN_GATE" runtime-identity \
        --binary "$RUN_QUERY" --role query \
        --env "LC_ALL=C" --env "TZ=UTC" --output "$output"
}

record_verifier_runtime_identity() {
    local output="$1"
    python3 "$FROZEN_GATE" runtime-identity \
        --binary "$RUN_STORAGE_VERIFY" --role verifier \
        --env "LC_ALL=C" --env "TZ=UTC" --output "$output"
}

printf 'recorded_at\tcontext\tpromotion_eligibility\n' >"$METADATA_DIR/seal-checks.tsv"
assert_experiment_seals initial-preserved-binaries

query_help="$(env -i LC_ALL=C TZ=UTC "$RUN_QUERY" --help 2>&1)"
assert_experiment_seals after-query-help
for flag in --segments-dir --storage-layout --raw-output --benchmark-repeats --chunk-read-mode --chunk-payload-coalesce-max-gap-bytes --query-label-storage --query-label-arena-max-bytes --range-scalar-cache-max-bytes --verify-readbacks --validate-segment-footers; do
    grep -Fq -- "$flag" <<<"$query_help" || die "query binary help is missing $flag"
done
assert_experiment_seals before-verifier-help
verify_help="$(env -i LC_ALL=C TZ=UTC "$RUN_STORAGE_VERIFY" --help 2>&1)"
assert_experiment_seals after-verifier-help
for flag in --segments-dir --schema --validate-segment-footers --verify-exact-postings --sample-series-per-segment; do
    grep -Fq -- "$flag" <<<"$verify_help" || die "storage verifier help is missing $flag"
done

cp --preserve=mode,timestamps -- "$CONFIG_TEMPLATE" "$METADATA_DIR/config-template.toml"
chmod 0444 -- "$METADATA_DIR/config-template.toml"
sha256sum "$METADATA_DIR/config-template.toml" >"$METADATA_DIR/config-template.sha256"
chmod 0444 -- "$METADATA_DIR/config-template.sha256"
printf '%s\n' "$RUN_NOTE" >"$METADATA_DIR/run-note.txt"
git -C "$REPO_ROOT" rev-parse HEAD >"$SOURCE_DIR/git-commit.txt"
git -C "$REPO_ROOT" rev-parse 'HEAD^{tree}' >"$SOURCE_DIR/git-tree.txt"
git -C "$REPO_ROOT" ls-files -s >"$SOURCE_DIR/tracked-index.txt"
git -C "$REPO_ROOT" status --porcelain=v2 --branch >"$SOURCE_DIR/git-status.txt"
git -C "$REPO_ROOT" diff --binary --full-index HEAD -- >"$SOURCE_DIR/tracked-source.patch"
git -C "$REPO_ROOT" ls-files --others --exclude-standard -z >"$SOURCE_DIR/untracked-files.nul"
while IFS= read -r -d '' relative; do
    path="$REPO_ROOT/$relative"
    [[ -f "$path" && ! -L "$path" ]] || continue
    printf '%s\t%s\t%s\n' "$(sha256sum "$path" | awk '{print $1}')" "$(stat -c %s "$path")" "$relative"
done <"$SOURCE_DIR/untracked-files.nul" >"$SOURCE_DIR/untracked-files.tsv"

{
    date --iso-8601=seconds
    uname -a
    rustc --version --verbose 2>/dev/null || true
    cargo --version --verbose 2>/dev/null || true
    if [[ "$PERF_STAT_MODE" == "off" ]]; then
        printf 'perf disabled\n'
    else
        env -i LC_ALL=C TZ=UTC "$PERF_BIN" --version
    fi
    lscpu 2>/dev/null || true
    findmnt -T "$RESULT_DIR" 2>/dev/null || true
    df -B1 "$RESULT_DIR"
    ulimit -a
    cat /proc/meminfo 2>/dev/null || true
    for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
        [[ -r "$pressure" ]] && { printf '%s\n' "$pressure"; cat "$pressure"; }
    done
    ps -eo pid=,ppid=,pcpu=,pmem=,rss=,etime=,stat=,comm=,args=
} >"$METADATA_DIR/environment.txt" 2>&1

assert_experiment_seals before-static-input-transforms
python3 "$FROZEN_GATE" capture-inventory \
    --capture "$CAPTURE" \
    --output "$INVENTORY_DIR/capture.json" \
    --paths-output "$INVENTORY_DIR/capture-files.nul"
python3 "$FROZEN_GATE" normalize-query-manifest \
    --input "$FROZEN_MANIFEST" \
    --output-tsv "$NORMALIZED_TSV" \
    --output-json "$NORMALIZED_JSON" \
    --default-range-cache-bytes 0
CAPACITY_CONTRACT_SHA256="$(sha256sum "$METADATA_DIR/capacity-contract.json" | awk '{print $1}')"
python3 "$FROZEN_GATE" admission-plan \
    --output "$ADMISSION_PLAN" \
    --result-dir "$RESULT_DIR" \
    --capture "$CAPTURE" \
    --repo "$REPO_ROOT" \
    --query-manifest "$FROZEN_MANIFEST" \
    --config-template "$METADATA_DIR/config-template.toml" \
    --validated-input-config-template "$CONFIG_TEMPLATE" \
    --expectations "$FROZEN_EXPECTATIONS" \
    --binary-provenance-mode "$BINARY_PROVENANCE_MODE" \
    --promotion-eligibility "$PROMOTION_ELIGIBILITY" \
    --stop-after-messages "$STOP_AFTER_MESSAGES" \
    --replay-blocks "$REPLAY_BLOCKS" \
    --query-blocks "$QUERY_BLOCKS" \
    --benchmark-repeats "$BENCHMARK_REPEATS" \
    --rss-interval-ms "$RSS_INTERVAL_MS" \
    --guard-interval-ms "$GUARD_INTERVAL_MS" \
    --capacity-monitor-interval-ms "$CAPACITY_MONITOR_INTERVAL_MS" \
    --page-size-bytes "$PAGE_SIZE_BYTES" \
    --max-capture-resident-bytes-after-evict "$MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT" \
    --max-corpus-resident-bytes-after-evict "$MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT" \
    --max-dirty-writeback-bytes "$MAX_DIRTY_WRITEBACK_BYTES" \
    --capacity-contract-sha256 "$CAPACITY_CONTRACT_SHA256" \
    --readback-sample-limit-per-kind "$READBACK_SAMPLE_LIMIT_PER_KIND" \
    --rust-log "$RUST_LOG_VALUE" \
    --perf-stat-mode "$PERF_STAT_MODE" \
    --perf-binary "$PERF_BIN" \
    --perf-binary-sha256 "$PERF_BINARY_SHA256" \
    --perf-version "$PERF_VERSION" \
    --chunk-read-queue-depth "$CHUNK_READ_QUEUE_DEPTH" \
    --query-label-arena-max-bytes "$QUERY_LABEL_ARENA_MAX_BYTES" \
    --query-max-series-matched "$QUERY_MAX_SERIES_MATCHED" \
    --query-max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
    --query-max-chunks-read "$QUERY_MAX_CHUNKS_READ" \
    --query-max-bytes-read "$QUERY_MAX_BYTES_READ" \
    --query-max-samples "$QUERY_MAX_SAMPLES" \
    --regex-max-expanded-values "$REGEX_MAX_EXPANDED_VALUES"
cc -O2 -Wall -Wextra -Werror -o "$FADVISE_BIN" "$FROZEN_FADVISE_SOURCE"
chmod 0555 -- "$FADVISE_BIN"
sha256sum "$FADVISE_BIN" >"$METADATA_DIR/fadvise.sha256"
chmod 0444 -- "$METADATA_DIR/fadvise.sha256"
assert_experiment_seals after-static-input-transforms

codec_order() {
    if (( $1 % 2 == 1 )); then
        printf 'raw gorilla gorilla raw\n'
    else
        printf 'gorilla raw raw gorilla\n'
    fi
}

read -r CAPACITY_RAW_BOUND_BYTES CAPACITY_GORILLA_BOUND_BYTES \
    CAPACITY_RAW_SAFE_BYTES CAPACITY_GORILLA_SAFE_BYTES < <(
    python3 -c '
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
derivation = value["derivation"]
print(derivation["corpus_bound_bytes"]["raw"],
      derivation["corpus_bound_bytes"]["gorilla"],
      derivation["safe_corpus_reserve_bytes"]["raw"],
      derivation["safe_corpus_reserve_bytes"]["gorilla"])
' "$METADATA_DIR/capacity-contract.json"
)
remaining_raw=$((REPLAY_BLOCKS * 2))
remaining_gorilla=$((REPLAY_BLOCKS * 2))
replay_ordinal=0
printf 'label\tordinal\tblock\tslot\tcodec\tconfig\tsegments_dir\tcodec_bound_bytes\tcodec_safe_reserve_bytes\tfuture_safe_reserve_bytes\tpre_required_free_bytes\tmonitor_minimum_free_bytes\n' \
    >"$RESULT_DIR/replay-plan.tsv"
for ((block = 1; block <= REPLAY_BLOCKS; block++)); do
    read -r -a codecs <<<"$(codec_order "$block")"
    for ((slot = 1; slot <= 4; slot++)); do
        codec="${codecs[$((slot - 1))]}"
        label="$(printf 'replay-b%02d-s%02d-%s' "$block" "$slot" "$codec")"
        run_dir="$REPLAY_DIR/$label"
        segments_dir="$run_dir/segments"
        mkdir "$run_dir"
        config="$CONFIG_DIR/$label.toml"
        replay_ordinal=$((replay_ordinal + 1))
        if [[ "$codec" == "raw" ]]; then
            codec_bound_bytes="$CAPACITY_RAW_BOUND_BYTES"
            codec_safe_reserve_bytes="$CAPACITY_RAW_SAFE_BYTES"
            remaining_raw=$((remaining_raw - 1))
        else
            codec_bound_bytes="$CAPACITY_GORILLA_BOUND_BYTES"
            codec_safe_reserve_bytes="$CAPACITY_GORILLA_SAFE_BYTES"
            remaining_gorilla=$((remaining_gorilla - 1))
        fi
        future_safe_reserve_bytes=$((
            remaining_raw * CAPACITY_RAW_SAFE_BYTES
            + remaining_gorilla * CAPACITY_GORILLA_SAFE_BYTES
        ))
        monitor_minimum_free_bytes=$((
            future_safe_reserve_bytes + CAPACITY_OPERATIONAL_FLOOR_BYTES
        ))
        pre_required_free_bytes=$((
            codec_safe_reserve_bytes + monitor_minimum_free_bytes
        ))
        python3 "$FROZEN_GATE" render-config \
            --template "$METADATA_DIR/config-template.toml" \
            --output "$config" \
            --capture "$CAPTURE" \
            --segments-dir "$segments_dir" \
            --stop-after-messages "$STOP_AFTER_MESSAGES" \
            --codec "$codec" >"$run_dir/config.json"
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$label" "$replay_ordinal" "$block" "$slot" "$codec" "$config" \
            "$segments_dir" "$codec_bound_bytes" "$codec_safe_reserve_bytes" \
            "$future_safe_reserve_bytes" "$pre_required_free_bytes" \
            "$monitor_minimum_free_bytes" \
            >>"$RESULT_DIR/replay-plan.tsv"
    done
done
(( remaining_raw == 0 && remaining_gorilla == 0 )) \
    || die "capacity replay schedule did not consume the pinned codec counts"
chmod 0444 -- "$CONFIG_DIR"/*.toml
sha256sum "$CONFIG_DIR"/*.toml >"$METADATA_DIR/rendered-configs.sha256"
chmod 0444 -- "$METADATA_DIR/rendered-configs.sha256"
chmod 0444 -- "$NORMALIZED_TSV" "$NORMALIZED_JSON" "$ADMISSION_PLAN" "$RESULT_DIR/replay-plan.tsv"
CONTROLLED_INPUT_FILES=(
    "$METADATA_DIR/capacity-contract.json"
    "$METADATA_DIR/config-template.toml"
    "$METADATA_DIR/config-template.sha256"
    "$METADATA_DIR/fadvise.sha256"
    "$METADATA_DIR/rendered-configs.sha256"
    "$METADATA_DIR/validated-inputs.json"
    "$FADVISE_BIN"
    "$NORMALIZED_TSV"
    "$NORMALIZED_JSON"
    "$ADMISSION_PLAN"
    "$RESULT_DIR/replay-plan.tsv"
    "$CONFIG_DIR"/*.toml
)
sha256sum "${CONTROLLED_INPUT_FILES[@]}" >"$METADATA_DIR/controlled-inputs.sha256"
chmod 0444 -- "$METADATA_DIR/controlled-inputs.sha256"
CONTROL_INPUTS_MANIFEST_SHA256="$(sha256sum "$METADATA_DIR/controlled-inputs.sha256" | awk '{print $1}')"
CONTROL_INPUTS_READY=1
assert_experiment_seals after-config-rendering

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'binary_provenance_mode=%s\n' "$BINARY_PROVENANCE_MODE"
    printf 'promotion_eligibility=%s\n' "$PROMOTION_ELIGIBILITY"
    printf 'stop_after_messages=%s\n' "$STOP_AFTER_MESSAGES"
    printf 'formal_build=--locked --release --no-default-features; one isolated target build from an exact read-only git archive HEAD snapshot when internal\n'
    printf 'quiet_host_confirmed=%s\n' "$QUIET_HOST_CONFIRMED"
    printf 'rss_interval_ms=%s\n' "$RSS_INTERVAL_MS"
    printf 'conflict_guard_interval_ms=%s\n' "$GUARD_INTERVAL_MS"
    printf 'conflict_precheck=same classifier; exact PID ancestry exclusions only\n'
    printf 'capacity_monitor_interval_ms=%s\n' "$CAPACITY_MONITOR_INTERVAL_MS"
    printf 'page_size_bytes=%s\n' "$PAGE_SIZE_BYTES"
    printf 'max_capture_resident_bytes_after_evict=%s\n' \
        "$MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT"
    printf 'max_corpus_resident_bytes_after_evict=%s\n' \
        "$MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT"
    printf 'max_dirty_writeback_bytes=%s\n' "$MAX_DIRTY_WRITEBACK_BYTES"
    printf 'replay_launch=held_until_root_starttime_bound_rss_and_capacity_first_samples\n'
    printf 'replay_monitor_ready_markers=distinct_immutable_atomic_mode_0444\n'
    printf 'replay_monitor_cadence=edge_inclusive_initial_sample_terminal_max_200ms\n'
    printf 'capacity_operational_floor_bytes=%s\n' "$CAPACITY_OPERATIONAL_FLOOR_BYTES"
    printf 'capacity_build_source_result_allowance_bytes=%s\n' \
        "$((CAPACITY_INITIAL_REQUIRED_BYTES - CAPACITY_POSTBUILD_REQUIRED_BYTES))"
    printf 'capacity_schedule_safe_reserve_bytes=%s\n' \
        "$((CAPACITY_POSTBUILD_REQUIRED_BYTES - CAPACITY_OPERATIONAL_FLOOR_BYTES))"
    printf 'same_binary_runtime_control=head_buffer.float_encoding plus matching segment_writer.float_encoding\n'
    printf 'replay_blocks=%s\n' "$REPLAY_BLOCKS"
    printf 'query_blocks=%s\n' "$QUERY_BLOCKS"
    printf 'schedule=odd raw,gorilla,gorilla,raw; even reversed\n'
    printf 'benchmark_repeats=%s (cold,warm,warm)\n' "$BENCHMARK_REPEATS"
    printf 'storage_layout=schema8\n'
    printf 'query_backend=pread\n'
    printf 'query_payload_gap_bytes=4096\n'
    printf 'query_label_materialization=demand-driven\n'
    printf 'query_label_storage=compact-ids\n'
    printf 'query_label_arena_max_bytes=%s\n' "$QUERY_LABEL_ARENA_MAX_BYTES"
    printf 'query_instrumentation=off\n'
    printf 'chunk_read_queue_depth=%s\n' "$CHUNK_READ_QUEUE_DEPTH"
    printf 'query_max_series_matched=%s\n' "$QUERY_MAX_SERIES_MATCHED"
    printf 'query_max_projected_series=%s\n' "$QUERY_MAX_PROJECTED_SERIES"
    printf 'query_max_chunks_read=%s\n' "$QUERY_MAX_CHUNKS_READ"
    printf 'query_max_bytes_read=%s\n' "$QUERY_MAX_BYTES_READ"
    printf 'query_max_samples=%s\n' "$QUERY_MAX_SAMPLES"
    printf 'regex_max_expanded_values=%s\n' "$REGEX_MAX_EXPANDED_VALUES"
    printf 'range_scalar_cache_max_bytes=manifest; Phase 6 entries use 0\n'
    printf 'perf_stat_mode=%s\n' "$PERF_STAT_MODE"
    printf 'perf_binary=%s\n' "$PERF_BIN"
    printf 'perf_binary_sha256=%s\n' "$PERF_BINARY_SHA256"
    printf 'perf_version=%s\n' "$PERF_VERSION"
    printf 'perf_events=task-clock,cycles,instructions,branches,branch-misses,cache-references,cache-misses,page-faults,context-switches,cpu-migrations\n'
    printf 'footer_validation=exhaustive verifier pass outside replay/query timing\n'
    printf 'readback_sample_limit_per_kind=%s\n' "$READBACK_SAMPLE_LIMIT_PER_KIND"
    printf 'readback_validation=separate untimed independent oracle, zero skips required\n'
    printf 'timestamp_runtime_ab=blocked: no versioned writer/reader selector; verifier candidate inventory only\n'
    printf 'timestamp_evidence_scope=native payload; typed scalar-lane timestamps excluded\n'
    printf 'rust_log=%s\n' "$RUST_LOG_VALUE"
    printf 'run_note=%s\n' "$RUN_NOTE"
} >"$METADATA_DIR/settings.txt"

if [[ "$DRY_RUN" == "1" ]]; then
    assert_experiment_seals dry-run-finalization
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    note "dry run complete; no replay, verifier, readback, or query process launched: $RESULT_DIR"
    exit 0
fi

RAW_AUTHORITY_ENTRIES=()
record_raw_authority() {
    local seal="$1" relative digest
    [[ "$seal" == "$RESULT_DIR/"* ]] || die "raw leaf seal escapes the result root: $seal"
    relative="${seal#"$RESULT_DIR/"}"
    [[ -n "$relative" && -f "$seal" && ! -L "$seal" ]] \
        || die "raw leaf seal is missing or invalid: $seal"
    digest="$(sha256sum "$seal" | awk '{print $1}')"
    RAW_AUTHORITY_ENTRIES+=("$relative=$digest")
}

PERF_EVENTS="task-clock,cycles,instructions,branches,branch-misses,cache-references,cache-misses,page-faults,context-switches,cpu-migrations"
PERF_EFFECTIVE=off
if [[ "$PERF_STAT_MODE" != "off" ]]; then
    set +e
    LC_ALL=C TZ=UTC "$PERF_BIN" stat --no-big-num --field-separator $'\t' \
        --event "$PERF_EVENTS" \
        --output "$METADATA_DIR/perf-preflight.tsv" -- \
        "$PYTHON_BIN" -I -S -B -c 'sum(range(10000000))' \
        >"$METADATA_DIR/perf-preflight.log" 2>&1
    perf_status=$?
    set -e
    printf '%s\n' "$perf_status" >"$METADATA_DIR/perf-preflight.exit-status"
    if (( perf_status == 0 )); then
        set +e
        python3 "$FROZEN_GATE" parse-perf \
            --input "$METADATA_DIR/perf-preflight.tsv" \
            --output "$METADATA_DIR/perf-preflight.json" \
            >"$METADATA_DIR/perf-preflight-parse.log" 2>&1
        perf_parse_status=$?
        set -e
        if (( perf_parse_status == 0 )); then
            PERF_EFFECTIVE=on
        elif [[ "$PERF_STAT_MODE" == "required" ]]; then
            die "perf preflight output did not contain the required counters"
        fi
    elif [[ "$PERF_STAT_MODE" == "required" ]]; then
        die "perf stat preflight failed"
    fi
fi
printf '%s\n' "$PERF_EFFECTIVE" >"$METADATA_DIR/perf-effective.txt"
assert_experiment_seals after-perf-preflight

GUARD_STOP="$METADATA_DIR/guardian.stop"
GUARD_OUTPUT="$METADATA_DIR/guardian-conflicts.tsv"
GUARD_READY="$METADATA_DIR/guardian.ready"
GUARD_PID=''
GUARD_PPID=''
GUARD_STARTTIME_TICKS=''
GUARD_BINDING=''
active_run_dir=''
active_control=''
active_rss_ready=''
active_capacity_ready=''
active_launch=''
active_root_pid=''
active_root_ppid=''
active_root_starttime_ticks=''
active_rss_pid=''
active_rss_ppid=''
active_rss_starttime_ticks=''
active_capacity_pid=''
active_capacity_ppid=''
active_capacity_starttime_ticks=''
REAP_STATUS=0

read_process_identity() {
    local pid="$1" stat_line stat_tail
    local -a fields
    IFS= read -r stat_line <"/proc/$pid/stat" || return 1
    stat_tail="${stat_line##*) }"
    read -r -a fields <<<"$stat_tail"
    (( ${#fields[@]} > 19 )) || return 1
    [[ "${fields[0]}" =~ ^[A-Za-z]$ && "${fields[1]}" =~ ^[0-9]+$ \
        && "${fields[19]}" =~ ^[1-9][0-9]*$ ]] || return 1
    printf '%s\t%s\t%s\n' "${fields[0]}" "${fields[1]}" "${fields[19]}"
}

bind_live_process() {
    local pid="$1" identity state ppid starttime
    identity="$(read_process_identity "$pid")" || return 1
    read -r state ppid starttime <<<"$identity" || return 1
    [[ "$state" != Z && "$state" != X && "$state" != x ]] || return 1
    printf '%s\t%s\n' "$ppid" "$starttime"
}

record_cleanup() {
    [[ -n "$active_run_dir" && -d "$active_run_dir" ]] || return 0
    printf '%s\t%s\t%s\n' "$1" "$2" "$3" \
        >>"$active_run_dir/interrupted-cleanup.tsv"
}

stop_bound_tree() {
    local role="$1" pid="$2" ppid="$3" starttime="$4"
    [[ -n "$pid" ]] || return 0
    if [[ -z "$ppid" || -z "$starttime" ]]; then
        record_cleanup "$role" unbound-signal-refused "pid=$pid"
        return 1
    fi
    python3 "$FROZEN_GATE" terminate-process-tree --root-pid "$pid" \
        --root-ppid "$ppid" --root-starttime-ticks "$starttime" \
        >"${active_run_dir:-$METADATA_DIR}/interrupted-$role-termination.json" \
        2>&1 || true
}

bounded_reap_job() {
    local role="$1" pid="$2" expected_ppid="$3" expected_starttime="$4"
    local attempts="${5:-200}" attempt identity state ppid starttime
    REAP_STATUS=0
    [[ -n "$pid" ]] || return 0
    if [[ -z "$expected_ppid" || -z "$expected_starttime" ]]; then
        record_cleanup "$role" unbound-wait-refused "pid=$pid"
        return 1
    fi
    for ((attempt = 0; attempt < attempts; attempt++)); do
        if ! identity="$(read_process_identity "$pid")"; then
            if wait "$pid" 2>/dev/null; then REAP_STATUS=0; else REAP_STATUS=$?; fi
            return 0
        fi
        read -r state ppid starttime <<<"$identity" || {
            record_cleanup "$role" malformed-identity "pid=$pid"
            return 1
        }
        if [[ "$ppid" != "$expected_ppid" || "$starttime" != "$expected_starttime" ]]; then
            record_cleanup "$role" identity-changed \
                "pid=$pid expected_ppid=$expected_ppid observed_ppid=$ppid expected_start=$expected_starttime observed_start=$starttime"
            return 1
        fi
        if [[ "$state" == Z || "$state" == X || "$state" == x ]]; then
            if wait "$pid" 2>/dev/null; then REAP_STATUS=0; else REAP_STATUS=$?; fi
            return 0
        fi
        sleep 0.01
    done
    record_cleanup "$role" bounded-wait-timeout "pid=$pid"
    return 1
}

clear_active_replay() {
    active_run_dir=''; active_control=''; active_rss_ready=''
    active_capacity_ready=''; active_launch=''; active_root_pid=''
    active_root_ppid=''; active_root_starttime_ticks=''; active_rss_pid=''
    active_rss_ppid=''; active_rss_starttime_ticks=''; active_capacity_pid=''
    active_capacity_ppid=''; active_capacity_starttime_ticks=''
}

cleanup_active_replay() {
    local controlled=0
    if [[ -n "$active_control" && -f "$active_control" && ! -L "$active_control" ]]; then
        if python3 "$FROZEN_GATE" cleanup-replay-processes \
            --control "$active_control" --rss-ready "$active_rss_ready" \
            --capacity-ready "$active_capacity_ready" --launch "$active_launch" \
            --interval-ms 100 \
            >"$active_run_dir/interrupted-controlled-cleanup.json" 2>&1; then
            controlled=1
        fi
    fi
    if [[ "$controlled" == 0 ]]; then
        stop_bound_tree root "$active_root_pid" "$active_root_ppid" \
            "$active_root_starttime_ticks" || true
        stop_bound_tree rss-monitor "$active_rss_pid" "$active_rss_ppid" \
            "$active_rss_starttime_ticks" || true
        stop_bound_tree capacity-monitor "$active_capacity_pid" \
            "$active_capacity_ppid" "$active_capacity_starttime_ticks" || true
    fi
    bounded_reap_job root "$active_root_pid" "$active_root_ppid" \
        "$active_root_starttime_ticks" 200 || true
    bounded_reap_job rss-monitor "$active_rss_pid" "$active_rss_ppid" \
        "$active_rss_starttime_ticks" 200 || true
    bounded_reap_job capacity-monitor "$active_capacity_pid" \
        "$active_capacity_ppid" "$active_capacity_starttime_ticks" 200 || true
    clear_active_replay
}

cleanup_guardian() {
    local status=0
    [[ -n "$GUARD_PID" ]] || return 0
    if [[ ! -e "$GUARD_STOP" && ! -L "$GUARD_STOP" ]]; then
        python3 "$FROZEN_GATE" create-empty-marker --output "$GUARD_STOP" \
            >/dev/null || status=1
    fi
    if ! bounded_reap_job guardian "$GUARD_PID" "$GUARD_PPID" \
        "$GUARD_STARTTIME_TICKS" 500; then
        stop_bound_tree guardian "$GUARD_PID" "$GUARD_PPID" \
            "$GUARD_STARTTIME_TICKS" || true
        bounded_reap_job guardian "$GUARD_PID" "$GUARD_PPID" \
            "$GUARD_STARTTIME_TICKS" 200 || status=1
    fi
    (( REAP_STATUS == 0 )) || status="$REAP_STATUS"
    GUARD_PID=''; GUARD_PPID=''; GUARD_STARTTIME_TICKS=''
    return "$status"
}

cleanup_all() {
    trap '' HUP INT TERM
    cleanup_active_replay || true
    cleanup_guardian || true
}

cleanup_signal_pending=0
cleanup_signal_exit() { cleanup_all; exit 130; }
defer_cleanup_signals() { trap 'cleanup_signal_pending=1' HUP INT TERM; }
arm_cleanup_signals() {
    trap 'cleanup_signal_exit' HUP INT TERM
    if [[ "$cleanup_signal_pending" == 1 ]]; then
        cleanup_signal_pending=0
        cleanup_signal_exit
    fi
}
trap 'cleanup_all' EXIT
arm_cleanup_signals

python3 "$FROZEN_GATE" check-current-conflicts \
    --parent-pid "$$" \
    --output "$METADATA_DIR/guardian-precheck.json"
python3_background "$FROZEN_GATE" guard-conflicts \
    --parent-pid "$$" \
    --stop-file "$GUARD_STOP" \
    --output "$GUARD_OUTPUT" \
    --interval-ms "$GUARD_INTERVAL_MS" \
    --filesystem "$RESULT_DIR/.." \
    --minimum-free-bytes "$CAPACITY_OPERATIONAL_FLOOR_BYTES" \
    --samples "$METADATA_DIR/guardian-samples.tsv" \
    --summary "$METADATA_DIR/guardian.json" \
    --ready-file "$GUARD_READY" \
    >"$METADATA_DIR/guardian.log" 2>&1 &
GUARD_PID=$!
GUARD_BINDING="$(bind_live_process "$GUARD_PID")" \
    || { cleanup_all; die "could not bind conflict guardian process identity"; }
read -r GUARD_PPID GUARD_STARTTIME_TICKS <<<"$GUARD_BINDING"
[[ "$GUARD_PPID" == "$$" ]] \
    || { cleanup_all; die "conflict guardian has an unexpected parent"; }

check_guardian() {
    local identity state ppid starttime
    if ! identity="$(read_process_identity "$GUARD_PID")"; then
        set +e
        wait "$GUARD_PID"
        status=$?
        set -e
        die "continuous conflict guardian stopped with status $status; measurement is invalid"
    fi
    read -r state ppid starttime <<<"$identity" \
        || die "continuous conflict guardian identity is malformed; measurement is invalid"
    [[ "$state" != Z && "$state" != X && "$state" != x \
        && "$ppid" == "$GUARD_PPID" \
        && "$starttime" == "$GUARD_STARTTIME_TICKS" ]] \
        || die "continuous conflict guardian identity changed or exited; measurement is invalid"
}

wait_for_guardian_ready() {
    local attempt
    for ((attempt = 1; attempt <= 50; attempt++)); do
        check_guardian
        if [[ -f "$GUARD_READY" && ! -L "$GUARD_READY" ]]; then
            [[ ! -s "$GUARD_READY" && "$(stat -c '%a' -- "$GUARD_READY")" == 444 ]] \
                || die "conflict guardian ready sentinel is malformed"
            return 0
        fi
        sleep 0.1
    done
    die "conflict guardian did not complete its first scan within five seconds"
}

wait_for_guardian_ready

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

wait_for_writeback() {
    local phase="$1"
    local output="$2"
    local attempt dirty_kib writeback_kib total_bytes status
    sync
    printf 'phase\tattempt\trecorded_at\tdirty_kib\twriteback_kib\ttotal_bytes\tceiling_bytes\tstatus\n' \
        >"$output"
    for ((attempt = 1; attempt <= 30; attempt++)); do
        dirty_kib="$(awk '
            $1 == "Dirty:" { count += 1; value = $2 }
            END {
                if (count != 1 || value !~ /^[0-9]+$/) exit 1
                print value
            }
        ' /proc/meminfo)" || die "could not parse one Dirty row from /proc/meminfo"
        writeback_kib="$(awk '
            $1 == "Writeback:" { count += 1; value = $2 }
            END {
                if (count != 1 || value !~ /^[0-9]+$/) exit 1
                print value
            }
        ' /proc/meminfo)" || die "could not parse one Writeback row from /proc/meminfo"
        total_bytes=$(( (dirty_kib + writeback_kib) * 1024 ))
        if (( total_bytes <= MAX_DIRTY_WRITEBACK_BYTES )); then
            status=pass
        else
            status=retry
        fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$phase" "$attempt" "$(date --iso-8601=ns)" "$dirty_kib" \
            "$writeback_kib" "$total_bytes" "$MAX_DIRTY_WRITEBACK_BYTES" \
            "$status" >>"$output"
        if [[ "$status" == "pass" ]]; then
            return 0
        fi
        sleep 1
    done
    die "dirty plus writeback bytes did not reach the configured ceiling"
}

evict_paths() {
    local paths_file="$1"
    local file
    while IFS= read -r -d '' file; do
        "$FADVISE_BIN" "$file"
    done <"$paths_file"
}

snapshot_residency() {
    local phase="$1"
    local paths_file="$2"
    local output="$3"
    local ceiling_bytes="$4"
    local file line resident size sequence=0 resident_total=0 size_total=0
    printf 'phase\tsequence\trow_kind\tresident_bytes\tsize_bytes\tceiling_bytes\tpath\n' \
        >"$output"
    while IFS= read -r -d '' file; do
        [[ -f "$file" && ! -L "$file" ]] || die "inventoried file changed type: $file"
        line="$(fincore --bytes --noheadings --output RES,SIZE -- "$file")"
        read -r resident size <<<"$line"
        [[ "$resident" =~ ^[0-9]+$ && "$size" =~ ^[0-9]+$ ]] || die "could not parse fincore output for $file"
        sequence=$((sequence + 1))
        resident_total=$((resident_total + resident))
        size_total=$((size_total + size))
        printf '%s\t%s\tfile\t%s\t%s\t%s\t%s\n' \
            "$phase" "$sequence" "$resident" "$size" "$ceiling_bytes" "$file" \
            >>"$output"
    done <"$paths_file"
    sequence=$((sequence + 1))
    printf '%s\t%s\ttotal\t%s\t%s\t%s\t-\n' \
        "$phase" "$sequence" "$resident_total" "$size_total" "$ceiling_bytes" \
        >>"$output"
    printf '%s\n' "$resident_total"
}

printf 'label\tblock\tslot\tcodec\tconfig_json\tcorrectness_json\tmanifest_tsv\tcorpus_summary_json\ttime_json\trss_json\tseal_json\tperf_json\n' \
    >"$RESULT_DIR/replay-index.tsv"

run_replay() {
    local label="$1" ordinal="$2" block="$3" slot="$4" codec="$5" config="$6"
    local segments_dir="$7" codec_bound_bytes="$8" codec_safe_reserve_bytes="$9"
    local future_safe_reserve_bytes="${10}" pre_required_free_bytes="${11}"
    local monitor_minimum_free_bytes="${12}"
    local run_dir="$REPLAY_DIR/$label"
    local launcher_pid monitor_pid capacity_monitor_pid status monitor_status
    local capacity_monitor_status report perf_json resident binding
    local control="$run_dir/replay-monitor-control.json"
    local rss_ready="$run_dir/rss-monitor.ready"
    local capacity_ready="$run_dir/capacity-monitor.ready"
    local launch="$run_dir/replay.launch"
    local -a command raw_leaf_args
    check_guardian
    wait_for_writeback "$label-before-replay" "$run_dir/writeback-before.tsv"
    evict_paths "$INVENTORY_DIR/capture-files.nul"
    resident="$(snapshot_residency \
        "$label-capture-after-evict" \
        "$INVENTORY_DIR/capture-files.nul" \
        "$run_dir/capture-residency-before.tsv" \
        "$MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT")"
    (( resident <= MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT )) \
        || die "$label starts with $resident resident capture bytes"
    snapshot_pressure "$run_dir/pressure-before.txt"
    record_ingester_runtime_identity "$run_dir/runtime-identity.json" "$config"
    chmod 0444 -- "$run_dir/runtime-identity.json"
    python3 "$FROZEN_GATE" write-invocation \
        --binary "$RUN_INGESTER" \
        --role ingester \
        --env "LC_ALL=C" \
        --env "TZ=UTC" \
        --env "CONFIG_FILE=$config" \
        --env "RUST_LOG=$RUST_LOG_VALUE" \
        --output "$run_dir/invocation.json"
    python3 "$FROZEN_GATE" capacity-snapshot \
        --filesystem "$RESULT_DIR/.." \
        --minimum-free-bytes "$pre_required_free_bytes" \
        --phase "$label-before" \
        --output "$run_dir/capacity-before.json"
    assert_experiment_seals "$label-before-replay"
    command=(
        env
        -i
        "LC_ALL=C"
        "TZ=UTC"
        "CONFIG_FILE=$config"
        "RUST_LOG=$RUST_LOG_VALUE"
        "$RUN_INGESTER"
    )
    perf_json="-"
    if [[ "$PERF_EFFECTIVE" == "on" ]]; then
        command=("$PERF_BIN" stat --no-big-num --field-separator $'\t' --event "$PERF_EVENTS" --output "$run_dir/perf.tsv" -- "${command[@]}")
        perf_json="$run_dir/perf.json"
    fi
    note "running replay $label"
    active_run_dir="$run_dir"
    active_control="$control"
    active_rss_ready="$rss_ready"
    active_capacity_ready="$capacity_ready"
    active_launch="$launch"
    defer_cleanup_signals
    (
        cd "$run_dir"
        while [[ ! -e "$launch" && ! -L "$launch" ]]; do sleep 0.001; done
        [[ -f "$launch" && ! -L "$launch" && ! -s "$launch" \
            && "$(stat -c '%a' -- "$launch")" == 444 ]] || exit 125
        exec env LC_ALL=C /usr/bin/time -v -o "$run_dir/replay.time.txt" \
            "${command[@]}" >"$run_dir/replay.log" 2>&1
    ) &
    launcher_pid=$!
    active_root_pid="$launcher_pid"
    binding="$(bind_live_process "$launcher_pid")" || {
        arm_cleanup_signals; cleanup_active_replay
        die "$label held replay root exited before identity binding"
    }
    read -r active_root_ppid active_root_starttime_ticks <<<"$binding"
    arm_cleanup_signals
    [[ "$active_root_ppid" == "$$" ]] \
        || { cleanup_active_replay; die "$label held root has an unexpected parent"; }
    defer_cleanup_signals
    python3_background "$FROZEN_GATE" monitor-rss \
        --pid "$launcher_pid" \
        --output "$run_dir/rss-samples.tsv" \
        --summary "$run_dir/rss.json" \
        --interval-ms "$RSS_INTERVAL_MS" \
        --control "$control" --rss-ready "$rss_ready" \
        --capacity-ready "$capacity_ready" --launch "$launch" \
        >"$run_dir/rss-monitor.log" 2>&1 &
    monitor_pid=$!
    active_rss_pid="$monitor_pid"
    binding="$(bind_live_process "$monitor_pid")" || {
        arm_cleanup_signals; cleanup_active_replay
        die "$label RSS monitor exited before identity binding"
    }
    read -r active_rss_ppid active_rss_starttime_ticks <<<"$binding"
    arm_cleanup_signals
    [[ "$active_rss_ppid" == "$$" ]] \
        || { cleanup_active_replay; die "$label RSS monitor has an unexpected parent"; }
    defer_cleanup_signals
    python3_background "$FROZEN_GATE" monitor-capacity \
        --pid "$launcher_pid" \
        --filesystem "$RESULT_DIR/.." \
        --minimum-free-bytes "$monitor_minimum_free_bytes" \
        --interval-ms "$CAPACITY_MONITOR_INTERVAL_MS" \
        --output "$run_dir/capacity-samples.tsv" \
        --summary "$run_dir/capacity.json" \
        --control "$control" --rss-ready "$rss_ready" \
        --capacity-ready "$capacity_ready" --launch "$launch" \
        >"$run_dir/capacity-monitor.log" 2>&1 &
    capacity_monitor_pid=$!
    active_capacity_pid="$capacity_monitor_pid"
    binding="$(bind_live_process "$capacity_monitor_pid")" || {
        arm_cleanup_signals; cleanup_active_replay
        die "$label capacity monitor exited before identity binding"
    }
    read -r active_capacity_ppid active_capacity_starttime_ticks <<<"$binding"
    arm_cleanup_signals
    [[ "$active_capacity_ppid" == "$$" ]] \
        || { cleanup_active_replay; die "$label capacity monitor has an unexpected parent"; }
    python3 "$FROZEN_GATE" create-replay-monitor-control \
        --root-pid "$launcher_pid" --root-ppid "$active_root_ppid" \
        --root-starttime-ticks "$active_root_starttime_ticks" \
        --rss-pid "$monitor_pid" --rss-ppid "$active_rss_ppid" \
        --rss-starttime-ticks "$active_rss_starttime_ticks" \
        --capacity-pid "$capacity_monitor_pid" \
        --capacity-ppid "$active_capacity_ppid" \
        --capacity-starttime-ticks "$active_capacity_starttime_ticks" \
        --interval-ms 100 \
        --rss-ready "$rss_ready" --capacity-ready "$capacity_ready" \
        --launch "$launch" --output "$control" >/dev/null \
        || { cleanup_active_replay; die "$label replay control publication failed"; }
    python3 "$FROZEN_GATE" wait-replay-monitors-ready \
        --control "$control" --rss-ready "$rss_ready" \
        --capacity-ready "$capacity_ready" --launch "$launch" \
        --interval-ms 100 --timeout-ms 5000 >/dev/null \
        || { cleanup_active_replay; die "$label monitors did not become ready"; }
    python3 "$FROZEN_GATE" release-replay-launch \
        --control "$control" --rss-ready "$rss_ready" \
        --capacity-ready "$capacity_ready" --launch "$launch" \
        --interval-ms 100 >/dev/null \
        || { cleanup_active_replay; die "$label held replay release failed"; }
    set +e
    wait "$launcher_pid"
    status=$?
    if bounded_reap_job rss-monitor "$monitor_pid" "$active_rss_ppid" \
        "$active_rss_starttime_ticks" 500; then
        monitor_status="$REAP_STATUS"
    else
        monitor_status=124
    fi
    if bounded_reap_job capacity-monitor "$capacity_monitor_pid" \
        "$active_capacity_ppid" "$active_capacity_starttime_ticks" 500; then
        capacity_monitor_status="$REAP_STATUS"
    else
        capacity_monitor_status=124
    fi
    if (( monitor_status == 124 || capacity_monitor_status == 124 )); then
        cleanup_active_replay
    else
        clear_active_replay
    fi
    set -e
    assert_experiment_seals "$label-after-replay"
    printf '%s\n' "$status" >"$run_dir/replay.exit-status"
    printf '%s\n' "$monitor_status" >"$run_dir/rss-monitor.exit-status"
    printf '%s\n' "$capacity_monitor_status" >"$run_dir/capacity-monitor.exit-status"
    (( status == 0 && monitor_status == 0 && capacity_monitor_status == 0 )) || {
        tail -n 80 "$run_dir/replay.log" >&2 || true
        tail -n 80 "$run_dir/capacity-monitor.log" >&2 || true
        die "$label, its RSS monitor, or its capacity monitor failed"
    }
    python3 "$FROZEN_GATE" capacity-snapshot \
        --filesystem "$RESULT_DIR/.." \
        --minimum-free-bytes "$monitor_minimum_free_bytes" \
        --phase "$label-after" \
        --output "$run_dir/capacity-after.json"
    [[ -d "$segments_dir" ]] || die "$label produced no segment corpus"
    report="$(python3 "$FROZEN_GATE" find-replay-report --run-dir "$run_dir")"
    snapshot_pressure "$run_dir/pressure-after.txt"
    python3 "$FROZEN_GATE" validate-writeback-evidence \
        --input "$run_dir/writeback-before.tsv" \
        --phase "$label-before-replay" \
        --ceiling-bytes "$MAX_DIRTY_WRITEBACK_BYTES" >/dev/null
    python3 "$FROZEN_GATE" validate-residency-evidence \
        --input "$run_dir/capture-residency-before.tsv" \
        --phase "$label-capture-after-evict" \
        --paths "$INVENTORY_DIR/capture-files.nul" \
        --ceiling-bytes "$MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT" \
        --page-size-bytes "$PAGE_SIZE_BYTES" >/dev/null
    raw_leaf_args=(
        raw-leaf-seal
        --result-dir "$RESULT_DIR"
        --file "$run_dir/capture-residency-before.tsv"
        --file "$run_dir/capacity-after.json"
        --file "$run_dir/capacity-before.json"
        --file "$run_dir/capacity-monitor.exit-status"
        --file "$run_dir/capacity-monitor.log"
        --file "$run_dir/capacity-monitor.ready"
        --file "$run_dir/capacity-samples.tsv"
        --file "$run_dir/invocation.json"
        --file "$run_dir/pressure-after.txt"
        --file "$run_dir/pressure-before.txt"
        --file "$run_dir/replay.exit-status"
        --file "$run_dir/replay-monitor-control.json"
        --file "$run_dir/replay.launch"
        --file "$run_dir/replay.log"
        --file "$run_dir/replay.time.txt"
        --file "$run_dir/rss-monitor.exit-status"
        --file "$run_dir/rss-monitor.log"
        --file "$run_dir/rss-monitor.ready"
        --file "$run_dir/rss-samples.tsv"
        --file "$run_dir/runtime-identity.json"
        --file "$run_dir/writeback-before.tsv"
        --file "$report"
        --tree "$segments_dir"
        --output "$run_dir/raw-leaves.json"
    )
    if [[ "$PERF_EFFECTIVE" == "on" ]]; then
        raw_leaf_args+=(--file "$run_dir/perf.tsv")
    fi
    python3 "$FROZEN_GATE" "${raw_leaf_args[@]}"
    record_raw_authority "$run_dir/raw-leaves.json"
    python3 "$FROZEN_GATE" parse-time --input "$run_dir/replay.time.txt" --output "$run_dir/replay.time.json" >/dev/null
    if [[ "$PERF_EFFECTIVE" == "on" ]]; then
        python3 "$FROZEN_GATE" parse-perf --input "$run_dir/perf.tsv" --output "$run_dir/perf.json" >/dev/null
    fi
    python3 "$FROZEN_GATE" replay-report --report "$report" --output "$run_dir/replay-correctness.json"
    python3 "$FROZEN_GATE" parse-seal-log --log "$run_dir/replay.log" --output "$run_dir/seal.json"
    python3 "$FROZEN_GATE" tree-manifest \
        --corpus "$segments_dir" \
        --manifest "$run_dir/segments.sha256" \
        --inventory "$run_dir/segments.tsv" \
        --summary "$run_dir/corpus-summary.json"
    python3 "$FROZEN_GATE" check-corpus-capacity \
        --summary "$run_dir/corpus-summary.json" \
        --contract "$METADATA_DIR/capacity-contract.json" \
        --codec "$codec" \
        --output "$run_dir/capacity-corpus-check.json"
    python3 "$FROZEN_GATE" artifact-inventory --corpus "$segments_dir" --output "$run_dir/artifacts.json"
    assert_experiment_seals "$label-after-replay-transforms"
    check_guardian
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$label" "$block" "$slot" "$codec" "$run_dir/config.json" \
        "$run_dir/replay-correctness.json" "$run_dir/segments.tsv" \
        "$run_dir/corpus-summary.json" "$run_dir/replay.time.json" "$run_dir/rss.json" \
        "$run_dir/seal.json" "$perf_json" >>"$RESULT_DIR/replay-index.tsv"
}

while IFS=$'\t' read -r label ordinal block slot codec config segments_dir \
        codec_bound_bytes codec_safe_reserve_bytes future_safe_reserve_bytes \
        pre_required_free_bytes monitor_minimum_free_bytes; do
    [[ "$label" != "label" ]] || continue
    run_replay "$label" "$ordinal" "$block" "$slot" "$codec" "$config" \
        "$segments_dir" "$codec_bound_bytes" "$codec_safe_reserve_bytes" \
        "$future_safe_reserve_bytes" "$pre_required_free_bytes" \
        "$monitor_minimum_free_bytes"
done <"$RESULT_DIR/replay-plan.tsv"

python3 "$FROZEN_GATE" capture-inventory \
    --capture "$CAPTURE" \
    --output "$INVENTORY_DIR/capture-after-replays.json" \
    --paths-output "$INVENTORY_DIR/capture-files-after-replays.nul"
cmp -s "$INVENTORY_DIR/capture.json" "$INVENTORY_DIR/capture-after-replays.json" \
    || die "capture content changed during replay measurement"
cmp -s "$INVENTORY_DIR/capture-files.nul" "$INVENTORY_DIR/capture-files-after-replays.nul" \
    || die "capture path set changed during replay measurement"
assert_experiment_seals after-final-capture-inventory

python3 "$FROZEN_GATE" compare-replays \
    --index "$RESULT_DIR/replay-index.tsv" \
    --blocks "$REPLAY_BLOCKS" \
    --output "$COMPARISON_DIR/replay-equivalence.json" \
    --summary "$RESULT_DIR/replay-summary.tsv"
assert_experiment_seals after-replay-comparison

RAW_REP_LABEL="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["representative_labels"]["raw"])' "$COMPARISON_DIR/replay-equivalence.json")"
GORILLA_REP_LABEL="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["representative_labels"]["gorilla"])' "$COMPARISON_DIR/replay-equivalence.json")"
RAW_CORPUS="$REPLAY_DIR/$RAW_REP_LABEL/segments"
GORILLA_CORPUS="$REPLAY_DIR/$GORILLA_REP_LABEL/segments"

run_verifier() {
    local codec="$1" corpus="$2"
    local output_dir="$VALIDATION_DIR/$codec"
    local status
    mkdir "$output_dir"
    check_guardian
    wait_for_writeback "$codec-before-verifier" \
        "$output_dir/writeback-before-verifier.tsv"
    record_verifier_runtime_identity "$output_dir/storage-verify.runtime-identity.json"
    chmod 0444 -- "$output_dir/storage-verify.runtime-identity.json"
    python3 "$FROZEN_GATE" write-invocation \
        --binary "$RUN_STORAGE_VERIFY" \
        --role verifier \
        --arg=--segments-dir \
        --arg="$corpus" \
        --arg=--schema \
        --arg=schema8 \
        --arg=--validate-segment-footers \
        --arg=--verify-exact-postings \
        --env "LC_ALL=C" \
        --env "TZ=UTC" \
        --output "$output_dir/storage-verify-invocation.json"
    assert_experiment_seals "$codec-before-verifier"
    note "running exhaustive $codec verifier and footer pass outside measurements"
    set +e
    /usr/bin/time -v -o "$output_dir/storage-verify.time.txt" \
        env -i LC_ALL=C TZ=UTC "$RUN_STORAGE_VERIFY" \
            --segments-dir "$corpus" \
            --schema schema8 \
            --validate-segment-footers \
            --verify-exact-postings \
            >"$output_dir/storage-verify.json" 2>"$output_dir/storage-verify.log"
    status=$?
    set -e
    assert_experiment_seals "$codec-after-verifier"
    printf '%s\n' "$status" >"$output_dir/storage-verify.exit-status"
    (( status == 0 )) || die "$codec exhaustive verifier failed"
    python3 "$FROZEN_GATE" validate-writeback-evidence \
        --input "$output_dir/writeback-before-verifier.tsv" \
        --phase "$codec-before-verifier" \
        --ceiling-bytes "$MAX_DIRTY_WRITEBACK_BYTES" >/dev/null
    python3 "$FROZEN_GATE" raw-leaf-seal \
        --result-dir "$RESULT_DIR" \
        --file "$output_dir/storage-verify.exit-status" \
        --file "$output_dir/storage-verify-invocation.json" \
        --file "$output_dir/storage-verify.json" \
        --file "$output_dir/storage-verify.log" \
        --file "$output_dir/storage-verify.runtime-identity.json" \
        --file "$output_dir/storage-verify.time.txt" \
        --file "$output_dir/writeback-before-verifier.tsv" \
        --output "$output_dir/storage-verify-raw-leaves.json"
    record_raw_authority "$output_dir/storage-verify-raw-leaves.json"
    python3 "$FROZEN_GATE" parse-time \
        --input "$output_dir/storage-verify.time.txt" \
        --output "$output_dir/storage-verify.time.json" >/dev/null
    check_guardian
}

run_readback() {
    local codec="$1" corpus="$2"
    local output_dir="$VALIDATION_DIR/$codec"
    local status
    check_guardian
    record_query_runtime_identity "$output_dir/readbacks.runtime-identity.json"
    chmod 0444 -- "$output_dir/readbacks.runtime-identity.json"
    python3 "$FROZEN_GATE" write-invocation \
        --binary "$RUN_QUERY" \
        --role query \
        --arg=--segments-dir \
        --arg="$corpus" \
        --arg=--storage-layout \
        --arg=schema8 \
        --arg=--sample-limit-per-kind \
        --arg="$READBACK_SAMPLE_LIMIT_PER_KIND" \
        --arg=--verify-readbacks \
        --arg=--output \
        --arg="$output_dir/readbacks.md" \
        --env "LC_ALL=C" \
        --env "TZ=UTC" \
        --output "$output_dir/readback-invocation.json"
    assert_experiment_seals "$codec-before-readbacks"
    note "running independent $codec readbacks outside measurements"
    set +e
    /usr/bin/time -v -o "$output_dir/readbacks.time.txt" \
        env -i LC_ALL=C TZ=UTC "$RUN_QUERY" \
            --segments-dir "$corpus" \
            --storage-layout schema8 \
            --sample-limit-per-kind "$READBACK_SAMPLE_LIMIT_PER_KIND" \
            --verify-readbacks \
            --output "$output_dir/readbacks.md" \
            >"$output_dir/readbacks.log" 2>&1
    status=$?
    set -e
    assert_experiment_seals "$codec-after-readbacks"
    printf '%s\n' "$status" >"$output_dir/readbacks.exit-status"
    (( status == 0 )) || die "$codec independent readbacks failed"
    python3 "$FROZEN_GATE" raw-leaf-seal \
        --result-dir "$RESULT_DIR" \
        --file "$output_dir/readback-invocation.json" \
        --file "$output_dir/readbacks.exit-status" \
        --file "$output_dir/readbacks.log" \
        --file "$output_dir/readbacks.md" \
        --file "$output_dir/readbacks.runtime-identity.json" \
        --file "$output_dir/readbacks.time.txt" \
        --output "$output_dir/readback-raw-leaves.json"
    record_raw_authority "$output_dir/readback-raw-leaves.json"
    python3 "$FROZEN_GATE" check-readback \
        --report "$output_dir/readbacks.md" \
        --output "$output_dir/readbacks.json"
    check_guardian
}

run_verifier raw "$RAW_CORPUS"
run_verifier gorilla "$GORILLA_CORPUS"
python3 "$FROZEN_GATE" compare-verifiers \
    --raw "$VALIDATION_DIR/raw/storage-verify.json" \
    --gorilla "$VALIDATION_DIR/gorilla/storage-verify.json" \
    --output "$COMPARISON_DIR/verifier-equivalence-and-codec-inventory.json"
assert_experiment_seals after-verifier-comparison
run_readback raw "$RAW_CORPUS"
run_readback gorilla "$GORILLA_CORPUS"
cmp -s "$VALIDATION_DIR/raw/readbacks.json" "$VALIDATION_DIR/gorilla/readbacks.json" \
    || die "Raw and Gorilla independent readback summaries differ"

for codec in raw gorilla; do
    if [[ "$codec" == "raw" ]]; then corpus="$RAW_CORPUS"; else corpus="$GORILLA_CORPUS"; fi
    python3 "$FROZEN_GATE" query-inventory \
        --corpus "$corpus" \
        --output "$INVENTORY_DIR/$codec-before.json" \
        --paths-output "$INVENTORY_DIR/$codec-files.nul"
done

printf 'process_label\tquery_name\tcategory\tmode\tblock\tslot\tcodec\tcorpus\traw_output\tmax_rss_kib\tperf_json\n' \
    >"$RESULT_DIR/query-index.tsv"

run_query() {
    local query_name="$1" category="$2" mode="$3" start_ms="$4" end_ms="$5" step_ms="$6" cache_bytes="$7" boundaries_csv="$8" expression="$9" block="${10}" slot="${11}" codec="${12}"
    local corpus paths_file process_label run_dir resident max_rss status perf_json boundary
    local -a args command boundaries invocation_command raw_leaf_args
    if [[ "$codec" == "raw" ]]; then
        corpus="$RAW_CORPUS"
        paths_file="$INVENTORY_DIR/raw-files.nul"
    else
        corpus="$GORILLA_CORPUS"
        paths_file="$INVENTORY_DIR/gorilla-files.nul"
    fi
    process_label="$(printf '%s-b%02d-s%02d-%s' "$query_name" "$block" "$slot" "$codec")"
    run_dir="$QUERY_RUN_DIR/$process_label"
    [[ ! -e "$run_dir" ]] || die "query output already exists: $run_dir"
    mkdir "$run_dir"
    check_guardian
    wait_for_writeback "$process_label-before-query" "$run_dir/writeback-before.tsv"
    evict_paths "$paths_file"
    resident="$(snapshot_residency \
        "$process_label-corpus-after-evict" \
        "$paths_file" \
        "$run_dir/residency-after-evict.tsv" \
        "$MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT")"
    (( resident <= MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT )) \
        || die "$process_label starts with $resident resident corpus bytes"
    snapshot_pressure "$run_dir/pressure-before.txt"
    args=(
        --segments-dir "$corpus"
        --storage-layout schema8
        --label-materialization demand-driven
        --query-label-storage compact-ids
        --query-label-arena-max-bytes "$QUERY_LABEL_ARENA_MAX_BYTES"
        --query-instrumentation off
        --start-ms "$start_ms"
        --end-ms "$end_ms"
        --benchmark-repeats "$BENCHMARK_REPEATS"
        --chunk-read-mode pread
        --chunk-read-queue-depth "$CHUNK_READ_QUEUE_DEPTH"
        --chunk-payload-coalesce-max-gap-bytes 4096
        --query-max-series-matched "$QUERY_MAX_SERIES_MATCHED"
        --query-max-projected-series "$QUERY_MAX_PROJECTED_SERIES"
        --query-max-chunks-read "$QUERY_MAX_CHUNKS_READ"
        --query-max-bytes-read "$QUERY_MAX_BYTES_READ"
        --query-max-samples "$QUERY_MAX_SAMPLES"
        --regex-max-expanded-values "$REGEX_MAX_EXPANDED_VALUES"
        --output "$run_dir/report.md"
        --raw-output "$run_dir/raw.json"
        --query "$expression"
    )
    if [[ "$mode" == "range" ]]; then
        [[ "$step_ms" != "-" && "$cache_bytes" != "-" ]] || die "$query_name range settings are incomplete"
        args+=(--step-ms "$step_ms" --range-scalar-cache-max-bytes "$cache_bytes")
    else
        [[ "$step_ms" == "-" && "$cache_bytes" == "-" ]] || die "$query_name has unexpected range settings"
    fi
    if [[ "$boundaries_csv" != "-" ]]; then
        IFS=',' read -r -a boundaries <<<"$boundaries_csv"
        for boundary in "${boundaries[@]}"; do
            args+=(--exponential-histogram-bucket-boundary "$boundary")
        done
    fi
    record_query_runtime_identity "$run_dir/runtime-identity.json"
    chmod 0444 -- "$run_dir/runtime-identity.json"
    invocation_command=(
        python3 "$FROZEN_GATE" write-invocation
        --binary "$RUN_QUERY"
        --role query
        --env "LC_ALL=C"
        --env "TZ=UTC"
        --output "$run_dir/invocation.json"
    )
    for argument in "${args[@]}"; do
        invocation_command+=(--arg="$argument")
    done
    "${invocation_command[@]}"
    assert_experiment_seals "$process_label-before-query"
    command=(env -i LC_ALL=C TZ=UTC "$RUN_QUERY" "${args[@]}")
    perf_json="-"
    if [[ "$PERF_EFFECTIVE" == "on" ]]; then
        command=("$PERF_BIN" stat --no-big-num --field-separator $'\t' --event "$PERF_EVENTS" --output "$run_dir/perf.tsv" -- "${command[@]}")
        perf_json="$run_dir/perf.json"
    fi
    note "running query $process_label"
    set +e
    LC_ALL=C /usr/bin/time -v -o "$run_dir/time.txt" "${command[@]}" >"$run_dir/query.log" 2>&1
    status=$?
    set -e
    assert_experiment_seals "$process_label-after-query"
    printf '%s\n' "$status" >"$run_dir/exit-status"
    if (( status != 0 )); then
        tail -n 60 "$run_dir/query.log" >&2 || true
        die "$process_label failed"
    fi
    max_rss="$(awk -F: '/Maximum resident set size/ {gsub(/^[[:space:]]+/, "", $2); print $2}' "$run_dir/time.txt")"
    [[ "$max_rss" =~ ^[1-9][0-9]*$ ]] || die "could not parse query max RSS for $process_label"
    snapshot_pressure "$run_dir/pressure-after.txt"
    snapshot_residency \
        "$process_label-corpus-after-run" \
        "$paths_file" \
        "$run_dir/residency-after-run.tsv" \
        - >/dev/null
    python3 "$FROZEN_GATE" validate-writeback-evidence \
        --input "$run_dir/writeback-before.tsv" \
        --phase "$process_label-before-query" \
        --ceiling-bytes "$MAX_DIRTY_WRITEBACK_BYTES" >/dev/null
    python3 "$FROZEN_GATE" validate-residency-evidence \
        --input "$run_dir/residency-after-evict.tsv" \
        --phase "$process_label-corpus-after-evict" \
        --paths "$paths_file" \
        --ceiling-bytes "$MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT" \
        --page-size-bytes "$PAGE_SIZE_BYTES" >/dev/null
    python3 "$FROZEN_GATE" validate-residency-evidence \
        --input "$run_dir/residency-after-run.tsv" \
        --phase "$process_label-corpus-after-run" \
        --paths "$paths_file" \
        --page-size-bytes "$PAGE_SIZE_BYTES" >/dev/null
    raw_leaf_args=(
        raw-leaf-seal
        --result-dir "$RESULT_DIR"
        --file "$run_dir/exit-status"
        --file "$run_dir/invocation.json"
        --file "$run_dir/pressure-after.txt"
        --file "$run_dir/pressure-before.txt"
        --file "$run_dir/query.log"
        --file "$run_dir/raw.json"
        --file "$run_dir/report.md"
        --file "$run_dir/residency-after-evict.tsv"
        --file "$run_dir/residency-after-run.tsv"
        --file "$run_dir/runtime-identity.json"
        --file "$run_dir/time.txt"
        --file "$run_dir/writeback-before.tsv"
        --output "$run_dir/raw-leaves.json"
    )
    if [[ "$PERF_EFFECTIVE" == "on" ]]; then
        raw_leaf_args+=(--file "$run_dir/perf.tsv")
    fi
    python3 "$FROZEN_GATE" "${raw_leaf_args[@]}"
    record_raw_authority "$run_dir/raw-leaves.json"
    if [[ "$PERF_EFFECTIVE" == "on" ]]; then
        python3 "$FROZEN_GATE" parse-perf --input "$run_dir/perf.tsv" --output "$run_dir/perf.json" >/dev/null
    fi
    assert_experiment_seals "$process_label-after-query-transforms"
    check_guardian
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$query_name" "$category" "$mode" "$block" "$slot" "$codec" \
        "$corpus" "$run_dir/raw.json" "$max_rss" "$perf_json" >>"$RESULT_DIR/query-index.tsv"
}

while IFS=$'\t' read -r query_name category mode start_ms end_ms step_ms cache_bytes boundaries_csv expression; do
    [[ "$query_name" != "query_name" ]] || continue
    for ((block = 1; block <= QUERY_BLOCKS; block++)); do
        read -r -a codecs <<<"$(codec_order "$block")"
        for ((slot = 1; slot <= 4; slot++)); do
            run_query "$query_name" "$category" "$mode" "$start_ms" "$end_ms" "$step_ms" \
                "$cache_bytes" "$boundaries_csv" "$expression" "$block" "$slot" "${codecs[$((slot - 1))]}"
        done
    done
done <"$RESULT_DIR/queries.tsv"

python3 "$FROZEN_GATE" compare-queries \
    --index "$RESULT_DIR/query-index.tsv" \
    --manifest "$RESULT_DIR/queries.normalized.json" \
    --summary "$RESULT_DIR/query-summary.tsv" \
    --output "$COMPARISON_DIR/query-equivalence.json" \
    --blocks "$QUERY_BLOCKS" \
    --benchmark-repeats "$BENCHMARK_REPEATS" \
    --queue-depth "$CHUNK_READ_QUEUE_DEPTH" \
    --label-materialization demand-driven \
    --max-matched-series "$QUERY_MAX_SERIES_MATCHED" \
    --max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
    --max-chunk-reads "$QUERY_MAX_CHUNKS_READ" \
    --max-bytes-read "$QUERY_MAX_BYTES_READ" \
    --max-samples-decoded "$QUERY_MAX_SAMPLES" \
    --max-regex-values-examined "$REGEX_MAX_EXPANDED_VALUES"
assert_experiment_seals after-query-comparison

for codec in raw gorilla; do
    if [[ "$codec" == "raw" ]]; then corpus="$RAW_CORPUS"; else corpus="$GORILLA_CORPUS"; fi
    python3 "$FROZEN_GATE" query-inventory \
        --corpus "$corpus" \
        --output "$INVENTORY_DIR/$codec-after.json" \
        --paths-output "$INVENTORY_DIR/$codec-files-after.nul"
    cmp -s "$INVENTORY_DIR/$codec-before.json" "$INVENTORY_DIR/$codec-after.json" \
        || die "$codec corpus changed during validation/query measurement"
    cmp -s "$INVENTORY_DIR/$codec-files.nul" "$INVENTORY_DIR/$codec-files-after.nul" \
        || die "$codec corpus path set changed during validation/query measurement"
done
assert_experiment_seals after-final-corpus-inventory

check_guardian
python3 "$FROZEN_GATE" capacity-snapshot \
    --filesystem "$RESULT_DIR/.." \
    --minimum-free-bytes "$CAPACITY_OPERATIONAL_FLOOR_BYTES" \
    --phase final \
    --output "$METADATA_DIR/capacity-final.json"
cleanup_guardian || die "conflict guardian reported an unrelated workload"
trap - EXIT

global_raw_args=(
    raw-leaf-seal
    --result-dir "$RESULT_DIR"
    --file "$METADATA_DIR/capacity-final.json"
    --file "$METADATA_DIR/capacity-postbuild.json"
    --file "$METADATA_DIR/capacity-prebuild.json"
    --file "$METADATA_DIR/environment.txt"
    --file "$METADATA_DIR/guardian-conflicts.tsv"
    --file "$METADATA_DIR/guardian.log"
    --file "$METADATA_DIR/guardian-precheck.json"
    --file "$METADATA_DIR/guardian.ready"
    --file "$METADATA_DIR/guardian-samples.tsv"
    --file "$METADATA_DIR/guardian.stop"
    --file "$METADATA_DIR/perf-effective.txt"
    --output "$METADATA_DIR/final-raw-leaves.json"
)
if [[ "$PERF_STAT_MODE" != "off" ]]; then
    global_raw_args+=(
        --file "$METADATA_DIR/perf-preflight.exit-status"
        --file "$METADATA_DIR/perf-preflight.log"
        --file "$METADATA_DIR/perf-preflight.tsv"
    )
fi
python3 "$FROZEN_GATE" "${global_raw_args[@]}"
record_raw_authority "$METADATA_DIR/final-raw-leaves.json"
raw_authority_command=(
    python3 "$FROZEN_GATE" write-raw-authorities
    --result-dir "$RESULT_DIR"
    --output "$METADATA_DIR/raw-authorities.tsv"
    --checksum-output "$METADATA_DIR/raw-authorities.sha256"
)
for authority in "${RAW_AUTHORITY_ENTRIES[@]}"; do
    raw_authority_command+=(--entry "$authority")
done
"${raw_authority_command[@]}"

assert_experiment_seals finalization
if [[ "$BINARY_PROVENANCE_MODE" == "internal" ]]; then
    python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
        >"$METADATA_DIR/build/source-check-final.json"
    sha256sum --check --strict "$SOURCE_ARCHIVE_SHA256" \
        >"$METADATA_DIR/build/source-archive-check-final.txt"
    python3 "$FROZEN_GATE" check-source-snapshot-seal \
        --repo "$REPO_ROOT" \
        --snapshot "$BUILD_SOURCE_DIR" \
        --source-seal "$SOURCE_SEAL" \
        --seal "$SOURCE_SNAPSHOT_SEAL" \
        >"$METADATA_DIR/build/source-snapshot-check-final.json"
    python3 "$FROZEN_GATE" check-cargo-config-isolation \
        --snapshot "$BUILD_SOURCE_DIR" \
        --cargo-home "$BUILD_CARGO_HOME" \
        >"$METADATA_DIR/build/cargo-config-isolation-final.json"
fi
assert_experiment_seals final-authorities

python3 "$FROZEN_GATE" final-admission \
    --result-dir "$RESULT_DIR" \
    --plan "$ADMISSION_PLAN" \
    --output "$METADATA_DIR/final-admission.json"

cat >"$RESULT_DIR/TIMESTAMP_CODEC_AB_BLOCKED.txt" <<'EOF'
The current storage verifier emitted exhaustive evidence-only timestamp
candidates, but the current writer, reader, config, and CLI expose no versioned
timestamp codec selector. No timestamp replay/query A/B was possible. Candidate
native-payload byte totals exclude typed scalar-lane timestamp bytes and are not
a production capacity claim.
EOF
if [[ "$PROMOTION_ELIGIBILITY" == "formal_source_bound" ]]; then
    touch "$RESULT_DIR/RAW_GORILLA_COMPLETE_TIMESTAMP_AB_BLOCKED"
    COMPLETION_NOTE="formal Raw/Gorilla gate complete; timestamp runtime A/B remains explicitly blocked: $RESULT_DIR"
else
    cat >"$RESULT_DIR/EXPLORATORY_EXTERNAL_BINARIES_NON_PROMOTABLE.txt" <<'EOF'
This result used caller-supplied external binaries. The binaries were preserved
and hash-checked, but they were not produced by this result's clean source-bound
build. This result is exploratory, cannot create the formal completion marker,
and is not admissible for codec promotion.
EOF
    COMPLETION_NOTE="exploratory Raw/Gorilla gate complete; result is non-promotable: $RESULT_DIR"
fi

python3 "$FROZEN_GATE" final-artifact-inventory \
    --result-dir "$RESULT_DIR" \
    --output "$METADATA_DIR/result-artifacts.nul"
chmod 0444 -- "$METADATA_DIR/result-artifacts.nul"
(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        [[ "$artifact" != /* && "$artifact" != ".." && "$artifact" != ../* \
            && -f "$artifact" && ! -L "$artifact" ]] \
            || die "final artifact inventory contains an unsafe entry: $artifact"
        sha256sum -- "$artifact"
    done <metadata/result-artifacts.nul
    sha256sum -- metadata/result-artifacts.nul
) >"$METADATA_DIR/result-artifacts.sha256"
chmod 0444 -- "$METADATA_DIR/result-artifacts.sha256"
(
    cd "$RESULT_DIR"
    sha256sum --check --strict metadata/result-artifacts.sha256 >/dev/null
) || die "final result artifact seal did not validate"
python3 "$FROZEN_GATE" verify-final-artifact-seal --result-dir "$RESULT_DIR"
note "$COMPLETION_NOTE"
