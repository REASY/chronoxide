#!/usr/bin/env bash

set -euo pipefail

export PYTHONDONTWRITEBYTECODE=1
export PYTHONNOUSERSITE=1
unset PYTHONHOME PYTHONPATH PYTHONSTARTUP PYTHONUSERBASE

python3() {
    command python3 -B -I -S "$@"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE="$SCRIPT_DIR/phase5_head_topology_gate.py"
GUARD="$SCRIPT_DIR/phase5_head_topology_guard.py"
PHASE1_GATE="$SCRIPT_DIR/phase1_replay_gate.py"
REPORT_GATE="$SCRIPT_DIR/ab_gate.py"
EXPECTATIONS="$SCRIPT_DIR/phase1_4m_expectations.json"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"
HARNESS_FILES=(
    ab_gate.py
    fadvise_regular_dontneed.c
    phase1_4m_expectations.json
    phase1_replay_gate.py
    phase5_head_topology_gate.py
    phase5_head_topology_guard.py
    phase5_head_topology_run.sh
    test_phase5_head_topology_gate.py
    test_phase5_head_topology_guard.py
)

CAPTURE="${CAPTURE:-/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001}"
CONFIG_TEMPLATE="${CONFIG_TEMPLATE:-/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/post-adaptive-head-profile-20260716-223717/config.toml}"
REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
RESULT_DIR="${RESULT_DIR:-}"
MESSAGES="${MESSAGES:-4000000}"
RUST_LOG_VALUE="${RUST_LOG_VALUE:-chronoxide_ingester=info,chronoxide_core=warn}"
RUN_NOTE="${RUN_NOTE:-}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
RSS_INTERVAL_MS="${RSS_INTERVAL_MS:-100}"
GUARD_INTERVAL_MS="${GUARD_INTERVAL_MS:-100}"
SIZING_MESSAGES="${SIZING_MESSAGES:-250000}"
SIZING_SAFETY_MULTIPLIER="${SIZING_SAFETY_MULTIPLIER:-2}"
DETERMINISM_PREFIX_MESSAGES="${DETERMINISM_PREFIX_MESSAGES:-8192}"
DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES="${DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES:-268435456}"
FULL_CAPTURE_COUNT=2
FULL_CAPTURE_LAYOUT_OVERHEAD_BYTES=67108864
BOUNDED_PREFIX_OUTPUT_COUNT=4
SIZING_CORPUS_COUNT=2
HARNESS_OVERHEAD_BYTES=4294967296
MIN_SAFETY_RESERVE_BYTES=17179869184
BUILD_OUTPUT_ALLOWANCE_BYTES=8589934592
SAFETY_RESERVE_BYTES="${SAFETY_RESERVE_BYTES:-$MIN_SAFETY_RESERVE_BYTES}"
DRY_RUN=0

usage() {
    cat <<'EOF'
Usage:
  RESULT_DIR=/absolute/new/external/path \
  QUIET_HOST_CONFIRMED=1 \
  RUN_NOTE='quiet host; no build, profiler, footer scan, replay, or unrelated database active' \
    docs/experiments/storage_vnext/phase5_head_topology_run.sh [--dry-run]

The formal run creates one full 4M transform per 16-partition topology. Two
independent bounded Zstd-prefix transforms per topology prove deterministic
transform bytes without duplicating the full captures. It then executes one
full pp/ap/pa/aa 2x2 factorial for each topology with one controlled,
clean-tree release build from a fresh, exact, read-only `git archive HEAD`
extraction. The eight unreplicated cells support isolated directional
conclusions, never production promotion. Only
hash-locked preserved copies of its four binaries are executed. The runner
never deletes or reuses an output path.
EOF
}

die() {
    echo "Phase 5 head topology: $*" >&2
    exit 2
}

note() {
    echo "Phase 5 head topology: $*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

if (($# > 1)); then
    usage >&2
    die "at most one argument is accepted"
fi
if (($# == 1)); then
    [[ "$1" == "--dry-run" ]] || { usage >&2; die "unknown argument: $1"; }
    DRY_RUN=1
fi

for command in awk bash cargo cc chmod cmp cp date df diff find fincore git grep mkdir perf ps python3 realpath rustc rustup sha256sum sort stat sync tail uname xargs /usr/bin/time; do
    require_command "$command"
done
for file in "$GATE" "$GUARD" "$PHASE1_GATE" "$REPORT_GATE" "$EXPECTATIONS" "$FADVISE_SOURCE"; do
    [[ -f "$file" ]] || die "required harness dependency is missing: $file"
done
python3 "$GATE" check-ambient-env >/dev/null \
    || die "ambient build/runtime environment violates the sanitized contract"
for legacy_binary_variable in INGESTER_BIN REPARTITION_BIN QUERY_BIN STORAGE_VERIFY_BIN; do
    [[ -z "${!legacy_binary_variable:-}" ]] \
        || die "$legacy_binary_variable is forbidden; Phase 5 performs one controlled source-bound build"
done
[[ "$MESSAGES" =~ ^[1-9][0-9]*$ ]] || die "MESSAGES must be a positive integer"
[[ "$MESSAGES" == "4000000" ]] || die "the pinned correctness contract requires MESSAGES=4000000"
[[ "$RSS_INTERVAL_MS" == "100" ]] \
    || die "the formal RSS cadence is fixed at RSS_INTERVAL_MS=100"
[[ "$GUARD_INTERVAL_MS" == "100" ]] \
    || die "the formal guardian cadence is fixed at GUARD_INTERVAL_MS=100"
[[ "$SIZING_MESSAGES" == "250000" ]] \
    || die "the predeclared sizing contract requires SIZING_MESSAGES=250000"
[[ "$SIZING_SAFETY_MULTIPLIER" =~ ^[1-9][0-9]*$ \
    && "$SIZING_SAFETY_MULTIPLIER" -ge 2 ]] \
    || die "SIZING_SAFETY_MULTIPLIER must be an integer of at least 2"
[[ "$DETERMINISM_PREFIX_MESSAGES" =~ ^[1-9][0-9]*$ \
    && "$DETERMINISM_PREFIX_MESSAGES" -ge 80 \
    && "$DETERMINISM_PREFIX_MESSAGES" -le 100000 ]] \
    || die "DETERMINISM_PREFIX_MESSAGES must be in 80..=100000"
[[ "$DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES" =~ ^[1-9][0-9]*$ ]] \
    || die "DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES must be positive"
[[ "$SAFETY_RESERVE_BYTES" =~ ^[1-9][0-9]*$ \
    && "$SAFETY_RESERVE_BYTES" -ge "$MIN_SAFETY_RESERVE_BYTES" ]] \
    || die "SAFETY_RESERVE_BYTES must be at least $MIN_SAFETY_RESERVE_BYTES"
[[ "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
    || die "RUN_NOTE must not contain tabs or newlines"
if [[ "$DRY_RUN" != "1" ]]; then
    [[ "$QUIET_HOST_CONFIRMED" == "1" ]] || die "QUIET_HOST_CONFIRMED=1 is required"
    [[ -n "$RUN_NOTE" ]] || die "RUN_NOTE is required"
fi

[[ "$CAPTURE" == /* && -d "$CAPTURE" ]] || die "CAPTURE must be an absolute directory"
[[ "$CONFIG_TEMPLATE" == /* && -f "$CONFIG_TEMPLATE" ]] \
    || die "CONFIG_TEMPLATE must be an absolute regular file"
[[ "$REPO_ROOT" == /* && -d "$REPO_ROOT" ]] || die "REPO_ROOT must be absolute"
CAPTURE="$(realpath -e -- "$CAPTURE")"
CONFIG_TEMPLATE="$(realpath -e -- "$CONFIG_TEMPLATE")"
REPO_ROOT="$(realpath -e -- "$REPO_ROOT")"
[[ "$(git -C "$REPO_ROOT" rev-parse --show-toplevel)" == "$REPO_ROOT" ]] \
    || die "REPO_ROOT is not a Git worktree root"

[[ -n "$RESULT_DIR" && "$RESULT_DIR" == /* ]] \
    || die "RESULT_DIR must be a new absolute external path"
result_name="$(basename "$RESULT_DIR")"
[[ -n "$result_name" && "$result_name" != "." && "$result_name" != ".." ]] \
    || die "RESULT_DIR must name a child"
result_parent_input="$(dirname "$RESULT_DIR")"
[[ -d "$result_parent_input" ]] || die "RESULT_DIR parent does not exist"
result_parent="$(realpath -e -- "$result_parent_input")"
RESULT_DIR="$result_parent/$result_name"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists"
case "$RESULT_DIR/" in
    "$REPO_ROOT/"*|"$CAPTURE/"*) die "RESULT_DIR must be outside source and capture roots" ;;
esac
for path in "$CAPTURE" "$CONFIG_TEMPLATE" "$REPO_ROOT" "$RESULT_DIR"; do
    [[ "$path" != *$'\n'* && "$path" != *$'\t'* ]] || die "paths must not contain tabs/newlines"
done

note "validating the pinned source capture and configuration"
python3 "$PHASE1_GATE" validate-inputs \
    --capture "$CAPTURE" --template "$CONFIG_TEMPLATE" --expectations "$EXPECTATIONS" \
    >/dev/null

source_capture_bytes="$(find "$CAPTURE" -maxdepth 1 -type f -printf '%s\n' \
    | awk '{sum += $1} END {printf "%.0f", sum}')"
[[ "$source_capture_bytes" =~ ^[1-9][0-9]*$ ]] \
    || die "could not determine source capture bytes"
full_capture_estimate_bytes="$((
    FULL_CAPTURE_COUNT * (source_capture_bytes + FULL_CAPTURE_LAYOUT_OVERHEAD_BYTES)
))"
bounded_prefix_estimate_bytes="$((
    BOUNDED_PREFIX_OUTPUT_COUNT * DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES
))"
sizing_corpus_upper_bound_bytes="$source_capture_bytes"
sizing_corpus_estimate_bytes="$((SIZING_CORPUS_COUNT * sizing_corpus_upper_bound_bytes))"
sizing_transient_headroom_bytes="$source_capture_bytes"
estimated_outputs_bytes="$((
    full_capture_estimate_bytes
    + bounded_prefix_estimate_bytes
    + sizing_corpus_estimate_bytes
    + sizing_transient_headroom_bytes
    + BUILD_OUTPUT_ALLOWANCE_BYTES
    + HARNESS_OVERHEAD_BYTES
))"
required_free_bytes="$((estimated_outputs_bytes + SAFETY_RESERVE_BYTES))"
available_bytes="$(df -B1 --output=avail "$result_parent" | awk 'NR == 2 { print $1 }')"
[[ "$available_bytes" =~ ^[0-9]+$ ]] || die "could not determine result filesystem space"
((available_bytes >= required_free_bytes)) || die \
    "result filesystem has $available_bytes bytes; require $required_free_bytes including $BUILD_OUTPUT_ALLOWANCE_BYTES build-output allowance and $SAFETY_RESERVE_BYTES safety"

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/captures" "$RESULT_DIR/captures/determinism" \
    "$RESULT_DIR/configs" "$RESULT_DIR/runs" \
    "$RESULT_DIR/sizing" "$RESULT_DIR/validation" "$RESULT_DIR/comparisons" \
    "$RESULT_DIR/inventory" "$RESULT_DIR/metadata"
mkdir "$RESULT_DIR/metadata/binaries" "$RESULT_DIR/metadata/harness" \
    "$RESULT_DIR/metadata/source" "$RESULT_DIR/metadata/tools" \
    "$RESULT_DIR/metadata/build"

for file in "${HARNESS_FILES[@]}"; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$file" "$RESULT_DIR/metadata/harness/$file"
done
FROZEN_GATE="$RESULT_DIR/metadata/harness/phase5_head_topology_gate.py"
FROZEN_GUARD="$RESULT_DIR/metadata/harness/phase5_head_topology_guard.py"
FROZEN_PHASE1_GATE="$RESULT_DIR/metadata/harness/phase1_replay_gate.py"
FROZEN_REPORT_GATE="$RESULT_DIR/metadata/harness/ab_gate.py"
FROZEN_EXPECTATIONS="$RESULT_DIR/metadata/harness/phase1_4m_expectations.json"
FROZEN_CONFIG_TEMPLATE="$RESULT_DIR/metadata/config-template.toml"
cp --preserve=mode,timestamps -- "$CONFIG_TEMPLATE" "$FROZEN_CONFIG_TEMPLATE"

(
    cd "$RESULT_DIR/metadata/harness"
    find . -type f -print0 | sort -z | xargs -0 sha256sum
) >"$RESULT_DIR/metadata/harness.sha256"
find "$RESULT_DIR/metadata/harness" -maxdepth 1 -type f -exec chmod 0444 -- {} +
chmod 0555 -- "$RESULT_DIR/metadata/harness"
python3 "$FROZEN_GATE" check-frozen-harness \
    --harness "$RESULT_DIR/metadata/harness" >/dev/null
sha256sum "$FROZEN_CONFIG_TEMPLATE" >"$RESULT_DIR/metadata/config-template.sha256"

SOURCE_SEAL="$RESULT_DIR/metadata/source/formal-source-seal.json"
python3 "$FROZEN_GATE" source-seal --repo "$REPO_ROOT" --output "$SOURCE_SEAL"
SOURCE_ARCHIVE="$RESULT_DIR/metadata/source/source-head.tar"
SOURCE_ARCHIVE_SEAL="$RESULT_DIR/metadata/source/source-archive-seal.json"
SOURCE_SNAPSHOT_SEAL="$RESULT_DIR/metadata/source/source-snapshot-seal.json"
BUILD_SOURCE_DIR="$RESULT_DIR/build-source"
git -C "$REPO_ROOT" archive --format=tar --output="$SOURCE_ARCHIVE" HEAD
chmod 0444 -- "$SOURCE_ARCHIVE"
python3 "$FROZEN_GATE" extract-source-archive \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
    --destination "$BUILD_SOURCE_DIR" --output "$SOURCE_ARCHIVE_SEAL"
python3 "$FROZEN_GATE" source-snapshot-seal \
    --repo "$REPO_ROOT" --snapshot "$BUILD_SOURCE_DIR" \
    --output "$SOURCE_SNAPSHOT_SEAL"
chmod 0444 -- "$SOURCE_SEAL" "$SOURCE_ARCHIVE_SEAL" "$SOURCE_SNAPSHOT_SEAL"

preserve_binary() {
    local role="$1"
    local source="$2"
    local destination="$RESULT_DIR/metadata/binaries/$role"
    cp --reflink=auto --preserve=mode,timestamps -- "$source" "$destination"
    cmp -s -- "$source" "$destination" || die "preserved binary differs: $role"
    chmod a-w -- "$destination"
    [[ -x "$destination" ]] || die "preserved binary is not executable: $role"
    printf '%s\t%s\t%s\t%s\n' "$role" "$source" "$destination" \
        "$(sha256sum "$destination" | awk '{print $1}')" \
        >>"$RESULT_DIR/metadata/binaries.tsv"
}

BUILD_DIR="$RESULT_DIR/metadata/build"
BUILD_HOME="$RESULT_DIR/build-state/home"
BUILD_CARGO_HOME="$RESULT_DIR/build-state/cargo-home"
BUILD_TARGET_DIR="$RESULT_DIR/build-target"
mkdir "$RESULT_DIR/build-state" "$BUILD_HOME" "$BUILD_CARGO_HOME" "$BUILD_TARGET_DIR"
CARGO_BIN="$(type -P cargo)"
RUSTC_BIN="$(type -P rustc)"
CC_BIN="$(type -P cc)"
PERF_BIN="$(type -P perf)"
FINCORE_BIN="$(type -P fincore)"
SYNC_BIN="$(type -P sync)"
PYTHON_BIN="$(type -P python3)"
TIME_BIN="$(realpath -e -- /usr/bin/time)"
[[ "$CARGO_BIN" == /* && -x "$CARGO_BIN" && "$RUSTC_BIN" == /* && -x "$RUSTC_BIN" \
    && "$CC_BIN" == /* && -x "$CC_BIN" && "$PERF_BIN" == /* && -x "$PERF_BIN" \
    && "$FINCORE_BIN" == /* && -x "$FINCORE_BIN" && "$SYNC_BIN" == /* \
    && -x "$SYNC_BIN" && "$PYTHON_BIN" == /* && -x "$PYTHON_BIN" \
    && "$TIME_BIN" == /* && -x "$TIME_BIN" ]] \
    || die "build and measurement tools must resolve to absolute executables"
printf 'perf\t%s\nfincore\t%s\nsync\t%s\npython3\t%s\ntime\t%s\n' \
    "$PERF_BIN" "$FINCORE_BIN" "$SYNC_BIN" "$PYTHON_BIN" "$TIME_BIN" \
    >"$RESULT_DIR/metadata/tools/runtime-tool-paths.tsv"
sha256sum "$PERF_BIN" "$FINCORE_BIN" "$SYNC_BIN" "$PYTHON_BIN" "$TIME_BIN" \
    >"$RESULT_DIR/metadata/tools/runtime-tool-binaries.sha256"
RUSTUP_HOME_EFFECTIVE="$HOME/.rustup"
[[ -d "$RUSTUP_HOME_EFFECTIVE" ]] || die "RUSTUP_HOME is unavailable: $RUSTUP_HOME_EFFECTIVE"
BUILD_TARGET_TRIPLE="$(env -i \
    PATH="$PATH" HOME="$BUILD_HOME" RUSTUP_HOME="$RUSTUP_HOME_EFFECTIVE" \
    CARGO_HOME="$BUILD_CARGO_HOME" LC_ALL=C TZ=UTC \
    "$RUSTC_BIN" --version --verbose | awk -F': ' '$1 == "host" {print $2}')"
[[ "$BUILD_TARGET_TRIPLE" =~ ^[A-Za-z0-9_.-]+$ ]] \
    || die "could not determine the formal build target triple"
SOURCE_DATE_EPOCH="$(git -C "$REPO_ROOT" show -s --format=%ct HEAD)"
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
    --bin chronoxide-capture-repartition
    --bin chronoxide-query
    --bin chronoxide-storage-verify
)
{
    printf 'name\tvalue\n'
    for assignment in "${BUILD_ENV[@]}"; do
        printf '%s\t%s\n' "${assignment%%=*}" "${assignment#*=}"
    done
    printf 'RUSTFLAGS\t<unset>\nRUSTDOCFLAGS\t<unset>\n'
    printf 'features\tno-default-features\nprofile\trelease\ntarget\t%s\n' \
        "$BUILD_TARGET_TRIPLE"
} >"$BUILD_DIR/build-environment.tsv"
printf '%q ' "${BUILD_COMMAND[@]}" >"$BUILD_DIR/build-command.txt"
printf '\n' >>"$BUILD_DIR/build-command.txt"
printf 'formal_build_command=cargo build --locked --release --no-default-features\n' \
    >"$BUILD_DIR/build-contract.txt"
env -i "${BUILD_ENV[@]}" "$RUSTC_BIN" --version --verbose \
    >"$BUILD_DIR/rustc-version.txt"
env -i "${BUILD_ENV[@]}" "$CARGO_BIN" --version --verbose \
    >"$BUILD_DIR/cargo-version.txt"
env -i PATH="$PATH" LC_ALL=C TZ=UTC "$CC_BIN" --version \
    >"$BUILD_DIR/cc-version.txt" 2>&1
printf 'cargo\t%s\nrustc\t%s\ncc\t%s\n' "$CARGO_BIN" "$RUSTC_BIN" "$CC_BIN" \
    >"$BUILD_DIR/tool-paths.tsv"
sha256sum "$CARGO_BIN" "$RUSTC_BIN" "$CC_BIN" >"$BUILD_DIR/tool-binaries.sha256"
if command -v rustup >/dev/null 2>&1; then
    RUSTUP_BIN="$(type -P rustup)"
    RUSTC_TOOLCHAIN_BIN="$(env -i "${BUILD_ENV[@]}" "$RUSTUP_BIN" which rustc)"
    CARGO_TOOLCHAIN_BIN="$(env -i "${BUILD_ENV[@]}" "$RUSTUP_BIN" which cargo)"
    printf 'rustup\t%s\nrustc-toolchain\t%s\ncargo-toolchain\t%s\n' \
        "$RUSTUP_BIN" "$RUSTC_TOOLCHAIN_BIN" "$CARGO_TOOLCHAIN_BIN" \
        >>"$BUILD_DIR/tool-paths.tsv"
    sha256sum "$RUSTUP_BIN" "$RUSTC_TOOLCHAIN_BIN" "$CARGO_TOOLCHAIN_BIN" \
        >>"$BUILD_DIR/tool-binaries.sha256"
    env -i "${BUILD_ENV[@]}" "$RUSTUP_BIN" show active-toolchain \
        >"$BUILD_DIR/rustup-active-toolchain.txt"
fi
(
    cd "$BUILD_SOURCE_DIR"
    env -i "${BUILD_ENV[@]}" "$CARGO_BIN" metadata --locked --no-deps \
        --format-version 1 --no-default-features
) >"$BUILD_DIR/cargo-metadata.json"
python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    >"$BUILD_DIR/source-check-before-build.json"
python3 "$FROZEN_GATE" check-source-archive-seal \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" --seal "$SOURCE_ARCHIVE_SEAL" \
    >"$BUILD_DIR/source-archive-check-before-build.json"
python3 "$FROZEN_GATE" check-source-snapshot-seal \
    --repo "$REPO_ROOT" --snapshot "$BUILD_SOURCE_DIR" --seal "$SOURCE_SNAPSHOT_SEAL" \
    >"$BUILD_DIR/source-snapshot-check-before-build.json"
note "performing one controlled read-only HEAD-snapshot release build"
set +e
(
    cd "$BUILD_SOURCE_DIR"
    env -i "${BUILD_ENV[@]}" "${BUILD_COMMAND[@]}"
) >"$BUILD_DIR/build.log" 2>&1
build_status=$?
set -e
printf '%s\n' "$build_status" >"$BUILD_DIR/build.exit-status"
((build_status == 0)) || die "controlled source-bound release build failed"
python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    >"$BUILD_DIR/source-check-after-build.json"
python3 "$FROZEN_GATE" check-source-archive-seal \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" --seal "$SOURCE_ARCHIVE_SEAL" \
    >"$BUILD_DIR/source-archive-check-after-build.json"
python3 "$FROZEN_GATE" check-source-snapshot-seal \
    --repo "$REPO_ROOT" --snapshot "$BUILD_SOURCE_DIR" --seal "$SOURCE_SNAPSHOT_SEAL" \
    >"$BUILD_DIR/source-snapshot-check-after-build.json"
BUILT_RELEASE_DIR="$BUILD_TARGET_DIR/$BUILD_TARGET_TRIPLE/release"
printf 'role\tsource\tpreserved\tsha256\n' >"$RESULT_DIR/metadata/binaries.tsv"
preserve_binary chronoxide-ingester "$BUILT_RELEASE_DIR/chronoxide-ingester"
preserve_binary chronoxide-capture-repartition \
    "$BUILT_RELEASE_DIR/chronoxide-capture-repartition"
preserve_binary chronoxide-query "$BUILT_RELEASE_DIR/chronoxide-query"
preserve_binary chronoxide-storage-verify "$BUILT_RELEASE_DIR/chronoxide-storage-verify"
RUN_INGESTER="$RESULT_DIR/metadata/binaries/chronoxide-ingester"
RUN_REPARTITION="$RESULT_DIR/metadata/binaries/chronoxide-capture-repartition"
RUN_QUERY="$RESULT_DIR/metadata/binaries/chronoxide-query"
RUN_STORAGE_VERIFY="$RESULT_DIR/metadata/binaries/chronoxide-storage-verify"
sha256sum "$RUN_INGESTER" "$RUN_REPARTITION" "$RUN_QUERY" "$RUN_STORAGE_VERIFY" \
    >"$RESULT_DIR/metadata/binaries.sha256"

assert_source_seal() {
    python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
        >/dev/null || die "formal source seal changed"
    python3 "$FROZEN_GATE" check-source-archive-seal \
        --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" --seal "$SOURCE_ARCHIVE_SEAL" \
        >/dev/null || die "formal source archive seal changed"
    python3 "$FROZEN_GATE" check-source-snapshot-seal \
        --repo "$REPO_ROOT" --snapshot "$BUILD_SOURCE_DIR" --seal "$SOURCE_SNAPSHOT_SEAL" \
        >/dev/null || die "formal read-only source snapshot seal changed"
}

assert_binary_seal() {
    sha256sum --check --strict "$RESULT_DIR/metadata/binaries.sha256" >/dev/null \
        || die "preserved binary seal changed"
}

assert_harness_seal() {
    python3 "$FROZEN_GATE" check-frozen-harness \
        --harness "$RESULT_DIR/metadata/harness" >/dev/null \
        || die "frozen harness path/type/mode allowlist changed"
    (
        cd "$RESULT_DIR/metadata/harness"
        sha256sum --check --strict ../harness.sha256 >/dev/null
    ) || die "frozen harness seal changed"
    sha256sum --check --strict "$RESULT_DIR/metadata/config-template.sha256" >/dev/null \
        || die "frozen configuration template seal changed"
    if [[ -f "$RESULT_DIR/metadata/configs.sha256" ]]; then
        sha256sum --check --strict "$RESULT_DIR/metadata/configs.sha256" >/dev/null \
            || die "rendered configuration seal changed"
    fi
    if [[ -f "$RESULT_DIR/metadata/run-plan.sha256" ]]; then
        sha256sum --check --strict "$RESULT_DIR/metadata/run-plan.sha256" >/dev/null \
            || die "run plan seal changed"
    fi
    if [[ -f "$RESULT_DIR/metadata/replay-summary.sha256" ]]; then
        sha256sum --check --strict "$RESULT_DIR/metadata/replay-summary.sha256" >/dev/null \
            || die "replay summary seal changed"
    fi
    if [[ -f "$RESULT_DIR/metadata/tools/fadvise-regular-dontneed.sha256" ]]; then
        sha256sum --check --strict \
            "$RESULT_DIR/metadata/tools/fadvise-regular-dontneed.sha256" >/dev/null \
            || die "fadvise helper seal changed"
    fi
    sha256sum --check --strict \
        "$RESULT_DIR/metadata/tools/runtime-tool-binaries.sha256" >/dev/null \
        || die "measurement tool seal changed"
}

assert_experiment_seals() {
    local context="$1"
    assert_source_seal
    assert_binary_seal
    assert_harness_seal
    printf '%s\t%s\n' "$(date --iso-8601=ns)" "$context" \
        >>"$RESULT_DIR/metadata/seal-checks.tsv"
}

record_runtime_identity() {
    local output="$1" role="$2" binary="$3"
    shift 3
    python3 "$FROZEN_GATE" runtime-identity --binary "$binary" --role "$role" \
        "$@" --output "$output"
}

record_repartition_runtime_identity() {
    local output="$1"
    shift
    local -a options=(--env=LC_ALL=C --env=TZ=UTC)
    local argument
    for argument in "$@"; do options+=("--arg=$argument"); done
    record_runtime_identity "$output" repartition "$RUN_REPARTITION" "${options[@]}"
}

record_ingester_runtime_identity() {
    local output="$1" config="$2"
    record_runtime_identity "$output" ingester "$RUN_INGESTER" \
        --env=LC_ALL=C --env=TZ=UTC --env="CONFIG_FILE=$config" \
        --env="RUST_LOG=$RUST_LOG_VALUE"
}

record_query_runtime_identity() {
    local output="$1"
    shift
    local -a options=(--env=LC_ALL=C --env=TZ=UTC)
    local argument
    for argument in "$@"; do options+=("--arg=$argument"); done
    record_runtime_identity "$output" query "$RUN_QUERY" "${options[@]}"
}

record_verifier_runtime_identity() {
    local output="$1"
    shift
    local -a options=(--env=LC_ALL=C --env=TZ=UTC)
    local argument
    for argument in "$@"; do options+=("--arg=$argument"); done
    record_runtime_identity "$output" verifier "$RUN_STORAGE_VERIFY" "${options[@]}"
}

printf 'recorded_at\tcontext\n' >"$RESULT_DIR/metadata/seal-checks.tsv"
assert_experiment_seals initial-preserved-binaries

repartition_help="$(env -i LC_ALL=C TZ=UTC "$RUN_REPARTITION" --help 2>&1)"
for flag in --input --output --report --layout --partitions --max-messages; do
    grep -Fq -- "$flag" <<<"$repartition_help" || die "repartition binary lacks $flag"
done
query_help="$(env -i LC_ALL=C TZ=UTC "$RUN_QUERY" --help 2>&1)"
for flag in --segments-dir --storage-layout --verify-readbacks --validate-segment-footers; do
    grep -Fq -- "$flag" <<<"$query_help" || die "query binary lacks $flag"
done
verify_help="$(env -i LC_ALL=C TZ=UTC "$RUN_STORAGE_VERIFY" --help 2>&1)"
for flag in --segments-dir --schema --validate-segment-footers --verify-exact-postings --decoded-semantic-fingerprint; do
    grep -Fq -- "$flag" <<<"$verify_help" || die "storage verifier lacks $flag"
done
assert_experiment_seals after-preserved-binary-help-probes

git -C "$REPO_ROOT" rev-parse HEAD >"$RESULT_DIR/metadata/source/git-revision.txt"
git -C "$REPO_ROOT" rev-parse 'HEAD^{tree}' >"$RESULT_DIR/metadata/source/git-tree.txt"
git -C "$REPO_ROOT" ls-files -s >"$RESULT_DIR/metadata/source/tracked-index.txt"
git -C "$REPO_ROOT" status --short --branch >"$RESULT_DIR/metadata/source/git-status.txt"
git -C "$REPO_ROOT" diff --binary --full-index HEAD -- \
    >"$RESULT_DIR/metadata/source/tracked.patch"
(
    cd "$REPO_ROOT"
    git ls-files -z | sort -z | xargs -0 sha256sum
) >"$RESULT_DIR/metadata/source/tracked-files.sha256"
{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'capture=%s\nconfig_template=%s\nfrozen_config_template=%s\nrepo_root=%s\nresult_dir=%s\n' \
        "$CAPTURE" "$CONFIG_TEMPLATE" "$FROZEN_CONFIG_TEMPLATE" "$REPO_ROOT" "$RESULT_DIR"
    printf 'build_source_dir=%s\nsource_archive=%s\nsource_snapshot_seal=%s\n' \
        "$BUILD_SOURCE_DIR" "$SOURCE_ARCHIVE" "$SOURCE_SNAPSHOT_SEAL"
    printf 'messages=%s\nrss_interval_ms=%s\nguard_interval_ms=%s\n' \
        "$MESSAGES" "$RSS_INTERVAL_MS" "$GUARD_INTERVAL_MS"
    printf 'rust_log_value=%s\n' "$RUST_LOG_VALUE"
    printf 'determinism_prefix_messages=%s\ndeterminism_prefix_output_bound_bytes=%s\n' \
        "$DETERMINISM_PREFIX_MESSAGES" "$DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES"
    printf 'source_capture_bytes=%s\nfull_capture_count=%s\nfull_capture_layout_overhead_bytes_each=%s\n' \
        "$source_capture_bytes" "$FULL_CAPTURE_COUNT" "$FULL_CAPTURE_LAYOUT_OVERHEAD_BYTES"
    printf 'full_capture_estimate_bytes=%s\nbounded_prefix_output_count=%s\nbounded_prefix_estimate_bytes=%s\n' \
        "$full_capture_estimate_bytes" "$BOUNDED_PREFIX_OUTPUT_COUNT" "$bounded_prefix_estimate_bytes"
    printf 'sizing_messages=%s\nsizing_safety_multiplier=%s\nsizing_corpus_count=%s\n' \
        "$SIZING_MESSAGES" "$SIZING_SAFETY_MULTIPLIER" "$SIZING_CORPUS_COUNT"
    printf 'sizing_corpus_upper_bound_bytes_each=%s\nsizing_corpus_estimate_bytes=%s\n' \
        "$sizing_corpus_upper_bound_bytes" "$sizing_corpus_estimate_bytes"
    printf 'sizing_transient_headroom_bytes=%s\n' "$sizing_transient_headroom_bytes"
    printf 'build_output_allowance_bytes=%s\nharness_overhead_bytes=%s\nsafety_reserve_bytes=%s\nestimated_outputs_bytes=%s\n' \
        "$BUILD_OUTPUT_ALLOWANCE_BYTES" "$HARNESS_OVERHEAD_BYTES" \
        "$SAFETY_RESERVE_BYTES" "$estimated_outputs_bytes"
    printf 'required_free_bytes=%s\navailable_bytes_at_plan=%s\n' \
        "$required_free_bytes" "$available_bytes"
    printf 'quiet_host_confirmed=%s\nrun_note=%s\n' "$QUIET_HOST_CONFIRMED" "$RUN_NOTE"
    printf 'mapping_uniform=destination_partition = global_ordinal %% 16\n'
    printf 'mapping_skew80_20=four of every five records to p0; every fifth round-robin p1..p15\n'
    printf 'comparators=2x2 cell order is adaptive_series_table/adaptive_last_timestamp_table: pp, ap, pa, aa\n'
    printf 'measurement_order=250k pp sizing per topology; uniform-pp seed, skew80-20-pp seed, dynamic disk gate, uniform-ap-pa-aa, skew80-20-pa-ap-aa\n'
    printf 'performance_interpretation=one unreplicated observation per cell; directional factor evidence only; production promotion forbidden\n'
} >"$RESULT_DIR/metadata/settings.txt"
{
    printf 'term\tcount\tbytes_each\tbytes_total\n'
    printf 'full_derived_capture\t%s\t%s\t%s\n' "$FULL_CAPTURE_COUNT" \
        "$((source_capture_bytes + FULL_CAPTURE_LAYOUT_OVERHEAD_BYTES))" \
        "$full_capture_estimate_bytes"
    printf 'bounded_determinism_prefix\t%s\t%s\t%s\n' "$BOUNDED_PREFIX_OUTPUT_COUNT" \
        "$DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES" "$bounded_prefix_estimate_bytes"
    printf 'schema8_sizing_corpus_upper_bound\t%s\t%s\t%s\n' "$SIZING_CORPUS_COUNT" \
        "$sizing_corpus_upper_bound_bytes" "$sizing_corpus_estimate_bytes"
    printf 'schema8_sizing_transient_headroom\t1\t%s\t%s\n' \
        "$sizing_transient_headroom_bytes" "$sizing_transient_headroom_bytes"
    printf 'controlled_build_output_allowance\t1\t%s\t%s\n' \
        "$BUILD_OUTPUT_ALLOWANCE_BYTES" "$BUILD_OUTPUT_ALLOWANCE_BYTES"
    printf 'report_log_tool_overhead\t1\t%s\t%s\n' \
        "$HARNESS_OVERHEAD_BYTES" "$HARNESS_OVERHEAD_BYTES"
    printf 'post_estimate_safety_reserve\t1\t%s\t%s\n' \
        "$SAFETY_RESERVE_BYTES" "$SAFETY_RESERVE_BYTES"
    printf 'required_free\t1\t%s\t%s\n' "$required_free_bytes" "$required_free_bytes"
    printf 'available_at_plan\t1\t%s\t%s\n' "$available_bytes" "$available_bytes"
} >"$RESULT_DIR/metadata/disk-budget.tsv"
printf '%s\n' "$RUN_NOTE" >"$RESULT_DIR/metadata/run-note.txt"
{
    date --iso-8601=seconds
    uname -a
    rustc --version --verbose 2>/dev/null || true
    cargo --version --verbose 2>/dev/null || true
    "$PERF_BIN" --version
    lscpu 2>/dev/null || true
    cat /proc/meminfo 2>/dev/null || true
    for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
        [[ -r "$pressure" ]] && { echo "$pressure"; cat "$pressure"; }
    done
} >"$RESULT_DIR/metadata/environment.txt" 2>&1
ps -eo pid=,ppid=,pcpu=,pmem=,rss=,etime=,stat=,comm=,args= \
    >"$RESULT_DIR/metadata/processes-at-plan.txt"

declare -a RUNS=(
    uniform-pp-01 skew80-20-pp-01
    uniform-ap-01 uniform-pa-01 uniform-aa-01
    skew80-20-pa-01 skew80-20-ap-01 skew80-20-aa-01
)
declare -A RUN_TOPOLOGY RUN_CELL RUN_SERIES_MODE RUN_LAST_MODE
for run in "${RUNS[@]}"; do
    run_without_suffix="${run%-01}"
    cell="${run_without_suffix##*-}"
    topology="${run%-"$cell"-01}"
    case "$cell" in
        pp) series_mode=plain; last_mode=plain ;;
        ap) series_mode=adaptive; last_mode=plain ;;
        pa) series_mode=plain; last_mode=adaptive ;;
        aa) series_mode=adaptive; last_mode=adaptive ;;
        *) die "unknown factorial cell: $cell" ;;
    esac
    RUN_TOPOLOGY[$run]="$topology"
    RUN_CELL[$run]="$cell"
    RUN_SERIES_MODE[$run]="$series_mode"
    RUN_LAST_MODE[$run]="$last_mode"
done

printf 'order\trun\ttopology\tcell\tadaptive_series_table\tadaptive_last_timestamp_table\tcapture\tconfig\tsegments\n' >"$RESULT_DIR/run-plan.tsv"
for index in "${!RUNS[@]}"; do
    run="${RUNS[$index]}"
    topology="${RUN_TOPOLOGY[$run]}"
    cell="${RUN_CELL[$run]}"
    series_mode="${RUN_SERIES_MODE[$run]}"
    last_mode="${RUN_LAST_MODE[$run]}"
    run_dir="$RESULT_DIR/runs/$run"
    mkdir "$run_dir"
    capture_path="$RESULT_DIR/captures/$topology"
    config="$RESULT_DIR/configs/$run.toml"
    segments="$run_dir/segments"
    python3 "$FROZEN_GATE" render-config \
        --template "$FROZEN_CONFIG_TEMPLATE" --output "$config" \
        --capture "$capture_path" --segments-dir "$segments" \
        --messages "$MESSAGES" --series-mode "$series_mode" \
        --last-mode "$last_mode" >"$run_dir/config-render.json"
    [[ "$series_mode" == adaptive ]] && series_flag=true || series_flag=false
    [[ "$last_mode" == adaptive ]] && last_flag=true || last_flag=false
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$((index + 1))" "$run" "$topology" "$cell" "$series_flag" "$last_flag" \
        "$capture_path" "$config" "$segments" >>"$RESULT_DIR/run-plan.tsv"
done
for topology in uniform skew80-20; do
    sizing_dir="$RESULT_DIR/sizing/$topology"
    mkdir "$sizing_dir"
    python3 "$FROZEN_GATE" render-config \
        --template "$FROZEN_CONFIG_TEMPLATE" \
        --output "$RESULT_DIR/configs/sizing-$topology.toml" \
        --capture "$RESULT_DIR/captures/$topology" \
        --segments-dir "$sizing_dir/segments" \
        --messages "$SIZING_MESSAGES" --series-mode plain --last-mode plain \
        >"$sizing_dir/config-render.json"
done
sha256sum "$RESULT_DIR/configs"/*.toml >"$RESULT_DIR/metadata/configs.sha256"
sha256sum "$RESULT_DIR/run-plan.tsv" >"$RESULT_DIR/metadata/run-plan.sha256"
python3 "$FROZEN_GATE" validate-run-plan --result-dir "$RESULT_DIR" \
    --plan "$RESULT_DIR/run-plan.tsv" \
    --output "$RESULT_DIR/comparisons/run-plan-validation.json"
assert_experiment_seals after-run-plan-and-config-rendering

if [[ "$DRY_RUN" == "1" ]]; then
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    note "dry run complete; archive-only build, provenance, CLI help probes, configs, and run plan completed; no transform, replay, perf workload, storage verification, or query evaluation was launched: $RESULT_DIR"
    exit 0
fi

active_lifecycle_dir=''
active_control=''
active_guardian_ready=''
active_rss_ready=''
active_launch=''
active_root_pid=''
active_root_starttime_ticks=''
active_guardian_pid=''
active_guardian_starttime_ticks=''
active_rss_pid=''
active_rss_starttime_ticks=''
cleanup_signal_pending=0

read_process_state_starttime_ticks() {
    local pid="$1" stat_line stat_tail
    local -a stat_fields
    IFS= read -r stat_line <"/proc/$pid/stat" || return 1
    stat_tail="${stat_line##*) }"
    read -r -a stat_fields <<<"$stat_tail"
    (( ${#stat_fields[@]} > 19 )) || return 1
    [[ "${stat_fields[0]}" =~ ^[A-Za-z]$ \
        && "${stat_fields[19]}" =~ ^[1-9][0-9]*$ ]] || return 1
    printf '%s\t%s\n' "${stat_fields[0]}" "${stat_fields[19]}"
}

read_live_starttime_ticks() {
    local identity state starttime_ticks
    identity="$(read_process_state_starttime_ticks "$1")" || return 1
    read -r state starttime_ticks <<<"$identity" || return 1
    [[ "$state" != Z && "$state" != X && "$state" != x \
        && "$starttime_ticks" =~ ^[1-9][0-9]*$ ]] || return 1
    printf '%s\n' "$starttime_ticks"
}

bind_live_starttime_ticks() {
    local pid="$1" attempt starttime_ticks
    for ((attempt = 0; attempt < 50; attempt++)); do
        if starttime_ticks="$(read_live_starttime_ticks "$pid")"; then
            printf '%s\n' "$starttime_ticks"
            return 0
        fi
        [[ -e "/proc/$pid/stat" ]] || return 1
        sleep 0.002
    done
    return 1
}

clear_active_lifecycle() {
    active_lifecycle_dir=''
    active_control=''
    active_guardian_ready=''
    active_rss_ready=''
    active_launch=''
    active_root_pid=''
    active_root_starttime_ticks=''
    active_guardian_pid=''
    active_guardian_starttime_ticks=''
    active_rss_pid=''
    active_rss_starttime_ticks=''
}

record_cleanup_reap() {
    local role="$1" status="$2" detail="$3"
    [[ -n "$active_lifecycle_dir" && -d "$active_lifecycle_dir" ]] || return 0
    printf '%s\t%s\t%s\n' "$role" "$status" "$detail" \
        >>"$active_lifecycle_dir/interrupted-cleanup-reap.tsv"
}

stop_bound_tree() {
    local role="$1" pid="$2" starttime_ticks="$3"
    [[ -n "$pid" ]] || return 0
    if [[ -z "$starttime_ticks" ]]; then
        note "refusing to signal unbound $role PID $pid"
        record_cleanup_reap "$role" unbound-signal-refused "pid=$pid"
        return 1
    fi
    python3 "$FROZEN_GUARD" terminate-tree --root-pid "$pid" \
        --root-starttime-ticks "$starttime_ticks" \
        >"$active_lifecycle_dir/interrupted-$role-termination.json" 2>&1 || true
}

bounded_reap_job() {
    local role="$1" pid="$2" expected_starttime_ticks="$3"
    local attempt state current_starttime_ticks identity
    [[ -n "$pid" ]] || return 0
    if [[ -z "$expected_starttime_ticks" ]]; then
        record_cleanup_reap "$role" unbound-wait-refused "pid=$pid"
        return 1
    fi
    for ((attempt = 0; attempt < 200; attempt++)); do
        identity="$(read_process_state_starttime_ticks "$pid")" || {
            if [[ ! -e "/proc/$pid/stat" ]]; then
                wait "$pid" 2>/dev/null || true
                record_cleanup_reap "$role" reaped-after-exit "pid=$pid"
                return 0
            fi
            record_cleanup_reap "$role" identity-read-failed "pid=$pid"
            return 1
        }
        read -r state current_starttime_ticks <<<"$identity"
        if [[ "$current_starttime_ticks" != "$expected_starttime_ticks" ]]; then
            record_cleanup_reap "$role" reused-wait-refused \
                "pid=$pid expected=$expected_starttime_ticks current=$current_starttime_ticks"
            return 1
        fi
        if [[ "$state" == Z || "$state" == X || "$state" == x ]]; then
            wait "$pid" 2>/dev/null || true
            record_cleanup_reap "$role" reaped-dead \
                "pid=$pid state=$state starttime=$current_starttime_ticks"
            return 0
        fi
        sleep 0.01
    done
    record_cleanup_reap "$role" timeout-live \
        "pid=$pid starttime=$expected_starttime_ticks"
    return 1
}

stop_active_lifecycle() {
    local controlled=0
    trap '' EXIT HUP INT TERM
    if [[ -n "$active_control" && -f "$active_control" && ! -L "$active_control" ]]; then
        if python3 "$FROZEN_GUARD" cleanup-control --control "$active_control" \
            --guardian-ready "$active_guardian_ready" --rss-ready "$active_rss_ready" \
            --launch "$active_launch" --interval-ms 100 \
            >"$active_lifecycle_dir/interrupted-controlled-cleanup.json" 2>&1; then
            controlled=1
        fi
    fi
    if [[ "$controlled" == 0 ]]; then
        stop_bound_tree root "$active_root_pid" "$active_root_starttime_ticks" || true
        stop_bound_tree rss-monitor "$active_rss_pid" "$active_rss_starttime_ticks" || true
        stop_bound_tree guardian "$active_guardian_pid" \
            "$active_guardian_starttime_ticks" || true
    fi
    bounded_reap_job root "$active_root_pid" "$active_root_starttime_ticks" || true
    bounded_reap_job rss-monitor "$active_rss_pid" "$active_rss_starttime_ticks" || true
    bounded_reap_job guardian "$active_guardian_pid" \
        "$active_guardian_starttime_ticks" || true
    clear_active_lifecycle
}

cleanup_signal_exit() {
    stop_active_lifecycle
    exit 130
}

cleanup_on_exit() {
    local status=$?
    if [[ -n "$active_root_pid" || -n "$active_guardian_pid" || -n "$active_rss_pid" ]]; then
        stop_active_lifecycle
    fi
    exit "$status"
}

defer_cleanup_signals() {
    trap 'cleanup_signal_pending=1' HUP INT TERM
}

arm_cleanup_signals() {
    trap 'cleanup_signal_exit' HUP INT TERM
    if [[ "$cleanup_signal_pending" == 1 ]]; then
        cleanup_signal_pending=0
        cleanup_signal_exit
    fi
}

trap 'cleanup_on_exit' EXIT
arm_cleanup_signals

disk_budget_blocked() {
    local message="$1"
    printf '%s\n' "$message" >"$RESULT_DIR/PARTIAL_DISK_BUDGET_BLOCKED"
    die "$message; partial result retained at $RESULT_DIR"
}

guardian_blocked() {
    local label="$1"
    local violation="$2"
    {
        printf 'label=%s\n' "$label"
        printf 'violation=%s\n' "$violation"
    } >"$RESULT_DIR/PARTIAL_MEASUREMENT_GUARD_BLOCKED"
    die "$label was stopped by the continuous disk/process guardian; partial result retained at $RESULT_DIR"
}

run_held_workload() {
    local label="$1" lifecycle_dir="$2" work_dir="$3" stdout_path="$4"
    local status_path="$5" minimum_free_bytes="$6"
    shift 6
    local -a command=("$@")
    local control="$lifecycle_dir/lifecycle-control.json"
    local guardian_ready="$lifecycle_dir/guardian-ready"
    local rss_ready="$lifecycle_dir/rss-ready"
    local launch="$lifecycle_dir/launch"
    local root_pid guardian_pid rss_pid root_status guardian_status rss_status
    local binding_failed=0 handshake output
    if [[ ! -e "$lifecycle_dir" && ! -L "$lifecycle_dir" ]]; then
        mkdir "$lifecycle_dir"
    fi
    [[ -d "$lifecycle_dir" && ! -L "$lifecycle_dir" ]] \
        || die "$label lifecycle root must be a non-symlink directory"
    for output in "$stdout_path" "$status_path"; do
        [[ ! -e "$output" && ! -L "$output" ]] \
            || die "$label refuses to reuse a workload output"
    done
    for handshake in "$control" "$guardian_ready" "$rss_ready" "$launch"; do
        [[ ! -e "$handshake" && ! -L "$handshake" ]] \
            || die "$label refuses to reuse a lifecycle handshake artifact"
    done
    python3 "$FROZEN_GUARD" scan-conflicts \
        --output "$lifecycle_dir/processes-immediately-before-launch.json" >/dev/null \
        || die "$label found a quiet-host conflict before launch"
    active_lifecycle_dir="$lifecycle_dir"
    active_control="$control"
    active_guardian_ready="$guardian_ready"
    active_rss_ready="$rss_ready"
    active_launch="$launch"
    defer_cleanup_signals
    (
        cd "$work_dir"
        while [[ ! -e "$launch" && ! -L "$launch" ]]; do sleep 0.001; done
        [[ -f "$launch" && ! -L "$launch" && ! -s "$launch" \
            && "$(stat -c '%a' -- "$launch")" == 444 ]] || exit 125
        exec "${command[@]}" >"$stdout_path" 2>&1
    ) &
    root_pid=$!
    active_root_pid="$root_pid"
    active_root_starttime_ticks="$(bind_live_starttime_ticks "$root_pid")" \
        || binding_failed=1
    arm_cleanup_signals
    ((binding_failed == 0)) \
        || { stop_active_lifecycle; die "$label held root exited before binding"; }
    binding_failed=0
    defer_cleanup_signals
    python3 "$FROZEN_GUARD" monitor-rss --root-pid "$root_pid" \
        --samples "$lifecycle_dir/rss-samples.tsv" \
        --summary "$lifecycle_dir/rss-summary.json" --control "$control" \
        --guardian-ready "$guardian_ready" --rss-ready "$rss_ready" \
        --launch "$launch" --interval-ms 100 \
        >/dev/null 2>"$lifecycle_dir/rss-monitor.log" &
    rss_pid=$!
    active_rss_pid="$rss_pid"
    active_rss_starttime_ticks="$(bind_live_starttime_ticks "$rss_pid")" \
        || binding_failed=1
    arm_cleanup_signals
    ((binding_failed == 0)) \
        || { stop_active_lifecycle; die "$label RSS monitor exited before binding"; }
    binding_failed=0
    defer_cleanup_signals
    python3 "$FROZEN_GUARD" monitor-guardian --root-pid "$root_pid" \
        --filesystem "$RESULT_DIR" --minimum-free-bytes "$minimum_free_bytes" \
        --disk-log "$lifecycle_dir/disk-guardian.tsv" \
        --process-log "$lifecycle_dir/process-guardian.tsv" \
        --violation "$lifecycle_dir/guardian-violation.json" \
        --summary "$lifecycle_dir/guardian-summary.json" --control "$control" \
        --guardian-ready "$guardian_ready" --rss-ready "$rss_ready" \
        --launch "$launch" --interval-ms 100 \
        >/dev/null 2>"$lifecycle_dir/guardian.log" &
    guardian_pid=$!
    active_guardian_pid="$guardian_pid"
    active_guardian_starttime_ticks="$(bind_live_starttime_ticks "$guardian_pid")" \
        || binding_failed=1
    arm_cleanup_signals
    ((binding_failed == 0)) \
        || { stop_active_lifecycle; die "$label guardian exited before binding"; }
    python3 "$FROZEN_GUARD" create-control --output "$control" \
        --guardian-ready "$guardian_ready" --rss-ready "$rss_ready" \
        --launch "$launch" --root-pid "$root_pid" --guardian-pid "$guardian_pid" \
        --rss-monitor-pid "$rss_pid" --interval-ms 100 >/dev/null \
        || { stop_active_lifecycle; die "$label control publication failed"; }
    python3 "$FROZEN_GUARD" wait-ready --control "$control" \
        --guardian-ready "$guardian_ready" --rss-ready "$rss_ready" \
        --launch "$launch" --interval-ms 100 --timeout-ms 5000 >/dev/null \
        || { stop_active_lifecycle; die "$label monitor readiness failed"; }
    python3 "$FROZEN_GUARD" release-launch --control "$control" \
        --guardian-ready "$guardian_ready" --rss-ready "$rss_ready" \
        --launch "$launch" --interval-ms 100 >/dev/null \
        || { stop_active_lifecycle; die "$label launch release failed"; }
    set +e
    wait "$root_pid"; root_status=$?
    wait "$rss_pid"; rss_status=$?
    wait "$guardian_pid"; guardian_status=$?
    set -e
    clear_active_lifecycle
    printf '%s\n' "$root_status" >"$status_path"
    printf '%s\n' "$rss_status" >"$lifecycle_dir/rss-monitor.exit-status"
    printf '%s\n' "$guardian_status" >"$lifecycle_dir/guardian.exit-status"
    [[ ! -f "$lifecycle_dir/guardian-violation.json" ]] \
        || guardian_blocked "$label" "$lifecycle_dir/guardian-violation.json"
    ((rss_status == 0)) \
        || guardian_blocked "$label-rss-monitor" "$lifecycle_dir/rss-monitor.log"
    ((guardian_status == 0)) \
        || guardian_blocked "$label-guardian" "$lifecycle_dir/guardian.log"
    ((root_status == 0)) || die "$label workload failed with status $root_status"
}

python3 "$FROZEN_GUARD" scan-conflicts \
    --output "$RESULT_DIR/metadata/processes-before-transforms.json" >/dev/null \
    || die "quiet-host conflict detected before generated-capture transforms"

assert_experiment_seals before-source-capture-inventory
python3 "$FROZEN_GATE" capture-inventory --capture "$CAPTURE" \
    --output "$RESULT_DIR/inventory/source-before.json" \
    --paths-output "$RESULT_DIR/inventory/source-before-files.nul"
assert_experiment_seals after-source-capture-inventory

check_conflicts() {
    local output="$1"
    ps -eo pid=,ppid=,comm=,args= >"$output"
    python3 "$FROZEN_GATE" validate-process-snapshot --snapshot "$output" >/dev/null \
        || die "measurement conflict detected"
}

snapshot_pressure() {
    local output="$1"
    {
        date --iso-8601=ns
        cat /proc/loadavg 2>/dev/null || true
        for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
            [[ -r "$pressure" ]] && { echo "$pressure"; cat "$pressure"; }
        done
    } >"$output"
}

capture_tree_bytes() {
    local path="$1"
    local bytes
    bytes="$(find "$path" -maxdepth 1 -type f -printf '%s\n' \
        | awk '{sum += $1} END {printf "%.0f", sum}')"
    [[ "$bytes" =~ ^[1-9][0-9]*$ ]] || die "could not determine capture bytes: $path"
    printf '%s\n' "$bytes"
}

writeback_capture_files() {
    local capture_path="$1"
    local purpose="$2"
    local output="$3"
    local file count=0
    while IFS= read -r -d '' file; do
        "$SYNC_BIN" -f -- "$file"
        printf '%s\t%s\t%s\t%s\n' "$purpose" "$file" \
            "$(stat -c '%s' -- "$file")" "$(date --iso-8601=ns)" >>"$output"
        count="$((count + 1))"
    done < <(find "$capture_path" -maxdepth 1 -type f -print0 | sort -z)
    ((count > 0)) || die "capture has no regular files to write back: $capture_path"
    "$SYNC_BIN" -f -- "$capture_path"
}

mkdir "$RESULT_DIR/validation/transform-guards"
printf 'order\tlabel\toutput_upper_bound_bytes\tremaining_before_bytes\tremaining_after_bytes\tavailable_before_bytes\trequired_before_bytes\tguardian_minimum_free_bytes\n' \
    >"$RESULT_DIR/validation/transform-capacity-plan.tsv"
full_capture_bound_bytes="$((source_capture_bytes + FULL_CAPTURE_LAYOUT_OVERHEAD_BYTES))"
transform_remaining_bound_bytes="$((bounded_prefix_estimate_bytes + full_capture_estimate_bytes))"
transform_base_reserve_bytes="$((sizing_corpus_estimate_bytes + sizing_transient_headroom_bytes + HARNESS_OVERHEAD_BYTES + SAFETY_RESERVE_BYTES))"
transform_order=0

note "creating bounded independent Zstd-prefix transforms for byte determinism"
printf 'topology\tvariant\tmessages\tactual_bytes\tupper_bound_bytes\n' \
    >"$RESULT_DIR/validation/determinism-prefix-sizes.tsv"
for topology in uniform skew80-20; do
    for variant in a b; do
        repartition_args=(
            --input "$CAPTURE"
            --output "$RESULT_DIR/captures/determinism/$topology-$variant"
            --report "$RESULT_DIR/validation/repartition-prefix-$topology-$variant.json"
            --layout "$topology" --partitions 16
            --max-messages "$DETERMINISM_PREFIX_MESSAGES"
        )
        record_repartition_runtime_identity \
            "$RESULT_DIR/validation/repartition-prefix-$topology-$variant.runtime-identity.json" \
            "${repartition_args[@]}"
        transform_label="prefix-$topology-$variant"
        transform_order="$((transform_order + 1))"
        transform_bound_bytes="$DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES"
        transform_remaining_after_bytes="$((transform_remaining_bound_bytes - transform_bound_bytes))"
        transform_guard_minimum_bytes="$((transform_remaining_after_bytes + transform_base_reserve_bytes))"
        transform_required_before_bytes="$((transform_bound_bytes + transform_guard_minimum_bytes))"
        transform_available_before_bytes="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 { print $1 }')"
        [[ "$transform_available_before_bytes" =~ ^[0-9]+$ ]] \
            || die "could not determine free space before $transform_label"
        ((transform_available_before_bytes >= transform_required_before_bytes)) \
            || disk_budget_blocked \
                "before $transform_label filesystem has $transform_available_before_bytes bytes; require $transform_required_before_bytes"
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$transform_order" "$transform_label" "$transform_bound_bytes" \
            "$transform_remaining_bound_bytes" "$transform_remaining_after_bytes" \
            "$transform_available_before_bytes" "$transform_required_before_bytes" \
            "$transform_guard_minimum_bytes" \
            >>"$RESULT_DIR/validation/transform-capacity-plan.tsv"
        assert_experiment_seals "before-prefix-transform-$topology-$variant"
        run_held_workload "$transform_label" \
            "$RESULT_DIR/validation/transform-guards/$transform_label" "$RESULT_DIR" \
            "$RESULT_DIR/validation/repartition-prefix-$topology-$variant.stdout.json" \
            "$RESULT_DIR/validation/transform-guards/$transform_label/workload.exit-status" \
            "$transform_guard_minimum_bytes" \
            env -i LC_ALL=C TZ=UTC "$RUN_REPARTITION" "${repartition_args[@]}"
        transform_remaining_bound_bytes="$transform_remaining_after_bytes"
        assert_experiment_seals "after-prefix-transform-$topology-$variant"
        prefix_bytes="$(capture_tree_bytes \
            "$RESULT_DIR/captures/determinism/$topology-$variant")"
        ((prefix_bytes <= DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES)) \
            || die "$topology-$variant determinism prefix uses $prefix_bytes bytes; bound is $DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES"
        printf '%s\t%s\t%s\t%s\t%s\n' "$topology" "$variant" \
            "$DETERMINISM_PREFIX_MESSAGES" "$prefix_bytes" \
            "$DETERMINISM_PREFIX_OUTPUT_BOUND_BYTES" \
            >>"$RESULT_DIR/validation/determinism-prefix-sizes.tsv"
    done
    python3 "$FROZEN_GATE" gate-repartition-repeat \
        --first "$RESULT_DIR/validation/repartition-prefix-$topology-a.json" \
        --second "$RESULT_DIR/validation/repartition-prefix-$topology-b.json" \
        --output "$RESULT_DIR/comparisons/repartition-$topology-repeat.json"
done
python3 "$FROZEN_GATE" gate-repartition \
    --uniform "$RESULT_DIR/validation/repartition-prefix-uniform-a.json" \
    --skew "$RESULT_DIR/validation/repartition-prefix-skew80-20-a.json" \
    --messages "$DETERMINISM_PREFIX_MESSAGES" \
    --output "$RESULT_DIR/comparisons/repartition-prefix-matrix.json"

note "creating one full 4M transformed capture per topology outside measurement"
printf 'topology\tmessages\tactual_bytes\tupper_bound_bytes\n' \
    >"$RESULT_DIR/validation/full-capture-sizes.tsv"
for topology in uniform skew80-20; do
    repartition_args=(
        --input "$CAPTURE"
        --output "$RESULT_DIR/captures/$topology"
        --report "$RESULT_DIR/validation/repartition-$topology.json"
        --layout "$topology" --partitions 16 --max-messages "$MESSAGES"
    )
    record_repartition_runtime_identity \
        "$RESULT_DIR/validation/repartition-$topology.runtime-identity.json" \
        "${repartition_args[@]}"
    transform_label="full-$topology"
    transform_order="$((transform_order + 1))"
    transform_bound_bytes="$full_capture_bound_bytes"
    transform_remaining_after_bytes="$((transform_remaining_bound_bytes - transform_bound_bytes))"
    transform_guard_minimum_bytes="$((transform_remaining_after_bytes + transform_base_reserve_bytes))"
    transform_required_before_bytes="$((transform_bound_bytes + transform_guard_minimum_bytes))"
    transform_available_before_bytes="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 { print $1 }')"
    [[ "$transform_available_before_bytes" =~ ^[0-9]+$ ]] \
        || die "could not determine free space before $transform_label"
    ((transform_available_before_bytes >= transform_required_before_bytes)) \
        || disk_budget_blocked \
            "before $transform_label filesystem has $transform_available_before_bytes bytes; require $transform_required_before_bytes"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$transform_order" "$transform_label" "$transform_bound_bytes" \
        "$transform_remaining_bound_bytes" "$transform_remaining_after_bytes" \
        "$transform_available_before_bytes" "$transform_required_before_bytes" \
        "$transform_guard_minimum_bytes" \
        >>"$RESULT_DIR/validation/transform-capacity-plan.tsv"
    assert_experiment_seals "before-full-transform-$topology"
    run_held_workload "$transform_label" \
        "$RESULT_DIR/validation/transform-guards/$transform_label" "$RESULT_DIR" \
        "$RESULT_DIR/validation/repartition-$topology.stdout.json" \
        "$RESULT_DIR/validation/transform-guards/$transform_label/workload.exit-status" \
        "$transform_guard_minimum_bytes" \
        env -i LC_ALL=C TZ=UTC "$RUN_REPARTITION" "${repartition_args[@]}"
    transform_remaining_bound_bytes="$transform_remaining_after_bytes"
    assert_experiment_seals "after-full-transform-$topology"
    full_capture_bytes="$(capture_tree_bytes "$RESULT_DIR/captures/$topology")"
    ((full_capture_bytes <= full_capture_bound_bytes)) \
        || die "$topology full capture uses $full_capture_bytes bytes; bound is $full_capture_bound_bytes"
    printf '%s\t%s\t%s\t%s\n' "$topology" "$MESSAGES" \
        "$full_capture_bytes" "$full_capture_bound_bytes" \
        >>"$RESULT_DIR/validation/full-capture-sizes.tsv"
done
[[ "$transform_remaining_bound_bytes" == "0" ]] \
    || die "generated-capture transform reserve did not drain exactly"
python3 "$FROZEN_GATE" gate-repartition \
    --uniform "$RESULT_DIR/validation/repartition-uniform.json" \
    --skew "$RESULT_DIR/validation/repartition-skew80-20.json" \
    --messages "$MESSAGES" --output "$RESULT_DIR/comparisons/repartition-matrix.json"

assert_experiment_seals before-derived-capture-inventories
for capture_label in uniform skew80-20 \
    determinism-uniform-a determinism-uniform-b \
    determinism-skew80-20-a determinism-skew80-20-b; do
    if [[ "$capture_label" == determinism-* ]]; then
        capture_path="$RESULT_DIR/captures/determinism/${capture_label#determinism-}"
    else
        capture_path="$RESULT_DIR/captures/$capture_label"
    fi
    python3 "$FROZEN_GATE" capture-inventory --capture "$capture_path" \
        --output "$RESULT_DIR/inventory/$capture_label-before-runs.json" \
        --paths-output "$RESULT_DIR/inventory/$capture_label-before-runs-files.nul"
done
python3 "$FROZEN_GATE" capture-inventory --capture "$CAPTURE" \
    --output "$RESULT_DIR/inventory/source-after-transforms.json" \
    --paths-output "$RESULT_DIR/inventory/source-after-transforms-files.nul"
cmp -s "$RESULT_DIR/inventory/source-before.json" \
    "$RESULT_DIR/inventory/source-after-transforms.json" \
    || die "source capture content changed during topology transforms"
cmp -s "$RESULT_DIR/inventory/source-before-files.nul" \
    "$RESULT_DIR/inventory/source-after-transforms-files.nul" \
    || die "source capture path set changed during topology transforms"
assert_experiment_seals after-derived-capture-inventories

note "forcing generated capture writeback outside replay timing"
printf 'purpose\tfile\tbytes\twriteback_completed_at\n' \
    >"$RESULT_DIR/validation/generated-capture-writeback.tsv"
for topology in uniform skew80-20; do
    for variant in a b; do
        writeback_capture_files \
            "$RESULT_DIR/captures/determinism/$topology-$variant" \
            "determinism-$topology-$variant" \
            "$RESULT_DIR/validation/generated-capture-writeback.tsv"
    done
    writeback_capture_files "$RESULT_DIR/captures/$topology" "full-$topology" \
        "$RESULT_DIR/validation/generated-capture-writeback.tsv"
done

env -i PATH="$PATH" LC_ALL=C TZ=UTC "$CC_BIN" -O2 -Wall -Wextra -Werror \
    -o "$RESULT_DIR/metadata/tools/fadvise-regular-dontneed" \
    "$RESULT_DIR/metadata/harness/fadvise_regular_dontneed.c"
sha256sum "$RESULT_DIR/metadata/tools/fadvise-regular-dontneed" \
    >"$RESULT_DIR/metadata/tools/fadvise-regular-dontneed.sha256"
assert_experiment_seals after-fadvise-tool-build
PERF_EVENTS="task-clock,cycles,instructions,branches,branch-misses,cache-references,cache-misses,page-faults,context-switches,cpu-migrations"
"$PERF_BIN" stat --no-big-num --field-separator $'\t' --event "$PERF_EVENTS" \
    --output "$RESULT_DIR/metadata/perf-preflight.tsv" -- \
    "$PYTHON_BIN" -B -I -S -c 'sum(range(10000000))'
IFS=, read -r -a preflight_events <<<"$PERF_EVENTS"
preflight_args=()
for event in "${preflight_events[@]}"; do preflight_args+=(--require-event "$event"); done
python3 "$FROZEN_PHASE1_GATE" parse-perf-stat \
    --input "$RESULT_DIR/metadata/perf-preflight.tsv" \
    --output "$RESULT_DIR/metadata/perf-preflight.json" \
    "${preflight_args[@]}" >/dev/null

note "running untimed bounded 250k topology sizing before formal measurements"
printf 'topology\tmessages\tcorpus_bytes\tmax_chunks_bytes\tfull_scale\tsafety_multiplier\tformal_corpus_upper_bound_bytes\ttransient_rewrite_upper_bound_bytes\n' \
    >"$RESULT_DIR/comparisons/topology-sizing.tsv"
declare -A TOPOLOGY_CORPUS_BOUND TOPOLOGY_TRANSIENT_BOUND
sizing_replays_remaining=2
for topology in uniform skew80-20; do
    sizing_dir="$RESULT_DIR/sizing/$topology"
    available_before="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 { print $1 }')"
    required_before="$((
        sizing_replays_remaining * sizing_corpus_upper_bound_bytes
        + sizing_transient_headroom_bytes
        + HARNESS_OVERHEAD_BYTES
        + SAFETY_RESERVE_BYTES
    ))"
    sizing_guard_minimum="$((
        (sizing_replays_remaining - 1) * sizing_corpus_upper_bound_bytes
        + HARNESS_OVERHEAD_BYTES
        + SAFETY_RESERVE_BYTES
    ))"
    {
        printf 'topology\tavailable_before_bytes\trequired_before_bytes\tguardian_minimum_free_bytes\tsizing_replays_remaining\tcorpus_upper_bound_bytes\ttransient_headroom_bytes\tharness_overhead_bytes\tsafety_reserve_bytes\n'
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$topology" "$available_before" "$required_before" \
            "$sizing_guard_minimum" "$sizing_replays_remaining" \
            "$sizing_corpus_upper_bound_bytes" "$sizing_transient_headroom_bytes" \
            "$HARNESS_OVERHEAD_BYTES" "$SAFETY_RESERVE_BYTES"
    } >"$sizing_dir/disk-budget-before.tsv"
    ((available_before >= required_before)) \
        || disk_budget_blocked \
            "before $topology sizing filesystem has $available_before bytes; require $required_before"
    check_conflicts "$sizing_dir/processes-before.txt"
    record_ingester_runtime_identity "$sizing_dir/runtime-identity.json" \
        "$RESULT_DIR/configs/sizing-$topology.toml"
    assert_experiment_seals "before-sizing-$topology"
    run_held_workload "sizing-$topology" "$sizing_dir" "$sizing_dir" \
        "$sizing_dir/replay.log" "$sizing_dir/replay.exit-status" \
        "$sizing_guard_minimum" \
        env -i LC_ALL=C TZ=UTC \
            CONFIG_FILE="$RESULT_DIR/configs/sizing-$topology.toml" \
            RUST_LOG="$RUST_LOG_VALUE" "$RUN_INGESTER"
    assert_experiment_seals "after-sizing-$topology"
    check_conflicts "$sizing_dir/processes-after.txt"
    mapfile -d '' -t sizing_reports < <(
        find "$sizing_dir" -maxdepth 1 -type f -name 'ingestion_stats_*.md' -print0
    )
    ((${#sizing_reports[@]} == 1)) \
        || die "$topology sizing replay must produce exactly one ingestion report"
    python3 "$FROZEN_REPORT_GATE" replay-report --report "${sizing_reports[0]}" \
        --output "$sizing_dir/replay-correctness.json"
    python3 "$FROZEN_GATE" parse-head-report --report "${sizing_reports[0]}" \
        --output "$sizing_dir/head-structure.json"
    python3 "$FROZEN_PHASE1_GATE" tree-manifest --corpus "$sizing_dir/segments" \
        --manifest "$sizing_dir/segments.sha256" --inventory "$sizing_dir/segments.tsv" \
        --summary "$sizing_dir/corpus-summary.json" >/dev/null
    printf '%s\n' \
        '{"perf_enabled":false,"reason":"untimed_capacity_sizing","schema":"chronoxide/head-topology-performance-disabled/v1"}' \
        >"$sizing_dir/performance-disabled.json"
    sizing_bytes="$(find "$sizing_dir/segments" -type f -printf '%s\n' \
        | awk '{sum += $1} END {printf "%.0f", sum}')"
    [[ "$sizing_bytes" =~ ^[1-9][0-9]*$ ]] \
        || die "$topology sizing corpus bytes are invalid"
    ((sizing_bytes <= sizing_corpus_upper_bound_bytes)) \
        || disk_budget_blocked \
            "$topology sizing corpus uses $sizing_bytes bytes; bound is $sizing_corpus_upper_bound_bytes"
    max_chunks_bytes="$(find "$sizing_dir/segments" -type f -name chunks.bin -printf '%s\n' \
        | sort -nr | awk 'NR == 1 { print; exit }')"
    [[ "$max_chunks_bytes" =~ ^[1-9][0-9]*$ ]] \
        || die "$topology sizing corpus has no nonempty chunks.bin"
    full_scale="$(((MESSAGES + SIZING_MESSAGES - 1) / SIZING_MESSAGES))"
    formal_bound="$((sizing_bytes * full_scale * SIZING_SAFETY_MULTIPLIER))"
    transient_bound="$((max_chunks_bytes * full_scale * SIZING_SAFETY_MULTIPLIER))"
    ((transient_bound >= 1073741824)) || transient_bound=1073741824
    TOPOLOGY_CORPUS_BOUND[$topology]="$formal_bound"
    TOPOLOGY_TRANSIENT_BOUND[$topology]="$transient_bound"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$topology" "$SIZING_MESSAGES" "$sizing_bytes" "$max_chunks_bytes" \
        "$full_scale" "$SIZING_SAFETY_MULTIPLIER" "$formal_bound" \
        "$transient_bound" >>"$RESULT_DIR/comparisons/topology-sizing.tsv"
    sizing_replays_remaining="$((sizing_replays_remaining - 1))"
done
transient_rewrite_headroom_bytes="${TOPOLOGY_TRANSIENT_BOUND[uniform]}"
skew_transient_rewrite_headroom_bytes="${TOPOLOGY_TRANSIENT_BOUND[skew80-20]}"
if ((skew_transient_rewrite_headroom_bytes > transient_rewrite_headroom_bytes)); then
    transient_rewrite_headroom_bytes="$skew_transient_rewrite_headroom_bytes"
fi
touch "$RESULT_DIR/SIZING_GATE_PASSED"

prepare_capture() {
    local capture_path="$1"
    local writeback="$2"
    local residency="$3"
    local file
    printf 'file\tbytes\twriteback_completed_at\n' >"$writeback"
    while IFS= read -r -d '' file; do
        "$SYNC_BIN" -f -- "$file"
        printf '%s\t%s\t%s\n' "$file" "$(stat -c '%s' -- "$file")" \
            "$(date --iso-8601=ns)" >>"$writeback"
    done < <(find "$capture_path" -maxdepth 1 -type f -print0 | sort -z)
    "$SYNC_BIN" -f -- "$capture_path"
    while IFS= read -r -d '' file; do
        "$RESULT_DIR/metadata/tools/fadvise-regular-dontneed" "$file"
    done < <(find "$capture_path" -maxdepth 1 -type f -name '*.capture' -print0 | sort -z)
    : >"$residency"
    while IFS= read -r -d '' file; do
        "$FINCORE_BIN" --bytes --noheadings --output RES,SIZE,FILE -- "$file" >>"$residency"
    done < <(find "$capture_path" -maxdepth 1 -type f -name '*.capture' -print0 | sort -z)
    resident="$(awk '{sum += $1} END {printf "%.0f", sum}' "$residency")"
    [[ "$resident" == "0" ]] || die "capture retained $resident resident bytes after eviction"
}

printf 'run\ttopology\tcell\tadaptive_series_table\tadaptive_last_timestamp_table\telapsed\tuser_seconds\tsystem_seconds\tmax_rss_kib\tproc_peak_rss_kib\tcorpus_files\tcorpus_bytes\tmanifest_sha256\n' \
    >"$RESULT_DIR/replay-summary.tsv"
capacity_stage=seed
seed_replays_remaining=2
uniform_replays_remaining=4
skew_replays_remaining=4
uniform_seed_bytes=0
skew_seed_bytes=0

run_replay() {
    local run="$1"
    local topology="${RUN_TOPOLOGY[$run]}"
    local cell="${RUN_CELL[$run]}"
    local series_mode="${RUN_SERIES_MODE[$run]}"
    local last_mode="${RUN_LAST_MODE[$run]}"
    local series_flag last_flag
    local run_dir="$RESULT_DIR/runs/$run"
    local capture_path="$RESULT_DIR/captures/$topology"
    local report corpus_size expected_corpus_size current_corpus_bound guard_minimum_free_bytes
    local available_before required_before uniform_corpus_bound skew_corpus_bound
    local -a replay_command
    available_before="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 { print $1 }')"
    [[ "$available_before" =~ ^[0-9]+$ ]] || die "could not determine free space before $run"
    current_corpus_bound="${TOPOLOGY_CORPUS_BOUND[$topology]}"
    uniform_corpus_bound="${TOPOLOGY_CORPUS_BOUND[uniform]}"
    skew_corpus_bound="${TOPOLOGY_CORPUS_BOUND[skew80-20]}"
    required_before="$((
        uniform_replays_remaining * uniform_corpus_bound
        + skew_replays_remaining * skew_corpus_bound
        + transient_rewrite_headroom_bytes
        + HARNESS_OVERHEAD_BYTES
        + SAFETY_RESERVE_BYTES
    ))"
    guard_minimum_free_bytes="$((
        required_before - current_corpus_bound - transient_rewrite_headroom_bytes
    ))"
    {
        printf 'stage\tavailable_bytes\trequired_bytes\tguard_minimum_free_bytes\tseed_replays_remaining\tuniform_replays_remaining\tuniform_corpus_bound_bytes\tskew_replays_remaining\tskew_corpus_bound_bytes\ttransient_rewrite_headroom_bytes\tharness_overhead_bytes\tsafety_reserve_bytes\n'
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$capacity_stage" "$available_before" "$required_before" \
            "$guard_minimum_free_bytes" "$seed_replays_remaining" \
            "$uniform_replays_remaining" "${TOPOLOGY_CORPUS_BOUND[uniform]}" \
            "$skew_replays_remaining" "${TOPOLOGY_CORPUS_BOUND[skew80-20]}" \
            "$transient_rewrite_headroom_bytes" \
            "$HARNESS_OVERHEAD_BYTES" "$SAFETY_RESERVE_BYTES"
    } >"$run_dir/disk-budget-before.tsv"
    ((available_before >= required_before)) \
        || disk_budget_blocked \
            "before $run filesystem has $available_before bytes; remaining modeled work plus reserve requires $required_before"
    check_conflicts "$run_dir/processes-before.txt"
    snapshot_pressure "$run_dir/pressure-before.txt"
    prepare_capture "$capture_path" "$run_dir/capture-writeback-before.tsv" \
        "$run_dir/capture-residency-before.tsv"
    record_ingester_runtime_identity "$run_dir/runtime-identity.json" \
        "$RESULT_DIR/configs/$run.toml"
    assert_experiment_seals "$run-before-replay"
    note "running $run"
    replay_command=(
        env LC_ALL=C "$TIME_BIN" -v -o "$run_dir/replay.time.txt"
        "$PERF_BIN" stat --no-big-num --field-separator $'\t' --event "$PERF_EVENTS"
        --output "$run_dir/perf-stat.tsv" -- env -i LC_ALL=C TZ=UTC
        CONFIG_FILE="$RESULT_DIR/configs/$run.toml" RUST_LOG="$RUST_LOG_VALUE"
        "$RUN_INGESTER"
    )
    run_held_workload "$run" "$run_dir" "$run_dir" "$run_dir/replay.log" \
        "$run_dir/replay.exit-status" "$guard_minimum_free_bytes" \
        "${replay_command[@]}"
    assert_experiment_seals "$run-after-replay"
    snapshot_pressure "$run_dir/pressure-after.txt"
    check_conflicts "$run_dir/processes-after.txt"
    python3 "$FROZEN_PHASE1_GATE" parse-time --input "$run_dir/replay.time.txt" \
        --output "$run_dir/replay.time.json" >/dev/null
    IFS=, read -r -a event_names <<<"$PERF_EVENTS"
    perf_args=()
    for event in "${event_names[@]}"; do perf_args+=(--require-event "$event"); done
    python3 "$FROZEN_PHASE1_GATE" parse-perf-stat --input "$run_dir/perf-stat.tsv" \
        --output "$run_dir/perf-stat.json" "${perf_args[@]}" >/dev/null
    mapfile -d '' -t reports < <(find "$run_dir" -maxdepth 1 -type f -name 'ingestion_stats_*.md' -print0)
    ((${#reports[@]} == 1)) || die "$run must produce exactly one ingestion report"
    report="${reports[0]}"
    python3 "$FROZEN_REPORT_GATE" replay-report --report "$report" \
        --output "$run_dir/replay-correctness.json"
    python3 "$FROZEN_PHASE1_GATE" gate-correctness --actual "$run_dir/replay-correctness.json" \
        --expectations "$FROZEN_EXPECTATIONS"
    python3 "$FROZEN_GATE" parse-head-report --report "$report" \
        --output "$run_dir/head-structure.json"
    python3 "$FROZEN_PHASE1_GATE" tree-manifest --corpus "$run_dir/segments" \
        --manifest "$run_dir/segments.sha256" --inventory "$run_dir/segments.tsv" \
        --summary "$run_dir/corpus-summary.json" >/dev/null
    [[ "$series_mode" == adaptive ]] && series_flag=true || series_flag=false
    [[ "$last_mode" == adaptive ]] && last_flag=true || last_flag=false
    python3 - "$run" "$topology" "$cell" "$series_flag" "$last_flag" \
        "$run_dir" >>"$RESULT_DIR/replay-summary.tsv" <<'PY'
import json, sys
run, topology, cell, series_adaptive, last_adaptive, root = sys.argv[1:]
time = json.load(open(root + "/replay.time.json"))
rss = json.load(open(root + "/rss-summary.json"))
corpus = json.load(open(root + "/corpus-summary.json"))
values = [
    run,
    topology,
    cell,
    series_adaptive,
    last_adaptive,
    time["elapsed"],
    time["user_seconds"],
    time["system_seconds"],
    time["max_rss_kib"],
    rss["aggregate_rss_kib"],
    corpus["file_count"],
    corpus["size_bytes"],
    corpus["manifest_sha256"],
]
print("\t".join(map(str, values)))
PY
    corpus_size="$(python3 - "$run_dir/corpus-summary.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["size_bytes"])
PY
)"
    [[ "$corpus_size" =~ ^[1-9][0-9]*$ ]] || die "$run corpus size is invalid"
    if [[ "$capacity_stage" == "seed" ]]; then
        ((corpus_size <= current_corpus_bound)) \
            || disk_budget_blocked \
                "$run seed corpus uses $corpus_size bytes; sizing-derived upper bound is $current_corpus_bound"
        if [[ "$topology" == "uniform" ]]; then
            uniform_seed_bytes="$corpus_size"
            TOPOLOGY_CORPUS_BOUND[uniform]="$corpus_size"
        else
            skew_seed_bytes="$corpus_size"
            TOPOLOGY_CORPUS_BOUND[skew80-20]="$corpus_size"
        fi
        seed_replays_remaining="$((seed_replays_remaining - 1))"
    else
        if [[ "$topology" == "uniform" ]]; then
            expected_corpus_size="$uniform_seed_bytes"
        else
            expected_corpus_size="$skew_seed_bytes"
        fi
        [[ "$corpus_size" == "$expected_corpus_size" ]] \
            || die "$run corpus uses $corpus_size bytes; same-topology seed uses $expected_corpus_size"
    fi
    if [[ "$topology" == "uniform" ]]; then
        uniform_replays_remaining="$((uniform_replays_remaining - 1))"
    else
        skew_replays_remaining="$((skew_replays_remaining - 1))"
    fi
}

run_replay uniform-pp-01
run_replay skew80-20-pp-01
[[ "$seed_replays_remaining" == "0" && "$uniform_seed_bytes" != "0" \
    && "$skew_seed_bytes" != "0" ]] || die "both seed corpora must complete"
dynamic_available_bytes="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 { print $1 }')"
[[ "$dynamic_available_bytes" =~ ^[0-9]+$ ]] \
    || die "could not determine free space after seed replays"
dynamic_remaining_corpus_bytes="$((3 * uniform_seed_bytes + 3 * skew_seed_bytes))"
dynamic_required_free_bytes="$((
    dynamic_remaining_corpus_bytes + transient_rewrite_headroom_bytes
    + HARNESS_OVERHEAD_BYTES + SAFETY_RESERVE_BYTES
))"
{
    printf 'uniform_seed_bytes\tskew_seed_bytes\tuniform_remaining\tskew_remaining\tremaining_corpus_bytes\ttransient_rewrite_headroom_bytes\tharness_overhead_bytes\tsafety_reserve_bytes\trequired_free_bytes\tavailable_bytes\n'
    printf '%s\t%s\t3\t3\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$uniform_seed_bytes" "$skew_seed_bytes" "$dynamic_remaining_corpus_bytes" \
        "$transient_rewrite_headroom_bytes" \
        "$HARNESS_OVERHEAD_BYTES" "$SAFETY_RESERVE_BYTES" \
        "$dynamic_required_free_bytes" "$dynamic_available_bytes"
} >"$RESULT_DIR/comparisons/seed-dynamic-disk-budget.tsv"
((dynamic_available_bytes >= dynamic_required_free_bytes)) \
    || disk_budget_blocked \
        "after seed replays filesystem has $dynamic_available_bytes bytes; six same-topology copies plus overhead/reserve require $dynamic_required_free_bytes"
touch "$RESULT_DIR/SEED_CAPACITY_GATE_PASSED"
capacity_stage=dynamic
for run in uniform-ap-01 uniform-pa-01 uniform-aa-01 \
    skew80-20-pa-01 skew80-20-ap-01 skew80-20-aa-01; do
    run_replay "$run"
done
[[ "$uniform_replays_remaining" == "0" && "$skew_replays_remaining" == "0" ]] \
    || die "formal replay accounting did not reach zero"
python3 "$FROZEN_GATE" validate-replay-summary \
    --summary "$RESULT_DIR/replay-summary.tsv" \
    --output "$RESULT_DIR/comparisons/replay-summary-validation.json"
sha256sum "$RESULT_DIR/replay-summary.tsv" \
    >"$RESULT_DIR/metadata/replay-summary.sha256"
assert_experiment_seals after-replay-summary-validation

for topology in uniform skew80-20; do
    baseline="$RESULT_DIR/runs/$topology-pp-01"
    for suffix in ap-01 pa-01 aa-01; do
        candidate="$RESULT_DIR/runs/$topology-$suffix"
        cmp -s "$baseline/segments.sha256" "$candidate/segments.sha256" \
            || { diff -u "$baseline/segments.sha256" "$candidate/segments.sha256" >"$RESULT_DIR/comparisons/$topology-$suffix-segments.diff" || true; die "$topology $suffix corpus differs"; }
        cmp -s "$baseline/replay-correctness.json" "$candidate/replay-correctness.json" \
            || die "$topology $suffix replay counters differ"
    done
done
python3 "$FROZEN_GATE" gate-matrix \
    --uniform-pp "$RESULT_DIR/runs/uniform-pp-01/head-structure.json" \
    --uniform-ap "$RESULT_DIR/runs/uniform-ap-01/head-structure.json" \
    --uniform-pa "$RESULT_DIR/runs/uniform-pa-01/head-structure.json" \
    --uniform-aa "$RESULT_DIR/runs/uniform-aa-01/head-structure.json" \
    --skew-pp "$RESULT_DIR/runs/skew80-20-pp-01/head-structure.json" \
    --skew-ap "$RESULT_DIR/runs/skew80-20-ap-01/head-structure.json" \
    --skew-pa "$RESULT_DIR/runs/skew80-20-pa-01/head-structure.json" \
    --skew-aa "$RESULT_DIR/runs/skew80-20-aa-01/head-structure.json" \
    --output "$RESULT_DIR/comparisons/head-structure-matrix.json"
for topology in uniform skew80-20; do
    corpus="$RESULT_DIR/runs/$topology-aa-01/segments"
    validation="$RESULT_DIR/validation/$topology"
    mkdir "$validation"
    note "running separate footer/postings validation for $topology"
    verifier_args=(
        --segments-dir "$corpus" --schema schema8
        --validate-segment-footers --verify-exact-postings
        --decoded-semantic-fingerprint
    )
    record_verifier_runtime_identity "$validation/storage-verify.runtime-identity.json" \
        "${verifier_args[@]}"
    assert_experiment_seals "$topology-before-storage-verifier"
    env -i LC_ALL=C TZ=UTC "$RUN_STORAGE_VERIFY" "${verifier_args[@]}" \
        >"$validation/storage-verify.json" 2>"$validation/storage-verify.log"
    assert_experiment_seals "$topology-after-storage-verifier"
    note "running separate independent readbacks for $topology"
    query_args=(
        --segments-dir "$corpus" --storage-layout schema8
        --sample-limit-per-kind 2 --verify-readbacks
        --output "$validation/readbacks.md"
    )
    record_query_runtime_identity "$validation/readbacks.runtime-identity.json" \
        "${query_args[@]}"
    assert_experiment_seals "$topology-before-readbacks"
    env -i LC_ALL=C TZ=UTC "$RUN_QUERY" "${query_args[@]}" \
        >"$validation/readbacks.log" 2>&1
    assert_experiment_seals "$topology-after-readbacks"
    python3 "$FROZEN_PHASE1_GATE" gate-readbacks --report "$validation/readbacks.md" \
        --expectations "$FROZEN_EXPECTATIONS" --output "$validation/readbacks.json" >/dev/null
done
python3 "$FROZEN_GATE" gate-storage-validation \
    --uniform "$RESULT_DIR/validation/uniform/storage-verify.json" \
    --skew "$RESULT_DIR/validation/skew80-20/storage-verify.json" \
    --expectations "$FROZEN_EXPECTATIONS" \
    --output "$RESULT_DIR/comparisons/storage-validation.json"
assert_experiment_seals before-capture-after-runs-inventories
python3 "$FROZEN_GATE" capture-inventory --capture "$CAPTURE" \
    --output "$RESULT_DIR/inventory/source-capture-after-runs.json" \
    --paths-output "$RESULT_DIR/inventory/source-capture-after-runs-files.nul"
cmp -s "$RESULT_DIR/inventory/source-before.json" \
    "$RESULT_DIR/inventory/source-capture-after-runs.json" \
    || die "source capture content changed after its initial inventory"
cmp -s "$RESULT_DIR/inventory/source-before-files.nul" \
    "$RESULT_DIR/inventory/source-capture-after-runs-files.nul" \
    || die "source capture path set changed after its initial inventory"
for capture_label in uniform skew80-20 \
    determinism-uniform-a determinism-uniform-b \
    determinism-skew80-20-a determinism-skew80-20-b; do
    if [[ "$capture_label" == determinism-* ]]; then
        capture_path="$RESULT_DIR/captures/determinism/${capture_label#determinism-}"
    else
        capture_path="$RESULT_DIR/captures/$capture_label"
    fi
    python3 "$FROZEN_GATE" capture-inventory --capture "$capture_path" \
        --output "$RESULT_DIR/inventory/$capture_label-capture-after-runs.json" \
        --paths-output "$RESULT_DIR/inventory/$capture_label-capture-after-runs-files.nul"
    cmp -s "$RESULT_DIR/inventory/$capture_label-before-runs.json" \
        "$RESULT_DIR/inventory/$capture_label-capture-after-runs.json" \
        || die "$capture_label capture content changed during the formal run"
    cmp -s "$RESULT_DIR/inventory/$capture_label-before-runs-files.nul" \
        "$RESULT_DIR/inventory/$capture_label-capture-after-runs-files.nul" \
        || die "$capture_label capture path set changed during the formal run"
done
assert_experiment_seals after-capture-after-runs-inventories

python3 "$FROZEN_GATE" gate-performance --result-dir "$RESULT_DIR" \
    --output "$RESULT_DIR/comparisons/performance-decision.json"
performance_disposition="$(python3 - "$RESULT_DIR/comparisons/performance-decision.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["overall_disposition"])
PY
)"
[[ "$performance_disposition" == defer ]] \
    || die "unreplicated factorial gate emitted a promotable disposition"
touch "$RESULT_DIR/PERFORMANCE_DEFER"

check_conflicts "$RESULT_DIR/metadata/processes-before-final-seal.txt"
assert_experiment_seals before-final-artifact-seal
python3 "$FROZEN_GATE" check-source-seal \
    --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    >"$BUILD_DIR/source-check-final.json"
python3 "$FROZEN_GATE" check-source-archive-seal \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" --seal "$SOURCE_ARCHIVE_SEAL" \
    >"$BUILD_DIR/source-archive-check-final.json"
python3 "$FROZEN_GATE" check-source-snapshot-seal \
    --repo "$REPO_ROOT" --snapshot "$BUILD_SOURCE_DIR" --seal "$SOURCE_SNAPSHOT_SEAL" \
    >"$BUILD_DIR/source-snapshot-check-final.json"
FINAL_ARTIFACT_PATHS="$RESULT_DIR/build-state/final-artifact-paths.nul"
(
    cd "$RESULT_DIR"
    find metadata configs validation comparisons inventory sizing runs -type f \
        ! -path metadata/result-artifacts.sha256 -print0
) >"$FINAL_ARTIFACT_PATHS" \
    || die "could not enumerate the complete formal artifact matrix"
printf 'run-plan.tsv\0replay-summary.tsv\0' >>"$FINAL_ARTIFACT_PATHS"
(
    cd "$RESULT_DIR"
    sort -z -- "$FINAL_ARTIFACT_PATHS" | xargs -0 -r sha256sum --
) >"$RESULT_DIR/metadata/result-artifacts.sha256" \
    || die "could not hash the complete formal artifact matrix"
assert_source_seal
assert_binary_seal
assert_harness_seal
final_seal_validation="$(
    python3 "$FROZEN_GATE" validate-final-seal --stage evidence --result-dir "$RESULT_DIR"
)"
printf '%s\n' "$final_seal_validation" >"$RESULT_DIR/FINAL_SEAL_VALIDATED"
printf 'chronoxide/head-topology-complete/v1\n' >"$RESULT_DIR/COMPLETE"
python3 "$FROZEN_GATE" validate-final-seal --stage complete --result-dir "$RESULT_DIR" \
    >/dev/null
note "complete: $RESULT_DIR"
