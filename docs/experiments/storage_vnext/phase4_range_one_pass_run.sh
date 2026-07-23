#!/usr/bin/env bash

# Same-binary Phase 4 diagnostic comparator. Four counterbalanced blocks give
# each execution arm eight fresh processes per query. Every process records one
# cold and two warm evaluations. The artifact is deliberately non-promotable:
# one-pass-assume-scalar preallocation is not governed, finite QueryLimits are
# disabled, and this corpus supplies no dense 24-hour event-time evidence.

set -euo pipefail
export LC_ALL=C
export PYTHONDONTWRITEBYTECODE=1
export PYTHONNOUSERSITE=1
PYTHON_BIN="$(realpath -e /usr/bin/python3)"
[[ -f "$PYTHON_BIN" && -x "$PYTHON_BIN" && ! -L "$PYTHON_BIN" ]] || {
    echo "Phase 4 range one-pass-assume-scalar: pinned Python is unavailable: $PYTHON_BIN" >&2
    exit 2
}
python3() {
    "$PYTHON_BIN" -I -S -B "$@"
}

# A Bash function launched with `&` can keep a wrapper shell as `$!` and start
# the interpreter as its child.  Guardian controls bind `$!` by PID, parent,
# and start time, so the long-lived monitor must replace any wrapper instead
# of running beneath it.
python3_background() {
    exec "$PYTHON_BIN" -I -S -B "$@"
}

verify_background_python_pid_binding() {
    local probe observed_pid bound_pid probe_status
    probe="$({
        python3_background -c \
            'import os,sys; sys.stdout.write(str(os.getpid())); sys.stdout.flush()' &
        bound_pid=$!
        if wait "$bound_pid"; then
            probe_status=0
        else
            probe_status=$?
        fi
        printf '\t%s\t%s\n' "$bound_pid" "$probe_status"
    })"
    IFS=$'\t' read -r observed_pid bound_pid probe_status <<<"$probe"
    [[ "$observed_pid" =~ ^[1-9][0-9]*$ \
        && "$bound_pid" =~ ^[1-9][0-9]*$ \
        && "$probe_status" == 0 \
        && "$observed_pid" == "$bound_pid" ]] \
        || die "background Python PID binding probe failed: observed=$observed_pid bound=$bound_pid status=$probe_status"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
QUERY_MANIFEST_DEFAULT="$SCRIPT_DIR/phase4_range_one_pass_queries.json"

DEFAULT_SEGMENTS_DIR="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase1-4m-20260721T051609Z/runs/replay-01/segments"
DEFAULT_RESULT_PARENT="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide"

DRY_RUN="${DRY_RUN:-0}"
BENCHMARK_REPEATS=3
BLOCKS=4
PROCESSES_PER_ARM_PER_QUERY=8
QUERY_LABEL_ARENA_MAX_BYTES=536870912
CHUNK_READ_QUEUE_DEPTH=128
CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES=4096
READBACK_SAMPLE_LIMIT_PER_KIND="${READBACK_SAMPLE_LIMIT_PER_KIND:-2}"
MAX_RESIDENT_BYTES_AFTER_EVICT="${MAX_RESIDENT_BYTES_AFTER_EVICT:-0}"
GUARD_INTERVAL_MS="${GUARD_INTERVAL_MS:-100}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
DISK_SPACE_CONFIRMED="${DISK_SPACE_CONFIRMED:-0}"
ALLOW_NOISY_HOST="${ALLOW_NOISY_HOST:-0}"
RUN_NOTE="${RUN_NOTE:-}"

usage() {
    cat <<EOF
Usage:
  RUN_NOTE='quiet host; no build, replay, profiler, or unrelated DB work' \\
  QUIET_HOST_CONFIRMED=1 \\
  DISK_SPACE_CONFIRMED=1 \\
    docs/experiments/storage_vnext/phase4_range_one_pass_run.sh [--dry-run]

Optional environment:
  SEGMENTS_DIR=/absolute/schema8/segments
  QUERY_MANIFEST=/absolute/phase4_range_one_pass_queries.json
  RESULT_DIR=/absolute/new-output-directory
  RESULT_PARENT=/absolute/existing-parent
  READBACK_SAMPLE_LIMIT_PER_KIND=2
  MAX_RESIDENT_BYTES_AFTER_EVICT=0
  GUARD_INTERVAL_MS=100 (fixed for formal evidence)
  DISK_SPACE_CONFIRMED=1 (manual acknowledgement; no capacity admission)

One query binary built from a read-only one-OID Git archive with isolated Cargo
home/target serves both runtime arms. The fixed ABBA/BAAB
schedule gives repeated and one-pass-assume-scalar eight fresh processes per
query, with one cold and two warm evaluations in every process. Timed queries
always use Schema 8, CompactIds, DemandDriven labels, pread queue depth 128,
4096-byte payload coalescing, instrumentation off, scalar range cache 0, and
--query-unlimited.

The 30-minute sum and count queries are dense real-corpus evidence. The 6-hour
and 24-hour sum queries are sparse scheduler controls because the accepted
corpus has only 1.25 hours of dense event-time coverage. The gate forbids a
promotion verdict regardless of latency: preallocation is not governed, finite
limit/error semantics are not exercised, QueryStats intentionally describe
different logical work, and dense 24-hour evidence is absent.

Footer validation and independent readbacks run outside timed processes.
POSIX_FADV_DONTNEED plus fincore records Linux page-cache residency; it does not
flush device/controller caches. This read-only experiment writes only its small
evidence tree and has no automated disk-capacity admission; manually confirm
that the result filesystem has ample free space. Every result directory must be
new.
EOF
}

die() {
    echo "Phase 4 range one-pass-assume-scalar: $*" >&2
    exit 2
}

note() {
    echo "Phase 4 range one-pass-assume-scalar: $*"
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
    [[ -n "$value" && "$value" != *$'\n'* && "$value" != *$'\r'* \
        && "$value" != *$'\t'* ]] \
        || die "$name is required and must contain no tabs or control newlines"
}

require_safe_path_text() {
    local name="$1" value="$2"
    [[ -n "$value" && "$value" != *$'\n'* && "$value" != *$'\r'* \
        && "$value" != *$'\t'* ]] \
        || die "$name must contain no tabs or control newlines"
}

for argument in "$@"; do
    case "$argument" in
        --dry-run) DRY_RUN=1 ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; die "unknown argument: $argument" ;;
    esac
done

for command in awk bash cc cmp cp date df fincore git grep ps python3 \
    realpath rm rustup sha256sum sleep sort stat tar uname /usr/bin/time; do
    require_command "$command"
done
for harness_file in phase4_range_one_pass_run.sh \
    phase4_range_one_pass_gate.py phase4_range_one_pass_guard.py \
    phase4_range_one_pass_queries.json \
    phase4_range_one_pass_plan.md test_phase4_range_one_pass_gate.py \
    test_phase4_range_one_pass_guard.py \
    phase3_payload_coalescing_gate.py phase2_compact_ids_ab_gate.py \
    schema8_query_ab_gate.py schema7_query_ab_gate.py phase1_query_gate.py \
    fadvise_regular_dontneed.c; do
    [[ -f "$SCRIPT_DIR/$harness_file" ]] \
        || die "required harness file is missing: $harness_file"
done

python3 "$SCRIPT_DIR/phase4_range_one_pass_gate.py" check-ambient-env
verify_background_python_pid_binding

require_bool DRY_RUN "$DRY_RUN"
require_bool QUIET_HOST_CONFIRMED "$QUIET_HOST_CONFIRMED"
require_bool DISK_SPACE_CONFIRMED "$DISK_SPACE_CONFIRMED"
require_bool ALLOW_NOISY_HOST "$ALLOW_NOISY_HOST"
[[ "$ALLOW_NOISY_HOST" == "0" ]] \
    || die "ALLOW_NOISY_HOST is forbidden for formal Phase 4 evidence"
[[ "$DRY_RUN" == "1" || "$QUIET_HOST_CONFIRMED" == "1" ]] \
    || die "non-dry measurement requires QUIET_HOST_CONFIRMED=1"
[[ "$DRY_RUN" == "1" || "$DISK_SPACE_CONFIRMED" == "1" ]] \
    || die "non-dry measurement requires manual DISK_SPACE_CONFIRMED=1"
require_single_line RUN_NOTE "$RUN_NOTE"
[[ "$READBACK_SAMPLE_LIMIT_PER_KIND" =~ ^[1-9][0-9]*$ ]] \
    || die "READBACK_SAMPLE_LIMIT_PER_KIND must be positive"
[[ "$MAX_RESIDENT_BYTES_AFTER_EVICT" == "0" ]] \
    || die "MAX_RESIDENT_BYTES_AFTER_EVICT is fixed at zero for formal evidence"
[[ "$GUARD_INTERVAL_MS" == "100" ]] \
    || die "the formal process guardian cadence is fixed at 100 ms"

SEGMENTS_DIR="${SEGMENTS_DIR:-$DEFAULT_SEGMENTS_DIR}"
QUERY_MANIFEST="${QUERY_MANIFEST:-$QUERY_MANIFEST_DEFAULT}"
RESULT_PARENT="${RESULT_PARENT:-$DEFAULT_RESULT_PARENT}"
require_safe_path_text SEGMENTS_DIR "$SEGMENTS_DIR"
require_safe_path_text QUERY_MANIFEST "$QUERY_MANIFEST"
require_safe_path_text RESULT_PARENT "$RESULT_PARENT"
[[ -z "${RESULT_DIR:-}" ]] || require_safe_path_text RESULT_DIR "$RESULT_DIR"

[[ "$SEGMENTS_DIR" == /* && -d "$SEGMENTS_DIR" ]] \
    || die "SEGMENTS_DIR must be an absolute existing directory"
SEGMENTS_DIR="$(realpath -e -- "$SEGMENTS_DIR")"
require_safe_path_text SEGMENTS_DIR "$SEGMENTS_DIR"
[[ "$QUERY_MANIFEST" == /* && -f "$QUERY_MANIFEST" ]] \
    || die "QUERY_MANIFEST must be an absolute regular file"
QUERY_MANIFEST="$(realpath -e -- "$QUERY_MANIFEST")"
require_safe_path_text QUERY_MANIFEST "$QUERY_MANIFEST"

if [[ -z "${RESULT_DIR:-}" ]]; then
    [[ "$RESULT_PARENT" == /* && -d "$RESULT_PARENT" ]] \
        || die "RESULT_PARENT must be an absolute existing directory"
    RESULT_PARENT="$(realpath -e -- "$RESULT_PARENT")"
    require_safe_path_text RESULT_PARENT "$RESULT_PARENT"
    RESULT_DIR="$RESULT_PARENT/storage-vnext-phase4-range-one-pass-$(date +%Y%m%d-%H%M%S)"
fi
[[ "$RESULT_DIR" == /* ]] || die "RESULT_DIR must be absolute"
result_name="$(basename "$RESULT_DIR")"
require_safe_path_text result_name "$result_name"
[[ -n "$result_name" && "$result_name" != "." && "$result_name" != ".." ]] \
    || die "RESULT_DIR must name a new child of an existing directory"
result_parent_input="$(dirname "$RESULT_DIR")"
[[ -d "$result_parent_input" ]] || die "RESULT_DIR parent does not exist"
result_parent="$(realpath -e -- "$result_parent_input")"
require_safe_path_text result_parent "$result_parent"
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
HARNESS_DIR="$METADATA_DIR/harness"
SOURCE_DIR="$METADATA_DIR/source"
BUILD_DIR="$METADATA_DIR/build"
BUILD_HOME="$BUILD_DIR/home"
BUILD_CARGO_HOME="$BUILD_DIR/cargo-home"
BUILD_SOURCE_DIR="$RESULT_DIR/build-source"
BUILD_TARGET_DIR="$RESULT_DIR/build-target"
RUN_BIN="$METADATA_DIR/chronoxide-query"
FADVISE_BIN="$METADATA_DIR/fadvise-regular-dontneed"
NORMALIZED_TSV="$RESULT_DIR/queries.tsv"
NORMALIZED_JSON="$RESULT_DIR/queries.normalized.json"
RUN_PLAN="$RESULT_DIR/run-plan.tsv"
RAW_INDEX="$RESULT_DIR/raw-index.tsv"
RESIDENCY_SUMMARY="$RESULT_DIR/residency-summary.tsv"
mkdir "$HARNESS_DIR" "$SOURCE_DIR" "$BUILD_DIR" "$BUILD_HOME" \
    "$BUILD_CARGO_HOME"

HARNESS_FILES=(
    phase4_range_one_pass_run.sh
    phase4_range_one_pass_gate.py
    phase4_range_one_pass_guard.py
    phase4_range_one_pass_queries.json
    phase4_range_one_pass_plan.md
    test_phase4_range_one_pass_gate.py
    test_phase4_range_one_pass_guard.py
    phase3_payload_coalescing_gate.py
    phase2_compact_ids_ab_gate.py
    schema8_query_ab_gate.py
    schema7_query_ab_gate.py
    phase1_query_gate.py
    fadvise_regular_dontneed.c
)
for harness_file in "${HARNESS_FILES[@]}"; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$HARNESS_DIR/$harness_file"
done
chmod -R a-w -- "$HARNESS_DIR"
FROZEN_GATE_TOOL="$HARNESS_DIR/phase4_range_one_pass_gate.py"
FROZEN_GUARD_TOOL="$HARNESS_DIR/phase4_range_one_pass_guard.py"
FROZEN_MANIFEST="$HARNESS_DIR/phase4_range_one_pass_queries.json"
FROZEN_FADVISE_SOURCE="$HARNESS_DIR/fadvise_regular_dontneed.c"
cmp -s -- "$QUERY_MANIFEST" "$FROZEN_MANIFEST" \
    || die "QUERY_MANIFEST must be byte-identical to the sealed source manifest"
(
    cd "$METADATA_DIR"
    for harness_file in "${HARNESS_FILES[@]}"; do
        sha256sum -- "harness/$harness_file"
    done
) >"$METADATA_DIR/harness.sha256"
chmod 0444 -- "$METADATA_DIR/harness.sha256"
HARNESS_MANIFEST_SHA256="$(sha256sum "$METADATA_DIR/harness.sha256" | awk '{print $1}')"

assert_harness_seal() {
    local harness_file mode
    [[ "$(stat -c '%a' -- "$HARNESS_DIR")" == "555" ]] \
        || die "frozen harness directory mode changed"
    [[ "$(stat -c '%a' -- "$METADATA_DIR/harness.sha256")" == "444" ]] \
        || die "harness checksum authority mode changed"
    [[ "$(sha256sum "$METADATA_DIR/harness.sha256" | awk '{print $1}')" == "$HARNESS_MANIFEST_SHA256" ]] \
        || die "harness checksum authority changed"
    (
        cd "$METADATA_DIR"
        sha256sum --check --strict harness.sha256 >/dev/null
    ) || die "frozen harness changed"
    for harness_file in "${HARNESS_FILES[@]}"; do
        [[ -f "$HARNESS_DIR/$harness_file" && ! -L "$HARNESS_DIR/$harness_file" ]] \
            || die "frozen harness file changed type: $harness_file"
        mode="$(stat -c '%a' -- "$HARNESS_DIR/$harness_file")"
        [[ "$mode" == "444" || "$mode" == "555" ]] \
            || die "frozen harness file has an unexpected mode: $harness_file"
    done
}
assert_harness_seal

run_gate() {
    assert_harness_seal
    python3 "$FROZEN_GATE_TOOL" "$@"
    assert_harness_seal
}

SOURCE_SEAL="$SOURCE_DIR/formal-source-seal.json"
SOURCE_SNAPSHOT_SEAL="$SOURCE_DIR/source-snapshot-seal.json"
SOURCE_ARCHIVE="$SOURCE_DIR/source-head.tar"
SOURCE_ARCHIVE_SHA256="$SOURCE_DIR/source-head.tar.sha256"
run_gate source-seal --repo "$REPO_ROOT" --output "$SOURCE_SEAL"
chmod 0444 -- "$SOURCE_SEAL"
SOURCE_SEAL_SHA256="$(sha256sum "$SOURCE_SEAL" | awk '{print $1}')"
SEALED_HEAD="$(git -C "$REPO_ROOT" rev-parse HEAD)"
[[ "$SEALED_HEAD" =~ ^[0-9a-f]{40,64}$ ]] \
    || die "formal source seal has an invalid HEAD"
git -C "$REPO_ROOT" archive --format=tar --output="$SOURCE_ARCHIVE" "$SEALED_HEAD"
chmod 0444 -- "$SOURCE_ARCHIVE"
sha256sum "$SOURCE_ARCHIVE" >"$SOURCE_ARCHIVE_SHA256"
chmod 0444 -- "$SOURCE_ARCHIVE_SHA256"
SOURCE_ARCHIVE_AUTHORITY_SHA256="$(sha256sum "$SOURCE_ARCHIVE_SHA256" | awk '{print $1}')"
mkdir "$BUILD_SOURCE_DIR" "$BUILD_TARGET_DIR"
tar -xf "$SOURCE_ARCHIVE" -C "$BUILD_SOURCE_DIR"
chmod -R a-w -- "$BUILD_SOURCE_DIR"
run_gate source-snapshot-seal \
    --repo "$REPO_ROOT" \
    --snapshot "$BUILD_SOURCE_DIR" \
    --source-seal "$SOURCE_SEAL" \
    --output "$SOURCE_SNAPSHOT_SEAL"
chmod 0444 -- "$SOURCE_SNAPSHOT_SEAL"
SOURCE_SNAPSHOT_SEAL_SHA256="$(sha256sum "$SOURCE_SNAPSHOT_SEAL" | awk '{print $1}')"
for harness_file in "${HARNESS_FILES[@]}"; do
    snapshot_harness="$BUILD_SOURCE_DIR/docs/experiments/storage_vnext/$harness_file"
    [[ -f "$snapshot_harness" && ! -L "$snapshot_harness" ]] \
        || die "source snapshot is missing harness file: $harness_file"
    cmp -s -- "$HARNESS_DIR/$harness_file" "$snapshot_harness" \
        || die "frozen harness differs from sealed HEAD: $harness_file"
done

RUSTUP_BIN="$(realpath -e -- "$(type -P rustup)")"
CARGO_BIN="$(realpath -e -- "$(rustup which cargo)")"
RUSTC_BIN="$(realpath -e -- "$(rustup which rustc)")"
for tool in "$RUSTUP_BIN" "$CARGO_BIN" "$RUSTC_BIN"; do
    [[ "$tool" == /* && -f "$tool" && -x "$tool" && ! -L "$tool" ]] \
        || die "formal build tool is not an absolute regular executable: $tool"
done
RUSTUP_HOME_EFFECTIVE="$(realpath -e -- "${RUSTUP_HOME:-$HOME/.rustup}")"
BUILD_PATH="$(dirname "$CARGO_BIN"):/usr/bin:/bin"
BUILD_TARGET_TRIPLE="$(env -i PATH="$BUILD_PATH" HOME="$BUILD_HOME" \
    RUSTUP_HOME="$RUSTUP_HOME_EFFECTIVE" CARGO_HOME="$BUILD_CARGO_HOME" \
    LC_ALL=C TZ=UTC "$RUSTC_BIN" --version --verbose | \
    awk -F': ' '$1 == "host" {print $2}')"
[[ "$BUILD_TARGET_TRIPLE" =~ ^[A-Za-z0-9_.-]+$ ]] \
    || die "could not determine formal build target triple"
SOURCE_DATE_EPOCH="$(git -C "$REPO_ROOT" show -s --format=%ct "$SEALED_HEAD")"
BUILD_ENV=(
    "PATH=$BUILD_PATH"
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
    "$CARGO_BIN" build --locked --release --target "$BUILD_TARGET_TRIPLE"
    -p chronoxide-ingester --bin chronoxide-query
)
{
    printf 'name\tvalue\n'
    for assignment in "${BUILD_ENV[@]}"; do
        printf '%s\t%s\n' "${assignment%%=*}" "${assignment#*=}"
    done
    printf 'profile\trelease\n'
    printf 'target\t%s\n' "$BUILD_TARGET_TRIPLE"
    printf 'features\tdefault\n'
} >"$BUILD_DIR/build-environment.tsv"
printf '%s\0' "${BUILD_COMMAND[@]}" >"$BUILD_DIR/build-argv.nul"
env -i "${BUILD_ENV[@]}" "$RUSTC_BIN" --version --verbose \
    >"$BUILD_DIR/rustc-version.txt"
env -i "${BUILD_ENV[@]}" "$CARGO_BIN" --version --verbose \
    >"$BUILD_DIR/cargo-version.txt"
env -i "${BUILD_ENV[@]}" "$RUSTUP_BIN" show active-toolchain \
    >"$BUILD_DIR/rustup-active-toolchain.txt"
printf 'name\tpath\ncargo\t%s\nrustc\t%s\nrustup\t%s\n' \
    "$CARGO_BIN" "$RUSTC_BIN" "$RUSTUP_BIN" >"$BUILD_DIR/tool-paths.tsv"
sha256sum "$CARGO_BIN" "$RUSTC_BIN" "$RUSTUP_BIN" \
    >"$BUILD_DIR/tool-binaries.sha256"
run_gate check-cargo-config-isolation \
    --snapshot "$BUILD_SOURCE_DIR" --cargo-home "$BUILD_CARGO_HOME" \
    >"$BUILD_DIR/cargo-config-isolation-before-metadata.json"
(
    cd "$BUILD_SOURCE_DIR"
    env -i "${BUILD_ENV[@]}" "$CARGO_BIN" metadata --locked --no-deps \
        --format-version 1
) >"$BUILD_DIR/cargo-metadata.json"
run_gate check-cargo-config-isolation \
    --snapshot "$BUILD_SOURCE_DIR" --cargo-home "$BUILD_CARGO_HOME" \
    >"$BUILD_DIR/cargo-config-isolation-after-metadata.json"
run_gate check-source-seal \
    --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    >"$BUILD_DIR/source-check-before-build.json"
sha256sum --check --strict "$SOURCE_ARCHIVE_SHA256" \
    >"$BUILD_DIR/source-archive-check-before-build.txt"
run_gate check-source-snapshot-seal \
    --repo "$REPO_ROOT" --snapshot "$BUILD_SOURCE_DIR" \
    --source-seal "$SOURCE_SEAL" --seal "$SOURCE_SNAPSHOT_SEAL" \
    >"$BUILD_DIR/source-snapshot-check-before-build.json"
note "performing one isolated source-bound release build"
set +e
(
    cd "$BUILD_SOURCE_DIR"
    env -i "${BUILD_ENV[@]}" "${BUILD_COMMAND[@]}"
) >"$BUILD_DIR/build.log" 2>&1
build_status=$?
set -e
printf '%s\n' "$build_status" >"$BUILD_DIR/build.exit-status"
(( build_status == 0 )) || die "formal source-bound release build failed"
run_gate check-source-seal \
    --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    >"$BUILD_DIR/source-check-after-build.json"
sha256sum --check --strict "$SOURCE_ARCHIVE_SHA256" \
    >"$BUILD_DIR/source-archive-check-after-build.txt"
run_gate check-source-snapshot-seal \
    --repo "$REPO_ROOT" --snapshot "$BUILD_SOURCE_DIR" \
    --source-seal "$SOURCE_SEAL" --seal "$SOURCE_SNAPSHOT_SEAL" \
    >"$BUILD_DIR/source-snapshot-check-after-build.json"
run_gate check-cargo-config-isolation \
    --snapshot "$BUILD_SOURCE_DIR" --cargo-home "$BUILD_CARGO_HOME" \
    >"$BUILD_DIR/cargo-config-isolation-after-build.json"
BUILT_QUERY="$BUILD_TARGET_DIR/$BUILD_TARGET_TRIPLE/release/chronoxide-query"
[[ -f "$BUILT_QUERY" && -x "$BUILT_QUERY" && ! -L "$BUILT_QUERY" ]] \
    || die "isolated release build did not produce chronoxide-query"
cp --reflink=auto --preserve=mode,timestamps -- "$BUILT_QUERY" "$RUN_BIN"
cmp -s -- "$BUILT_QUERY" "$RUN_BIN" \
    || die "preserved query binary differs from isolated build output"
chmod 0555 -- "$RUN_BIN"
(
    cd "$METADATA_DIR"
    sha256sum -- chronoxide-query
) >"$METADATA_DIR/query-binary.sha256"
chmod 0444 -- "$METADATA_DIR/query-binary.sha256"
BINARY_SHA256="$(awk '{print $1}' "$METADATA_DIR/query-binary.sha256")"
[[ "$BINARY_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "could not hash query binary"
BINARY_MANIFEST_SHA256="$(sha256sum "$METADATA_DIR/query-binary.sha256" | awk '{print $1}')"
rm -rf -- "$BUILD_TARGET_DIR" "$BUILD_CARGO_HOME"
mkdir "$BUILD_CARGO_HOME"

assert_binary_seal() {
    [[ "$(stat -c '%a' -- "$RUN_BIN")" == "555" && -f "$RUN_BIN" && ! -L "$RUN_BIN" ]] \
        || die "preserved query binary type or mode changed"
    [[ "$(stat -c '%a' -- "$METADATA_DIR/query-binary.sha256")" == "444" ]] \
        || die "query binary checksum authority mode changed"
    [[ "$(sha256sum "$METADATA_DIR/query-binary.sha256" | awk '{print $1}')" == "$BINARY_MANIFEST_SHA256" ]] \
        || die "query binary checksum authority changed"
    (
        cd "$METADATA_DIR"
        sha256sum --check --strict query-binary.sha256 >/dev/null
    ) || die "preserved query binary changed"
}

assert_source_seal() {
    [[ "$(stat -c '%a' -- "$SOURCE_SEAL")" == "444" \
        && "$(sha256sum "$SOURCE_SEAL" | awk '{print $1}')" == "$SOURCE_SEAL_SHA256" ]] \
        || die "formal source seal authority changed"
    [[ "$(stat -c '%a' -- "$SOURCE_SNAPSHOT_SEAL")" == "444" \
        && "$(sha256sum "$SOURCE_SNAPSHOT_SEAL" | awk '{print $1}')" == "$SOURCE_SNAPSHOT_SEAL_SHA256" ]] \
        || die "source snapshot seal authority changed"
    [[ "$(stat -c '%a' -- "$SOURCE_ARCHIVE_SHA256")" == "444" \
        && "$(sha256sum "$SOURCE_ARCHIVE_SHA256" | awk '{print $1}')" == "$SOURCE_ARCHIVE_AUTHORITY_SHA256" ]] \
        || die "source archive checksum authority changed"
    run_gate check-source-seal \
        --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" >/dev/null
    sha256sum --check --strict "$SOURCE_ARCHIVE_SHA256" >/dev/null
    run_gate check-source-snapshot-seal \
        --repo "$REPO_ROOT" --snapshot "$BUILD_SOURCE_DIR" \
        --source-seal "$SOURCE_SEAL" --seal "$SOURCE_SNAPSHOT_SEAL" >/dev/null
    run_gate check-cargo-config-isolation \
        --snapshot "$BUILD_SOURCE_DIR" --cargo-home "$BUILD_CARGO_HOME" >/dev/null
    for harness_file in "${HARNESS_FILES[@]}"; do
        cmp -s -- "$HARNESS_DIR/$harness_file" \
            "$BUILD_SOURCE_DIR/docs/experiments/storage_vnext/$harness_file" \
            || die "frozen harness lost source-snapshot binding: $harness_file"
    done
}

assert_binary_seal
assert_source_seal
help_text="$(env -i LC_ALL=C TZ=UTC "$RUN_BIN" --help 2>&1)"
for required_help in '--storage-layout' '--label-materialization' \
    '--query-label-storage' '--query-label-arena-max-bytes' \
    '--query-instrumentation' '--chunk-payload-coalesce-max-gap-bytes' \
    '--range-scalar-cache-max-bytes' '--range-execution-mode' \
    '--query-unlimited' '--verify-readbacks' '--validate-segment-footers' \
    'one-pass-assume-scalar' 'schema8' 'compact-ids'; do
    grep -Fq -- "$required_help" <<<"$help_text" \
        || die "query binary help is missing $required_help"
done
printf '%s\n' "$help_text" >"$METADATA_DIR/query-help.txt"
stat --printf='size_bytes=%s\nmtime=%y\ninode=%i\ndevice=%d\n' -- "$RUN_BIN" \
    >"$METADATA_DIR/query-binary.stat.txt"

run_gate normalize-manifest \
    --input "$FROZEN_MANIFEST" \
    --output-tsv "$NORMALIZED_TSV" \
    --output-json "$NORMALIZED_JSON"
run_gate write-plan \
    --manifest "$NORMALIZED_JSON" \
    --source-manifest "$FROZEN_MANIFEST" \
    --output "$RUN_PLAN"
run_gate inventory \
    --corpus "$SEGMENTS_DIR" \
    --output "$INVENTORY_DIR/before.json" \
    --paths-output "$INVENTORY_DIR/files.nul"
sha256sum "$INVENTORY_DIR/before.json" >"$INVENTORY_DIR/before.sha256"
cc -O2 -Wall -Wextra -Werror -o "$FADVISE_BIN" "$FROZEN_FADVISE_SOURCE"
chmod 0555 -- "$FADVISE_BIN"
(
    cd "$METADATA_DIR"
    sha256sum -- fadvise-regular-dontneed
) >"$METADATA_DIR/fadvise.sha256"
chmod 0444 -- "$METADATA_DIR/fadvise.sha256"
FADVISE_MANIFEST_SHA256="$(sha256sum "$METADATA_DIR/fadvise.sha256" | awk '{print $1}')"

CONTROLLED_INPUT_FILES=(
    "$RUN_BIN"
    "$FADVISE_BIN"
    "$FROZEN_MANIFEST"
    "$NORMALIZED_TSV"
    "$NORMALIZED_JSON"
    "$RUN_PLAN"
    "$INVENTORY_DIR/before.json"
    "$INVENTORY_DIR/files.nul"
)
chmod 0444 -- "$NORMALIZED_TSV" "$NORMALIZED_JSON" "$RUN_PLAN" \
    "$INVENTORY_DIR/before.json" "$INVENTORY_DIR/files.nul" \
    "$INVENTORY_DIR/before.sha256" "$METADATA_DIR/query-help.txt" \
    "$METADATA_DIR/query-binary.stat.txt"
(
    cd "$RESULT_DIR"
    sha256sum -- \
        metadata/chronoxide-query \
        metadata/fadvise-regular-dontneed \
        metadata/harness/phase4_range_one_pass_queries.json \
        queries.tsv queries.normalized.json run-plan.tsv \
        inventory/before.json inventory/files.nul
) >"$METADATA_DIR/controlled-inputs.sha256"
chmod 0444 -- "$METADATA_DIR/controlled-inputs.sha256"
CONTROL_INPUTS_MANIFEST_SHA256="$(sha256sum "$METADATA_DIR/controlled-inputs.sha256" | awk '{print $1}')"

assert_control_inputs_seal() {
    local input expected_mode
    [[ "$(stat -c '%a' -- "$METADATA_DIR/controlled-inputs.sha256")" == "444" ]] \
        || die "controlled input checksum authority mode changed"
    [[ "$(sha256sum "$METADATA_DIR/controlled-inputs.sha256" | awk '{print $1}')" == "$CONTROL_INPUTS_MANIFEST_SHA256" ]] \
        || die "controlled input checksum authority changed"
    (
        cd "$RESULT_DIR"
        sha256sum --check --strict metadata/controlled-inputs.sha256 >/dev/null
    ) || die "controlled experiment input changed"
    for input in "${CONTROLLED_INPUT_FILES[@]}"; do
        expected_mode=444
        [[ "$input" != "$RUN_BIN" && "$input" != "$FADVISE_BIN" ]] || expected_mode=555
        [[ -f "$input" && ! -L "$input" && "$(stat -c '%a' -- "$input")" == "$expected_mode" ]] \
            || die "controlled experiment input type or mode changed: $input"
    done
}

BUILD_INPUT_PATHS=(
    metadata/chronoxide-query
    metadata/source/formal-source-seal.json
    metadata/source/source-snapshot-seal.json
    metadata/source/source-head.tar
    metadata/source/source-head.tar.sha256
    metadata/build/build-environment.tsv
    metadata/build/build-argv.nul
    metadata/build/build.log
    metadata/build/build.exit-status
    metadata/build/cargo-version.txt
    metadata/build/rustc-version.txt
    metadata/build/rustup-active-toolchain.txt
    metadata/build/tool-paths.tsv
    metadata/build/tool-binaries.sha256
    metadata/build/cargo-metadata.json
    metadata/build/cargo-config-isolation-before-metadata.json
    metadata/build/cargo-config-isolation-after-metadata.json
    metadata/build/cargo-config-isolation-after-build.json
    metadata/build/source-check-before-build.json
    metadata/build/source-check-after-build.json
    metadata/build/source-snapshot-check-before-build.json
    metadata/build/source-snapshot-check-after-build.json
    metadata/build/source-archive-check-before-build.txt
    metadata/build/source-archive-check-after-build.txt
)
chmod 0444 -- "$BUILD_DIR"/*.json "$BUILD_DIR"/*.txt "$BUILD_DIR"/*.tsv \
    "$BUILD_DIR"/*.nul "$BUILD_DIR"/build.log "$BUILD_DIR"/build.exit-status
(
    cd "$RESULT_DIR"
    sha256sum -- "${BUILD_INPUT_PATHS[@]}"
) >"$BUILD_DIR/build-input-provenance.sha256"
chmod 0444 -- "$BUILD_DIR/build-input-provenance.sha256"
BUILD_INPUT_MANIFEST_SHA256="$(sha256sum "$BUILD_DIR/build-input-provenance.sha256" | awk '{print $1}')"

assert_build_input_seal() {
    [[ "$(stat -c '%a' -- "$BUILD_DIR/build-input-provenance.sha256")" == "444" ]] \
        || die "build input checksum authority mode changed"
    [[ "$(sha256sum "$BUILD_DIR/build-input-provenance.sha256" | awk '{print $1}')" == "$BUILD_INPUT_MANIFEST_SHA256" ]] \
        || die "build input checksum authority changed"
    (
        cd "$RESULT_DIR"
        sha256sum --check --strict metadata/build/build-input-provenance.sha256 >/dev/null
    ) || die "source-bound build provenance changed"
}

declare -a LEAF_MANIFESTS=()
declare -A LEAF_MANIFEST_AUTHORITIES=()

seal_process_leaves() {
    local base="$1" manifest_name="$2"
    shift 2
    local leaf manifest="$base/$manifest_name"
    [[ ! -e "$manifest" ]] || die "refusing to replace leaf authority: $manifest"
    for leaf in "$@"; do
        [[ -f "$base/$leaf" && ! -L "$base/$leaf" ]] \
            || die "process leaf changed type before sealing: $base/$leaf"
        chmod 0444 -- "$base/$leaf"
    done
    (
        cd "$base"
        sha256sum -- "$@"
    ) >"$manifest"
    chmod 0444 -- "$manifest"
    LEAF_MANIFESTS+=("$manifest")
    LEAF_MANIFEST_AUTHORITIES["$manifest"]="$(sha256sum "$manifest" | awk '{print $1}')"
}

assert_process_leaf_seals() {
    local manifest authority
    for manifest in "${LEAF_MANIFESTS[@]}"; do
        authority="${LEAF_MANIFEST_AUTHORITIES[$manifest]}"
        [[ -f "$manifest" && ! -L "$manifest" \
            && "$(stat -c '%a' -- "$manifest")" == "444" \
            && "$(sha256sum "$manifest" | awk '{print $1}')" == "$authority" ]] \
            || die "process leaf checksum authority changed: $manifest"
        (
            cd "$(dirname "$manifest")"
            sha256sum --check --strict "$(basename "$manifest")" >/dev/null
        ) || die "sealed process leaf changed: $manifest"
    done
}

printf 'recorded_at\tcontext\n' >"$METADATA_DIR/seal-checks.tsv"
assert_experiment_seals() {
    local context="$1"
    assert_harness_seal
    assert_source_seal
    assert_harness_seal
    assert_binary_seal
    assert_control_inputs_seal
    assert_build_input_seal
    assert_process_leaf_seals
    [[ "$(sha256sum "$METADATA_DIR/fadvise.sha256" | awk '{print $1}')" == "$FADVISE_MANIFEST_SHA256" ]] \
        || die "fadvise checksum authority changed"
    (
        cd "$METADATA_DIR"
        sha256sum --check --strict fadvise.sha256 >/dev/null
    ) || die "fadvise helper changed"
    printf '%s\t%s\n' "$(date --iso-8601=ns)" "$context" \
        >>"$METADATA_DIR/seal-checks.tsv"
}
assert_experiment_seals initial-controls

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\ncorpus=%s\nquery_binary=%s\n' \
        "$DRY_RUN" "$SEGMENTS_DIR" "$RUN_BIN"
    printf 'query_manifest=%s\n' "$FROZEN_MANIFEST"
    printf 'source_head=%s\nsource_snapshot=%s\n' \
        "$SEALED_HEAD" "$BUILD_SOURCE_DIR"
    printf 'build_mode=locked-release-default-features\n'
    printf 'python=%s\npython_flags=-I -S -B\n' "$PYTHON_BIN"
    printf 'blocks=%s\nprocesses_per_arm_per_query=%s\n' \
        "$BLOCKS" "$PROCESSES_PER_ARM_PER_QUERY"
    printf 'benchmark_repeats=%s\n' "$BENCHMARK_REPEATS"
    printf 'odd_schedule=repeated,one-pass-assume-scalar,one-pass-assume-scalar,repeated\n'
    printf 'even_schedule=one-pass-assume-scalar,repeated,repeated,one-pass-assume-scalar\n'
    printf 'storage_layout=schema8\nchunk_read_mode=pread\n'
    printf 'chunk_read_queue_depth=%s\n' "$CHUNK_READ_QUEUE_DEPTH"
    printf 'chunk_payload_coalesce_max_gap_bytes=%s\n' \
        "$CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES"
    printf 'label_materialization=demand-driven\nquery_label_storage=compact-ids\n'
    printf 'query_label_arena_max_bytes=%s\n' "$QUERY_LABEL_ARENA_MAX_BYTES"
    printf 'query_instrumentation=off\nrange_scalar_cache_max_bytes=0\n'
    printf 'query_limits=unlimited\n'
    printf 'process_guardian_interval_ms=%s\n' "$GUARD_INTERVAL_MS"
    printf 'process_guardian_maximum_edge_gap_ms=200\n'
    printf 'disk_capacity_admission=none-read-only-small-evidence-output\n'
    printf 'disk_space_confirmed=%s\n' "$DISK_SPACE_CONFIRMED"
    printf 'max_resident_bytes_after_evict=%s\n' "$MAX_RESIDENT_BYTES_AFTER_EVICT"
    printf 'dense_event_time_span_ms=4500000\n'
    printf 'dense_ranges=scalar_rate_sum_range_30m,scalar_rate_count_range_30m\n'
    printf 'sparse_scheduler_controls=scalar_rate_sum_range_6h,scalar_rate_sum_range_24h\n'
    printf 'promotion_verdict=forbidden\npreallocation_governed=false\n'
    printf 'quiet_host_confirmed=%s\nallow_noisy_host=%s\n' \
        "$QUIET_HOST_CONFIRMED" "$ALLOW_NOISY_HOST"
    printf 'run_note=%s\n' "$RUN_NOTE"
    printf 'footer_validation=separate pre-measurement pass\n'
    printf 'readback_validation=separate pre-measurement pass\n'
    printf 'timed_footer_validation=forbidden and enforced by the raw-output gate\n'
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
    local snapshot="$1"
    ps -eo pid=,ppid=,pcpu=,comm=,args= >"$snapshot"
    run_gate validate-process-snapshot \
        --snapshot "$snapshot" --allow-pid "$$" --allow-pid "$PPID" \
        || die "measurement conflict detected in $snapshot"
}

active_lifecycle_dir=''
active_control=''
active_ready=''
active_launch=''
active_root_pid=''
active_root_starttime_ticks=''
active_guardian_pid=''
active_guardian_starttime_ticks=''
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
    local pid="$1" identity state starttime_ticks
    identity="$(read_process_state_starttime_ticks "$pid")" || return 1
    read -r state starttime_ticks <<<"$identity"
    [[ "$state" != Z && "$state" != X && "$state" != x ]] || return 1
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
    active_ready=''
    active_launch=''
    active_root_pid=''
    active_root_starttime_ticks=''
    active_guardian_pid=''
    active_guardian_starttime_ticks=''
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
    python3 "$FROZEN_GUARD_TOOL" terminate-tree --root-pid "$pid" \
        --root-starttime-ticks "$starttime_ticks" \
        >"$active_lifecycle_dir/interrupted-$role-termination.json" 2>&1 || true
}

bounded_reap_job() {
    local role="$1" pid="$2" expected_starttime_ticks="$3"
    local attempt identity state current_starttime_ticks
    [[ -n "$pid" ]] || return 0
    if [[ -z "$expected_starttime_ticks" ]]; then
        record_cleanup_reap "$role" unbound-wait-refused "pid=$pid"
        return 1
    fi
    for ((attempt = 0; attempt < 200; attempt++)); do
        if ! identity="$(read_process_state_starttime_ticks "$pid")"; then
            if [[ ! -e "/proc/$pid/stat" ]]; then
                wait "$pid" 2>/dev/null || true
                record_cleanup_reap "$role" reaped-after-exit "pid=$pid"
                return 0
            fi
            record_cleanup_reap "$role" identity-read-failed "pid=$pid"
            return 1
        fi
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
    if [[ -n "$active_control" && -f "$active_control" \
        && ! -L "$active_control" ]]; then
        if python3 "$FROZEN_GUARD_TOOL" cleanup-control \
            --control "$active_control" --ready "$active_ready" \
            --launch "$active_launch" --interval-ms "$GUARD_INTERVAL_MS" \
            >"$active_lifecycle_dir/interrupted-controlled-cleanup.json" 2>&1; then
            controlled=1
        fi
    fi
    if [[ "$controlled" == 0 ]]; then
        stop_bound_tree root "$active_root_pid" "$active_root_starttime_ticks" || true
        stop_bound_tree guardian "$active_guardian_pid" \
            "$active_guardian_starttime_ticks" || true
    fi
    bounded_reap_job root "$active_root_pid" "$active_root_starttime_ticks" || true
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
    if [[ -n "$active_root_pid" || -n "$active_guardian_pid" ]]; then
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

run_held_workload() {
    local label="$1" base="$2" prefix="$3" log_path="$4" status_path="$5"
    shift 5
    local -a command=("$@")
    local control="$base/$prefix.guardian-control.json"
    local ready="$base/$prefix.guardian-ready"
    local launch="$base/$prefix.guardian-launch"
    local samples="$base/$prefix.guardian-samples.tsv"
    local conflicts="$base/$prefix.guardian-conflicts.tsv"
    local summary="$base/$prefix.guardian-summary.json"
    local guardian_log="$base/$prefix.guardian.log"
    local guardian_status_path="$base/$prefix.guardian-exit-status"
    local immediate="$base/$prefix.guardian-immediate-conflicts.json"
    local root_pid guardian_pid root_status guardian_status binding_failed=0
    local artifact
    [[ -d "$base" && ! -L "$base" ]] \
        || die "$label lifecycle parent must be a non-symlink directory"
    for artifact in "$log_path" "$status_path" "$control" "$ready" "$launch" \
        "$samples" "$conflicts" "$summary" "$guardian_log" \
        "$guardian_status_path" "$immediate"; do
        [[ ! -e "$artifact" && ! -L "$artifact" ]] \
            || die "$label refuses to reuse lifecycle artifact: $artifact"
    done
    python3 "$FROZEN_GUARD_TOOL" scan-conflicts --output "$immediate" >/dev/null \
        || die "$label found a quiet-host conflict immediately before launch"
    active_lifecycle_dir="$base"
    active_control="$control"
    active_ready="$ready"
    active_launch="$launch"
    defer_cleanup_signals
    (
        while [[ ! -e "$launch" && ! -L "$launch" ]]; do sleep 0.001; done
        [[ -f "$launch" && ! -L "$launch" && ! -s "$launch" \
            && "$(stat -c '%a' -- "$launch")" == 444 ]] || exit 125
        exec "${command[@]}" >"$log_path" 2>&1
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
    python3_background "$FROZEN_GUARD_TOOL" monitor --runner-pid "$$" \
        --root-pid "$root_pid" --samples "$samples" --conflicts "$conflicts" \
        --summary "$summary" --control "$control" --ready "$ready" \
        --launch "$launch" --interval-ms "$GUARD_INTERVAL_MS" \
        >/dev/null 2>"$guardian_log" &
    guardian_pid=$!
    active_guardian_pid="$guardian_pid"
    active_guardian_starttime_ticks="$(bind_live_starttime_ticks "$guardian_pid")" \
        || binding_failed=1
    arm_cleanup_signals
    ((binding_failed == 0)) \
        || { stop_active_lifecycle; die "$label guardian exited before binding"; }
    python3 "$FROZEN_GUARD_TOOL" create-control --output "$control" \
        --ready "$ready" --launch "$launch" --runner-pid "$$" \
        --root-pid "$root_pid" --guardian-pid "$guardian_pid" \
        --interval-ms "$GUARD_INTERVAL_MS" >/dev/null \
        || { stop_active_lifecycle; die "$label control publication failed"; }
    python3 "$FROZEN_GUARD_TOOL" wait-ready --control "$control" \
        --ready "$ready" --launch "$launch" --interval-ms "$GUARD_INTERVAL_MS" \
        --timeout-ms 5000 >/dev/null \
        || { stop_active_lifecycle; die "$label guardian readiness failed"; }
    python3 "$FROZEN_GUARD_TOOL" release-launch --control "$control" \
        --ready "$ready" --launch "$launch" --interval-ms "$GUARD_INTERVAL_MS" \
        >/dev/null \
        || { stop_active_lifecycle; die "$label launch release failed"; }
    set +e
    wait "$root_pid"; root_status=$?
    wait "$guardian_pid"; guardian_status=$?
    set -e
    clear_active_lifecycle
    printf '%s\n' "$root_status" >"$status_path"
    printf '%s\n' "$guardian_status" >"$guardian_status_path"
    ((guardian_status == 0)) \
        || die "$label process guardian failed; partial evidence was preserved"
    [[ ! -s "$guardian_log" ]] \
        || die "$label successful process guardian wrote diagnostics"
    ((root_status == 0)) || die "$label workload failed with status $root_status"
    run_gate validate-process-guardian --samples "$samples" \
        --conflicts "$conflicts" --summary "$summary" --control "$control" \
        --exit-status "$guardian_status_path" --ready "$ready" \
        --launch "$launch" --immediate-conflicts "$immediate" \
        || die "$label lifecycle reconstruction failed"
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
    local process_label="$1" block="$2" execution_mode="$3" phase="$4" output="$5"
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
        "$process_label" "$block" "$execution_mode" "$phase" "$file_count" \
        "$total_resident" "$total_size" >>"$RESIDENCY_SUMMARY"
    printf '%s\n' "$total_resident"
}

run_validation_passes() {
    note "validating every segment footer outside timed query processes"
    check_measurement_conflicts "$VALIDATION_DIR/processes-before-footer.txt"
    assert_experiment_seals before-footer-validation
    check_measurement_conflicts \
        "$VALIDATION_DIR/processes-immediate-before-footer.txt"
    run_held_workload footer-validation "$VALIDATION_DIR" footer \
        "$VALIDATION_DIR/footer.log" "$VALIDATION_DIR/footer.exit-status" \
        /usr/bin/time -v -o "$VALIDATION_DIR/footer.time.txt" \
        "$RUN_BIN" \
            --segments-dir "$SEGMENTS_DIR" \
            --storage-layout schema8 \
            --sample-limit-per-kind 0 \
            --validate-segment-footers \
            --output "$VALIDATION_DIR/footer.md"
    seal_process_leaves "$VALIDATION_DIR" footer-leaves.sha256 \
        processes-before-footer.txt processes-immediate-before-footer.txt \
        footer.guardian-immediate-conflicts.json footer.guardian-control.json \
        footer.guardian-ready footer.guardian-launch \
        footer.guardian-samples.tsv footer.guardian-conflicts.tsv \
        footer.guardian-summary.json footer.guardian.log \
        footer.guardian-exit-status \
        footer.time.txt footer.md footer.log footer.exit-status
    assert_experiment_seals after-footer-validation
    run_gate validate-smoke-report \
        --kind footer \
        --report "$VALIDATION_DIR/footer.md" \
        --output "$VALIDATION_DIR/footer.json"

    note "running independent readback oracle outside timed query processes"
    check_measurement_conflicts "$VALIDATION_DIR/processes-before-readbacks.txt"
    assert_experiment_seals before-readback-validation
    check_measurement_conflicts \
        "$VALIDATION_DIR/processes-immediate-before-readbacks.txt"
    run_held_workload readback-validation "$VALIDATION_DIR" readbacks \
        "$VALIDATION_DIR/readbacks.log" "$VALIDATION_DIR/readbacks.exit-status" \
        /usr/bin/time -v -o "$VALIDATION_DIR/readbacks.time.txt" \
        "$RUN_BIN" \
            --segments-dir "$SEGMENTS_DIR" \
            --storage-layout schema8 \
            --sample-limit-per-kind "$READBACK_SAMPLE_LIMIT_PER_KIND" \
            --verify-readbacks \
            --output "$VALIDATION_DIR/readbacks.md"
    seal_process_leaves "$VALIDATION_DIR" readbacks-leaves.sha256 \
        processes-before-readbacks.txt processes-immediate-before-readbacks.txt \
        readbacks.guardian-immediate-conflicts.json \
        readbacks.guardian-control.json readbacks.guardian-ready \
        readbacks.guardian-launch readbacks.guardian-samples.tsv \
        readbacks.guardian-conflicts.tsv readbacks.guardian-summary.json \
        readbacks.guardian.log readbacks.guardian-exit-status \
        readbacks.time.txt readbacks.md readbacks.log readbacks.exit-status
    assert_experiment_seals after-readback-validation
    run_gate validate-smoke-report \
        --kind readback \
        --report "$VALIDATION_DIR/readbacks.md" \
        --output "$VALIDATION_DIR/readbacks.json"
}

run_validation_passes

printf 'process_label\tblock\trange_execution_mode\tphase\tfile_count\tresident_bytes\tcorpus_file_bytes\n' \
    >"$RESIDENCY_SUMMARY"
printf 'process_label\tquery_name\tevidence_class\tblock\torder_index\trange_execution_mode\tbinary_sha256\tcorpus\traw_output\tprocess_wall_seconds\tprocess_user_seconds\tprocess_system_seconds\tmax_rss_kib\n' \
    >"$RAW_INDEX"

declare -A QUERY_START QUERY_END QUERY_STEP QUERY_EXPRESSION
while IFS=$'\t' read -r query_name _mode start_ms end_ms step_ms _window_ms \
    _outer_range_ms _expected_count _cache_bytes _evidence_class \
    _dense_promotion_evidence expression; do
    [[ "$query_name" != "query_name" ]] || continue
    QUERY_START["$query_name"]="$start_ms"
    QUERY_END["$query_name"]="$end_ms"
    QUERY_STEP["$query_name"]="$step_ms"
    QUERY_EXPRESSION["$query_name"]="$expression"
done <"$NORMALIZED_TSV"

read_time_value() {
    local key="$1" file="$2"
    awk -F '\t' -v key="$key" '$1 == key { print $2 }' "$file"
}

run_process() {
    local process_label="$1" query_name="$2" evidence_class="$3"
    local block="$4" order_index="$5" execution_mode="$6"
    local run_dir raw markdown log time_file resident_after_evict
    local wall_seconds user_seconds system_seconds max_rss_kib
    local -a args

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
        "$process_label" "$block" "$execution_mode" after-evict \
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
        --start-ms "${QUERY_START[$query_name]}"
        --end-ms "${QUERY_END[$query_name]}"
        --step-ms "${QUERY_STEP[$query_name]}"
        --benchmark-repeats "$BENCHMARK_REPEATS"
        --chunk-read-mode pread
        --chunk-read-queue-depth "$CHUNK_READ_QUEUE_DEPTH"
        --chunk-payload-coalesce-max-gap-bytes "$CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES"
        --range-scalar-cache-max-bytes 0
        --range-execution-mode "$execution_mode"
        --query-unlimited
        --output "$markdown"
        --raw-output "$raw"
        --query "${QUERY_EXPRESSION[$query_name]}"
    )
    for argument in "${args[@]}"; do
        [[ "$argument" != "--validate-segment-footers" ]] \
            || die "internal error: footer validation entered timed arguments"
    done
    printf '%s\0' "$RUN_BIN" "${args[@]}" >"$run_dir/argv.nul"

    note "running $process_label"
    assert_experiment_seals "before-$process_label"
    check_measurement_conflicts "$run_dir/processes-immediate-before.txt"
    run_held_workload "$process_label" "$run_dir" timed "$log" \
        "$run_dir/exit-status" \
        /usr/bin/time \
        -f $'process_wall_seconds\t%e\nprocess_user_seconds\t%U\nprocess_system_seconds\t%S\nmax_rss_kib\t%M\nexit_status\t%x' \
        -o "$time_file" "$RUN_BIN" "${args[@]}"
    assert_experiment_seals "after-$process_label"
    snapshot_pressure "$run_dir/pressure-after.txt"
    check_measurement_conflicts "$run_dir/processes-after.txt"
    snapshot_residency "$process_label" "$block" "$execution_mode" after-run \
        "$run_dir/residency-after-run.nul" >/dev/null
    seal_process_leaves "$run_dir" run-leaves.sha256 \
        argv.nul processes-before.txt processes-immediate-before.txt \
        timed.guardian-immediate-conflicts.json timed.guardian-control.json \
        timed.guardian-ready timed.guardian-launch \
        timed.guardian-samples.tsv timed.guardian-conflicts.tsv \
        timed.guardian-summary.json timed.guardian.log \
        timed.guardian-exit-status \
        pressure-before.txt residency-after-evict.nul raw.json report.md \
        query.log time.tsv exit-status pressure-after.txt processes-after.txt \
        residency-after-run.nul
    assert_experiment_seals "sealed-$process_label"

    wall_seconds="$(read_time_value process_wall_seconds "$time_file")"
    user_seconds="$(read_time_value process_user_seconds "$time_file")"
    system_seconds="$(read_time_value process_system_seconds "$time_file")"
    max_rss_kib="$(read_time_value max_rss_kib "$time_file")"
    [[ "$wall_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not parse wall time"
    [[ "$user_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not parse user time"
    [[ "$system_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "could not parse system time"
    [[ "$max_rss_kib" =~ ^[1-9][0-9]*$ ]] || die "could not parse maximum RSS"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$query_name" "$evidence_class" "$block" \
        "$order_index" "$execution_mode" "$BINARY_SHA256" "$SEGMENTS_DIR" \
        "$raw" "$wall_seconds" "$user_seconds" "$system_seconds" \
        "$max_rss_kib" >>"$RAW_INDEX"
}

while IFS=$'\t' read -r process_label query_name evidence_class block \
    order_index execution_mode; do
    [[ "$process_label" != "process_label" ]] || continue
    run_process "$process_label" "$query_name" "$evidence_class" "$block" \
        "$order_index" "$execution_mode"
done <"$RUN_PLAN"

note "re-inventorying the corpus to prove it remained immutable"
run_gate inventory \
    --corpus "$SEGMENTS_DIR" \
    --output "$INVENTORY_DIR/after.json" \
    --paths-output "$INVENTORY_DIR/files-after.nul"
cmp -s "$INVENTORY_DIR/before.json" "$INVENTORY_DIR/after.json" \
    || die "Schema 8 corpus changed during the benchmark"
cmp -s "$INVENTORY_DIR/files.nul" "$INVENTORY_DIR/files-after.nul" \
    || die "Schema 8 corpus path set changed during the benchmark"
sha256sum "$INVENTORY_DIR/after.json" >"$INVENTORY_DIR/after.sha256"

run_gate compare-results \
    --index "$RAW_INDEX" \
    --manifest "$NORMALIZED_JSON" \
    --source-manifest "$FROZEN_MANIFEST" \
    --inventory-before "$INVENTORY_DIR/before.json" \
    --inventory-after "$INVENTORY_DIR/after.json" \
    --residency "$RESIDENCY_SUMMARY" \
    --footer-validation "$VALIDATION_DIR/footer.json" \
    --readback-validation "$VALIDATION_DIR/readbacks.json" \
    --summary "$RESULT_DIR/summary.tsv" \
    --output "$COMPARISONS_DIR/result-gate.json" \
    --binary "$RUN_BIN" \
    --corpus "$SEGMENTS_DIR" \
    --runs-dir "$RUNS_DIR" \
    --max-resident-bytes-after-evict "$MAX_RESIDENT_BYTES_AFTER_EVICT" \
    --quiet-host-confirmed "$QUIET_HOST_CONFIRMED" \
    --allow-noisy-host "$ALLOW_NOISY_HOST" \
    --run-note-file "$METADATA_DIR/run-note.txt" \
    || die "strict Phase 4 correctness/accounting gate failed"

run_gate verify-leaf-evidence --result-dir "$RESULT_DIR" \
    || die "leaf-derived Phase 4 evidence recomputation failed"

run_gate check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    >"$BUILD_DIR/source-check-final.json"
sha256sum --check --strict "$SOURCE_ARCHIVE_SHA256" \
    >"$BUILD_DIR/source-archive-check-final.txt"
run_gate check-source-snapshot-seal \
    --repo "$REPO_ROOT" --snapshot "$BUILD_SOURCE_DIR" \
    --source-seal "$SOURCE_SEAL" --seal "$SOURCE_SNAPSHOT_SEAL" \
    >"$BUILD_DIR/source-snapshot-check-final.json"
run_gate check-cargo-config-isolation \
    --snapshot "$BUILD_SOURCE_DIR" --cargo-home "$BUILD_CARGO_HOME" \
    >"$BUILD_DIR/cargo-config-isolation-final.json"
chmod 0444 -- "$BUILD_DIR"/*-final.json \
    "$BUILD_DIR/source-archive-check-final.txt"

BUILD_PROVENANCE_PATHS=(
    "${BUILD_INPUT_PATHS[@]}"
    metadata/build/build-input-provenance.sha256
    metadata/build/source-check-final.json
    metadata/build/source-snapshot-check-final.json
    metadata/build/source-archive-check-final.txt
    metadata/build/cargo-config-isolation-final.json
)
(
    cd "$RESULT_DIR"
    sha256sum -- "${BUILD_PROVENANCE_PATHS[@]}"
) >"$BUILD_DIR/build-provenance.sha256"
chmod 0444 -- "$BUILD_DIR/build-provenance.sha256"
assert_experiment_seals final-authorities
chmod 0444 -- "$METADATA_DIR/settings.txt" "$METADATA_DIR/run-note.txt" \
    "$METADATA_DIR/environment.txt" "$METADATA_DIR/seal-checks.tsv" \
    "$INVENTORY_DIR/after.json" "$INVENTORY_DIR/files-after.nul" \
    "$INVENTORY_DIR/after.sha256" "$RAW_INDEX" "$RESIDENCY_SUMMARY" \
    "$RESULT_DIR/summary.tsv" "$COMPARISONS_DIR/result-gate.json"
touch "$RESULT_DIR/COMPLETE"
chmod 0444 -- "$RESULT_DIR/COMPLETE"

run_gate final-artifact-inventory \
    --result-dir "$RESULT_DIR" \
    --files-output "$METADATA_DIR/result-artifacts.nul" \
    --directories-output "$METADATA_DIR/result-directories.nul"
chmod 0444 -- "$METADATA_DIR/result-artifacts.nul" \
    "$METADATA_DIR/result-directories.nul"
(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        [[ "$artifact" != /* && "$artifact" != ".." && "$artifact" != ../* \
            && -f "$artifact" && ! -L "$artifact" ]] \
            || die "final artifact inventory contains an unsafe entry: $artifact"
        sha256sum -- "$artifact"
    done <metadata/result-artifacts.nul
    sha256sum -- metadata/result-artifacts.nul metadata/result-directories.nul
) >"$METADATA_DIR/result-artifacts.sha256"
chmod 0444 -- "$METADATA_DIR/result-artifacts.sha256"
(
    cd "$RESULT_DIR"
    sha256sum --check --strict metadata/result-artifacts.sha256 >/dev/null
) || die "final result artifact seal did not validate"

run_gate verify-seal --result-dir "$RESULT_DIR"
note "complete diagnostic artifact: $RESULT_DIR"
