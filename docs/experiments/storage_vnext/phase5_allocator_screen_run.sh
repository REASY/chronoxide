#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE="$SCRIPT_DIR/phase5_allocator_screen_gate.py"
PLAN="$SCRIPT_DIR/phase5_allocator_screen_plan.json"
PHASE1_GATE="$SCRIPT_DIR/phase1_replay_gate.py"
PHASE1_EXPECTATIONS="$SCRIPT_DIR/phase1_4m_expectations.json"
REPORT_GATE="$SCRIPT_DIR/ab_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DEFAULT_CAPTURE="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001"
DEFAULT_CONFIG_TEMPLATE="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/post-adaptive-head-profile-20260716-223717/config.toml"

CAPTURE="${CAPTURE:-$DEFAULT_CAPTURE}"
CONFIG_TEMPLATE="${CONFIG_TEMPLATE:-$DEFAULT_CONFIG_TEMPLATE}"
REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
RESULT_DIR="${RESULT_DIR:-}"
RUN_NOTE="${RUN_NOTE:-}"
RUST_LOG_OVERRIDE_PRESENT="${RUST_LOG_VALUE+x}"
RUST_LOG_VALUE='chronoxide_ingester=info,chronoxide_core=warn'
DRY_RUN=0
VALIDATE_ONLY=0

usage() {
    printf '%s\n' \
        'Usage:' \
        "  RESULT_DIR=/absolute/new/external/result-root \\" \
        "  REPO_ROOT=/absolute/clean/chronoxide-worktree \\" \
        "  RUN_NOTE='quiet host; no builds, profilers, footer scans, or unrelated databases active' \\" \
        '    docs/experiments/storage_vnext/phase5_allocator_screen_run.sh [--dry-run|--validate-only]' \
        '' \
        'The measured plan is frozen to ten 250k-message replays:' \
        'S,J0,J1,J2,J3,J3,J2,J1,J0,S. It always uses perf stat, external' \
        '/proc RSS sampling, capture eviction, and a 30-second post-Ingester-drop hold.' \
        'Both allocator binaries and the query/verification tools are built by this runner' \
        'from one clean commit under a sanitized, hash-bound build environment.' \
        'The hold is excluded from checkpoint workload wall time but included in GNU time' \
        'and perf full-process scope. Output paths are never reused or deleted.'
}

die() {
    printf 'Phase 5 allocator screen: %s\n' "$*" >&2
    exit 2
}

note() {
    printf 'Phase 5 allocator screen: %s\n' "$*"
}

assert_jemalloc_host_sources_absent() {
    for path in /etc/malloc.conf /etc/_rjem_malloc.conf; do
        [[ ! -e "$path" && ! -L "$path" ]] \
            || die "ambient jemalloc configuration source is forbidden: $path"
    done
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

require_executable() {
    local name="$1"
    local path="$2"
    [[ "$path" == /* && -f "$path" && ! -L "$path" && -x "$path" ]] \
        || die "$name must be an absolute executable non-symlink regular file: $path"
}

for argument in "$@"; do
    case "$argument" in
        --dry-run) DRY_RUN=1 ;;
        --validate-only) VALIDATE_ONLY=1 ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; die "unknown argument: $argument" ;;
    esac
done
(( DRY_RUN + VALIDATE_ONLY <= 1 )) \
    || die "--dry-run and --validate-only are mutually exclusive"
for forbidden_input in SYSTEM_BIN JEMALLOC_BIN QUERY_BIN STORAGE_VERIFY_BIN; do
    [[ ! -v "$forbidden_input" ]] \
        || die "$forbidden_input is not accepted; the runner controls builds and logging"
done
[[ -z "$RUST_LOG_OVERRIDE_PRESENT" ]] \
    || die "RUST_LOG_VALUE is not accepted; the runner freezes measured logging"
[[ -z "${LD_PRELOAD-}" ]] \
    || die "ambient LD_PRELOAD is forbidden; start from a sanitized shell"
[[ -z "${MALLOC_CONF-}" ]] \
    || die "ambient MALLOC_CONF is forbidden; start from a sanitized shell"
[[ -z "${_RJEM_MALLOC_CONF-}" ]] \
    || die "ambient _RJEM_MALLOC_CONF is forbidden; the runner sets policy per comparator"
for forbidden_environment in \
        LD_LIBRARY_PATH \
        MALLOC_ARENA_MAX MALLOC_ARENA_TEST MALLOC_TRIM_THRESHOLD_ MALLOC_TOP_PAD_ \
        MALLOC_MMAP_THRESHOLD_ MALLOC_MMAP_MAX_ MALLOC_CHECK_ MALLOC_PERTURB_ \
        GLIBC_TUNABLES JEMALLOC_SYS_WITH_MALLOC_CONF \
        RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER \
        CFLAGS CXXFLAGS CPPFLAGS LDFLAGS CC CXX AR RANLIB \
        PYTHONPATH PYTHONHOME PYTHONUSERBASE PYTHONSTARTUP PYTHONINSPECT; do
    [[ -z "${!forbidden_environment-}" ]] \
        || die "ambient $forbidden_environment is forbidden; start from a sanitized shell"
done
while IFS='=' read -r environment_name _; do
    case "$environment_name" in
        GIT_*) die "ambient $environment_name is forbidden for source sealing and archiving" ;;
    esac
done < <(env)

for command in awk bash cargo cc chmod cmp cp date df diff env file fincore find git grep mkdir \
        perf ps python3 readelf realpath sha256sum sort stat tail touch uname \
        xargs /usr/bin/time; do
    require_command "$command"
done
PYTHON_BIN="$(realpath -e -- "$(command -v python3)")"
require_executable python3 "$PYTHON_BIN"
PYTHON_BIN_SHA256="$(sha256sum -- "$PYTHON_BIN" | awk '{print $1}')"
PYTHON_VERSION="$("$PYTHON_BIN" -I -S -B --version 2>&1)"
PYTHON_FLAGS_PROBE="$("$PYTHON_BIN" -I -S -B -c \
    'import sys; print(":".join(str(int(value)) for value in (sys.flags.isolated, sys.flags.no_site, sys.flags.dont_write_bytecode, sys.flags.ignore_environment, sys.flags.safe_path)))')"
[[ "$PYTHON_FLAGS_PROBE" == "1:1:1:1:1" ]] \
    || die "python3 does not honor the required -I -S -B isolation flags"
PYTHON_SCRIPT_BOOTSTRAP='import os,stat,sys; script=os.path.realpath(sys.argv[1]); mode=os.lstat(script).st_mode; assert stat.S_ISREG(mode) and not os.path.islink(script); sys.argv=sys.argv[1:]; namespace={"__name__":"__main__","__file__":script,"__package__":None,"__cached__":None}; source=open(script,"rb").read(); exec(compile(source,script,"exec",dont_inherit=True),namespace)'
assert_python_interpreter() {
    [[ -f "$PYTHON_BIN" && ! -L "$PYTHON_BIN" && -x "$PYTHON_BIN" ]] \
        || die "pinned python3 interpreter changed type"
    [[ "$(sha256sum -- "$PYTHON_BIN" | awk '{print $1}')" == "$PYTHON_BIN_SHA256" ]] \
        || die "pinned python3 interpreter bytes changed"
}
python3() {
    local script
    local status
    local -a command
    assert_python_interpreter
    if (( $# > 0 )) && [[ "$1" == *.py ]]; then
        script="$1"
        shift
        command=("$PYTHON_BIN" -I -S -B -c "$PYTHON_SCRIPT_BOOTSTRAP" "$script" "$@")
    else
        command=("$PYTHON_BIN" -I -S -B "$@")
    fi
    if "${command[@]}"; then
        status=0
    else
        status=$?
    fi
    assert_python_interpreter
    return "$status"
}
python3_background() {
    local script
    local -a command
    assert_python_interpreter
    if (( $# > 0 )) && [[ "$1" == *.py ]]; then
        script="$1"
        shift
        command=("$PYTHON_BIN" -I -S -B -c "$PYTHON_SCRIPT_BOOTSTRAP" "$script" "$@")
    else
        command=("$PYTHON_BIN" -I -S -B "$@")
    fi
    exec "${command[@]}"
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
readonly PYTHON_BIN PYTHON_BIN_SHA256 PYTHON_VERSION PYTHON_FLAGS_PROBE \
    PYTHON_SCRIPT_BOOTSTRAP
verify_background_python_pid_binding
for file in "$GATE" "$PLAN" "$PHASE1_GATE" "$PHASE1_EXPECTATIONS" \
        "$REPORT_GATE" "$FADVISE_SOURCE"; do
    [[ -f "$file" && ! -L "$file" ]] || die "required harness file is missing: $file"
done

[[ "$CAPTURE" == /* && -d "$CAPTURE" ]] || die "CAPTURE must be an absolute directory"
[[ "$CONFIG_TEMPLATE" == /* && -f "$CONFIG_TEMPLATE" ]] \
    || die "CONFIG_TEMPLATE must be an absolute regular file"
[[ "$REPO_ROOT" == /* && -d "$REPO_ROOT" ]] || die "REPO_ROOT must be absolute"
CAPTURE="$(realpath -e -- "$CAPTURE")"
CONFIG_TEMPLATE="$(realpath -e -- "$CONFIG_TEMPLATE")"
REPO_ROOT="$(realpath -e -- "$REPO_ROOT")"
[[ "$(git -C "$REPO_ROOT" rev-parse --show-toplevel)" == "$REPO_ROOT" ]] \
    || die "REPO_ROOT must be the Git worktree root"
[[ "$SCRIPT_DIR" == "$REPO_ROOT/docs/experiments/storage_vnext" ]] \
    || die "the executing screen runner must come from the selected REPO_ROOT"

python3 "$GATE" validate-plan \
    --plan "$PLAN" --phase1-expectations "$PHASE1_EXPECTATIONS" >/dev/null
note "validating the pinned Phase 1 capture and configuration template"
CAPTURE_INPUTS_BEFORE="$(python3 "$PHASE1_GATE" validate-inputs \
    --capture "$CAPTURE" \
    --template "$CONFIG_TEMPLATE" \
    --expectations "$PHASE1_EXPECTATIONS")"

[[ -n "$RESULT_DIR" && "$RESULT_DIR" == /* ]] \
    || die "RESULT_DIR must be a new absolute external path"
result_name="$(basename "$RESULT_DIR")"
[[ -n "$result_name" && "$result_name" != "." && "$result_name" != ".." ]] \
    || die "RESULT_DIR must name a child of an existing directory"
result_parent_input="$(dirname "$RESULT_DIR")"
[[ -d "$result_parent_input" ]] || die "RESULT_DIR parent does not exist"
result_parent="$(realpath -e -- "$result_parent_input")"
RESULT_DIR="$result_parent/$result_name"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists: $RESULT_DIR"
case "$RESULT_DIR/" in
    "$REPO_ROOT/"*) die "RESULT_DIR must be outside the source worktree" ;;
    "$CAPTURE/"*) die "RESULT_DIR must not be inside the capture" ;;
esac
for path in "$CAPTURE" "$CONFIG_TEMPLATE" "$REPO_ROOT" "$RESULT_DIR"; do
    [[ "$path" != *$'\n'* && "$path" != *$'\t'* ]] \
        || die "paths must not contain tabs or newlines"
done

if [[ "$VALIDATE_ONLY" == "1" ]]; then
    note "validation complete; RESULT_DIR was not created: $RESULT_DIR"
    exit 0
fi
if [[ "$DRY_RUN" != "1" ]]; then
    [[ -n "$RUN_NOTE" && "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
        || die "RUN_NOTE is required and must be one line"
    [[ -z "$(git -C "$REPO_ROOT" status --porcelain)" ]] \
        || die "measured runs require a clean source worktree"
fi
[[ -z "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=normal)" ]] \
    || die "controlled allocator builds require one clean source worktree"
for cargo_config in "$HOME/.cargo/config" "$HOME/.cargo/config.toml"; do
    [[ ! -e "$cargo_config" ]] \
        || die "untracked Cargo home configuration is forbidden: $cargo_config"
done
ancestor="$(dirname "$REPO_ROOT")"
while [[ "$ancestor" != "/" ]]; do
    for cargo_config in "$ancestor/.cargo/config" "$ancestor/.cargo/config.toml"; do
        [[ ! -e "$cargo_config" ]] \
            || die "ancestor Cargo configuration is forbidden: $cargo_config"
    done
    ancestor="$(dirname "$ancestor")"
done

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/configs" "$RESULT_DIR/metadata" "$RESULT_DIR/runs" \
    "$RESULT_DIR/calibration" "$RESULT_DIR/validation" "$RESULT_DIR/comparisons"
CONFIG_DIR="$RESULT_DIR/configs"
METADATA_DIR="$RESULT_DIR/metadata"
RUNS_DIR="$RESULT_DIR/runs"
CALIBRATION_DIR="$RESULT_DIR/calibration"
VALIDATION_DIR="$RESULT_DIR/validation"
COMPARISONS_DIR="$RESULT_DIR/comparisons"
BINARY_DIR="$METADATA_DIR/binaries"
HARNESS_DIR="$METADATA_DIR/harness"
SOURCE_DIR="$METADATA_DIR/source"
TOOLS_DIR="$METADATA_DIR/tools"
PREFLIGHT_DIR="$METADATA_DIR/preflight"
RAW_AUTHORITY_DIR="$METADATA_DIR/raw-authorities"
mkdir "$BINARY_DIR" "$HARNESS_DIR" "$SOURCE_DIR" "$TOOLS_DIR" "$PREFLIGHT_DIR" \
    "$RAW_AUTHORITY_DIR"
PYTHON_RECORD="$METADATA_DIR/python-interpreter.txt"
{
    printf 'path=%s\n' "$PYTHON_BIN"
    printf 'sha256=%s\n' "$PYTHON_BIN_SHA256"
    printf 'version=%s\n' "$PYTHON_VERSION"
    printf 'flags_isolated_no_site_no_bytecode_ignore_environment_safe_path=%s\n' \
        "$PYTHON_FLAGS_PROBE"
} >"$PYTHON_RECORD"
chmod 0444 -- "$PYTHON_RECORD"
printf '%s\n' "$CAPTURE_INPUTS_BEFORE" >"$METADATA_DIR/capture-inputs-before.json"
chmod 0444 -- "$METADATA_DIR/capture-inputs-before.json"
printf '%s\n' \
    'This result is partial and non-promotable unless both COMPLETE and' \
    'comparisons/final-screen-decision.json exist and pass the frozen gate.' \
    >"$RESULT_DIR/PARTIAL_UNLESS_COMPLETE.txt"
active_lifecycle=0
active_run_dir=''
active_guardian_control=''
active_guardian_ready=''
active_guardian_launch=''
active_root_pid=''
active_root_starttime_ticks=''
active_rss_pid=''
active_rss_starttime_ticks=''
active_guardian_pid=''
active_guardian_starttime_ticks=''

# Emergency cleanup must still run after the fail-fast Python wrapper rejects
# interpreter drift. Use its captured absolute path without re-entering die().
cleanup_python3() {
    local script="$1"
    shift
    "$PYTHON_BIN" -I -S -B -c "$PYTHON_SCRIPT_BOOTSTRAP" "$script" "$@"
}

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

record_cleanup_reap() {
    local role="$1" status="$2" detail="$3"
    [[ -n "$active_run_dir" && -d "$active_run_dir" ]] || return 0
    printf '%s\t%s\t%s\n' "$role" "$status" "$detail" \
        >>"$active_run_dir/interrupted-cleanup-reap.tsv"
}

stop_bound_tree() {
    local role="$1" pid="$2" starttime_ticks="$3"
    local cleanup_gate="${FROZEN_GATE:-$GATE}"
    [[ -n "$pid" ]] || return 0
    if [[ -z "$starttime_ticks" ]]; then
        note "refusing to signal unbound $role PID $pid"
        record_cleanup_reap "$role" unbound-signal-refused "pid=$pid"
        return 1
    fi
    cleanup_python3 "$cleanup_gate" terminate-process-tree \
        --root-pid "$pid" --root-starttime-ticks "$starttime_ticks" \
        >"$active_run_dir/interrupted-$role-termination.json" 2>&1 || true
}

bounded_reap_job() {
    local role="$1" pid="$2" expected_starttime_ticks="$3"
    local attempt state current_starttime_ticks identity
    [[ -n "$pid" ]] || return 0
    if [[ -z "$expected_starttime_ticks" ]]; then
        note "refusing an unbounded wait for unbound $role PID $pid"
        record_cleanup_reap "$role" unbound-refused "pid=$pid"
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
            record_cleanup_reap "$role" reused-refused "pid=$pid"
            return 1
        fi
        if [[ "$state" == Z || "$state" == X || "$state" == x ]]; then
            wait "$pid" 2>/dev/null || true
            record_cleanup_reap "$role" reaped-dead "pid=$pid state=$state"
            return 0
        fi
        sleep 0.01
    done
    record_cleanup_reap "$role" timeout-live "pid=$pid"
    return 1
}

clear_active_processes() {
    active_run_dir=''
    active_guardian_control=''
    active_guardian_ready=''
    active_guardian_launch=''
    active_root_pid=''
    active_root_starttime_ticks=''
    active_rss_pid=''
    active_rss_starttime_ticks=''
    active_guardian_pid=''
    active_guardian_starttime_ticks=''
    active_lifecycle=0
}

stop_active_children() {
    local cleanup_gate="${FROZEN_GATE:-$GATE}" controlled=0
    trap '' HUP INT TERM
    if [[ -n "$active_guardian_control" && -f "$active_guardian_control" \
        && ! -L "$active_guardian_control" ]]; then
        if cleanup_python3 "$cleanup_gate" cleanup-guardian-processes \
            --control "$active_guardian_control" --ready "$active_guardian_ready" \
            --launch "$active_guardian_launch" --interval-ms 100 \
            >"$active_run_dir/interrupted-guardian-cleanup.json" 2>&1; then
            controlled=1
        fi
    fi
    if [[ "$controlled" == 0 ]]; then
        stop_bound_tree root "$active_root_pid" "$active_root_starttime_ticks" || true
        stop_bound_tree rss-monitor "$active_rss_pid" \
            "$active_rss_starttime_ticks" || true
        stop_bound_tree guardian "$active_guardian_pid" \
            "$active_guardian_starttime_ticks" || true
    fi
    bounded_reap_job root "$active_root_pid" "$active_root_starttime_ticks" || true
    bounded_reap_job rss-monitor "$active_rss_pid" \
        "$active_rss_starttime_ticks" || true
    bounded_reap_job guardian "$active_guardian_pid" \
        "$active_guardian_starttime_ticks" || true
    clear_active_processes
}

cleanup_signal_pending=0
cleanup_signal_exit() { exit 130; }
defer_cleanup_signals() { trap 'cleanup_signal_pending=1' HUP INT TERM; }
arm_cleanup_signals() {
    trap 'cleanup_signal_exit' HUP INT TERM
    if [[ "$cleanup_signal_pending" == 1 ]]; then
        cleanup_signal_pending=0
        cleanup_signal_exit
    fi
}
cleanup_on_exit() {
    local exit_status="$1"
    trap - EXIT
    trap '' HUP INT TERM
    set +e
    if [[ "$active_lifecycle" == 1 ]]; then
        stop_active_children || true
    fi
    exit "$exit_status"
}
arm_cleanup_signals
trap 'cleanup_on_exit "$?"' EXIT

HARNESS_FILES=(
    phase5_allocator_screen_run.sh phase5_allocator_profile_run.sh
    phase5_allocator_screen_gate.py
    phase5_allocator_screen_plan.json test_phase5_allocator_screen_gate.py
    phase1_replay_gate.py phase1_4m_expectations.json ab_gate.py
    fadvise_regular_dontneed.c README.md
)
for harness_file in "${HARNESS_FILES[@]}"; do
    [[ -f "$SCRIPT_DIR/$harness_file" ]] || die "harness file is missing: $harness_file"
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" "$HARNESS_DIR/$harness_file"
done
chmod -R a-w -- "$HARNESS_DIR"
FROZEN_GATE="$HARNESS_DIR/phase5_allocator_screen_gate.py"
FROZEN_PLAN="$HARNESS_DIR/phase5_allocator_screen_plan.json"
FROZEN_PHASE1_GATE="$HARNESS_DIR/phase1_replay_gate.py"
FROZEN_EXPECTATIONS="$HARNESS_DIR/phase1_4m_expectations.json"
FROZEN_REPORT_GATE="$HARNESS_DIR/ab_gate.py"
HARNESS_SEAL="$METADATA_DIR/harness.sha256"
sha256sum "${HARNESS_FILES[@]/#/$HARNESS_DIR/}" >"$HARNESS_SEAL"
chmod 0444 -- "$HARNESS_SEAL"
HARNESS_SEAL_SHA256="$(sha256sum -- "$HARNESS_SEAL" | awk '{print $1}')"
assert_harness_seal() {
    local harness_file
    [[ -d "$HARNESS_DIR" && ! -L "$HARNESS_DIR" && ! -w "$HARNESS_DIR" ]] \
        || die "frozen harness directory is mutable"
    [[ -f "$HARNESS_SEAL" && ! -L "$HARNESS_SEAL" && \
        "$(stat -c '%a' -- "$HARNESS_SEAL")" == "444" ]] \
        || die "frozen harness authority changed type or mode"
    [[ "$(sha256sum -- "$HARNESS_SEAL" | awk '{print $1}')" == "$HARNESS_SEAL_SHA256" ]] \
        || die "frozen harness authority changed"
    sha256sum --check --strict "$HARNESS_SEAL" >/dev/null \
        || die "frozen harness bytes changed"
    for harness_file in "${HARNESS_FILES[@]}"; do
        [[ -f "$HARNESS_DIR/$harness_file" && ! -L "$HARNESS_DIR/$harness_file" && \
            ! -w "$HARNESS_DIR/$harness_file" ]] \
            || die "frozen harness input is mutable: $harness_file"
    done
}
assert_harness_seal
python3 "$FROZEN_GATE" validate-plan \
    --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
    --output "$METADATA_DIR/validated-plan.json" >/dev/null
chmod 0444 -- "$METADATA_DIR/validated-plan.json"
SOURCE_SEAL="$SOURCE_DIR/formal-source-seal.json"
python3 "$FROZEN_GATE" source-seal --repo "$REPO_ROOT" --output "$SOURCE_SEAL"
chmod 0444 -- "$SOURCE_SEAL"
SOURCE_ARCHIVE="$SOURCE_DIR/git-head.tar"
BUILD_SOURCE="$RESULT_DIR/build-source"
EXTRACTED_SOURCE_SEAL="$SOURCE_DIR/extracted-build-source-seal.json"
python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    --output "$SOURCE_DIR/source-check-before-archive.json"
SEALED_HEAD="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["git_head"])' "$SOURCE_SEAL")"
git -C "$REPO_ROOT" archive --format=tar --output="$SOURCE_ARCHIVE" "$SEALED_HEAD"
python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    --output "$SOURCE_DIR/source-check-after-archive.json"
chmod 0444 -- "$SOURCE_ARCHIVE"
python3 "$FROZEN_GATE" extract-git-archive \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
    --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
    --output "$EXTRACTED_SOURCE_SEAL"
chmod 0444 -- "$EXTRACTED_SOURCE_SEAL"
python3 "$FROZEN_GATE" check-extracted-source-seal \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
    --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
    --seal "$EXTRACTED_SOURCE_SEAL" \
    --output "$SOURCE_DIR/extracted-source-check-before-builds.json"

assert_harness_source_binding() {
    local harness_file frozen source_file frozen_mode source_mode
    for harness_file in "${HARNESS_FILES[@]}"; do
        frozen="$HARNESS_DIR/$harness_file"
        source_file="$BUILD_SOURCE/docs/experiments/storage_vnext/$harness_file"
        [[ -f "$source_file" && ! -L "$source_file" ]] \
            || die "sealed HEAD lacks harness input: $harness_file"
        cmp -s -- "$frozen" "$source_file" \
            || die "frozen harness differs from sealed HEAD: $harness_file"
        frozen_mode="$(stat -c '%a' -- "$frozen")"
        source_mode="$(stat -c '%a' -- "$source_file")"
        [[ "$frozen_mode" == "$source_mode" ]] \
            || die "frozen harness mode differs from sealed HEAD: $harness_file"
    done
}
assert_harness_source_binding

printf 'role\tsource_path\tpreserved_path\tsha256\n' >"$METADATA_DIR/binaries.tsv"
preserve_binary() {
    local role="$1"
    local source="$2"
    local destination="$BINARY_DIR/$role"
    local source_hash
    local destination_hash
    cp --reflink=auto --preserve=mode,timestamps -- "$source" "$destination"
    source_hash="$(sha256sum -- "$source" | awk '{print $1}')"
    destination_hash="$(sha256sum -- "$destination" | awk '{print $1}')"
    [[ "$source_hash" == "$destination_hash" ]] \
        || die "preserved binary differs from source: $role"
    chmod 0555 -- "$destination"
    [[ "$(stat -c '%a' -- "$destination")" == "555" ]] \
        || die "preserved binary must be executable and non-writable: $role"
    printf '%s\t%s\t%s\t%s\n' "$role" "$source" "$destination" "$destination_hash" \
        >>"$METADATA_DIR/binaries.tsv"
}
BUILD_TARGET="$RESULT_DIR/build-target"
BUILD_LOG_DIR="$METADATA_DIR/build"
mkdir "$BUILD_TARGET" "$BUILD_LOG_DIR"
CARGO_BIN="$(command -v cargo)"
assert_jemalloc_host_sources_absent
[[ "$CARGO_BIN" == /* && -x "$CARGO_BIN" ]] || die "cargo must resolve to an absolute executable"
BUILD_PATH="$HOME/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
BUILD_RUSTC="$HOME/.cargo/bin/rustc"
BUILD_RUSTDOC="$HOME/.cargo/bin/rustdoc"
for rustup_proxy in "$CARGO_BIN" "$BUILD_RUSTC" "$BUILD_RUSTDOC"; do
    [[ -x "$rustup_proxy" && "$(realpath -e -- "$rustup_proxy")" == \
        "$(realpath -e -- "$CARGO_BIN")" ]] \
        || die "controlled Rust tool proxy is invalid: $rustup_proxy"
done
SYSTEM_BUILD_COMMAND='cargo build --manifest-path Cargo.toml --locked --release --no-default-features -p chronoxide-ingester -p chronoxide-query-cli --bin chronoxide-ingester --bin chronoxide-query --bin chronoxide-storage-verify'
JEMALLOC_BUILD_COMMAND='cargo build --manifest-path Cargo.toml --locked --release --no-default-features --features jemalloc-stats -p chronoxide-ingester --bin chronoxide-ingester'
NO_STATS_REVALIDATION_COMMAND='cargo build --manifest-path Cargo.toml --locked --release --no-default-features --features jemalloc -p chronoxide-ingester --bin chronoxide-ingester'
printf 'COMMAND\t%s\nCWD\t%s\nENV\tHOME=%s\tPATH=%s\tCARGO_HOME=%s/.cargo\tRUSTUP_HOME=%s/.rustup\tRUSTC=%s\tRUSTDOC=%s\tLC_ALL=C\tTZ=UTC\tCARGO_INCREMENTAL=0\tCARGO_TARGET_DIR=%s\n' \
    "$SYSTEM_BUILD_COMMAND" "$BUILD_SOURCE" "$HOME" "$BUILD_PATH" "$HOME" "$HOME" \
    "$BUILD_RUSTC" "$BUILD_RUSTDOC" "$BUILD_TARGET" >"$BUILD_LOG_DIR/system.log"
python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    --output "$BUILD_LOG_DIR/source-check-before-system-build.json"
python3 "$FROZEN_GATE" check-extracted-source-seal \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
    --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
    --seal "$EXTRACTED_SOURCE_SEAL" \
    --output "$BUILD_LOG_DIR/extracted-source-check-before-system-build.json"
(
    cd "$BUILD_SOURCE"
    env -i HOME="$HOME" PATH="$BUILD_PATH" CARGO_HOME="$HOME/.cargo" \
        RUSTUP_HOME="$HOME/.rustup" LC_ALL=C TZ=UTC \
        RUSTC="$BUILD_RUSTC" RUSTDOC="$BUILD_RUSTDOC" \
        CARGO_TARGET_DIR="$BUILD_TARGET" CARGO_INCREMENTAL=0 \
        "$CARGO_BIN" build --manifest-path Cargo.toml --locked --release --no-default-features \
        -p chronoxide-ingester -p chronoxide-query-cli \
        --bin chronoxide-ingester --bin chronoxide-query \
        --bin chronoxide-storage-verify
) >>"$BUILD_LOG_DIR/system.log" 2>&1
python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    --output "$BUILD_LOG_DIR/source-check-after-system-build.json"
python3 "$FROZEN_GATE" check-extracted-source-seal \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
    --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
    --seal "$EXTRACTED_SOURCE_SEAL" \
    --output "$BUILD_LOG_DIR/extracted-source-check-after-system-build.json"
preserve_binary chronoxide-ingester-system "$BUILD_TARGET/release/chronoxide-ingester"
preserve_binary chronoxide-query "$BUILD_TARGET/release/chronoxide-query"
preserve_binary chronoxide-storage-verify "$BUILD_TARGET/release/chronoxide-storage-verify"

printf 'COMMAND\t%s\nCWD\t%s\nENV\tHOME=%s\tPATH=%s\tCARGO_HOME=%s/.cargo\tRUSTUP_HOME=%s/.rustup\tRUSTC=%s\tRUSTDOC=%s\tLC_ALL=C\tTZ=UTC\tCARGO_INCREMENTAL=0\tCARGO_TARGET_DIR=%s\n' \
    "$JEMALLOC_BUILD_COMMAND" "$BUILD_SOURCE" "$HOME" "$BUILD_PATH" "$HOME" "$HOME" \
    "$BUILD_RUSTC" "$BUILD_RUSTDOC" "$BUILD_TARGET" >"$BUILD_LOG_DIR/jemalloc.log"
python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    --output "$BUILD_LOG_DIR/source-check-before-jemalloc-build.json"
python3 "$FROZEN_GATE" check-extracted-source-seal \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
    --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
    --seal "$EXTRACTED_SOURCE_SEAL" \
    --output "$BUILD_LOG_DIR/extracted-source-check-before-jemalloc-build.json"
(
    cd "$BUILD_SOURCE"
    env -i HOME="$HOME" PATH="$BUILD_PATH" CARGO_HOME="$HOME/.cargo" \
        RUSTUP_HOME="$HOME/.rustup" LC_ALL=C TZ=UTC \
        RUSTC="$BUILD_RUSTC" RUSTDOC="$BUILD_RUSTDOC" \
        CARGO_TARGET_DIR="$BUILD_TARGET" CARGO_INCREMENTAL=0 \
        "$CARGO_BIN" build --manifest-path Cargo.toml --locked --release \
        --no-default-features --features jemalloc-stats \
        -p chronoxide-ingester --bin chronoxide-ingester
) >>"$BUILD_LOG_DIR/jemalloc.log" 2>&1
python3 "$FROZEN_GATE" check-source-seal --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    --output "$BUILD_LOG_DIR/source-check-after-jemalloc-build.json"
python3 "$FROZEN_GATE" check-extracted-source-seal \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
    --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
    --seal "$EXTRACTED_SOURCE_SEAL" \
    --output "$BUILD_LOG_DIR/extracted-source-check-after-jemalloc-build.json"
preserve_binary chronoxide-ingester-jemalloc "$BUILD_TARGET/release/chronoxide-ingester"
RUN_SYSTEM="$BINARY_DIR/chronoxide-ingester-system"
RUN_JEMALLOC="$BINARY_DIR/chronoxide-ingester-jemalloc"
RUN_QUERY="$BINARY_DIR/chronoxide-query"
RUN_STORAGE_VERIFY="$BINARY_DIR/chronoxide-storage-verify"
RUN_SYSTEM_SHA256="$(sha256sum -- "$RUN_SYSTEM" | awk '{print $1}')"
RUN_JEMALLOC_SHA256="$(sha256sum -- "$RUN_JEMALLOC" | awk '{print $1}')"
[[ "$RUN_SYSTEM_SHA256" != "$RUN_JEMALLOC_SHA256" ]] \
    || die "preserved system and jemalloc binaries must have different hashes"
PRESERVED_BINARY_SEAL="$METADATA_DIR/preserved-binaries.sha256"
sha256sum "$RUN_SYSTEM" "$RUN_JEMALLOC" "$RUN_QUERY" "$RUN_STORAGE_VERIFY" \
    >"$PRESERVED_BINARY_SEAL"
chmod 0444 -- "$PRESERVED_BINARY_SEAL" "$METADATA_DIR/binaries.tsv"
BUILD_PROVENANCE="$METADATA_DIR/build-provenance.json"
python3 "$FROZEN_GATE" record-build-provenance \
    --repo "$REPO_ROOT" --target-dir "$BUILD_TARGET" --source-seal "$SOURCE_SEAL" \
    --build-source "$BUILD_SOURCE" --source-archive "$SOURCE_ARCHIVE" \
    --extracted-source-seal "$EXTRACTED_SOURCE_SEAL" \
    --system-binary "$RUN_SYSTEM" --jemalloc-binary "$RUN_JEMALLOC" \
    --query-binary "$RUN_QUERY" --storage-verify-binary "$RUN_STORAGE_VERIFY" \
    --system-log "$BUILD_LOG_DIR/system.log" --jemalloc-log "$BUILD_LOG_DIR/jemalloc.log" \
    --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
    --output "$BUILD_PROVENANCE"
chmod 0444 -- "$BUILD_PROVENANCE"

CORE_CONTROL_SEAL="$METADATA_DIR/core-controls.json"
core_control_inputs=(
    "$HARNESS_SEAL" "$METADATA_DIR/validated-plan.json" "$PYTHON_RECORD"
    "$SOURCE_SEAL" "$SOURCE_ARCHIVE" "$EXTRACTED_SOURCE_SEAL"
    "$METADATA_DIR/binaries.tsv" "$PRESERVED_BINARY_SEAL" "$BUILD_PROVENANCE"
    "$RUN_SYSTEM" "$RUN_JEMALLOC" "$RUN_QUERY" "$RUN_STORAGE_VERIFY"
)
for harness_file in "${HARNESS_FILES[@]}"; do
    core_control_inputs+=("$HARNESS_DIR/$harness_file")
done
core_control_args=()
for control_input in "${core_control_inputs[@]}"; do
    core_control_args+=(--input "$control_input")
done
python3 "$FROZEN_GATE" create-control-seal \
    "${core_control_args[@]}" --output "$CORE_CONTROL_SEAL"
chmod 0444 -- "$CORE_CONTROL_SEAL"
CORE_CONTROL_SEAL_SHA256="$(sha256sum -- "$CORE_CONTROL_SEAL" | awk '{print $1}')"
MEASUREMENT_CONTROL_READY=0

assert_control_seal_file() {
    local seal="$1"
    local expected_sha256="$2"
    local context="$3"
    [[ -f "$seal" && ! -L "$seal" && "$(stat -c '%a' -- "$seal")" == "444" ]] \
        || die "control-seal authority changed type or mode at $context: $seal"
    [[ "$(sha256sum -- "$seal" | awk '{print $1}')" == "$expected_sha256" ]] \
        || die "control-seal authority bytes changed at $context: $seal"
    python3 "$FROZEN_GATE" check-control-seal --seal "$seal" >/dev/null \
        || die "fixed control input changed at $context: $seal"
}

assert_experiment_seals() {
    local context="$1"
    assert_harness_seal
    assert_harness_source_binding
    assert_control_seal_file \
        "$CORE_CONTROL_SEAL" "$CORE_CONTROL_SEAL_SHA256" "$context-core"
    if [[ "$MEASUREMENT_CONTROL_READY" == "1" ]]; then
        assert_control_seal_file \
            "$MEASUREMENT_CONTROL_SEAL" "$MEASUREMENT_CONTROL_SEAL_SHA256" \
            "$context-measurement"
    fi
    python3 "$FROZEN_GATE" check-source-seal \
        --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" >/dev/null \
        || die "formal source seal changed at $context"
    python3 "$FROZEN_GATE" check-extracted-source-seal \
        --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
        --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
        --seal "$EXTRACTED_SOURCE_SEAL" --build-provenance "$BUILD_PROVENANCE" \
        >/dev/null \
        || die "extracted build-source seal changed at $context"
    python3 "$FROZEN_GATE" check-executable-set \
        --build-provenance "$BUILD_PROVENANCE" \
        --system-binary "$RUN_SYSTEM" --jemalloc-binary "$RUN_JEMALLOC" \
        --query-binary "$RUN_QUERY" --storage-verify-binary "$RUN_STORAGE_VERIFY" \
        >/dev/null || die "preserved executable seal changed at $context"
    assert_control_seal_file \
        "$CORE_CONTROL_SEAL" "$CORE_CONTROL_SEAL_SHA256" "$context-core-after"
    if [[ "$MEASUREMENT_CONTROL_READY" == "1" ]]; then
        assert_control_seal_file \
            "$MEASUREMENT_CONTROL_SEAL" "$MEASUREMENT_CONTROL_SEAL_SHA256" \
            "$context-measurement-after"
    fi
    assert_harness_seal
    assert_harness_source_binding
    printf '%s\t%s\n' "$(date --iso-8601=ns)" "$context" \
        >>"$METADATA_DIR/seal-checks.tsv"
}
assert_experiment_seals initial-preserved-executables
for binary in "$RUN_SYSTEM" "$RUN_JEMALLOC" "$RUN_QUERY" "$RUN_STORAGE_VERIFY"; do
    name="$(basename "$binary")"
    file -- "$binary" >"$METADATA_DIR/$name.file.txt" 2>&1
    readelf -n -- "$binary" >"$METADATA_DIR/$name.elf-notes.txt" 2>&1 || true
done
assert_experiment_seals before-query-help
"$RUN_QUERY" --help >"$METADATA_DIR/chronoxide-query.help.txt" 2>&1
assert_experiment_seals after-query-help
for flag in --segments-dir --storage-layout --sample-limit-per-kind --verify-readbacks --output; do
    grep -F -- "$flag" "$METADATA_DIR/chronoxide-query.help.txt" >/dev/null \
        || die "chronoxide-query lacks required interface: $flag"
done
assert_experiment_seals before-storage-verify-help
"$RUN_STORAGE_VERIFY" --help >"$METADATA_DIR/chronoxide-storage-verify.help.txt" 2>&1
assert_experiment_seals after-storage-verify-help
for flag in --segments-dir --schema --validate-segment-footers --verify-exact-postings; do
    grep -F -- "$flag" "$METADATA_DIR/chronoxide-storage-verify.help.txt" >/dev/null \
        || die "chronoxide-storage-verify lacks required interface: $flag"
done

git -C "$REPO_ROOT" rev-parse HEAD >"$SOURCE_DIR/git-head.txt"
git -C "$REPO_ROOT" status --porcelain=v2 --branch >"$SOURCE_DIR/git-status.txt"
git -C "$REPO_ROOT" remote -v >"$SOURCE_DIR/git-remotes.txt"
git -C "$REPO_ROOT" ls-files -s >"$SOURCE_DIR/tracked-index.txt"
git -C "$REPO_ROOT" diff --binary --full-index HEAD -- >"$SOURCE_DIR/tracked-combined.patch"
git -C "$REPO_ROOT" diff --cached --binary --full-index -- >"$SOURCE_DIR/tracked-index.patch"
git -C "$REPO_ROOT" diff --binary --full-index -- >"$SOURCE_DIR/tracked-worktree.patch"
git -C "$REPO_ROOT" ls-files --others --exclude-standard >"$SOURCE_DIR/untracked-paths.txt"
(
    cd "$REPO_ROOT"
    while IFS= read -r -d '' path; do
        [[ -f "$path" && ! -L "$path" ]] && sha256sum -z -- "$path"
    done < <(git ls-files -z)
) >"$SOURCE_DIR/tracked-working-tree.sha256.nul"
(
    cd "$REPO_ROOT"
    while IFS= read -r -d '' path; do
        [[ -f "$path" && ! -L "$path" ]] && sha256sum -z -- "$path"
    done < <(git ls-files --others --exclude-standard -z)
) >"$SOURCE_DIR/untracked-working-tree.sha256.nul"
sha256sum "$SOURCE_DIR/git-head.txt" "$SOURCE_DIR/tracked-index.txt" \
    "$SOURCE_DIR/tracked-combined.patch" "$SOURCE_DIR/tracked-working-tree.sha256.nul" \
    "$SOURCE_DIR/untracked-working-tree.sha256.nul" >"$SOURCE_DIR/source-state.sha256"

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'capture=%s\n' "$CAPTURE"
    printf 'config_template=%s\n' "$CONFIG_TEMPLATE"
    printf 'repo_root=%s\n' "$REPO_ROOT"
    printf 'result_dir=%s\n' "$RESULT_DIR"
    printf 'system_build_command=%s\n' "$SYSTEM_BUILD_COMMAND"
    printf 'jemalloc_build_command=%s\n' "$JEMALLOC_BUILD_COMMAND"
    printf 'later_no_stats_revalidation_command=%s\n' "$NO_STATS_REVALIDATION_COMMAND"
    printf 'formal_source_seal=%s\n' "$SOURCE_SEAL"
    printf 'build_provenance=%s\n' "$BUILD_PROVENANCE"
    printf 'jemalloc_screen_build_stats_enabled=%s\n' 'true'
    printf 'no_stats_production_build_validated=%s\n' 'false'
    printf 'rust_log=%s\n' "$RUST_LOG_VALUE"
    printf 'run_note=%s\n' "$RUN_NOTE"
    printf 'workload_wall_scope=%s\n' 'main entry through the ingester_dropped checkpoint; excludes the release hold'
    printf 'gnu_time_scope=%s\n' 'complete process including the 30-second release hold'
    printf 'perf_stat_scope=%s\n' 'complete process including the 30-second release hold'
    printf 'allocator_internal_telemetry=%s\n' 'epoch-refreshed jemalloc release stats; system allocator fields are explicit null'
    printf 'allocator_telemetry_self_observation=%s\n' 'both snapshots precede telemetry writer creation; epoch refresh/checkpoint machinery may still perturb allocator state'
    printf 'workload_cpu_scope=%s\n' 'non-double-counted process-tree utime+stime at first post-drop sample; CLK_TCK and boundary uncertainty recorded'
    printf 'workload_rss_scope=%s\n' 'workload-phase external process-tree peak plus boundary VmHWM; total lifecycle peak retained separately'
    printf 'external_conflict_scope=%s\n' 'continuous 100ms guardian for every measured process lifetime'
    printf 'quiescence_scope=%s\n' 'per-run corpus fsync followed by three Dirty+Writeback samples at or below 65536 KiB'
    printf 'footer_and_readback_scope=%s\n' 'one canonical byte-identical corpus, outside measured replay'
    printf 'promql_fingerprint_calibration=%s\n' 'fresh untimed 250k system replay before measured schedule; raw reports hash-bound'
    printf 'profiling_scope=%s\n' 'separate harness/result only; never part of A/B timing or RSS'
} >"$METADATA_DIR/settings.txt"
printf '%s\n' "$RUN_NOTE" >"$METADATA_DIR/run-note.txt"
cp --preserve=mode,timestamps -- "$CONFIG_TEMPLATE" "$METADATA_DIR/config-template.toml"
cp --preserve=mode,timestamps -- "$CAPTURE/manifest.json" "$METADATA_DIR/capture-manifest.json"
chmod 0444 -- "$METADATA_DIR/config-template.toml" "$METADATA_DIR/capture-manifest.json"
{
    date --iso-8601=seconds
    uname -a
    command -v lscpu >/dev/null 2>&1 && lscpu
    command -v rustc >/dev/null 2>&1 && rustc --version --verbose
    command -v cargo >/dev/null 2>&1 && cargo --version --verbose
    perf --version
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$CAPTURE"
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$RESULT_DIR"
    stat -f -c 'capture_filesystem_type=%T capture_mount=%m' "$CAPTURE"
    stat -f -c 'result_filesystem_type=%T result_mount=%m' "$RESULT_DIR"
    df -B1 "$RESULT_DIR"
    ulimit -a
    cat /proc/meminfo
    for pressure in /proc/pressure/cpu /proc/pressure/io /proc/pressure/memory; do
        [[ -r "$pressure" ]] && { printf '%s\n' "$pressure"; cat "$pressure"; }
    done
} >"$METADATA_DIR/environment.txt" 2>&1
ps -eo pid=,ppid=,pcpu=,pmem=,rss=,etime=,stat=,comm=,args= \
    >"$METADATA_DIR/processes-at-plan.txt"

policy_conf() {
    python3 -c 'import json,sys; value=json.load(open(sys.argv[1]))["policies"][sys.argv[2]]["jemalloc_conf"]; print("" if value is None else value)' \
        "$FROZEN_PLAN" "$1"
}
policy_binary() {
    if [[ "$1" == "S" ]]; then
        printf '%s\n' "$RUN_SYSTEM"
    else
        printf '%s\n' "$RUN_JEMALLOC"
    fi
}
policy_binary_sha256() {
    if [[ "$1" == "S" ]]; then
        printf '%s\n' "$RUN_SYSTEM_SHA256"
    else
        printf '%s\n' "$RUN_JEMALLOC_SHA256"
    fi
}
assert_policy_binary_unchanged() {
    local policy="$1"
    local binary="$2"
    local expected_hash
    local observed_hash
    expected_hash="$(policy_binary_sha256 "$policy")"
    observed_hash="$(sha256sum -- "$binary" | awk '{print $1}')"
    [[ "$observed_hash" == "$expected_hash" ]] \
        || die "$policy comparator binary changed: expected $expected_hash, got $observed_hash"
}

run_preflight() {
    local policy="$1"
    local binary
    local conf
    local -a command
    local -a source_audit_argument
    binary="$(policy_binary "$policy")"
    assert_jemalloc_host_sources_absent
    assert_policy_binary_unchanged "$policy" "$binary"
    conf="$(policy_conf "$policy")"
    command=(env -i LC_ALL=C TZ=UTC)
    [[ -n "$conf" ]] && command+=("_RJEM_MALLOC_CONF=$conf")
    command+=("$binary" --allocator-preflight)
    assert_experiment_seals "$policy-before-preflight"
    "${command[@]}" >"$PREFLIGHT_DIR/$policy.stdout" 2>"$PREFLIGHT_DIR/$policy.stderr"
    assert_experiment_seals "$policy-after-preflight"
    source_audit_argument=()
    if [[ "$policy" != "S" ]]; then
        if [[ "$policy" == "J0" ]]; then
            assert_experiment_seals "$policy-before-source-audit-preflight"
            env -i LC_ALL=C TZ=UTC \
                _RJEM_MALLOC_CONF='abort_conf:true,confirm_conf:true' \
                "$binary" --allocator-preflight \
                >"$PREFLIGHT_DIR/$policy.source-audit.stdout" \
                2>"$PREFLIGHT_DIR/$policy.source-audit.stderr"
            assert_experiment_seals "$policy-after-source-audit-preflight"
        else
            cp --preserve=mode,timestamps -- "$PREFLIGHT_DIR/$policy.stderr" \
                "$PREFLIGHT_DIR/$policy.source-audit.stderr"
        fi
        source_audit_argument=(--source-audit-stderr \
            "$PREFLIGHT_DIR/$policy.source-audit.stderr")
    fi
    python3 "$FROZEN_GATE" parse-preflight \
        --stdout "$PREFLIGHT_DIR/$policy.stdout" \
        --stderr "$PREFLIGHT_DIR/$policy.stderr" \
        --binary "$binary" \
        --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
        "${source_audit_argument[@]}" \
        --policy "$policy" --output "$PREFLIGHT_DIR/$policy.json" >/dev/null
    assert_experiment_seals "$policy-after-preflight-gate"
}
for policy in S J0 J1 J2 J3; do
    run_preflight "$policy"
done

stop_after_messages="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["workload"]["stop_after_messages"])' "$FROZEN_PLAN")"
hold_secs="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["workload"]["post_ingester_drop_hold_secs"])' "$FROZEN_PLAN")"
rss_interval_ms="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["workload"]["rss_interval_ms"])' "$FROZEN_PLAN")"
conflict_interval_ms="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["environment_contract"]["external_conflict_poll_interval_ms"])' "$FROZEN_PLAN")"
quiescence_maximum_kib="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["quiescence_contract"]["maximum_dirty_writeback_kib"])' "$FROZEN_PLAN")"
quiescence_consecutive="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["quiescence_contract"]["required_consecutive_samples"])' "$FROZEN_PLAN")"
quiescence_interval_ms="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["quiescence_contract"]["poll_interval_ms"])' "$FROZEN_PLAN")"
quiescence_timeout_secs="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["quiescence_contract"]["timeout_secs"])' "$FROZEN_PLAN")"
RUN_PLAN_PATH="$RESULT_DIR/run-plan.tsv"
printf 'run_index\tblock\tposition\tpolicy\tbinary_role\tbinary_sha256\tconfig\tsegments_dir\n' \
    >"$RUN_PLAN_PATH"
render_control_files=()
mapfile -t schedule_rows < <(python3 -c '
import json,sys
for row in json.load(open(sys.argv[1]))["schedule"]:
    print("\t".join(str(row[key]) for key in ("run_index","block","position","policy")))
' "$FROZEN_PLAN")
for schedule_row in "${schedule_rows[@]}"; do
    IFS=$'\t' read -r run_index block position policy <<<"$schedule_row"
    label="run-$(printf '%02d' "$run_index")-$policy"
    run_dir="$RUNS_DIR/$label"
    segments_dir="$run_dir/segments"
    mkdir "$run_dir"
    python3 "$FROZEN_PHASE1_GATE" render-config \
        --template "$METADATA_DIR/config-template.toml" \
        --output "$CONFIG_DIR/$label.toml" \
        --capture "$CAPTURE" --segments-dir "$segments_dir" \
        --stop-after-messages "$stop_after_messages" \
        >"$run_dir/config-render.json"
    chmod 0444 -- "$CONFIG_DIR/$label.toml" "$run_dir/config-render.json"
    python3 "$FROZEN_GATE" check-rendered-config \
        --record "$run_dir/config-render.json" --config "$CONFIG_DIR/$label.toml" \
        --capture "$CAPTURE" --segments-dir "$segments_dir" \
        --stop-after-messages "$stop_after_messages" >/dev/null
    render_control_files+=("$CONFIG_DIR/$label.toml" "$run_dir/config-render.json")
    binary_role="$([[ "$policy" == "S" ]] && printf system || printf jemalloc)"
    binary_sha256="$(policy_binary_sha256 "$policy")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$run_index" "$block" "$position" \
        "$policy" "$binary_role" "$binary_sha256" "$CONFIG_DIR/$label.toml" "$segments_dir" \
        >>"$RUN_PLAN_PATH"
done
CALIBRATION_SEGMENTS_DIR="$CALIBRATION_DIR/segments"
python3 "$FROZEN_PHASE1_GATE" render-config \
    --template "$METADATA_DIR/config-template.toml" \
    --output "$CONFIG_DIR/calibration-system.toml" \
    --capture "$CAPTURE" --segments-dir "$CALIBRATION_SEGMENTS_DIR" \
    --stop-after-messages "$stop_after_messages" \
    >"$CALIBRATION_DIR/config-render.json"
chmod 0444 -- "$CONFIG_DIR/calibration-system.toml" "$CALIBRATION_DIR/config-render.json"
python3 "$FROZEN_GATE" check-rendered-config \
    --record "$CALIBRATION_DIR/config-render.json" \
    --config "$CONFIG_DIR/calibration-system.toml" \
    --capture "$CAPTURE" --segments-dir "$CALIBRATION_SEGMENTS_DIR" \
    --stop-after-messages "$stop_after_messages" >/dev/null
render_control_files+=(
    "$CONFIG_DIR/calibration-system.toml" "$CALIBRATION_DIR/config-render.json"
)
chmod 0444 -- "$RUN_PLAN_PATH"
RENDERED_CONFIG_SEAL="$METADATA_DIR/rendered-configs.sha256"
sha256sum "$CONFIG_DIR"/*.toml >"$RENDERED_CONFIG_SEAL"
chmod 0444 -- "$RENDERED_CONFIG_SEAL"

FADVISE_BINARY="$TOOLS_DIR/fadvise-regular-dontneed"
FADVISE_SEAL="$TOOLS_DIR/fadvise-regular-dontneed.sha256"
cc -O2 -Wall -Wextra -Werror -o "$FADVISE_BINARY" \
    "$HARNESS_DIR/fadvise_regular_dontneed.c"
chmod 0555 -- "$FADVISE_BINARY"
sha256sum "$FADVISE_BINARY" >"$FADVISE_SEAL"
chmod 0444 -- "$FADVISE_SEAL"

MEASUREMENT_CONTROL_SEAL="$METADATA_DIR/measurement-controls.json"
measurement_control_inputs=(
    "$CORE_CONTROL_SEAL"
    "$METADATA_DIR/capture-inputs-before.json"
    "$METADATA_DIR/config-template.toml" "$METADATA_DIR/capture-manifest.json"
    "$RUN_PLAN_PATH" "$RENDERED_CONFIG_SEAL"
    "$FADVISE_BINARY" "$FADVISE_SEAL"
    "${render_control_files[@]}"
)
measurement_control_args=()
for control_input in "${measurement_control_inputs[@]}"; do
    measurement_control_args+=(--input "$control_input")
done
python3 "$FROZEN_GATE" create-control-seal \
    "${measurement_control_args[@]}" --output "$MEASUREMENT_CONTROL_SEAL"
chmod 0444 -- "$MEASUREMENT_CONTROL_SEAL"
MEASUREMENT_CONTROL_SEAL_SHA256="$(sha256sum -- "$MEASUREMENT_CONTROL_SEAL" | awk '{print $1}')"
MEASUREMENT_CONTROL_READY=1
chmod 0555 -- "$CONFIG_DIR" "$TOOLS_DIR" "$BINARY_DIR"
assert_experiment_seals complete-fixed-control-set

if [[ "$DRY_RUN" == "1" ]]; then
    assert_experiment_seals dry-run-finalization
    python3 "$FROZEN_GATE" check-source-seal \
        --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
        --output "$BUILD_LOG_DIR/source-check-dry-run-final.json"
    python3 "$FROZEN_GATE" check-extracted-source-seal \
        --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
        --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
        --seal "$EXTRACTED_SOURCE_SEAL" --build-provenance "$BUILD_PROVENANCE" \
        --output "$BUILD_LOG_DIR/extracted-source-check-dry-run-final.json"
    python3 "$FROZEN_GATE" check-executable-set \
        --build-provenance "$BUILD_PROVENANCE" \
        --system-binary "$RUN_SYSTEM" --jemalloc-binary "$RUN_JEMALLOC" \
        --query-binary "$RUN_QUERY" --storage-verify-binary "$RUN_STORAGE_VERIFY" \
        --output "$BUILD_LOG_DIR/executable-check-dry-run-final.json"
    assert_experiment_seals dry-run-after-final-gates
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    note "dry run complete; no replay, perf, cache eviction, or validation was launched: $RESULT_DIR"
    exit 0
fi

perf_events="$(python3 -c 'import json,sys; print(",".join(json.load(open(sys.argv[1]))["perf_stat_events"]))' "$FROZEN_PLAN")"
mapfile -t perf_event_names < <(python3 -c 'import json,sys; print("\n".join(json.load(open(sys.argv[1]))["perf_stat_events"]))' "$FROZEN_PLAN")
perf_required_args=()
for event in "${perf_event_names[@]}"; do
    perf_required_args+=(--require-event "$event")
done
assert_python_interpreter
set +e
perf stat --no-big-num --field-separator $'\t' --event "$perf_events" \
    --output "$METADATA_DIR/perf-stat-preflight.tsv" -- \
    "$PYTHON_BIN" -I -S -B -c 'sum(range(10000000))' \
    >"$METADATA_DIR/perf-stat-preflight.log" 2>&1
perf_preflight_status=$?
set -e
assert_python_interpreter
printf '%s\n' "$perf_preflight_status" >"$METADATA_DIR/perf-stat-preflight.exit-status"
(( perf_preflight_status == 0 )) || die "perf stat preflight failed"
python3 "$FROZEN_PHASE1_GATE" parse-perf-stat \
    --input "$METADATA_DIR/perf-stat-preflight.tsv" \
    --output "$METADATA_DIR/perf-stat-preflight.json" \
    "${perf_required_args[@]}" >/dev/null

expected_phase1_bytes="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["corpus"]["size_bytes"])' "$FROZEN_EXPECTATIONS")"
capacity_reserve_bytes=$((8 * 1024 * 1024 * 1024))
minimum_free_bytes=$((expected_phase1_bytes * 11 / 4 + capacity_reserve_bytes))
calibration_guardian_free_bytes=$((expected_phase1_bytes * 10 / 4 + capacity_reserve_bytes))
available_bytes="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 {print $1}')"
[[ "$available_bytes" =~ ^[0-9]+$ && "$available_bytes" -ge "$minimum_free_bytes" ]] \
    || die "result filesystem needs at least $minimum_free_bytes bytes free"

check_measurement_conflicts() {
    local snapshot="$1"
    ps -eo pid=,ppid=,pcpu=,pmem=,rss=,etime=,stat=,comm=,args= >"$snapshot"
    python3 "$FROZEN_GATE" check-process-snapshot \
        --snapshot "$snapshot" --allow-pid "$$" >/dev/null \
        || die "measurement conflict detected in $snapshot"
}

snapshot_pressure() {
    local output="$1"
    {
        date --iso-8601=ns
        cat /proc/loadavg
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
    local file
    local resident_bytes
    assert_experiment_seals "$(basename "$run_dir")-before-cache-eviction-helper"
    while IFS= read -r -d '' file; do
        "$FADVISE_BINARY" "$file"
    done < <(find "$CAPTURE" -maxdepth 1 -type f -name '*.capture' -print0 | sort -z)
    assert_experiment_seals "$(basename "$run_dir")-after-cache-eviction-helper"
    snapshot_capture_residency "$run_dir/capture-residency-before.tsv"
    resident_bytes="$(awk '{sum += $1} END {printf "%.0f", sum}' "$run_dir/capture-residency-before.tsv")"
    [[ "$resident_bytes" == "0" ]] \
        || die "capture retained $resident_bytes bytes after eviction before $(basename "$run_dir")"
}

note "running the fresh untimed 250k system-allocator calibration before the measured schedule"
assert_jemalloc_host_sources_absent
assert_policy_binary_unchanged S "$RUN_SYSTEM"
check_measurement_conflicts "$CALIBRATION_DIR/processes-before.txt"
prepare_capture_cache "$CALIBRATION_DIR"
assert_experiment_seals calibration-before-ingester
calibration_control="$CALIBRATION_DIR/external-conflict-guardian-control.json"
calibration_ready="$CALIBRATION_DIR/external-conflict-guardian-ready"
calibration_launch="$CALIBRATION_DIR/external-conflict-guardian-launch"
active_run_dir="$CALIBRATION_DIR"
active_guardian_control="$calibration_control"
active_guardian_ready="$calibration_ready"
active_guardian_launch="$calibration_launch"
active_lifecycle=1
set +e
defer_cleanup_signals
(
    cd "$CALIBRATION_DIR"
    while [[ ! -e "$calibration_launch" && ! -L "$calibration_launch" ]]; do
        sleep 0.001
    done
    [[ -f "$calibration_launch" && ! -L "$calibration_launch" \
        && ! -s "$calibration_launch" \
        && "$(stat -c '%a' -- "$calibration_launch")" == 444 ]] || exit 125
    exec env -i LC_ALL=C TZ=UTC \
        CONFIG_FILE="$CONFIG_DIR/calibration-system.toml" RUST_LOG="$RUST_LOG_VALUE" \
        "$RUN_SYSTEM" >"$CALIBRATION_DIR/replay.log" 2>&1
) &
calibration_pid=$!
active_root_pid="$calibration_pid"
calibration_binding_failed=0
active_root_starttime_ticks="$(read_live_starttime_ticks "$calibration_pid")" \
    || calibration_binding_failed=1
arm_cleanup_signals
(( calibration_binding_failed == 0 )) \
    || { stop_active_children; die "calibration root identity binding failed"; }
defer_cleanup_signals
python3_background "$FROZEN_GATE" monitor-external-conflicts \
    --pid "$calibration_pid" \
    --output "$CALIBRATION_DIR/external-conflict-guardian.json" \
    --interval-ms "$conflict_interval_ms" --filesystem "$RESULT_DIR" \
    --minimum-free-bytes "$calibration_guardian_free_bytes" \
    --control "$calibration_control" --ready "$calibration_ready" \
    --launch "$calibration_launch" \
    >"$CALIBRATION_DIR/external-conflict-guardian.log" 2>&1 &
calibration_guardian_pid=$!
active_guardian_pid="$calibration_guardian_pid"
calibration_guardian_binding_failed=0
active_guardian_starttime_ticks="$(read_live_starttime_ticks \
    "$calibration_guardian_pid")" || calibration_guardian_binding_failed=1
arm_cleanup_signals
(( calibration_guardian_binding_failed == 0 )) \
    || { stop_active_children; die "calibration guardian identity binding failed"; }
python3 "$FROZEN_GATE" create-guardian-control \
    --root-pid "$calibration_pid" --guardian-pid "$calibration_guardian_pid" \
    --interval-ms "$conflict_interval_ms" --ready "$calibration_ready" \
    --launch "$calibration_launch" --output "$calibration_control" >/dev/null \
    || { stop_active_children; die "calibration guardian control failed"; }
python3 "$FROZEN_GATE" wait-guardian-ready \
    --control "$calibration_control" --ready "$calibration_ready" \
    --launch "$calibration_launch" --interval-ms "$conflict_interval_ms" \
    --timeout-ms 5000 >/dev/null \
    || { stop_active_children; die "calibration guardian readiness failed"; }
python3 "$FROZEN_GATE" release-guardian-launch \
    --control "$calibration_control" --ready "$calibration_ready" \
    --launch "$calibration_launch" --interval-ms "$conflict_interval_ms" \
    >/dev/null \
    || { stop_active_children; die "calibration guardian release failed"; }
wait "$calibration_pid"
calibration_status=$?
wait "$calibration_guardian_pid"
calibration_guardian_status=$?
clear_active_processes
set -e
printf '%s\n' "$calibration_status" >"$CALIBRATION_DIR/replay.exit-status"
printf '%s\n' "$calibration_guardian_status" \
    >"$CALIBRATION_DIR/external-conflict-guardian.exit-status"
(( calibration_status == 0 )) || die "calibration replay failed"
(( calibration_guardian_status == 0 )) || die "calibration guardian failed"
assert_experiment_seals calibration-after-ingester
assert_policy_binary_unchanged S "$RUN_SYSTEM"
mapfile -d '' -t calibration_reports \
    < <(find "$CALIBRATION_DIR" -maxdepth 1 -type f -name 'ingestion_stats_*.md' -print0)
(( ${#calibration_reports[@]} == 1 )) \
    || die "calibration must produce exactly one ingestion report; found ${#calibration_reports[@]}"
python3 "$FROZEN_REPORT_GATE" replay-report \
    --report "${calibration_reports[0]}" \
    --output "$CALIBRATION_DIR/replay-correctness.json"
python3 "$FROZEN_PHASE1_GATE" tree-manifest \
    --corpus "$CALIBRATION_SEGMENTS_DIR" \
    --manifest "$CALIBRATION_DIR/segments.sha256" \
    --inventory "$CALIBRATION_DIR/segments.tsv" \
    --summary "$CALIBRATION_DIR/corpus-summary.json" >/dev/null
assert_experiment_seals calibration-before-storage-verify
env -i LC_ALL=C TZ=UTC \
    "$RUN_STORAGE_VERIFY" --segments-dir "$CALIBRATION_SEGMENTS_DIR" \
    --schema schema8 --validate-segment-footers --verify-exact-postings \
    >"$CALIBRATION_DIR/storage-verify.json" \
    2>"$CALIBRATION_DIR/storage-verify.log"
assert_experiment_seals calibration-after-storage-verify
python3 "$FROZEN_GATE" check-storage-completeness \
    --storage "$CALIBRATION_DIR/storage-verify.json" \
    --correctness "$CALIBRATION_DIR/replay-correctness.json" \
    --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
    >/dev/null
assert_experiment_seals calibration-before-query
env -i LC_ALL=C TZ=UTC \
    "$RUN_QUERY" --segments-dir "$CALIBRATION_SEGMENTS_DIR" \
    --storage-layout schema8 --sample-limit-per-kind 2 --verify-readbacks \
    --output "$CALIBRATION_DIR/readbacks.md" \
    >"$CALIBRATION_DIR/readbacks.log" 2>&1
assert_experiment_seals calibration-after-query
python3 "$FROZEN_GATE" create-calibration \
    --storage "$CALIBRATION_DIR/storage-verify.json" \
    --readbacks "$CALIBRATION_DIR/readbacks.md" \
    --correctness "$CALIBRATION_DIR/replay-correctness.json" \
    --corpus "$CALIBRATION_DIR/corpus-summary.json" \
    --build-provenance "$BUILD_PROVENANCE" \
    --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
    --output "$CALIBRATION_DIR/calibration.json"
sha256sum \
    "$CALIBRATION_DIR/storage-verify.json" \
    "$CALIBRATION_DIR/readbacks.md" \
    "$CALIBRATION_DIR/replay-correctness.json" \
    "$CALIBRATION_DIR/corpus-summary.json" \
    "$CALIBRATION_DIR/calibration.json" \
    >"$CALIBRATION_DIR/raw-inputs.sha256"
python3 "$FROZEN_GATE" sync-and-wait-writeback-quiescent \
    --corpus "$CALIBRATION_SEGMENTS_DIR" \
    --samples "$CALIBRATION_DIR/writeback-quiescence-samples.tsv" \
    --summary "$CALIBRATION_DIR/writeback-quiescence.json" \
    --maximum-kib "$quiescence_maximum_kib" \
    --consecutive "$quiescence_consecutive" \
    --interval-ms "$quiescence_interval_ms" \
    --timeout-secs "$quiescence_timeout_secs" \
    >"$CALIBRATION_DIR/writeback-quiescence.log" 2>&1
check_measurement_conflicts "$CALIBRATION_DIR/processes-after.txt"
touch "$CALIBRATION_DIR/FROZEN_BEFORE_MEASURED_SCHEDULE"
calibration_corpus_bytes="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["size_bytes"])' "$CALIBRATION_DIR/corpus-summary.json")"
[[ "$calibration_corpus_bytes" =~ ^[1-9][0-9]*$ ]] \
    || die "calibration corpus size is not a positive integer"
python3 "$FROZEN_GATE" seal-evidence-tree \
    --root "$CALIBRATION_DIR" --kind calibration \
    --output "$RAW_AUTHORITY_DIR/calibration.json"
minimum_free_bytes=$((calibration_corpus_bytes * 10 + capacity_reserve_bytes))
available_bytes="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 {print $1}')"
[[ "$available_bytes" =~ ^[0-9]+$ && "$available_bytes" -ge "$minimum_free_bytes" ]] \
    || die "post-calibration result filesystem needs at least $minimum_free_bytes bytes free"

observation_args=()
for schedule_row in "${schedule_rows[@]}"; do
    IFS=$'\t' read -r run_index block position policy <<<"$schedule_row"
    label="run-$(printf '%02d' "$run_index")-$policy"
    run_dir="$RUNS_DIR/$label"
    config="$CONFIG_DIR/$label.toml"
    segments_dir="$run_dir/segments"
    checkpoint="$run_dir/allocator-release-checkpoint.tsv"
    allocator_telemetry="$run_dir/allocator-release-telemetry.ndjson"
    binary="$(policy_binary "$policy")"
    conf="$(policy_conf "$policy")"
    assert_jemalloc_host_sources_absent
    assert_policy_binary_unchanged "$policy" "$binary"
    python3 "$FROZEN_GATE" sync-and-wait-writeback-quiescent \
        --corpus "$CONFIG_DIR" \
        --samples "$run_dir/pre-run-writeback-quiescence-samples.tsv" \
        --summary "$run_dir/pre-run-writeback-quiescence.json" \
        --maximum-kib "$quiescence_maximum_kib" \
        --consecutive "$quiescence_consecutive" \
        --interval-ms "$quiescence_interval_ms" \
        --timeout-secs "$quiescence_timeout_secs" \
        >"$run_dir/pre-run-writeback-quiescence.log" 2>&1
    check_measurement_conflicts "$run_dir/processes-before.txt"
    snapshot_pressure "$run_dir/pressure-before.txt"
    prepare_capture_cache "$run_dir"
    check_measurement_conflicts "$run_dir/processes-immediately-before-launch.txt"
    assert_experiment_seals "$label-before-ingester"
    note "running $label"

    run_required_free_bytes=$((calibration_corpus_bytes * (11 - run_index) + capacity_reserve_bytes))
    run_guardian_free_bytes=$((calibration_corpus_bytes * (10 - run_index) + capacity_reserve_bytes))
    available_bytes="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 {print $1}')"
    [[ "$available_bytes" =~ ^[0-9]+$ && "$available_bytes" -ge "$run_required_free_bytes" ]] \
        || die "$label needs at least $run_required_free_bytes bytes free before launch"

    command=(env -i LC_ALL=C TZ=UTC
        "CONFIG_FILE=$config" "RUST_LOG=$RUST_LOG_VALUE"
        "CHRONOXIDE_DIAGNOSTIC_POST_INGESTER_DROP_HOLD_SECS=$hold_secs"
        "CHRONOXIDE_DIAGNOSTIC_POST_INGESTER_DROP_CHECKPOINT=$checkpoint"
        "CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_TELEMETRY=$allocator_telemetry")
    [[ -n "$conf" ]] && command+=("_RJEM_MALLOC_CONF=$conf")
    command+=("$binary")
    command=(perf stat --no-big-num --field-separator $'\t' --event "$perf_events"
        --output "$run_dir/perf-stat.tsv" -- "${command[@]}")

    guardian_control="$run_dir/external-conflict-guardian-control.json"
    guardian_ready="$run_dir/external-conflict-guardian-ready"
    guardian_launch="$run_dir/external-conflict-guardian-launch"
    rss_ready="$run_dir/rss-monitor-ready"
    active_run_dir="$run_dir"
    active_guardian_control="$guardian_control"
    active_guardian_ready="$guardian_ready"
    active_guardian_launch="$guardian_launch"
    active_lifecycle=1
    set +e
    defer_cleanup_signals
    (
        cd "$run_dir"
        while [[ ! -e "$guardian_launch" && ! -L "$guardian_launch" ]]; do
            sleep 0.001
        done
        [[ -f "$guardian_launch" && ! -L "$guardian_launch" \
            && ! -s "$guardian_launch" \
            && "$(stat -c '%a' -- "$guardian_launch")" == 444 ]] || exit 125
        exec env LC_ALL=C /usr/bin/time -v -o "$run_dir/replay.time.txt" \
            "${command[@]}" >"$run_dir/replay.log" 2>&1
    ) &
    launcher_pid=$!
    active_root_pid="$launcher_pid"
    root_binding_failed=0
    active_root_starttime_ticks="$(read_live_starttime_ticks "$launcher_pid")" \
        || root_binding_failed=1
    arm_cleanup_signals
    (( root_binding_failed == 0 )) \
        || { stop_active_children; die "$label root identity binding failed"; }
    defer_cleanup_signals
    python3_background "$FROZEN_GATE" monitor-rss-release \
        --pid "$launcher_pid" --checkpoint "$checkpoint" \
        --output "$run_dir/rss-samples.tsv" --summary "$run_dir/rss-summary.json" \
        --interval-ms "$rss_interval_ms" --control "$guardian_control" \
        --rss-ready "$rss_ready" --launch "$guardian_launch" \
        >"$run_dir/rss-monitor.log" 2>&1 &
    monitor_pid=$!
    active_rss_pid="$monitor_pid"
    rss_binding_failed=0
    active_rss_starttime_ticks="$(read_live_starttime_ticks "$monitor_pid")" \
        || rss_binding_failed=1
    arm_cleanup_signals
    (( rss_binding_failed == 0 )) \
        || { stop_active_children; die "$label RSS identity binding failed"; }
    defer_cleanup_signals
    python3_background "$FROZEN_GATE" monitor-external-conflicts \
        --pid "$launcher_pid" --output "$run_dir/external-conflict-guardian.json" \
        --interval-ms "$conflict_interval_ms" --filesystem "$RESULT_DIR" \
        --minimum-free-bytes "$run_guardian_free_bytes" \
        --control "$guardian_control" --ready "$guardian_ready" \
        --launch "$guardian_launch" \
        >"$run_dir/external-conflict-guardian.log" 2>&1 &
    guardian_pid=$!
    active_guardian_pid="$guardian_pid"
    guardian_binding_failed=0
    active_guardian_starttime_ticks="$(read_live_starttime_ticks "$guardian_pid")" \
        || guardian_binding_failed=1
    arm_cleanup_signals
    (( guardian_binding_failed == 0 )) \
        || { stop_active_children; die "$label guardian identity binding failed"; }
    python3 "$FROZEN_GATE" create-guardian-control \
        --root-pid "$launcher_pid" --rss-monitor-pid "$monitor_pid" \
        --rss-ready "$rss_ready" \
        --guardian-pid "$guardian_pid" --interval-ms "$conflict_interval_ms" \
        --ready "$guardian_ready" --launch "$guardian_launch" \
        --output "$guardian_control" >/dev/null \
        || { stop_active_children; die "$label guardian control failed"; }
    python3 "$FROZEN_GATE" wait-guardian-ready \
        --control "$guardian_control" --ready "$guardian_ready" \
        --launch "$guardian_launch" --interval-ms "$conflict_interval_ms" \
        --timeout-ms 5000 >/dev/null \
        || { stop_active_children; die "$label guardian readiness failed"; }
    python3 "$FROZEN_GATE" release-guardian-launch \
        --control "$guardian_control" --ready "$guardian_ready" \
        --launch "$guardian_launch" --interval-ms "$conflict_interval_ms" \
        >/dev/null \
        || { stop_active_children; die "$label guardian release failed"; }
    wait "$launcher_pid"
    replay_status=$?
    wait "$monitor_pid"
    monitor_status=$?
    wait "$guardian_pid"
    guardian_status=$?
    clear_active_processes
    set -e
    printf '%s\n' "$replay_status" >"$run_dir/replay.exit-status"
    printf '%s\n' "$monitor_status" >"$run_dir/rss-monitor.exit-status"
    printf '%s\n' "$guardian_status" >"$run_dir/external-conflict-guardian.exit-status"
    (( monitor_status == 0 )) || die "$label RSS monitor failed"
    (( guardian_status == 0 )) || die "$label external-conflict guardian failed"
    if (( replay_status != 0 )); then
        tail -n 100 "$run_dir/replay.log" >&2 || true
        die "$label failed with status $replay_status"
    fi
    assert_experiment_seals "$label-after-ingester"
    assert_policy_binary_unchanged "$policy" "$binary"
    [[ -d "$segments_dir" ]] || die "$label produced no segment corpus"
    python3 "$FROZEN_PHASE1_GATE" parse-time \
        --input "$run_dir/replay.time.txt" --output "$run_dir/replay.time.json" >/dev/null
    python3 "$FROZEN_PHASE1_GATE" parse-perf-stat \
        --input "$run_dir/perf-stat.tsv" --output "$run_dir/perf-stat.json" \
        "${perf_required_args[@]}" >/dev/null
    python3 "$FROZEN_GATE" parse-checkpoint \
        --checkpoint "$checkpoint" --rss "$run_dir/rss-summary.json" \
        --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
        --output "$run_dir/allocator-release-summary.json" >/dev/null
    python3 "$FROZEN_GATE" parse-allocator-telemetry \
        --telemetry "$allocator_telemetry" --checkpoint "$checkpoint" \
        --rss-samples "$run_dir/rss-samples.tsv" \
        --rss-summary "$run_dir/rss-summary.json" --policy "$policy" \
        --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
        --output "$run_dir/allocator-telemetry-summary.json" >/dev/null
    python3 "$FROZEN_GATE" gate-runtime-log \
        --log "$run_dir/replay.log" --preflight "$PREFLIGHT_DIR/$policy.json" \
        --plan "$FROZEN_PLAN" \
        --phase1-expectations "$FROZEN_EXPECTATIONS" --policy "$policy" \
        --output "$run_dir/allocator-runtime-log.json" >/dev/null
    snapshot_capture_residency "$run_dir/capture-residency-after.tsv"
    snapshot_pressure "$run_dir/pressure-after.txt"
    check_measurement_conflicts "$run_dir/processes-after.txt"

    mapfile -d '' -t reports \
        < <(find "$run_dir" -maxdepth 1 -type f -name 'ingestion_stats_*.md' -print0)
    (( ${#reports[@]} == 1 )) \
        || die "$label must produce exactly one ingestion report; found ${#reports[@]}"
    python3 "$FROZEN_REPORT_GATE" replay-report \
        --report "${reports[0]}" --output "$run_dir/replay-correctness.json"
    python3 "$FROZEN_PHASE1_GATE" tree-manifest \
        --corpus "$segments_dir" --manifest "$run_dir/segments.sha256" \
        --inventory "$run_dir/segments.tsv" --summary "$run_dir/corpus-summary.json" >/dev/null
    python3 "$FROZEN_GATE" sync-and-wait-writeback-quiescent \
        --corpus "$segments_dir" \
        --samples "$run_dir/writeback-quiescence-samples.tsv" \
        --summary "$run_dir/writeback-quiescence.json" \
        --maximum-kib "$quiescence_maximum_kib" \
        --consecutive "$quiescence_consecutive" \
        --interval-ms "$quiescence_interval_ms" \
        --timeout-secs "$quiescence_timeout_secs" \
        >"$run_dir/writeback-quiescence.log" 2>&1
    python3 "$FROZEN_GATE" make-observation \
        --run-index "$run_index" --policy "$policy" --plan "$FROZEN_PLAN" \
        --phase1-expectations "$FROZEN_EXPECTATIONS" \
        --build-provenance "$BUILD_PROVENANCE" \
        --preflight "$PREFLIGHT_DIR/$policy.json" --binary "$binary" \
        --runtime-policy "$run_dir/allocator-runtime-log.json" \
        --allocator-telemetry "$run_dir/allocator-telemetry-summary.json" \
        --checkpoint "$checkpoint" \
        --rss "$run_dir/rss-summary.json" --time "$run_dir/replay.time.json" \
        --perf "$run_dir/perf-stat.json" \
        --guardian "$run_dir/external-conflict-guardian.json" \
        --pre-quiescence "$run_dir/pre-run-writeback-quiescence.json" \
        --quiescence "$run_dir/writeback-quiescence.json" \
        --correctness "$run_dir/replay-correctness.json" \
        --corpus "$run_dir/corpus-summary.json" --output "$run_dir/observation.json"
    python3 "$FROZEN_GATE" seal-evidence-tree \
        --root "$run_dir" --kind run \
        --output "$RAW_AUTHORITY_DIR/$label.json"
    observation_args+=(--observation "$run_dir/observation.json")
done

for schedule_row in "${schedule_rows[@]:1}"; do
    IFS=$'\t' read -r run_index _block _position policy <<<"$schedule_row"
    label="run-$(printf '%02d' "$run_index")-$policy"
    if ! cmp -s "$RUNS_DIR/run-01-S/segments.sha256" "$RUNS_DIR/$label/segments.sha256"; then
        diff -u "$RUNS_DIR/run-01-S/segments.sha256" "$RUNS_DIR/$label/segments.sha256" \
            >"$COMPARISONS_DIR/run-01-S-vs-$label.manifest.diff" || true
        die "$label corpus differs from run-01-S"
    fi
    if ! cmp -s "$RUNS_DIR/run-01-S/replay-correctness.json" \
            "$RUNS_DIR/$label/replay-correctness.json"; then
        diff -u "$RUNS_DIR/run-01-S/replay-correctness.json" \
            "$RUNS_DIR/$label/replay-correctness.json" \
            >"$COMPARISONS_DIR/run-01-S-vs-$label.correctness.diff" || true
        die "$label correctness differs from run-01-S"
    fi
done
printf '%s\n' 'all ten corpora and replay-correctness documents are byte-identical' \
    >"$COMPARISONS_DIR/determinism.txt"

python3 "$FROZEN_GATE" compare-screen "${observation_args[@]}" \
    --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
    --output "$COMPARISONS_DIR/screen-summary.json"

note "running canonical exhaustive storage verification outside measured replay"
assert_jemalloc_host_sources_absent
check_measurement_conflicts "$VALIDATION_DIR/processes-before-storage-verify.txt"
assert_experiment_seals canonical-before-storage-verify
/usr/bin/time -v -o "$VALIDATION_DIR/storage-verify.time.txt" \
    env -i LC_ALL=C TZ=UTC \
    "$RUN_STORAGE_VERIFY" --segments-dir "$RUNS_DIR/run-01-S/segments" \
    --schema schema8 --validate-segment-footers --verify-exact-postings \
    >"$VALIDATION_DIR/storage-verify.json" 2>"$VALIDATION_DIR/storage-verify.log"
assert_experiment_seals canonical-after-storage-verify
python3 "$FROZEN_GATE" check-storage-completeness \
    --storage "$VALIDATION_DIR/storage-verify.json" \
    --correctness "$RUNS_DIR/run-01-S/replay-correctness.json" \
    --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
    >/dev/null

note "running canonical independent readbacks outside measured replay"
check_measurement_conflicts "$VALIDATION_DIR/processes-before-readbacks.txt"
assert_experiment_seals canonical-before-query
/usr/bin/time -v -o "$VALIDATION_DIR/readbacks.time.txt" \
    env -i LC_ALL=C TZ=UTC \
    "$RUN_QUERY" --segments-dir "$RUNS_DIR/run-01-S/segments" \
    --storage-layout schema8 --sample-limit-per-kind 2 --verify-readbacks \
    --output "$VALIDATION_DIR/readbacks.md" >"$VALIDATION_DIR/readbacks.log" 2>&1
assert_experiment_seals canonical-after-query
python3 "$FROZEN_GATE" gate-validation \
    --storage "$VALIDATION_DIR/storage-verify.json" \
    --readbacks "$VALIDATION_DIR/readbacks.md" \
    --correctness "$RUNS_DIR/run-01-S/replay-correctness.json" \
    --corpus "$RUNS_DIR/run-01-S/corpus-summary.json" \
    --calibration "$CALIBRATION_DIR/calibration.json" \
    --calibration-storage "$CALIBRATION_DIR/storage-verify.json" \
    --calibration-readbacks "$CALIBRATION_DIR/readbacks.md" \
    --calibration-correctness "$CALIBRATION_DIR/replay-correctness.json" \
    --calibration-corpus "$CALIBRATION_DIR/corpus-summary.json" \
    --build-provenance "$BUILD_PROVENANCE" \
    --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
    --output "$VALIDATION_DIR/validation-summary.json"
check_measurement_conflicts "$VALIDATION_DIR/processes-after.txt"
python3 "$FROZEN_GATE" seal-evidence-tree \
    --root "$VALIDATION_DIR" --kind validation \
    --output "$RAW_AUTHORITY_DIR/validation.json"

note "re-inventorying the source capture after all replay and validation work"
python3 "$FROZEN_PHASE1_GATE" validate-inputs \
    --capture "$CAPTURE" --template "$CONFIG_TEMPLATE" \
    --expectations "$FROZEN_EXPECTATIONS" \
    --output "$METADATA_DIR/capture-inputs-after.json"
cmp -s "$METADATA_DIR/capture-inputs-before.json" \
    "$METADATA_DIR/capture-inputs-after.json" \
    || die "source capture or configuration changed during the screen"
sha256sum "$METADATA_DIR/capture-inputs-before.json" \
    "$METADATA_DIR/capture-inputs-after.json" \
    >"$METADATA_DIR/capture-inputs-before-after.sha256"

python3 "$FROZEN_GATE" seal-screen "${observation_args[@]}" \
    --screen-summary "$COMPARISONS_DIR/screen-summary.json" \
    --validation "$VALIDATION_DIR/validation-summary.json" \
    --storage "$VALIDATION_DIR/storage-verify.json" \
    --readbacks "$VALIDATION_DIR/readbacks.md" \
    --correctness "$RUNS_DIR/run-01-S/replay-correctness.json" \
    --corpus "$RUNS_DIR/run-01-S/corpus-summary.json" \
    --calibration "$CALIBRATION_DIR/calibration.json" \
    --calibration-storage "$CALIBRATION_DIR/storage-verify.json" \
    --calibration-readbacks "$CALIBRATION_DIR/readbacks.md" \
    --calibration-correctness "$CALIBRATION_DIR/replay-correctness.json" \
    --calibration-corpus "$CALIBRATION_DIR/corpus-summary.json" \
    --capture-inputs-before "$METADATA_DIR/capture-inputs-before.json" \
    --capture-inputs-after "$METADATA_DIR/capture-inputs-after.json" \
    --build-provenance "$BUILD_PROVENANCE" \
    --plan "$FROZEN_PLAN" --phase1-expectations "$FROZEN_EXPECTATIONS" \
    --output "$COMPARISONS_DIR/final-screen-decision.json"

assert_experiment_seals finalization
python3 "$FROZEN_GATE" check-source-seal \
    --repo "$REPO_ROOT" --seal "$SOURCE_SEAL" \
    --output "$BUILD_LOG_DIR/source-check-final.json"
python3 "$FROZEN_GATE" check-extracted-source-seal \
    --repo "$REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
    --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
    --seal "$EXTRACTED_SOURCE_SEAL" --build-provenance "$BUILD_PROVENANCE" \
    --output "$BUILD_LOG_DIR/extracted-source-check-final.json"
python3 "$FROZEN_GATE" check-executable-set \
    --build-provenance "$BUILD_PROVENANCE" \
    --system-binary "$RUN_SYSTEM" --jemalloc-binary "$RUN_JEMALLOC" \
    --query-binary "$RUN_QUERY" --storage-verify-binary "$RUN_STORAGE_VERIFY" \
    --output "$BUILD_LOG_DIR/executable-check-final.json"
assert_experiment_seals after-final-direct-gates
python3 "$FROZEN_GATE" revalidate-screen-from-raw \
    --result-root "$RESULT_DIR" --plan "$FROZEN_PLAN" \
    --phase1-expectations "$FROZEN_EXPECTATIONS" \
    --output "$METADATA_DIR/final-raw-revalidation.json"
chmod 0444 -- "$METADATA_DIR/final-raw-revalidation.json"
python3 "$FROZEN_GATE" create-final-artifact-inventory \
    --result-root "$RESULT_DIR" \
    --files "$METADATA_DIR/result-artifacts.nul" \
    --directories "$METADATA_DIR/result-directories.nul" \
    --manifest "$METADATA_DIR/result-artifacts.sha256" >/dev/null
python3 "$FROZEN_GATE" validate-final-artifacts \
    --result-root "$RESULT_DIR" --stage precomplete \
    --output "$METADATA_DIR/FINAL_SEAL_VALIDATED.json"
python3 "$FROZEN_GATE" revalidate-screen-from-raw \
    --result-root "$RESULT_DIR" --plan "$FROZEN_PLAN" \
    --phase1-expectations "$FROZEN_EXPECTATIONS" >/dev/null
printf '%s\n' 'chronoxide/allocator-screen-complete/v1' >"$RESULT_DIR/COMPLETE"
chmod 0444 -- "$RESULT_DIR/COMPLETE"
python3 "$FROZEN_GATE" validate-final-artifacts \
    --result-root "$RESULT_DIR" --stage complete >/dev/null
python3 "$FROZEN_GATE" revalidate-screen-from-raw \
    --result-root "$RESULT_DIR" --plan "$FROZEN_PLAN" \
    --phase1-expectations "$FROZEN_EXPECTATIONS" >/dev/null
note "complete: $RESULT_DIR"
