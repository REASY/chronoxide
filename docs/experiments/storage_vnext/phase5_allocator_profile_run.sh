#!/usr/bin/env bash

set -euo pipefail

DEFAULT_CAPTURE="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001"
DEFAULT_CONFIG_TEMPLATE="/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/post-adaptive-head-profile-20260716-223717/config.toml"

SCREEN_RESULT_DIR="${SCREEN_RESULT_DIR:-}"
RESULT_DIR="${RESULT_DIR:-}"
CAPTURE="${CAPTURE:-$DEFAULT_CAPTURE}"
CONFIG_TEMPLATE="${CONFIG_TEMPLATE:-$DEFAULT_CONFIG_TEMPLATE}"
ENABLE_PERF_RECORD="${ENABLE_PERF_RECORD:-0}"
PERF_POLICY="${PERF_POLICY:-system}"
RUN_NOTE="${RUN_NOTE:-}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
PROFILE_MIN_FREE_BYTES="${PROFILE_MIN_FREE_BYTES:-17179869184}"
RUST_LOG_VALUE='chronoxide_ingester=info,chronoxide_core=warn'

die() {
    printf 'Phase 5 allocator profile: %s\n' "$*" >&2
    exit 2
}

assert_jemalloc_host_sources_absent() {
    local path
    for path in /etc/malloc.conf /etc/_rjem_malloc.conf; do
        [[ ! -e "$path" && ! -L "$path" ]] \
            || die "ambient jemalloc configuration source is forbidden: $path"
    done
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

[[ "$SCREEN_RESULT_DIR" == /* && -d "$SCREEN_RESULT_DIR" ]] \
    || die "SCREEN_RESULT_DIR must be an absolute completed screen result"
[[ -n "$RESULT_DIR" && "$RESULT_DIR" == /* ]] \
    || die "RESULT_DIR must be a new absolute external path"
[[ "$CAPTURE" == /* && -d "$CAPTURE" ]] || die "CAPTURE must be an absolute directory"
[[ "$CONFIG_TEMPLATE" == /* && -f "$CONFIG_TEMPLATE" ]] \
    || die "CONFIG_TEMPLATE must be an absolute regular file"
[[ "$ENABLE_PERF_RECORD" == "0" || "$ENABLE_PERF_RECORD" == "1" ]] \
    || die "ENABLE_PERF_RECORD must be 0 or 1"
[[ "$PERF_POLICY" == "system" || "$PERF_POLICY" == "selected" ]] \
    || die "PERF_POLICY must be system or selected"
for forbidden in LD_PRELOAD MALLOC_CONF _RJEM_MALLOC_CONF; do
    [[ -z "${!forbidden-}" ]] || die "ambient $forbidden is forbidden"
done
[[ -n "$RUN_NOTE" && "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
    || die "RUN_NOTE is required and must be one line"
[[ "$QUIET_HOST_CONFIRMED" == "1" ]] \
    || die "QUIET_HOST_CONFIRMED=1 is required immediately before profiling"
[[ "$PROFILE_MIN_FREE_BYTES" =~ ^[0-9]+$ && \
    "$PROFILE_MIN_FREE_BYTES" -ge 8589934592 ]] \
    || die "PROFILE_MIN_FREE_BYTES must be an integer of at least 8 GiB"
for forbidden in PYTHONPATH PYTHONHOME PYTHONUSERBASE PYTHONSTARTUP PYTHONINSPECT; do
    [[ -z "${!forbidden-}" ]] || die "ambient $forbidden is forbidden"
done
for command in awk cat chmod cmp df find git heaptrack heaptrack_print mkdir ps python3 \
        realpath rg sha256sum sort stat touch xargs; do
    require_command "$command"
done
PYTHON_BIN="$(realpath -e -- "$(command -v python3)")"
[[ "$PYTHON_BIN" == /* && -f "$PYTHON_BIN" && ! -L "$PYTHON_BIN" && \
    -x "$PYTHON_BIN" ]] \
    || die "python3 must resolve to an absolute executable non-symlink regular file"
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
(( ENABLE_PERF_RECORD == 0 )) || require_command perf

SCREEN_RESULT_DIR="$(realpath -e -- "$SCREEN_RESULT_DIR")"
CAPTURE="$(realpath -e -- "$CAPTURE")"
CONFIG_TEMPLATE="$(realpath -e -- "$CONFIG_TEMPLATE")"
result_parent_input="$(dirname "$RESULT_DIR")"
[[ -d "$result_parent_input" ]] || die "RESULT_DIR parent does not exist"
result_parent="$(realpath -e -- "$result_parent_input")"
RESULT_DIR="$result_parent/$(basename "$RESULT_DIR")"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists: $RESULT_DIR"
case "$RESULT_DIR/" in
    "$SCREEN_RESULT_DIR/"*|"$CAPTURE/"*)
        die "RESULT_DIR must be outside the screen result and capture"
        ;;
esac

HARNESS_DIR="$SCREEN_RESULT_DIR/metadata/harness"
GATE="$HARNESS_DIR/phase5_allocator_screen_gate.py"
FROZEN_PROFILE_RUNNER="$HARNESS_DIR/phase5_allocator_profile_run.sh"
PLAN="$HARNESS_DIR/phase5_allocator_screen_plan.json"
PHASE1_GATE="$HARNESS_DIR/phase1_replay_gate.py"
EXPECTATIONS="$HARNESS_DIR/phase1_4m_expectations.json"
REPORT_GATE="$HARNESS_DIR/ab_gate.py"
BUILD_PROVENANCE="$SCREEN_RESULT_DIR/metadata/build-provenance.json"
SCREEN_ARTIFACT_MANIFEST="$SCREEN_RESULT_DIR/metadata/result-artifacts.sha256"
CORE_CONTROL_SEAL="$SCREEN_RESULT_DIR/metadata/core-controls.json"
MEASUREMENT_CONTROL_SEAL="$SCREEN_RESULT_DIR/metadata/measurement-controls.json"
SOURCE_SEAL="$SCREEN_RESULT_DIR/metadata/source/formal-source-seal.json"
SOURCE_ARCHIVE="$SCREEN_RESULT_DIR/metadata/source/git-head.tar"
EXTRACTED_SOURCE_SEAL="$SCREEN_RESULT_DIR/metadata/source/extracted-build-source-seal.json"
FINAL_DECISION="$SCREEN_RESULT_DIR/comparisons/final-screen-decision.json"
COMPLETE_MARKER="$SCREEN_RESULT_DIR/COMPLETE"
CALIBRATION_DIR="$SCREEN_RESULT_DIR/calibration"
REFERENCE_DIR="$SCREEN_RESULT_DIR/runs/run-01-S"
SYSTEM_BINARY="$SCREEN_RESULT_DIR/metadata/binaries/chronoxide-ingester-system"
JEMALLOC_BINARY="$SCREEN_RESULT_DIR/metadata/binaries/chronoxide-ingester-jemalloc"
QUERY_BINARY="$SCREEN_RESULT_DIR/metadata/binaries/chronoxide-query"
STORAGE_VERIFY_BINARY="$SCREEN_RESULT_DIR/metadata/binaries/chronoxide-storage-verify"
for file in "$GATE" "$FROZEN_PROFILE_RUNNER" "$PLAN" "$PHASE1_GATE" \
        "$EXPECTATIONS" "$REPORT_GATE" \
        "$BUILD_PROVENANCE" "$FINAL_DECISION" "$COMPLETE_MARKER" \
        "$SCREEN_ARTIFACT_MANIFEST" "$CORE_CONTROL_SEAL" \
        "$MEASUREMENT_CONTROL_SEAL" "$SOURCE_SEAL" "$SOURCE_ARCHIVE" \
        "$EXTRACTED_SOURCE_SEAL" \
        "$CALIBRATION_DIR/calibration.json" \
        "$CALIBRATION_DIR/storage-verify.json" "$CALIBRATION_DIR/readbacks.md" \
        "$CALIBRATION_DIR/replay-correctness.json" \
        "$CALIBRATION_DIR/corpus-summary.json" \
        "$REFERENCE_DIR/segments.sha256" "$REFERENCE_DIR/replay-correctness.json" \
        "$REFERENCE_DIR/corpus-summary.json" "$SYSTEM_BINARY" "$JEMALLOC_BINARY" \
        "$QUERY_BINARY" "$STORAGE_VERIFY_BINARY"; do
    [[ -f "$file" && ! -L "$file" ]] || die "required frozen screen input is missing: $file"
done
[[ "$(cat "$COMPLETE_MARKER")" == 'chronoxide/allocator-screen-complete/v1' && \
    "$(stat -c '%a' -- "$COMPLETE_MARKER")" == "444" ]] \
    || die "screen COMPLETE marker version, content, or mode changed"
EXECUTING_PROFILE_RUNNER="$(realpath -e -- "${BASH_SOURCE[0]}")"
[[ "$EXECUTING_PROFILE_RUNNER" == "$(realpath -e -- "$FROZEN_PROFILE_RUNNER")" ]] \
    || die "execute the profile runner frozen inside SCREEN_RESULT_DIR"
[[ -d "$HARNESS_DIR" && ! -L "$HARNESS_DIR" && ! -w "$HARNESS_DIR" ]] \
    || die "completed screen harness directory is mutable"
[[ "$(stat -c '%a' -- "$FROZEN_PROFILE_RUNNER")" == "555" ]] \
    || die "frozen profile runner must have exact mode 0555"
[[ "$(stat -c '%a' -- "$SCREEN_ARTIFACT_MANIFEST")" == "444" ]] \
    || die "completed screen artifact manifest must have exact mode 0444"
SCREEN_ARTIFACT_MANIFEST_SHA256="$(sha256sum -- "$SCREEN_ARTIFACT_MANIFEST" | awk '{print $1}')"
GATE_SHA256="$(sha256sum -- "$GATE" | awk '{print $1}')"
CORE_CONTROL_SEAL_SHA256="$(sha256sum -- "$CORE_CONTROL_SEAL" | awk '{print $1}')"
MEASUREMENT_CONTROL_SEAL_SHA256="$(sha256sum -- "$MEASUREMENT_CONTROL_SEAL" | awk '{print $1}')"
SOURCE_BINDINGS="$(python3 -c '
import json,sys
live=json.load(open(sys.argv[1], encoding="utf-8"))
extracted=json.load(open(sys.argv[2], encoding="utf-8"))
build=json.load(open(sys.argv[3], encoding="utf-8"))
repo=live.get("repo")
source_root=extracted.get("source_root")
archive=extracted.get("archive_path")
build_source=build.get("build_source")
if not isinstance(build_source, dict): raise SystemExit("missing build-source provenance")
values=(repo, source_root, archive, extracted.get("repo"), build_source.get("root"), build_source.get("archive_path"))
if any(not isinstance(value,str) or "\n" in value for value in values): raise SystemExit("invalid sealed source path")
if repo != extracted["repo"] or source_root != build_source["root"] or archive != build_source["archive_path"]: raise SystemExit("inconsistent sealed source bindings")
print(repo)
print(source_root)
' "$SOURCE_SEAL" "$EXTRACTED_SOURCE_SEAL" "$BUILD_PROVENANCE")"
mapfile -t SOURCE_BINDING_LINES <<<"$SOURCE_BINDINGS"
(( ${#SOURCE_BINDING_LINES[@]} == 2 )) \
    || die "completed screen has malformed sealed source bindings"
SEALED_REPO_ROOT="${SOURCE_BINDING_LINES[0]}"
BUILD_SOURCE="${SOURCE_BINDING_LINES[1]}"
[[ "$SEALED_REPO_ROOT" == /* && -d "$SEALED_REPO_ROOT" && \
    ! -L "$SEALED_REPO_ROOT" ]] \
    || die "sealed live repository is not an absolute non-symlink directory"
[[ "$BUILD_SOURCE" == "$SCREEN_RESULT_DIR/build-source" && \
    -d "$BUILD_SOURCE" && ! -L "$BUILD_SOURCE" ]] \
    || die "sealed extracted build-source is not canonical"
readonly SOURCE_BINDINGS SEALED_REPO_ROOT BUILD_SOURCE
active_lifecycle=0
active_run_dir=''
active_guardian_control=''
active_guardian_ready=''
active_guardian_launch=''
active_root_pid=''
active_root_starttime_ticks=''
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
    [[ -n "$active_run_dir" && -d "$active_run_dir" ]] || return 0
    printf '%s\t%s\t%s\n' "$1" "$2" "$3" \
        >>"$active_run_dir/interrupted-cleanup-reap.tsv"
}

stop_bound_tree() {
    local role="$1" pid="$2" starttime_ticks="$3"
    [[ -n "$pid" ]] || return 0
    if [[ -z "$starttime_ticks" ]]; then
        record_cleanup_reap "$role" unbound-signal-refused "pid=$pid"
        return 1
    fi
    cleanup_python3 "$GATE" terminate-process-tree --root-pid "$pid" \
        --root-starttime-ticks "$starttime_ticks" \
        >"$active_run_dir/interrupted-$role-termination.json" 2>&1 || true
}

bounded_reap_job() {
    local role="$1" pid="$2" expected="$3" attempt state current identity
    [[ -n "$pid" ]] || return 0
    if [[ -z "$expected" ]]; then
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
        read -r state current <<<"$identity"
        [[ "$current" == "$expected" ]] || {
            record_cleanup_reap "$role" reused-refused "pid=$pid"
            return 1
        }
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
    active_guardian_pid=''
    active_guardian_starttime_ticks=''
    active_lifecycle=0
}

cleanup_children() {
    local controlled=0
    trap '' HUP INT TERM
    if [[ -n "$active_guardian_control" && -f "$active_guardian_control" \
        && ! -L "$active_guardian_control" ]]; then
        if cleanup_python3 "$GATE" cleanup-guardian-processes \
            --control "$active_guardian_control" --ready "$active_guardian_ready" \
            --launch "$active_guardian_launch" --interval-ms 100 \
            >"$active_run_dir/interrupted-guardian-cleanup.json" 2>&1; then
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
        cleanup_children || true
    fi
    exit "$exit_status"
}
arm_cleanup_signals
trap 'cleanup_on_exit "$?"' EXIT

check_measurement_conflicts() {
    local snapshot="$1"
    ps -eo pid=,ppid=,pcpu=,pmem=,rss=,etime=,stat=,comm=,args= >"$snapshot"
    python3 "$GATE" check-process-snapshot \
        --snapshot "$snapshot" --allow-pid "$$" >/dev/null \
        || die "measurement conflict detected in $snapshot"
}

assert_screen_seal() {
    local context="$1"
    local current_manifest_hash
    [[ "$(sha256sum -- "$GATE" | awk '{print $1}')" == "$GATE_SHA256" ]] \
        || die "completed screen gate changed at $context"
    [[ "$(sha256sum -- "$CORE_CONTROL_SEAL" | awk '{print $1}')" == \
        "$CORE_CONTROL_SEAL_SHA256" ]] \
        || die "completed screen core-control authority changed at $context"
    [[ "$(sha256sum -- "$MEASUREMENT_CONTROL_SEAL" | awk '{print $1}')" == \
        "$MEASUREMENT_CONTROL_SEAL_SHA256" ]] \
        || die "completed screen measurement-control authority changed at $context"
    python3 "$GATE" check-control-seal --seal "$CORE_CONTROL_SEAL" >/dev/null \
        || die "completed screen core-control seal failed at $context"
    python3 "$GATE" check-control-seal --seal "$MEASUREMENT_CONTROL_SEAL" >/dev/null \
        || die "completed screen measurement-control seal failed at $context"
    current_manifest_hash="$(sha256sum -- "$SCREEN_ARTIFACT_MANIFEST" | awk '{print $1}')"
    [[ "$current_manifest_hash" == "$SCREEN_ARTIFACT_MANIFEST_SHA256" ]] \
        || die "completed screen artifact-manifest bytes changed at $context"
    python3 "$GATE" check-source-seal \
        --repo "$SEALED_REPO_ROOT" --seal "$SOURCE_SEAL" >/dev/null \
        || die "completed screen live-source seal failed at $context"
    python3 "$GATE" check-extracted-source-seal \
        --repo "$SEALED_REPO_ROOT" --archive "$SOURCE_ARCHIVE" \
        --source-root "$BUILD_SOURCE" --live-source-seal "$SOURCE_SEAL" \
        --seal "$EXTRACTED_SOURCE_SEAL" --build-provenance "$BUILD_PROVENANCE" \
        >/dev/null \
        || die "completed screen archived/extracted-source seal failed at $context"
    python3 "$GATE" check-executable-set \
        --build-provenance "$BUILD_PROVENANCE" \
        --system-binary "$SYSTEM_BINARY" --jemalloc-binary "$JEMALLOC_BINARY" \
        --query-binary "$QUERY_BINARY" \
        --storage-verify-binary "$STORAGE_VERIFY_BINARY" >/dev/null \
        || die "completed screen executable seal failed at $context"
    [[ "$(sha256sum -- "$GATE" | awk '{print $1}')" == "$GATE_SHA256" ]] \
        || die "completed screen gate changed during $context"
    [[ "$(sha256sum -- "$CORE_CONTROL_SEAL" | awk '{print $1}')" == \
        "$CORE_CONTROL_SEAL_SHA256" ]] \
        || die "completed screen core-control authority changed during $context"
    [[ "$(sha256sum -- "$MEASUREMENT_CONTROL_SEAL" | awk '{print $1}')" == \
        "$MEASUREMENT_CONTROL_SEAL_SHA256" ]] \
        || die "completed screen measurement-control authority changed during $context"
}
assert_jemalloc_host_sources_absent
assert_screen_seal initial-screen-input
python3 "$GATE" validate-final-artifacts \
    --result-root "$SCREEN_RESULT_DIR" --stage complete >/dev/null \
    || die "completed screen exact inventory failed before profiling"
python3 "$GATE" validate-plan --plan "$PLAN" \
    --phase1-expectations "$EXPECTATIONS" >/dev/null
CAPTURE_INPUTS_BEFORE="$(python3 "$PHASE1_GATE" validate-inputs \
    --capture "$CAPTURE" --template "$CONFIG_TEMPLATE" \
    --expectations "$EXPECTATIONS")"

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/configs" "$RESULT_DIR/heaptrack" "$RESULT_DIR/metadata"
mkdir "$RESULT_DIR/metadata/raw-authorities"
PROFILE_CAPACITY_CONTROL="$RESULT_DIR/metadata/profile-capacity-control.json"
python3 "$GATE" create-profile-capacity-control \
    --profile-min-free-bytes "$PROFILE_MIN_FREE_BYTES" \
    --output "$PROFILE_CAPACITY_CONTROL" >/dev/null
PROFILE_CAPACITY_CONTROL_SHA256="$(sha256sum -- "$PROFILE_CAPACITY_CONTROL" | awk '{print $1}')"
PYTHON_RECORD="$RESULT_DIR/metadata/python-interpreter.txt"
{
    printf 'path=%s\n' "$PYTHON_BIN"
    printf 'sha256=%s\n' "$PYTHON_BIN_SHA256"
    printf 'version=%s\n' "$PYTHON_VERSION"
    printf 'flags_isolated_no_site_no_bytecode_ignore_environment_safe_path=%s\n' \
        "$PYTHON_FLAGS_PROBE"
} >"$PYTHON_RECORD"
chmod 0444 -- "$PYTHON_RECORD"
printf '%s\n' "$CAPTURE_INPUTS_BEFORE" \
    >"$RESULT_DIR/metadata/capture-inputs-before.json"
chmod 0444 -- "$RESULT_DIR/metadata/capture-inputs-before.json"
printf '%s\n' \
    'Untimed diagnostic profile only. No artifact in this directory is A/B' \
    'latency, CPU, or RSS evidence. Heaptrack uses the frozen system-allocator' \
    'binary and is the allocation-stack authority. Candidate-specific linked-' \
    'jemalloc heap profiling is explicitly deferred.' \
    >"$RESULT_DIR/PROFILE_SCOPE.txt"
printf '%s\n' "$RUN_NOTE" >"$RESULT_DIR/metadata/run-note.txt"
chmod 0444 -- "$RESULT_DIR/PROFILE_SCOPE.txt" "$RESULT_DIR/metadata/run-note.txt"

stop_after_messages="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["workload"]["stop_after_messages"])' "$PLAN")"
profile_control_seals=()
profile_control_hashes=()

assert_profile_capacity_control() {
    local context="$1"
    [[ -f "$PROFILE_CAPACITY_CONTROL" && ! -L "$PROFILE_CAPACITY_CONTROL" \
        && "$(stat -c '%a' -- "$PROFILE_CAPACITY_CONTROL")" == "444" ]] \
        || die "profile capacity control changed type or mode at $context"
    [[ "$(sha256sum -- "$PROFILE_CAPACITY_CONTROL" | awk '{print $1}')" == \
        "$PROFILE_CAPACITY_CONTROL_SHA256" ]] \
        || die "profile capacity control changed at $context"
    python3 "$GATE" check-profile-capacity-control \
        --control "$PROFILE_CAPACITY_CONTROL" \
        --expected-profile-min-free-bytes "$PROFILE_MIN_FREE_BYTES" >/dev/null \
        || die "profile capacity control failed validation at $context"
}

assert_profile_control_seal() {
    local seal="$1"
    local expected_sha256="$2"
    local context="$3"
    assert_profile_capacity_control "$context-before-profile-control"
    assert_screen_seal "$context-before-profile-control"
    [[ -f "$seal" && ! -L "$seal" && "$(stat -c '%a' -- "$seal")" == "444" ]] \
        || die "profile-control authority changed type or mode at $context"
    [[ "$(sha256sum -- "$seal" | awk '{print $1}')" == "$expected_sha256" ]] \
        || die "profile-control authority changed at $context"
    python3 "$GATE" check-control-seal --seal "$seal" >/dev/null \
        || die "profile fixed control changed at $context"
    assert_screen_seal "$context-after-profile-control"
    assert_profile_capacity_control "$context-after-profile-control"
}

assert_all_profile_control_seals() {
    local context="$1"
    local index
    for index in "${!profile_control_seals[@]}"; do
        assert_profile_control_seal \
            "${profile_control_seals[$index]}" "${profile_control_hashes[$index]}" \
            "$context-$index"
    done
}

assert_profile_capacity_control profile-result-initial

run_selected_policy_preflight() {
    local policy="$1"
    local binary="$2"
    local conf="$3"
    local profile_dir="$4"
    local stdout="$profile_dir/selected-preflight.stdout"
    local stderr="$profile_dir/selected-preflight.stderr"
    local output="$profile_dir/selected-preflight.json"

    [[ "$policy" == "J1" || "$policy" == "J2" || "$policy" == "J3" ]] \
        || die "selected perf policy must be J1, J2, or J3"
    [[ -n "$conf" ]] || die "selected perf policy has no frozen jemalloc configuration"
    assert_jemalloc_host_sources_absent
    assert_screen_seal "$policy-before-selected-preflight"
    env -i LC_ALL=C TZ=UTC _RJEM_MALLOC_CONF="$conf" \
        "$binary" --allocator-preflight >"$stdout" 2>"$stderr"
    assert_screen_seal "$policy-after-selected-preflight"
    python3 "$GATE" parse-preflight \
        --stdout "$stdout" --stderr "$stderr" --source-audit-stderr "$stderr" \
        --binary "$binary" --plan "$PLAN" --phase1-expectations "$EXPECTATIONS" \
        --policy "$policy" --output "$output" >/dev/null
    assert_screen_seal "$policy-after-selected-preflight-gate"
}

run_profile_replay() {
    local profile_kind="$1"
    local policy="$2"
    local binary="$3"
    local conf="$4"
    local profile_dir="$5"
    local profiler_data="$6"
    local profiler_log="$7"
    local analysis="$8"
    local lost_events="$9"
    local config="$RESULT_DIR/configs/$profile_kind.toml"
    local segments="$profile_dir/segments"
    local config_record="$profile_dir/config-render.json"
    local control_seal="$RESULT_DIR/metadata/$profile_kind-$policy-controls.json"
    local control_seal_sha256
    local before_hash
    local after_hash
    local -a run_command
    local -a profile_command
    local -a reports
    local -a selected_policy_args
    local -a profile_control_args
    local reference_corpus_bytes
    local required_free_bytes
    local guardian_free_bytes
    local available_bytes
    local launcher_pid
    local guardian_pid
    local replay_status
    local guardian_status
    local evidence_kind

    python3 "$PHASE1_GATE" render-config \
        --template "$CONFIG_TEMPLATE" --output "$config" \
        --capture "$CAPTURE" --segments-dir "$segments" \
        --stop-after-messages "$stop_after_messages" \
        >"$config_record"
    chmod 0444 -- "$config" "$config_record"
    python3 "$GATE" check-rendered-config \
        --record "$config_record" --config "$config" --capture "$CAPTURE" \
        --segments-dir "$segments" --stop-after-messages "$stop_after_messages" \
        >/dev/null
    profile_control_args=(
        --input "$config"
        --input "$config_record"
        --input "$RESULT_DIR/metadata/capture-inputs-before.json"
        --input "$PROFILE_CAPACITY_CONTROL"
        --input "$PYTHON_RECORD"
        --input "$FROZEN_PROFILE_RUNNER"
    )
    python3 "$GATE" create-control-seal \
        "${profile_control_args[@]}" --output "$control_seal"
    chmod 0444 -- "$control_seal"
    control_seal_sha256="$(sha256sum -- "$control_seal" | awk '{print $1}')"
    profile_control_seals+=("$control_seal")
    profile_control_hashes+=("$control_seal_sha256")
    assert_profile_control_seal \
        "$control_seal" "$control_seal_sha256" "$profile_kind-$policy-rendered"
    before_hash="$(sha256sum -- "$binary" | awk '{print $1}')"
    if [[ "$profile_kind" == "heaptrack" ]]; then
        profile_command=(env -i LC_ALL=C TZ=UTC PATH=/usr/bin:/bin \
            CONFIG_FILE="$config" RUST_LOG="$RUST_LOG_VALUE" \
            heaptrack --record-only -o "$profile_dir/heaptrack.trace" "$binary")
    else
        run_command=(env -i LC_ALL=C TZ=UTC CONFIG_FILE="$config" RUST_LOG="$RUST_LOG_VALUE")
        [[ -n "$conf" ]] && run_command+=("_RJEM_MALLOC_CONF=$conf")
        if [[ "$policy" != "S" ]]; then
            run_command+=("CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_RUNTIME_POLICY=1")
        fi
        run_command+=("$binary")
        profile_command=(perf record --call-graph "dwarf,16384" --freq 199 \
            --output "$profiler_data" -- "${run_command[@]}")
    fi
    assert_jemalloc_host_sources_absent
    assert_profile_control_seal \
        "$control_seal" "$control_seal_sha256" "$profile_kind-$policy-before-ingester"
    check_measurement_conflicts "$profile_dir/processes-before.txt"
    reference_corpus_bytes="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["size_bytes"])' "$REFERENCE_DIR/corpus-summary.json")"
    [[ "$reference_corpus_bytes" =~ ^[1-9][0-9]*$ ]] \
        || die "reference corpus size is not a positive integer"
    required_free_bytes=$((reference_corpus_bytes * 2 + PROFILE_MIN_FREE_BYTES))
    guardian_free_bytes=$((reference_corpus_bytes + PROFILE_MIN_FREE_BYTES))
    available_bytes="$(df -B1 --output=avail "$RESULT_DIR" | awk 'NR == 2 {print $1}')"
    [[ "$available_bytes" =~ ^[0-9]+$ && "$available_bytes" -ge "$required_free_bytes" ]] \
        || die "$profile_kind needs at least $required_free_bytes bytes free before launch"
    local guardian_control="$profile_dir/external-conflict-guardian-control.json"
    local guardian_ready="$profile_dir/external-conflict-guardian-ready"
    local guardian_launch="$profile_dir/external-conflict-guardian-launch"
    active_run_dir="$profile_dir"
    active_guardian_control="$guardian_control"
    active_guardian_ready="$guardian_ready"
    active_guardian_launch="$guardian_launch"
    active_lifecycle=1
    set +e
    defer_cleanup_signals
    (
        cd "$profile_dir"
        while [[ ! -e "$guardian_launch" && ! -L "$guardian_launch" ]]; do
            sleep 0.001
        done
        [[ -f "$guardian_launch" && ! -L "$guardian_launch" \
            && ! -s "$guardian_launch" \
            && "$(stat -c '%a' -- "$guardian_launch")" == 444 ]] || exit 125
        exec "${profile_command[@]}" >"$profile_dir/replay.log" 2>"$profiler_log"
    ) &
    launcher_pid=$!
    active_root_pid="$launcher_pid"
    local root_binding_failed=0
    active_root_starttime_ticks="$(read_live_starttime_ticks "$launcher_pid")" \
        || root_binding_failed=1
    arm_cleanup_signals
    (( root_binding_failed == 0 )) \
        || { cleanup_children; die "$profile_kind root identity binding failed"; }
    defer_cleanup_signals
    python3_background "$GATE" monitor-external-conflicts \
        --pid "$launcher_pid" --output "$profile_dir/external-conflict-guardian.json" \
        --interval-ms 100 --filesystem "$RESULT_DIR" \
        --minimum-free-bytes "$guardian_free_bytes" \
        --control "$guardian_control" --ready "$guardian_ready" \
        --launch "$guardian_launch" \
        >"$profile_dir/external-conflict-guardian.log" 2>&1 &
    guardian_pid=$!
    active_guardian_pid="$guardian_pid"
    local guardian_binding_failed=0
    active_guardian_starttime_ticks="$(read_live_starttime_ticks "$guardian_pid")" \
        || guardian_binding_failed=1
    arm_cleanup_signals
    (( guardian_binding_failed == 0 )) \
        || { cleanup_children; die "$profile_kind guardian identity binding failed"; }
    python3 "$GATE" create-guardian-control --root-pid "$launcher_pid" \
        --guardian-pid "$guardian_pid" --interval-ms 100 \
        --ready "$guardian_ready" --launch "$guardian_launch" \
        --output "$guardian_control" >/dev/null \
        || { cleanup_children; die "$profile_kind guardian control failed"; }
    python3 "$GATE" wait-guardian-ready --control "$guardian_control" \
        --ready "$guardian_ready" --launch "$guardian_launch" \
        --interval-ms 100 --timeout-ms 5000 >/dev/null \
        || { cleanup_children; die "$profile_kind guardian readiness failed"; }
    python3 "$GATE" release-guardian-launch --control "$guardian_control" \
        --ready "$guardian_ready" --launch "$guardian_launch" \
        --interval-ms 100 >/dev/null \
        || { cleanup_children; die "$profile_kind guardian release failed"; }
    wait "$launcher_pid"
    replay_status=$?
    wait "$guardian_pid"
    guardian_status=$?
    clear_active_processes
    set -e
    printf '%s\n' "$replay_status" >"$profile_dir/replay.exit-status"
    printf '%s\n' "$guardian_status" \
        >"$profile_dir/external-conflict-guardian.exit-status"
    (( replay_status == 0 )) || die "$profile_kind replay failed"
    (( guardian_status == 0 )) || die "$profile_kind guardian failed"
    assert_profile_control_seal \
        "$control_seal" "$control_seal_sha256" "$profile_kind-$policy-after-ingester"
    after_hash="$(sha256sum -- "$binary" | awk '{print $1}')"
    [[ "$before_hash" == "$after_hash" ]] || die "$profile_kind binary changed"

    if [[ "$profile_kind" == "heaptrack" ]]; then
        mapfile -d '' -t trace_files \
            < <(find "$profile_dir" -maxdepth 1 -type f -name 'heaptrack.trace*' -print0)
        (( ${#trace_files[@]} == 1 )) \
            || die "heaptrack must produce exactly one trace; found ${#trace_files[@]}"
        [[ "${trace_files[0]}" == "$profiler_data" ]] \
            || die "heaptrack trace path differs from the frozen output path"
        heaptrack_print -f "$profiler_data" --peak-limit 50 --sub-peak-limit 20 \
            --flamegraph-cost-type allocations --print-flamegraph "$analysis" \
            >"$profile_dir/heaptrack-summary.txt" \
            2>"$profile_dir/heaptrack-print.log"
        rg -i 'lost (samples?|events?)|PERF_RECORD_LOST|dropped samples?' \
            "$profiler_log" "$profile_dir/heaptrack-print.log" >"$lost_events" || true
    else
        perf report --stdio --header --input "$profiler_data" \
            --sort comm,dso,symbol >"$profile_dir/perf-summary.txt" \
            2>"$profile_dir/perf-report.log"
        set +e
        perf script --input "$profiler_data" --show-lost-events \
            >"$analysis" 2>"$profile_dir/perf-script.log"
        perf_script_status=$?
        set -e
        (( perf_script_status == 0 )) || die "perf script could not audit lost events"
        rg -i 'PERF_RECORD_LOST|lost[^[:cntrl:]]*(samples?|events?|chunks?)|chunks?[^[:cntrl:]]*LOST|dropped[^[:cntrl:]]*samples?' \
            "$analysis" "$profile_dir/perf-script.log" "$profiler_log" \
            >"$lost_events" || true
    fi

    selected_policy_args=()
    if [[ "$profile_kind" == "perf-record" && "$policy" != "S" ]]; then
        {
            cat "$profile_dir/replay.log"
            cat "$profiler_log"
        } >"$profile_dir/selected-runtime-combined.log"
        python3 "$GATE" gate-profile-runtime-log \
            --log "$profile_dir/selected-runtime-combined.log" \
            --preflight "$profile_dir/selected-preflight.json" \
            --plan "$PLAN" --phase1-expectations "$EXPECTATIONS" \
            --policy "$policy" --output "$profile_dir/selected-runtime-policy.json"
        selected_policy_args=(
            --selected-runtime-log "$profile_dir/selected-runtime-combined.log"
            --selected-preflight "$profile_dir/selected-preflight.json"
        )
    fi

    mapfile -d '' -t reports \
        < <(find "$profile_dir" -maxdepth 1 -type f -name 'ingestion_stats_*.md' -print0)
    (( ${#reports[@]} == 1 )) \
        || die "$profile_kind must produce exactly one ingestion report; found ${#reports[@]}"
    python3 "$REPORT_GATE" replay-report --report "${reports[0]}" \
        --output "$profile_dir/replay-correctness.json"
    python3 "$PHASE1_GATE" tree-manifest --corpus "$segments" \
        --manifest "$profile_dir/segments.sha256" \
        --inventory "$profile_dir/segments.tsv" \
        --summary "$profile_dir/corpus-summary.json" >/dev/null
    cmp -s "$profile_dir/segments.sha256" "$REFERENCE_DIR/segments.sha256" \
        || die "$profile_kind corpus differs from the measured reference"
    cmp -s "$profile_dir/replay-correctness.json" \
        "$REFERENCE_DIR/replay-correctness.json" \
        || die "$profile_kind replay correctness differs from the measured reference"
    assert_profile_control_seal \
        "$control_seal" "$control_seal_sha256" \
        "$profile_kind-$policy-before-storage-verify"
    env -i LC_ALL=C TZ=UTC "$STORAGE_VERIFY_BINARY" \
        --segments-dir "$segments" --schema schema8 \
        --validate-segment-footers --verify-exact-postings \
        >"$profile_dir/storage-verify.json" 2>"$profile_dir/storage-verify.log"
    assert_profile_control_seal \
        "$control_seal" "$control_seal_sha256" \
        "$profile_kind-$policy-after-storage-verify"
    assert_profile_control_seal \
        "$control_seal" "$control_seal_sha256" "$profile_kind-$policy-before-query"
    env -i LC_ALL=C TZ=UTC "$QUERY_BINARY" \
        --segments-dir "$segments" --storage-layout schema8 \
        --sample-limit-per-kind 2 --verify-readbacks \
        --output "$profile_dir/readbacks.md" \
        >"$profile_dir/readbacks.log" 2>&1
    assert_profile_control_seal \
        "$control_seal" "$control_seal_sha256" "$profile_kind-$policy-after-query"
    python3 "$GATE" record-profile-evidence \
        --profile-kind "$profile_kind" --policy "$policy" --binary "$binary" \
        --screen-result "$SCREEN_RESULT_DIR" \
        --screen-artifact-manifest "$SCREEN_ARTIFACT_MANIFEST" \
        --system-binary "$SYSTEM_BINARY" --jemalloc-binary "$JEMALLOC_BINARY" \
        --query-binary "$QUERY_BINARY" \
        --storage-verify-binary "$STORAGE_VERIFY_BINARY" \
        --profile-data "$profiler_data" --profiler-log "$profiler_log" \
        --analysis "$analysis" --lost-events "$lost_events" \
        --profile-manifest "$profile_dir/segments.sha256" \
        --reference-manifest "$REFERENCE_DIR/segments.sha256" \
        --profile-correctness "$profile_dir/replay-correctness.json" \
        --reference-correctness "$REFERENCE_DIR/replay-correctness.json" \
        --profile-corpus "$profile_dir/corpus-summary.json" \
        --reference-corpus "$REFERENCE_DIR/corpus-summary.json" \
        --storage "$profile_dir/storage-verify.json" \
        --readbacks "$profile_dir/readbacks.md" \
        --calibration "$CALIBRATION_DIR/calibration.json" \
        --calibration-storage "$CALIBRATION_DIR/storage-verify.json" \
        --calibration-readbacks "$CALIBRATION_DIR/readbacks.md" \
        --calibration-correctness "$CALIBRATION_DIR/replay-correctness.json" \
        --calibration-corpus "$CALIBRATION_DIR/corpus-summary.json" \
        --final-decision "$FINAL_DECISION" --complete-marker "$COMPLETE_MARKER" \
        --build-provenance "$BUILD_PROVENANCE" --plan "$PLAN" \
        --phase1-expectations "$EXPECTATIONS" \
        "${selected_policy_args[@]}" \
        --output "$profile_dir/profile-evidence.json"
    if [[ "$profile_kind" == "heaptrack" ]]; then
        evidence_kind=profile-heaptrack
    elif [[ "$policy" == "S" ]]; then
        evidence_kind=profile-perf-system
    else
        evidence_kind=profile-perf-selected
    fi
    python3 "$GATE" seal-evidence-tree \
        --root "$profile_dir" --kind "$evidence_kind" \
        --output "$RESULT_DIR/metadata/raw-authorities/$profile_kind.json"
    assert_profile_control_seal \
        "$control_seal" "$control_seal_sha256" \
        "$profile_kind-$policy-after-profile-evidence"
}

HEAPTRACK_DIR="$RESULT_DIR/heaptrack"
run_profile_replay heaptrack S "$SYSTEM_BINARY" "" "$HEAPTRACK_DIR" \
    "$HEAPTRACK_DIR/heaptrack.trace.zst" "$HEAPTRACK_DIR/heaptrack.log" \
    "$HEAPTRACK_DIR/heaptrack-stacks.txt" "$HEAPTRACK_DIR/lost-events.txt"

if (( ENABLE_PERF_RECORD == 1 )); then
    mkdir "$RESULT_DIR/perf-record"
    if [[ "$PERF_POLICY" == "selected" ]]; then
        selected_policy="$(python3 -c 'import json,sys; value=json.load(open(sys.argv[1]))["selected_full_gate_policy"]; print("" if value is None else value)' "$FINAL_DECISION")"
        [[ -n "$selected_policy" ]] \
            || die "the completed screen selected no policy for optional perf recording"
        perf_policy="$selected_policy"
        perf_binary="$JEMALLOC_BINARY"
        perf_conf="$(python3 -c 'import json,sys; value=json.load(open(sys.argv[1]))["policies"][sys.argv[2]]["jemalloc_conf"]; print("" if value is None else value)' "$PLAN" "$perf_policy")"
    else
        perf_policy=S
        perf_binary="$SYSTEM_BINARY"
        perf_conf=""
    fi
    PERF_DIR="$RESULT_DIR/perf-record"
    if [[ "$perf_policy" != "S" ]]; then
        run_selected_policy_preflight "$perf_policy" "$perf_binary" "$perf_conf" "$PERF_DIR"
    fi
    run_profile_replay perf-record "$perf_policy" "$perf_binary" "$perf_conf" \
        "$PERF_DIR" "$PERF_DIR/perf.data" "$PERF_DIR/perf-record.log" \
        "$PERF_DIR/perf-script.txt" "$PERF_DIR/lost-events.txt"
fi

python3 "$PHASE1_GATE" validate-inputs \
    --capture "$CAPTURE" --template "$CONFIG_TEMPLATE" \
    --expectations "$EXPECTATIONS" \
    --output "$RESULT_DIR/metadata/capture-inputs-after.json"
chmod 0444 -- "$RESULT_DIR/metadata/capture-inputs-after.json"
cmp -s "$RESULT_DIR/metadata/capture-inputs-before.json" \
    "$RESULT_DIR/metadata/capture-inputs-after.json" \
    || die "source capture or configuration changed during profiling"
assert_screen_seal profile-finalization
assert_profile_capacity_control profile-finalization
python3 "$GATE" validate-final-artifacts \
    --result-root "$SCREEN_RESULT_DIR" --stage complete >/dev/null \
    || die "completed screen exact inventory failed after profiling"
assert_all_profile_control_seals profile-finalization
python3 "$GATE" revalidate-profile-from-raw \
    --result-root "$RESULT_DIR" --screen-result "$SCREEN_RESULT_DIR" \
    --output "$RESULT_DIR/metadata/final-raw-revalidation.json"
chmod 0444 -- "$RESULT_DIR/metadata/final-raw-revalidation.json"
python3 "$GATE" create-profile-artifact-inventory \
    --result-root "$RESULT_DIR" \
    --files "$RESULT_DIR/metadata/artifacts.nul" \
    --directories "$RESULT_DIR/metadata/directories.nul" \
    --manifest "$RESULT_DIR/metadata/artifacts.sha256" >/dev/null
python3 "$GATE" validate-profile-artifacts \
    --result-root "$RESULT_DIR" --stage precomplete \
    --output "$RESULT_DIR/metadata/FINAL_SEAL_VALIDATED.json"
python3 "$GATE" revalidate-profile-from-raw \
    --result-root "$RESULT_DIR" --screen-result "$SCREEN_RESULT_DIR" >/dev/null
printf '%s\n' 'chronoxide/allocator-profile-complete/v1' >"$RESULT_DIR/COMPLETE"
chmod 0444 -- "$RESULT_DIR/COMPLETE"
python3 "$GATE" validate-profile-artifacts \
    --result-root "$RESULT_DIR" --stage complete >/dev/null
python3 "$GATE" revalidate-profile-from-raw \
    --result-root "$RESULT_DIR" --screen-result "$SCREEN_RESULT_DIR" >/dev/null
printf 'Phase 5 allocator profile complete: %s\n' "$RESULT_DIR"
