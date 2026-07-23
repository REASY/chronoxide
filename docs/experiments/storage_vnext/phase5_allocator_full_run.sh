#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="$SCRIPT_DIR/phase5_allocator_full_gate.py"
PLAN="$SCRIPT_DIR/phase5_allocator_full_plan.json"

SCREEN_RESULT_DIR="${SCREEN_RESULT_DIR:-}"
RESULT_DIR="${RESULT_DIR:-}"
CAPTURE="${CAPTURE:-}"
CONFIG_TEMPLATE="${CONFIG_TEMPLATE:-}"
RUN_NOTE="${RUN_NOTE:-}"
QUIET_HOST_CONFIRMED="${QUIET_HOST_CONFIRMED:-0}"
DRY_RUN=0
VALIDATE_ONLY=0

usage() {
    printf '%s\n' \
        'Usage:' \
        "  SCREEN_RESULT_DIR=/absolute/completed/allocator-screen \\" \
        "  RESULT_DIR=/absolute/new/external/full-gate-result \\" \
        "  CAPTURE=/absolute/capture CONFIG_TEMPLATE=/absolute/config.toml \\" \
        "  QUIET_HOST_CONFIRMED=1 \\" \
        "  RUN_NOTE='quiet host; no competing builds, profilers, scans, or databases' \\" \
        "    \"\$SCREEN_RESULT_DIR/build-source/docs/experiments/storage_vnext/phase5_allocator_full_run.sh\"" \
        '    [--dry-run|--validate-only]' \
        '' \
        'The completed 250k screen dynamically supplies exactly one nominated J1-J3 policy.' \
        'Formal execution runs 4M stats-enabled S,C,C,S and then 4M plain-jemalloc' \
        'S,N,N,S. The harness never authorizes production promotion.'
}

die() {
    printf 'Phase 5 allocator full gate: %s\n' "$*" >&2
    exit 2
}

note() {
    printf 'Phase 5 allocator full gate: %s\n' "$*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"
}

assert_jemalloc_host_sources_absent() {
    local path
    for path in /etc/malloc.conf /etc/_rjem_malloc.conf; do
        [[ ! -e "$path" && ! -L "$path" ]] \
            || die "ambient jemalloc configuration source is forbidden: $path"
    done
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

for forbidden in \
        LD_PRELOAD LD_LIBRARY_PATH MALLOC_CONF _RJEM_MALLOC_CONF \
        MALLOC_ARENA_MAX MALLOC_ARENA_TEST MALLOC_TRIM_THRESHOLD_ MALLOC_TOP_PAD_ \
        MALLOC_MMAP_THRESHOLD_ MALLOC_MMAP_MAX_ MALLOC_CHECK_ MALLOC_PERTURB_ \
        GLIBC_TUNABLES JEMALLOC_SYS_WITH_MALLOC_CONF \
        RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER \
        CFLAGS CXXFLAGS CPPFLAGS LDFLAGS CC CXX AR RANLIB \
        PYTHONPATH PYTHONHOME PYTHONUSERBASE PYTHONSTARTUP PYTHONINSPECT; do
    [[ -z "${!forbidden-}" ]] || die "ambient $forbidden is forbidden"
done
while IFS='=' read -r environment_name _; do
    case "$environment_name" in
        GIT_*) die "ambient $environment_name is forbidden" ;;
    esac
done < <(env)
assert_jemalloc_host_sources_absent

for command in awk bash cargo chmod cmp cp date env file fincore find git mkdir perf \
        python3 readelf realpath sha256sum sort stat uname /usr/bin/time; do
    require_command "$command"
done

PYTHON_BIN="$(realpath -e -- "$(command -v python3)")"
[[ -f "$PYTHON_BIN" && ! -L "$PYTHON_BIN" && -x "$PYTHON_BIN" ]] \
    || die "python3 must resolve to an executable non-symlink regular file"
PYTHON_BIN_SHA256="$(sha256sum -- "$PYTHON_BIN" | awk '{print $1}')"
PYTHON_FLAGS_PROBE="$("$PYTHON_BIN" -I -S -B -c \
    'import sys; print(":".join(str(int(value)) for value in (sys.flags.isolated, sys.flags.no_site, sys.flags.dont_write_bytecode, sys.flags.ignore_environment, sys.flags.safe_path)))')"
[[ "$PYTHON_FLAGS_PROBE" == "1:1:1:1:1" ]] \
    || die "python3 does not honor -I -S -B isolation"
PYTHON_SCRIPT_BOOTSTRAP='import os,stat,sys; script=os.path.realpath(sys.argv[1]); mode=os.lstat(script).st_mode; assert stat.S_ISREG(mode) and not os.path.islink(script); sys.argv=sys.argv[1:]; namespace={"__name__":"__main__","__file__":script,"__package__":None,"__cached__":None}; source=open(script,"rb").read(); exec(compile(source,script,"exec",dont_inherit=True),namespace)'
assert_python() {
    [[ "$(sha256sum -- "$PYTHON_BIN" | awk '{print $1}')" == "$PYTHON_BIN_SHA256" ]] \
        || die "pinned python3 interpreter changed"
}
python3() {
    local script status
    local -a command
    assert_python
    if (( $# > 0 )) && [[ "$1" == *.py ]]; then
        script="$1"
        shift
        command=("$PYTHON_BIN" -I -S -B -c "$PYTHON_SCRIPT_BOOTSTRAP" "$script" "$@")
    else
        command=("$PYTHON_BIN" -I -S -B "$@")
    fi
    if "${command[@]}"; then status=0; else status=$?; fi
    assert_python
    return "$status"
}
python3_background() {
    local script
    local -a command
    assert_python
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
readonly PYTHON_BIN PYTHON_BIN_SHA256 PYTHON_FLAGS_PROBE PYTHON_SCRIPT_BOOTSTRAP
verify_background_python_pid_binding

[[ "$SCREEN_RESULT_DIR" == /* && -d "$SCREEN_RESULT_DIR" && ! -L "$SCREEN_RESULT_DIR" ]] \
    || die "SCREEN_RESULT_DIR must be an absolute completed screen result"
[[ "$CAPTURE" == /* && -d "$CAPTURE" && ! -L "$CAPTURE" ]] \
    || die "CAPTURE must be an absolute non-symlink directory"
[[ "$CONFIG_TEMPLATE" == /* && -f "$CONFIG_TEMPLATE" && ! -L "$CONFIG_TEMPLATE" ]] \
    || die "CONFIG_TEMPLATE must be an absolute non-symlink regular file"
SCREEN_RESULT_DIR="$(realpath -e -- "$SCREEN_RESULT_DIR")"
CAPTURE="$(realpath -e -- "$CAPTURE")"
CONFIG_TEMPLATE="$(realpath -e -- "$CONFIG_TEMPLATE")"

SCREEN_BUILD_SOURCE="$SCREEN_RESULT_DIR/build-source"
EXPECTED_RUNNER="$SCREEN_BUILD_SOURCE/docs/experiments/storage_vnext/phase5_allocator_full_run.sh"
[[ "$(realpath -e -- "${BASH_SOURCE[0]}")" == "$(realpath -e -- "$EXPECTED_RUNNER")" ]] \
    || die "execute the full-gate runner frozen in the completed screen build-source"
python3 "$GATE" validate-plan --plan "$PLAN" >/dev/null
SCREEN_HARNESS="$SCREEN_RESULT_DIR/metadata/harness"
SCREEN_GATE="$SCREEN_HARNESS/phase5_allocator_screen_gate.py"
[[ -f "$SCREEN_GATE" && ! -L "$SCREEN_GATE" ]] \
    || die "completed screen gate is missing"
python3 "$SCREEN_GATE" validate-final-artifacts \
    --result-root "$SCREEN_RESULT_DIR" --stage complete >/dev/null
python3 "$GATE" bind-screen --screen-result "$SCREEN_RESULT_DIR" --plan "$PLAN" >/dev/null

PHASE1_GATE="$SCREEN_HARNESS/phase1_replay_gate.py"
EXPECTATIONS="$SCREEN_HARNESS/phase1_4m_expectations.json"
REPORT_GATE="$SCREEN_HARNESS/ab_gate.py"
FADVISE_BINARY="$SCREEN_RESULT_DIR/metadata/tools/fadvise-regular-dontneed"
SCREEN_SYSTEM="$SCREEN_RESULT_DIR/metadata/binaries/chronoxide-ingester-system"
SCREEN_STATS="$SCREEN_RESULT_DIR/metadata/binaries/chronoxide-ingester-jemalloc"
SCREEN_QUERY="$SCREEN_RESULT_DIR/metadata/binaries/chronoxide-query"
SCREEN_STORAGE_VERIFY="$SCREEN_RESULT_DIR/metadata/binaries/chronoxide-storage-verify"
for file in "$SCREEN_GATE" "$PHASE1_GATE" "$EXPECTATIONS" "$REPORT_GATE" \
        "$FADVISE_BINARY" "$SCREEN_SYSTEM" "$SCREEN_STATS" "$SCREEN_QUERY" \
        "$SCREEN_STORAGE_VERIFY"; do
    [[ -f "$file" && ! -L "$file" ]] || die "screen authority is missing: $file"
done

CAPTURE_INPUTS="$(python3 "$PHASE1_GATE" validate-inputs \
    --capture "$CAPTURE" --template "$CONFIG_TEMPLATE" \
    --expectations "$EXPECTATIONS")"

if [[ "$VALIDATE_ONLY" == "1" ]]; then
    [[ -n "$RESULT_DIR" && "$RESULT_DIR" == /* ]] \
        || die "RESULT_DIR must name the proposed new absolute result path"
    result_parent="$(realpath -e -- "$(dirname "$RESULT_DIR")")"
    python3 "$GATE" check-capacity --result-parent "$result_parent" \
        --expectations "$EXPECTATIONS" --plan "$PLAN" >/dev/null
    note "validation complete; RESULT_DIR was not created"
    exit 0
fi

[[ -n "$RESULT_DIR" && "$RESULT_DIR" == /* ]] \
    || die "RESULT_DIR must be a new absolute external path"
result_parent="$(realpath -e -- "$(dirname "$RESULT_DIR")")"
RESULT_DIR="$result_parent/$(basename "$RESULT_DIR")"
[[ ! -e "$RESULT_DIR" && ! -L "$RESULT_DIR" ]] \
    || die "RESULT_DIR already exists: $RESULT_DIR"
case "$RESULT_DIR/" in
    "$SCREEN_RESULT_DIR/"*|"$CAPTURE/"*)
        die "RESULT_DIR must be outside the screen result and capture"
        ;;
esac
if [[ "$DRY_RUN" != "1" ]]; then
    [[ "$QUIET_HOST_CONFIRMED" == "1" ]] \
        || die "formal execution requires QUIET_HOST_CONFIRMED=1"
    [[ -n "$RUN_NOTE" && "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
        || die "RUN_NOTE is required and must be one line"
fi
for cargo_config in "$HOME/.cargo/config" "$HOME/.cargo/config.toml"; do
    [[ ! -e "$cargo_config" && ! -L "$cargo_config" ]] \
        || die "ambient Cargo home configuration is forbidden: $cargo_config"
done
if [[ "$DRY_RUN" != "1" ]]; then
    python3 "$GATE" scan-conflicts >/dev/null \
        || die "quiet-host preflight failed before RESULT_DIR creation"
fi
python3 "$GATE" check-capacity --result-parent "$result_parent" \
    --expectations "$EXPECTATIONS" --plan "$PLAN" >/dev/null \
    || die "initial result-filesystem capacity check failed"

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/metadata" "$RESULT_DIR/configs" "$RESULT_DIR/runs" \
    "$RESULT_DIR/validation" "$RESULT_DIR/comparisons" "$RESULT_DIR/build-target"
mkdir "$RESULT_DIR/metadata/harness" "$RESULT_DIR/metadata/preflight" \
    "$RESULT_DIR/metadata/build" "$RESULT_DIR/metadata/binaries" \
    "$RESULT_DIR/metadata/raw-authorities" "$RESULT_DIR/metadata/input-controls" \
    "$RESULT_DIR/metadata/final-controls"
printf '%s\n' \
    'Partial and non-promotable unless COMPLETE and the final decision both pass' \
    'fresh raw-evidence admission. The harness never authorizes promotion.' \
    >"$RESULT_DIR/PARTIAL_UNLESS_COMPLETE.txt"
printf 'stage\tposition\ttoken\tlabel\n' >"$RESULT_DIR/run-plan.tsv"
for stage in stats no-stats; do
    tokens=(S C C S)
    [[ "$stage" == "no-stats" ]] && tokens=(S N N S)
    for position in 1 2 3 4; do
        token="${tokens[$((position - 1))]}"
        printf '%s\t%s\t%s\t%s-%02d-%s\n' \
            "$stage" "$position" "$token" "$stage" "$position" "$token" \
            >>"$RESULT_DIR/run-plan.tsv"
    done
done

for harness_file in phase5_allocator_full_gate.py phase5_allocator_full_plan.json \
        phase5_allocator_full_run.sh test_phase5_allocator_full_gate.py; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$RESULT_DIR/metadata/harness/$harness_file"
done
chmod 0555 -- "$RESULT_DIR/metadata/harness/phase5_allocator_full_run.sh"
chmod 0444 -- "$RESULT_DIR/metadata/harness/"*.py \
    "$RESULT_DIR/metadata/harness/phase5_allocator_full_plan.json"
FROZEN_GATE="$RESULT_DIR/metadata/harness/phase5_allocator_full_gate.py"
FROZEN_PLAN="$RESULT_DIR/metadata/harness/phase5_allocator_full_plan.json"

SCREEN_BINDING="$RESULT_DIR/metadata/input-controls/screen-binding.json"
python3 "$FROZEN_GATE" bind-screen --screen-result "$SCREEN_RESULT_DIR" \
    --plan "$FROZEN_PLAN" --output "$SCREEN_BINDING"
printf '%s\n' "$CAPTURE_INPUTS" \
    >"$RESULT_DIR/metadata/input-controls/capture-inputs-before.json"
printf '%s\n' "$RUN_NOTE" >"$RESULT_DIR/metadata/input-controls/run-note.txt"
printf '%s\n' "$QUIET_HOST_CONFIRMED" \
    >"$RESULT_DIR/metadata/input-controls/quiet-host-confirmed.txt"
{
    printf 'path=%s\n' "$PYTHON_BIN"
    printf 'sha256=%s\n' "$PYTHON_BIN_SHA256"
    printf 'flags=%s\n' "$PYTHON_FLAGS_PROBE"
} >"$RESULT_DIR/metadata/input-controls/python-interpreter.txt"
python3 "$FROZEN_GATE" check-capacity --result-parent "$result_parent" \
    --expectations "$EXPECTATIONS" --plan "$FROZEN_PLAN" \
    --output "$RESULT_DIR/metadata/input-controls/capacity.json"

cp -- "$SCREEN_SYSTEM" "$RESULT_DIR/metadata/binaries/chronoxide-ingester-system"
cp -- "$SCREEN_STATS" "$RESULT_DIR/metadata/binaries/chronoxide-ingester-jemalloc-stats"
cp -- "$SCREEN_QUERY" "$RESULT_DIR/metadata/binaries/chronoxide-query"
cp -- "$SCREEN_STORAGE_VERIFY" "$RESULT_DIR/metadata/binaries/chronoxide-storage-verify"
RUN_SYSTEM="$RESULT_DIR/metadata/binaries/chronoxide-ingester-system"
RUN_STATS="$RESULT_DIR/metadata/binaries/chronoxide-ingester-jemalloc-stats"
RUN_QUERY="$RESULT_DIR/metadata/binaries/chronoxide-query"
RUN_STORAGE_VERIFY="$RESULT_DIR/metadata/binaries/chronoxide-storage-verify"
chmod 0555 -- "$RUN_SYSTEM" "$RUN_STATS" "$RUN_QUERY" "$RUN_STORAGE_VERIFY"

selected_conf="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["selected_jemalloc_conf"])' "$SCREEN_BINDING")"
[[ -n "$selected_conf" && "$selected_conf" != *$'\n'* ]] \
    || die "screen binding has an invalid selected policy"

BUILD_LOG="$RESULT_DIR/metadata/build/no-stats.log"
BUILD_TARGET="$RESULT_DIR/build-target"
BUILD_PATH="$HOME/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
BUILD_CARGO="$HOME/.cargo/bin/cargo"
BUILD_RUSTC="$HOME/.cargo/bin/rustc"
BUILD_RUSTDOC="$HOME/.cargo/bin/rustdoc"
[[ "$(command -v cargo)" == "$BUILD_CARGO" ]] \
    || die "cargo must resolve to the canonical rustup proxy: $BUILD_CARGO"
TOOLCHAIN_BINDING="$RESULT_DIR/metadata/build/toolchain-binding.json"
python3 "$FROZEN_GATE" bind-toolchain \
    --screen-environment "$SCREEN_RESULT_DIR/metadata/environment.txt" \
    --build-source "$SCREEN_BUILD_SOURCE" --cargo "$BUILD_CARGO" \
    --rustc "$BUILD_RUSTC" --rustdoc "$BUILD_RUSTDOC" \
    --output "$TOOLCHAIN_BINDING"
BUILD_COMMAND='cargo build --manifest-path Cargo.toml --locked --release --no-default-features --features jemalloc -p chronoxide-ingester --bin chronoxide-ingester'
{
    printf 'COMMAND\t%s\n' "$BUILD_COMMAND"
    printf 'CWD\t%s\n' "$SCREEN_BUILD_SOURCE"
    printf 'ENV\tHOME=%s\tPATH=%s\tCARGO_HOME=%s/.cargo\tRUSTUP_HOME=%s/.rustup\tRUSTC=%s/.cargo/bin/rustc\tRUSTDOC=%s/.cargo/bin/rustdoc\tLC_ALL=C\tTZ=UTC\tCARGO_INCREMENTAL=0\tCARGO_TARGET_DIR=%s\n' \
        "$HOME" "$BUILD_PATH" "$HOME" "$HOME" "$HOME" "$HOME" "$BUILD_TARGET"
} >"$BUILD_LOG"
note "building the plain no-stats jemalloc comparator from the screen archive"
(
    cd "$SCREEN_BUILD_SOURCE"
    env -i HOME="$HOME" PATH="$BUILD_PATH" CARGO_HOME="$HOME/.cargo" \
        RUSTUP_HOME="$HOME/.rustup" RUSTC="$HOME/.cargo/bin/rustc" \
        RUSTDOC="$HOME/.cargo/bin/rustdoc" LC_ALL=C TZ=UTC \
        CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$BUILD_TARGET" \
        cargo build --manifest-path Cargo.toml --locked --release \
            --no-default-features --features jemalloc -p chronoxide-ingester \
            --bin chronoxide-ingester
) >>"$BUILD_LOG" 2>&1
POST_BUILD_SCREEN_VALIDATION="$RESULT_DIR/metadata/build/screen-validation-after-no-stats-build.json"
python3 "$FROZEN_GATE" check-screen-binding --binding "$SCREEN_BINDING" \
    --plan "$FROZEN_PLAN" --full --output "$POST_BUILD_SCREEN_VALIDATION"
cp -- "$BUILD_TARGET/release/chronoxide-ingester" \
    "$RESULT_DIR/metadata/binaries/chronoxide-ingester-jemalloc"
RUN_NO_STATS="$RESULT_DIR/metadata/binaries/chronoxide-ingester-jemalloc"
chmod 0555 -- "$RUN_NO_STATS"
file -- "$RUN_NO_STATS" >"$RESULT_DIR/metadata/build/no-stats.file.txt"
readelf -n -- "$RUN_NO_STATS" >"$RESULT_DIR/metadata/build/no-stats.elf-notes.txt"

run_preflight() {
    local role="$1"
    local binary="$2"
    local conf="$3"
    local raw="$RESULT_DIR/metadata/preflight/$role.application.json"
    local stderr="$RESULT_DIR/metadata/preflight/$role.stderr"
    local -a command=(env -i LC_ALL=C TZ=UTC)
    [[ -n "$conf" ]] && command+=("_RJEM_MALLOC_CONF=$conf")
    command+=("$binary" --allocator-preflight)
    "${command[@]}" >"$raw" 2>"$stderr"
    python3 "$FROZEN_GATE" parse-preflight --raw "$raw" --stderr "$stderr" \
        --role "$role" --binary "$binary" --screen-binding "$SCREEN_BINDING" \
        --output "$RESULT_DIR/metadata/preflight/$role.json"
}
run_preflight system "$RUN_SYSTEM" ''
run_preflight stats-candidate "$RUN_STATS" "$selected_conf"
run_preflight no-stats-candidate "$RUN_NO_STATS" "$selected_conf"

NO_STATS_BUILD="$RESULT_DIR/metadata/build/no-stats-build.json"
python3 "$FROZEN_GATE" record-no-stats-build \
    --screen-binding "$SCREEN_BINDING" --plan "$FROZEN_PLAN" \
    --build-log "$BUILD_LOG" --binary "$RUN_NO_STATS" \
    --preflight "$RESULT_DIR/metadata/preflight/no-stats-candidate.json" \
    --target-dir "$BUILD_TARGET" --toolchain "$TOOLCHAIN_BINDING" \
    --post-build-screen-validation "$POST_BUILD_SCREEN_VALIDATION" \
    --output "$NO_STATS_BUILD"

perf_events='task-clock,cycles,instructions,branches,branch-misses,cache-references,cache-misses,page-faults,minor-faults,major-faults,context-switches,cpu-migrations'
perf_required_args=()
IFS=',' read -r -a perf_event_array <<<"$perf_events"
for event in "${perf_event_array[@]}"; do
    perf_required_args+=(--require-event "$event")
done
if [[ "$DRY_RUN" != "1" ]]; then
    python3 "$FROZEN_GATE" scan-conflicts \
        --output "$RESULT_DIR/metadata/input-controls/processes-after-build.json" >/dev/null
    set +e
    perf stat --no-big-num --field-separator $'\t' --event "$perf_events" \
        --output "$RESULT_DIR/metadata/input-controls/perf-stat-preflight.tsv" -- \
        "$PYTHON_BIN" -I -S -B -c 'sum(range(10000000))' \
        >"$RESULT_DIR/metadata/input-controls/perf-stat-preflight.log" 2>&1
    perf_preflight_status=$?
    set -e
    printf '%s\n' "$perf_preflight_status" \
        >"$RESULT_DIR/metadata/input-controls/perf-stat-preflight.exit-status"
    (( perf_preflight_status == 0 )) || die "perf stat preflight failed"
    python3 "$PHASE1_GATE" parse-perf-stat \
        --input "$RESULT_DIR/metadata/input-controls/perf-stat-preflight.tsv" \
        --output "$RESULT_DIR/metadata/input-controls/perf-stat-preflight.json" \
        "${perf_required_args[@]}" >/dev/null
fi

python3 "$FROZEN_GATE" seal-authority --root "$RESULT_DIR/metadata/harness" \
    --output "$RESULT_DIR/metadata/raw-authorities/harness.tsv" >/dev/null
python3 "$FROZEN_GATE" seal-authority --root "$RESULT_DIR/metadata/preflight" \
    --output "$RESULT_DIR/metadata/raw-authorities/preflight.tsv" >/dev/null
python3 "$FROZEN_GATE" seal-authority --root "$RESULT_DIR/metadata/build" \
    --output "$RESULT_DIR/metadata/raw-authorities/build.tsv" >/dev/null
python3 "$FROZEN_GATE" seal-authority --root "$RESULT_DIR/metadata/binaries" \
    --output "$RESULT_DIR/metadata/raw-authorities/binaries.tsv" >/dev/null
python3 "$FROZEN_GATE" seal-authority --root "$RESULT_DIR/metadata/input-controls" \
    --output "$RESULT_DIR/metadata/raw-authorities/input-controls.tsv" >/dev/null

stop_after_messages=4000000
for stage in stats no-stats; do
    tokens=(S C C S)
    [[ "$stage" == "no-stats" ]] && tokens=(S N N S)
    for position in 1 2 3 4; do
        token="${tokens[$((position - 1))]}"
        label="$stage-$(printf '%02d' "$position")-$token"
        mkdir "$RESULT_DIR/runs/$label"
        python3 "$PHASE1_GATE" render-config --template "$CONFIG_TEMPLATE" \
            --output "$RESULT_DIR/configs/$label.toml" --capture "$CAPTURE" \
            --segments-dir "$RESULT_DIR/runs/$label/segments" \
            --stop-after-messages "$stop_after_messages" \
            >"$RESULT_DIR/configs/$label.render.json"
    done
done
python3 "$FROZEN_GATE" seal-authority --root "$RESULT_DIR/configs" \
    --output "$RESULT_DIR/metadata/raw-authorities/configs.tsv" >/dev/null

assert_fixed_inputs() {
    local context="$1"
    python3 "$FROZEN_GATE" check-screen-binding --binding "$SCREEN_BINDING" \
        --plan "$FROZEN_PLAN" >/dev/null \
        || die "screen binding changed at $context"
    for authority in harness preflight build binaries input-controls configs; do
        python3 "$FROZEN_GATE" check-authority \
            --authority "$RESULT_DIR/metadata/raw-authorities/$authority.tsv" \
            >/dev/null || die "$authority authority changed at $context"
    done
}
assert_fixed_inputs setup-complete

if [[ "$DRY_RUN" == "1" ]]; then
    printf '%s\n' 'Dry run only: no replay, perf measurement, footer scan, or readback ran.' \
        >"$RESULT_DIR/metadata/DRY_RUN_NOT_EVIDENCE.txt"
    chmod 0444 -- "$RESULT_DIR/metadata/DRY_RUN_NOT_EVIDENCE.txt"
    note "dry run complete; no replay launched: $RESULT_DIR"
    exit 0
fi

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

stop_bound_tree() {
    local role="$1" pid="$2" starttime_ticks="$3"
    [[ -n "$pid" ]] || return 0
    if [[ -z "$starttime_ticks" ]]; then
        note "refusing to signal unbound $role PID $pid"
        record_cleanup_reap "$role" unbound-signal-refused "pid=$pid"
        return 1
    fi
    cleanup_python3 "$FROZEN_GATE" terminate-process-tree \
        --root-pid "$pid" --root-starttime-ticks "$starttime_ticks" \
        >"$active_run_dir/interrupted-$role-termination.json" 2>&1 || true
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

record_cleanup_reap() {
    local role="$1" status="$2" detail="$3"
    [[ -n "$active_run_dir" && -d "$active_run_dir" ]] || return 0
    printf '%s\t%s\t%s\n' "$role" "$status" "$detail" \
        >>"$active_run_dir/interrupted-cleanup-reap.tsv"
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
            note "could not verify $role PID $pid while bounding cleanup reap"
            record_cleanup_reap "$role" identity-read-failed "pid=$pid"
            return 1
        }
        read -r state current_starttime_ticks <<<"$identity"
        if [[ "$current_starttime_ticks" != "$expected_starttime_ticks" ]]; then
            note "refusing to wait for reused $role PID $pid"
            record_cleanup_reap "$role" reused-refused \
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
    note "$role PID $pid remained live after the bounded cleanup reap window"
    record_cleanup_reap "$role" timeout-live \
        "pid=$pid starttime=$expected_starttime_ticks"
    return 1
}

stop_children() {
    local controlled_cleanup_complete=0
    trap '' HUP INT TERM
    if [[ -n "$active_guardian_control" && -f "$active_guardian_control" \
        && ! -L "$active_guardian_control" ]]; then
        if cleanup_python3 "$FROZEN_GATE" cleanup-guardian-processes \
            --control "$active_guardian_control" \
            --ready "$active_guardian_ready" --launch "$active_guardian_launch" \
            --interval-ms 100 \
            >"$active_run_dir/interrupted-guardian-cleanup.json" 2>&1; then
            controlled_cleanup_complete=1
        fi
    fi
    if [[ "$controlled_cleanup_complete" == 0 ]]; then
        # Before atomic control publication the timed command is still held.
        # A rejected control also falls back to the already-bound identities.
        # Stop the measured tree first, then the two monitor jobs.
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
cleanup_signal_exit() {
    exit 130
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
cleanup_on_exit() {
    local exit_status="$1"
    trap - EXIT
    trap '' HUP INT TERM
    set +e
    if [[ "$active_lifecycle" == 1 ]]; then
        stop_children || true
    fi
    exit "$exit_status"
}

arm_cleanup_signals
trap 'cleanup_on_exit "$?"' EXIT

mapfile -t capture_files < <(python3 -c '
import json,os,sys
value=json.load(open(sys.argv[1],encoding="utf-8"))
for item in value["capture_files"]: print(os.path.join(value["capture"],item["name"]))
' "$RESULT_DIR/metadata/input-controls/capture-inputs-before.json")
(( ${#capture_files[@]} > 0 )) || die "capture inventory contains no files"

snapshot_capture_residency() {
    local output="$1" file
    : >"$output"
    for file in "${capture_files[@]}"; do
        fincore --bytes --noheadings --output RES,SIZE,FILE -- "$file" >>"$output"
    done
}

prepare_capture_cache() {
    local run_dir="$1" file resident_bytes
    for file in "${capture_files[@]}"; do "$FADVISE_BINARY" "$file"; done
    snapshot_capture_residency "$run_dir/capture-residency-before.tsv"
    resident_bytes="$(awk '{sum += $1} END {printf "%.0f", sum}' \
        "$run_dir/capture-residency-before.tsv")"
    [[ "$resident_bytes" == "0" ]] \
        || die "capture retained $resident_bytes bytes after eviction"
}

run_observation() {
    local stage="$1" position="$2" token="$3"
    local label
    label="$stage-$(printf '%02d' "$position")-$token"
    local run_dir="$RESULT_DIR/runs/$label"
    local config="$RESULT_DIR/configs/$label.toml"
    local binary role conf
    if [[ "$token" == "S" ]]; then
        binary="$RUN_SYSTEM"; role=system; conf=''
    elif [[ "$stage" == "stats" ]]; then
        binary="$RUN_STATS"; role=stats-candidate; conf="$selected_conf"
    else
        binary="$RUN_NO_STATS"; role=no-stats-candidate; conf="$selected_conf"
    fi
    assert_fixed_inputs "$label-before"
    python3 "$FROZEN_GATE" scan-conflicts \
        --output "$run_dir/processes-before.json" >/dev/null
    python3 "$SCREEN_GATE" sync-and-wait-writeback-quiescent \
        --corpus "$RESULT_DIR/configs" \
        --samples "$run_dir/pre-run-writeback-quiescence-samples.tsv" \
        --summary "$run_dir/pre-run-writeback-quiescence.json" \
        --maximum-kib 65536 --consecutive 3 --interval-ms 250 --timeout-secs 120 \
        >"$run_dir/pre-run-writeback-quiescence.log" 2>&1
    prepare_capture_cache "$run_dir"
    local ordinal="$position"
    [[ "$stage" == "no-stats" ]] && ordinal=$((4 + position))
    local -a capacity_args=(--filesystem "$RESULT_DIR" --stage "$stage" \
        --position "$position" --expectations "$EXPECTATIONS" --plan "$FROZEN_PLAN")
    if (( ordinal > 1 )); then
        capacity_args+=(--first-corpus-summary \
            "$RESULT_DIR/runs/stats-01-S/corpus-summary.json")
    fi
    python3 "$FROZEN_GATE" check-run-capacity "${capacity_args[@]}" \
        --output "$run_dir/run-capacity.json"
    local minimum_guardian_free_bytes
    minimum_guardian_free_bytes="$(python3 -c \
        'import json,sys; print(json.load(open(sys.argv[1]))["guardian_minimum_free_bytes"])' \
        "$run_dir/run-capacity.json")"
    [[ "$minimum_guardian_free_bytes" =~ ^[0-9]+$ ]] \
        || die "$label has an invalid guardian capacity floor"
    python3 "$FROZEN_GATE" scan-conflicts \
        --output "$run_dir/processes-immediately-before-launch.json" >/dev/null

    local checkpoint="$run_dir/allocator-release-checkpoint.tsv"
    local telemetry="$run_dir/allocator-release-telemetry.ndjson"
    local -a command=(env -i LC_ALL=C TZ=UTC \
        "CONFIG_FILE=$config" "RUST_LOG=chronoxide_ingester=info,chronoxide_core=warn" \
        "CHRONOXIDE_DIAGNOSTIC_POST_INGESTER_DROP_HOLD_SECS=30" \
        "CHRONOXIDE_DIAGNOSTIC_POST_INGESTER_DROP_CHECKPOINT=$checkpoint" \
        "CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_TELEMETRY=$telemetry")
    [[ -n "$conf" ]] && command+=("_RJEM_MALLOC_CONF=$conf")
    command+=("$binary")
    command=(perf stat --no-big-num --field-separator $'\t' --event "$perf_events" \
        --output "$run_dir/perf-stat.tsv" -- "${command[@]}")
    printf '%q ' "${command[@]}" >"$run_dir/command.txt"
    printf '\n' >>"$run_dir/command.txt"

    local guardian_control="$run_dir/external-conflict-guardian-control.json"
    local guardian_ready="$run_dir/external-conflict-guardian-ready"
    local guardian_launch="$run_dir/external-conflict-guardian-launch"
    local rss_ready="$run_dir/rss-monitor-ready"
    local handshake_path
    for handshake_path in "$guardian_control" "$guardian_ready" "$guardian_launch" \
        "$rss_ready"; do
        [[ ! -e "$handshake_path" && ! -L "$handshake_path" ]] \
            || die "$label refuses to reuse guardian handshake artifact"
    done
    active_run_dir="$run_dir"
    active_guardian_control="$guardian_control"
    active_guardian_ready="$guardian_ready"
    active_guardian_launch="$guardian_launch"
    active_lifecycle=1

    note "running $label"
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
    local launcher_pid=$!
    active_root_pid="$launcher_pid"
    local root_binding_failed=0
    active_root_starttime_ticks="$(read_live_starttime_ticks "$launcher_pid")" \
        || root_binding_failed=1
    arm_cleanup_signals
    (( root_binding_failed == 0 )) \
        || { stop_children; die "$label held root exited before identity binding"; }
    defer_cleanup_signals
    python3_background "$SCREEN_GATE" monitor-rss-release --pid "$launcher_pid" \
        --checkpoint "$checkpoint" --output "$run_dir/rss-samples.tsv" \
        --summary "$run_dir/rss-summary.json" --interval-ms 100 \
        --control "$guardian_control" --rss-ready "$rss_ready" \
        --launch "$guardian_launch" \
        >"$run_dir/rss-monitor.log" 2>&1 &
    local rss_pid=$!
    active_rss_pid="$rss_pid"
    local rss_binding_failed=0
    active_rss_starttime_ticks="$(read_live_starttime_ticks "$rss_pid")" \
        || rss_binding_failed=1
    arm_cleanup_signals
    (( rss_binding_failed == 0 )) \
        || { stop_children; die "$label RSS monitor exited before identity binding"; }
    defer_cleanup_signals
    python3_background "$FROZEN_GATE" monitor-conflicts --pid "$launcher_pid" \
        --interval-ms 100 --filesystem "$RESULT_DIR" \
        --minimum-free-bytes "$minimum_guardian_free_bytes" \
        --control "$guardian_control" --ready "$guardian_ready" \
        --launch "$guardian_launch" \
        --output "$run_dir/external-conflict-guardian.json" \
        >"$run_dir/external-conflict-guardian.log" 2>&1 &
    local guardian_pid=$!
    active_guardian_pid="$guardian_pid"
    local guardian_binding_failed=0
    active_guardian_starttime_ticks="$(read_live_starttime_ticks "$guardian_pid")" \
        || guardian_binding_failed=1
    arm_cleanup_signals
    (( guardian_binding_failed == 0 )) \
        || { stop_children; die "$label guardian exited before identity binding"; }
    python3 "$FROZEN_GATE" create-guardian-control \
        --root-pid "$launcher_pid" --guardian-pid "$guardian_pid" \
        --rss-monitor-pid "$rss_pid" --rss-ready "$rss_ready" --interval-ms 100 \
        --ready "$guardian_ready" --launch "$guardian_launch" \
        --output "$guardian_control" >/dev/null \
        || { stop_children; die "$label could not bind guardian launch control"; }
    python3 "$FROZEN_GATE" wait-guardian-ready \
        --control "$guardian_control" --ready "$guardian_ready" \
        --launch "$guardian_launch" --interval-ms 100 --timeout-ms 5000 \
        >/dev/null \
        || { stop_children; die "$label guardian readiness failed"; }
    python3 "$FROZEN_GATE" release-guardian-launch \
        --control "$guardian_control" --ready "$guardian_ready" \
        --launch "$guardian_launch" --interval-ms 100 >/dev/null \
        || { stop_children; die "$label guardian launch release failed"; }
    set +e
    wait "$launcher_pid"; local replay_status=$?
    wait "$rss_pid"; local rss_status=$?
    wait "$guardian_pid"; local guardian_status=$?
    clear_active_processes
    set -e
    printf '%s\n' "$replay_status" >"$run_dir/replay.exit-status"
    printf '%s\n' "$rss_status" >"$run_dir/rss-monitor.exit-status"
    printf '%s\n' "$guardian_status" >"$run_dir/external-conflict-guardian.exit-status"
    (( replay_status == 0 )) || die "$label replay failed with status $replay_status"
    (( rss_status == 0 )) || die "$label RSS monitor failed"
    (( guardian_status == 0 )) || die "$label quiet/capacity guardian failed"
    assert_fixed_inputs "$label-after"

    python3 "$PHASE1_GATE" parse-time --input "$run_dir/replay.time.txt" \
        --output "$run_dir/replay.time.json" >/dev/null
    python3 "$PHASE1_GATE" parse-perf-stat --input "$run_dir/perf-stat.tsv" \
        --output "$run_dir/perf-stat.json" "${perf_required_args[@]}" >/dev/null
    mapfile -d '' -t reports < <(find "$run_dir" -maxdepth 1 -type f \
        -name 'ingestion_stats_*.md' -print0)
    (( ${#reports[@]} == 1 )) \
        || die "$label must produce exactly one ingestion report"
    python3 "$REPORT_GATE" replay-report --report "${reports[0]}" \
        --output "$run_dir/replay-correctness.json"
    python3 "$PHASE1_GATE" gate-correctness \
        --actual "$run_dir/replay-correctness.json" --expectations "$EXPECTATIONS"
    python3 "$PHASE1_GATE" tree-manifest --corpus "$run_dir/segments" \
        --manifest "$run_dir/segments.sha256" --inventory "$run_dir/segments.tsv" \
        --summary "$run_dir/corpus-summary.json" >/dev/null
    snapshot_capture_residency "$run_dir/capture-residency-after.tsv"
    python3 "$SCREEN_GATE" sync-and-wait-writeback-quiescent \
        --corpus "$run_dir/segments" \
        --samples "$run_dir/post-run-writeback-quiescence-samples.tsv" \
        --summary "$run_dir/post-run-writeback-quiescence.json" \
        --maximum-kib 65536 --consecutive 3 --interval-ms 250 --timeout-secs 120 \
        >"$run_dir/post-run-writeback-quiescence.log" 2>&1
    python3 "$FROZEN_GATE" scan-conflicts \
        --output "$run_dir/processes-after.json" >/dev/null
    python3 "$FROZEN_GATE" make-observation --stage "$stage" --position "$position" \
        --binary "$binary" --screen-binding "$SCREEN_BINDING" \
        --no-stats-build "$NO_STATS_BUILD" \
        --preflight "$RESULT_DIR/metadata/preflight/$role.json" \
        --runtime-log "$run_dir/replay.log" --checkpoint "$checkpoint" \
        --telemetry "$telemetry" --rss "$run_dir/rss-summary.json" \
        --rss-samples "$run_dir/rss-samples.tsv" \
        --time-raw "$run_dir/replay.time.txt" --time "$run_dir/replay.time.json" \
        --perf-raw "$run_dir/perf-stat.tsv" --perf "$run_dir/perf-stat.json" \
        --guardian "$run_dir/external-conflict-guardian.json" \
        --capacity "$run_dir/run-capacity.json" \
        --pre-quiescence-samples "$run_dir/pre-run-writeback-quiescence-samples.tsv" \
        --pre-quiescence "$run_dir/pre-run-writeback-quiescence.json" \
        --post-quiescence-samples "$run_dir/post-run-writeback-quiescence-samples.tsv" \
        --post-quiescence "$run_dir/post-run-writeback-quiescence.json" \
        --replay-report "${reports[0]}" \
        --correctness "$run_dir/replay-correctness.json" \
        --corpus "$run_dir/corpus-summary.json" \
        --segments-manifest "$run_dir/segments.sha256" \
        --segments-inventory "$run_dir/segments.tsv" \
        --capture-residency-before "$run_dir/capture-residency-before.tsv" \
        --capture-residency-after "$run_dir/capture-residency-after.tsv" \
        --capture-inputs "$RESULT_DIR/metadata/input-controls/capture-inputs-before.json" \
        --expectations "$EXPECTATIONS" --plan "$FROZEN_PLAN" \
        --output "$run_dir/observation.json"
    python3 "$FROZEN_GATE" seal-authority --root "$run_dir" \
        --output "$RESULT_DIR/metadata/raw-authorities/$label.tsv" >/dev/null
}

for stage in stats no-stats; do
    tokens=(S C C S)
    [[ "$stage" == "no-stats" ]] && tokens=(S N N S)
    for position in 1 2 3 4; do
        run_observation "$stage" "$position" "${tokens[$((position - 1))]}"
    done
done

for stage in stats no-stats; do
    observation_args=()
    tokens=(S C C S)
    [[ "$stage" == "no-stats" ]] && tokens=(S N N S)
    for position in 1 2 3 4; do
        label="$stage-$(printf '%02d' "$position")-${tokens[$((position - 1))]}"
        observation_args+=(--observation "$RESULT_DIR/runs/$label/observation.json")
    done
    python3 "$FROZEN_GATE" compare-stage "${observation_args[@]}" \
        --stage "$stage" --plan "$FROZEN_PLAN" \
        --output "$RESULT_DIR/comparisons/$stage-stage-decision.json"
done

run_validation() {
    local role="$1" run_label_value="$2" binary="$3"
    local validation_dir="$RESULT_DIR/validation/$role"
    local run_dir="$RESULT_DIR/runs/$run_label_value"
    mkdir "$validation_dir"
    python3 "$FROZEN_GATE" scan-conflicts \
        --output "$validation_dir/processes-before-storage.json" >/dev/null
    assert_fixed_inputs "$role-before-storage-validation"
    /usr/bin/time -v -o "$validation_dir/storage-verify.time.txt" \
        env -i LC_ALL=C TZ=UTC "$RUN_STORAGE_VERIFY" \
            --segments-dir "$run_dir/segments" --schema schema8 \
            --validate-segment-footers --verify-exact-postings \
            >"$validation_dir/storage-verify.json" \
            2>"$validation_dir/storage-verify.log"
    python3 "$FROZEN_GATE" check-storage-completeness \
        --storage "$validation_dir/storage-verify.json" \
        --correctness "$run_dir/replay-correctness.json" \
        --expectations "$EXPECTATIONS" >/dev/null
    python3 "$FROZEN_GATE" scan-conflicts \
        --output "$validation_dir/processes-before-readbacks.json" >/dev/null
    assert_fixed_inputs "$role-before-readbacks"
    /usr/bin/time -v -o "$validation_dir/readbacks.time.txt" \
        env -i LC_ALL=C TZ=UTC "$RUN_QUERY" --segments-dir "$run_dir/segments" \
            --storage-layout schema8 --sample-limit-per-kind 2 --verify-readbacks \
            --output "$validation_dir/readbacks.md" \
            >"$validation_dir/readbacks.log" 2>&1
    python3 "$FROZEN_GATE" scan-conflicts \
        --output "$validation_dir/processes-after.json" >/dev/null
    python3 "$FROZEN_GATE" validate-canonical --role "$role" \
        --storage "$validation_dir/storage-verify.json" \
        --readbacks "$validation_dir/readbacks.md" \
        --correctness "$run_dir/replay-correctness.json" \
        --corpus "$run_dir/corpus-summary.json" \
        --segments-manifest "$run_dir/segments.sha256" \
        --expectations "$EXPECTATIONS" --binary "$binary" \
        --screen-binding "$SCREEN_BINDING" --no-stats-build "$NO_STATS_BUILD" \
        --output "$validation_dir/validation.json"
    python3 "$FROZEN_GATE" seal-authority --root "$validation_dir" \
        --output "$RESULT_DIR/metadata/raw-authorities/validation-$role.tsv" >/dev/null
}
run_validation stats-candidate stats-02-C "$RUN_STATS"
run_validation no-stats-candidate no-stats-02-N "$RUN_NO_STATS"

python3 "$PHASE1_GATE" validate-inputs --capture "$CAPTURE" \
    --template "$CONFIG_TEMPLATE" --expectations "$EXPECTATIONS" \
    --output "$RESULT_DIR/metadata/final-controls/capture-inputs-after.json"
cmp -s "$RESULT_DIR/metadata/input-controls/capture-inputs-before.json" \
    "$RESULT_DIR/metadata/final-controls/capture-inputs-after.json" \
    || die "capture/config authority changed during the full gate"
printf '%s\n' "$(date --iso-8601=ns)" \
    >"$RESULT_DIR/metadata/final-controls/finished-at.txt"
python3 "$FROZEN_GATE" seal-authority --root "$RESULT_DIR/metadata/final-controls" \
    --output "$RESULT_DIR/metadata/raw-authorities/final-controls.tsv" >/dev/null

FINAL_DECISION="$RESULT_DIR/comparisons/final-full-gate-decision.json"
python3 "$SCREEN_GATE" validate-final-artifacts \
    --result-root "$SCREEN_RESULT_DIR" --stage complete >/dev/null
python3 "$FROZEN_GATE" admit-result --result-root "$RESULT_DIR" \
    --screen-binding "$SCREEN_BINDING" --no-stats-build "$NO_STATS_BUILD" \
    --system-binary "$RUN_SYSTEM" --stats-binary "$RUN_STATS" \
    --no-stats-binary "$RUN_NO_STATS" --expectations "$EXPECTATIONS" \
    --plan "$FROZEN_PLAN" --output "$FINAL_DECISION"
chmod 0444 -- "$RESULT_DIR/PARTIAL_UNLESS_COMPLETE.txt" "$RESULT_DIR/run-plan.tsv" \
    "$RESULT_DIR/comparisons/"*.json

ARTIFACT_MANIFEST="$RESULT_DIR/metadata/result-artifacts.tsv"
python3 "$FROZEN_GATE" seal-artifacts --result-root "$RESULT_DIR" \
    --output "$ARTIFACT_MANIFEST" >/dev/null
python3 "$FROZEN_GATE" finalize --result-root "$RESULT_DIR" \
    --screen-binding "$SCREEN_BINDING" --no-stats-build "$NO_STATS_BUILD" \
    --system-binary "$RUN_SYSTEM" --stats-binary "$RUN_STATS" \
    --no-stats-binary "$RUN_NO_STATS" --expectations "$EXPECTATIONS" \
    --plan "$FROZEN_PLAN" --final-decision "$FINAL_DECISION" \
    --artifact-manifest "$ARTIFACT_MANIFEST" --complete "$RESULT_DIR/COMPLETE" \
    >/dev/null
python3 "$FROZEN_GATE" validate-final-artifacts --result-root "$RESULT_DIR" \
    --stage complete >/dev/null
note "complete, non-promotional full-gate evidence: $RESULT_DIR"
