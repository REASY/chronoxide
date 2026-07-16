#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
INVENTORY_TOOL="$SCRIPT_DIR/storage_inventory.py"
GATE_TOOL="$SCRIPT_DIR/ab_gate.py"

RUN_MODE="${RUN_MODE:-prefix}"
DRY_RUN="${DRY_RUN:-0}"
PREFIX_MESSAGES="${PREFIX_MESSAGES:-2000000}"
ALLOW_FULL_REPLAY="${ALLOW_FULL_REPLAY:-0}"
EVICT_CAPTURE="${EVICT_CAPTURE:-1}"
RUN_QUERY_VALIDATION="${RUN_QUERY_VALIDATION:-auto}"
SAMPLE_LIMIT_PER_KIND="${SAMPLE_LIMIT_PER_KIND:-2}"
SEMANTIC_QUERY="${SEMANTIC_QUERY:-sum by (service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257_count[15m]))}"
SEMANTIC_END_MS="${SEMANTIC_END_MS:-}"
RUST_LOG_VALUE="${RUST_LOG_VALUE:-chronoxide_ingester=info,chronoxide_core=info}"
READBACK_SKIP_WAIVER_KIND="${READBACK_SKIP_WAIVER_KIND:-}"
READBACK_SKIP_WAIVER_COUNT="${READBACK_SKIP_WAIVER_COUNT:-}"
READBACK_SKIP_WAIVER_REASON="${READBACK_SKIP_WAIVER_REASON:-}"
V7_UNTRACKED_TASK_SOURCES="${V7_UNTRACKED_TASK_SOURCES:-}"
VNEXT_UNTRACKED_TASK_SOURCES="${VNEXT_UNTRACKED_TASK_SOURCES:-}"
HOST_NOISE_NOTE="${HOST_NOISE_NOTE:-unspecified}"

usage() {
    cat <<'EOF'
Usage:
  CAPTURE=/absolute/capture-dir \
  V7_INGESTER_BIN=/absolute/chronoxide-ingester \
  VNEXT_INGESTER_BIN=/absolute/chronoxide-ingester \
  RESULT_DIR=/new/absolute/result-dir \
    docs/experiments/storage_vnext/storage_format_ab_run.sh [--dry-run|--prefix|--full]

Optional query validation:
  V7_QUERY_BIN=/absolute/chronoxide-query
  VNEXT_QUERY_BIN=/absolute/chronoxide-query

Required source provenance:
  V7_REPO_ROOT=/absolute/v7-worktree
  VNEXT_REPO_ROOT=/absolute/vnext-worktree

Every untracked task-source file must be named explicitly, as a colon-separated
repo-relative path list, in V7_UNTRACKED_TASK_SOURCES or
VNEXT_UNTRACKED_TASK_SOURCES. Runtime ingestion_stats_*.md and Python bytecode
are excluded and are never copied.

Skipped readbacks are a coverage gap and fail unless all three values name and
quantify the narrow waiver:
  READBACK_SKIP_WAIVER_KIND=isolation_check
  READBACK_SKIP_WAIVER_COUNT=16
  READBACK_SKIP_WAIVER_REASON='prefix corpus cannot isolate these readbacks'

The prefix gate is the default and replays 2,000,000 source messages. It runs
v7-a, vnext-a, vnext-b, v7-b, never deletes output, and requires RESULT_DIR not
to exist. Full mode runs the same four-way schedule, requires query validation,
and additionally requires ALLOW_FULL_REPLAY=1.
EOF
}

die() {
    echo "storage format A/B: $*" >&2
    exit 2
}

note() {
    echo "storage format A/B: $*"
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
    [[ "$value" == "0" || "$value" == "1" ]] \
        || die "$name must be 0 or 1; got $value"
}

require_absolute_file() {
    local name="$1"
    local path="$2"
    [[ "$path" == /* ]] || die "$name must be an absolute path"
    [[ -f "$path" && -x "$path" ]] || die "$name is not an executable file: $path"
}

require_absolute_repo() {
    local name="$1"
    local path="$2"
    [[ -z "$path" ]] && return
    [[ "$path" == /* && -d "$path/.git" || "$path" == /* && -f "$path/.git" ]] \
        || die "$name is not an absolute Git worktree path: $path"
}

toml_quote() {
    python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

for argument in "$@"; do
    case "$argument" in
        --dry-run)
            DRY_RUN=1
            ;;
        --prefix)
            RUN_MODE=prefix
            ;;
        --full)
            RUN_MODE=full
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

require_env CAPTURE
require_env V7_INGESTER_BIN
require_env VNEXT_INGESTER_BIN
require_env RESULT_DIR

for command in awk cmp cp date df diff find git grep realpath sha256sum sort stat python3 /usr/bin/time; do
    require_command "$command"
done
[[ -f "$INVENTORY_TOOL" ]] || die "storage inventory helper is missing: $INVENTORY_TOOL"
[[ -f "$GATE_TOOL" ]] || die "A/B correctness gate helper is missing: $GATE_TOOL"

require_bool DRY_RUN "$DRY_RUN"
require_bool ALLOW_FULL_REPLAY "$ALLOW_FULL_REPLAY"
require_bool EVICT_CAPTURE "$EVICT_CAPTURE"
case "$RUN_QUERY_VALIDATION" in
    auto|0|1) ;;
    *) die "RUN_QUERY_VALIDATION must be auto, 0, or 1" ;;
esac
case "$RUN_MODE" in
    prefix|full) ;;
    *) die "RUN_MODE must be prefix or full; got $RUN_MODE" ;;
esac
[[ "$PREFIX_MESSAGES" =~ ^[1-9][0-9]*$ ]] \
    || die "PREFIX_MESSAGES must be a positive integer"
[[ "$SAMPLE_LIMIT_PER_KIND" =~ ^[1-9][0-9]*$ ]] \
    || die "SAMPLE_LIMIT_PER_KIND must be a positive integer"
if [[ -n "$SEMANTIC_END_MS" && ! "$SEMANTIC_END_MS" =~ ^[0-9]+$ ]]; then
    die "SEMANTIC_END_MS must be empty or a non-negative integer"
fi
if [[ -n "$READBACK_SKIP_WAIVER_KIND" || -n "$READBACK_SKIP_WAIVER_COUNT" \
        || -n "$READBACK_SKIP_WAIVER_REASON" ]]; then
    [[ "$READBACK_SKIP_WAIVER_KIND" == "isolation_check" ]] \
        || die "READBACK_SKIP_WAIVER_KIND must be isolation_check"
    [[ "$READBACK_SKIP_WAIVER_COUNT" =~ ^[1-9][0-9]*$ ]] \
        || die "READBACK_SKIP_WAIVER_COUNT must be a positive integer"
    [[ -n "$READBACK_SKIP_WAIVER_REASON" ]] \
        || die "READBACK_SKIP_WAIVER_REASON must be non-empty"
fi
[[ "$READBACK_SKIP_WAIVER_REASON" != *$'\t'* \
    && "$READBACK_SKIP_WAIVER_REASON" != *$'\n'* ]] \
    || die "READBACK_SKIP_WAIVER_REASON must not contain tabs or newlines"
[[ "$HOST_NOISE_NOTE" != *$'\n'* ]] \
    || die "HOST_NOISE_NOTE must not contain newlines"
if [[ "$RUN_MODE" == "full" && "$DRY_RUN" != "1" && "$ALLOW_FULL_REPLAY" != "1" ]]; then
    die "full mode writes four full corpora; set ALLOW_FULL_REPLAY=1 explicitly"
fi

[[ "$CAPTURE" == /* && -d "$CAPTURE" ]] || die "CAPTURE must be an absolute directory"
CAPTURE="$(realpath -e -- "$CAPTURE")"
[[ -f "$CAPTURE/manifest.json" ]] || die "capture manifest is missing: $CAPTURE/manifest.json"
python3 - "$CAPTURE" <<'PY'
import json
import pathlib
import sys

capture = pathlib.Path(sys.argv[1])
with (capture / "manifest.json").open(encoding="utf-8") as source:
    manifest = json.load(source)
if not isinstance(manifest.get("version"), int):
    raise SystemExit("capture manifest version is missing or invalid")
partitions = manifest.get("partitions")
if not isinstance(partitions, list) or not partitions:
    raise SystemExit("capture manifest has no partitions")
for partition in partitions:
    name = partition.get("file_name")
    if not isinstance(name, str) or pathlib.Path(name).name != name:
        raise SystemExit(f"unsafe capture partition filename: {name!r}")
    path = capture / name
    if not path.is_file():
        raise SystemExit(f"capture partition file is missing: {path}")
PY
mapfile -d '' CAPTURE_FILES < <(find "$CAPTURE" -maxdepth 1 -type f -name '*.capture' -print0 | sort -z)
(( ${#CAPTURE_FILES[@]} > 0 )) || die "capture has no top-level .capture files: $CAPTURE"

require_absolute_file V7_INGESTER_BIN "$V7_INGESTER_BIN"
require_absolute_file VNEXT_INGESTER_BIN "$VNEXT_INGESTER_BIN"
V7_INGESTER_BIN="$(realpath -e -- "$V7_INGESTER_BIN")"
VNEXT_INGESTER_BIN="$(realpath -e -- "$VNEXT_INGESTER_BIN")"
V7_QUERY_BIN="${V7_QUERY_BIN:-}"
VNEXT_QUERY_BIN="${VNEXT_QUERY_BIN:-}"
if [[ -n "$V7_QUERY_BIN" || -n "$VNEXT_QUERY_BIN" ]]; then
    [[ -n "$V7_QUERY_BIN" && -n "$VNEXT_QUERY_BIN" ]] \
        || die "set both V7_QUERY_BIN and VNEXT_QUERY_BIN, or neither"
    require_absolute_file V7_QUERY_BIN "$V7_QUERY_BIN"
    require_absolute_file VNEXT_QUERY_BIN "$VNEXT_QUERY_BIN"
    V7_QUERY_BIN="$(realpath -e -- "$V7_QUERY_BIN")"
    VNEXT_QUERY_BIN="$(realpath -e -- "$VNEXT_QUERY_BIN")"
fi
if [[ "$RUN_QUERY_VALIDATION" == "1" && -z "$V7_QUERY_BIN" ]]; then
    die "RUN_QUERY_VALIDATION=1 requires V7_QUERY_BIN and VNEXT_QUERY_BIN"
fi

V7_REPO_ROOT="${V7_REPO_ROOT:-}"
VNEXT_REPO_ROOT="${VNEXT_REPO_ROOT:-$REPO_ROOT}"
[[ -n "$V7_REPO_ROOT" ]] || die "V7_REPO_ROOT is required for source provenance"
[[ -n "$VNEXT_REPO_ROOT" ]] || die "VNEXT_REPO_ROOT is required for source provenance"
require_absolute_repo V7_REPO_ROOT "$V7_REPO_ROOT"
require_absolute_repo VNEXT_REPO_ROOT "$VNEXT_REPO_ROOT"
V7_REPO_ROOT="$(realpath -e -- "$V7_REPO_ROOT")"
VNEXT_REPO_ROOT="$(realpath -e -- "$VNEXT_REPO_ROOT")"

if [[ "$RUN_QUERY_VALIDATION" == "auto" ]]; then
    if [[ -n "$V7_QUERY_BIN" ]]; then
        RUN_QUERY_VALIDATION=1
    else
        RUN_QUERY_VALIDATION=0
    fi
fi
if [[ "$RUN_MODE" == "full" && "$RUN_QUERY_VALIDATION" != "1" ]]; then
    die "full mode requires both query binaries and RUN_QUERY_VALIDATION=1"
fi

[[ "$RESULT_DIR" == /* ]] || die "RESULT_DIR must be absolute"
result_name="$(basename "$RESULT_DIR")"
[[ -n "$result_name" && "$result_name" != "." && "$result_name" != ".." ]] \
    || die "RESULT_DIR must name a new child of an existing directory"
result_parent_input="$(dirname "$RESULT_DIR")"
[[ -d "$result_parent_input" ]] || die "RESULT_DIR parent does not exist"
result_parent="$(realpath -e -- "$result_parent_input")"
RESULT_DIR="$result_parent/$result_name"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists; outputs are never reused: $RESULT_DIR"
case "$RESULT_DIR/" in
    "$CAPTURE/"*) die "RESULT_DIR must not be inside CAPTURE" ;;
esac

umask 022
mkdir "$RESULT_DIR"
mkdir "$RESULT_DIR/configs" "$RESULT_DIR/metadata" "$RESULT_DIR/runs" \
    "$RESULT_DIR/comparisons"

CONFIG_DIR="$RESULT_DIR/configs"
METADATA_DIR="$RESULT_DIR/metadata"
RUNS_DIR="$RESULT_DIR/runs"
COMPARISONS_DIR="$RESULT_DIR/comparisons"
BINARY_DIR="$METADATA_DIR/binaries"
HARNESS_DIR="$METADATA_DIR/harness"
SOURCE_DIR="$METADATA_DIR/source"
mkdir "$BINARY_DIR" "$HARNESS_DIR" "$SOURCE_DIR"

V7_INGESTER_SOURCE_BIN="$V7_INGESTER_BIN"
VNEXT_INGESTER_SOURCE_BIN="$VNEXT_INGESTER_BIN"
V7_QUERY_SOURCE_BIN="$V7_QUERY_BIN"
VNEXT_QUERY_SOURCE_BIN="$VNEXT_QUERY_BIN"

printf 'role\tsource_path\tpreserved_path\tsha256\n' >"$METADATA_DIR/binary-sources.tsv"

preserve_binary() {
    local role="$1"
    local source="$2"
    local destination="$BINARY_DIR/$role"
    local source_hash
    local destination_hash

    [[ ! -e "$destination" ]] || die "refusing to reuse preserved binary: $destination"
    cp --reflink=auto --preserve=mode,timestamps -- "$source" "$destination"
    [[ -f "$destination" && -x "$destination" ]] \
        || die "preserved binary is not executable: $destination"
    source_hash="$(sha256sum -- "$source")"
    source_hash="${source_hash%% *}"
    destination_hash="$(sha256sum -- "$destination")"
    destination_hash="${destination_hash%% *}"
    [[ "$source_hash" == "$destination_hash" ]] \
        || die "preserved binary differs from source: $role"
    printf '%s\t%s\t%s\t%s\n' \
        "$role" "$source" "$destination" "$destination_hash" \
        >>"$METADATA_DIR/binary-sources.tsv"
}

preserve_binary v7-chronoxide-ingester "$V7_INGESTER_SOURCE_BIN"
preserve_binary vnext-chronoxide-ingester "$VNEXT_INGESTER_SOURCE_BIN"
V7_INGESTER_BIN="$BINARY_DIR/v7-chronoxide-ingester"
VNEXT_INGESTER_BIN="$BINARY_DIR/vnext-chronoxide-ingester"
if [[ -n "$V7_QUERY_SOURCE_BIN" ]]; then
    preserve_binary v7-chronoxide-query "$V7_QUERY_SOURCE_BIN"
    preserve_binary vnext-chronoxide-query "$VNEXT_QUERY_SOURCE_BIN"
    V7_QUERY_BIN="$BINARY_DIR/v7-chronoxide-query"
    VNEXT_QUERY_BIN="$BINARY_DIR/vnext-chronoxide-query"
fi

for harness_file in \
    storage_format_ab_run.sh storage_inventory.py ab_gate.py \
    test_storage_inventory.py test_ab_gate.py README.md; do
    [[ -f "$SCRIPT_DIR/$harness_file" ]] \
        || die "harness provenance file is missing: $SCRIPT_DIR/$harness_file"
    cp --preserve=mode,timestamps -- \
        "$SCRIPT_DIR/$harness_file" "$HARNESS_DIR/$harness_file"
done

REPLAY_SUMMARY="$RESULT_DIR/replay-summary.tsv"
REPLAY_CORRECTNESS_SUMMARY="$RESULT_DIR/replay-correctness-summary.tsv"
READBACK_SUMMARY="$RESULT_DIR/readback-summary.tsv"
SEMANTIC_SUMMARY="$RESULT_DIR/semantic-summary.tsv"
STORAGE_INVENTORY="$RESULT_DIR/storage-artifact-inventory.tsv"
SYMBOLS_LAYOUT_INVENTORY="$RESULT_DIR/symbols-layout-inventory.tsv"

printf 'label\timplementation\tartifact\tfile_count\tbytes\n' >"$STORAGE_INVENTORY"
printf 'label\timplementation\tsymbols_version\tcomponent\tfile_count\tsymbol_count\tpage_count\tbytes\n' \
    >"$SYMBOLS_LAYOUT_INVENTORY"
printf 'label\timplementation\tstable_fingerprint\ttotal_messages\tobserved_datapoints\taccepted_datapoints\trecorded_samples\tdropped_too_old\tdropped_too_future\tmissing_timestamp\tsource_min_ts\tsource_max_ts\taccepted_skew_min_ms\taccepted_skew_max_ms\n' \
    >"$REPLAY_CORRECTNESS_SUMMARY"

declare -a RUN_ORDER=(v7-a vnext-a vnext-b v7-b)
declare -A RUN_IMPLEMENTATION=(
    [v7-a]=v7
    [v7-b]=v7
    [vnext-a]=vnext
    [vnext-b]=vnext
)
declare -A INGESTER_BIN=(
    [v7]="$V7_INGESTER_BIN"
    [vnext]="$VNEXT_INGESTER_BIN"
)
declare -A QUERY_BIN=(
    [v7]="$V7_QUERY_BIN"
    [vnext]="$VNEXT_QUERY_BIN"
)

capture_toml="$(toml_quote "$CAPTURE")"

write_config() {
    local label="$1"
    local segments_dir="$RUNS_DIR/$label/segments"
    local config="$CONFIG_DIR/$label.toml"
    local segments_toml
    segments_toml="$(toml_quote "$segments_dir")"

    {
        cat <<EOF
[kafka]
topic = "otlp_metrics"

[ingestion]
max_event_age_secs = 3600
max_event_lead_secs = 5
drop_outdated = true
labelset_store = "flat_interned"
replay_from = $capture_toml
capture_only = false
labelset_report_interval_secs = 10
EOF
        if [[ "$RUN_MODE" == "prefix" ]]; then
            printf 'stop_after_messages = %s\n' "$PREFIX_MESSAGES"
        fi
        cat <<EOF

[ingestion.head_buffer]
enabled = true
window_duration_secs = 3600
out_of_order_time_window_secs = 3600
float_encoding = "gorilla"
int_encoding = "delta_zig_zag"
varlen_encoding = "raw"

[ingestion.segment_writer]
enabled = true
segments_dir = $segments_toml
segment_duration_secs = 900
deterministic_id_seed = 42
float_encoding = "gorilla"
int_encoding = "delta_zig_zag"
varlen_encoding = "raw"
EOF
    } >"$config"
}

for label in "${RUN_ORDER[@]}"; do
    mkdir "$RUNS_DIR/$label"
    write_config "$label"
done

python3 - "$CONFIG_DIR" "$CAPTURE" "$RUN_MODE" "$PREFIX_MESSAGES" <<'PY'
import pathlib
import sys
import tomllib

config_dir = pathlib.Path(sys.argv[1])
capture = sys.argv[2]
mode = sys.argv[3]
prefix_messages = int(sys.argv[4])
for path in sorted(config_dir.glob("*.toml")):
    with path.open("rb") as source:
        document = tomllib.load(source)
    ingestion = document["ingestion"]
    writer = ingestion["segment_writer"]
    assert ingestion["replay_from"] == capture
    assert ingestion["labelset_store"] == "flat_interned"
    assert writer["segment_duration_secs"] == 900
    assert writer["deterministic_id_seed"] == 42
    assert not pathlib.Path(writer["segments_dir"]).exists()
    if mode == "prefix":
        assert ingestion["stop_after_messages"] == prefix_messages
    else:
        assert "stop_after_messages" not in ingestion
PY

{
    printf 'label\timplementation\tingester_bin\tquery_bin\tconfig\tsegments_dir\n'
    for label in "${RUN_ORDER[@]}"; do
        implementation="${RUN_IMPLEMENTATION[$label]}"
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$label" "$implementation" "${INGESTER_BIN[$implementation]}" \
            "${QUERY_BIN[$implementation]}" "$CONFIG_DIR/$label.toml" \
            "$RUNS_DIR/$label/segments"
    done
} >"$RESULT_DIR/run-plan.tsv"

record_repo_state() {
    local name="$1"
    local root="$2"
    local declared_sources="$3"
    local provenance_dir="$SOURCE_DIR/$name"
    local untracked_dir="$provenance_dir/untracked-task-sources"
    local source
    local destination
    local hash
    local relative
    local -a declared_paths=()
    local -a untracked_paths=()
    local -A declared=()
    local -A observed=()

    mkdir "$provenance_dir" "$untracked_dir"
    [[ "$(git -C "$root" rev-parse --show-toplevel)" == "$root" ]] \
        || die "$name repo root must be the Git worktree root: $root"
    git -C "$root" rev-parse HEAD >"$provenance_dir/git-commit.txt"
    git -C "$root" status --porcelain=v2 --branch >"$provenance_dir/git-status.txt"
    git -C "$root" remote -v >"$provenance_dir/git-remotes.txt"
    git -C "$root" ls-files -s >"$provenance_dir/tracked-index.txt"
    git -C "$root" diff --binary --full-index HEAD -- \
        >"$provenance_dir/tracked-combined.patch"
    git -C "$root" diff --cached --binary --full-index -- \
        >"$provenance_dir/tracked-index.patch"
    git -C "$root" diff --binary --full-index -- \
        >"$provenance_dir/tracked-worktree.patch"
    git -C "$root" diff --name-status HEAD -- \
        >"$provenance_dir/tracked-modifications.txt"
    git -C "$root" submodule status --recursive \
        >"$provenance_dir/submodules.txt" 2>&1 || true

    if [[ -n "$declared_sources" ]]; then
        IFS=: read -r -a declared_paths <<<"$declared_sources"
    fi
    for relative in "${declared_paths[@]}"; do
        [[ -n "$relative" && "$relative" != /* \
            && "$relative" != ".." && "$relative" != ../* \
            && "$relative" != */../* && "$relative" != */.. ]] \
            || die "$name has an unsafe declared untracked path: $relative"
        [[ "$relative" != *$'\t'* && "$relative" != *$'\n'* ]] \
            || die "$name declared untracked paths must not contain tabs or newlines"
        [[ -z "${declared[$relative]+present}" ]] \
            || die "$name declares an untracked task source twice: $relative"
        case "$relative" in
            chronoxide-ingester/ingestion_stats_*.md|*/__pycache__/*|*.pyc)
                die "$name runtime artifact must not be declared as task source: $relative"
                ;;
        esac
        declared["$relative"]=1
    done

    mapfile -d '' -t untracked_paths \
        < <(git -C "$root" ls-files --others --exclude-standard -z)
    printf 'path\treason\n' >"$provenance_dir/untracked-runtime-excluded.tsv"
    for relative in "${untracked_paths[@]}"; do
        case "$relative" in
            chronoxide-ingester/ingestion_stats_*.md)
                printf '%s\tgenerated ingestion statistics\n' "$relative" \
                    >>"$provenance_dir/untracked-runtime-excluded.tsv"
                continue
                ;;
            */__pycache__/*|*.pyc)
                printf '%s\tgenerated Python bytecode\n' "$relative" \
                    >>"$provenance_dir/untracked-runtime-excluded.tsv"
                continue
                ;;
        esac
        [[ -n "${declared[$relative]+present}" ]] \
            || die "$name untracked file is not explicitly classified as task source: $relative"
        observed["$relative"]=1
    done

    printf 'sha256\tsize_bytes\tpath\n' \
        >"$provenance_dir/untracked-task-sources.tsv"
    for relative in "${declared_paths[@]}"; do
        [[ -n "${observed[$relative]+present}" ]] \
            || die "$name declared task source is not an untracked file: $relative"
        source="$root/$relative"
        [[ -f "$source" && ! -L "$source" ]] \
            || die "$name task source must be a regular non-symlink file: $relative"
        [[ "$(realpath -e -- "$source")" == "$root/"* ]] \
            || die "$name task source resolves outside its worktree: $relative"
        destination="$untracked_dir/$relative"
        mkdir -p -- "$(dirname "$destination")"
        [[ ! -e "$destination" ]] \
            || die "refusing to reuse task-source snapshot: $destination"
        cp --preserve=mode,timestamps -- "$source" "$destination"
        cmp -s -- "$source" "$destination" \
            || die "$name task-source snapshot differs from source: $relative"
        hash="$(sha256sum -- "$destination")"
        hash="${hash%% *}"
        printf '%s\t%s\t%s\n' "$hash" "$(stat -c '%s' -- "$destination")" \
            "$relative" >>"$provenance_dir/untracked-task-sources.tsv"
    done

    (
        cd "$provenance_dir"
        while IFS= read -r -d '' source; do
            sha256sum -- "${source#./}"
        done < <(find . -type f -print0 | sort -z)
    ) >"$METADATA_DIR/$name-source-provenance.sha256"
}

record_repo_state v7 "$V7_REPO_ROOT" "$V7_UNTRACKED_TASK_SOURCES"
record_repo_state vnext "$VNEXT_REPO_ROOT" "$VNEXT_UNTRACKED_TASK_SOURCES"

{
    sha256sum "$V7_INGESTER_BIN" "$VNEXT_INGESTER_BIN"
    if [[ -n "$V7_QUERY_BIN" ]]; then
        sha256sum "$V7_QUERY_BIN" "$VNEXT_QUERY_BIN"
    fi
} >"$METADATA_DIR/binaries.sha256"
sha256sum "$CONFIG_DIR"/*.toml >"$METADATA_DIR/configs.sha256"
(
    cd "$HARNESS_DIR"
    while IFS= read -r -d '' source; do
        sha256sum -- "${source#./}"
    done < <(find . -type f -print0 | sort -z)
) >"$METADATA_DIR/harness.sha256"
cp "$CAPTURE/manifest.json" "$METADATA_DIR/capture-manifest.json"
sha256sum "$CAPTURE/manifest.json" >"$METADATA_DIR/capture-manifest.sha256"
printf '%s\n' "$HOST_NOISE_NOTE" >"$METADATA_DIR/run-note.txt"

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'run_mode=%s\n' "$RUN_MODE"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'prefix_messages=%s\n' "$PREFIX_MESSAGES"
    printf 'capture=%s\n' "$CAPTURE"
    printf 'result_dir=%s\n' "$RESULT_DIR"
    printf 'semantic_query=%s\n' "$SEMANTIC_QUERY"
    printf 'semantic_end_ms=%s\n' "$SEMANTIC_END_MS"
    printf 'rust_log=%s\n' "$RUST_LOG_VALUE"
    printf 'evict_capture=%s\n' "$EVICT_CAPTURE"
    printf 'run_query_validation=%s\n' "$RUN_QUERY_VALIDATION"
    printf 'readback_skip_waiver_kind=%s\n' "$READBACK_SKIP_WAIVER_KIND"
    printf 'readback_skip_waiver_count=%s\n' "$READBACK_SKIP_WAIVER_COUNT"
    printf 'readback_skip_waiver_reason=%s\n' "$READBACK_SKIP_WAIVER_REASON"
    printf 'host_noise_note=%s\n' "$HOST_NOISE_NOTE"
    printf 'v7_repo_root=%s\n' "$V7_REPO_ROOT"
    printf 'vnext_repo_root=%s\n' "$VNEXT_REPO_ROOT"
    printf 'v7_ingester_source_bin=%s\n' "$V7_INGESTER_SOURCE_BIN"
    printf 'vnext_ingester_source_bin=%s\n' "$VNEXT_INGESTER_SOURCE_BIN"
    printf 'v7_query_source_bin=%s\n' "$V7_QUERY_SOURCE_BIN"
    printf 'vnext_query_source_bin=%s\n' "$VNEXT_QUERY_SOURCE_BIN"
    printf 'normal_head_note=with the current application wiring, segment_writer.enabled makes the normal head duration equal segment_duration_secs (900); head_buffer.window_duration_secs is not effective\n'
    uname -a || true
    command -v rustc >/dev/null 2>&1 && rustc --version --verbose || true
    command -v cargo >/dev/null 2>&1 && cargo --version --verbose || true
    command -v lscpu >/dev/null 2>&1 && lscpu || true
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$CAPTURE" || true
    stat -f -c 'capture_filesystem_type=%T capture_mount=%m' "$CAPTURE" || true
    df -B1 "$RESULT_DIR" || true
    ulimit -a || true
    [[ -r /proc/meminfo ]] && cat /proc/meminfo || true
} >"$METADATA_DIR/environment.txt" 2>&1

{
    printf 'path\tsize_bytes\tinode\tmtime\n'
    for file in "$CAPTURE/manifest.json" "${CAPTURE_FILES[@]}"; do
        stat -c '%n\t%s\t%i\t%y' "$file"
    done
} >"$METADATA_DIR/capture-files.tsv"

if [[ "$DRY_RUN" == "1" ]]; then
    note "dry run complete; replay was not launched"
    touch "$RESULT_DIR/DRY_RUN_COMPLETE"
    exit 0
fi

note "hashing capture files outside measured replay"
(
    cd "$CAPTURE"
    while IFS= read -r -d '' file; do
        sha256sum -- "${file#./}"
    done < <(find . -maxdepth 1 -type f -print0 | sort -z)
) >"$METADATA_DIR/capture-files.sha256"

FADVISE_BIN=""
if [[ "$EVICT_CAPTURE" == "1" ]]; then
    require_command cc
    require_command fincore
    FADVISE_BIN="$METADATA_DIR/fadvise-dontneed"
    cc -O2 -Wall -Wextra -o "$FADVISE_BIN" \
        "$SCRIPT_DIR/../iouring/fadvise_dontneed.c"
    sha256sum "$FADVISE_BIN" >"$METADATA_DIR/fadvise-dontneed.sha256"
fi

snapshot_capture_residency() {
    local output="$1"
    : >"$output"
    for file in "${CAPTURE_FILES[@]}"; do
        fincore --bytes --noheadings --output PAGES,RES,SIZE,FILE "$file" >>"$output"
    done
}

prepare_capture_cache() {
    local run_dir="$1"
    [[ "$EVICT_CAPTURE" == "1" ]] || return 0
    for file in "${CAPTURE_FILES[@]}"; do
        "$FADVISE_BIN" "$file"
    done
    snapshot_capture_residency "$run_dir/capture-residency-before.txt"
}

time_value() {
    local file="$1"
    local key="$2"
    awk -F= -v key="$key" '$1 == key { print $2; exit }' "$file"
}

printf 'label\timplementation\telapsed_seconds\tuser_seconds\tsystem_seconds\tmax_rss_kib\tfs_inputs\tfs_outputs\tfiles\tbytes\tsegments\n' \
    >"$REPLAY_SUMMARY"

run_replay() {
    local label="$1"
    local implementation="${RUN_IMPLEMENTATION[$label]}"
    local binary="${INGESTER_BIN[$implementation]}"
    local run_dir="$RUNS_DIR/$label"
    local segments_dir="$run_dir/segments"
    local config="$CONFIG_DIR/$label.toml"
    local log="$run_dir/replay.log"
    local time_file="$run_dir/replay.time.txt"
    local status
    local time_format

    [[ ! -e "$segments_dir" ]] || die "refusing to reuse segment output: $segments_dir"
    prepare_capture_cache "$run_dir"
    note "running $label"
    time_format=$'elapsed_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M\nfs_inputs=%I\nfs_outputs=%O\nexit_status=%x'
    set +e
    (
        cd "$run_dir"
        LC_ALL=C /usr/bin/time -f "$time_format" -o "$time_file" \
            env \
                -u OTEL_EXPORTER_OTLP_ENDPOINT \
                -u OTEL_EXPORTER_OTLP_LOGS_ENDPOINT \
                -u OTEL_EXPORTER_OTLP_METRICS_ENDPOINT \
                CONFIG_FILE="$config" \
                RUST_LOG="$RUST_LOG_VALUE" \
                "$binary" >"$log" 2>&1
    )
    status=$?
    set -e
    printf '%s\n' "$status" >"$run_dir/replay.exit-status"
    if (( status != 0 )); then
        tail -n 50 "$log" >&2 || true
        die "$label replay failed with status $status; partial output was preserved"
    fi
    [[ -d "$segments_dir" ]] || die "$label completed without creating $segments_dir"
    if [[ "$EVICT_CAPTURE" == "1" ]]; then
        snapshot_capture_residency "$run_dir/capture-residency-after.txt"
    fi
}

write_tree_manifest() {
    local corpus="$1"
    local output="$2"
    local file
    local relative
    local hash
    local size

    if find "$corpus" -type l -print -quit | awk 'NF { found=1 } END { exit !found }'; then
        die "corpus contains a symbolic link: $corpus"
    fi
    printf 'sha256\tsize_bytes\tpath\n' >"$output"
    (
        cd "$corpus"
        while IFS= read -r -d '' file; do
            relative="${file#./}"
            hash="$(sha256sum -- "$relative")"
            hash="${hash%% *}"
            size="$(stat -c '%s' -- "$relative")"
            printf '%s\t%s\t%s\n' "$hash" "$size" "$relative"
        done < <(find . -type f ! -path './.tmp/*' -print0 | sort -z)
    ) >>"$output"
    sha256sum "$output" >"$output.sha256"
}

write_segment_ids() {
    local corpus="$1"
    local output="$2"
    find "$corpus" -mindepth 1 -maxdepth 1 -type d -name 'seg-*' -printf '%f\n' \
        | sort >"$output"
}

write_storage_inventory() {
    local label="$1"
    local implementation="${RUN_IMPLEMENTATION[$label]}"
    local run_dir="$RUNS_DIR/$label"
    local corpus="$run_dir/segments"
    local artifacts_output="$run_dir/storage-artifacts.tsv"
    local symbols_output="$run_dir/symbols-layout.tsv"

    [[ ! -e "$artifacts_output" && ! -e "$symbols_output" ]] \
        || die "refusing to reuse storage inventory output for $label"
    if ! python3 "$INVENTORY_TOOL" \
            --corpus "$corpus" \
            --artifacts-output "$artifacts_output" \
            --symbols-output "$symbols_output"; then
        die "$label storage inventory failed; replay output was preserved"
    fi
    awk -F'\t' -v OFS='\t' -v label="$label" -v implementation="$implementation" \
        'NR > 1 { print label, implementation, $1, $2, $3 }' \
        "$artifacts_output" >>"$STORAGE_INVENTORY"
    awk -F'\t' -v OFS='\t' -v label="$label" -v implementation="$implementation" \
        'NR > 1 { print label, implementation, $1, $2, $3, $4, $5, $6 }' \
        "$symbols_output" >>"$SYMBOLS_LAYOUT_INVENTORY"
}

write_replay_correctness() {
    local label="$1"
    local implementation="${RUN_IMPLEMENTATION[$label]}"
    local run_dir="$RUNS_DIR/$label"
    local parsed="$run_dir/replay-correctness.json"
    local -a reports=()

    mapfile -d '' -t reports \
        < <(find "$run_dir" -maxdepth 1 -type f -name 'ingestion_stats_*.md' -print0)
    (( ${#reports[@]} == 1 )) \
        || die "$label must produce exactly one ingestion statistics report; found ${#reports[@]}"
    [[ ! -e "$parsed" ]] || die "refusing to reuse replay correctness output: $parsed"
    if ! python3 "$GATE_TOOL" replay-report \
            --report "${reports[0]}" --output "$parsed"; then
        die "$label ingestion statistics report failed stable-counter parsing"
    fi
    if ! python3 "$GATE_TOOL" replay-summary \
            --label "$label" --implementation "$implementation" --parsed "$parsed" \
            >>"$REPLAY_CORRECTNESS_SUMMARY"; then
        die "$label replay correctness summary failed"
    fi
}

for label in "${RUN_ORDER[@]}"; do
    run_replay "$label"
    run_dir="$RUNS_DIR/$label"
    corpus="$run_dir/segments"
    write_replay_correctness "$label"
    write_tree_manifest "$corpus" "$run_dir/files.sha256.tsv"
    write_segment_ids "$corpus" "$run_dir/segment-ids.txt"
    write_storage_inventory "$label"
    file_count="$(awk 'NR > 1 { count++ } END { print count + 0 }' "$run_dir/files.sha256.tsv")"
    byte_count="$(awk -F'\t' 'NR > 1 { bytes += $2 } END { printf "%.0f", bytes }' "$run_dir/files.sha256.tsv")"
    segment_count="$(awk 'END { print NR + 0 }' "$run_dir/segment-ids.txt")"
    implementation="${RUN_IMPLEMENTATION[$label]}"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$label" "$implementation" \
        "$(time_value "$run_dir/replay.time.txt" elapsed_seconds)" \
        "$(time_value "$run_dir/replay.time.txt" user_seconds)" \
        "$(time_value "$run_dir/replay.time.txt" system_seconds)" \
        "$(time_value "$run_dir/replay.time.txt" max_rss_kib)" \
        "$(time_value "$run_dir/replay.time.txt" fs_inputs)" \
        "$(time_value "$run_dir/replay.time.txt" fs_outputs)" \
        "$file_count" "$byte_count" "$segment_count" >>"$REPLAY_SUMMARY"
done

compare_identical() {
    local description="$1"
    local left="$2"
    local right="$3"
    local diff_output="$4"
    if ! cmp -s "$left" "$right"; then
        diff -u "$left" "$right" >"$diff_output" || true
        die "$description mismatch; see $diff_output"
    fi
}

compare_identical "v7 deterministic file tree" \
    "$RUNS_DIR/v7-a/files.sha256.tsv" "$RUNS_DIR/v7-b/files.sha256.tsv" \
    "$COMPARISONS_DIR/v7-file-tree.diff"
compare_identical "vNext deterministic file tree" \
    "$RUNS_DIR/vnext-a/files.sha256.tsv" "$RUNS_DIR/vnext-b/files.sha256.tsv" \
    "$COMPARISONS_DIR/vnext-file-tree.diff"
compare_identical "v7/vNext segment IDs" \
    "$RUNS_DIR/v7-a/segment-ids.txt" "$RUNS_DIR/vnext-a/segment-ids.txt" \
    "$COMPARISONS_DIR/cross-format-segment-ids.diff"
compare_identical "v7 deterministic replay correctness" \
    "$RUNS_DIR/v7-a/replay-correctness.json" \
    "$RUNS_DIR/v7-b/replay-correctness.json" \
    "$COMPARISONS_DIR/v7-replay-correctness.diff"
compare_identical "vNext deterministic replay correctness" \
    "$RUNS_DIR/vnext-a/replay-correctness.json" \
    "$RUNS_DIR/vnext-b/replay-correctness.json" \
    "$COMPARISONS_DIR/vnext-replay-correctness.diff"
compare_identical "v7/vNext replay correctness" \
    "$RUNS_DIR/v7-a/replay-correctness.json" \
    "$RUNS_DIR/vnext-a/replay-correctness.json" \
    "$COMPARISONS_DIR/cross-format-replay-correctness.diff"

python3 "$GATE_TOOL" cross-format-files \
    --baseline "$RUNS_DIR/v7-a/files.sha256.tsv" \
    --candidate "$RUNS_DIR/vnext-a/files.sha256.tsv" \
    --output "$COMPARISONS_DIR/cross-format-allowed-diffs.tsv" \
    --allow-artifact symbols.bin --allow-artifact footer.bin \
    --require-difference symbols.bin --require-difference footer.bin \
    || die "unexpected v7-a/vNext-a artifact difference"
python3 "$GATE_TOOL" cross-format-files \
    --baseline "$RUNS_DIR/v7-b/files.sha256.tsv" \
    --candidate "$RUNS_DIR/vnext-b/files.sha256.tsv" \
    --output "$COMPARISONS_DIR/cross-format-allowed-diffs-b.tsv" \
    --allow-artifact symbols.bin --allow-artifact footer.bin \
    --require-difference symbols.bin --require-difference footer.bin \
    || die "unexpected v7-b/vNext-b artifact difference"
compare_identical "cross-format allowed-difference inventory" \
    "$COMPARISONS_DIR/cross-format-allowed-diffs.tsv" \
    "$COMPARISONS_DIR/cross-format-allowed-diffs-b.tsv" \
    "$COMPARISONS_DIR/cross-format-allowed-diffs.diff"

printf 'same-format file manifests, stable replay counters/time ranges, and cross-format segment IDs match; all cross-format byte differences are inventoried and limited to symbols.bin/footer.bin\n' \
    >"$COMPARISONS_DIR/determinism.txt"

query_help_has() {
    local binary="$1"
    local flag="$2"
    local help
    help="$("$binary" --help 2>&1 || true)"
    grep -Fq -- "$flag" <<<"$help"
}

query_supports_readback() {
    local binary="$1"
    query_help_has "$binary" '--verify-readbacks' \
        && query_help_has "$binary" '--validate-segment-footers' \
        && query_help_has "$binary" '--sample-limit-per-kind' \
        && query_help_has "$binary" '--output'
}

query_supports_semantic_benchmark() {
    local binary="$1"
    query_help_has "$binary" '--query' \
        && query_help_has "$binary" '--benchmark-repeats' \
        && query_help_has "$binary" '--raw-output' \
        && query_help_has "$binary" '--output'
}

printf 'label\timplementation\texpected_queries\texecuted_queries\tskipped_queries\tisolation_check_skips\tmismatches\tstatus\twaiver_kind\twaiver_count\twaiver_reason\n' \
    >"$READBACK_SUMMARY"
printf 'label\timplementation\tcorpus_fingerprint\tsemantic_fingerprint\tportable_fingerprint\tresult_series\tresult_samples\n' \
    >"$SEMANTIC_SUMMARY"

coverage_gaps=0

run_readback() {
    local label="$1"
    local implementation="${RUN_IMPLEMENTATION[$label]}"
    local binary="${QUERY_BIN[$implementation]}"
    local run_dir="$RUNS_DIR/$label"
    local report="$run_dir/readback.md"
    local log="$run_dir/readback.log"
    local row
    local -a gate_args

    [[ -n "$binary" ]] || return 1
    query_supports_readback "$binary" || return 1
    note "validating readbacks and footers for $label"
    if ! "$binary" \
            --segments-dir "$run_dir/segments" \
            --sample-limit-per-kind "$SAMPLE_LIMIT_PER_KIND" \
            --verify-readbacks \
            --validate-segment-footers \
            --output "$report" >"$log" 2>&1; then
        tail -n 50 "$log" >&2 || true
        die "readback/footer validation failed for $label"
    fi
    gate_args=(
        readback
        --label "$label"
        --implementation "$implementation"
        --report "$report"
    )
    if [[ -n "$READBACK_SKIP_WAIVER_KIND" ]]; then
        gate_args+=(
            --skip-waiver-kind "$READBACK_SKIP_WAIVER_KIND"
            --skip-waiver-count "$READBACK_SKIP_WAIVER_COUNT"
            --skip-waiver-reason "$READBACK_SKIP_WAIVER_REASON"
        )
    fi
    if ! row="$(python3 "$GATE_TOOL" "${gate_args[@]}")"; then
        die "readback gate failed for $label"
    fi
    printf '%s\n' "$row" >>"$READBACK_SUMMARY"
    if [[ "$row" == *$'\tcoverage_gap_waived\t'* ]]; then
        coverage_gaps=1
    fi
}

run_semantic_benchmark() {
    local label="$1"
    local implementation="${RUN_IMPLEMENTATION[$label]}"
    local binary="${QUERY_BIN[$implementation]}"
    local run_dir="$RUNS_DIR/$label"
    local markdown="$run_dir/semantic.md"
    local raw="$run_dir/semantic.json"
    local log="$run_dir/semantic.log"
    local args

    [[ -n "$binary" ]] || return 1
    query_supports_semantic_benchmark "$binary" || return 1
    args=(
        --segments-dir "$run_dir/segments"
        --query "$SEMANTIC_QUERY"
        --benchmark-repeats 1
        --output "$markdown"
        --raw-output "$raw"
    )
    if [[ -n "$SEMANTIC_END_MS" ]]; then
        args+=(--end-ms "$SEMANTIC_END_MS")
    fi
    note "running semantic fingerprint query for $label"
    if ! "$binary" "${args[@]}" >"$log" 2>&1; then
        tail -n 50 "$log" >&2 || true
        die "semantic fingerprint query failed for $label"
    fi
    if ! python3 "$GATE_TOOL" semantic \
            --label "$label" --implementation "$implementation" --raw "$raw" \
            >>"$SEMANTIC_SUMMARY"; then
        die "semantic query returned an invalid or empty result for $label"
    fi
}

query_validation_ran=0
if [[ "$RUN_QUERY_VALIDATION" != "0" ]]; then
    for label in "${RUN_ORDER[@]}"; do
        if run_readback "$label"; then
            query_validation_ran=1
        elif [[ "$RUN_QUERY_VALIDATION" == "1" ]]; then
            die "query binary for $label does not support required readback flags"
        else
            note "readback validation unavailable for $label; recorded as a coverage gap"
        fi
        if run_semantic_benchmark "$label"; then
            query_validation_ran=1
        elif [[ "$RUN_QUERY_VALIDATION" == "1" ]]; then
            die "query binary for $label does not support raw semantic benchmarks"
        else
            note "semantic benchmark unavailable for $label; recorded as a coverage gap"
        fi
    done
else
    coverage_gaps=1
    printf 'query validation did not run; readback/footer and semantic equivalence remain coverage gaps\n' \
        >"$COMPARISONS_DIR/query-validation.txt"
fi

if (( $(awk 'END { print NR - 1 }' "$SEMANTIC_SUMMARY") == ${#RUN_ORDER[@]} )); then
    python3 - "$SEMANTIC_SUMMARY" "$COMPARISONS_DIR/semantic.txt" <<'PY'
import csv
import sys

summary_path, result_path = sys.argv[1:]
with open(summary_path, newline="", encoding="utf-8") as source:
    rows = list(csv.DictReader(source, delimiter="\t"))
semantic_shapes = {
    (
        row["semantic_fingerprint"],
        row["portable_fingerprint"],
        row["result_series"],
        row["result_samples"],
    )
    for row in rows
}
if len(semantic_shapes) != 1:
    raise SystemExit("semantic fingerprints or result shape differ; inspect semantic-summary.tsv")
by_label = {row["label"]: row for row in rows}
for left, right in (("v7-a", "v7-b"), ("vnext-a", "vnext-b")):
    if by_label[left]["corpus_fingerprint"] != by_label[right]["corpus_fingerprint"]:
        raise SystemExit(f"same-format corpus fingerprints differ: {left} vs {right}")
with open(result_path, "w", encoding="utf-8") as output:
    output.write("all four semantic fingerprints and result shapes match; same-format corpus fingerprints match\n")
PY
fi

if (( $(awk 'END { print NR - 1 }' "$READBACK_SUMMARY") == ${#RUN_ORDER[@]} )); then
    python3 - "$READBACK_SUMMARY" "$COMPARISONS_DIR/readback.txt" <<'PY'
import csv
import sys

summary_path, result_path = sys.argv[1:]
with open(summary_path, newline="", encoding="utf-8") as source:
    rows = list(csv.DictReader(source, delimiter="\t"))
diagnostic_shapes = {
    (
        row["expected_queries"],
        row["executed_queries"],
        row["skipped_queries"],
        row["isolation_check_skips"],
        row["mismatches"],
        row["status"],
        row["waiver_kind"],
        row["waiver_count"],
        row["waiver_reason"],
    )
    for row in rows
}
if len(diagnostic_shapes) != 1:
    raise SystemExit("readback diagnostics differ across corpora; inspect readback-summary.tsv")
row = rows[0]
with open(result_path, "w", encoding="utf-8") as output:
    if row["status"] == "pass":
        output.write(
            "all four readback diagnostics match; executed queries are nonzero, "
            "skips and mismatches are zero\n"
        )
    else:
        output.write(
            "all four readback diagnostics match with zero mismatches, but skipped "
            f"queries remain an explicitly waived coverage gap: kind={row['waiver_kind']} "
            f"count={row['waiver_count']} reason={row['waiver_reason']}\n"
        )
PY
fi

if [[ "$RUN_QUERY_VALIDATION" == "1" && "$query_validation_ran" != "1" ]]; then
    die "query validation was required but did not run"
fi

if (( coverage_gaps != 0 )); then
    touch "$RESULT_DIR/COMPLETE_WITH_COVERAGE_GAPS"
    note "complete with explicitly recorded coverage gaps: $RESULT_DIR"
else
    touch "$RESULT_DIR/COMPLETE"
    note "complete: $RESULT_DIR"
fi
