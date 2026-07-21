#!/usr/bin/env bash

# Controlled code-version A/B for proving that QueryInstrumentation::Off does
# not materially regress the established query path.  Every timed process uses
# one immutable Schema 8 corpus.  Each block is strict A-B-B-A, where A is the
# pre-instrumentation raw-v9 reference and B is the raw-v10 candidate with
# --query-instrumentation off.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE_TOOL="$SCRIPT_DIR/query_instrumentation_off_ab_gate.py"
MANIFEST_TOOL="$SCRIPT_DIR/schema8_query_ab_gate.py"
COMMON_GATE_TOOL="$SCRIPT_DIR/schema7_query_ab_gate.py"
FADVISE_SOURCE="$SCRIPT_DIR/fadvise_regular_dontneed.c"

DRY_RUN="${DRY_RUN:-0}"
BLOCKS="${BLOCKS:-2}"
BENCHMARK_REPEATS="${BENCHMARK_REPEATS:-3}"
DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES="${DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES:-0}"
CHUNK_READ_QUEUE_DEPTH="${CHUNK_READ_QUEUE_DEPTH:-128}"
LABEL_MATERIALIZATION="${LABEL_MATERIALIZATION:-demand-driven}"
READBACK_SAMPLE_LIMIT_PER_KIND="${READBACK_SAMPLE_LIMIT_PER_KIND:-2}"
MAX_RESIDENT_BYTES_AFTER_EVICT="${MAX_RESIDENT_BYTES_AFTER_EVICT:-0}"
ALLOW_NOISY_HOST="${ALLOW_NOISY_HOST:-0}"
RUN_NOTE="${RUN_NOTE:-}"

# The broad selector has a deliberately tighter threshold because it is the
# path most exposed to accidental per-series observer overhead.
BROAD_MAX_REGRESSION_PCT="${BROAD_MAX_REGRESSION_PCT:-3}"
GENERAL_MAX_REGRESSION_PCT="${GENERAL_MAX_REGRESSION_PCT:-5}"
RSS_MAX_REGRESSION_PCT="${RSS_MAX_REGRESSION_PCT:-5}"

QUERY_MAX_SERIES_MATCHED="${QUERY_MAX_SERIES_MATCHED:-1000000}"
QUERY_MAX_PROJECTED_SERIES="${QUERY_MAX_PROJECTED_SERIES:-2000000}"
QUERY_MAX_CHUNKS_READ="${QUERY_MAX_CHUNKS_READ:-5000000}"
QUERY_MAX_BYTES_READ="${QUERY_MAX_BYTES_READ:-2147483648}"
QUERY_MAX_SAMPLES="${QUERY_MAX_SAMPLES:-50000000}"
REGEX_MAX_EXPANDED_VALUES="${REGEX_MAX_EXPANDED_VALUES:-100000}"

usage() {
    cat <<'EOF'
Usage:
  CORPUS_DIR=/absolute/schema8/segments \
  REFERENCE_QUERY_BIN=/absolute/pre-instrumentation/chronoxide-query \
  CANDIDATE_QUERY_BIN=/absolute/current/chronoxide-query \
  REFERENCE_SOURCE_ROOT=/absolute/clean/reference/source \
  CANDIDATE_SOURCE_ROOT=/absolute/current/source \
  QUERY_MANIFEST=/absolute/fixed-query-matrix.json \
  BROAD_QUERY_NAME=broad-full-label-selector \
  RESULT_DIR=/absolute/new-result \
  RUN_NOTE='controlled quiet-host observer-cost A/B' \
    docs/experiments/storage_vnext/query_instrumentation_off_ab_run.sh [--dry-run]

The reference must emit chronoxide.query-benchmark.raw/v9 and must not expose
--query-instrumentation.  The candidate must emit raw/v10 and is always invoked
with --query-instrumentation off.  Every query gets BLOCKS strict A-B-B-A
processes (default 2).  Each fresh process records one cold and at least one
warm query evaluation (BENCHMARK_REPEATS defaults to 3).

Defaults gate the named broad query at <=3% candidate median cold and warm
latency regression; every other query at <=5%; and per-query process median RSS
at <=5%.  All semantic/portable fingerprints, result shapes, public QueryStats,
payload/read counters, label counters, and range-cache counters must match.

The two source roots are snapshotted independently.  A tracked patch is part of
the recorded source-state digest; untracked Rust/Cargo build inputs are rejected.
An archive source root without .git is also accepted when the corresponding
REFERENCE_SOURCE_COMMIT/REFERENCE_SOURCE_TREE (or CANDIDATE_*) 40-hex object
IDs are supplied; its complete file inventory becomes the source-state digest.
Footer validation and independent readbacks run outside timed processes.
EOF
}

die() {
    echo "Query instrumentation Off A/B: $*" >&2
    exit 2
}

note() {
    echo "Query instrumentation Off A/B: $*"
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

require_single_line() {
    local name="$1"
    local value="$2"
    [[ -n "$value" && "$value" != *$'\n'* && "$value" != *$'\t'* ]] \
        || die "$name is required and must contain no tabs or newlines"
}

for argument in "$@"; do
    case "$argument" in
        --dry-run)
            DRY_RUN=1
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

for command in awk bash cc cmp cp date df find fincore git grep ps python3 \
    realpath sha256sum sort stat /usr/bin/time; do
    require_command "$command"
done
for harness_file in "$GATE_TOOL" "$MANIFEST_TOOL" "$COMMON_GATE_TOOL" "$FADVISE_SOURCE"; do
    [[ -f "$harness_file" ]] || die "required harness file is missing: $harness_file"
done
for required in CORPUS_DIR REFERENCE_QUERY_BIN CANDIDATE_QUERY_BIN \
    REFERENCE_SOURCE_ROOT CANDIDATE_SOURCE_ROOT QUERY_MANIFEST \
    BROAD_QUERY_NAME RESULT_DIR; do
    require_env "$required"
done
require_bool DRY_RUN "$DRY_RUN"
require_bool ALLOW_NOISY_HOST "$ALLOW_NOISY_HOST"
require_single_line RUN_NOTE "$RUN_NOTE"
require_single_line BROAD_QUERY_NAME "$BROAD_QUERY_NAME"
if [[ "$ALLOW_NOISY_HOST" == "1" && "$RUN_NOTE" != *[Nn][Oo][Ii][Ss][Yy]* ]]; then
    die "ALLOW_NOISY_HOST=1 requires RUN_NOTE to explicitly contain the word noisy"
fi

[[ "$BLOCKS" =~ ^[1-9][0-9]*$ ]] || die "BLOCKS must be positive"
[[ "$BENCHMARK_REPEATS" =~ ^[1-9][0-9]*$ ]] \
    || die "BENCHMARK_REPEATS must be positive"
(( BENCHMARK_REPEATS >= 2 )) \
    || die "BENCHMARK_REPEATS must be at least 2 to record cold and warm runs"
[[ "$DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES" =~ ^[0-9]+$ ]] \
    || die "DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES must be non-negative"
[[ "$CHUNK_READ_QUEUE_DEPTH" =~ ^[1-9][0-9]*$ ]] \
    || die "CHUNK_READ_QUEUE_DEPTH must be positive"
[[ "$LABEL_MATERIALIZATION" == "full" || "$LABEL_MATERIALIZATION" == "demand-driven" ]] \
    || die "LABEL_MATERIALIZATION must be full or demand-driven"
[[ "$READBACK_SAMPLE_LIMIT_PER_KIND" =~ ^[1-9][0-9]*$ ]] \
    || die "READBACK_SAMPLE_LIMIT_PER_KIND must be positive"
[[ "$MAX_RESIDENT_BYTES_AFTER_EVICT" =~ ^[0-9]+$ ]] \
    || die "MAX_RESIDENT_BYTES_AFTER_EVICT must be non-negative"
for threshold_name in BROAD_MAX_REGRESSION_PCT GENERAL_MAX_REGRESSION_PCT \
    RSS_MAX_REGRESSION_PCT; do
    [[ "${!threshold_name}" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || die "$threshold_name must be a finite non-negative decimal"
done
for limit_name in QUERY_MAX_SERIES_MATCHED QUERY_MAX_PROJECTED_SERIES \
    QUERY_MAX_CHUNKS_READ QUERY_MAX_BYTES_READ QUERY_MAX_SAMPLES \
    REGEX_MAX_EXPANDED_VALUES; do
    [[ "${!limit_name}" =~ ^[1-9][0-9]*$ ]] \
        || die "$limit_name must be positive"
done

[[ "$CORPUS_DIR" == /* && -d "$CORPUS_DIR" ]] \
    || die "CORPUS_DIR must be an absolute directory"
CORPUS_DIR="$(realpath -e -- "$CORPUS_DIR")"
for binary_name in REFERENCE_QUERY_BIN CANDIDATE_QUERY_BIN; do
    binary="${!binary_name}"
    [[ "$binary" == /* && -f "$binary" && -x "$binary" ]] \
        || die "$binary_name must be an absolute executable regular file"
    binary="$(realpath -e -- "$binary")"
    printf -v "$binary_name" '%s' "$binary"
done
[[ "$REFERENCE_QUERY_BIN" != "$CANDIDATE_QUERY_BIN" ]] \
    || die "reference and candidate binary paths must differ"
for root_name in REFERENCE_SOURCE_ROOT CANDIDATE_SOURCE_ROOT; do
    source_root="${!root_name}"
    [[ "$source_root" == /* && -d "$source_root" ]] \
        || die "$root_name must be an absolute source directory"
    source_root="$(realpath -e -- "$source_root")"
    if [[ -e "$source_root/.git" ]]; then
        git -C "$source_root" rev-parse --is-inside-work-tree >/dev/null \
            || die "$root_name has invalid Git metadata"
    fi
    printf -v "$root_name" '%s' "$source_root"
done
[[ "$QUERY_MANIFEST" == /* && -f "$QUERY_MANIFEST" ]] \
    || die "QUERY_MANIFEST must be an absolute regular file"
QUERY_MANIFEST="$(realpath -e -- "$QUERY_MANIFEST")"

[[ "$RESULT_DIR" == /* ]] || die "RESULT_DIR must be absolute"
result_name="$(basename "$RESULT_DIR")"
[[ -n "$result_name" && "$result_name" != "." && "$result_name" != ".." ]] \
    || die "RESULT_DIR must name a new child of an existing directory"
result_parent_input="$(dirname "$RESULT_DIR")"
[[ -d "$result_parent_input" ]] || die "RESULT_DIR parent does not exist"
result_parent="$(realpath -e -- "$result_parent_input")"
RESULT_DIR="$result_parent/$result_name"
[[ ! -e "$RESULT_DIR" ]] || die "RESULT_DIR already exists; outputs are never reused"
case "$RESULT_DIR/" in
    "$CORPUS_DIR/"*) die "RESULT_DIR must not be inside the corpus" ;;
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
NORMALIZED_TSV="$RESULT_DIR/queries.tsv"
NORMALIZED_JSON="$RESULT_DIR/queries.normalized.json"
RAW_INDEX="$RESULT_DIR/raw-index.tsv"
RESIDENCY_SUMMARY="$RESULT_DIR/residency-summary.tsv"
FADVISE_BIN="$METADATA_DIR/fadvise-regular-dontneed"
FROZEN_GATE_TOOL="$METADATA_DIR/query_instrumentation_off_ab_gate.py"
BINARY_PROVENANCE="$METADATA_DIR/binaries.tsv"
SOURCE_PROVENANCE="$METADATA_DIR/sources.tsv"

declare -A RUN_BIN
declare -A BINARY_SHA256
RUN_BIN[reference]="$METADATA_DIR/chronoxide-query-reference"
RUN_BIN[candidate]="$METADATA_DIR/chronoxide-query-candidate"

printf 'role\tsource_path\tpreserved_path\tsha256\n' >"$BINARY_PROVENANCE"
preserve_binary() {
    local role="$1"
    local source="$2"
    local destination="${RUN_BIN[$role]}"
    local hash
    cp --reflink=auto --preserve=mode,timestamps -- "$source" "$destination"
    cmp -s -- "$source" "$destination" || die "copied $role binary differs from source"
    [[ -x "$destination" ]] || die "copied $role binary is not executable"
    hash="$(sha256sum -- "$destination" | awk '{print $1}')"
    BINARY_SHA256[$role]="$hash"
    printf '%s\t%s\t%s\t%s\n' "$role" "$source" "$destination" "$hash" \
        >>"$BINARY_PROVENANCE"
}
preserve_binary reference "$REFERENCE_QUERY_BIN"
preserve_binary candidate "$CANDIDATE_QUERY_BIN"
[[ "${BINARY_SHA256[reference]}" != "${BINARY_SHA256[candidate]}" ]] \
    || die "reference and candidate binaries are byte-identical"

reference_help="$("${RUN_BIN[reference]}" --help 2>&1)"
candidate_help="$("${RUN_BIN[candidate]}" --help 2>&1)"
for required_help in '--storage-layout' 'schema8' '--label-materialization' \
    '--query-label-storage' 'owned-strings' '--range-scalar-cache-max-bytes' \
    '--raw-output'; do
    grep -Fq -- "$required_help" <<<"$reference_help" \
        || die "reference binary help is missing $required_help"
    grep -Fq -- "$required_help" <<<"$candidate_help" \
        || die "candidate binary help is missing $required_help"
done
if grep -Fq -- '--query-instrumentation' <<<"$reference_help"; then
    die "reference binary is not pre-instrumentation"
fi
for required_help in '--query-instrumentation' 'off' 'detailed'; do
    grep -Fq -- "$required_help" <<<"$candidate_help" \
        || die "candidate binary help is missing $required_help"
done

for harness_file in query_instrumentation_off_ab_run.sh \
    query_instrumentation_off_ab_gate.py schema8_query_ab_gate.py \
    schema7_query_ab_gate.py fadvise_regular_dontneed.c; do
    cp --preserve=mode,timestamps -- "$SCRIPT_DIR/$harness_file" \
        "$METADATA_DIR/$harness_file"
done
cp --preserve=mode,timestamps -- "$QUERY_MANIFEST" \
    "$METADATA_DIR/query-manifest.input.json"
(
    cd "$METADATA_DIR"
    sha256sum query_instrumentation_off_ab_run.sh \
        query_instrumentation_off_ab_gate.py schema8_query_ab_gate.py \
        schema7_query_ab_gate.py fadvise_regular_dontneed.c \
        query-manifest.input.json
) >"$METADATA_DIR/harness.sha256"

printf 'role\tsource_root\thead_commit\thead_tree\tsource_state_sha256\ttracked_patch_sha256\tstatus_sha256\n' \
    >"$SOURCE_PROVENANCE"
snapshot_source() {
    local role="$1"
    local source_root="$2"
    local output="$METADATA_DIR/source-$role"
    local untracked_path head_commit head_tree state_hash patch_hash status_hash
    local role_upper="${role^^}"
    local commit_variable="${role_upper}_SOURCE_COMMIT"
    local tree_variable="${role_upper}_SOURCE_TREE"
    mkdir "$output"
    if [[ -e "$source_root/.git" ]]; then
        git -C "$source_root" rev-parse HEAD >"$output/head-commit.txt"
        git -C "$source_root" rev-parse 'HEAD^{tree}' >"$output/head-tree.txt"
        git -C "$source_root" status --porcelain=v2 --branch >"$output/status.txt"
        git -C "$source_root" diff --binary --full-index HEAD -- \
            >"$output/tracked-source.patch"
        git -C "$source_root" ls-files --others --exclude-standard -z \
            >"$output/untracked-paths.nul"
        while IFS= read -r -d '' untracked_path; do
            case "$untracked_path" in
                *.rs|Cargo.toml|Cargo.lock|*/Cargo.toml|*/Cargo.lock)
                    die "$role source has an untracked build input: $untracked_path"
                    ;;
            esac
        done <"$output/untracked-paths.nul"
        (
            cd "$output"
            sha256sum head-commit.txt head-tree.txt status.txt tracked-source.patch \
                untracked-paths.nul
        ) >"$output/source-components.sha256"
        state_hash="$(sha256sum "$output/source-components.sha256" | awk '{print $1}')"
    else
        head_commit="${!commit_variable:-}"
        head_tree="${!tree_variable:-}"
        [[ "$head_commit" =~ ^[0-9a-f]{40}$ ]] \
            || die "$commit_variable is required for archive source roots"
        [[ "$head_tree" =~ ^[0-9a-f]{40}$ ]] \
            || die "$tree_variable is required for archive source roots"
        printf '%s\n' "$head_commit" >"$output/head-commit.txt"
        printf '%s\n' "$head_tree" >"$output/head-tree.txt"
        printf 'archive_source_root=%s\n' "$source_root" >"$output/status.txt"
        : >"$output/tracked-source.patch"
        : >"$output/untracked-paths.nul"
        python3 "$FROZEN_GATE_TOOL" inventory \
            --corpus "$source_root" \
            --output "$output/archive-source-inventory.json" \
            --paths-output "$output/archive-source-paths.nul"
        state_hash="$(sha256sum "$output/archive-source-inventory.json" | awk '{print $1}')"
        sha256sum "$output/archive-source-inventory.json" \
            "$output/archive-source-paths.nul" >"$output/source-components.sha256"
    fi
    patch_hash="$(sha256sum "$output/tracked-source.patch" | awk '{print $1}')"
    status_hash="$(sha256sum "$output/status.txt" | awk '{print $1}')"
    head_commit="$(tr -d '\n' <"$output/head-commit.txt")"
    head_tree="$(tr -d '\n' <"$output/head-tree.txt")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$role" "$source_root" "$head_commit" "$head_tree" "$state_hash" \
        "$patch_hash" "$status_hash" >>"$SOURCE_PROVENANCE"
}
snapshot_source reference "$REFERENCE_SOURCE_ROOT"
snapshot_source candidate "$CANDIDATE_SOURCE_ROOT"

python3 "$FROZEN_GATE_TOOL" normalize-manifest \
    --input "$METADATA_DIR/query-manifest.input.json" \
    --output-tsv "$NORMALIZED_TSV" \
    --output-json "$NORMALIZED_JSON" \
    --default-range-cache-bytes "$DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES"
grep -Fq -- $'"query_name": "'"$BROAD_QUERY_NAME"'"' "$NORMALIZED_JSON" \
    || die "BROAD_QUERY_NAME is absent from normalized manifest"

python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$CORPUS_DIR" \
    --output "$INVENTORY_DIR/corpus.json" \
    --paths-output "$INVENTORY_DIR/corpus-files.nul"
sha256sum "$INVENTORY_DIR/corpus.json" >"$INVENTORY_DIR/inventory.sha256"
cc -O2 -Wall -Wextra -Werror -o "$FADVISE_BIN" "$FADVISE_SOURCE"
sha256sum -- "$FADVISE_BIN" >"$METADATA_DIR/fadvise.sha256"

{
    printf 'process_label\tquery_name\tcategory\tmode\tblock\torder_index\trole\tcorpus\n'
    while IFS=$'\t' read -r query_name category mode _start _end _step \
        _cache _boundaries _expression; do
        [[ "$query_name" != "query_name" ]] || continue
        for ((block = 1; block <= BLOCKS; block++)); do
            order=(reference candidate candidate reference)
            for ((index = 0; index < 4; index++)); do
                role="${order[$index]}"
                process_label="$(printf '%s-b%02d-%02d-%s' \
                    "$query_name" "$block" "$((index + 1))" "$role")"
                printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                    "$process_label" "$query_name" "$category" "$mode" \
                    "$block" "$((index + 1))" "$role" "$CORPUS_DIR"
            done
        done
    done <"$NORMALIZED_TSV"
} >"$RESULT_DIR/run-plan.tsv"

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'corpus=%s\n' "$CORPUS_DIR"
    printf 'reference_binary=%s\n' "${RUN_BIN[reference]}"
    printf 'reference_binary_sha256=%s\n' "${BINARY_SHA256[reference]}"
    printf 'candidate_binary=%s\n' "${RUN_BIN[candidate]}"
    printf 'candidate_binary_sha256=%s\n' "${BINARY_SHA256[candidate]}"
    printf 'candidate_query_instrumentation=off\n'
    printf 'query_manifest=%s\n' "$QUERY_MANIFEST"
    printf 'broad_query_name=%s\n' "$BROAD_QUERY_NAME"
    printf 'schedule=reference,candidate,candidate,reference\n'
    printf 'blocks=%s\n' "$BLOCKS"
    printf 'benchmark_repeats=%s\n' "$BENCHMARK_REPEATS"
    printf 'chunk_read_mode=pread\n'
    printf 'chunk_read_queue_depth=%s\n' "$CHUNK_READ_QUEUE_DEPTH"
    printf 'label_materialization=%s\n' "$LABEL_MATERIALIZATION"
    printf 'query_label_storage=owned-strings\n'
    printf 'default_range_scalar_cache_max_bytes=%s\n' "$DEFAULT_RANGE_SCALAR_CACHE_MAX_BYTES"
    printf 'broad_max_regression_pct=%s\n' "$BROAD_MAX_REGRESSION_PCT"
    printf 'general_max_regression_pct=%s\n' "$GENERAL_MAX_REGRESSION_PCT"
    printf 'rss_max_regression_pct=%s\n' "$RSS_MAX_REGRESSION_PCT"
    printf 'max_resident_bytes_after_evict=%s\n' "$MAX_RESIDENT_BYTES_AFTER_EVICT"
    printf 'allow_noisy_host=%s\n' "$ALLOW_NOISY_HOST"
    printf 'run_note=%s\n' "$RUN_NOTE"
    printf 'footer_validation=separate pre-measurement pass for each binary\n'
    printf 'readback_validation=separate pre-measurement pass for each binary\n'
    printf 'timed_footer_validation=forbidden and enforced by raw-output gate\n'
    printf 'cache_note=POSIX_FADV_DONTNEED/fincore cover Linux page-cache residency, not device caches\n'
} >"$METADATA_DIR/settings.txt"
printf '%s\n' "$RUN_NOTE" >"$METADATA_DIR/run-note.txt"

{
    date --iso-8601=seconds
    uname -a || true
    command -v rustc >/dev/null 2>&1 && rustc --version --verbose || true
    command -v cargo >/dev/null 2>&1 && cargo --version --verbose || true
    command -v lscpu >/dev/null 2>&1 && lscpu || true
    command -v findmnt >/dev/null 2>&1 && findmnt -T "$CORPUS_DIR" || true
    stat -f -c 'corpus_filesystem_type=%T corpus_mount=%m' "$CORPUS_DIR" || true
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
    note "dry run complete; validation, eviction, and query processes were not launched: $RESULT_DIR"
    exit 0
fi

check_measurement_conflicts() {
    local snapshot="$1"
    local conflicts
    ps -eo pid=,ppid=,comm=,args= >"$snapshot"
    conflicts="$(
        awk -v own="$$" '
            $1 != own && ($3 == "cargo" || $3 == "rustc" || $3 == "perf" ||
                $3 ~ /^chronoxide-/ || $3 ~ /^greptime/ || $3 == "prometheus") { print }
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

evict_all_files() {
    local file
    while IFS= read -r -d '' file; do
        "$FADVISE_BIN" "$file"
    done <"$INVENTORY_DIR/corpus-files.nul"
}

snapshot_residency() {
    local process_label="$1"
    local role="$2"
    local block="$3"
    local phase="$4"
    local output="$5"
    local file line resident size
    local file_count=0
    local total_resident=0
    local total_size=0
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
    done <"$INVENTORY_DIR/corpus-files.nul"
    (( file_count > 0 )) || die "residency snapshot saw no corpus files"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$process_label" "$role" "$block" "$phase" "$file_count" \
        "$total_resident" "$total_size" >>"$RESIDENCY_SUMMARY"
    printf '%s\n' "$total_resident"
}

run_validation_passes() {
    local role="$1"
    local binary="${RUN_BIN[$role]}"
    local output="$VALIDATION_DIR/$role"
    local status
    mkdir "$output"
    note "validating all segment footers with $role binary outside timing"
    check_measurement_conflicts "$output/processes-before-footer.txt"
    set +e
    /usr/bin/time -v -o "$output/footer.time.txt" \
        "$binary" --segments-dir "$CORPUS_DIR" --storage-layout schema8 \
            --sample-limit-per-kind 0 --validate-segment-footers \
            --output "$output/footer.md" >"$output/footer.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$output/footer.exit-status"
    (( status == 0 )) || die "$role footer validation failed"

    note "running independent readbacks with $role binary outside timing"
    check_measurement_conflicts "$output/processes-before-readbacks.txt"
    set +e
    /usr/bin/time -v -o "$output/readbacks.time.txt" \
        "$binary" --segments-dir "$CORPUS_DIR" --storage-layout schema8 \
            --sample-limit-per-kind "$READBACK_SAMPLE_LIMIT_PER_KIND" \
            --verify-readbacks --output "$output/readbacks.md" \
            >"$output/readbacks.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$output/readbacks.exit-status"
    (( status == 0 )) || die "$role independent readback verification failed"
}

run_validation_passes reference
run_validation_passes candidate

printf 'process_label\trole\tblock\tphase\tfile_count\tresident_bytes\tcorpus_file_bytes\n' \
    >"$RESIDENCY_SUMMARY"
printf 'process_label\tquery_name\tcategory\tmode\tblock\torder_index\trole\tbinary_sha256\tcorpus\traw_output\tprocess_wall_seconds\tprocess_user_seconds\tprocess_system_seconds\tmax_rss_kib\n' \
    >"$RAW_INDEX"

read_time_value() {
    local key="$1"
    local path="$2"
    awk -F '\t' -v key="$key" '$1 == key { print $2 }' "$path"
}

run_process() {
    local query_name="$1"
    local category="$2"
    local mode="$3"
    local start_ms="$4"
    local end_ms="$5"
    local step_ms="$6"
    local cache_bytes="$7"
    local boundaries_csv="$8"
    local expression="$9"
    local block="${10}"
    local order_index="${11}"
    local role="${12}"
    local binary="${RUN_BIN[$role]}"
    local process_label run_dir raw markdown log time_file status resident_after_evict
    local wall_seconds user_seconds system_seconds max_rss_kib boundary argument
    local -a args boundaries

    process_label="$(printf '%s-b%02d-%02d-%s' \
        "$query_name" "$block" "$order_index" "$role")"
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
        "$process_label" "$role" "$block" after-evict \
        "$run_dir/residency-after-evict.nul")"
    if (( resident_after_evict > MAX_RESIDENT_BYTES_AFTER_EVICT )); then
        die "resident bytes after eviction are $resident_after_evict for $process_label; limit is $MAX_RESIDENT_BYTES_AFTER_EVICT"
    fi

    args=(
        --segments-dir "$CORPUS_DIR"
        --storage-layout schema8
        --label-materialization "$LABEL_MATERIALIZATION"
        --query-label-storage owned-strings
        --start-ms "$start_ms"
        --end-ms "$end_ms"
        --benchmark-repeats "$BENCHMARK_REPEATS"
        --chunk-read-mode pread
        --chunk-read-queue-depth "$CHUNK_READ_QUEUE_DEPTH"
        --query-max-series-matched "$QUERY_MAX_SERIES_MATCHED"
        --query-max-projected-series "$QUERY_MAX_PROJECTED_SERIES"
        --query-max-chunks-read "$QUERY_MAX_CHUNKS_READ"
        --query-max-bytes-read "$QUERY_MAX_BYTES_READ"
        --query-max-samples "$QUERY_MAX_SAMPLES"
        --regex-max-expanded-values "$REGEX_MAX_EXPANDED_VALUES"
        --output "$markdown"
        --raw-output "$raw"
        --query "$expression"
    )
    if [[ "$role" == "candidate" ]]; then
        args+=(--query-instrumentation off)
    fi
    if [[ "$mode" == "range" ]]; then
        [[ "$step_ms" != "-" && "$cache_bytes" != "-" ]] \
            || die "normalized range query lacks step/cache values"
        args+=(--step-ms "$step_ms" --range-scalar-cache-max-bytes "$cache_bytes")
    else
        [[ "$step_ms" == "-" && "$cache_bytes" == "-" ]] \
            || die "normalized instant query unexpectedly has step/cache values"
    fi
    if [[ "$boundaries_csv" != "-" ]]; then
        IFS=',' read -r -a boundaries <<<"$boundaries_csv"
        for boundary in "${boundaries[@]}"; do
            args+=(--exponential-histogram-bucket-boundary "$boundary")
        done
    fi
    for argument in "${args[@]}"; do
        [[ "$argument" != "--validate-segment-footers" ]] \
            || die "internal error: footer validation entered timed arguments"
    done

    note "running $process_label"
    set +e
    /usr/bin/time -o "$time_file" \
        -f $'process_wall_seconds\t%e\nprocess_user_seconds\t%U\nprocess_system_seconds\t%S\nmax_rss_kib\t%M\nexit_status\t%x' \
        "$binary" "${args[@]}" >"$log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$run_dir/exit-status"
    if (( status != 0 )); then
        tail -n 50 "$log" >&2 || true
        die "$process_label failed with status $status; partial output was preserved"
    fi
    snapshot_pressure "$run_dir/pressure-after.txt"
    check_measurement_conflicts "$run_dir/processes-after.txt"
    snapshot_residency "$process_label" "$role" "$block" after-run \
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
        "$process_label" "$query_name" "$category" "$mode" "$block" \
        "$order_index" "$role" "${BINARY_SHA256[$role]}" "$CORPUS_DIR" \
        "$raw" "$wall_seconds" "$user_seconds" "$system_seconds" "$max_rss_kib" \
        >>"$RAW_INDEX"
}

while IFS=$'\t' read -r query_name category mode start_ms end_ms step_ms \
    cache_bytes boundaries_csv expression; do
    [[ "$query_name" != "query_name" ]] || continue
    for ((block = 1; block <= BLOCKS; block++)); do
        order=(reference candidate candidate reference)
        for ((index = 0; index < 4; index++)); do
            run_process "$query_name" "$category" "$mode" "$start_ms" "$end_ms" \
                "$step_ms" "$cache_bytes" "$boundaries_csv" "$expression" \
                "$block" "$((index + 1))" "${order[$index]}"
        done
    done
done <"$NORMALIZED_TSV"

python3 "$FROZEN_GATE_TOOL" compare-results \
    --index "$RAW_INDEX" \
    --manifest "$NORMALIZED_JSON" \
    --binaries "$BINARY_PROVENANCE" \
    --sources "$SOURCE_PROVENANCE" \
    --corpus "$CORPUS_DIR" \
    --summary "$RESULT_DIR/summary.tsv" \
    --output "$COMPARISONS_DIR/comparison.json" \
    --broad-query-name "$BROAD_QUERY_NAME" \
    --blocks "$BLOCKS" \
    --benchmark-repeats "$BENCHMARK_REPEATS" \
    --queue-depth "$CHUNK_READ_QUEUE_DEPTH" \
    --label-materialization "$LABEL_MATERIALIZATION" \
    --max-matched-series "$QUERY_MAX_SERIES_MATCHED" \
    --max-projected-series "$QUERY_MAX_PROJECTED_SERIES" \
    --max-chunk-reads "$QUERY_MAX_CHUNKS_READ" \
    --max-bytes-read "$QUERY_MAX_BYTES_READ" \
    --max-samples-decoded "$QUERY_MAX_SAMPLES" \
    --max-regex-values-examined "$REGEX_MAX_EXPANDED_VALUES" \
    --broad-max-regression-pct "$BROAD_MAX_REGRESSION_PCT" \
    --general-max-regression-pct "$GENERAL_MAX_REGRESSION_PCT" \
    --rss-max-regression-pct "$RSS_MAX_REGRESSION_PCT" \
    || die "semantic/counter equivalence or observer-cost regression gate failed"

note "re-inventorying the corpus to prove it stayed immutable"
python3 "$FROZEN_GATE_TOOL" inventory \
    --corpus "$CORPUS_DIR" \
    --output "$INVENTORY_DIR/corpus-after.json" \
    --paths-output "$INVENTORY_DIR/corpus-files-after.nul"
cmp -s "$INVENTORY_DIR/corpus.json" "$INVENTORY_DIR/corpus-after.json" \
    || die "corpus bytes changed during the benchmark"
cmp -s "$INVENTORY_DIR/corpus-files.nul" "$INVENTORY_DIR/corpus-files-after.nul" \
    || die "corpus path set changed during the benchmark"
sha256sum "$INVENTORY_DIR/corpus-after.json" \
    >"$INVENTORY_DIR/inventory-after.sha256"

(
    cd "$RESULT_DIR"
    while IFS= read -r -d '' artifact; do
        sha256sum -- "${artifact#./}"
    done < <(find validation runs comparisons -type f -print0 | sort -z)
) >"$METADATA_DIR/result-artifacts.sha256"

touch "$RESULT_DIR/COMPLETE"
note "complete: $RESULT_DIR"
