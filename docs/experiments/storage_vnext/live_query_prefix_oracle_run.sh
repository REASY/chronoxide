#!/usr/bin/env bash

# Post-measurement only. This script never writes to, serves, or times D/P/Q.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ORACLE_GATE="$SCRIPT_DIR/live_query_prefix_oracle.py"
ORACLE_TEST="$SCRIPT_DIR/test_live_query_prefix_oracle.py"

RESULT_DIR="${RESULT_DIR:-}"
ORACLE_DIR="${ORACLE_DIR:-}"
API_BIN="${API_BIN:-}"
API_BIN_SHA256="${API_BIN_SHA256:-}"
API_PROVENANCE_NOTE="${API_PROVENANCE_NOTE:-}"
ALLOW_POSTHOC_API_BIN="${ALLOW_POSTHOC_API_BIN:-0}"
REPO_ROOT="${REPO_ROOT:-$DEFAULT_REPO_ROOT}"
API_LISTEN="${API_LISTEN:-127.0.0.1:19092}"
QUERY_NAME="${QUERY_NAME:-}"
QUERY_TIMEOUT_MS="${QUERY_TIMEOUT_MS:-30000}"
MAX_RESPONSE_BYTES="${MAX_RESPONSE_BYTES:-67108864}"
RUN_NOTE="${RUN_NOTE:-}"

usage() {
    cat <<'EOF'
Usage:
  RESULT_DIR=/absolute/completed/live-query-dpq-root \
  ORACLE_DIR=/absolute/fresh/external/oracle-root \
  API_BIN=/absolute/frozen/chronoxide-api \
  API_BIN_SHA256=<pre-recorded-64-hex-digest> \
  API_PROVENANCE_NOTE='built with the measured binaries before formal timing' \
  RUN_NOTE='post-measurement prefix oracle; no measured process overlap' \
    docs/experiments/storage_vnext/live_query_prefix_oracle_run.sh

The script chooses the earliest designated non-empty Q response whose complete
QueryStats reports segments_queried=0 and whose generation/cut is present in
the raw successful-publication log. It replays exactly that capture prefix with
the preserved ingester and live API disabled into a fresh sealed store, serves
the store with a preserved chronoxide-api binary, and compares the exact
Prometheus data hash/cardinality/sample count.

This is a head-versus-sealed storage-path oracle. Both HTTP paths share the
Chronoxide PromQL evaluator, so it is not an independent PromQL semantic
oracle. The canonical hash retains result-array order and fails closed.

Formal evidence requires chronoxide-api to have been frozen in
RESULT_DIR/metadata/binaries before timing. A screen whose completed result
lacks it must explicitly set ALLOW_POSTHOC_API_BIN=1 and say "post-hoc" in
API_PROVENANCE_NOTE; that run records a provenance limitation.
EOF
}

die() {
    echo "live query prefix oracle: $*" >&2
    exit 2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command missing: $1"
}

require_executable() {
    [[ "$1" == /* && -f "$1" && ! -L "$1" && -x "$1" ]] \
        || die "expected absolute executable regular file: $1"
}

[[ $# -eq 0 ]] || {
    [[ $# -eq 1 && ( "$1" == "--help" || "$1" == "-h" ) ]] && {
        usage
        exit 0
    }
    usage >&2
    die "unexpected arguments"
}

for command in awk bash cat cmp cp date diff dirname env find mkdir python3 \
    realpath sha256sum sort stat sync touch xargs /usr/bin/time; do
    require_command "$command"
done
for value in "$QUERY_TIMEOUT_MS" "$MAX_RESPONSE_BYTES"; do
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "query limits must be positive integers"
done
[[ -n "$RUN_NOTE" && "$RUN_NOTE" != *$'\n'* && "$RUN_NOTE" != *$'\t'* ]] \
    || die "RUN_NOTE is required and must be one line"
[[ "$API_BIN_SHA256" =~ ^[0-9a-f]{64}$ ]] \
    || die "API_BIN_SHA256 must explicitly pin the supplied API binary"
[[ -n "$API_PROVENANCE_NOTE" && "$API_PROVENANCE_NOTE" != *$'\n'* \
    && "$API_PROVENANCE_NOTE" != *$'\t'* ]] \
    || die "API_PROVENANCE_NOTE is required and must be one line"
[[ "$ALLOW_POSTHOC_API_BIN" == "0" || "$ALLOW_POSTHOC_API_BIN" == "1" ]] \
    || die "ALLOW_POSTHOC_API_BIN must be 0 or 1"
[[ -f "$ORACLE_GATE" && ! -L "$ORACLE_GATE" ]] || die "oracle gate is missing"
[[ -f "$ORACLE_TEST" && ! -L "$ORACLE_TEST" ]] || die "oracle tests are missing"
[[ -n "$RESULT_DIR" && "$RESULT_DIR" == /* && -d "$RESULT_DIR" ]] \
    || die "RESULT_DIR must be an absolute completed D/P/Q root"
RESULT_DIR="$(realpath -e -- "$RESULT_DIR")"
[[ -f "$RESULT_DIR/COMPLETE" ]] || die "RESULT_DIR is incomplete"
REPO_ROOT="$(realpath -e -- "$REPO_ROOT")"
[[ -n "$ORACLE_DIR" && "$ORACLE_DIR" == /* ]] \
    || die "ORACLE_DIR must be a fresh absolute path"
oracle_parent="$(dirname "$ORACLE_DIR")"
oracle_name="$(basename "$ORACLE_DIR")"
[[ -d "$oracle_parent" && "$oracle_name" != "." && "$oracle_name" != ".." ]] \
    || die "ORACLE_DIR parent must already exist"
ORACLE_DIR="$(realpath -e -- "$oracle_parent")/$oracle_name"
[[ ! -e "$ORACLE_DIR" ]] || die "ORACLE_DIR exists; roots are never reused"
case "$ORACLE_DIR/" in
    "$RESULT_DIR/"*|"$REPO_ROOT/"*) die "ORACLE_DIR must be outside result/source roots" ;;
esac
require_executable "$API_BIN"
API_BIN="$(realpath -e -- "$API_BIN")"
[[ "$(sha256sum "$API_BIN" | awk '{print $1}')" == "$API_BIN_SHA256" ]] \
    || die "API binary differs from API_BIN_SHA256"
MEASURED_API="$RESULT_DIR/metadata/binaries/chronoxide-api"
API_PROVENANCE_BOUND=0
if [[ -e "$MEASURED_API" ]]; then
    require_executable "$MEASURED_API"
    cmp -s "$MEASURED_API" "$API_BIN" \
        || die "API_BIN differs from the API binary frozen before D/P/Q timing"
    API_PROVENANCE_BOUND=1
elif [[ "$ALLOW_POSTHOC_API_BIN" != "1" \
    || "$API_PROVENANCE_NOTE" != *[Pp][Oo][Ss][Tt]-[Hh][Oo][Cc]* ]]; then
    die "result lacks a pre-timing API binary; screening requires explicit post-hoc opt-in/note"
fi

FROZEN_PHASE1="$RESULT_DIR/metadata/harness/phase1_replay_gate.py"
FROZEN_REPORT="$RESULT_DIR/metadata/harness/ab_gate.py"
FROZEN_EXPECTATIONS="$RESULT_DIR/metadata/harness/phase1_4m_expectations.json"
FROZEN_TEMPLATE="$RESULT_DIR/metadata/config-template.toml"
FROZEN_INGESTER="$RESULT_DIR/metadata/binaries/chronoxide-ingester"
for path in "$FROZEN_PHASE1" "$FROZEN_REPORT" "$FROZEN_EXPECTATIONS" \
    "$FROZEN_TEMPLATE" "$FROZEN_INGESTER" \
    "$RESULT_DIR/metadata/result-artifacts.sha256" \
    "$RESULT_DIR/metadata/binaries.sha256"; do
    [[ -f "$path" && ! -L "$path" ]] || die "missing frozen measured artifact: $path"
done
require_executable "$FROZEN_INGESTER"

# Verify the completed measured evidence before selecting anything from it.
(
    cd "$RESULT_DIR"
    sha256sum --check --strict metadata/result-artifacts.sha256
) >/dev/null
sha256sum --check --strict "$RESULT_DIR/metadata/binaries.sha256" >/dev/null

api_help="$("$API_BIN" --help 2>&1)"
for flag in --segments-dir --listen --chunk-read-mode \
    --chunk-read-queue-depth --chunk-payload-coalesce-max-gap-bytes \
    --query-max-series-matched --query-max-projected-series \
    --query-max-chunks-read --query-max-bytes-read --query-max-samples \
    --query-max-regex-values-examined --range-scalar-cache-max-bytes \
    --max-concurrent-queries --storage-schema --validate-segment-footers; do
    [[ "$api_help" == *"$flag"* ]] || die "API binary help lacks $flag"
done

umask 022
mkdir "$ORACLE_DIR"
mkdir "$ORACLE_DIR/config" "$ORACLE_DIR/logs" "$ORACLE_DIR/metadata" \
    "$ORACLE_DIR/metadata/binaries" "$ORACLE_DIR/metadata/harness" \
    "$ORACLE_DIR/metadata/measured-inputs" \
    "$ORACLE_DIR/replay" "$ORACLE_DIR/results" "$ORACLE_DIR/status" \
    "$ORACLE_DIR/validation"
{
    printf 'started_at=%s\n' "$(date --iso-8601=ns)"
    printf 'measured_result_root=%s\noracle_root=%s\napi_binary_source=%s\n' \
        "$RESULT_DIR" "$ORACLE_DIR" "$API_BIN"
    printf 'api_listen=%s\nquery_name_override=%s\nrun_note=%s\n' \
        "$API_LISTEN" "$QUERY_NAME" "$RUN_NOTE"
    printf 'api_binary_sha256=%s\napi_provenance_note=%s\n' \
        "$API_BIN_SHA256" "$API_PROVENANCE_NOTE"
    printf 'api_provenance_bound_to_measured_result=%s\n' "$API_PROVENANCE_BOUND"
    printf 'oracle_kind=head-vs-sealed-storage-path\n'
    printf 'independent_promql_evaluator=false\n'
    printf 'canonical_hash_array_order_sensitive=true\n'
    printf 'measured_timing_includes_oracle=false\n'
} >"$ORACLE_DIR/metadata/invocation.txt"

SUCCESS=0
cleanup() {
    local status=$?
    trap - EXIT INT TERM
    if [[ "$SUCCESS" != "1" && -f "$ORACLE_DIR/status/api-child.pid" ]]; then
        local child
        child="$(cat "$ORACLE_DIR/status/api-child.pid" 2>/dev/null || true)"
        if [[ "$child" =~ ^[0-9]+$ ]] && kill -0 "$child" 2>/dev/null; then
            kill -TERM "$child" 2>/dev/null || true
        fi
    fi
    if [[ "$SUCCESS" != "1" ]]; then
        touch "$ORACLE_DIR/FAILED"
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

cp --preserve=mode,timestamps -- "$ORACLE_GATE" \
    "$ORACLE_DIR/metadata/harness/live_query_prefix_oracle.py"
cp --preserve=mode,timestamps -- "$ORACLE_TEST" \
    "$ORACLE_DIR/metadata/harness/test_live_query_prefix_oracle.py"
cp --preserve=mode,timestamps -- "$FROZEN_PHASE1" \
    "$ORACLE_DIR/metadata/harness/phase1_replay_gate.py"
cp --preserve=mode,timestamps -- "$FROZEN_REPORT" \
    "$ORACLE_DIR/metadata/harness/ab_gate.py"
cp --preserve=mode,timestamps -- "$FROZEN_EXPECTATIONS" \
    "$ORACLE_DIR/metadata/measured-inputs/phase1_4m_expectations.json"
cp --preserve=mode,timestamps -- "$FROZEN_TEMPLATE" \
    "$ORACLE_DIR/metadata/measured-inputs/config-template.toml"
cp --preserve=mode,timestamps -- \
    "$RESULT_DIR/metadata/harness/live_query_ingest_queries.json" \
    "$ORACLE_DIR/metadata/measured-inputs/live_query_ingest_queries.json"
cp --preserve=mode,timestamps -- "$RESULT_DIR/configs/Q.toml" \
    "$ORACLE_DIR/metadata/measured-inputs/Q.toml"
cp --preserve=mode,timestamps -- "$RESULT_DIR/runs/Q/client-summary.json" \
    "$ORACLE_DIR/metadata/measured-inputs/Q-client-summary.json"
cp --reflink=auto --preserve=mode,timestamps -- "$FROZEN_INGESTER" \
    "$ORACLE_DIR/metadata/binaries/chronoxide-ingester"
cp --reflink=auto --preserve=mode,timestamps -- "$API_BIN" \
    "$ORACLE_DIR/metadata/binaries/chronoxide-api"
cmp -s "$FROZEN_INGESTER" "$ORACLE_DIR/metadata/binaries/chronoxide-ingester" \
    || die "preserved ingester differs"
cmp -s "$API_BIN" "$ORACLE_DIR/metadata/binaries/chronoxide-api" \
    || die "preserved API differs"
[[ "$(sha256sum "$ORACLE_DIR/metadata/binaries/chronoxide-api" | awk '{print $1}')" \
    == "$API_BIN_SHA256" ]] || die "preserved API hash differs from pinned hash"
RUN_GATE="$ORACLE_DIR/metadata/harness/live_query_prefix_oracle.py"
RUN_PHASE1="$ORACLE_DIR/metadata/harness/phase1_replay_gate.py"
RUN_REPORT="$ORACLE_DIR/metadata/harness/ab_gate.py"
RUN_EXPECTATIONS="$ORACLE_DIR/metadata/measured-inputs/phase1_4m_expectations.json"
RUN_TEMPLATE="$ORACLE_DIR/metadata/measured-inputs/config-template.toml"
RUN_INGESTER="$ORACLE_DIR/metadata/binaries/chronoxide-ingester"
RUN_API="$ORACLE_DIR/metadata/binaries/chronoxide-api"
find "$ORACLE_DIR/metadata/harness" "$ORACLE_DIR/metadata/measured-inputs" \
    "$ORACLE_DIR/metadata/binaries" -type f -print0 \
    | sort -z | xargs -0 sha256sum \
    >"$ORACLE_DIR/metadata/frozen-artifacts.sha256"
(
    cd "$ORACLE_DIR/metadata/harness"
    python3 test_live_query_prefix_oracle.py -v
) >"$ORACLE_DIR/logs/harness-tests.log" 2>&1

select_args=(
    select
    --result-root "$RESULT_DIR"
    --output "$ORACLE_DIR/metadata/selection.json"
)
[[ -z "$QUERY_NAME" ]] || select_args+=(--query-name "$QUERY_NAME")
python3 "$RUN_GATE" "${select_args[@]}" \
    >"$ORACLE_DIR/logs/selection.log" 2>&1

CAPTURE="$(python3 - "$ORACLE_DIR/metadata/selection.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["capture"])
PY
)"
[[ "$CAPTURE" == /* && -d "$CAPTURE" && ! -L "$CAPTURE" ]] \
    || die "selected capture is no longer a non-symlink directory"
CAPTURE="$(realpath -e -- "$CAPTURE")"
case "$ORACLE_DIR/" in
    "$CAPTURE/"*) die "ORACLE_DIR must be outside the capture root" ;;
esac
[[ -f "$CAPTURE/manifest.json" && ! -L "$CAPTURE/manifest.json" ]] \
    || die "selected capture manifest is missing"
cp --preserve=mode,timestamps -- "$CAPTURE/manifest.json" \
    "$ORACLE_DIR/metadata/measured-inputs/capture-manifest.json"

# Re-hash the frozen capture after measured timing and before prefix replay.
python3 "$RUN_PHASE1" validate-inputs \
    --capture "$CAPTURE" \
    --template "$RUN_TEMPLATE" \
    --expectations "$RUN_EXPECTATIONS" \
    --output "$ORACLE_DIR/validation/validated-inputs.json" \
    >"$ORACLE_DIR/logs/validate-inputs.log" 2>&1
find "$CAPTURE" -maxdepth 1 -type f \
    -printf '%i\t%s\t%T@\t%f\n' | sort \
    >"$ORACLE_DIR/metadata/capture-stat-before.tsv"

SEGMENTS_DIR="$ORACLE_DIR/replay/segments"
PREFIX_CONFIG="$ORACLE_DIR/config/prefix.toml"
python3 "$RUN_GATE" render-prefix-config \
    --result-root "$RESULT_DIR" \
    --selection "$ORACLE_DIR/metadata/selection.json" \
    --segments-dir "$SEGMENTS_DIR" \
    --output "$PREFIX_CONFIG" \
    --gate-output "$ORACLE_DIR/validation/config-gate.json" \
    --q-config "$ORACLE_DIR/metadata/measured-inputs/Q.toml" \
    >"$ORACLE_DIR/logs/render-prefix-config.log" 2>&1

set +e
(
    cd "$ORACLE_DIR/replay"
    exec /usr/bin/time -v -o "$ORACLE_DIR/logs/prefix-replay.time.txt" \
        env LC_ALL=C TZ=UTC \
        CONFIG_FILE="$PREFIX_CONFIG" \
        RUST_LOG="chronoxide_ingester=info,chronoxide_core=warn" \
        "$RUN_INGESTER"
) >"$ORACLE_DIR/logs/prefix-replay.log" 2>&1
replay_status=$?
set -e
printf '%s\n' "$replay_status" >"$ORACLE_DIR/status/prefix-replay.exit-status"
(( replay_status == 0 )) || die "exact prefix replay failed; partial root preserved"

mapfile -d '' -t reports \
    < <(find "$ORACLE_DIR/replay" -maxdepth 1 -type f \
        -name 'ingestion_stats_*.md' -print0)
(( ${#reports[@]} == 1 )) || die "prefix replay must emit exactly one stats report"
python3 "$RUN_REPORT" replay-report \
    --report "${reports[0]}" \
    --output "$ORACLE_DIR/validation/replay-correctness.json" \
    >"$ORACLE_DIR/logs/parse-replay-report.log" 2>&1
python3 "$RUN_PHASE1" tree-manifest \
    --corpus "$SEGMENTS_DIR" \
    --manifest "$ORACLE_DIR/validation/segments.sha256" \
    --inventory "$ORACLE_DIR/validation/segments.tsv" \
    --summary "$ORACLE_DIR/validation/corpus-summary.json" \
    >"$ORACLE_DIR/logs/tree-manifest.log" 2>&1
python3 "$RUN_GATE" validate-prefix-replay \
    --selection "$ORACLE_DIR/metadata/selection.json" \
    --replay "$ORACLE_DIR/validation/replay-correctness.json" \
    --corpus-summary "$ORACLE_DIR/validation/corpus-summary.json" \
    --output "$ORACLE_DIR/validation/replay-gate.json" \
    >"$ORACLE_DIR/logs/replay-gate.log" 2>&1
sync -f "$SEGMENTS_DIR"

find "$CAPTURE" -maxdepth 1 -type f \
    -printf '%i\t%s\t%T@\t%f\n' | sort \
    >"$ORACLE_DIR/metadata/capture-stat-after.tsv"
diff -u "$ORACLE_DIR/metadata/capture-stat-before.tsv" \
    "$ORACLE_DIR/metadata/capture-stat-after.tsv" \
    >"$ORACLE_DIR/metadata/capture-stat.diff" \
    || die "capture file identity/size/mtime changed during prefix replay"
python3 "$RUN_PHASE1" validate-inputs \
    --capture "$CAPTURE" \
    --template "$RUN_TEMPLATE" \
    --expectations "$RUN_EXPECTATIONS" \
    --output "$ORACLE_DIR/validation/validated-inputs-after.json" \
    >"$ORACLE_DIR/logs/validate-inputs-after.log" 2>&1
diff -u "$ORACLE_DIR/validation/validated-inputs.json" \
    "$ORACLE_DIR/validation/validated-inputs-after.json" \
    >"$ORACLE_DIR/validation/validated-inputs.diff" \
    || die "capture/config cryptographic identity changed during prefix replay"

python3 "$RUN_GATE" check-listen-free --listen "$API_LISTEN"
python3 "$RUN_GATE" emit-api-args \
    --config "$PREFIX_CONFIG" \
    --segments-dir "$SEGMENTS_DIR" \
    --listen "$API_LISTEN" \
    >"$ORACLE_DIR/metadata/api-args.nul"
mapfile -d '' -t API_ARGS <"$ORACLE_DIR/metadata/api-args.nul"
(( ${#API_ARGS[@]} > 0 )) || die "sealed API argument construction failed"

STOP_FILE="$ORACLE_DIR/status/stop-api"
api_supervisor() {
    set +e
    "$RUN_API" "${API_ARGS[@]}" >"$ORACLE_DIR/logs/sealed-api.log" 2>&1 &
    local child=$!
    printf '%s\n' "$child" >"$ORACLE_DIR/status/api-child.pid"
    while [[ ! -f "$STOP_FILE" ]]; do
        if ! kill -0 "$child" 2>/dev/null; then
            wait "$child"
            local early_status=$?
            printf '%s\n' "$child" >"$ORACLE_DIR/status/api-child.last-pid"
            : >"$ORACLE_DIR/status/api-child.pid"
            python3 - "$early_status" "$ORACLE_DIR/status/api-child-termination.json" <<'PY'
import json, sys
with open(sys.argv[2], "x", encoding="utf-8") as out:
    json.dump({"expected": False, "shell_status": int(sys.argv[1])}, out, sort_keys=True)
    out.write("\n")
PY
            return 1
        fi
        sleep 0.05
    done
    kill -TERM "$child" 2>/dev/null
    wait "$child"
    local child_status=$?
    printf '%s\n' "$child" >"$ORACLE_DIR/status/api-child.last-pid"
    : >"$ORACLE_DIR/status/api-child.pid"
    python3 - "$child_status" "$ORACLE_DIR/status/api-child-termination.json" <<'PY'
import json, sys
status = int(sys.argv[1])
with open(sys.argv[2], "x", encoding="utf-8") as out:
    json.dump(
        {
            "expected": status == 143,
            "shell_status": status,
            "signal": "SIGTERM" if status == 143 else None,
        },
        out,
        sort_keys=True,
    )
    out.write("\n")
PY
    [[ "$child_status" -eq 143 ]]
}

set +e
api_supervisor &
supervisor_pid=$!
python3 "$RUN_GATE" query-sealed \
    --base-url "http://$API_LISTEN" \
    --selection "$ORACLE_DIR/metadata/selection.json" \
    --body-output "$ORACLE_DIR/results/sealed-response.json" \
    --headers-output "$ORACLE_DIR/results/sealed-response-headers.json" \
    --comparison-output "$ORACLE_DIR/results/comparison.json" \
    --timeout-ms "$QUERY_TIMEOUT_MS" \
    --max-response-bytes "$MAX_RESPONSE_BYTES" \
    >"$ORACLE_DIR/logs/sealed-query.log" 2>&1
query_status=$?
printf '%s\n' "$query_status" >"$ORACLE_DIR/status/sealed-query.exit-status"
touch "$STOP_FILE"
wait "$supervisor_pid"
supervisor_status=$?
printf '%s\n' "$supervisor_status" >"$ORACLE_DIR/status/api-supervisor.exit-status"
set -e
(( query_status == 0 )) || die "sealed query comparison failed"
(( supervisor_status == 0 )) || die "sealed API supervisor failed"

python3 "$RUN_GATE" gate-final \
    --selection "$ORACLE_DIR/metadata/selection.json" \
    --config-gate "$ORACLE_DIR/validation/config-gate.json" \
    --replay-gate "$ORACLE_DIR/validation/replay-gate.json" \
    --comparison "$ORACLE_DIR/results/comparison.json" \
    --replay-status "$ORACLE_DIR/status/prefix-replay.exit-status" \
    --query-status "$ORACLE_DIR/status/sealed-query.exit-status" \
    --supervisor-status "$ORACLE_DIR/status/api-supervisor.exit-status" \
    --termination "$ORACLE_DIR/status/api-child-termination.json" \
    --output "$ORACLE_DIR/results/oracle-gate.json" \
    >"$ORACLE_DIR/logs/final-gate.log" 2>&1

{
    printf 'recorded_at=%s\n' "$(date --iso-8601=ns)"
    printf 'measured_result_root=%s\noracle_root=%s\ncapture=%s\n' \
        "$RESULT_DIR" "$ORACLE_DIR" "$CAPTURE"
    printf 'api_listen=%s\nrun_note=%s\n' "$API_LISTEN" "$RUN_NOTE"
    printf 'api_binary_sha256=%s\napi_provenance_note=%s\n' \
        "$API_BIN_SHA256" "$API_PROVENANCE_NOTE"
    printf 'api_provenance_bound_to_measured_result=%s\n' "$API_PROVENANCE_BOUND"
    printf 'oracle_kind=head-vs-sealed-storage-path\n'
    printf 'independent_promql_evaluator=false\n'
    printf 'canonical_hash_array_order_sensitive=true\n'
    printf 'measured_timing_includes_oracle=false\n'
} >"$ORACLE_DIR/metadata/settings.txt"
(
    cd "$ORACLE_DIR"
    find config logs metadata results status validation replay \
        -type f ! -path 'replay/segments/*' \
        ! -path 'metadata/oracle-artifacts.sha256' \
        -print0 | sort -z | xargs -0 sha256sum
) >"$ORACLE_DIR/metadata/oracle-artifacts.sha256"

touch "$ORACLE_DIR/COMPLETE"
SUCCESS=1
echo "live query prefix oracle complete: $ORACLE_DIR"
