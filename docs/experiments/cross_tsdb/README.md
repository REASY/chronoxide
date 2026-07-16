# Chronoxide, Prometheus, and GreptimeDB comparison

This harness replays the original OTLP capture into pinned Prometheus and
GreptimeDB containers, then compares PromQL results with the existing native
Chronoxide corpus. It checks a portable result fingerprint before retaining
latencies. This prevents a faster empty, differently labelled, or otherwise
different result from being reported as a performance win.

The source for this experiment is the capture, not the native Chronoxide
segments. The replay preserves source-message and `resource_metrics` order,
batches requests without changing datapoints, does not retry, and fails on an
OTLP partial rejection. Each database must start with an empty data directory.

## 1. Start empty databases

Choose a new run root on the real data filesystem:

```sh
export STACK_RESULT_DIR=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/cross-tsdb-$(date +%Y%m%d-%H%M%S)
RESULT_DIR="$STACK_RESULT_DIR" docs/experiments/cross_tsdb/stack_up.sh
```

The images are pinned to Prometheus `v3.13.1` and GreptimeDB `v1.1.2`. The run
root records the resolved Compose file and immutable image IDs. GreptimeDB's
`/greptimedb_data` is bind-mounted into the run root so storage accounting and
restart behavior cover the actual database, not a container-local directory.

## 2. Replay identical input

Do a bounded smoke replay first. Use a new stack for the full replay; do not
continue from the smoke database because that would duplicate its prefix.

```sh
RESULT_DIR="$STACK_RESULT_DIR" TARGET=prometheus MAX_SOURCE_MESSAGES=1000 \
  docs/experiments/cross_tsdb/replay_capture.sh
RESULT_DIR="$STACK_RESULT_DIR" TARGET=greptime MAX_SOURCE_MESSAGES=1000 BUILD=0 \
  docs/experiments/cross_tsdb/replay_capture.sh
```

For a fresh full stack, omit `MAX_SOURCE_MESSAGES`:

```sh
RESULT_DIR="$STACK_RESULT_DIR" TARGET=prometheus \
  docs/experiments/cross_tsdb/replay_capture.sh
RESULT_DIR="$STACK_RESULT_DIR" TARGET=greptime BUILD=0 \
  docs/experiments/cross_tsdb/replay_capture.sh
```

The current capture contains 13 million source messages and about 142 GB of
uncompressed OTLP protobuf, so a full replay is intentionally never started by
`stack_up.sh`.

`MAX_BATCH_BYTES` and `MAX_BATCH_MESSAGES` can override the default 4 MiB/512
source-message batching for receiver-compatibility diagnostics. A comparison
must record and use the same successful replay policy for both backends.

The wrapper defaults `DROP_MISSING_NUMBER_VALUES=1`. This applies Chronoxide's
documented ingest rule: OTLP Gauge/Sum datapoints without a number value are
counted and omitted, never converted to zero. Empty metric/scope/resource
envelopes left by that omission are also removed. Set the variable to `0` only
for receiver diagnostics; GreptimeDB rejects such envelopes as having no field
column.

The wrapper also defaults `MAX_EVENT_AGE_SECS=3600` and
`MAX_EVENT_LEAD_SECS=5`, matching `metric_smoke_replay.toml`. For every capture
record it evaluates each OTLP datapoint against that record's trusted
`captured_at_ms`, drops missing timestamps and points outside the inclusive
window, and leaves accepted `time_unix_nano` values unchanged. The replay
report records observed points and every drop reason.

## 3. Audit translated schema

Prometheus and GreptimeDB can translate OTLP metric/resource names differently
from Chronoxide. Inventory the names after replay:

```sh
OUTPUT_DIR="$STACK_RESULT_DIR/schema" \
  docs/experiments/cross_tsdb/discover_metrics.sh
rg 'http.*client.*duration|service.name|service_name' "$STACK_RESULT_DIR/schema"
```

Copy `queries.example.json` into a new file and correct the translated metric
and group-label names found by the audit. The example deliberately uses an
aggregate that removes metric-name labels, then maps each database's group
label to Chronoxide's canonical label before fingerprinting.

Prometheus uses `NoUTF8EscapingWithSuffixes`: the underscore-escaping strategy
can collapse distinct production OTLP identities and caused an in-message
duplicate-timestamp rejection during validation. UTF-8 preservation keeps
those source identities distinct; the discovery output supplies the quoted
PromQL names needed for punctuation-bearing labels.

Prometheus is started with OTLP delta-to-cumulative conversion. GreptimeDB
documents that delta Sum and Histogram values are stored directly rather than
converted to cumulative values. Therefore, do not call a delta-derived query
semantically equivalent merely because its spelling looks the same. The
fingerprint check is the gate; if it fails, report the semantic mismatch and
do not compare that latency.

## 4. Run the warm-query comparison

```sh
export QUERY_RESULT_DIR=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/cross-tsdb-query-$(date +%Y%m%d-%H%M%S)
RESULT_DIR="$QUERY_RESULT_DIR" \
SEGMENTS_DIR=/absolute/path/to/schema8-run/segments \
QUERIES=/absolute/path/to/verified-queries.json \
REPEATS=9 WARMUPS=1 \
  docs/experiments/cross_tsdb/compare_promql.sh
```

`SEGMENTS_DIR` is mandatory. The runner never guesses or reuses a historical
corpus because its schema, capture prefix, and query coverage must be selected
explicitly for every comparison.

Select the Chronoxide corpus schema explicitly when comparing schema 7 or
schema 8. The runner applies the same selection to the direct-core oracle and
the HTTP API, and rejects any value other than `schema7` or `schema8`. Omission
uses the production `schema8` default:

```sh
RESULT_DIR="$QUERY_RESULT_DIR" \
SEGMENTS_DIR=/absolute/path/to/schema8-run/segments \
CHRONOXIDE_STORAGE_SCHEMA=schema8 \
QUERIES=/absolute/path/to/verified-queries.json \
REPEATS=9 WARMUPS=1 \
  docs/experiments/cross_tsdb/compare_promql.sh
```

To benchmark preserved release binaries instead of the current checkout, set
all applicable binary paths and disable the build. The default paths remain
the current checkout's `target/release` binaries when these variables are
omitted:

```sh
RESULT_DIR="$QUERY_RESULT_DIR" \
SEGMENTS_DIR=/absolute/path/to/schema8-run/segments \
CHRONOXIDE_STORAGE_SCHEMA=schema8 BUILD=0 \
QUERY_BIN=/absolute/path/to/chronoxide-query \
HTTP_BIN=/absolute/path/to/chronoxide-promql-http-bench \
API_BIN=/absolute/path/to/chronoxide-api \
QUERIES=/absolute/path/to/verified-queries.json \
  docs/experiments/cross_tsdb/compare_promql.sh
```

Set `BACKENDS=greptime` or `BACKENDS=prometheus` to benchmark only a receiver
that completed a correctness-preserving replay. `greptime_full_pipeline.sh`
chains full replay, schema discovery, resource snapshots, and the focused
GreptimeDB comparison for a supervised long-running experiment.

The runner uses one fixed set of Chronoxide release binaries, checks stable
results across repetitions, and passes the direct-core oracle's portable
fingerprint, series count, and sample count as required expectations to every
HTTP endpoint. Raw JSON, configuration, binary hashes, the Git state, and
`summary.tsv` stay together in the new result directory.

The default backend list now starts `chronoxide-api` and measures Chronoxide,
Prometheus, and GreptimeDB with the same HTTP client and wall-latency boundary.
The `chronoxide-core` summary row is the internal CLI measurement retained as a
semantic oracle and overhead diagnostic; use the `chronoxide` row for the
apples-to-apples HTTP comparison. The Chronoxide API defaults to `pread`, one
admitted query, and a disabled range-scalar cache for this harness. Override
these with `CHRONOXIDE_CHUNK_READ_MODE`,
`CHRONOXIDE_MAX_CONCURRENT_QUERIES`, and
`CHRONOXIDE_RANGE_SCALAR_CACHE_MAX_BYTES`; the chosen values are recorded in
the result directory. `chronoxide-config.txt` records the selected storage
schema, corpus, and resolved binary paths, while `binaries.sha256` records the
exact executable contents.

Cold operating-system-cache testing still needs a separate run schedule that
stops the services, evicts only the measured data files, verifies residency,
restarts the service, and executes exactly one query. A first request to a live
server is not proof of a cold page cache.

For resource evidence, record container stats and disk use after replay and
after the query schedule:

```sh
docker stats --no-stream >"$STACK_RESULT_DIR/docker-stats.txt"
du -sb "$STACK_RESULT_DIR/prometheus-data" "$STACK_RESULT_DIR/greptime-data" \
  >"$STACK_RESULT_DIR/disk-usage.txt"
```

Prometheus and GreptimeDB do not expose Chronoxide's logical payload-used and
coalesced-read counters. Keep those fields as unavailable rather than
substituting filesystem size or process-issued bytes with a different meaning.
