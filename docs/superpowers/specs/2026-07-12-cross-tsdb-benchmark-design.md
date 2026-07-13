# Cross-TSDB OTLP replay and PromQL benchmark design

## Status and objective

This document defines the first reproducible comparison of Chronoxide,
Prometheus, and GreptimeDB on the captured production-shape OTLP corpus. The
comparison covers ingestion accounting, storage footprint, and PromQL query
latency. It does not claim semantic equivalence for a query until canonical
results match.

The source of truth is the original capture, not a Chronoxide native segment
directory:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001
```

Its manifest records one ordered partition, 13,000,000 source messages, and
142,302,490,056 uncompressed OTLP protobuf bytes. Native Chronoxide segments
cannot be imported by either comparison system.

## Correctness constraints

1. Read capture records through `OtlpCaptureReader`, preserving its stable
   global sequence and the original `ExportMetricsServiceRequest` contents.
2. Batching may concatenate ordered `resource_metrics` arrays but must not
   reorder resources, scopes, metrics, or datapoints.
3. Apply Chronoxide's ingest rule for missing Gauge/Sum number values: count
   and omit those datapoints and any envelopes made empty by the omission.
   Never synthesize zero.
4. Before batching, apply the selected Chronoxide event-time policy to every
   datapoint using its source record's trusted `captured_at_ms`: reject a zero
   OTLP timestamp and accept only the inclusive range from capture minus
   `max_event_age_secs` through capture plus `max_event_lead_secs`. Preserve
   accepted `time_unix_nano` values unchanged; never use the Kafka timestamp.
5. Batches are bounded by both input-message count and estimated protobuf
   bytes. One oversized source request is sent alone.
6. Automatic retries are excluded: retrying an ambiguously accepted OTLP/HTTP
   request can create duplicates. A failed request stops replay and reports
   its first/last capture position.
7. A non-2xx response, malformed OTLP response, or positive
   `rejected_data_points` count fails replay. Warning-only partial-success
   responses are recorded.
8. Every report records capture manifest metadata, source and emitted message
   counts, protobuf bytes, resource metrics, datapoints, request latency, and
   endpoint/header configuration with authorization values redacted.
9. PromQL timing begins only after ingestion is complete, rejection counts are
   zero, and the backend is quiescent.
10. A cross-backend query is correctness-comparable only after canonicalized
   labels, timestamps, values, series count, and sample count match. Backend-
   local stable fingerprints are useful diagnostics but are not cross-backend
   correctness proof.

`captured_at_ms` remains capture metadata. The destination systems place data
using the OTLP datapoint timestamps embedded in the unchanged request. The
replayer never rewrites event time from capture or Kafka timestamps.

## Replay architecture

```text
OtlpCaptureReader
    -> decode ExportMetricsServiceRequest
    -> append ordered resource_metrics to bounded batch
    -> encode one ExportMetricsServiceRequest
    -> persistent OTLP/HTTP client
    -> decode ExportMetricsServiceResponse
    -> update atomic run report
```

The first implementation is deliberately sequential. Query benchmarking is
the objective, so ingestion throughput must not be improved by concurrency at
the cost of ordering ambiguity. A later ingestion-only experiment may add
bounded per-partition concurrency, but it is a separate comparison.

Default bounds are 4 MiB of estimated source protobuf and 512 source messages
per HTTP request. The capture's average source message is about 10.9 KiB, so
these defaults reduce 13 million HTTP calls to tens of thousands while
retaining bounded memory.

## Backend configuration

Prometheus receives OTLP/HTTP at `/api/v1/otlp/v1/metrics` with
`--web.enable-otlp-receiver`. The pinned experiment enables its experimental
`otlp-deltatocumulative` feature and uses the collision-preserving
`NoUTF8EscapingWithSuffixes` translation. Resource-attribute promotion and
the finite out-of-order window are recorded explicitly.

GreptimeDB receives OTLP/HTTP at `/v1/otlp/v1/metrics`. The replay request uses
`X-Greptime-DB-Name` and promotes all resource attributes for an initial
series-identity audit.

The configurations are not assumed semantically identical to Chronoxide:

- metric and label translation rules differ;
- Prometheus converts delta OTLP streams only through an experimental feature;
- GreptimeDB documents that delta Sum and Histogram values are persisted
  directly rather than accumulated;
- native Histogram and ExponentialHistogram PromQL support differs.

Therefore the first comparable suite starts with gauges, cumulative scalar
streams, and aggregate scalar projections. Delta/native histogram expressions
remain diagnostic until their canonical result oracle passes.

## Query benchmark

Prometheus uses `/api/v1/query` and `/api/v1/query_range`. GreptimeDB uses the
same paths below `/v1/prometheus/` with an explicit database. The HTTP runner:

- uses one persistent client per measured process;
- performs an unmeasured semantic warmup;
- canonicalizes matrix/vector/scalar results;
- supports explicit label renames and drops needed to reconcile translation;
- requires all measured repetitions to retain one fingerprint and result
  shape;
- optionally requires an expected cross-backend fingerprint;
- records client wall latency, response bytes, series, samples, and SHA-256.

Large-result response serialization is part of HTTP end-to-end latency. It is
not directly comparable to Chronoxide's current internal CLI duration. Reports
must show both HTTP latency and Chronoxide engine latency until Chronoxide has
an equivalent HTTP API.

## Cold and warm methodology

Warm runs reuse a quiescent long-lived backend and repeat the same query. A
payload/file-cache-evicted run requires stopping the backend, issuing
`POSIX_FADV_DONTNEED` for only that backend's data directory, verifying
residency with `fincore`, restarting, waiting for health, and then issuing one
query. This does not flush controller cache.

Prometheus may naturally retain a 1.5-hour corpus in its mutable head while
Chronoxide queries sealed segments. Default-product-state and explicitly
compacted/on-disk results must be reported separately rather than presented as
one equivalent storage state.

Each accepted comparison records:

- exact images/binaries and hashes;
- host, kernel, filesystem, CPU and memory limits;
- capture manifest fingerprint and replay report;
- backend configuration and on-disk bytes;
- nine deterministically interleaved repetitions;
- cold and warm latency distributions;
- response fingerprint, series, and samples;
- server CPU, peak RSS, and client response bytes.

Builds, ingestion, compaction, profilers, and unrelated workloads must not
overlap timed query processes.

## Deliverables

- `chronoxide-otlp-http-replay`: bounded ordered capture replay over OTLP/HTTP.
- `chronoxide-promql-http-bench`: canonical Prometheus-compatible HTTP query
  benchmark.
- `chronoxide-api`: the managed Prometheus-compatible Chronoxide endpoint used
  for the same client-side timing boundary as Prometheus and GreptimeDB; the
  direct core runner remains an oracle and internal-overhead diagnostic.
- pinned single-node Prometheus and GreptimeDB Compose configuration.
- lifecycle, replay, discovery, and query scripts under
  `docs/experiments/cross_tsdb/`.
- focused fake-server tests for batching, ordering, response rejection, and
  result canonicalization.
- a run-specific external artifact directory containing raw reports and logs.

No Chronoxide on-disk format or query semantics change is part of this work.
