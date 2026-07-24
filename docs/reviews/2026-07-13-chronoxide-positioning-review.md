# Chronoxide Product Positioning and Competitive Review

- **Date:** 2026-07-13
- **Status:** Evidence-backed strategy review; non-normative
- **Implementation revision reviewed:**
  `ccd7adec97c784de946537f87b567d4dd2b93445`

## Authority and scope

[storage.md](../superpowers/specs/storage.md), [clock.md](../superpowers/specs/clock.md), and
[promql-coverage.md](../promql-coverage.md) remain authoritative for storage,
time, and query semantics. This document assesses whether the product concept is
coherent, where it is competitively differentiated, what current evidence does
and does not support, and which claims require more proof.

Competitor statements are time-sensitive. The external documentation cited here
was checked on 2026-07-13. Benchmark artifacts and dated design documents are
evidence, not standing performance guarantees.

This review does not change implementation behavior, on-disk formats, or the
project backlog.

## Executive verdict

The concept makes sense. Chronoxide has a defensible product wedge, but it is
not currently a generally better TSDB than Prometheus, GreptimeDB,
VictoriaMetrics, or Grafana Mimir.

The strongest positioning is:

> Chronoxide is a deterministic OTLP metrics flight recorder and typed
> near-store: it preserves raw capture input, carries interval, reset, and
> staleness semantics into native segments, and exposes verified PromQL
> projections.

The core differentiator is not accepting OTLP. Prometheus, GreptimeDB, and
VictoriaMetrics all accept OTLP metrics. It is not Rust, a custom segment
format, or a single warm-query latency result either. The differentiator is the
combination of:

1. native typed persistence of OTLP Histogram, ExponentialHistogram, and
   Summary datapoints;
2. preservation of temporality, start time, flags, and reset hints;
3. cumulative-shaped PromQL projection from delta intervals;
4. exact handling of Prometheus staleness and non-finite values for covered
   expressions;
5. raw captured OTLP plus deterministic replay; and
6. a specialized immutable local near-store designed around this richer source
   model.

That is a narrow and valuable technical thesis. It is not yet a broad
production-database thesis.

## Why the concept is coherent

### Store the richer model and derive compatibility views

OTLP metrics contain correctness information that ordinary scalar Prometheus
series cannot always represent without transformation:

- delta versus cumulative aggregation temporality;
- the start of an aggregation interval;
- explicit counter reset evidence;
- no-recorded-value flags;
- Histogram and ExponentialHistogram bucket structure;
- optional signed and non-finite sums;
- resource and instrumentation-scope identity; and
- future typed fields retained in captured raw OTLP.

Chronoxide stores the richer source model and makes the Prometheus-compatible
series a query-time projection. This is an information-preserving direction:
several compatibility views can be derived from a richer source record, while
discarded temporal metadata generally cannot be reconstructed later.

This is analogous to retaining an authoritative event log and building
materialized views over it. PromQL is an important compatibility surface, but
it does not need to be the physical storage schema.

### `Lossless` needs two explicit meanings

Chronoxide has two different preservation boundaries and should never blur
them:

1. A capture record can preserve the raw `ExportMetricsServiceRequest` bytes
   plus trusted capture metadata. This is the byte-preserving flight-recorder
   boundary.
2. A native segment preserves the normalized query identity and the named
   typed correctness fields implemented by its format. This is a semantic
   readback boundary, not a promise that every OTLP field survives in the
   segment.

For example, exemplar segment sidecars remain forward-looking, and resource or
scope identity is normalized into the configured label model. A public
`lossless` claim must therefore enumerate its boundary and fields. The safer
default language is `raw-capture preserving` and `typed-semantic preserving`
for the documented segment fields.

### Delta metrics are intervals, not disguised cumulative samples

An OTLP delta Histogram or ExponentialHistogram datapoint describes an
interval. PromQL counter functions expect a cumulative-shaped history.
Correctly bridging those models requires more than summing values:

- selected non-stale intervals need valid `start_time_ms < timestamp_ms`;
- stale gaps must not silently invent resets or shorten the logical range;
- retention boundaries may need one aligned predecessor as a subtraction seed;
- stored reset hints remain authoritative where specified;
- optional sums remain signed IEEE interval values; and
- count/bucket and sum evaluation do not always share one algorithm.

Chronoxide's normative rules and focused tests explicitly address these cases.
That makes the projection model difficult, but internally coherent.

### Deterministic capture and replay is a product feature

The trusted `captured_at_ms` anchor, stable source order, identical writer
configuration, and deterministic segment IDs support reproducible storage
output. Keeping raw `ExportMetricsServiceRequest` bytes in the capture/WAL
model also avoids making the current normalized schema the only surviving
record of the input.

This enables workflows that ordinary best-effort telemetry ingestion does not
make easy:

- incident reconstruction;
- deterministic backfill;
- query-engine regression testing;
- format migration and readback verification;
- semantic A/B comparison between releases; and
- auditable replay of a known input sequence.

The feature is valuable only if crash recovery, checkpoints, compaction,
out-of-order handling, and replay retain the same invariants. Those paths must
be proved, not inferred from the design.

### Typed storage can avoid write-time series explosion

Persisting one native typed datapoint avoids eagerly expanding every Histogram
or ExponentialHistogram into many synthetic scalar series. It retains the
option to:

- read only `_count` or `_sum` scalar lanes;
- decode bucket bodies only for bucket-dependent functions;
- project query-configured ExponentialHistogram boundaries;
- downscale compatible exponential histograms during aggregation; and
- add future projection policies without replaying already-flattened input.

The current schema-varlen payload still duplicates some common fields, so the
architectural opportunity is real but the physical layout is not finished.
See [the storage read-path review](2026-07-13-storage-read-layout-review.md).

## Defensible competitive position

| Dimension | Chronoxide | Prometheus | GreptimeDB | VictoriaMetrics / Mimir |
| --- | --- | --- | --- | --- |
| Direct OTLP metrics ingestion | Yes | Yes | Yes | Yes, including native OTLP/HTTP in both VictoriaMetrics and Mimir |
| Original typed OTLP persistence | Strong design and implemented typed chunks for Histogram, ExponentialHistogram, and Summary | Translates OTLP into the Prometheus data model | Prometheus-compatible mapping transforms names/metadata; current docs save delta Sum/Histogram values directly | VictoriaMetrics translates OTLP into its storage model; Mimir is primarily a distributed Prometheus backend |
| Delta Histogram PromQL projection | Explicit cumulative-shaped projection with start/reset/stale rules | Delta-to-cumulative conversion is documented as experimental | Current docs say delta values are saved directly without cumulative calculation | VictoriaMetrics recommends cumulative input or collector conversion for delta workflows |
| OTLP ExponentialHistogram | Native typed storage and query projection | Converted to Prometheus native histograms | Current Prometheus-compatible OTLP model says unsupported | VictoriaMetrics converts it to its histogram representation; Mimir supports Prometheus native histograms |
| PromQL breadth | Partial | Reference implementation | Broad, documented as over 90%, with gaps | Broad MetricsQL/PromQL or PromQL-compatible surface |
| Exact Prometheus semantics | Strong proof discipline for covered cases, incomplete overall | Reference | Broad compatibility, not the reference | MetricsQL intentionally differs in some cases; Mimir targets Prometheus compatibility |
| Byte/semantic-deterministic captured replay | Strong intended differentiator | Not a core local-TSDB workflow | Kafka and ingestion integrations exist, but deterministic raw OTLP corpus reproduction is not the core product | Mimir has ordered Kafka ingest and offset replay; Chronoxide must differentiate exact capture-time, segment-ID, byte, and query reproducibility |
| Local single-node specialization | Core scope | Core local TSDB | Standalone mode plus distributed mode | VictoriaMetrics has mature single-node and cluster products |
| Distributed query and object-store durability | Explicitly out of current scope | Local Prometheus is not clustered; remote systems fill this role | Mature architectural focus | Mature architectural focus |
| Logs, traces, SQL, broad analytics | Not the goal | Metrics-focused | Strong multimodal and SQL surface | Product-dependent; broader than Chronoxide in deployed observability workflows |
| Operational maturity and ecosystem | Early | Excellent | Strong | Strong |

The table describes different product boundaries, not a universal ranking.

## Competitor-specific assessment

### Prometheus

Prometheus remains the reference PromQL implementation and the operational
default for scrape-oriented monitoring. Its current documentation includes a
direct OTLP/HTTP receiver, resource-attribute promotion controls, OTLP name
translation strategies, conversion of OTLP ExponentialHistogram data into
Prometheus native histograms, and an experimental OTLP delta-to-cumulative
feature.

Consequences for Chronoxide positioning:

- `OTLP ingestion` is not a differentiator by itself.
- `ExponentialHistogram support` is not a differentiator by itself.
- Preserving original OTLP interval semantics and replay provenance is a
  differentiator.
- Prometheus wins on complete PromQL behavior, scrape discovery, rules,
  alerts, integrations, documentation, recovery history, and user trust.
- Local Prometheus is not clustered or replicated, so a specialized local
  Chronoxide near-store is conceptually credible without claiming distributed
  superiority.

Official sources:

- [Prometheus OpenTelemetry guide](https://prometheus.io/docs/guides/opentelemetry/)
- [Prometheus native histogram specification](https://prometheus.io/docs/specs/native_histograms/)
- [Prometheus storage documentation](https://prometheus.io/docs/prometheus/latest/storage/)

### GreptimeDB

GreptimeDB's current documentation describes native OTLP/HTTP ingestion and a
Prometheus-compatible data model. It also says that:

- delta Sum and Histogram values are saved directly without calculating
  cumulative values;
- ExponentialHistogram is not yet supported in that Prometheus-compatible OTLP
  model;
- name, unit, type, resource-attribute, and scope-attribute mappings depend on
  ingestion configuration; and
- its native Rust PromQL engine supports over 90% of PromQL, while retaining
  documented gaps such as the `@` modifier.

GreptimeDB has compute/storage separation, distributed metadata and routing,
independently scalable frontend and datanode roles, WAL, immutable object-store
files, multiple object-storage backends, SQL, logs, traces, and continuous
aggregation components.

Consequences for Chronoxide positioning:

- Chronoxide has a credible semantic lead for delta Histogram and
  ExponentialHistogram PromQL projection.
- Chronoxide must not claim broader PromQL support.
- Chronoxide must not compete on distributed scale, object-store durability,
  multimodal observability, or SQL breadth today.
- A leaner local execution path may win selected latency shapes, but this must
  be demonstrated per workload rather than inferred from architectural size.

Official sources:

- [GreptimeDB OpenTelemetry ingestion](https://docs.greptime.com/user-guide/ingest-data/for-observability/opentelemetry/)
- [GreptimeDB PromQL](https://docs.greptime.com/user-guide/query-data/promql/)
- [GreptimeDB architecture](https://docs.greptime.com/user-guide/concepts/architecture/)
- [GreptimeDB storage locations](https://docs.greptime.com/user-guide/concepts/storage-location/)

### VictoriaMetrics

VictoriaMetrics accepts OTLP metrics directly. Its current documentation says
that OTLP ExponentialHistogram data is converted into the VictoriaMetrics
histogram representation and that delta metrics are stored as received. It
recommends cumulative temporality or collector-side delta-to-cumulative
conversion for workflows that need cumulative behavior.

MetricsQL is intentionally PromQL-compatible rather than bit-for-bit identical
in every semantic corner. VictoriaMetrics also has mature single-node and
cluster deployments.

Consequences for Chronoxide positioning:

- Direct OTLP and ExponentialHistogram acceptance are again not unique.
- Source-semantic preservation and strict Prometheus-oracle agreement for
  covered cases remain meaningful differentiators.
- Chronoxide cannot yet claim VictoriaMetrics-level operational maturity,
  ingest scale, compression, availability, or cost efficiency.

Official sources:

- [VictoriaMetrics OpenTelemetry integration](https://docs.victoriametrics.com/victoriametrics/integrations/opentelemetry/)
- [VictoriaMetrics MetricsQL](https://docs.victoriametrics.com/victoriametrics/metricsql/)
- [VictoriaMetrics cluster architecture](https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/)

### Grafana Mimir and distributed Prometheus backends

Grafana Mimir is designed as a horizontally scalable, highly available,
multi-tenant, long-term Prometheus backend using object storage and distributed
components. Current Mimir also accepts OTLP/HTTP directly and converts OTLP
ExponentialHistogram datapoints to Prometheus native histograms.

Mimir's preferred ingest-storage architecture uses Kafka as durable ingest
storage, including at-least-once consumption, per-partition ordering,
offset-based replay, WAL recovery, availability-zone replication, and optional
strong read consistency. Therefore neither `Kafka-backed ingestion` nor
`replayable metrics input` is unique. Chronoxide's narrower claim must be
stable capture-time policy and reproducible segment IDs, bytes or documented
semantic hashes, and query fingerprints from a preserved raw OTLP corpus.

This distributed scope is outside Chronoxide's current local-storage boundary.

Chronoxide may eventually compose with this category as a semantic ingest,
replay, or near-store layer. It should not present itself as a replacement for
their distributed control planes today.

Official source:

- [Grafana Mimir architecture](https://grafana.com/docs/mimir/latest/get-started/about-grafana-mimir-architecture/)
- [Grafana Mimir OpenTelemetry ingestion](https://grafana.com/docs/mimir/latest/configure/configure-otel-collector/)
- [Grafana Mimir ingest-storage architecture](https://grafana.com/docs/mimir/latest/get-started/about-grafana-mimir-architecture/about-ingest-storage-architecture/)
- [Grafana Mimir query engine](https://grafana.com/docs/mimir/latest/references/architecture/mimir-query-engine/)

### Thanos

Thanos already provides a horizontally scalable PromQL query layer, global
views, high-availability deduplication, compaction, immutable Prometheus blocks,
and object-storage range reads. Immutable files plus PromQL are therefore not a
market differentiator by themselves.

Chronoxide's opportunity is below that layer: retaining typed OTLP interval
semantics and reproducible raw input before producing a Prometheus-compatible
view or downstream artifact.

Official sources:

- [Thanos Querier](https://thanos.io/tip/components/query.md/)
- [Thanos object-storage format](https://thanos.io/tip/thanos/storage.md/)

## Evidence already available

### Production-shaped corpus

The captured input and resulting corpus are large enough to expose real
cardinality and metadata behavior:

- 13,000,000 ordered source messages;
- 142,302,490,056 uncompressed OTLP protobuf bytes;
- 500,600,784 stored datapoints;
- 9,834,035 unique global labelsets in the ingestion report;
- 47,766,209 segment-local series rows; and
- exactly 47,766,209 chunks, or 10.480 datapoints per chunk on average.

The capture counts and comparison rules are documented in
[the cross-TSDB benchmark design](../superpowers/specs/archive/benchmarks/2026-07-12-cross-tsdb-benchmark-design.md).
The segment counts and physical layout are documented in
[the storage read-path review](2026-07-13-storage-read-layout-review.md).

This is evidence that Chronoxide is being exercised against a difficult
high-cardinality OTLP shape rather than only synthetic low-cardinality
microbenchmarks. It is not by itself evidence of competitive cost or latency.

### Semantic proof

The current Prometheus golden harness covers a substantial set of scalar,
staleness, reset, native histogram, typed OTLP projection, non-finite, and
range-query cases against the real Prometheus evaluator. This is a meaningful
engineering asset.

The coverage matrix remains explicit that:

- parser/lowering and range-query support are partial;
- `@` is unsupported;
- subqueries are unsupported;
- full native-histogram operator parity is incomplete; and
- deeper staleness, reset, and non-finite compositions remain.

The defensible statement is therefore `Prometheus-compatible semantics for the
covered surface`, not `full PromQL compatibility`.

### Narrow warm HTTP latency result

Three completed same-host warm HTTP schedules compared Chronoxide and
GreptimeDB for one portable-fingerprint-matched stored query:

```promql
count(go_gc_duration_seconds_count)
```

| Run | Chronoxide median | GreptimeDB median | Ratio |
| --- | ---: | ---: | ---: |
| pread A | 4.311 ms | 14.467 ms | 3.36x |
| pread B | 4.509 ms | 12.938 ms | 2.87x |
| io_uring diagnostic | 4.404 ms | 12.790 ms | 2.90x |

All rows returned one series and one sample with portable fingerprint
`5ecda2859cf1211dcf37b5eca0cfe426661074d990467351a5c811f0e76c9d6b`.
The raw run roots are:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/cross-tsdb-http-greptime-20260713-111958
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/cross-tsdb-http-greptime-20260713-112141
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/cross-tsdb-http-iouring-greptime-20260713-113545
```

This is an encouraging proof point for one warm aggregate over a selected
metric. It is not support for `Chronoxide is 3x faster than GreptimeDB` because
the accepted comparison does not yet cover:

- Prometheus;
- cold operating-system cache;
- broad selectors or regex-heavy planning;
- high-cardinality result materialization;
- range queries;
- `rate()` or `increase()`;
- native or virtual histogram evaluation;
- concurrent clients;
- ingestion throughput;
- storage footprint, CPU, or peak RSS; or
- long-duration compaction and recovery behavior.

The `vector(1)` result in the same schedules mostly measures endpoint and
framework overhead, not storage performance, and should not headline a storage
claim.

### Current storage cost is a warning, not an advantage

The measured segment corpus contains 10.644 GB of tracked metadata and 10.901
GB of `chunks.bin`. Metadata is 49.4% of tracked bytes and 97.6% of the chunk
payload size. Selective queries can spend more time and traffic on symbols,
series rows, and label materialization than on selected samples.

Consequently, Chronoxide should not currently claim superior compression or
storage efficiency. The highest-leverage layout work is paged dictionaries,
compact columnar series metadata, inline single-chunk metadata, compressed
postings, independently readable typed scalar lanes, and later packed frames.
SIMD is most useful after those data structures become columnar.

## Claims that are supportable now

Each claim must retain its qualifier.

### Product-concept claims

- `OTLP-native typed metrics storage` is supportable when accompanied by the
  exact implemented types and metadata fields.
- `Deterministic replay design` is supportable; `deterministic crash recovery`
  requires the full recovery matrix to pass.
- `PromQL-compatible query API` is supportable; `full PromQL` is not.
- `Native Histogram, ExponentialHistogram, and Summary persistence` is
  supportable for implemented layouts and projections.
- `Cumulative-shaped PromQL projection for OTLP delta Histogram and
  ExponentialHistogram` is supportable for the covered semantics.
- `Prometheus-backed semantic verification` is supportable because the golden
  harness uses a real external evaluator rather than production helpers as its
  expected-value oracle.

### Performance claims

- `Promising single-node warm-query latency` is supportable.
- A named query, corpus, backend versions, configuration, fingerprint, and
  median result may be reported exactly.
- `Faster than GreptimeDB` without a workload qualifier is not supportable.
- No compression, cost, ingestion, tail-latency, or concurrency advantage is
  currently proved.

## Claims to avoid

Do not claim:

- `the first` or `the only OTLP-native TSDB`;
- `a drop-in Prometheus replacement`;
- `full PromQL compatibility`;
- `faster than Prometheus`, `faster than GreptimeDB`, or `fastest TSDB`;
- `better compression` or `lower storage cost`;
- `production-grade durability` before WAL/checkpoint/recovery evidence;
- `distributed`, `highly available`, or `object-store native`;
- `lossless` without separating byte-preserving raw capture from the named
  correctness fields retained by normalized native segments;
- semantic equivalence based only on series/sample counts; or
- a performance comparison for any query whose portable semantic fingerprint
  does not match.

Rust, SIMD, io_uring, and custom encodings are implementation techniques. They
may enable an advantage, but they are not customer value propositions by
themselves.

## Best initial workloads

Chronoxide is best positioned for operators who have several of these traits:

- OpenTelemetry-first metrics rather than scrape-first instrumentation;
- Kafka or captured OTLP as an authoritative ordered input;
- substantial delta Histogram or ExponentialHistogram traffic;
- a need to preserve temporal provenance and replay exactly;
- strict Prometheus query expectations for the supported surface;
- high-cardinality local analysis where a specialized near-store is useful;
- incident, migration, or query-regression workflows that need a stable corpus;
  and
- willingness to use a focused metrics engine rather than a multimodal
  observability database.

Poor initial targets are teams that primarily need:

- turnkey scraping, service discovery, alerting, and recording rules;
- complete PromQL and immediate Grafana ecosystem parity;
- distributed multi-region availability;
- long-term object-store retention;
- one database for metrics, logs, traces, and SQL analytics; or
- an operationally proven hosted service.

## Recommended product boundary

### Primary recommendation: semantic near-store and replay engine

Develop Chronoxide first as a high-performance local metrics engine with three
explicit roles:

1. authoritative typed storage for a bounded near-store window;
2. deterministic replay and semantic validation of captured OTLP; and
3. Prometheus-compatible query access for supported expressions.

Kafka provides durable ordered distribution outside the process. A mature
long-term backend may remain downstream. This boundary makes Chronoxide useful
before it owns a distributed control plane.

### Plausible later role: semantic ingestion layer

Chronoxide could eventually emit one or more derived products:

- Prometheus remote-write/native-histogram streams;
- object-store blocks;
- verified cumulative streams from delta inputs;
- compact rollups retaining typed reset boundaries; or
- semantic-diff reports for downstream databases.

This role turns the richer source model into an integration advantage rather
than requiring Chronoxide to replace every existing backend.

### Defer: general distributed observability database

A distributed metadata service, replication protocol, global compactor,
object-store consistency model, distributed query planner, tenant scheduler,
and multimodal SQL layer would multiply the correctness and operational scope.
They do not strengthen the initial semantic wedge enough to justify building
them before local durability and query correctness are established.

## Priority roadmap

### Priority 0: prove durability and replay invariants

Before production positioning, demonstrate:

1. normal shutdown and restart;
2. interruption during WAL append;
3. interruption during segment seal and manifest publication;
4. checkpoint recovery and Kafka/source offset agreement;
5. raw-capture replay after loss of the local near-store;
6. deterministic segment IDs and byte/fingerprint stability;
7. out-of-order and event-time-policy equivalence after recovery; and
8. query equivalence before and after restart/replay.

Recovery correctness is more valuable to the product claim than another
single-query optimization.

### Priority 1: remove the metadata tax

Execute the evidence program in the storage layout review:

1. establish no-format batching/cache baselines;
2. make planning dictionaries and directories paged and point-addressable;
3. split hot routing columns from cold series metadata;
4. inline the common one-chunk case;
5. compress postings adaptively;
6. separate typed `_count` and `_sum` lanes from bucket bodies; and
7. evaluate packed frames and adjacent-segment packing after the earlier wins.

Every layout change must preserve corruption propagation, immutable positional
reads, deterministic bytes, and typed semantics.

### Priority 2: close high-value PromQL gaps

Prioritize by real query traces and compatibility leverage:

1. subqueries;
2. `@` modifiers;
3. remaining native-histogram composition and annotations;
4. deeper reset/staleness/non-finite compositions;
5. lookback and range-query parity; and
6. the most frequent missing scalar/math functions.

Continue using Prometheus as the external semantic oracle. Unsupported syntax
must fail explicitly rather than produce approximate results.

### Priority 3: complete falsifiable cross-TSDB proof

The comparison matrix should include, at minimum:

- Prometheus, GreptimeDB, and Chronoxide from identical accepted capture input;
- a VictoriaMetrics run when the harness can preserve a fair semantic boundary;
- scalar gauge and cumulative counter selectors;
- high-cardinality equality and regex matchers;
- `rate()` and `increase()` with resets and staleness;
- classic histogram projection;
- delta Histogram and ExponentialHistogram native and virtual queries;
- instant and range queries;
- small, medium, and large result sets;
- cold-cache and warm-cache schedules;
- one and multiple concurrent clients;
- ingest throughput through equivalent public protocols;
- disk bytes, CPU, peak RSS, and response bytes; and
- restart/recovery time.

Semantic fingerprints gate latency comparison. A semantic mismatch is itself a
useful competitive finding, but its latency is not comparable.

### Priority 4: make the wedge easy to adopt

The product needs a clear supported deployment and integration story:

- OTLP/HTTP and/or OTLP/gRPC entry points with documented translation policy;
- raw Kafka OTLP and captured-replay workflows;
- a Prometheus-compatible HTTP query endpoint;
- Grafana datasource compatibility for the supported surface;
- explicit retention and local-disk capacity guidance;
- health, readiness, metrics, and corruption diagnostics;
- export or handoff to a mature long-term backend; and
- a versioned compatibility statement for storage, query, and OTLP semantics.

## Proof gates for stronger positioning

| Desired claim | Minimum proof gate |
| --- | --- |
| Deterministic | Same capture/configuration produces identical segment IDs and semantic/byte fingerprints across clean replay and recovery |
| Durable | Power-loss/torn-write matrix passes with no acknowledged-data loss beyond the documented sync policy |
| PromQL-compatible | Supported-surface matrix passes against real Prometheus, with unsupported forms enumerated |
| Faster for workload X | Same host, public protocol, semantic fingerprint, backend state, cold/warm policy, concurrency, and resource report |
| More storage-efficient | Same accepted datapoints and semantics, measured physical bytes after comparable quiescence/compaction |
| Lower cost | Ingest, retention, query SLO, recovery, CPU, memory, disk, and operator effort measured together |
| Production-ready | Durability, upgrades, observability, capacity limits, corruption handling, and sustained soak evidence |

## Strategic risks

### Competitors can close an individual feature gap

Prometheus can mature its delta conversion. GreptimeDB can add
ExponentialHistogram support. VictoriaMetrics can retain more OTLP metadata.
Therefore the moat cannot be one unchecked feature box.

The more durable advantage is an integrated discipline:

- source-semantic preservation;
- deterministic capture and replay;
- explicit byte formats;
- independent expected-value oracles;
- typed query semantics; and
- reproducible, fingerprint-gated real-corpus evidence.

### PromQL scope can consume the project

Implementing every PromQL corner, storage engine feature, alerting subsystem,
and distributed-database component would erase the benefits of specialization.
PromQL work should be guided by observed workloads and semantic leverage, while
unsupported expressions remain explicit.

### Rich semantics can become expensive semantics

Preserving more metadata is useful only if ordinary queries do not pay to
decode it all. The physical format must keep hot scalar lanes and planning
columns independently readable. Otherwise semantic fidelity will translate
into persistent latency and storage disadvantages.

### Replay is only as deterministic as every boundary

Stable input order is insufficient if segment IDs, map iteration, dictionary
assignment, OOO dedupe, configuration, compaction, floating aggregation, or
recovery scheduling can change the result. Determinism needs an end-to-end
fingerprint and fault-injection suite.

### A local engine still needs an operational answer

Even a near-store must state what happens when a node dies, a disk fills, an
index is corrupt, Kafka retention expires, or a storage version changes. A
focused scope reduces these obligations but does not remove them.

## Suggested public language

### One sentence

> Chronoxide captures raw OTLP metrics for deterministic replay, preserves
> interval, reset, flag, and native histogram semantics in typed near-store
> segments, and exposes verified PromQL-compatible projections.

### Short technical description

> Chronoxide is an OTLP-native metrics near-store and replay engine. It ingests
> ordered OTLP from Kafka or captured files, persists native typed metric
> segments, and exposes a Prometheus-compatible query surface. Its storage and
> query model retain interval starts, temporality, reset hints, no-recorded-value
> flags, and native histogram structure so covered PromQL projections can be
> verified against Prometheus without discarding the original OTLP semantics.

### Honest maturity statement

> Chronoxide currently targets a correctness-first local metrics engine. PromQL
> coverage is substantial but incomplete, distributed object-store operation is
> out of scope, and production durability claims remain gated on the documented
> recovery and replay verification matrix.

## Final recommendation

Continue the project, but defend the narrow thesis.

Chronoxide can be better than Prometheus for OTLP-first, delta/native-histogram,
capture-and-replay workflows where source semantics matter more than scrape and
alerting breadth. It can be better than GreptimeDB for the covered typed-OTLP to
PromQL semantic bridge, while being substantially behind GreptimeDB in
distributed and multimodal database capabilities. It can complement rather
than immediately replace VictoriaMetrics, Mimir, and other mature long-term
backends.

The next credible milestone is not `another TSDB feature`. It is a demonstrated
chain from accepted raw OTLP, through crash-safe deterministic storage, to
fingerprint-matched PromQL results and measured resource use on the full
production-shaped corpus.
