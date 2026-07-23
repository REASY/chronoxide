# Storage layer specification (OTLP-native TSDB with PromQL)

This document specifies the **local / SSD-friendly storage layer** for an OTLP-native metrics TSDB that supports **PromQL querying**.

The default sealed-store writer and reader contract is schema 8. It uses
`symbols.bin` v3, `series.bin` v3, overflow-only `chunk_index.bin` v2, and
`indexes.puffin` v9 with deterministic RAW32/delta-ULEB128 adaptive exact
postings. Schema 7 remains available only as an explicit prior-format
comparator and continues to require raw-postings `indexes.puffin` v8. Schema 6
is readable only through the explicit, fully footer-validated `schema6-ab`
comparison policy; it is not a production fallback. Existing corpora migrate
through deterministic replay, and mixed-schema stores are rejected.

It covers: write path, crash safety, on-disk layout and formats, out-of-order handling, and the index structures required to execute PromQL selectors efficiently — including **regex matchers**.

**Scope**
- **In scope**: ingestion durability (WAL), windowed in-memory head buffer (currently tied to `segment_duration`, default 1h), SSD segments (“near” store), indexes for label matchers (including regex), crash recovery, late-sample policy, and shard-local offset checkpointing.
- **Out of scope (but shaped by this spec)**: long-term object-store blocks, global compaction, and distributed query routing.

## Primer: metric vs series (example)

PromQL distinguishes between a **metric name** and a **time series**.

Example metric name:
- `pod_cpu_usage_seconds_total`

In Prometheus/PromQL, the metric name is conceptually just a label called `__name__`. So the selector:
- `pod_cpu_usage_seconds_total`
is shorthand for:
- `{__name__="pod_cpu_usage_seconds_total"}`

**What is a metric?**
- A **metric** is a named measurement (here: CPU usage seconds accumulated) that can have many dimensional variations.

**What is a series?**
- A **series** is one unique labelset for a metric name, producing a sequence of samples over time.
- In other words: `Series = metric_name + full set of labels` (label order does not matter; the set of key/value pairs does).

Concrete series examples (same metric name, different labelsets ⇒ different series):
- `pod_cpu_usage_seconds_total{namespace="default",pod="nginx-123",container="app",cpu="0"}`
- `pod_cpu_usage_seconds_total{namespace="default",pod="nginx-123",container="app",cpu="1"}`
- `pod_cpu_usage_seconds_total{namespace="default",pod="nginx-123",container="sidecar",cpu="0"}`

**How is one series distinguished from another?**
- Two series are different if **either** the metric name differs **or** any label key/value differs (including “label missing” vs “label present”).

**OTLP mapping note**
- This TSDB treats the PromQL labelset as the canonical Series identity: OTLP resource/scope/datapoint attributes become labels (after PromQL name normalization), and the OTLP metric name becomes `__name__` (also normalized). A stable `series_id` is derived from that canonicalized labelset.

---

## 0) Key ideas and constraints

1. **SSD friendly**: prefer sequential appends and reads in *tens of KB or more*; avoid tiny random writes.
2. **Shared-nothing per shard**: each shard owns its files and memory, so hot path has no cross-shard locks.
3. **OTLP-native data types**:
   - Current implementation persists **Gauge/Sum number datapoints** as FLOAT chunks (f64).
   - Histogram, ExponentialHistogram, and Summary datapoints are persisted as native typed chunks with first-pass PromQL scalar projections.
4. **Out-of-order is normal**: collector retries, batching, and failover cause OOO points; support bounded lateness and an OOO lane.
5. **Single-writer stream assumption**: you can maintain per-stream state (temporality normalization, reset detection) inside a shard, but must tolerate duplicates/replays.
6. **Greptime-inspired improvements** (adopted here):
   - **Separate index artifacts from data artifacts** so you can evolve / rebuild indexes without rewriting chunk data.
   - Use an **inverted index with an FST + bitmaps** to accelerate high-cardinality label lookups and regex.
   - Treat indexes as a set of **blobs in a container file** (a “Puffin-like” bundle) to keep segment layouts stable and extensible.

**Binary encoding conventions**
- All fixed-width integer fields are little-endian.
- All fixed-width floating point fields are raw IEEE-754 binary64 bit patterns stored little-endian. NaN/Inf values round-trip as bits; stale NaN is a distinct sentinel.
- Unsigned varints are unsigned LEB128.
- Signed varints are zigzag-encoded signed integers stored as unsigned LEB128.
- PromQL label string projections for floating values (`le`, `quantile`) use the Go `strconv.FormatFloat(v, 'g', -1, 64)` spelling. Positive infinity is always `+Inf`.

---

## 1) Terminology

- **Tenant**: multi-tenant boundary.
- **Stream**: OTLP metric stream identity (metric identity + resource/scope attrs + datapoint attrs).
- **Series**: queryable time series, identified by a PromQL labelset (including `__name__`).
- **SeriesID (`series_id`)**: a stable `u64` identifier for a Series within a shard (typically a fingerprint of the canonicalized PromQL labelset). Used to identify the “same series” across head and multiple segments; if derived from a hash/fingerprint, segment readers must verify against the stored labelset to guard against collisions.
- **SeriesRef (`series_ref`)**: a **segment-local** dense `u32` identifier for a series, assigned when sealing a segment. All per-segment postings/bitmaps and chunk addressing use `series_ref` (not `series_id`) so indexes stay compact and can use fast 32-bit roaring containers and delta-encoded lists.
- **HeadSeriesRef (`head_series_ref`)**: a **head-local** dense `u32` identifier for a series, used for head postings/bitmaps. The head maintains a mapping `series_id <-> head_series_ref` so it can use compact u32 postings while still merging results across head and segments by stable `series_id`.
- **SymbolID (`symbol_id`)**: a `u32` identifier for an interned string (metric name, label key, label value) used inside `symbols.bin`/`series.bin`/indexes to avoid repeating strings; **symbol ids are segment-scoped** (each sealed segment has its own dictionary), and shard/head intern ids must be remapped when sealing a segment.
- **Shard**: shared-nothing unit of ownership and persistence.
- **Head**: in-memory windowed buffer (duration configurable; current default 1h).
- **WAL**: write-ahead log for durability + offset checkpointing.
- **Segment**: SSD-resident immutable time-partitioned unit (short retention, write-optimized).
- **OOO**: out-of-order samples handled in a separate lane and merged at read/compaction time.

---

**Implementation status (current code)**
- Head is a **windowed, compressed buffer** for segment sealing; it is not yet a queryable head store (no head postings/bitmaps or `head_series_ref` mapping).
- Head window duration is tied to `segment_duration` (default 1h) and buffers per-series samples using **delta-encoded timestamps** and **Gorilla XOR** values; blocks carry min/max timestamps for range filtering.
- Segment writing is **single-writer per ingestion worker/shard** to avoid cross-thread coordination.
- FLOAT chunks and first-pass native Histogram/ExponentialHistogram/Summary chunks are persisted. Typed chunks now carry per-sample start time, OTLP datapoint flags, temporality, and reset hints in their current value payloads, plus compact scalar projection lanes for `_count`/`_sum`. Query-time PromQL projections include classic Histogram buckets, ExponentialHistogram buckets for deterministic query-configured boundaries, and Summary quantiles; the fully separated common-lane byte layout and exemplar sidecars described later remain forward-looking.

---

## 2) Storage directory layout

Everything is namespaced by `{tenant}/{shard_id}`.

```
data/
  tenants/
    <tenant_id>/
      shards/
        <shard_id>/
          wal/
            wal-000000.log
            wal-000001.log
            checkpoint.meta
          head/
            head.snapshot (optional)
          segments/
            seg-<start_ms>-<end_ms>-<ulid>/
              meta.json
              symbols.bin
              series.bin
              chunks.bin
              ooo_chunks.bin           (optional)
              chunk_index.bin
              indexes.puffin           (index container; extensible)
              footer.bin
            ...
          manifest/
            CURRENT
            MANIFEST-000001
```

**Immutability rule**  
Once a segment is sealed, none of its files are modified.

---

## 3) Sharding & partitioning

### 3.1 Shard key (blast radius control)
A “partition label” is configured per tenant to keep k8s label explosions local:

Default candidates:
- `k8s.cluster.name`
- `k8s.namespace.name`
- `service.name`

Note: these are OTLP attribute keys (often containing `.`). They are normalized to PromQL label names for querying per §4.2, but shard routing should use the attribute value from the original OTLP key to avoid ambiguity.

Routing key:
```
ShardKey = hash(tenant_id, partition_label_value, series_fingerprint)
```

### 3.2 Ownership and movement
- Shards consume Kafka partitions (or other transport partitions).
- Offsets are checkpointed **in shard-local WAL state** (not only in Kafka).
- If partitions move between nodes, you need either:
  - sticky assignment (preferred for v1), or
  - checkpoint replication to a shared store (S3/KV) so the new owner can resume exactly.

---

## 4) Write path (OTLP -> head -> segment)

### 4.1 Pipeline overview
1. Decode OTLP record from transport
2. Normalize to internal series identity:
   - labels (resource + datapoint attrs)
   - type: Gauge / Sum / Hist / ExpHist
   - temporality, monotonicity
3. Intern strings: symbols (metric names, label keys/vals) -> `u32` ids
4. Append to WAL (durable, checksummed)
5. Apply to Head buffer (per-series windowed buffer with delta+Gorilla encoding and block indexing)
6. Periodically flush sealed segments; rotate WAL

**String interning scope**  
Interning during ingestion is for speed/memory in the shard/head, but **persisted symbol ids are per-segment**: when sealing a segment, build a segment-local `symbols.bin` and write `series.bin` + index blobs using that segment’s symbol ids. Persisted `symbol_id`s are the ordinal positions of the sorted segment dictionary, not the shard/head intern ids. The seal path must remap every symbol reference in `series.bin`, postings, label-value time ranges, and other index blobs to the sorted segment ids. This keeps segments standalone and movable, avoids global/distributed symbol coordination, and makes replay output independent of first-seen string interning order.

### 4.2 PromQL name normalization (required)

PromQL has strict identifier rules:
- **Metric names** must match: `[A-Za-z_:][A-Za-z0-9_:]*`
- **Label names** must match: `[A-Za-z_][A-Za-z0-9_]*`

OTLP metric names and attribute keys (e.g., `service.name`, `k8s.cluster.name`) frequently violate these rules. The storage layer must therefore define a **deterministic normalization** that is applied at ingestion and is part of the canonical Series identity.

#### 4.2.1 Normalize metric names (value of `__name__`)
Algorithm (no lowercasing; deterministic):
1. Replace any character not in `[A-Za-z0-9_:]` with `_`.
2. If the first character is not in `[A-Za-z_:]`, prefix the name with `_`.
3. If the result is empty, use `_`.

If two distinct OTLP metric names normalize to the same PromQL metric name, **disambiguate** by appending a stable suffix:
- `normalized = base + "_x" + hex(xxhash64(original_bytes))`

This preserves PromQL legality and guarantees that different OTLP names never alias into one PromQL metric.

#### 4.2.2 Normalize label names (attribute keys)
Algorithm:
1. Replace any character not in `[A-Za-z0-9_]` with `_`.
2. If the first character is not in `[A-Za-z_]`, prefix with `_`.
3. If the normalized name equals `__name__` or starts with `__`, prefix with `otel_` (Prometheus reserves `__*` for internal use).
4. Disambiguate collisions using the same stable suffix rule as metric names:
   - `normalized = base + "_x" + hex(xxhash64(original_key_bytes))`

Label values are stored as strings after deterministic OTLP `AnyValue` canonicalization:
- string: the UTF-8 string as-is
- bool: `true` or `false`
- integer: base-10 signed decimal
- double: Go `strconv.FormatFloat(v, 'g', -1, 64)`, preserving NaN/Inf spellings consistently
- bytes: base64url without padding
- array: JSON array with canonical element encoding, no insignificant whitespace, and nested doubles emitted with the same Go `strconv.FormatFloat(v, 'g', -1, 64)` spelling
- key/value list: JSON object with keys sorted by raw key bytes, no insignificant whitespace, and values recursively canonicalized with the same nested-double rule
- empty / unknown value: empty string

This canonicalization is part of the series identity and must be stable across ingestion, replay, and compaction.

#### 4.2.3 Canonical Series identity
The canonical labelset used for:
- computing `series_id`
- sorting/deduping labels inside `series.bin`
- query matching

is:
- `__name__=<normalized_metric_name>` plus all normalized labels,
- sorted by label name, and
- with collision-disambiguated label keys as described above.

Query note: the storage/index layer matches on normalized metric/label names. Any API layer that accepts OTLP-style names must apply the same normalization before building selector matchers.

### 4.3 Durability boundary
The durable “ingested” point is: **present in WAL + covered by a CHECKPOINT record** (see §7).  
Everything else is rebuildable.

Persistence watermark (used for head eviction) should advance on WAL checkpoints
or chunk/segment flush markers, not only full segment seal.

### 4.4 Clock and watermarks (see docs/superpowers/specs/clock.md)

- Ingest watermark (per partition): max accepted event time; drives read horizon
  and segment sealing.
- Persistence watermark (per partition): max durably persisted event time (WAL
  checkpoint or flush marker); drives head eviction.
- Read horizon uses active partitions; synthetic watermarks affect reads only.

### 4.5 CaptureRecord for safe replay

Captured source traffic is stored as records around the raw OTLP payload:

```
CaptureRecord {
  sequence: u64,             // capture-local order
  topic: string,
  partition: i32,
  offset: i64,
  source_timestamp_ms: i64,  // diagnostic metadata only; never event/policy time
  captured_at_ms: i64,       // trusted policy clock; never event-time fallback
  payload: bytes,            // raw OTLP ExportMetricsServiceRequest
}
```

`captured_at_ms` is required. It is the trusted replay anchor used by event-time
validation, future-skew policy, lag diagnostics, and any replay clock that wants
to reproduce the original ingestion safety decision.

The source/Kafka timestamp is diagnostic metadata only. It must not be used as
event time or trusted policy time because a producer or broker timestamp can be
wrong or malicious. OTLP datapoints require their own event timestamps. The
exact missing representation, `time_unix_nano == 0`, becomes
`MissingTimestamp` before age/lead policy evaluation and is never replaced by
`source_timestamp_ms`, `captured_at_ms`, or current wall time.

Segment placement remains event-time based: datapoint timestamps decide the
head window, segment range, compression deltas, and query time range. Capture
time must not decide where samples are stored.

Replay from capture should preserve per-partition record order and evaluate
event-time policy using `captured_at_ms` (or future explicit capture watermark
records). Replaying a file later with the current wall clock must not make
previously future-dated datapoints appear safe.

An offline head-topology experiment may derive a new capture by walking the
source in global sequence order and assigning each complete record to another
partition. Such a transform must leave the topic, raw payload bytes,
`source_timestamp_ms`, and `captured_at_ms` unchanged; emit a new dense global
sequence in that same order; and assign a new zero-based, monotonically
increasing offset within each destination partition. The derived partition and
offset are experimental transport metadata, not the source transport identity.
The transform must save its exact mapping rule and physical stream
fingerprints. It must also independently hash the input and reopened output
with one shared-domain canonical content encoding over global ordinal, topic,
`source_timestamp_ms`, `captured_at_ms`, and raw payload bytes, excluding the
deliberately changed partition and offset, and require those two hashes to be
equal. Reopened output must still be verified record-for-record before it can
be used as replay evidence. The transform must never decode and re-encode the
OTLP payload.

Exact segment folder replay requires deterministic segment IDs. The storage
writer must support a `SegmentIdProvider`:

- Production/default mode uses random ULIDs for low collision risk.
- Replay/test mode may use a deterministic provider seeded from the replay
  context. With the same capture, writer config, segment duration, policy, seed,
  and record order, it must produce the same `seg-<start_ms>-<end_ms>-<ulid>/`
  names across runs.

This guarantees folder-name repeatability. Byte-for-byte segment equality also
requires deterministic series/symbol/chunk ordering and is a separate contract.

---

## 5) Head buffer (windowed in-memory)

The current head is a **minimal in-memory buffer** used to batch samples before sealing a segment. It is not yet a queryable head store.

Per shard:
- Series identity comes from the labelset interner (`SeriesRef`); there is no stable `series_id`/`head_series_ref` mapping yet.
- The head holds a **single active time window** (`start_ms`, `end_ms`);
  samples with `event_ms >= end_ms` advance the window and flush it to the
  segment writer. Samples older than the active window route to the OOO/late
  path (§14) and must not force head rotation.
- If head retention/eviction is enabled, eviction is anchored to the
  persistence watermark (WAL checkpoint or flush marker), not the ingest
  watermark.
- Per series, samples are stored in **encoded blocks**:
  - timestamps are **delta-encoded** from the window start (`t0_ms`) using varint
  - values use **Gorilla XOR** encoding for f64
  - each block tracks `min_ts`/`max_ts` for range filtering
  - block size defaults to **256 samples** (configurable)
  - implementations may stage a bounded number of short numeric-series samples
    inline as exact `(timestamp, value bits)` pairs before allocating a block
    encoder. Promotion must preserve append order, configured block boundaries,
    exact float bits, and the sealed segment bytes. Disabling this staging is a
    diagnostic/performance control only and must not change query or storage
    semantics.
  - rejecting a sample must not partially append either its timestamp or value;
    the block's streams, sample count, and time extrema remain aligned and
    unchanged. If that sample would start the next active window, its first
    encode must succeed before the completed window is rotated out. A public
    multi-sample head call preserves its accepted prefix on a later error; any
    windows rotated by that prefix remain retained by the head and are exposed
    to queries, a later successful recording call, or `drain_windows` rather
    than being stranded in the failed call's return buffer.
  - after event-time acceptance, Histogram, ExponentialHistogram, and Summary
    shapes are validated before label interning, reset tracking, or head
    mutation. A malformed typed datapoint is counted as time-policy accepted
    but not recorded, increments the explicit invalid-typed-value storage
    counter, and does not abort valid sibling datapoints in the OTLP message.
    On a clean stored-head run with zero labelset errors or series-kind
    mismatches, missing-number and invalid-typed counters together explain
    accepted datapoints that were not recorded. Formal replay gates require
    that exact reconciliation and therefore fail closed on any additional
    unaccounted storage rejection.
  - the current per-window series lookup and long-lived per-series
    last-timestamp lookup may promote bounded `SeriesRef` pages from a sparse
    hash representation to direct storage. Both structures must retain sparse
    fallbacks for strided pages and refs above the bounded page directory.
    The two runtime plain/adaptive controls are independently selectable
    diagnostic comparators; a joint plain-versus-adaptive observation cannot
    attribute an effect to either table. Accepted samples, OOO decisions,
    rotations, sealed bytes, and query results must not change. Periodic
    structural telemetry snapshots are O(1): maintained
    counters avoid scanning table keys, pages, or occupancy maps while a
    replay is timed. Reports expose dense/direct and sparse coverage separately
    per source partition. `in_order_rotations` counts only a completed active
    window returned because a later accepted sample advanced the head window;
    it does not count the still-active in-order window or any OOO window drained
    at shutdown. Window/lane totals remain separate structural counters. When
    multiple partition heads drain into one segment writer, completed windows
    are ordered by `(start_ms, end_ms, partition, lane)` before the writer is
    touched; hash-map seed and partition discovery order must not affect segment
    boundaries or deterministic IDs. For an identical range and partition, the
    OOO lane retains precedence before the active in-order lane.

Window duration is currently tied to `segment_duration` (default 1h). Late
samples that fall into older windows are routed to the OOO/backfill path (§14)
and should not create tiny segments by forcing window rotation.

---

## 6) Segments (SSD “near” store)

### 6.1 Segment time range
Default segment duration: **1 hour** of event time.

Directory name:
`seg-<start_ms>-<end_ms>-<ulid>/`

**SSD note (file-count vs flush granularity)**  
`1h` segments are the default balance for head memory and file counts. Shorter
segments improve freshness but create more files and index rebuild work:
- for `15m` segments: segments/day/shard = `24h / 15m = 96`
- files/segment in this spec ≈ 7–9 (plus directory entries)

If you expect many shards *or* multi-day SSD retention, consider one of:
- adjust `segment_duration` (e.g., 30m–2h)
- if you keep `segment_duration=15m`, group/pack adjacent segments into a larger “block-in-progress” container and only seal/index the larger unit (Prometheus-like)
- if you keep many small sealed segments, add a shard-local “segment packer” that merges adjacent segments into larger segments to cap file counts and startup/index costs

**High-cardinality note**  
If a shard can accumulate millions of series in hours (as observed in production-like OTLP workloads), treat one of the “block-in-progress” / “segment packer” options as mandatory to avoid spending most of the system’s time and I/O sealing tiny segments and rebuilding indexes.

**Operational note (FD / metadata scaling)**
Implementations must not keep every segment mapped/open. Use the aggregate
metadata/FD governor and the manifest's time-range inventory so only overlapping
segments are touched.

Sealing trigger (policy): use ingest watermark progress and lateness tolerance
to decide when a segment can be sealed. See `docs/spec/clock.md`.

### 6.2 Sealing protocol (crash-safe)
Segments are built in a temp dir and published atomically:

1. Create `seg-.../.tmp/`
2. Write: `symbols.bin`, `series.bin`, `chunks.bin`, `ooo_chunks.bin` (if needed), `chunk_index.bin`, `indexes.puffin`
3. `fsync()` files (policy configurable; see §10)
4. Write `footer.bin` last (checksums, sizes, schema)
5. `fsync()` directory
6. Atomic rename `.tmp` -> final
7. Append to `MANIFEST-*` and update `CURRENT`

On crash:
- temp dirs are ignored / cleaned
- only manifest-published segments are queryable

A final head or segment-writer flush failure is a fatal ingestion result. It
must propagate to the caller and process exit; logging the failure and exiting
successfully would falsely present an incomplete corpus as a completed replay.

Current implementation note: `SegmentWriter` appends a `SEGMENT_SEALED` record
under `segments_dir/manifest/` after the segment directory is atomically
published, then updates `CURRENT`. `chronoxide-query` prefers this manifest
inventory when present and falls back to scanning `seg-*` directories only for
older/manual segment directories without a manifest.

### 6.3 Segment files

#### Data files
- `chunks.bin`: in-order chunk frames (append-only)
- `ooo_chunks.bin`: out-of-order chunk frames (append-only, optional)

#### Metadata
- `symbols.bin`: **segment-local** sorted string dictionary (metric names, label
  keys/vals) used by `series.bin` and `indexes.puffin` within this segment.
  `symbol_id == sorted_dictionary_ordinal`. Version 3 stores deterministic,
  independently checksummed pages behind a compact immutable root. Query-time
  resolution binary-searches complete first/last-string fences, then reads and
  validates at most one candidate page.
- `series.bin`: SeriesRef -> SeriesID + labelset + type metadata; v2 also stores this series' byte range inside `chunk_index.bin` so selective queries can jump directly to the relevant chunk-index span
- `chunk_index.bin`: per-series time-ordered entries -> **(file, offset, length)** of each chunk within chunk files (so readers can pread only required chunks)

Current sealed segment query map:

```
                         segment inventory / manifest
                                      |
                                      v
                               +-----------+
                               | meta.json |
                               | time span |
                               +-----------+
                                      |
                         query time overlaps segment?
                                      |
                                      v
          +-------------+      +----------------+      +------------------+
query --> | symbols.bin | <--> | indexes.puffin | ---> | series_ref set   |
strings   | string IDs  |      | routing,       |      | from matchers    |
          +-------------+      | postings,      |      +------------------+
                 ^             | FSTs, metric   |               |
                 |             | series ranges  |               v
                 |             +----------------+      +------------------+
                 |                                     | series.bin       |
                 +-----------------------------------> | labels, kind,    |
                                                       | series_id,       |
                                                       | chunk-index span |
                                                       +------------------+
                                                                  |
                                                                  v
                                                       +------------------+
                                                       | chunk_index.bin  |
                                                       | time ranges,     |
                                                       | file, offset,    |
                                                       | length, scalar   |
                                                       | lane ranges      |
                                                       +------------------+
                                                        |              |
                                                        v              v
                                                +------------+  +----------------+
                                                | chunks.bin |  | ooo_chunks.bin |
                                                | in-order   |  | OOO lane       |
                                                | payloads   |  | payloads       |
                                                +------------+  +----------------+

footer.bin validates tracked file sizes/checksums for corruption detection.
```

The split is intentional in the current design: `indexes.puffin` answers
"which series may match?", `series.bin` answers "what is this series and where
is its chunk-index span?", and `chunk_index.bin` answers "which chunk byte
ranges overlap this time query?". Keeping those responsibilities separate avoids
dragging chunk directories into label-only scans and avoids scanning series rows
for high-cardinality selector planning.

Example single-query walkthrough:

```
Incoming PromQL selector:

  http_request_duration_seconds_count{route="/api"} @ [start_ms..end_ms]

0. PromQL parse/lower
   - normalize metric/label names
   - recognize `_count` as a typed scalar projection candidate
   - native metric candidate: `http_request_duration_seconds`
   - required projection: Count

1. Segment inventory and coarse time pruning

   manifest/CURRENT + MANIFEST-*  -> candidate seg-* directories
   seg-*/meta.json                -> keep segments whose [start_ms,end_ms] overlap

2. Root binding and symbol resolution

   symbols.bin                    -> "__name__", "http_request_duration_seconds",
                                     "route", "/api" become segment-local symbol_ids
   indexes.puffin root            -> bind index series/symbol counts to the same
                                     validated segment generation

3. Selector planning

   indexes.puffin postings/FSTs   -> integrity-checked series_refs matching both
                                     __name__ and route="/api"
   intersection                   -> candidate series_ref set

   Routing and metric-range summaries may replace or prefilter these steps
   only while the query session holds their opaque same-generation authority
   minted by complete semantic validation. Without that capability, absence,
   time, and kind facts in those summaries cannot remove a segment or series.

4. Series materialization

   series.bin                     -> for each candidate series_ref:
                                     - stable series_id
                                     - stored labelset for result labels
                                     - kind_mask/type metadata
                                     - chunk_index_offset/chunk_index_len
   symbols.bin                    -> resolve label symbols back to strings when needed

5. Chunk selection

   chunk_index.bin                -> read each candidate series' exact index span:
                                     - filter chunk entries by query time
                                     - filter by required source kind
                                     - collect file_id, offset, length
                                     - for Count/Sum, prefer scalar_lane_offset/len

6. Payload I/O

   chunks.bin                     -> coalesced reads for in-order chunk payload bytes
                                     Count/Sum typed projection reads only
                                     ChunkHeader + TypedScalarLane when available
   ooo_chunks.bin                 -> same, only if chunk_index entries point to OOO lane

7. Decode/project/merge

   ChunkHeader                    -> validate kind, encoding, time range, CRC
   TypedScalarLane                -> decode count samples without full histogram buckets
   query reader                   -> project output metric name back to
                                     `http_request_duration_seconds_count`
   merge layer                    -> merge chunks, OOO lane, other segments, and head;
                                     sort/dedupe duplicate timestamps

Schema-6 open reads and validates the small `footer.bin` itself so unsupported
segment schemas are rejected before metadata lookup. Re-reading and hashing
every tracked file against the footer remains opt-in through `open_validated`
or explicit full validation and is excluded from timed query benchmarks.
```

### 6.3.1 `symbols.bin` v3

All fixed-width integers are little-endian. The physical order is exact and
contains no unaccounted gaps, padding, or trailing bytes:

```text
SymbolsHeaderV3                    // 80 bytes
SymbolPageDescriptorV3[page_count] // 48 bytes each
FenceBytes                         // complete first/last strings
SymbolPageV1[page_count]           // variable length
EOF
```

The 80-byte root header is:

```text
SymbolsHeaderV3:
  u32 magic              // 'SYMB'
  u16 version            // 3
  u16 flags              // 0
  u32 header_len         // 80
  u32 descriptor_len     // 48
  u32 symbol_count
  u32 page_count
  u64 directory_offset   // 80
  u64 directory_len      // page_count * 48
  u64 fence_offset       // directory_offset + directory_len
  u64 fence_len
  u64 pages_offset       // fence_offset + fence_len
  u64 file_len           // exact physical file length
  u32 root_crc32c         // CRC of [0, pages_offset), this field zeroed
  u32 reserved0          // 0
```

Each 48-byte descriptor is:

```text
SymbolPageDescriptorV3:
  u32 first_symbol_id
  u32 symbol_count       // non-zero
  u64 page_offset        // absolute file offset
  u32 page_len           // exact encoded page length
  u32 page_crc32c        // CRC over the complete page
  u32 first_fence_offset // relative to fence_offset
  u32 first_fence_len
  u32 last_fence_offset  // relative to fence_offset
  u32 last_fence_len
  u32 string_bytes_len
  u32 reserved0          // 0
```

Descriptor ordinal is the page index. Symbol-ID ranges and physical page
ranges are contiguous, begin at symbol ID zero and `pages_offset`, and end
exactly at `symbol_count` and `file_len`. Fence locators describe the canonical
fence region in descriptor order: the complete first symbol followed by the
complete last symbol for every page. A singleton therefore stores the same
fence bytes twice. Fences are valid UTF-8 and compare by raw bytes; each page
satisfies `first_fence == last_fence` exactly when `symbol_count == 1`, and
otherwise satisfies `first_fence < last_fence`; adjacent pages satisfy
`previous.last_fence < next.first_fence`. A singleton's string byte length is
its fence length. For two symbols, the string byte length is exactly the sum of
the two fence lengths. For larger pages, it is at least the two fence lengths
plus one byte for every interior symbol.

Each page begins with this exact 32-byte header:

```text
SymbolPageHeaderV1:
  u32 magic           // 'SYPG'
  u16 version         // 1
  u16 flags           // 0
  u32 page_index
  u32 first_symbol_id
  u32 symbol_count    // non-zero
  u32 offsets_len     // 4 * (symbol_count + 1)
  u32 strings_len     // equals descriptor.string_bytes_len
  u32 reserved0       // 0

  u32 local_offsets[symbol_count + 1]
  u8  strings[strings_len]
```

Local offsets are relative to `strings`. The first is zero, the last equals
`strings_len`, and they are non-decreasing. Every sliced symbol is valid UTF-8;
symbols are strictly increasing and unique by raw bytes. The first and last
symbols equal the root fences. The exact page length is
`32 + offsets_len + strings_len`, and the descriptor CRC covers those bytes.

Page construction is deterministic. Starting from a strictly sorted, unique
dictionary, the writer greedily packs the maximal consecutive symbols whose
exact encoded page length is at most 32,768 bytes. If the first symbol alone
exceeds 32,768 bytes, it forms one oversized singleton page, up to the schema-6
operational maximum page length of 16,777,216 bytes. A larger page is rejected
by writers and readers. Pages contain no padding. For `n` symbols containing
`s` string bytes, the exact size is
`32 + 4 * (n + 1) + s`.

Readers validate the header, directory, fences, canonical contiguous ranges,
stored physical file length, checked arithmetic, and root CRC before following
a descriptor. A touched page is CRC-checked before parsing, then its complete
header, lengths, offsets, UTF-8, ordering, and fence agreement are validated.
Schema 6 imposes a 67,108,864-byte operational maximum on the complete root
`[0, pages_offset)`; writers must not emit and readers must not allocate a
larger root. Readers reject `page_count > symbol_count` from the fixed header
before sizing or reading the variable root, and reject a descriptor page length
above 16,777,216 bytes before allocating a page buffer.
Touched corruption is an error and must never become a missing symbol, cache
miss, pruning decision, or empty result. An explicit full-validation pass reads
every page and also validates actual cross-page ordering.

`string -> symbol_id` and `symbol_id -> string` are fallible operations. They
binary-search the immutable root and touch at most one candidate page. Returned
string views retain ownership of their validated page. Batch resolution groups
work by page. Validated pages may be retained only in an explicitly
byte-bounded cache; a zero budget disables retention. Structural corruption is
sticky after its first detection even if valid pages are later evicted. Readers
use immutable positional I/O rather than a shared seek cursor.

Read counters belong to each query/session clone, but retained resources belong
to the shared reader state. Store-level reporting therefore deduplicates reader
state before summing retained open files, decoded root charge, eager-dictionary
charge, validated-page charge, and configured page-cache capacity. Decoded root
charge includes the retained root object, decoded descriptor array, and complete
fence bytes; page charge follows the fixed allocation/bookkeeping rule in the
paged-symbol design. Successful lookup/resolution results also report logical
value count and UTF-8 bytes, so physical symbol-read amplification has a named
denominator. Missing symbols and validation-only reads contribute no logical
returned bytes.

The completed standalone schema-6 prototype configured 256 KiB independently
for every retained paged reader. Deduplicated reporting prevented clones from
being double-counted, but it was not a memory governor. The homogeneous
schema-6/schema-7 A/B path and schema-8 reader instead route symbol roots and
pages through the same aggregate governor and runtime zero-retention setting
required by schemas 7 and 8; the per-reader prototype cache is historical
behavior, not a valid A/B backend.

The complete construction, cache contract, query integration, and validation
matrix are recorded in
[the paged-symbol design](2026-07-13-storage-vnext-paged-symbols-design.md).

#### Index container (Greptime-inspired)
- `indexes.puffin`: a container holding multiple index blobs:
  - segment routing metadata for capability-gated early equality/time pruning
  - postings index
  - label-value FSTs
  - label-value time ranges
  - capability-gated metric-series ranges
  - bitmap dictionaries / roaring containers
  - optional bloom filters and min/max stats per series

#### Integrity
- `footer.bin`: per-file sizes + checksums + segment schema version
- `meta.json`: human-readable summary

Footer schemas 6, 7, and 8 contain exactly the seven tracked-file entries in this
canonical order: `meta.json`, `symbols.bin`, `series.bin`, `chunks.bin`,
`ooo_chunks.bin`, `chunk_index.bin`, and `indexes.puffin`. The payload-reserved
word and every entry-reserved word are zero. Ordinary open verifies the
footer CRC and rejects a different count, order, duplicate, missing or unknown
entry, non-zero reserved field, or trailing byte before opening metadata roots.
Complete tracked-file checksum validation remains the explicit validation pass.
That pass binds every tracked path to one registered immutable generation before
hashing, streams each checksum through one aggregate-governed 1 MiB buffer, and
rechecks the opened descriptor's identity and exact length after its final read.
It parses `meta.json` through the same registered generation and requires its
segment ID and time bounds to match the canonical directory identity. Readers
reject a `meta.json` larger than 65,536 bytes before registration; this is an
operational allocation bound, not a field encoded into the footer.

Segment footer schema version `6` requires `symbols.bin` version `3` and retains
segment index-container version `7`, `series.bin` version `2`, and
`chunk_index.bin` version `1`. Routing metadata and required
metric-series ranges remain inside `indexes.puffin`; there is no separate
`routing_index.bin`.

Schema-6 readers reject every other segment schema during a lightweight footer
schema preflight, even when complete footer checksum validation is disabled.
They also reject every symbol version other than 3. Production readers have no
compatibility fallback and accept no mixed-schema manifest: old corpora must be
regenerated by deterministic replay into a new output root. Before creating or
publishing any segment, a writer opening an existing root reads the active
manifest inventory and requires every live segment's fixed-size, CRC-checked
footer to match its configured schema exactly. A mismatched, missing, or
malformed live footer fails startup before the root is mutated; complete
tracked-file checksum validation remains a separate operation. Same-schema
restart and append are allowed. A root containing `seg-*` directories but no
authoritative `manifest/CURRENT` is not appendable: repair its manifest or
replay it into a fresh output root so the first new manifest cannot silently
hide existing segments. Empty and nonexistent output roots remain valid. The
read-only `chronoxide-query --storage-layout schema6-ab` benchmark path may open one
homogeneous schema-6 corpus containing `symbols.bin` v3, `series.bin` v2,
`chunk_index.bin` v1, and `indexes.puffin` v7 so one identical query binary
can compare layouts. That exception requires explicit complete footer checksum
validation of the homogeneous corpus outside timed queries and continued
binding to the same opened file identities. Its v7 exact-postings, FST, and
label-value time-range payloads are not locally integrity-checked, so the adapter
must retain complete final label-predicate verification and must not use a v7
time-range summary to remove a candidate. Routing and metric-range summaries
remain non-authoritative without their separate semantic capabilities. The
adapter uses the same aggregate metadata and file-descriptor governor as schema
7, never accepts a mixed manifest, and does not alter writer output or
production/API version policy.

### 6.3.2 Schema-7 prior-format boundary

Schema 7 remains a strict, homogeneous prior-format comparator. Its writer is
selected explicitly with `storage_schema = "schema7"`; readers use the exact
`schema7` policy and never probe or fall back per segment. The schema-neutral
metadata facade, aggregate governor, validated schema-6 A/B adapter, and
independent `chronoxide-query --verify-readbacks` decoder continue to cover
its contract. No changed byte layout may be published under footer schema 6 or
7. Top-level PromQL range
queries admit schema-7 and schema-8 facade payloads to the query-local decoded
scalar cache only for in-order dedicated Count/Sum lanes. Logical request and
limit accounting occurs before cache hits are removed from physical I/O. A
miss integrity-checks the schema-7 indexed prefix and validates the complete
scalar lane before admission; touched corruption is returned and never becomes
a cache miss or bypass. OOO lanes and layouts without a dedicated scalar lane
remain explicit unsupported bypasses. The validated schema-6 comparator
retains its existing cache path.

The schema-7 prior-format contract uses `symbols.bin` v3,
integrity-checked `indexes.puffin` v8, `series.bin` v3, and overflow-only
`chunk_index.bin` v2. Index-container v8 changes only the container/root and
directory metadata required to integrity-check touched exact-postings, label FST,
and label-value time-range payloads; the routing-v2, metric-series-ranges-v1,
postings-body, FST-body, and range-body encodings remain unchanged. Its exact
headers, field offsets, control bits, hot and cold page descriptors, inline
locator, overflow blob, index-payload and chunk-header integrity checks,
validation order, deterministic writer, aggregate metadata/FD governor, strict
version boundary, and acceptance gate are incorporated normatively from
[the focused schema-7 design](2026-07-13-storage-schema7-inline-series-design.md).
An implementation change to any of those bytes or invariants must update both
documents before code is accepted.

Schema 7 retains the v2 keyset/value-code logical byte stream but adds
independent fixed-range CRC descriptors so every touched cold label byte is
integrity-checked without eager full-file validation. Per-sample typed metadata
inside native values and scalar lanes remains authoritative and unchanged.
Schemas 7 and 8 define no series-level metadata sidecar; every reserved
metadata word is zero. Additional uniform series facts that are not recoverable
from integrity-checked chunks require a future explicit series/blob/segment
version.

### 6.3.3 Schema-8 production adaptive-postings boundary

Footer schema 8 retains `symbols.bin` v3, `series.bin` v3,
`chunk_index.bin` v2, and every chunk byte from schema 7. It requires
`indexes.puffin` v9 and changes only the exact-postings payload encoding and
the routing metadata's corresponding encoded byte lengths. Its exact byte
layout, deterministic codec rule, corruption requirements, and experiment gate
are specified in
[the focused schema-8 design](2026-07-15-storage-schema8-adaptive-postings-design.md)
and incorporated normatively here. Schema 8 is the default writer, sealed-store
reader, `chronoxide-query` policy, and HTTP API policy. Schema 7 continues to
require v8 raw postings; neither footer version accepts the other's
index-container version.

### 6.4 `series.bin` formats

`series.bin` is optimized for:
- O(1) access to a series’ labelset given `series_ref`
- fast iteration for metadata discovery / label materialization

In low-cardinality environments, a flat list of `(key_sym, val_sym)` pairs per series is sufficient. In high-cardinality environments (millions of series, ~25–30 labels/series), **keyset/value-code encoding** is recommended to keep `series.bin` and index build costs under control.

All integer fields are little-endian.

#### 6.4.1 `series.bin` v1 (flat label pairs; simple, but larger)

```
SeriesBinHeader:
  u32 magic     // 'SERI'
  u16 version   // 1
  u16 flags
  u32 num_series

  // Directory: entry offsets by SeriesRef (dense 0..num_series-1).
  // Offsets are relative to the start of the file.
  u64 entry_offsets[num_series + 1]
  // entries...
```

Each `series_ref` entry is variable-length:
```
SeriesEntryV1:
  u64 series_id
  u8  kind_mask        // bitmask: FLOAT/HIST/EXPHIST/SUMMARY present in this segment for this series
  u8  reserved0
  u16 reserved1
  u32 meta_len         // 0 if none
  u32 num_labels
  u8  meta[meta_len]   // optional TLV metadata (temporality, monotonicity, reset policy, original OTLP type)
  LabelPair labels[num_labels]  // sorted by key_sym

LabelPair:
  u32 key_sym
  u32 val_sym
```

`series_ref` is not stored in the entry; it is the index into `entry_offsets`.

#### 6.4.1.1 Sealed `series_ref` assignment order

When sealing a segment, assign dense segment-local `series_ref`s in
**metric-query order**:

1. normalized metric name (`__name__` value)
2. persisted kind/type mask
3. canonical full labelset
4. stable `series_id`
5. previous in-memory local ref as a final deterministic tie-breaker

This keeps series for the same PromQL metric physically adjacent in
`series.bin` and `chunk_index.bin`, so common metric-name selectors can be
served with fewer scattered metadata reads. This is a physical locality rule
only; stable identity remains `series_id` plus labelset verification.

The final `series_ref` mapping must be applied consistently to:
- `series.bin` table order
- `chunk_index.bin` per-series offset order
- all postings/bitmap index blobs in `indexes.puffin`
- `ChunkHeader.series_ref` inside chunk payloads

If a writer appends chunk payloads before the final seal-time order is known,
it must patch each affected `ChunkHeader.series_ref` and the enclosing frame
CRC before publishing the immutable segment.

#### 6.4.2 `series.bin` v2 (keyset/value-code encoding; recommended)

Motivation:
- Many series share the same *set of label keys* (e.g., k8s labels); only the *values* vary.
- Storing `LabelPair{key_sym,val_sym}` per series repeats the same `key_sym`s millions of times.
  - Empirical note: in a production-like 10M-message sample, only a small number of keys required 4-byte value codes (cardinality > 65k); most keys fit in 0/1/2 bytes, making fixed-width-per-keyset packing very effective.

`series.bin` v2 stores:
- a fixed-size per-series table: `series_ref -> {series_id, kind_mask, chunk_index_offset, chunk_index_len, keyset_id, row, meta}`
- a keyset table: `keyset_id -> [key_sym...]` (sorted)
- per-key dictionaries: `key_sym -> [value_sym...]` where `value_code = index`
- per-keyset packed value-code blocks: all rows’ values stored compactly using per-key widths (0/1/2/4 bytes)

```
SeriesBinHeaderV2:
  u32 magic     // 'SERI'
  u16 version   // 2
  u16 flags
  u32 num_series
  u32 num_keysets
  u32 num_value_dicts
  u32 reserved0

  u64 series_table_offset
  u64 keysets_offset
  u64 value_dicts_offset
  u64 keyset_blocks_offset
  u64 meta_offset

SeriesEntryV2:  // fixed-size, 40 bytes
  u64 series_id
  u8  kind_mask        // FLOAT/HIST/EXPHIST/SUMMARY present in this segment for this series
  u8  flags
  u16 reserved0
  u64 chunk_index_offset // byte offset in chunk_index.bin for this series' entry span
  u32 chunk_index_len    // byte length of this series' entry span; 0 means no chunks in this segment
  u32 keyset_id        // KeySetId (dense 0..num_keysets-1)
  u32 row              // row index within that keyset block
  u32 meta_off         // byte offset relative to meta_offset (0 if meta_len=0)
  u32 meta_len

KeySetsSectionV2:
  u64 keyset_offsets[num_keysets + 1]
  // keyset entries...

KeySetEntryV2:
  u32 key_count
  u32 reserved0
  u32 key_syms[key_count]   // sorted

ValueDictsSectionV2:
  u64 dict_offsets[num_value_dicts + 1]
  // dict entries...

ValueDictEntryV2:
  u32 key_sym
  u32 cardinality
  u32 value_syms[cardinality]  // ValueCode == index into this array

KeySetBlocksSectionV2:
  u64 block_offsets[num_keysets + 1]
  // blocks...

KeySetBlockV2:
  u32 rows
  u32 key_count
  u32 row_len_bytes
  u32 data_len
  u8  widths[key_count]   // each in {0,1,2,4}
  u8  data[data_len]      // packed value codes, row-major
```

Packed code rules (row-major):
- For each row: iterate keys in keyset order.
- If `widths[i] == 0`: omit bytes; code is implicitly `0`.
- Else: read/write `widths[i]` bytes little-endian as an integer `value_code`.
- To materialize `(key,value)` strings:
  - key string = `symbols.resolve(key_sym)`
  - value sym = `value_syms[value_code]` from that key’s `ValueDictEntryV2`
  - value string = `symbols.resolve(value_sym)`

Width selection:
- `cardinality <= 1` ⇒ width `0`
- `cardinality <= 256` ⇒ width `1`
- `cardinality <= 65_536` ⇒ width `2`
- else ⇒ width `4`

This layout corresponds to the in-memory `KeySetLabelSetStore` + `PackedKeySetLabelSetStore` approach and is designed to scale better under high-cardinality workloads.

`chunk_index_offset + chunk_index_len` MUST be within `chunk_index.bin`. The
chunk-index span for a given `series_ref` contains exactly that series' entries,
ordered by `(start_ms, end_ms, lane, chunk order)`. This duplicates the
series-to-chunk-index routing pointer in `series.bin` so a selective reader can
plan the exact chunk-entry body span without materializing the complete offsets
directory. It does not make the selected directory pair optional. Before
trusting the pointer, a governed reader MUST positionally read or reuse exactly
`series_offsets[series_ref]` and `series_offsets[series_ref + 1]` (16 bytes),
validate the pair, and require its resulting range to equal the
`SeriesEntryV2` range. A mismatch, including an aligned in-bounds range
belonging to another series, is touched corruption. Only then may the reader
fetch the chunk-entry body.

A decoded `SeriesEntryV2.series_id` is unverified until the referenced
canonical label row has been materialized, every required symbol has been
resolved, and the canonical label-byte fingerprint has been recomputed and
matched. A metadata-only routing result MAY expose only `series_ref`,
`kind_mask`, and the directory-bound chunk-index range; it MUST NOT expose or
consume the stored `series_id` as stable identity. A missing or out-of-range
required symbol, a substituted valid row, or an identity mismatch is touched
corruption.

#### 6.4.3 Reserved series metadata fields

Despite the reserved `meta_offset`, `meta_off`, and `meta_len` fields in the v2
layout, footer schema 6 defines no series-metadata payload encoding. Its writer
sets every `meta_off` and `meta_len` to zero, `meta_offset` is canonical EOF,
and its reader rejects a nonzero metadata length as an unsupported/corrupt
v2 record. A future series-level metadata encoding requires a new
`series.bin` and segment schema version; schema-6 readers must not interpret
unversioned bytes here.

Typed sample semantics are not omitted. Histogram, ExponentialHistogram, and
Summary native values, and their typed scalar lanes, encode each sample's
`start_time_ms`, OTLP flags, aggregation temporality, and reset hint. Those
per-sample fields are authoritative. `ChunkHeader.flags` and the v1 chunk-index
flags are integrity-checked aggregate hints, not replacements for the
per-sample values. Delta and cumulative samples must not be silently merged as
one continuous stream; query evaluation follows the decoded per-sample
temporality and reset metadata.

A `kind_mask` with multiple sample kinds for the same labelset is allowed only
to preserve conflicting source data. Query merge and dedupe remain kind-aware
(§14, §16.5).

### 6.5 `chunk_index.bin` format (v1)

`chunk_index.bin` is optimized for:
- fast “chunks for (`series_ref`, time range)” lookups without scanning unrelated series
- predictable positional access patterns (fixed-size chunk entries)

The file keeps its own authoritative `series_offsets` directory so it is
self-describing and can be validated independently. Readers MAY avoid complete
directory materialization, but MUST bind every selected `SeriesEntryV2` span to
the exact 16-byte directory pair as defined in §6.4.2 before reading its body.

All integer fields are little-endian.

```
ChunkIndexHeader:
  u32 magic     // 'CHIX'
  u16 version   // 1
  u16 flags
  u32 num_series

  // Directory: per-series chunk entry ranges by SeriesRef (dense 0..num_series-1).
  // Offsets are relative to the start of the file.
  u64 series_offsets[num_series + 1]
  // entries...
```

For each `series_ref`, the bytes in `[series_offsets[i], series_offsets[i+1])` are a sequence of fixed-size chunk records:
```
ChunkEntryV1:
  u8  file_id          // 0 = chunks.bin, 1 = ooo_chunks.bin
  u8  kind             // FLOAT/HIST/EXPHIST/SUMMARY (optional hint; reader can validate via ChunkHeader)
  u16 flags
  u64 min_time_ms
  u64 max_time_ms
  u64 offset           // byte offset in the selected chunk file, points to ChunkHeader
  u32 length           // bytes to read (ChunkHeader + native payload + optional scalar lane)
  u32 scalar_lane_offset // 0 if absent; byte offset from ChunkHeader start to TypedScalarLaneHeader
  u32 scalar_lane_len    // 0 if absent; bytes in TypedScalarLaneHeader + body
```

Ordering rules:
- Entries are sorted by `(min_time_ms, max_time_ms, offset)` within each (`series_ref`, `file_id`) lane.
- For `chunks.bin` (in-order lane), a writer should ensure chunk time ranges for a series do not overlap and are increasing by time.
- For `ooo_chunks.bin`, overlaps are allowed; queries must merge/dedupe at read time.

Scalar lane rules:
- For typed HIST/EXPHIST/SUMMARY chunks, writers SHOULD populate `scalar_lane_offset` and `scalar_lane_len` so `_count` and `_sum` projections can read `ChunkHeader + TypedScalarLane` without reading the full native typed payload.
- If one scalar-lane field is zero and the other is non-zero, the chunk index is corrupt.
- `scalar_lane_offset + scalar_lane_len <= length`, and `scalar_lane_offset >= sizeof(ChunkHeader)`.

---

## 7) WAL + shard-local offset checkpointing (no Kafka-offset dependency)

### 7.1 WAL record format
The WAL is a sequence of append-only outer records. All fixed-width fields are
little-endian:

```text
offset  size  field
0       4     magic u32             // bytes "CWAL"
4       2     version u16           // 1
6       2     record_type u16       // 1=OTLP_BATCH, 2=CHECKPOINT, 3=SEGMENT_SEALED
8       8     payload_len u64
16      N     payload[N]
16+N    4     crc32c u32            // CRC over the 16-byte header and payload
```

Record types:
- `OTLP_BATCH`: raw OTLP `ExportMetricsServiceRequest` bytes plus capture
  metadata, not lossy normalized points
- `CHECKPOINT`: (partition -> next_offset) map + wal_lsn + wall clock
- `SEGMENT_SEALED`: segment id + range + wal_lsn boundary

An `OTLP_BATCH` outer payload is exactly OBAT version 2. The fixed header is 48
bytes; the topic and raw protobuf immediately follow it with no alignment or
padding:

| Offset | Size/type | Field | Required value or meaning |
| ---: | ---: | --- | --- |
| 0 | `u32` | `magic` | bytes `OBAT` |
| 4 | `u16` | `version` | `2` |
| 6 | `u16` | `flags` | `0` |
| 8 | `u32` | `topic_len` | byte length of `topic` |
| 12 | `i32` | `partition` | transport partition |
| 16 | `i64` | `offset` | transport offset |
| 24 | `i64` | `source_timestamp_ms` | diagnostic transport metadata only; `-1` means unavailable |
| 32 | `i64` | `captured_at_ms` | trusted ingest-time policy anchor |
| 40 | `u64` | `payload_len` | byte length of the raw OTLP protobuf |
| 48 | `topic_len` bytes | `topic` | valid UTF-8, copied exactly |
| `48 + topic_len` | `payload_len` bytes | `payload` | exact raw `ExportMetricsServiceRequest` bytes |

The exact OBAT length is `48 + topic_len + payload_len`, using checked
arithmetic. The outer WAL CRC covers the complete OBAT bytes, so OBAT has no
second checksum. Decoders validate both magics, both versions, outer record
type, zero OBAT flags, checked length conversions and bounds, topic UTF-8,
outer CRC, and exact consumption with no trailing bytes. Truncation, overflow,
an unsupported version, non-zero flags, invalid UTF-8, a checksum mismatch, or
trailing bytes is not a partial batch.

OBAT version 1 is rejected as unsupported. A reader must not reinterpret it,
synthesize `captured_at_ms`, or fall back to a source timestamp. Existing v1
WALs migrate by deterministic replay from an original raw capture that
preserves `captured_at_ms` and stable input order into a new WAL/output root.
Without that trusted capture anchor, policy-equivalent migration cannot be
claimed.

The raw protobuf bytes are appended without decode/re-encode, preserving
unknown protobuf fields byte-for-byte along with known flags, exemplars,
start times, temporality, and future OTLP fields. `source_timestamp_ms` remains
diagnostic metadata and never supplies event time or policy time.
A structurally valid, CRC-valid OBAT within the replay's single source scope
whose raw bytes are not a decodable `ExportMetricsServiceRequest` is a rejected
source batch, not corrupt WAL framing. Recovery counts and skips that batch and
continues with the next record, matching live decode rejection.

WAL order is increasing record position, not source timestamp, transport
offset, or datapoint event time. The current checkpoint and replay API operates
on one WAL file. In that API, a record LSN and `checkpoint.meta.wal_lsn` are the
record's byte offset from the start of that file. They are not a cross-file LSN
and must be passed unchanged to `SeekFrom::Start`. Manifest-driven retention
uses a separate sequence-qualified boundary for ordered `wal-NNNNNN.log`
files; that boundary is not a valid checkpoint seek offset. Cross-file recovery
must carry the file sequence and local offset explicitly and replay files in
sequence order rather than passing an encoded retention boundary to the
single-file replay API. Transport metadata is retained for diagnostics,
checkpointing, and source resumption, but never reorders WAL records.

Replay contract:
- Live ingestion and WAL replay use the same concrete
  `chronoxide_core::event_time::EventTimePolicy` and the same configured
  age/lead behavior. Replay does not carry a second policy implementation.
- For every Number, Histogram, ExponentialHistogram, and Summary datapoint,
  evaluate the raw `time_unix_nano` with OBAT `captured_at_ms` before label
  canonicalization/interning, watermark advancement, or head mutation.
  `time_unix_nano == 0` yields `MissingTimestamp`; source/capture timestamps
  never replace it. Too-old, too-future, and missing-timestamp datapoints do
  not create series or symbols.
- After acceptance, WAL replay re-runs the same normalization, temporality
  handling, reset detection, and chunk-building code as live ingestion. Live
  ingestion and replay share the same stateful Histogram and
  ExponentialHistogram reset tracker.
- Live ingestion and WAL replay apply the same typed-shape validation before
  label interning and reset tracking. WAL replay counts invalid typed
  datapoints separately, skips only those datapoints, and continues the valid
  siblings and following records.
- Deterministic replay requires the same writer config, segment duration,
  `EventTimePolicy` configuration, deterministic segment id seed, and WAL
  record order (§4.5).
- The current single-head replay may contain at most one `(topic, partition)`
  identity. Encountering an OBAT for another topic or partition is a recovery
  error, not another stream to merge into the same head.
- `HeadBuffer::record_sample` may rotate the active window while replaying.
  Replay returns every completed `HeadWindow` in rotation order, while the
  caller-supplied `HeadBuffer` retains the still-active head and any configured
  late/out-of-order windows. A caller must publish or otherwise retain all of
  those windows; silently discarding any of them loses recovered samples.
- Replay is not transactional. Callers must supply a fresh head and label
  store; the replay API rejects nonempty state. Callers must discard both on a
  fatal replay error. Earlier valid records may already have mutated them,
  while completed windows accumulated inside the failed replay are not
  returned.

### 7.2 Checkpoint file (replay boundary)
`checkpoint.meta` is a small atomically replaced snapshot of the latest checkpoint:
- offsets per transport partition
- wal_lsn of the checkpoint record
- checksum

The `wal_lsn` is the checkpoint record's local byte offset in the single WAL
file consumed by the current replay API. `checkpoint.meta` contains no durable
head snapshot, label-interner/reset-tracker snapshot, or manifest coverage
proof. It is therefore not, by itself, a safe sample-replay boundary.

A checkpoint may be published only after its WAL record is durable. The safe
ordered operation is:

1. append the `CHECKPOINT` record;
2. flush and sync the WAL file;
3. write and sync the temporary `checkpoint.meta`, atomically rename it, and
   sync the containing directory.

Callers must use
`WalWriter::append_checkpoint_and_publish(checkpoint_dir, wall_time_ms,
offsets)` for this sequence. Calling the lower-level append and atomic metadata
writers without preserving this ordering can publish a checkpoint that points
beyond the durable WAL after a crash.

On startup:
1. read `checkpoint.meta`
2. seek to the local `wal_lsn` and validate that the exact WAL record is a
   matching `CHECKPOINT` record
3. rewind to byte zero and replay the complete valid WAL into a fresh replay
   state, materializing samples and collecting every rotated `HeadWindow` plus
   the active and late/out-of-order head windows; after source-scope
   validation, a CRC-valid OBAT with malformed protobuf is counted and skipped,
   as in §7.1; discard the fresh state if replay returns a fatal error
4. prove that the recovered head/windows and any manifest-published sealed
   segments durably cover the offsets that will be skipped
5. only after that proof, resume transport consumption from the recorded
   offsets

Until a future checkpoint durably snapshots head state or references a
manifest-published state that proves equivalent coverage, replaying only the
tail after `wal_lsn` is incorrect. If coverage cannot be proven, the transport
must resume from an earlier safe offset rather than treating
`checkpoint.meta` as evidence that the samples are already published.

---

## 8) Checksums and corruption handling

Use checksums at three layers:

- **WAL record**: crc32c (detect torn writes and corruption)
- **Chunk frames**: each frame carries a crc32c
- **Segment footer**: strong checksum per file (xxhash64 or blake3)

Behavior:
- WAL replay stops at the first invalid outer record or corrupt OBAT envelope
  (framing, flags, length, UTF-8, or CRC); earlier records remain valid. An
  unsupported OBAT version is instead a hard `Unsupported` recovery error so
  an incompatible format cannot be mistaken for a safely truncated tail.
  Within the accepted source scope, a CRC-valid OBAT containing malformed
  source protobuf is rejected and counted as described in §7.1, and replay
  continues.
- Segment read:
  - if footer checksum fails => quarantine segment
  - if a touched chunk frame fails CRC => return structural corruption and
    quarantine the segment; a query must not convert it to a partial/empty
    result. An explicit offline recovery scan may stop at the invalid tail, but
    that is not query behavior.
  - if a chunk has an unknown `(kind, encoding)` pair => do not decode it;
    return an unsupported-encoding error. Schema-7 and Schema-8 reads define no
    silent partial-results mode.
  - if `chunk_index.bin` kind disagrees with `ChunkHeader.kind` => treat the segment index as corrupt and quarantine or rebuild the index

---

## 9) mmap vs explicit I/O

Use both, by file class:

### legacy mmap option (not schema 7/8 or their A/B path)

Older reader experiments may map compact `indexes.puffin` blobs. Schemas 7 and
8 and the homogeneous schema-6 A/B adapter prohibit metadata mappings outside
the aggregate governor: retained bytes, VMAs, and backing descriptors would
evade its accounting and hard FD cap.

### immutable positional metadata
- `symbols.bin` v3 root and symbol pages

The symbol reader opens and validates the compact root, then positionally reads
only requested pages. It may retain fully validated pages in an explicitly
byte-bounded cache. It must not mmap or eagerly materialize the complete symbol
dictionary as part of ordinary segment open.

### schema-7/8 governed positional metadata

Footer schemas 7 and 8 do not mmap `series.bin`, `chunk_index.bin`, or
`indexes.puffin`. They read their roots, directories, hot/cold pages, postings,
FST ranges, and overflow blobs through immutable positional reads and the
store-wide governor defined by the focused schema contracts. The homogeneous
schema-6 benchmark adapter must charge and govern its legacy series/index
roots, decoded directories, pages, postings, and retained file descriptors
through the same mechanism; an mmap or per-segment cache outside those charges
is not a valid A/B baseline.

The four initial governor settings are `retained_max_bytes = 64 MiB`,
`in_flight_max_bytes = 256 MiB`, `max_open_files = 128`, and
`max_cached_open_files = 64`. They are aggregate store limits, not per-segment
budgets. Schema-7/8 production and both sides of the benchmark expose them before
opening any segment.

Resident recency bookkeeping MUST provide expected O(1) keyed hit promotion,
oldest eviction, and keyed removal. A resident-cache hit MUST NOT scan the
resident population to find its prior recency position. Cache values and their
governed charges must still be detached under the cache mutex and destroyed
only after unlocking.

### explicit I/O (large data)
- `chunks.bin`
- `ooo_chunks.bin`

Reader uses `pread`/`io_uring` to batch reads of required frames and avoid page-fault storms from mmapping huge chunk files.
Implementation note: use `io_uring` on Linux when available; on macOS fall back to standard `pread`.

Current query readers deterministically coalesce selected payload ranges per
file using an immutable session setting. `chunk_payload_coalesce_max_gap_bytes`
defaults to 4096 and currently accepts `0..=4096`; zero still merges
overlapping and exactly contiguous ranges. Coalesced gap bytes are physical
read amplification only: they do not become selected chunks, are not decoded,
and do not change logical `QueryStats` charging or corruption authority.
The same upper bound applies at the public batch-planner boundary, so callers
cannot bypass the session constructor and request unbounded over-read.

`QueryStats.chunk_reads`, `QueryStats.bytes_read`, `query_max_chunks_read`, and
`query_max_bytes_read` describe selected logical payload requests before cache
filtering and coalescing. For a file-specific plan with `n` non-empty logical
ranges, `p` resulting physical spans, and gap bound `G`, physical bytes are at
most logical selected bytes plus `G * (n - p)`; overlaps can make them lower.
The chunk-count limit and bounded `G` therefore bound amplification, and the
payload/scheduler profiles expose actual process-issued spans and bytes, but
there is currently no separate cumulative per-query physical-byte limit. A
deployment requiring `query_max_bytes_read` itself to be a hard physical-I/O
cap must use gap zero. Any future physical-byte guardrail must be a distinct,
pre-fetch limit with explicit accounting rather than silently changing the
meaning of public `QueryStats.bytes_read`.

Scheduler submission-depth and peak-in-flight fields are session high-water
gauges. A profile delta exposes the current high-water only if its interval
contains a new scheduler execution; it does not subtract one maximum from
another. Raw query schema v13 names these fields with a
`session_*_high_water` suffix, and reporting must not add them across runs.
The scheduler's monotonic physical-byte counter is named
`total_physical_bytes_executed`; it is not a current-memory gauge.

### 9.1 Direct I/O (O_DIRECT) alignment (optional)

`io_uring` does not imply direct I/O, but if you choose `O_DIRECT` for chunk files to reduce page-cache thrash, you must handle alignment constraints (common on Linux):
- file offsets must be aligned to the device logical block size (often 4096)
- read lengths must be multiples of that block size
- user buffers must be aligned (e.g., `posix_memalign(4096)`)

Because `chunk_index.bin` addresses **individual chunks**, chunk `(offset, length)` pairs are not guaranteed to be 4KB-aligned. Therefore, an `O_DIRECT` reader must round reads to aligned boundaries and then slice:
- `aligned_off = floor(offset / 4096) * 4096`
- `aligned_end = ceil((offset + length) / 4096) * 4096`
- read `[aligned_off, aligned_end)` into an aligned scratch buffer, then decode from `buffer[(offset - aligned_off) .. (offset - aligned_off + length)]`.

This keeps the on-disk layout space-efficient while still allowing direct I/O.

**Scaling note**
If SSD retention is large enough to produce hundreds/thousands of segments,
keep only the manifest inventory resident. Load integrity-checked metadata ranges
positionally through the aggregate byte-bounded LRU and FD manager.

---

## 10) fsync policy (configurable)

Two modes:

### Strong durability (default)
- WAL: group commit with `fdatasync` every `wal_sync_interval` (e.g., 10–50ms) or on size threshold
- Segment sealing: `fsync` all files + dir before publish

### Faster / weaker (optional)
- WAL: rely on OS flush with less frequent sync (risk: lose last few seconds on power loss)
- Segment sealing: fsync only footer + dir (still safe, but might lose the last segment-in-progress)

---

## 11) Chunk encoding (frames with self-describing chunks)

`chunks.bin` is a sequence of **frames**. A frame groups one or more chunks and targets ~32–256KB.
Current implementation note: frames currently carry **one chunk each** (`num_chunks = 1`); frame packing is future work.

**Read amplification rule**  
Frames are a **physical I/O container**, not the unit of addressing. `chunk_index.bin` must index **individual chunks** (offset + length) so queries can read only the chunks needed for the selected series/time range, without having to read entire mixed-series frames.

**Write order for SSD locality (recommended)**  
When sealing a segment, assign `series_ref` densely in metric-query order as
defined in §6.4.1.1. Then write chunks in **series-major order** (sort by
`series_ref`, then by time) and pack consecutive chunks into frames until
`frame_target_size` is reached. This keeps bytes for a metric and series
contiguous on disk and reduces read amplification without requiring background
compaction. Current single-chunk-frame writers that append chunks before final
seal ordering must rewrite `chunks.bin` during sealing into final series-major
order, patch each `ChunkHeader.series_ref`, update chunk-index offsets, and
recalculate frame CRCs before publishing. The head-window seal path should feed
series to the segment writer in metric-query order so `chunks.bin` is emitted in
final order without a post-write rewrite. Frame packing remains future work.

### 11.1 Frame header
```
FrameHeader:
  u32 frame_len
  u32 frame_crc32c
  u16 flags          // must be zero in the current format
  u32 num_chunks
  ... chunk payloads ...
```

`frame_len` includes the 14-byte header. A reader must reject a nonzero
`flags` value, a `frame_len` smaller than the header, a frame range beyond the
physical file, or a truncated header/payload as structural corruption. It must
validate the complete declared range before allocating a frame-sized buffer.
The current reader additionally requires `num_chunks = 1`.

### 11.2 Common chunk header
Each chunk belongs to one series and covers a contiguous time range.

```
ChunkHeader:
  u8  kind            // FLOAT (Gauge/Sum), HIST, EXPHIST, SUMMARY
  u8  encoding        // per-kind encoding id
  u16 flags
  u32 series_ref
  u64 min_time_ms
  u64 max_time_ms
  u32 num_points
  u32 header_len
  u32 payload_len
  u32 chunk_crc32c    // covers native payload bytes only, not scalar lane
  ... typed_scalar_lane? ...
  ... payload ...
```

Chunk kind ids:
```
0 = FLOAT
1 = INT64
2 = HIST
3 = EXPHIST
4 = SUMMARY
```

The encoding byte uses one shared namespace. The implemented values and valid
kind pairs are:
```
0 = SCHEMA_VARLEN          // HIST, EXPHIST, SUMMARY
1 = RAW_F64                // FLOAT
2 = RAW_I64                // INT64
3 = GORILLA                // FLOAT
4 = INT_DELTA_ZIGZAG       // INT64
```

Unknown `(kind, encoding)` pairs are treated as unreadable data: the reader must not attempt best-effort decoding. It may skip the chunk, quarantine the segment, or return a typed "unsupported chunk encoding" error depending on query policy (§8).

`ChunkHeader.flags`:
```
bit 0      reserved                    // 0
bit 1      HAS_START_TIME              // at least one sample has start_time_ms
bit 2      HAS_PER_SAMPLE_FLAGS        // at least one sample has nonzero OTLP flags
bit 3      HAS_COUNTER_RESET_HINTS     // at least one non-Unknown reset hint
bit 4      TEMPORALITY_DELTA           // every sample is Delta
bits 5-15 reserved                     // 0
```

`ChunkEntryV1` fields are index hints for planning. Before decoding an indexed
record or projection, readers validate its kind, flags, time range, complete
record length, and canonical scalar-lane range against `ChunkHeader` and the
external authenticated locator. A mismatch is corruption, never a cache miss
or empty result.

Flag invariants:
- FLOAT and INT64 chunks have `flags == 0`.
- HIST, EXPHIST, and SUMMARY chunks reject `flags & !0x001e != 0`.
- For a non-empty typed chunk, bits 1, 2, and 3 are set exactly when any
  decoded sample has the corresponding field, and bit 4 is set exactly when
  every decoded sample has Delta temporality. The reader verifies these
  aggregate hints against native values or the complete typed scalar lane.
- Current chunks always have `num_points >= 1`. Adding new flag meanings
  requires a new encoding or segment version; readers never reinterpret a
  reserved bit.

### 11.3 Current payload prefix and typed metadata

Footer schemas 6, 7, and 8 use the current compact payload, not separated common
metadata lanes. Every native payload starts with `u64 t0_ms`.
`SCHEMA_VARLEN`, `GORILLA`, and `INT_DELTA_ZIGZAG` then split timestamps from
their value stream:

```text
u64      t0_ms
uLEB128  dt_ms[num_points]       // timestamp_ms - t0_ms
... encoding-specific values ...
```

`RAW_F64` and `RAW_I64` instead interleave each timestamp delta and value:

```text
RAW_F64:
u64 t0_ms
repeated num_points times:
  uLEB128 dt_ms
  f64le value

RAW_I64:
u64 t0_ms
repeated num_points times:
  uLEB128 dt_ms
  i64le value
```

All additions and counts are checked. Current chunks are non-empty and
timestamp ordered, and `t0_ms == min_time_ms`; the decoder rejects a zero point
count or a different timestamp base. It consumes exactly `num_points`
rows/deltas and the complete encoding-specific value stream; truncation,
overflow, or trailing bytes are corruption.
Timestamp reconstruction specifically uses checked `t0_ms + dt_ms`; it must
never clamp or wrap an overflowing timestamp. A `GORILLA` value stream ends in
the byte containing its last encoded bit. Any unused low-order bits in that
byte are zero, and no additional byte is permitted, including an all-zero byte.
Bits are written most-significant first. Its value-stream grammar is:

```text
u64be first_value_ieee_bits
repeated for each later value:
  bit 0                              // XOR == 0; repeat prior value
  or
  bits 10
  bits prior_significant_window      // XOR != 0 and fits prior window
  or
  bits 11
  bits[5] leading_zero_count         // unsigned; at most 31
  bits[6] significant_width_code     // 0 means 64, otherwise 1..63
  bits[significant_width] xor_payload
```

For a new window, `trailing_zero_count = 64 - leading_zero_count -
significant_width`; negative/impossible widths are corruption. The payload is
the significant XOR bits after removing those trailing zeroes. A nonzero XOR
reuses the prior window whenever the XOR fits it; otherwise it introduces one
window whose trailing zero count is exact and whose leading zero count is
`min(actual, 31)`. Encoding a zero XOR through a window, introducing a wider
window than those rules produce, or introducing a new window when the prior
window fits is noncanonical corruption even if it decodes to the same IEEE
values.

The `INT_DELTA_ZIGZAG` value stream contains exactly `num_points` canonical
`zLEB128` fields. Starting with `previous = 0`, the writer encodes
`value.wrapping_sub(previous)` and then assigns `previous = value`; the reader
uses the matching `previous.wrapping_add(delta)`. Two's-complement wrapping is
part of this encoding and is not an overflow error.

Every unsigned LEB128 field in the current chunk and typed-scalar-lane layouts
is the shortest encoding of one `u64`: at most ten bytes are accepted, the
tenth byte has no payload bit other than bit zero, and redundant leading-zero
groups are corruption. Signed `zLEB128` fields first use the stated ZigZag
mapping and then this same canonical unsigned representation. This rule also
applies to lengths, counts, schema IDs, timestamps, metadata enums, and bucket
values; a checksum-valid noncanonical representation is still corruption.

For `SCHEMA_VARLEN`, the encoding-specific stream is:

```text
uLEB128  num_schemas
repeated num_schemas times:
  uLEB128 schema_len
  u8      schema[schema_len]
repeated num_points times:
  uLEB128 schema_id
  ... kind-specific value using that schema ...
```

Schema IDs are dense `0..num_schemas-1` in deterministic first-seen order.
Schema definitions are byte-unique, every definition is referenced by at least
one sample, and first uses introduce IDs exactly in order `0, 1, ...`; duplicate
definitions, a skipped/out-of-order first use, or an unused definition are
noncanonical corruption. Every sample always contains `schema_id`, including
when `num_schemas == 1`; footer schemas 6, 7, and 8 define no `SINGLE_SCHEMA`
omission and chunk-header bit 0 remains reserved. `schema_id >= num_schemas`,
an unconsumed schema byte, or any trailing value byte is corruption. The
complete stream is inside `payload_len` and integrity-checked by
`chunk_crc32c`.

Every Histogram, ExponentialHistogram, and Summary schema-varlen value begins
with this exact per-sample metadata:

```text
uLEB128 otlp_datapoint_flags      // must fit u32
uLEB128 temporality               // 0=Unspecified, 1=Delta, 2=Cumulative
uLEB128 reset_hint                // 0=Unknown, 1=Reset, 2=NotReset, 3=Gauge
u8      start_time_present        // exactly 0 or 1
uLEB128 start_time_ms?            // present only when the preceding byte is 1
```

`ChunkHeader.flags` contains integrity-checked aggregate hints; it never causes
these fields to be omitted. A selected non-stale delta Histogram or
ExponentialHistogram interval requires a present `start_time_ms < timestamp`.
`FLAG_NO_RECORDED_VALUE` means a semantic gap and projects to the exact
Prometheus stale-NaN sentinel. Its typed value body remains byte-present and
num-points aligned; readers derive staleness from the per-sample flags, not
from numeric zeroes.

A future separated common-lane layout requires a new chunk encoding or segment
version. The earlier `HAS_*`-controlled start-time/flag/reset lane sketches and
uniform-reset omission are not valid byte layouts for footer schemas 6, 7, or 8.

### 11.3.1 Typed scalar projection lane

Typed HIST/EXPHIST/SUMMARY chunks may store a compact scalar projection lane immediately after `ChunkHeader` and before the native typed payload. This makes `_count`/`_sum` projection a single contiguous `ChunkHeader + TypedScalarLane` read. The lane is outside `ChunkHeader.payload_len` and outside `chunk_crc32c`; it is inside `ChunkEntryV1.length`, and its body is covered by `body_crc32c`. `ChunkHeader.header_len` points to the start of the native typed payload, so when the scalar lane is present `header_len = sizeof(ChunkHeader) + scalar_lane_len`.

```
TypedScalarLaneHeader:
  u32 magic        // "TSCL"
  u16 version      // 1
  u16 flags        // 0 in v1
  u32 body_len
  u32 body_crc32c

TypedScalarLaneBody:
  u64      t0_ms
  uLEB128  dt_ms[num_points]
  repeated num_points times:
    TypedSampleMetadata metadata
    uLEB128 count
    u8      sum_present
    f64le   sum?
```

The locator's scalar-lane length is either zero or at least 16. A nonzero lane
is valid only for a non-empty Histogram, ExponentialHistogram, or Summary
`SCHEMA_VARLEN` chunk, begins at byte 40, and satisfies checked
`16 + body_len == scalar_lane_len`. The presence byte for `sum?` is exactly 0
or 1. Any other shape is corruption/replay rejection, not an alternate encoding
and not a reason to route a schema-7/8 record through overflow.

`TypedSampleMetadata` uses the same wire encoding as the native typed payload: `flags`, `temporality`, `reset_hint`, and optional `start_time_ms`.

`body_crc32c` covers the lane body, not the 16-byte lane header. Footer schemas
7 and 8 therefore integrity-check the exact lane header together with
`ChunkHeader` through each locator's external indexed-prefix CRC. Readers use
`chunk_index.bin` `scalar_lane_offset/scalar_lane_len` to fetch only
`ChunkHeader + TypedScalarLane` for `<metric>_count` and `<metric>_sum`. The
reader must validate lane magic, version, zero flags, body length, body CRC, and
that `ChunkHeader.kind` is one of HIST/EXPHIST/SUMMARY with `SCHEMA_VARLEN`
encoding. The lane is non-empty, uses `t0_ms == min_time_ms`, has ordered
timestamps, and its decoded first and last timestamps equal the authenticated
chunk range. When both representations are read for verification, every lane
row must equal its native row in timestamp, all typed metadata, count, and the
presence and exact IEEE bits of the optional sum. The complete native or scalar
lane also recomputes the aggregate `ChunkHeader.flags`; disagreement is
corruption. If the scalar lane is absent, a reader may fall back to scanning the
full native typed payload.

### 11.3.2 Experimental decoded codec evidence

The exhaustive `chronoxide-storage-verify` report includes a bounded
`chunk_inventory` for the verifier's selected series. With no series-sampling
limit, this is an exhaustive corpus inventory. `chunk_inventory.layout` is
`sealed_chunk_v1`, and `by_kind_encoding` contains one deterministic row per
observed valid `(kind, encoding)` pair. A row records chunk and point counts;
indexed, common-header, scalar-lane, and native-payload bytes; and the native
payload partition into timestamp base, timestamp deltas, and values. The byte
partitions, decoded point count, timestamp ordering, and decoded first/last
timestamps versus the authenticated chunk range must reconcile exactly before
the row is accepted.

Point counts and adjacent within-chunk timestamp cadences use power-of-two
histograms. Zero has a separate count; each other bucket is the inclusive
range `[2^n, 2^(n+1)-1]`. Histogram state has a fixed 64 counters per measure
and does not retain per-point rows.

For every decoded FLOAT chunk, `raw_f64_vs_gorilla` reports exact canonical
candidate sizes for both current codecs using the same decoded timestamps and
exact IEEE value bits. Each candidate consists of the common 40-byte header,
the canonical `t0_ms` plus unsigned-LEB128 timestamp-delta stream, and either
eight bytes per RAW_F64 value or the exact byte-rounded GORILLA bitstream. The
report totals existing, RAW_F64, GORILLA, and per-chunk adaptive-minimum
payload/indexed bytes and counts chunks and points won by RAW_F64, GORILLA, or
a tie. Candidate calculation streams over the decoded chunk and does not
materialize a second encoded value buffer. Equal payload-byte candidates select
RAW_F64 deterministically. The report records that rule, adaptive selection
totals, exact IEEE-class totals (`+0`, `-0`, finite nonzero, both infinities,
ordinary NaN, and the exact stale-NaN sentinel), repeated XORs, reused versus
new Gorilla windows, and a power-of-two histogram of significant XOR widths.
Those totals must reconcile with decoded points and Gorilla transitions. The
candidate matching the persisted codec must also match the authenticated
payload and indexed lengths exactly or the evidence run fails closed.

`timestamp_candidates` evaluates the native-payload timestamp stream of every
decoded chunk. It deliberately excludes the duplicate timestamps in a typed
scalar lane, which are reported separately as `scalar_lane_bytes`. Candidate
sizes include the eight-byte little-endian first timestamp and use the
authenticated `ChunkHeader.num_points` to determine the number of values:

- `current_offset_uleb`: `t0_ms`, followed by one unsigned-LEB128
  `timestamp_ms - t0_ms` for every point, including the first zero offset;
- `adjacent_delta_uleb`: `t0_ms`, followed by one unsigned-LEB128 adjacent
  delta for each point after the first;
- `delta_of_delta_zigzag_uleb128`: `t0_ms`, the first adjacent delta as
  unsigned LEB128 when present, then each signed `delta[i] - delta[i-1]` as
  128-bit ZigZag unsigned LEB128; and
- `fixed_step_residual_bitpack`: for a single point, only `t0_ms`; otherwise
  `t0_ms`, unsigned-LEB128 `step = (last - first) / (num_points - 1)`, one
  `u8` residual bit width, and fixed-width ZigZag-i128 residuals for points
  `1..num_points` relative to `first + index * step`. Residual bits are packed
  least-significant bit first into each value and byte, with zero high padding
  in the final byte.

The report sets `selector_bytes_included = false`: no new per-block codec tag,
selector, alignment, or migration overhead is charged. It partitions blocks
into `single_point`, `constant_zero_step`,
`constant_positive_step`, and `variable_step`, and records exact total bytes,
unique wins, ties, and adaptive selections. Ties select the first minimum in
this stable order: current offset, adjacent delta, delta-of-delta, fixed-step.
These are evidence-only candidate layouts. No reader or writer selects a new
timestamp encoding until a versioned on-disk design and the Phase 6 promotion
gate are separately accepted. For every real chunk, the current candidate
must exactly equal `timestamp_base_bytes + timestamp_delta_bytes`; disagreement
is corruption in the evidence run. Timestamp savings are native-payload-only:
the current typed scalar lane remains byte-identical and its duplicate
timestamps stay charged in `scalar_lane_bytes`. A candidate that would require
changing that lane cannot be activated from these native-only estimates.

The same report includes `decoded_semantic_fingerprint`, SHA-256 under the
domain `chronoxide-verified-decoded-storage-semantics-v2`. It commits to the
selection mode; manifest-order segment identity and bounds; stable series id,
kind mask, and complete canonical labels; and every decoded sample in stored
source-lane order. Within each series, samples are accumulated independently by
`(file_id, kind)` lane, those lanes are folded in ascending key order, and each
lane preserves its decoded chunk/sample order. Sample content includes the lane
id, kind, timestamp, exact FLOAT bits or INT64 value, and every ordered typed
metadata/schema/value field for Histogram, ExponentialHistogram, and Summary.
Thus physical interleaving between distinct kind lanes does not change the
semantic identity, while same-lane duplicate order and in-order versus
out-of-order provenance remain visible.

Version 2 supersedes the experimental version-1 domain because version 1
accidentally made the digest depend on physical interleaving between distinct
kind lanes. Reports must never compare a v1 digest to a v2 digest as though the
domains were interchangeable.

The decoded semantic fingerprint deliberately excludes `series_ref`, chunk
boundaries, encoding, CRC, physical offset, and encoded length. Physical
rechunking or a RAW_F64/GORILLA substitution therefore preserves it when the
ordered decoded semantics are identical. `verified_selection_fingerprint`
remains the separate locator- and exact-byte-sensitive identity. Both are
required evidence: semantic equality must not erase a physical-layout change,
and physical equality is not a substitute for complete decoded semantics.

The explicit exhaustive topology-comparison verifier additionally emits
`topology_independent_decoded_semantic_fingerprint`. This is a separate
streaming multiset identity under the
`chronoxide-topology-independent-semantic-*-v1` domains. It hashes complete
canonical labels, kind, timestamp, exact logical value, typed metadata, and
duplicate multiplicity, while excluding segment identity/bounds, stable and
local series IDs, chunk order/boundaries, encoding, offsets, CRCs, and
in-order/OOO placement. It is therefore suitable for proving decoded-record
multiset preservation across deterministic repartitioning experiments whose
physical topology intentionally differs.

The multiset digest deliberately does not commit to relative order among
records with the same canonical labels, kind, and timestamp. Equal topology-
independent digests consequently do not prove equal duplicate-winner or query-
surface semantics across two repartitionings. Such topologies must be treated
as independent workload strata unless a separate exhaustive proof closes that
ordering question. This digest does not replace byte identity and the ordered
version-2 fingerprint for same-topology comparator/replay equivalence; reports
must name both the identity they compare and the claim it supports.

### 11.4 Native typed value formats

Histogram, ExponentialHistogram, and Summary data are persisted as native typed chunks, not expanded into Prometheus-compatible scalar series on the ingestion path.

Rationale:
- Expanding histograms into `_bucket` / `_sum` / `_count` series at write time multiplies cardinality and index size.
- OTLP carries native shape information that would be lost or made ambiguous by eager scalar projection.
- Query-time projection keeps storage faithful to the source and lets compaction/materialization policies evolve later.

#### HIST/SCHEMA_VARLEN

Schema bytes:
```
uLEB128  num_bounds
f64le    explicit_bounds[num_bounds]   // finite, strictly ascending
uLEB128  bucket_count                  // MUST equal num_bounds + 1
```

Per-sample bytes:
```
uLEB128 schema_id                   // always present
TypedSampleMetadata metadata
uLEB128  count                      // u64
u8       sum_present                // exactly 0 or 1
f64le    sum?                       // present iff preceding byte is 1
u8       min_present                // exactly 0 or 1
f64le    min?                       // present iff preceding byte is 1
u8       max_present                // exactly 0 or 1
f64le    max?                       // present iff preceding byte is 1
uLEB128  bucket_counts[bucket_count]
```

Validation:
- `explicit_bounds` are finite, non-NaN values and strictly ascending by numeric value. `+Inf`, `-Inf`, and `NaN` bounds are rejected so they cannot collide with the synthetic Prometheus `le="+Inf"` bucket.
- `len(bucket_counts) == len(explicit_bounds) + 1`.
- `sum(bucket_counts) == count` for classic OTLP histograms. Overflow in the accumulator is a corrupt-chunk error.
- Bucket counts and `count` decode to `u64`; no `u32` narrowing is allowed.
- Present NaN/Inf values round-trip as raw IEEE bits and are distinct from absent fields and from stale NaN.

`HIST/RAW_VARLEN` is not assigned an encoding byte for footer schemas 6, 7, or 8.
Writers must not emit it and readers reject it as an unknown `(kind,
encoding)` pair. Any future raw fallback requires a new explicit encoding or
segment version.

#### EXPHIST/SCHEMA_VARLEN

Schema bytes:
```
zLEB128  scale
f64le    zero_threshold
```

Per-sample bytes:
```
uLEB128 schema_id                   // always present
TypedSampleMetadata metadata
uLEB128  count                      // u64
u8       sum_present                // exactly 0 or 1
f64le    sum?
u8       min_present                // exactly 0 or 1
f64le    min?
u8       max_present                // exactly 0 or 1
f64le    max?
uLEB128  zero_count                 // u64
zLEB128  positive_offset
uLEB128  positive_len
uLEB128  positive_counts[positive_len]
zLEB128  negative_offset
uLEB128  negative_len
uLEB128  negative_counts[negative_len]
```

`positive_len` and `negative_len` are per-sample fields because dense ExponentialHistogram spans can change frequently. They are not schema fields and therefore do not create schema churn by themselves.

Validation:
- `zero_threshold` is part of the schema. Same `scale` and bucket counts with different `zero_threshold` are different schemas.
- `zero_count + sum(positive_counts) + sum(negative_counts) == count`; overflow is a corrupt-chunk error.
- `count`, `zero_count`, and bucket counts decode to `u64`.

`EXPHIST/RAW_VARLEN` is not assigned an encoding byte for footer schemas 6 or
7. Writers must not emit it and readers reject it as an unknown `(kind,
encoding)` pair. Schema churn is handled by additional schema-table entries or
chunk splitting; any future raw fallback requires a new explicit version.

#### SUMMARY/SCHEMA_VARLEN

Schema bytes:
```
uLEB128 num_quantiles
f64le   quantiles[num_quantiles]    // strictly ascending quantile positions
```

Per-sample bytes:
```
uLEB128 schema_id                   // always present
TypedSampleMetadata metadata
uLEB128  count                     // u64
f64le    sum
f64le    values[num_quantiles]
```

`SUMMARY/RAW_VARLEN` is not assigned an encoding byte for footer schemas 6 or
7. Writers must not emit it and readers reject it as an unknown `(kind,
encoding)` pair. Any future raw fallback requires a new explicit version.

Summary semantics:
- Quantile positions are finite values in `[0, 1]` and strictly ascending.
  NaN, infinities, out-of-range positions, duplicates, and descending order are
  rejected by both writers and readers.
- Summary quantile values are gauges.
- Summary quantiles are not mergeable across series or arbitrary time ranges.
- `_count` and `_sum` projections are scalar views, but query functions must not treat summary quantile samples as histogram buckets.

### 11.5 PromQL projections from native typed chunks

PromQL-compatible scalar views are virtual first. They read native chunks and emit scalar samples to the query engine. Later compaction may materialize hot projections, but materialized projections are rebuildable artifacts and must carry source chunk identities, source footer checksums, covered time range, and a complete-below-watermark marker so OOO data cannot make them silently stale.

Classic Histogram projection:
- `<metric>_count`: `count`
- `<metric>_sum`: `sum`, when present
- `<metric>_bucket{le="..."}`: cumulative prefix sum over `bucket_counts`

For a histogram with `N` explicit bounds:
- `bucket_counts.len() == N + 1`.
- For bound index `i`, emit `le=format_float(explicit_bounds[i])` with `sum(bucket_counts[0..=i])`.
- Emit synthetic `le="+Inf"` with `sum(bucket_counts[0..=N])`, and this value MUST equal `_count`.
- Buckets are emitted in ascending numeric bound order, then `+Inf`.
- For a single timestamp, projected bucket values are monotonically non-decreasing as `le` increases.
- Projected `le` strings use the canonical float spelling from §0.

ExponentialHistogram projection:
- Prefer native histogram results when the PromQL engine supports them.
- `<metric>_count` and `<metric>_sum` are safe scalar projections.
- Optional classic-bucket projection is allowed only with deterministic configured boundaries. The output follows the same cumulative `le` and `+Inf` rules as classic histograms.
- Current implementation supports query-configured finite boundaries for `_bucket{le="..."}` projection. A finite `le` that is not configured emits no series; `le="+Inf"` is derived from `count`. Projection sums whole native exponential buckets whose upper bound is `<= le`; it does not split a native bucket across a configured boundary.

Summary projection:
- `<metric>_count`: `count`
- `<metric>_sum`: `sum`
- `<metric>{quantile="..."}`: quantile value gauge, with canonical `quantile` string formatting from §0

Temporality and projection:
- Cumulative histogram/exponential histogram projections expose cumulative-monotonic counters, using stored counter reset hints (§13.2, §13.5) to make `rate()`/`increase()` correct.
- Delta histogram/exponential histogram projections must not expose raw delta samples as counters. Query code must aggregate deltas over their `[start_time_ms, time_ms)` windows, align compatible schemas, and emit cumulative-shaped virtual samples for PromQL range evaluation.
- Delta and cumulative chunks for the same `(series_id, kind)` are not merged as one continuous stream.

Current parser note: PromQL syntax is parsed with the `promql-parser` crate and
then lowered into Chronoxide's storage-aware evaluator subset. The query API
keeps a compatibility rewrite for OTLP-style dotted metric and label names,
which are normalized at storage-selector lowering.

Current parser note: scalar-only parameters for `histogram_quantile`,
`histogram_fraction`, `topk`, `bottomk`, and `quantile` accept constant scalar
arithmetic expressions over `+`, `-`, `*`, `/`, `%`, and `^`. Parameter
expressions that depend on vector results remain invalid.

Current implementation note: scalar `rate(selector[range])` and
`increase(selector[range])` are implemented over vector/projection query results
with counter reset handling. Scalar `delta(selector[range])` and
`idelta(selector[range])` are implemented for cumulative/unknown scalar
gauge-like streams, without counter reset adjustment. Scalar
`irate(selector[range])` is implemented for cumulative/unknown scalar counter
streams. `rate`, `increase`, and `delta` use range-boundary extrapolation;
scalar `irate` and `idelta` use only the last two valid samples after the last
stale/non-finite boundary. `irate` divides by the observed interval between
those samples; `idelta` returns the raw last-two-sample difference. Scalar
`changes(selector[range])` counts value transitions between non-stale scalar
samples in the same range, treats consecutive ordinary IEEE `NaN` values as
unchanged, and drops the metric name. Scalar `resets(selector[range])` counts
counter resets after the last stale/non-finite boundary, using stored
`CounterResetHint::CounterReset` metadata when aligned and otherwise falling
back to scalar value decreases; `CounterResetHint::GaugeType` makes the
function return no result. Scalar
`last_over_time(selector[range])` returns the last non-stale scalar sample in
the PromQL left-open/right-closed range `(end_ms - range, end_ms]` and preserves
the metric name. Scalar `count_over_time(selector[range])` counts non-stale
scalar samples in the same range, drops the metric name, and treats ordinary
IEEE `NaN`/`Inf` values as present samples. Scalar
`present_over_time(selector[range])` returns `1` when any non-stale scalar
sample is present in the same range, drops the metric name, and treats ordinary
IEEE `NaN`/`Inf` values as present samples. Scalar
`sum_over_time(selector[range])` sums non-stale scalar samples in the same
range, drops the metric name, and preserves ordinary IEEE `NaN`/`Inf` values as
values. Scalar `avg_over_time(selector[range])` averages non-stale scalar
samples in the same range with overflow-resistant mean calculation, drops the
metric name, and preserves ordinary IEEE `NaN`/`Inf` values as values. Scalar
`stddev_over_time(selector[range])` and
`stdvar_over_time(selector[range])` calculate population standard deviation and
variance over non-stale scalar samples in the same range, drop the metric name,
use Prometheus-compatible ordinary IEEE `NaN`/`Inf` propagation, and use a
compensated Welford-style update matching Prometheus' range functions. Scalar
`min_over_time(selector[range])` and `max_over_time(selector[range])` select the
minimum or maximum non-stale scalar sample in the same range, drop the metric
name, preserve infinities, and let ordinary IEEE `NaN` win only when no later
comparable value replaces an already-NaN candidate. Native
Histogram/ExponentialHistogram count/sum/bucket projections preserve stored
`CounterResetHint` metadata and consume it during scalar range evaluation;
scalar series without reset metadata still use counter-decrease reset handling.
For `rate()` and `increase()`, the exact Prometheus stale-NaN marker is omitted
from the selected scalar or native-histogram range before counter reset and
extrapolation math. The retained samples use the original range boundaries; a
stale marker neither truncates that range nor creates a reset by itself. Delta
Histogram and ExponentialHistogram projection reset their internal cumulative
fragment accumulator at a stale datapoint, preserve the marker until range
evaluation, and then apply the same omission rule. The first generated sample
in the restarted fragment uses unknown-reset detection, so a decrease from the
last retained pre-stale cumulative value is evaluated as a counter reset while
an equal or increasing boundary is not; the stale datapoint itself is never a
reset. Stored cumulative/unknown-temporality reset hints remain authoritative
across stale omission, including an explicit `CounterReset`. Only the synthetic
fragment-start hint on the delta virtual fallback is normalized to `Unknown`.

Ordinary IEEE `NaN`, `+Inf`, and `-Inf` are retained values, not stale markers.
Scalar float `rate()`/`increase()` uses Prometheus endpoint subtraction plus
counter-reset adjustments: an interior ordinary NaN can still produce a finite
result, infinities participate in reset arithmetic, and an ordinary non-finite
endpoint propagates as an ordinary NaN or infinity. This applies to virtual
Histogram/ExponentialHistogram `_sum` projections as well as stored scalar
series. For cumulative or unknown-temporality scalar and native-histogram
counters, Prometheus floating-point operation order is normative: first form
the endpoint extrapolation factor; for `rate()` divide that factor by the
logical range duration; then multiply the raw increase or native histogram
components by the resulting factor. Deriving `rate()` by dividing an
already-rounded `increase()` is not equivalent at the final ULP. Native
Histogram/ExponentialHistogram count and bucket math remains
reset-aware and independent of the optional sum. Cumulative native sum math
uses the same endpoint/reset shape, so interior ordinary non-finite sums do not
poison finite endpoints while a non-finite endpoint propagates. Delta optional
sums are signed interval values rather than monotonic counter components:
single- and multi-interval native results and virtual `_sum` projections add
them directly with IEEE arithmetic. A finite negative or ordinary non-finite
optional sum therefore never rejects an otherwise valid count/bucket result.
This does not weaken finite count, bucket, layout, or reset-contradiction
validation. None of these ordinary NaN results is rewritten to the exact stale
sentinel.

If an evaluation range begins before epoch zero, scalar, virtual projection,
and direct native `rate()`/`increase()` selection includes the timestamp-zero
sample and extrapolation retains the logical pre-epoch left-boundary duration
rather than treating zero as an ordinary left-open boundary.

Scoped instant-vector aggregations (`sum`, `count`, `avg`, `min`, `max`,
`stddev`, `stdvar`, `group`, `topk`, `bottomk`, `quantile`, and
`count_values` with `by`/`without`) are implemented over the latest sample in
each input series; selector children of instant-vector operators are read
through a 5-minute lookback window ending at the evaluation timestamp. For the
standard scalar aggregation operators, the exact Prometheus stale-NaN marker
makes that series absent, while present IEEE float values such as `+Inf` and
`-Inf` still participate in aggregation. Scalar `avg` uses an
overflow-resistant running mean for finite inputs, so large same-signed finite
values do not turn a finite average into `+Inf`/`-Inf`. `count_values` skips the
exact stale marker, counts other present IEEE float values, normalizes the
configured output label name with the same PromQL label-name normalization used
for stored labels, and formats value-label strings with the same Go
`strconv.FormatFloat(v, 'g', -1, 64)` style used for PromQL float labels,
including special values as `+Inf`, `-Inf`, or `NaN`.
`topk` and `bottomk` rank ordinary IEEE `NaN` values after finite/infinite
values for both operators, while still returning `NaN` samples when `k` exceeds
the number of finite/infinite candidates. `quantile` aggregation sorts ordinary
IEEE `NaN` values before finite/infinite values, matching Prometheus vector
quantile ordering. The exact stale marker remains absent before these
aggregation-specific ordering rules are applied.
Instant-vector `sort()` and `sort_desc()` are implemented over the latest
sample in each input series, preserve input labels and metric names, skip exact
stale markers, and order ordinary IEEE `NaN` values after finite/infinite
values for both directions.
Aggregation `by(...)` grouping preserves `__name__` when it is explicitly
listed, so grouping by metric name does not collapse different metrics.
Ungrouped aggregations and `without(...)` grouping drop `__name__`.
`absent(expr)` is implemented over
instant-vector inputs: it emits no result when the input has a present latest
sample, otherwise emits a single `1` sample with output labels derived from
unique equality matchers on direct selector inputs, using the same normalized
PromQL label names as stored series.
`absent_over_time(selector[range])` reads selector ranges as
`(end_ms - range, end_ms]` and emits `1` only when no non-stale sample is
present in that left-open, right-closed range; stale markers alone do not count
as present range samples, while IEEE `NaN`/`Inf` values are still present
samples. Output labels follow the same normalized unique-equality matcher
derivation as `absent()`.

Binary scalar/vector and vector/vector arithmetic/comparison/set expressions
are implemented for instant-vector inputs, including `+`, `-`, `*`, `/`, `%`,
and `^`. Arithmetic/comparison vector matching supports `on(...)`,
`ignoring(...)`, `group_left`, and `group_right`; set operators support
many-to-many `on(...)` and `ignoring(...)` matching, but not group modifiers.
The exact Prometheus stale-NaN marker makes an instant-vector sample absent
from binary expression input; present IEEE float values such as `+Inf` and
`-Inf` remain valid binary expression values.
Default vector matching uses all labels except `__name__`; `on(...)` matches
only the listed labels, including `__name__` when explicitly listed. Arithmetic
result labels use PromQL grouping-label output and drop `__name__`; non-`bool`
comparison results retain the left metric name except for one-to-one `on(...)`,
which drops `__name__` even when explicitly listed, and `group_right`
comparison results retain the right metric name.
Binary fill modifiers remain unsupported. Top-level selector queries still use
the caller's explicit read range for smoke/readback compatibility.
CLI readback verification checks decoded exact selectors/projections and, when
decoded chunk metadata supports independent expected counter math, also checks
`rate()`/`increase()` over scalar counters and cumulative or unspecified
Histogram/ExponentialHistogram `_count`, `_sum`, and sampled `_bucket`
projections. For schema 7 and schema 8, the independent oracle selects a bounded
set of exact series identities, verifies that each repeated identity resolves
to the same labels, decodes every overlapping chunk for those identities across
the corpus, orders samples by timestamp, and keeps the last duplicate before
computing exact and derived expectations. This makes multi-chunk and
cross-segment range checks isolation-safe without using production evaluator
helpers. The schema-6 comparison oracle remains record-scoped; when another
chunk with the same labelset overlaps its verification range, it exact-readback
checks that record but skips the derived range checks.
Readback diagnostics must report expected, executed, and skipped query counts,
including isolation-check skips, so real replay verification makes coverage
gaps visible instead of only showing a lower executed query count.
Delta-temporality typed histogram range readbacks are verified in focused query
tests because the PromQL path uses decoded `[start_time_ms, time_ms)` intervals
rather than pure projected counter samples.

`histogram_quantile(q, ...)` is implemented for classic `_bucket` vectors,
including production-shaped
`histogram_quantile(q, sum by (le, route)(rate(<metric>_bucket[range])))`
inputs. A first native classic Histogram path is implemented for sealed and
active-head `histogram_quantile(q, rate(metric[range]))`,
`histogram_quantile(q, sum by/without (...)(rate(metric[range])))`, and
`histogram_quantile(q, avg by/without (...)(rate(metric[range])))`: it reads
typed Histogram samples directly, computes a native histogram `rate`/`increase`
for compatible cumulative samples with identical explicit bounds, supports
native Histogram `sum`/`avg` aggregation over compatible bucket layouts, and
converts only the final quantile result back to scalar output without
materializing `_bucket` series. Native Histogram `sum`/`avg` aggregation treats
stale input samples as absent and averages over the remaining compatible
inputs. When `histogram_quantile` input contains both native histogram samples
and physical scalar bucket samples with an `le` label, the evaluator returns
both native and classic bucket quantile results; the scalar side excludes
virtual projections of the same native histograms.

A first sealed and active-head native ExponentialHistogram path is implemented
for `histogram_quantile(q, rate(metric[range]))`,
`histogram_quantile(q, sum by/without (...)(rate(metric[range])))`, and
`histogram_quantile(q, avg by/without (...)(rate(metric[range])))`: it reads
typed ExponentialHistogram samples directly, downscales compatible cumulative
samples to a common coarser scale, supports native ExponentialHistogram
`sum`/`avg` aggregation over compatible zero thresholds, consumes reset hints,
applies exponential interpolation for positive and negative exponential buckets,
clamps one-sided zero-bucket interpolation to the observed side of zero, and
trims bucket bounds adjacent to a non-zero zero threshold. Native
ExponentialHistogram `sum`/`avg` aggregation treats stale input samples as
absent and averages over the remaining compatible inputs.

Native `histogram_count(...)`, `histogram_sum(...)`, and `histogram_avg(...)`
are implemented over native Histogram/ExponentialHistogram instant-vector
results, including `rate()`/`increase()` and native `sum`/`avg` aggregation
inputs; classic bucket vectors are ignored by these native scalar functions.
Native Histogram/ExponentialHistogram `count` and `group` aggregations over
native instant-vector inputs return scalar PromQL aggregation results and count
native histogram elements directly instead of counting virtual `_bucket`,
`_count`, or `_sum` projections.
When the aggregation input contains both physical Float/Int64 scalar elements
and native Histogram/ExponentialHistogram elements, `count` and `group` combine
those scalar and native elements in the same grouping accumulator while still
excluding virtual histogram/exponential histogram projections from the scalar
side.
Inside native histogram functions, metric names ending in projection-looking
suffixes such as `_count`, `_sum`, or `_bucket` are treated as literal native
metric names, not as virtual scalar or bucket projection rewrites. Metric-name
regex matchers on `__name__` are also evaluated against literal native metric
names on the native Histogram/ExponentialHistogram path. Direct native
histogram function selectors treat labels named `le` or `quantile` as ordinary
stored labels; only virtual `_bucket` and Summary projection rewrites give
those labels synthetic projection meaning.
Native `histogram_fraction(lower, upper, expr)` is implemented over native
Histogram/ExponentialHistogram instant-vector results, including
`rate()`/`increase()` and native `sum`/`avg` aggregation inputs. Fraction bounds
may be finite or `-Inf`/`Inf`, but not `NaN`. Classic `_bucket` vectors are
ignored by this native function.

Delta-temporality scalar projections carry decoded `start_time_ms` in memory
when available; their `rate()`/`increase()` paths sum selected
`[start_time_ms, time_ms)` intervals that intersect the evaluation range, so a
single complete delta interval can produce a valid scalar virtual range result
without fabricating a second endpoint sample. Every selected non-stale delta
sample must have a present start strictly earlier than its timestamp; a
missing, equal, or later start invalidates the complete range result. A stale
no-recorded-value datapoint is a gap rather than an interval and is exempt.
Virtual delta evaluation retains at most one aligned cumulative projection
sample immediately before the logical range solely to subtract the projection
seed from the first selected interval. That predecessor is neither selected nor
validated as an in-range interval and contributes no value by itself.
Direct native Histogram and ExponentialHistogram evaluation preserves the
single-interval capability. With two or more retained delta samples, native
count and bucket evaluation converts them to an in-range cumulative-shaped
sequence and applies reset-aware Prometheus counter math, while the optional
signed sum remains the direct IEEE sum of the valid intersecting intervals.
Consequently native and virtual sums agree for signed intervals; across a
discontinuous multi-sample fragment, native count/bucket extrapolation and
virtual interval aggregation are intentionally separate tested shapes.

Classic `histogram_quantile` bucket groups with fewer than two buckets or without
a synthetic or real `le="+Inf"` bucket emit a NaN result sample, matching
Prometheus `BucketQuantile` special cases, instead of being silently dropped.
When multiple classic bucket vectors collapse to the same label group and the
same `le` bound after removing `__name__`, duplicate bucket bounds are
coalesced by summing their non-negative counts before monotonic repair and
interpolation.

Delta virtual scalar projections (`_count`, `_sum`, `_bucket`) may be accumulated
in chunk-local, segment-local, or head-local fragments before query merge.
`rate()`/`increase()` consumes aligned reset hints or value decreases across
those internal boundaries; the resulting increase is equivalent to stitching
the fragments into one in-range cumulative sequence, without exposing an
internal fragment boundary as an additional PromQL-visible stale reset.

Projected selector rewrite:
- A selector for `<metric>_bucket{le="..."}` may match real scalar bucket series with that exact name and is also rewritten to native `<metric>` with kind `HIST` or configured EXPHIST classic projection, then `le` matchers are applied after decoding/projection.
- Native virtual bucket projection supports absent `le`, equality `le="..."`,
  inequality `le!="..."`, regex `le=~"..."`, and negative regex
  `le!~"..."` matchers. Multiple `le` matchers are evaluated as a conjunction
  against the synthetic bucket label, matching normal PromQL label matcher
  behavior.
- A selector for `<metric>_count` or `<metric>_sum` is rewritten to matching native histogram/exphist/summary kinds and may also match real scalar metrics with that exact name. If real and virtual series produce the same final labelset, the query layer returns an invalid-query conflict error; it must not silently dedupe them.
- Selector indexes remain label-based over native series. Optional per-kind bitmaps may be added in `indexes.puffin` to reduce planning work, but correctness comes from `series.bin.kind_mask` and chunk-header validation.

### 11.6 Chunk sizing and logical fragmentation

In high-cardinality OTLP workloads, it is common to have millions of sparse series. If you flush a new chunk per series per head window unconditionally, you can create many tiny chunks and a large `chunk_index.bin`.

Recommendations:
- `chunk_target_bytes` is the primary trigger for all kinds.
- `chunk_target_points` is a hard cap, not the primary sizing signal for wide histogram payloads.
- A chunk always accepts at least one sample; a frame may contain exactly one oversized chunk.
- Enforce `max_schemas_per_chunk` and a per-series schema-change rate limit. On
  breach, footer schemas 6, 7, and 8 split the chunk, reject the series, or emit an
  explicit ingestion error according to policy. A raw fallback requires a
  future assigned encoding/version.
- If sparse series dominate, use block-in-progress or segment packing (§6.1) rather than reducing `segment_duration` until most chunks are single-sample.
- Query budgets (§16.6) must charge post-projection fan-out, not only native chunk reads.

---

## 12) Write flow: Sum

Note: FLOAT chunks are implemented for Gauge/Sum number datapoints.
Histogram/ExponentialHistogram/Summary native chunk persistence and first-pass
scalar projections are implemented. Typed Histogram-family/Summary value
payloads currently persist start time, OTLP datapoint flags, temporality, and
reset hints; typed chunks also append compact scalar lanes for `_count`/`_sum`.
Number datapoints currently reach storage as only `f64`/`i64` values: their
Gauge-versus-Sum source kind, start time, flags, Sum temporality, and
monotonicity are not persisted. That is a known correctness gap requiring an
ingest/head/chunk semantic change, not a `series.bin` sidecar added only at
seal. The schema-7/8 metadata-layout experiments do not claim to repair it.
The fully separated common-lane byte layout and exemplar sidecars remain
forward-looking.

Sums are currently stored as float chunks without Sum-specific metadata.

### 12.1 Input handling
The semantics-complete future Sum path must, for each OTLP Sum datapoint:
- identify series_id (labels)
- read temporality and monotonic flag
- use `(start_time, time)` to detect resets/gaps

### 12.2 Normalization choices
Future config (not implemented by the current storage path):
- `sum_mode = store_cumulative | store_delta`

Notes:
- storing cumulative is friendlier for PromQL `rate()` and counter semantics
- storing delta preserves raw semantics but requires more work in query/compaction

### 12.3 Chunk encoding
FLOAT chunk payload uses the current prefix from §11.3:
- `t0_ms, dt_ms[]`
- value encoding:
  - xor-f64 (Gorilla-style) for float

### 12.4 Flush
At head window close or size threshold:
- emit chunk(s) for each series
- append to `chunks.bin` frames
- add entry to `chunk_index.bin`

---

## 13) Write flow: Histogram, ExponentialHistogram, Summary

Note: native chunk persistence and first-pass scalar projections for these types are implemented. Start time, OTLP datapoint flags, temporality, cumulative reset hints, stale projection, DELTA Histogram count/sum/bucket projection, deterministic query-configured ExponentialHistogram bucket projection, compact `_count`/`_sum` scalar lanes, and reusable ExponentialHistogram downscale/merge helpers are implemented in the current schema-varlen path. A sealed/head native classic Histogram path exists for `histogram_quantile(q, rate(<metric>[range]))` and native Histogram `sum`/`avg` aggregation over compatible cumulative or delta Histogram samples. A sealed/head native ExponentialHistogram path exists for `histogram_quantile(q, rate(<metric>[range]))` and native ExponentialHistogram `sum`/`avg` aggregation over compatible cumulative or delta ExponentialHistogram samples. Exemplar sidecars and the fully separated common-lane byte layout remain future work.

### 13.1 Histogram input handling

Per OTLP Histogram datapoint:
- identify canonical series_id from normalized metric name and labels
- read aggregation temporality (`CUMULATIVE` or `DELTA`)
- read `start_time_unix_nano`, `time_unix_nano`, `flags`
- read `count`, optional `sum`, optional `min`, optional `max`
- read `explicit_bounds` and `bucket_counts`
- validate `explicit_bounds` are finite, non-NaN, and strictly ascending
- validate `bucket_counts.len() == explicit_bounds.len() + 1`
- validate `sum(bucket_counts) == count`

Current schema-varlen storage is raw and retains each sample's temporality.
`hist_mode = store_cumulative | store_delta` is a future normalization option,
not a current configuration field.

Rules:
- Current effective temporality is decoded per sample; `ChunkHeader.flags` is
  only an aggregate hint. No series-level effective-mode payload exists.
- Delta and cumulative histogram samples MUST NOT mix within one continuous `(series_id, HIST)` stream.
- A mid-series temporality change is a type/reset boundary. The writer either starts a new logical stream boundary or rejects the input according to policy.
- `start_time_ms` is mandatory for `store_delta` and must be strictly earlier than `time_ms`; if missing or invalid, the sample cannot be safely converted to PromQL counter semantics.

### 13.2 Histogram reset detection

For cumulative monotonic histograms, the single-writer ingestion path computes `CounterResetHint` before sealing:
- `CounterReset` if `start_time_ms` advances relative to the previous point in the stream
- `CounterReset` if `count` decreases
- `CounterReset` if present `sum` decreases for a monotonic sum
- `CounterReset` if any schema-aligned bucket count decreases
- `UnknownCounterReset` if the schema changed and buckets cannot be compared without rebucketing
- `NotCounterReset` otherwise

Schema comparison is by schema fingerprint over explicit bounds. Classic histograms with different explicit bounds are not directly bucket-comparable.

### 13.3 Histogram delta mode

For `store_delta`, each datapoint represents the interval `[start_time_ms, time_ms)`.

Query-time merge for PromQL:
- select delta points whose interval intersects the query evaluation range
- align schemas only when explicit bounds match exactly
- additive fields: bucket-wise sum `count`, `bucket_counts`, and present signed IEEE `sum` over the selected intervals; a negative or non-finite optional sum does not invalidate count/bucket shape
- extrema fields: merged `min` is `min(min_i)` over present values; merged `max` is `max(max_i)` over present values; never sum `min` or `max`
- expose cumulative-shaped virtual projections to PromQL; do not expose raw deltas as counter samples
- gaps in start/end continuity create reset/unknown boundaries

Native typed aggregation of classic histograms requires identical explicit bounds. If bounds differ inside a native aggregation group, query code must drop that group from the native result until a warning/reporting channel exists. It must not interpolate classic bucket layouts.

### 13.4 ExponentialHistogram input handling

Per OTLP ExponentialHistogram datapoint:
- identify canonical series_id from normalized metric name and labels
- read aggregation temporality (`CUMULATIVE` or `DELTA`)
- read `start_time_unix_nano`, `time_unix_nano`, `flags`
- read `count`, optional `sum`, optional `min`, optional `max`
- read `scale`, `zero_count`, `zero_threshold`
- read positive and negative bucket `offset` and `counts[]`
- validate `zero_count + sum(positive_counts) + sum(negative_counts) == count`

Current storage is raw and retains each sample's temporality and scale.
`exphist_mode = store_cumulative | store_delta` is a future normalization
option. `keep | downscale_to_max_scale(K)` is currently a query/merge policy,
not a persisted storage mode.

Rules:
- Current effective temporality is decoded per sample; `ChunkHeader.flags` is
  only an aggregate hint. No series-level effective-mode payload exists.
- Delta and cumulative ExponentialHistogram samples MUST NOT mix within one continuous `(series_id, EXPHIST)` stream.
- `start_time_ms` is mandatory for `store_delta` and must be strictly earlier than `time_ms`; stale no-recorded-value gaps are exempt during range evaluation.
- `zero_threshold` is part of the schema.
- A `zero_threshold` change is a schema/layout boundary; cumulative reset detection must emit `UnknownCounterReset` unless the query path rejects cross-threshold native merge and routes through a projection that preserves correctness.

### 13.5 ExponentialHistogram reset detection

For cumulative monotonic ExponentialHistograms, ingestion computes `CounterResetHint`:
- `CounterReset` if `start_time_ms` advances relative to the previous point
- `CounterReset` if `count`, `zero_count`, or present monotonic `sum` decreases
- `CounterReset` if any comparable positive or negative bucket decreases
- If scales differ, first downscale finer samples to the common coarser scale (§13.6), then apply bucket-decrease tests on comparable bucket indexes.
- `UnknownCounterReset` if comparison would require coarser-to-finer upscaling or another non-lossless rebinning step.
- `UnknownCounterReset` if `zero_threshold` changes.
- `UnknownCounterReset` if bucket layout changes and cannot be losslessly rebinned to a comparable coarser layout.
- `NotCounterReset` otherwise

Query merge must consume the stored hint. It must not attempt to re-derive reset behavior from decoded bucket values alone.

### 13.6 ExponentialHistogram downscale and merge

Downscaling is deterministic and works by folding adjacent buckets into coarser buckets.

For downscale by `k >= 1`:
```
target_scale = source_scale - k
target_index = floor_div(source_index, 2^k)
target_count[target_index] += source_count[source_index]
target_offset = min(target_index over retained buckets)
```

`floor_div` is mathematical floor division and must be used for negative bucket indexes. Repeated downscale-by-1 and one downscale-by-k must produce identical output.

Merge policy:
- To merge multiple ExponentialHistogram samples, choose `target_scale = min(scale_i)` unless `downscale_to_max_scale(K)` forces a lower maximum scale.
- Downscale all samples to `target_scale`, then sum additive fields: `count`, `zero_count`, matching positive/negative bucket indexes, and present `sum`.
- Extrema fields are not additive: merged `min` is `min(min_i)` over present values; merged `max` is `max(max_i)` over present values.
- `zero_threshold` participates in layout compatibility. Native EXPHIST merge in v1 rejects differing `zero_threshold` values and must route through a projection or return a typed incompatibility error; it must not sum matching bucket indexes across different zero regions.
- Lossless rebinning is only finer-to-coarser. Coarser-to-finer is not allowed.

Current implementation:
- `chronoxide_core::storage::head::downscale_exponential_histogram` folds dense positive and negative spans using mathematical floor division, including negative bucket indexes.
- `merge_exponential_histograms` rejects `zero_threshold` mismatches, downscales to the target scale, sums additive fields, and merges extrema as min/max.
- Ingester cumulative reset detection uses the same downscale-to-map helper before bucket-decrease comparison.

### 13.7 Summary input handling and projection

Per OTLP Summary datapoint:
- identify canonical series_id from normalized metric name and labels
- read `start_time_unix_nano` if present, `time_unix_nano`, `flags`
- read `count`, `sum`, and quantile pairs
- validate quantile positions are sorted and in `[0, 1]`

Encoding:
- `SUMMARY/SCHEMA_VARLEN` (§11.4); footer schemas 6, 7, and 8 define no raw
  fallback

Projection contract:
- `<metric>_count`: scalar count series
- `<metric>_sum`: scalar sum series
- `<metric>{quantile="q"}`: quantile gauge series
- quantile gauge samples are not mergeable across series or time ranges and are not valid inputs to `rate()`/`increase()`

### 13.8 Exemplars

OTLP NumberDataPoint, HistogramDataPoint, and ExponentialHistogramDataPoint may carry exemplars. SummaryDataPoint does not.

Future exemplar persistence may be controlled by:
- `store_exemplars = false | true | sampled(N)`

When enabled:
- it requires a new chunk encoding or segment version and a specified
  sidecar/index byte format; footer schemas 6, 7, and 8 have no exemplar flag or
  sidecar.
- future exemplar sidecars must be chunk-keyed, reuse `symbols.bin` for filtered attributes, and round-trip exemplar time, value, span_id, and trace_id exactly.
- disabling exemplars must not affect metric sample correctness.

### 13.9 Flush

At head window close or size threshold:
- build typed chunks using the kind and encoding rules in §11
- append in-order samples to `chunks.bin`
- append accepted OOO samples to `ooo_chunks.bin`
- add entries to `chunk_index.bin`
- preserve typed start time, flags, temporality, and reset hints in each native
  value and typed scalar-lane row

---

## 14) Out-of-order / late samples (OOO lane)

Config:
- `out_of_order_time_window = 30m` (example)

Mechanics:
- Maintain `max_event_time_seen` per shard and per series `last_time`.
- If a point’s time < series `last_time`, it is OOO.
- OOO/late samples may be accepted without advancing ingest watermark
  (policy `AcceptNoAdvance`; see `docs/spec/clock.md`).
- Accept OOO if within window; otherwise:
  - drop, or
  - send to a backfill lane (optional feature)

Storage:
- OOO points are flushed into `ooo_chunks.bin` with their own chunk_index entries.

Query:
- merge in-order and OOO iterators for a `(series_id, kind)` stream over the requested time range.

**Duplicate timestamps / replays (deterministic dedupe)**  
Duplicates can happen due to retries/replays and because OOO points may overlap already-flushed in-order data.

Policy (PromQL-friendly):
- **Within a flush/chunk build**: sort points by `(kind, timestamp, ingest_order)` and keep only the last point for each `(kind, timestamp)` (last-write-wins).
- **At query merge time**: if multiple sources produce a sample at the same timestamp for the same `(series_id, kind)` stream, return **one** sample using this deterministic precedence order:
  1. Head (newest ingestion) > sealed segments
  2. Newer sealed segment (later manifest order) > older sealed segment
  3. Within a segment: OOO lane (`ooo_chunks.bin`) > in-order lane (`chunks.bin`)
  4. Within the same lane: later chunk entry order (as stored in `chunk_index.bin`) wins

Different chunk kinds for the same canonical labelset are not duplicates. A FLOAT sample and a HIST sample at the same timestamp are separate streams. If a query path cannot represent both, it must return a type conflict or route through the projection rules in §11.5.

**Segment interaction (immutability requirement)**  
Because sealed segments are immutable (§2), late points whose event time falls into an already-sealed segment cannot be appended to that segment. Pick one (and document it) to stay SSD-friendly:
- **Delay sealing**: keep each head window writable for at least `out_of_order_time_window`, then seal once lateness has aged out (higher RAM, fewer overlapping segments).
- **Overlapping OOO segments**: write late points into separate OOO-only segments named by their *event-time* min/max, allowing overlap with already-sealed in-order segments (more segments, requires merge/dedupe at read and/or later compaction).

---

## 15) PromQL selector indexes (including regex) — Greptime-inspired

PromQL label matchers:
- `=`, `!=`, `=~`, `!~`

Regex matchers are expensive if you scan all label values. We avoid that with a dedicated inverted index.

### 15.1 Index container: `indexes.puffin`

`indexes.puffin` stores immutable index payloads plus lazy, page-framed
directories. Footer schema 6 freezes container version `7`, which replaced the
version-6 millions-entry footer with fixed root locators and checksummed
directory pages. Footer schema 7 requires container version `8`, defined below,
so every correctness-affecting lazily read payload has expected count and
checksum metadata protected by its directory. Footer schema 8 requires
container version `9`, which retains that integrity-check chain and adds the
adaptive exact-postings payload defined in §15.1.3. All integer fields are
little-endian.

#### 15.1.1 Schema-6 container v7 baseline

This byte layout remains normative only for the explicit library-level
schema-6 writer used by format tests and the validated schema-6 A/B adapter.
It is not accepted by strict Schema-7 or Schema-8 readers.

Physical order is deterministic:

```
SegmentIndexesHeaderV7
RoutingPayload?                 // physically first when present
MetricSeriesRangesPayload       // required
ExactPostingsPayloadRegion      // zero or more existing postings blobs
AuxiliaryPayloadRegion          // label FST and label-time-range blobs
ExactDirectoryV1                // header + page descriptors
ExactDirectoryPage[page_count]  // fixed 16 KiB pages
AuxiliaryDirectoryV1            // header + fixed-width records
SegmentIndexesTrailerV7         // exactly 256 bytes at EOF
```

The header is exactly 16 bytes:

```
SegmentIndexesHeaderV7:
  u32 magic         // 'SIDX'
  u16 version       // 7
  u16 flags         // 0
  u32 header_len    // 16
  u32 reserved      // 0
```

A locator is exactly 16 bytes:

```
BlobLocatorV7:
  u64 offset        // absolute file offset
  u64 len
```

The trailer is exactly 256 bytes and begins at `file_len - 256`:

```
SegmentIndexesTrailerV7:
  u32 magic                         // 'SIDT'
  u16 version                       // 7
  u16 flags                         // 0
  u32 trailer_len                   // 256
  u32 reserved0                     // 0
  u64 file_len                      // must equal the actual file length
  BlobLocatorV7 routing             // optional; {0,0} means absent
  BlobLocatorV7 metric_ranges       // required
  BlobLocatorV7 exact_directory     // required, even when empty
  BlobLocatorV7 exact_pages         // empty iff exact_entry_count == 0
  BlobLocatorV7 exact_postings      // empty iff exact_entry_count == 0
  BlobLocatorV7 auxiliary_directory // required, even when empty
  BlobLocatorV7 auxiliary_payloads  // empty iff auxiliary_entry_count == 0
  u64 exact_entry_count
  u32 exact_page_count
  u32 exact_record_len              // 40
  u32 exact_page_len                // 16384
  u32 auxiliary_entry_count
  u32 trailer_crc32c                // CRC over all 256 bytes with this field zero
  u8  reserved1[88]                 // all zero
  u32 terminal_magic                // 'S7ND'
```

Every non-empty top-level locator must lie between the header and trailer.
Top-level ranges must not overlap, and `offset + len` uses checked arithmetic.
An optional locator must have both fields zero or both fields non-zero. Readers
reject unknown flags, non-zero reserved bytes, inconsistent counts, or a file
length mismatch before following any locator.

The exact-postings directory header is exactly 64 bytes:

```
ExactDirectoryHeaderV1:
  u32 magic              // 'EXD7'
  u16 version            // 1
  u16 flags              // 0
  u32 header_len         // 64
  u32 descriptor_len     // 32
  u32 page_len           // 16384
  u32 record_len         // 40
  u64 entry_count
  u32 page_count
  u32 records_per_page   // 409
  u64 descriptors_offset // 64, relative to exact_directory.offset
  u64 descriptors_len    // page_count * 32
  u32 directory_crc32c    // CRC over header + descriptors with this field zero
  u32 reserved           // 0
```

Each page descriptor is exactly 32 bytes:

```
ExactPageDescriptorV1:
  u32 first_label_name_sym
  u32 first_label_value_sym
  u32 last_label_name_sym
  u32 last_label_value_sym
  u32 record_count
  u32 reserved0          // 0
  u32 page_crc32c        // CRC over the complete 16 KiB page
  u32 reserved1          // 0
```

Descriptors are strictly ordered, their key ranges do not overlap, and every
page except the final page contains 409 records. The page offset is derived as
`exact_pages.offset + page_index * 16384`; it is not stored independently.
`page_count` must equal `ceil(exact_entry_count / 409)`. When the entry count is
zero, the page count and exact-pages length are zero. Otherwise the final page
contains `1..=409` records, `exact_directory.len` is exactly
`64 + page_count * 32`, and `exact_pages.len` is exactly
`page_count * 16384`.

Each exact-directory page is exactly 16 KiB:

```
ExactDirectoryPageV1:
  u32 magic        // 'XPG7'
  u16 version      // 1
  u16 flags        // 0
  u32 page_index
  u32 record_count
  ExactDirectoryRecordV1[record_count]
  u8 zero_padding[]
```

An exact record is exactly 40 bytes:

```
ExactDirectoryRecordV1:
  u32 label_name_sym
  u32 label_value_sym
  u64 postings_offset    // absolute file offset
  u64 postings_len
  u64 min_time_ms
  u64 max_time_ms
```

Records are strictly sorted and unique by
`(label_name_sym, label_value_sym)`. Every posting range must lie wholly within
the trailer's exact-postings region, have a byte length of `4 + count * 4`, and
satisfy `min_time_ms <= max_time_ms`. Every symbol in a touched descriptor
fence or record key is less than the authoritative same-generation
`symbols.bin` count. The payload begins with its little-endian
`u32 count`, followed by exactly `count` strictly increasing, unique little-endian
`u32 series_ref` values. The count is non-zero. Readers validate the encoded
count and exact byte length before allocating the result vector.

The auxiliary directory is read as one compact blob. Its header is 64 bytes:

```
AuxiliaryDirectoryHeaderV1:
  u32 magic          // 'AUX7'
  u16 version        // 1
  u16 flags          // 0
  u32 header_len     // 64
  u32 record_len     // 40
  u64 entry_count
  u64 records_offset // 64, relative to auxiliary_directory.offset
  u64 records_len    // entry_count * 40
  u32 directory_crc32c // CRC over header + records with this field zero
  u8  reserved[20]   // all zero
```

An auxiliary record is exactly 40 bytes:

```
AuxiliaryDirectoryRecordV1:
  u16 kind            // 2 = label FST, 3 = label-value time ranges
  u16 flags           // 0
  u32 label_name_sym
  u64 payload_offset  // absolute file offset
  u64 payload_len
  u64 min_time_ms
  u64 max_time_ms
```

Auxiliary records are strictly sorted and unique by `(kind, label_name_sym)`.
Every auxiliary payload must have non-zero length, and every payload range must
lie wholly within the auxiliary-payload region. A kind-2 FST must contain at
least one value; writers reject both semantically empty FSTs and other
zero-length auxiliary payloads instead of introducing implicit padding.
Every auxiliary `label_name_sym`, and every `label_value_sym` decoded from a
kind-3 payload, is less than the authoritative same-generation `symbols.bin`
count.
`auxiliary_directory.len` is exactly `64 + auxiliary_entry_count * 40`.

A kind-3 label-value time-range payload begins with a little-endian `u32`
entry count followed by exactly that many 20-byte records. Each record contains
`u32 label_value_sym`, `u64 min_time_ms`, and `u64 max_time_ms`. Value symbols
are strictly increasing and unique, the entry count is non-zero, and every
record satisfies `min_time_ms <= max_time_ms`. The kind-3 directory time range
equals the aggregate minimum and maximum across its payload records. When a
kind-2 FST record exists for the same label name, its time range is identical to
the kind-3 summary. A kind-2 record without a matching kind-3 record uses the
canonical unconstrained range `[0, u64::MAX]`. Readers validate the count-derived
exact payload length before allocating the decoded range vector.

Fast open reads only the 16-byte header and 256-byte trailer. Exact-directory
descriptors are loaded on the first exact lookup; a lookup binary-searches the
descriptors and reads at most one checksummed 16 KiB page. The auxiliary
directory is loaded only by label-value discovery, regex planning, or
label-value time pruning. Lazy directory corruption is returned as an I/O error
and must never be interpreted as a missing label or an empty result.

Before emitting this container, the production segment writer checks every
exact/auxiliary/metric symbol reference and every exact-postings series
reference against the authoritative symbol and series counts produced by the
same seal operation. The raw codec is private and production-reachable only
through that root-bound validation; its unbound entry point is test-only.

Container v7 retains the existing payload encodings, including routing-index
version 2, exact-postings payloads, label FSTs, label-value time ranges, and
metric-series ranges. Its page and directory CRCs distinguish touched directory
corruption from absence, but exact-postings, FST, and label-value time-range
payload bodies have no locally expected checksum. Complete `footer.bin`
validation is therefore mandatory for the schema-6 A/B exception and remains
outside timed queries; production schemas 7 and 8 do not inherit this
limitation.

Known blob kinds:
- `1`: exact postings for one `(label_name_sym, label_value_sym)`
- `2`: label-value FST for one `label_name_sym`
- `3`: label-value time ranges for one `label_name_sym`
- `4`: routing metadata for capability-gated early segment pruning
- `5`: metric-series ranges for metric-name equality routing

The routing metadata blob should be physically first in `indexes.puffin` so a
reader can fetch the routing header and a small number of fixed-size lookup
buckets before deciding whether to open `symbols.bin`, `series.bin`,
`chunk_index.bin`, or chunk files. The fixed trailer locates this blob without
opening either lazy directory.

Schema-6 segment-index version `7` requires metric-series ranges and both
directory headers. Its reader rejects version-6 containers and segments that
omit required locators. Schemas 7 and 8 reject version 7 entirely; old
experimental segments must be regenerated rather than read through a
compatibility path.

#### 15.1.2 Schema-7 container v8 integrity-checked payloads

Container version `8` is the only index-container version valid under footer
schema 7. It preserves the v7 physical region order and the existing routing
v2, metric-series-ranges v1, exact-postings body, raw FST body, and label-value
time-range body encodings. It changes the fixed root and the exact/auxiliary
directories so an ordinary touched read integrity-checks every payload that can
change equality membership, regex completeness, or label-value time pruning.

The deterministic physical order is:

```text
SegmentIndexesHeaderV8
RoutingPayloadV2?                  // unchanged; non-authoritative by default
MetricSeriesRangesPayloadV1       // unchanged; non-authoritative by default
ExactPostingsPayloadV2Region
AuxiliaryPayloadV2Region
ExactDirectoryV2                  // header + page descriptors
ExactDirectoryPageV2[page_count]  // fixed 16 KiB pages
AuxiliaryDirectoryV2              // header + fixed-width records
SegmentIndexesTrailerV8           // exactly 256 bytes at EOF
```

`SegmentIndexesHeaderV8` is the v7 16-byte header with `version == 8`; its
magic, flags, header length, and zero reserved field are unchanged. A v8 blob
locator remains the same 16-byte `{u64 offset, u64 len}` pair.

Unlike the frozen v7 reader's non-overlap-only rule, the v8 physical layout is
canonical and gap-free. Each present region begins exactly at the end of the
previous present region, the first present region begins at offset 16, and the
last directory ends exactly where the 256-byte trailer begins. An absent
optional or canonically empty region uses `{0, 0}`, consumes no bytes, and does
not interrupt adjacency between present regions. Any other byte between the
header and trailer is unaccounted and therefore corruption.

The v8 trailer remains exactly 256 bytes. Fields through offset 160 retain
their v7 offsets, but the exact-record length changes and former reserved bytes
bind the lazy directories to authoritative root counts:

```text
SegmentIndexesTrailerV8:
  u32 magic                            // 'SIDT', offset 0
  u16 version                          // 8
  u16 flags                            // 0
  u32 trailer_len                      // 256
  u32 reserved0                        // 0
  u64 file_len                         // offset 16; exact physical length
  BlobLocatorV8 routing                // offset 24
  BlobLocatorV8 metric_ranges          // offset 40
  BlobLocatorV8 exact_directory        // offset 56
  BlobLocatorV8 exact_pages            // offset 72
  BlobLocatorV8 exact_postings         // offset 88
  BlobLocatorV8 auxiliary_directory    // offset 104
  BlobLocatorV8 auxiliary_payloads     // offset 120
  u64 exact_entry_count                // offset 136
  u32 exact_page_count                 // offset 144
  u32 exact_record_len                 // offset 148; exactly 48
  u32 exact_page_len                   // offset 152; exactly 16384
  u32 auxiliary_entry_count            // offset 156
  u32 trailer_crc32c                   // offset 160; this field zeroed
  u32 series_count                     // offset 164
  u32 symbol_count                     // offset 168
  u32 exact_directory_crc32c           // offset 172
  u32 auxiliary_directory_crc32c       // offset 176
  u8  reserved1[72]                    // offsets 180..251; all zero
  u32 terminal_magic                   // offset 252; 'S8ND'
```

`trailer_crc32c` covers all 256 bytes with only its own field zeroed. Before
following any non-root locator, the schema-7/8 metadata session must require
`series_count` and `symbol_count` to equal the authoritative counts obtained
from the same registered generation's validated series and symbol roots.
Caller-supplied counts do not authorize a read. The two stored directory CRCs
must equal both the corresponding directory's encoded CRC field and a fresh
CRC over that exact directory with its CRC field zeroed. A mismatch is root or
directory corruption, never an absent index.

The exact-postings directory v2 header remains 64 bytes:

```text
ExactDirectoryHeaderV2:
  u32 magic              // 'EXD8'
  u16 version            // 2
  u16 flags              // 0
  u32 header_len         // 64
  u32 descriptor_len     // 32
  u32 page_len           // 16384
  u32 record_len         // 48
  u64 entry_count
  u32 page_count
  u32 records_per_page   // 341
  u64 descriptors_offset // 64, relative to exact_directory.offset
  u64 descriptors_len    // page_count * 32
  u32 directory_crc32c    // header + descriptors; this field zeroed
  u32 reserved            // 0
```

`ExactPageDescriptorV2` retains the v1 32-byte layout and complete-page CRC.
Descriptors and key fences retain the v7 ordering, uniqueness, symbol-bound,
and canonical-page-count rules, with `341` replacing `409`. Thus
`page_count == ceil(exact_entry_count / 341)`, every non-final page contains
341 records, and the final page contains `1..=341` records. A complete page is
exactly `16 + 341 * 48 == 16,384` bytes.

Each exact page uses magic `XPG8`, version `2`, zero flags, and the unchanged
16-byte page header. Its record is:

```text
ExactDirectoryRecordV2:             // exactly 48 bytes
  u32 label_name_sym                // offset 0
  u32 label_value_sym               // offset 4
  u64 postings_offset               // offset 8; absolute file offset
  u64 postings_len                  // offset 16
  u64 min_time_ms                   // offset 24
  u64 max_time_ms                   // offset 32
  u32 ref_count                     // offset 40; non-zero
  u32 payload_crc32c                // offset 44
```

The exact-postings v2 payload remains compact and has no redundant magic
because the v8 root and v2 directory record unambiguously select its decoder:

```text
u32 ref_count
u32 series_ref[ref_count]
```

`postings_len` is exactly `4 + 4 * ref_count`. `payload_crc32c` covers every
payload byte, including the encoded count. On every touched read, the reader
checks the payload CRC before allocation, requires the body count to equal the
directory `ref_count`, requires the exact count-derived length, requires refs
to be strictly increasing and unique, and requires every ref to be less than
the trailer's root-bound `series_count`. The exact-page CRC binds the expected
count, payload checksum, locator, time range, and label key. An in-range,
ordered, same-length ref substitution is therefore corruption rather than a
different candidate set.

The auxiliary directory v2 header remains 64 bytes:

```text
AuxiliaryDirectoryHeaderV2:
  u32 magic            // 'AUX8'
  u16 version          // 2
  u16 flags            // 0
  u32 header_len       // 64
  u32 record_len       // 48
  u64 entry_count
  u64 records_offset   // 64, relative to auxiliary_directory.offset
  u64 records_len      // entry_count * 48
  u32 directory_crc32c // header + records; this field zeroed
  u8  reserved[20]     // all zero
```

Each auxiliary record is:

```text
AuxiliaryDirectoryRecordV2:         // exactly 48 bytes
  u16 kind                          // 2 = FST, 3 = label-value time ranges
  u16 flags                         // 0
  u32 label_name_sym                // offset 4
  u64 payload_offset                // offset 8; absolute file offset
  u64 payload_len                   // offset 16; non-zero
  u64 min_time_ms                   // offset 24
  u64 max_time_ms                   // offset 32
  u32 item_count                    // offset 40; non-zero
  u32 payload_crc32c                // offset 44
```

The auxiliary-directory ordering, uniqueness, locator bounds, time ordering,
symbol bounds, FST/range summary agreement, and canonical unconstrained FST
summary rules remain unchanged. `auxiliary_directory.len` is exactly
`64 + auxiliary_entry_count * 48`. For both kinds, `payload_crc32c` covers the
complete exact payload bytes and is checked before parsing or allocation.

For kind 2, the payload remains the raw deterministic FST byte stream.
`item_count` equals its non-zero distinct-value count. After the CRC succeeds,
the reader validates the FST, requires `fst::Set::len() == item_count`, and
requires every visited value to be valid UTF-8. A value emitted to selector
planning must resolve through the bound symbol root; failure is corruption and
must not be skipped as a missing value.

For kind 3, the payload remains:

```text
u32 item_count
repeated item_count times:
  u32 label_value_sym
  u64 min_time_ms
  u64 max_time_ms
```

Its exact length is `4 + 20 * item_count`. The body count must equal the
directory count before allocation. Value symbols are strictly increasing,
unique, and less than the bound `symbol_count`; each time range is ordered and
their aggregate equals the directory summary. When kind 2 and kind 3 records
both exist for one label, their `item_count` values and time summaries must
match. The production writer derives both from the same complete sealed label
inventory; an unbound encoder remains test-only.

An opaque exact or auxiliary selection carries its complete v8 root, protected
directory record, expected count, checksum, and segment-generation provenance.
Cache values retain and recheck that context, even when the physical
`(offset, length)` key hits. CRC, count, root, symbol-resolution, ordering, or
context disagreement enters the sticky artifact-corruption ledger. It must not
be converted into a missing matcher, empty regex expansion, time prune, cache
miss, skipped series, or partial result.

Container v8 does not add local integrity checks to routing v2 or
metric-series-ranges v1. Those payloads are not correctness-affecting on an
ordinary schema-7 query path: without opaque authority minted by complete
same-generation semantic validation, they may inform ordering or prefetch but
must not remove a segment or candidate. A local payload CRC verifies
emitted bytes; it does not by itself prove that a buggy writer derived a
correct semantic summary.

The v8-aware real-corpus model observes 3,563,222 exact entries, 8,722 v7 exact
pages, and 33,322 auxiliary entries. Applying the specified 48-byte records and
341-record page density yields 10,458 v8 exact pages and adds 28,764,752 bytes
to `indexes.puffin`. Against the selected inline-series layout, the resulting
net projected saving is 2,257,877,360 bytes: 10.48% of all modeled standard
artifacts and 21.21% of modeled metadata. The exact provenance and held-constant
symbol-byte caveat are recorded in the
[schema-7 layout model](../../experiments/storage_vnext/2026-07-13-schema7-layout-model.md).

Those remain structural capacity estimates. The first encoded two-million-
message prefix result measured an 11.94% total-artifact reduction and a
matching focused schema-neutral readback fingerprint; it also found slower
schema-7 sealing and higher metadata bytes/cache retention. The exact scope,
raw-output root, and measurements are recorded in the
[schema-7 prefix result](../../experiments/storage_vnext/2026-07-14-schema7-prefix-results.md).
The optimized paired PromQL run at
`storage-schema7-promql-batched-20260714-171150` matched all 24 semantic and
complete-`QueryStats` pairs. Its directional noisy-host medians put schema 7
4.5 to 6.6 times ahead of schema 6 across scalar, regex, native Histogram, and
native ExponentialHistogram cold/warm shapes, with lower peak RSS. Exhaustive
metadata equivalence, write-side optimization, corruption coverage, and the
remaining promotion gates are still required. Estimates are not measured
evidence.

This is an alpha breaking change. There is no schema-7 v7 compatibility path
and no in-place upgrade. Schema-7 corpora are regenerated by deterministic
replay into a new output root, which rewrites `indexes.puffin`, `footer.bin`,
and the other schema-7 artifacts together. A named schema-6 v7 corpus may be
retained only for the validated A/B exception described above.

Required v8 writer and reader deterministic/corruption coverage includes:

- empty, one-entry, 341-entry, and 342-entry exact-directory golden bytes;
- an ordered in-range exact ref mutation with unchanged count and length, and
  an equal-length exact-payload swap between keys, both failing CRC rather than
  changing candidates;
- body/directory count mismatch, valid-CRC out-of-range refs, root-bound
  series/symbol-count mismatch before payload I/O, and truncation at every
  fixed or variable boundary;
- a structurally valid same-length FST replacement or swap, a range mutation
  preserving length, ordering, count, and aggregate summary, and FST/range
  item-count disagreement, all reported as corruption;
- directory and trailer checksum disagreement, foreign root/generation/cache
  context, and corruption remaining sticky after cache and FD eviction; and
- equality, regex, discovery, and time-pruning integration cases proving a
  touched failure returns an error instead of a wrong, empty, pruned, skipped,
  or partial result.

#### 15.1.3 Schema-8 container v9 adaptive exact postings

Container version `9` is the only index-container version valid under footer
schema 8. It retains the complete v8 physical region order, fixed sizes, root
counts, locators, directory records, auxiliary payloads, CRC chain, governor,
and generation-binding rules. Its header and trailer use version `9`, its
terminal magic is `S9ND`, its exact-directory header uses magic `EXD9` and
version `3`, and its exact pages use magic `XPG9` and version `3`. The
unchanged auxiliary directory remains `AUX8` version `2`.

The v9 exact record remains the 48-byte `ExactDirectoryRecordV2`. Its
`ref_count` is authoritative and its exact payload is:

```text
u8 codec       // 0 = RAW32, 1 = DELTA_ULEB128
u8 flags       // 0
u16 reserved   // 0
u8 body[]
```

RAW32 stores exactly `ref_count` little-endian `u32` references. Its complete
payload length is `4 + 4 * ref_count`, so it has no byte overhead relative to
the v8 count-prefixed raw payload. DELTA_ULEB128 stores the first reference as
an absolute canonical unsigned LEB128 value followed by exactly
`ref_count - 1` canonical unsigned LEB128 positive gaps. Every delta addition
is checked.

The writer selects DELTA_ULEB128 only when its complete length is strictly
smaller than RAW32; RAW32 wins ties. Readers enforce that canonical selection,
consume exactly the declared count and complete payload, and reject an unknown
codec, nonzero flags/reserved bytes, truncated/overlong/noncanonical varints,
zero gaps, arithmetic overflow, trailing bytes, non-increasing refs, or refs
outside the root-bound series count. The payload CRC is verified before parsing
or allocation. Before payload I/O the protected record must satisfy checked
`4 + ref_count <= postings_len <= 4 + 4 * ref_count`.

Routing v2 remains byte-compatible except that every
`exact_postings_blob_len` is the actual selected v9 encoded length. The
same-seal writer recomputes the adaptive choice and rejects a raw-length or
otherwise inconsistent routing entry before publication. Footer validation
verifies the published routing bytes but does not yet re-run a complete
semantic routing walk; that remains required future validator coverage.

The complete version boundary, deterministic writer rule, non-goals,
corruption matrix, and replay/query acceptance gate are normative in
[the focused schema-8 design](2026-07-15-storage-schema8-adaptive-postings-design.md).

### 15.2 Required index blobs

#### (A) Postings dictionary
Mapping:
- `(label_name_sym, label_value_sym) -> postings_bitmap_id`

Exact postings contain sorted `series_ref` / `u32` values. Schema 6 and Schema
7 encode every list as RAW32. Schema 8 deterministically chooses RAW32 or
canonical delta unsigned-LEB128 per list as specified in §15.1.3; RAW32 wins
ties. No current exact-postings version uses Roaring containers.

#### (B) Per-label value FST
For each label name:
- build an FST (finite state transducer) over **sorted distinct label values**
- leaf stores `label_value_sym` (or postings_bitmap_id)

Usage:
- prefix scans: enumerate all values under a prefix
- regex scans: compile regex into an automaton; traverse FST to enumerate matching values
- exact match: direct lookup (fast path)
- alternation set (`a|b|c`): lookup each literal, union postings

This is the key “regex accelerator”: you enumerate only matching values, not all values.

#### (C) Optional: bloom filters / min-max stats
Per segment or per series group:
- bloom on `series_id` presence (useful when you already have a candidate set of `series_id`s from higher-level planning and want to skip segments fast)
- time bounds per series in index (already in `chunk_index.bin`, but can be summarized here)

#### (D) All-series bitmap (for negative-only selectors)
A bitmap (or implicit range) of **all `series_ref`s present in the segment** (typically `0..num_series-1`).

This is needed to execute selectors that contain only negative matchers, e.g. `{job!="api"}` or `{pod!~"^kube-.*"}`, without scanning all series.

#### (E) Routing metadata blob
This blob exists to skip whole segments for selective positive equality
matchers without opening `symbols.bin` or decoding full postings.

Encoding for blob kind `4` is a point-lookup hash table. It must answer exact
positive equality matchers using normalized PromQL strings directly, without
reading `symbols.bin` and without deserializing all routing entries.

```
RoutingIndexV2Header:
  u32 magic             // 'RIDX'
  u16 version           // 2
  u16 flags             // 0
  u32 entry_count
  u32 bucket_count      // power of two; > entry_count
  u64 buckets_offset    // >= sizeof(RoutingIndexV2Header)
  u64 key_bytes_offset
  u64 key_bytes_len

RoutingBucket[bucket_count]:
  u64 key_hash          // deterministic FNV-1a over RoutingKey bytes
  u32 key_offset        // relative to key_bytes_offset
  u32 key_len           // 0 means empty bucket
  u64 min_time_ms
  u64 max_time_ms
  u64 exact_postings_blob_len

RoutingKey:
  u32 label_name_len
  u8[label_name_len] label_name_utf8
  u8[...] label_value_utf8
```

Buckets use linear probing. Writers choose a bucket count that keeps load factor
below 0.5. A lookup builds `RoutingKey` from the normalized matcher
`label_name/value`, hashes it, probes buckets until it finds an empty bucket or a
matching key. It reads and validates the key bytes of every occupied bucket in
the probe chain so corrupt collision metadata cannot be treated as absence.
Strings are normalized PromQL label names/values as used by queries, not
segment-local symbols. This is intentional: it lets the read path answer "can
this equality matcher exist in this segment and overlap this query time range?"
before loading `symbols.bin`.

`exact_postings_blob_len` is the byte length of the exact-postings blob that
would be read if the segment survives pruning. Query planning uses it to order
multiple equality matchers by cheapest postings read.

Routing flags must be zero. An empty routing bucket is canonical only when all
of its fields are zero. For every non-empty bucket, the key range must lie
wholly within `key_bytes_len`, the stored key must be a valid `RoutingKey`, its
FNV-1a hash must equal `key_hash`, `exact_postings_blob_len` must be non-zero,
and `min_time_ms <= max_time_ms`. A point reader validates every non-empty
bucket it probes, including hash-mismatching collision buckets; malformed
routing metadata is an error and must not be interpreted as a missing matcher
or a time-range prune.

The production writer derives the routing table from the complete exact
postings, label-value time ranges, and authoritative symbol dictionary produced
by the same seal. Every exact-postings key must resolve both symbols and have
one matching time range. Missing ranges, unresolved symbols, duplicate
normalized keys, or disagreement between a supplied routing table and that
derivation are writer errors; a writer must never omit such an entry and still
publish the segment.

`RoutingIndexV2` has no local payload checksum. Root-bound, governed point reads
therefore prove only the structure of the header, every bucket in the touched
probe chain, and every touched key. They do not prove that a valid-looking
empty bucket, key substitution, time range, or postings length. Absence and
time-range pruning are authoritative only while the query holds an opaque,
same-generation routing authority minted after complete-file validation and
semantic cross-artifact verification against exact postings, time ranges,
symbols, and series/chunk facts. Footer hashing alone does not mint that
authority. Without it, routing metadata may inform ordering or prefetch only;
it must not remove a segment or candidate.

PromQL evaluates a matcher against the empty string when its label is absent.
Consequently, an equality matcher with an empty value, or any regex matcher
whose predicate accepts the empty string, must never use routing absence as a
prune: a missing stored label is itself a match. The same rule applies to all
postings-based candidate planning below.

#### (F) Metric-series ranges blob
This required blob maps a metric-name symbol id to the contiguous
`series_ref` ranges for that metric in `series.bin` physical order. The key is
the symbol id for a `__name__` label value, not the symbol id for the
`__name__` label key.

The same metric can still have many labelsets. The range index helps by turning
`{__name__="metric"}` into the set of all series for that metric without reading
the exact postings blob for `(__name__, metric)`. Other label matchers are still
intersected or verified normally.

Encoding for blob kind `5`:

```
MetricSeriesRangesV1Header:
  u32 magic        // 'MSRG'
  u16 version      // 1
  u16 flags        // 0
  u32 metric_count

MetricSeriesRangeGroup[metric_count]:
  u32 metric_name_sym
  u32 range_count
  MetricSeriesRange[range_count]

MetricSeriesRange:
  u32 start_series_ref
  u32 series_count
  u16 kind_mask
  u16 reserved     // 0
  u64 min_time_ms
  u64 max_time_ms
```

`range_count` is stored even though current writers normally emit one range per
metric. It costs little and keeps the format robust if a future writer splits
the same metric by kind or lane.

Metric-range flags and each record's reserved field must be zero. Metric groups
are strictly ordered and unique by `metric_name_sym`, and every group has at
least one range. Within a group, ranges are ordered by `start_series_ref`, have
non-zero `series_count`, do not overlap, remain within the `u32` series-ref
domain, carry a non-zero known metric-kind mask, and satisfy
`min_time_ms <= max_time_ms`. Every `metric_name_sym` is less than the
authoritative `symbols.bin` symbol count. Concatenating ranges in encoded
metric-group/range order forms the exact partition `[0, num_series)`: the first
range begins at zero, every later range begins at the previous range's end,
and the final end equals the authoritative series-root count. Consequently the
blob is empty if and only if `num_series == 0`; gaps, overlaps, duplicate
ownership, and trailing uncovered series are corruption. This canonical order
matches the metric-query physical series order and permits one-pass validation
without an ungoverned interval scratch allocation. The production segment
writer validates the same cross-root partition before emitting the container;
the unbound entry point exists only in test builds for partial and corrupt
fixtures. The governed schema-6 adapter binds the blob to validated
same-generation series and symbol roots, bounds all encoded counts by the
remaining payload bytes before allocating, and treats malformed counts or
cross-root disagreement as errors, not empty candidate sets. The older
materializing schema-6 reader performs intrinsic blob validation only and is
retained solely as the A/B baseline until the schema-neutral facade replaces
it; its behavior is not evidence of the governed cross-root guarantee.

`MetricSeriesRangesV1` has no local payload checksum. Structural and root-count
validation therefore does not prove that an otherwise valid range is
owned by `metric_name_sym`, or that its time/kind summary agrees with the
series and chunks it summarizes. A production query facade may treat these
fields as authoritative only while holding an unforgeable authority minted by
complete-file validation **and** semantic cross-artifact verification against
series, chunk metadata, and exact `(__name__, value)` postings. A footer hash
alone verifies emitted bytes but cannot prove that a buggy writer emitted
the right summary. Without that authority, metric ranges may only inform
ordering or prefetch; absence, membership, time pruning, and kind pruning must
fall back to authoritative metadata. The schema-6 A/B baseline predates this
rule; schemas 7 and 8 must not inherit that known limitation.

### 15.3 Query execution plan for selectors
Given a selector `{a="x", b=~"^foo.*", c!~"bar"}`:

0. Normalize the selector and apply native projection rewrites (§11.5):
   - `<metric>_bucket{le="..."}` becomes native `<metric>` candidates with kind `HIST` or configured EXPHIST classic projection.
   - `<metric>_count` / `<metric>_sum` may map to native HIST/EXPHIST/SUMMARY projections and/or real scalar metrics with the exact name.
   - `le` and `quantile` matchers for virtual projections are not looked up in stored postings; they are applied after decoding schemas.
1. Resolve remaining equality matchers whose predicate does not accept the
   empty string:
   - for `__name__="metric"`, expand the metric-series ranges blob only while
     holding matching metric-range authority; otherwise read the exact
     `(__name__, metric)` postings;
   - for other non-empty equality matchers, read exact postings bitmaps;
   - if an earlier matcher already produced a small candidate set, a reader may
     verify equality directly from `series.bin` instead of reading another index
     payload.
   Equality to the empty string is evaluated against both explicit empty values
   and absent labels. It cannot be represented by the explicit-value posting
   alone, so it stays in the final label predicate unless the planner explicitly
   unions that posting with the complement of the label-presence set.
2. For **positive** regex matchers:
   - try fast-path classifier:
     - literal => equality
     - alternation set => union postings
     - anchored prefix/suffix => prefix enumeration in FST
   - else enumerate values via regex-automaton traversal of FST
   - union postings for matched values
   - **Heuristic (high cardinality)**: if regex enumeration would match “too many” distinct values (e.g., ID-like labels) and you already have a selective `base` from other matchers, prefer a **series-driven** plan:
     - compute `base` from other matchers first
     - for each `series_ref` in `base`, read only the target label’s value from `series.bin` and test it against the regex
     - keep matching series (no giant postings unions)
     - apply an explicit cap like `regex_max_expanded_values` and fall back to series-driven (or error) when exceeded
   A regex that matches the empty string must additionally include series where
   the label is absent. A postings union over explicit values alone is not a
   complete candidate set. Negative matchers follow the same empty-string rule:
   absence matches exactly when applying the matcher to `""` succeeds.
3. Intersect only candidate sets proven complete for their predicates to form
   `base`.
4. If no matcher produced a complete positive candidate set, set
   `base = all_series_bitmap` (blob D), or the implicit range
   `0..num_series-1`.
5. Apply every deferred matcher to the materialized label value, using `""` for
   absence. A postings subtraction is equivalent only when its handling of the
   absent-label complement matches that predicate; for example, `label!="x"`
   includes absence, while `label!=""` does not.
6. Use `chunk_index.bin` to load required chunks.

---

## 16) Read/query path (files touched)

This section describes the shard-local read path for a PromQL query: which artifacts are accessed and what is read from each.

Example selector:
- `pod_cpu_usage_seconds_total{namespace="default",pod=~"nginx-.*"}`

Example time range (from the PromQL engine):
- start/end timestamps for an instant or range query (e.g., `now-1h .. now`)

### 16.0 Query completeness and read horizon

Queries should respect the read horizon derived from ingest watermarks (see
`docs/spec/clock.md`). If the query end exceeds the read horizon:

- Partial: clamp the query end to `read_horizon` and mark the response partial.
- Strict: return a "lagging" error.

For SUM/COUNT aggregations over incomplete time slices, prefer returning
unknown (null/NaN) rather than a partial sum.

### 16.1 Head read (in-memory)
If the query time range overlaps the head window, evaluate the selector against
the shard's head and merge those samples with sealed segment results.

Current implementation note: active-head PromQL querying is implemented for
scalar Float/Int64 samples and native typed Histogram, ExponentialHistogram, and
Summary projections. Head results are deduped with sealed results by projected
`series_id` and timestamp using the same precedence rules described in §14.

- no files are read on the hot path (the head selector index and encoded blocks are in memory)
- the head uses shard-local interning; sealed segments use segment-local `symbols.bin` (see “String interning scope”)

### 16.2 Segment discovery (amortized)
On shard startup (or after a manifest refresh), load segment inventory:
- `manifest/CURRENT` + `manifest/MANIFEST-*`: list sealed segments and their time ranges
- `segments/seg-*/footer.bin`: validate file sizes/checksums (optional fast path: validate lazily on first access)

Keep an in-memory, time-ordered list of segments so most queries do **not** touch manifest files.

Current implementation note: CLI smoke/readback and explicit query benchmarks
open manifest-published segments when `manifest/CURRENT` exists. Orphan or
duplicate `seg-*` directories are ignored by that path.

### 16.3 Selector evaluation (per query, per relevant segment)
For each segment whose `[start_ms, end_ms]` overlaps the query time range:

0. If the selector contains a non-empty equality matcher and the session holds
   matching routing authority, read the routing metadata blob from
   `segments/seg-*/indexes.puffin` and skip the segment when:
   - any equality label/value is absent from the routing blob, or
   - the label/value time range does not overlap the query time range.
   If the segment survives, reuse the same opened `indexes.puffin` reader for
   the full selector plan instead of opening the file again. Without authority,
   or for a predicate that accepts the empty string, do not prune from routing.
1. Resolve query strings to this segment’s symbol ids:
   - `segments/seg-*/symbols.bin`: binary-search the validated v3 root fences,
     then positionally read and validate at most one candidate symbol page for
     each scalar lookup; batch lookups group work by page
2. Build candidate `series_ref` set from label matchers:
   - `segments/seg-*/indexes.puffin` (governed positional reads): read
     authoritative metric-series ranges for `__name__="..."` when available;
     otherwise read exact postings. Read postings + roaring containers for
     other exact matches, and per-label value FSTs
   - Use FST traversal for `=~` / `!~` to enumerate only matching label values, then union postings
   - For negative-only selectors, start from `all_series_bitmap` (blob D) and subtract negative postings
   - Preserve absent-label candidates whenever the matcher's predicate accepts
     the empty string; explicit-value postings alone are insufficient.
3. (Conditionally deferrable) materialize and verify labelsets:
   - `segments/seg-*/series.bin` plus validated pages from
     `segments/seg-*/symbols.bin`: map
     `series_ref -> (series_id, labelset) -> strings` (v1 flat pairs or v2
     keyset-encoded) to (a) return labels to the engine, (b) verify hash-based
     `series_id`s if you use a fingerprint scheme, and (c) unify series across
     segments/head by `series_id` (or by labelset); batch symbol IDs by page
     before materializing strings
   - Pure segment-local routing may delay this work, but cross-segment/head
     merge, stable-series budget accounting, semantic fingerprinting, and
     result construction MUST verify the identity first.
   - A proven terminal aggregation may own only the label names required for
     matching and final grouping. This does not weaken verification: the
     reader MUST decode the complete canonical row, resolve every referenced
     symbol, integrity-check every touched page, hash every canonical pair in
     order, and compare the complete stored `series_id` before exposing the
     selected subset. Omitted-label corruption remains a query error. Partial
     labels MUST NOT enter a full-label cache or escape the terminal
     aggregation. Pre-range cross-segment merging retains the integrity-checked
     full source identity. If a whitelisted range function drops `__name__`,
     its exact full-path result identity MUST be derived from all
     integrity-checked canonical pairs except `__name__`, never from the
     selected subset, before the terminal aggregation constructs a complete
     result.
   - PromQL query sessions default to the `DemandDriven` ownership policy.
     Planning assigns `Include(...)` only to a supported root terminal scalar
     aggregation whose direct selector or scalar `rate()`/`increase()` child
     has `AllPromql` projection, or to root native `count`/`group` with `All`
     or `by(...)` grouping over a direct pure Histogram/ExponentialHistogram
     selector or native `rate()`/`increase()`. The selective kind mask must
     contain the row's complete kind mask; mixed-kind rows and every other
     expression receive `Full` before storage I/O. An explicit `Full` policy
     keeps the same specialized terminal-aggregation flow while owning every
     label for one-binary A/B. `Full` is a planned semantic demand, never a
     retry after corruption or partial execution.
4. Apply kind filtering:
   - Use `series.bin.kind_mask` and query/projection requirements to keep only candidate chunks of the required kind.
   - If one canonical labelset has conflicting kinds, route each kind independently. Do not merge chunks across kinds.

### 16.4 Chunk selection and I/O (per candidate series)
For each candidate `series_ref`:

1. Locate the byte ranges that overlap the query time window:
   - `segments/seg-*/series.bin`: read `SeriesEntryV2.chunk_index_offset/chunk_index_len`
   - `segments/seg-*/chunk_index.bin`: read and validate the exact authoritative
     16-byte directory pair, require it to equal the `SeriesEntryV2` span, then
     read that exact entry body and filter entries by query time range
2. Read only the required chunks:
   - `segments/seg-*/chunks.bin` via batched `pread`/`io_uring` for in-order chunks (Linux: prefer `io_uring`, macOS: `pread`)
   - `segments/seg-*/ooo_chunks.bin` via batched `pread`/`io_uring` for OOO chunks (if present)
3. Decode and validate:
   - `ChunkHeader` is self-describing (`kind`, `encoding`) and carries a CRC, so readers can validate/decode individual chunks without reading an entire frame
   - `ChunkHeader.kind` must match the requested native kind or projection source kind. Mismatch is a corrupt-index or stale-index error.

### 16.5 Merge and return samples
- Merge iterators from in-order and OOO lanes for the same `(series_ref, kind)` within a segment.
- Merge results across segments (and head) for the same `(series_id, kind)` over the requested time range.
- Dedupe equal-timestamp samples using the precedence order in §14 so the PromQL engine sees at most one sample per timestamp per `(series, kind)` stream.
- For native histogram/exponential histogram counters, consume stored `CounterResetHint` values. Do not re-derive reset behavior from decoded bucket values alone.
- For virtual PromQL projections, synthesize the projected labelset (`__name__`, `le`, `quantile`) after native merge/dedupe and before returning samples.
- Return samples (and the projected or native series labelset) to the PromQL engine for range functions/aggregations.

Production multi-step PromQL range execution evaluates the instant expression
independently at every step. The optional `OnePassAssumeScalar` session mode is
a diagnostic Phase-4 comparator, not a production policy. It recognizes only
direct scalar `sum`/`count by (...)(rate(selector[window]))` shapes under an
explicit caller assertion that the exact metric is Float/Int64-only, with
unlimited public limits and a step no larger than the window. Every unsupported
shape and every finite-limit call selects the established repeated executor
before storage I/O. A decoded typed source violates the assertion and is an
error; it must not be omitted or retried after partial one-pass work.

For a successful diagnostic one-pass call, ordinary `QueryStats` describe the
actual union selector work once rather than the established sum of independent
per-step work. The mode reports a post-decode retained-byte estimate, but the
current selector/result boundary cannot reserve those bytes before allocation;
the estimate is not a memory governor. Consequently the mode may not become a
production default until preallocation admission and the public range-query
stats/limit contract are specified and tested. The focused design and
measurement restrictions are in
[`2026-07-21-one-pass-range-execution-design.md`](2026-07-21-one-pass-range-execution-design.md).

### 16.6 High-cardinality query guardrails (required)

High-cardinality environments make certain “valid PromQL” queries operationally unsafe without budgets. Enforce guardrails early and deterministically to prevent accidental overload.

Recommended limits (enforced per shard, per query):
- `query_max_series_matched`: hard cap on the number of candidate series after selector evaluation (before reading chunks)
- `query_max_chunks_read`: cap selected logical chunk requests before payload
  cache filtering and coalescing; this protects chunk fanout and indirectly
  bounds how many coalesced gaps can be over-read
- `query_max_bytes_read`: cap selected logical payload bytes before payload
  cache filtering and coalescing; physical process-issued bytes may be larger
  by the bounded gap amplification described in §9
- `query_max_samples`: cap decoded samples processed by the PromQL engine (Prometheus-style protection)
- `query_max_projected_series`: cap fan-out after native histogram/summary projection
- `regex_max_expanded_values`: cap how many distinct label values a regex is allowed to expand to via FST enumeration (fallback to series-driven filtering or return an error)

Planning/memory notes:
- **“Select all”**: if `base = all_series_bitmap` and its cardinality exceeds `query_max_series_matched`, return an error unless the caller explicitly opts in.
- **Top-N**: `topk/bottomk` should stream results and keep only a `k`-heap; do not materialize labelsets for all candidate series.
- **Label materialization**: defer reading/decoding full labelsets (`series.bin`) until the final output set is known; for aggregations, prefer reading only the grouping label values instead of full labelsets.
- **Projection fan-out**: charge budgets after virtual projection. A classic histogram with `N` explicit bounds can emit `N + 3` projected series (`N + 1` buckets, `_sum`, `_count`); summaries can emit `num_quantiles + 2`.

Current implementation note: `QueryLimits::production_default()` exposes the
recommended guardrails from §20. Sealed and active-head query paths enforce
matched-series, projected-series, chunk-read, byte-read, decoded-sample, and
regex-expanded-value budgets. `chronoxide-query --query ...` uses those
production defaults unless the operator overrides them with CLI flags.

---

## 17) Metadata discovery (metrics, tags, tag values)

PromQL is optimized for selecting and computing over time series, not enumerating metadata. For UI “discovery” use cases (Grafana variables, autocomplete), expose Prometheus-style metadata endpoints and serve them from the same shard-local indexes (head + segments).

Discovery queries should accept an optional time window (`start`, `end`) and optional matchers (`match[]`) so results can be time-bounded and scoped (important for high-cardinality environments).
Current implementation note: head indexes are not yet available, so discovery is **segment-only** for now.

### 17.1 Discover metrics (metric names)
**What you want**: list all metric names, e.g. `pod_cpu_usage_seconds_total`.

**Prometheus-style API**
- `GET /api/v1/label/__name__/values?start=...&end=...`
- optional scoping: `...&match[]={job="kubelet"}` (or any selector)

**How it is served (storage)**
- Head (planned): enumerate distinct values of `__name__` from the head label index.
- Segments: use the per-label value FST for label `__name__` in `segments/seg-*/indexes.puffin`, resolve symbols via `segments/seg-*/symbols.bin`, and union results across overlapping segments.

### 17.2 Discover metric tags (label names)
**What you want**: list label keys such as `namespace`, `pod`, `container`, `cpu`.

**Prometheus-style API**
- all label names: `GET /api/v1/labels?start=...&end=...`
- for a metric (or any selector): `GET /api/v1/labels?match[]=pod_cpu_usage_seconds_total&start=...&end=...`

**How it is served (storage)**
- Head (planned): enumerate label names from the head’s label index keys.
- Segments:
  - segment-wide label names: read the `indexes.puffin` blob directory and list all “per-label value FST” blobs (label-name symbols), then resolve via `symbols.bin`.
  - selector-scoped label names: get candidate `series_ref`s for the selector (postings/FST), then read only the matched series’ labelsets from `series.bin` (v1 flat pairs or v2 keyset-encoded) and union the label keys.

### 17.3 Discover tag values (label values for a selector)
**What you want**: list values for a tag, optionally scoped by a metric. Example: list all `namespace` values that exist for `pod_cpu_usage_seconds_total`.

**Prometheus-style API**
- `GET /api/v1/label/namespace/values?match[]=pod_cpu_usage_seconds_total&start=...&end=...`

**How it is served (storage)**
- Head (planned): for the selector’s candidate series set, gather distinct values from head label index.
- Segments: use one of these plans per segment (pick by cardinality heuristics):
  - **Value-driven** (good when the label has fewer distinct values): enumerate values from the label’s FST, fetch postings for each value, and keep those whose postings intersect the selector’s base series set (fast “AND non-empty” on roaring bitmaps).
  - **Series-driven** (good when the selector is very selective): iterate `series_ref`s from the selector’s postings and read the requested label’s value from `series.bin`, then deduplicate.

Note: this never reads `chunks.bin`; it is served entirely from `symbols.bin`/`series.bin`/`indexes.puffin`.

---

## 18) Manifest and segment discovery

The shard has:
- `MANIFEST-*`: append-only records of sealed segments
- `CURRENT`: pointer to latest manifest file

Manifest record:
- segment ULID
- time range
- file checksums (or footer hash)
- wal_lsn boundary (optional, helps WAL truncation)

Recovery:
- read CURRENT -> MANIFEST
- load sealed segments
- scan for orphan sealed dirs not in manifest and repair if footer validates

---

## 19) Retention, deletion, and maintenance

This spec defines immutable sealed segments; long-running shards therefore require explicit retention and housekeeping to avoid unbounded growth.

### 19.1 Segment retention (SSD “near” store)

Retention is enforced by segment time range:
- periodically compute `expiry_time = now - ssd_retention`
- any sealed segment with `end_ms < expiry_time` is eligible for deletion

Deletion protocol (crash-safe, shard-local):
1. Append a tombstone record to the manifest for the segment (e.g., `SEGMENT_DELETED {segment_id}`).
2. `fsync()` the manifest file and the manifest directory.
3. Rename `segments/seg-.../` to `segments/.trash/seg-.../` atomically.
4. Delete from `.trash` asynchronously.

This guarantees that a crash cannot resurrect a deleted segment: the manifest is authoritative.

### 19.2 Manifest compaction

Because manifests are append-only, periodically compact them:
1. Read the active manifest and compute the live segment set (sealed minus tombstones).
2. Write a new `MANIFEST-<n+1>` containing only live segments (plus any required checkpoints/metadata).
3. `fsync()` the new manifest and atomically update `CURRENT`.
4. Optionally delete older manifest files after a grace period.

### 19.3 WAL truncation / rotation

WAL should not grow without bound. A WAL prefix can be safely dropped when all data covered by that prefix is represented in manifest-published sealed segments.

Requirements:
- Each `SEGMENT_SEALED` record includes (or references) a `wal_lsn_boundary` that represents “all WAL records up to this LSN are included in this segment”.
- The manifest record for a sealed segment persists that boundary (or a hash of the footer that includes it).

Policy:
- After a segment is sealed **and** published to the manifest, the shard may truncate/delete WAL files whose last LSN is `< min(wal_lsn_boundary of oldest still-needed in-memory window)`.
- If using delayed sealing for OOO, ensure the “still-needed” window reflects the largest open head window and any backfill/OOO buffers.

### 19.4 Native histogram rollup (future)

Near-store retention may delete whole segments by time without rollup. If long-retention rollups are added later, use these rules:
- cumulative Histogram/ExponentialHistogram: last sample in the rollup window, preserving reset hints at window boundaries
- delta Histogram: over intervals in the rollup window, sum additive fields (`count`, `bucket_counts`, present `sum`) and merge extrema as `min(min_i)` / `max(max_i)`, requiring identical explicit bounds
- delta ExponentialHistogram: downscale to a common target scale, then sum additive fields (`count`, `zero_count`, buckets, present `sum`) and merge extrema as `min(min_i)` / `max(max_i)`
- Summary: `_count`/`_sum` can be rolled up according to their scalar semantics; quantile values are not rollup-able except as last-value gauges

Rollup output must preserve native typed chunks or explicitly mark itself as a derived projection.

---

## 20) Config knobs (storage)

- `ingestion.segment_writer.storage_schema = schema8 | schema7` (default:
  `schema8`; Schema 6 is not writable through application configuration;
  removed `experimental_schema7` and
  `experimental_schema8_adaptive_postings` keys are rejected)
- `head_window_duration = 1h` (current: tied to `segment_duration`)
- `head_block_size = 256`
- `adaptive_series_table = true` (runtime diagnostic comparator; in-memory only)
- `adaptive_last_timestamp_table = true` (runtime diagnostic comparator;
  in-memory only)
- `segment_duration = 1h`
- `ssd_retention = 6h` (example)
- `wal_sync_interval = 20ms`
- `out_of_order_time_window = 30m`
- `ooo_buffer_max_points = 32`
- `frame_target_size = 64KB`
- `chunk_target_points = 256` (example)
- `chunk_target_bytes = 16KB` (example; payload, not including headers)
- `min_chunk_points = 4` (example; best-effort floor)
- `min_chunk_bytes = 4KB` (example; best-effort floor, aligns with typical page size)
- `max_schemas_per_chunk = 16` (example; split/fallback/reject when exceeded)
- `schema_change_rate_limit = 100/minute/series` (example; protect write path from schema churn)
- `sum_mode = store_cumulative | store_delta`
- `hist_mode = store_cumulative | store_delta`
- `exphist_mode = store_cumulative | store_delta`
- `exphist_scale_policy = keep | downscale_to_max_scale(K)`
- `store_exemplars = false | true | sampled(N)`
- `hist_bucket_layout = row_varlen | columnar` (columnar is future/optional)
- `use_mmap_indexes = false` (required for schema 7/8 and schema-6 A/B; legacy
  readers only may opt in)
- `chunk_read_mode = auto | io_uring | pread` (`auto` is the API and
  `ChunkReadConfig` default; it uses available Linux `io_uring` only when the
  scheduler has enough physical spans and otherwise uses `pread`; benchmark
  and direct-session paths may explicitly default to or force `pread`)
- `chunk_payload_coalesce_max_gap_bytes = 4096` (current accepted range
  `0..=4096`; immutable per query session)
- `use_direct_io = false|true` (when true, apply §9.1 alignment rules)
- `direct_io_block_size = 4096`
- `index_container_format = puffin_like_v1`
- `name_normalization = otel_promql_v1`
- `metadata_retained_max_bytes = 64MiB` (aggregate; zero disables retention)
- `metadata_in_flight_max_bytes = 256MiB` (aggregate; nonzero)
- `max_open_files = 128` (hard aggregate descriptor cap; nonzero)
- `max_cached_open_files = 64` (idle subset of the hard cap; may be zero)
- `open_segment_cache_max = 64` (legacy pre-schema-7 compatibility only)
- `manifest_compact_interval = 5m`
- `segment_packer_target_duration = 1h` (optional, if packing many 15m segments)
- `query_max_series_matched = 1_000_000` (recommended; protect “select all”)
- `query_max_chunks_read = 5_000_000` (recommended; protect chunk-index fanout)
- `query_max_bytes_read = 2GB` (recommended; cap logical selected payload
  bytes, not coalesced physical over-read)
- `query_max_samples = 50_000_000` (recommended; Prometheus-style protection)
- `query_max_projected_series = 2_000_000` (recommended; protect histogram projection fan-out)
- `regex_max_expanded_values = 100_000` (recommended; fallback to series-driven or error)

Current implementation note: these query defaults are available as
`QueryLimits::production_default()` and are used by `chronoxide-query --query`
unless overridden:
- `--query-max-series-matched`
- `--query-max-projected-series`
- `--query-max-chunks-read`
- `--query-max-bytes-read`
- `--query-max-samples`
- `--regex-max-expanded-values`

---

## 21) References (for humans)

PromQL basics and matchers:
```text
https://prometheus.io/docs/prometheus/latest/querying/basics/
```

OpenTelemetry metrics data model (Sum/Histogram/ExponentialHistogram semantics):
```text
https://opentelemetry.io/docs/specs/otel/metrics/data-model/
```

GreptimeDB architecture (index separation, inverted index ideas):
```text
https://deepwiki.com/GreptimeTeam/greptimedb/1.1-system-architecture
https://docs.greptime.com/contributor-guide/datanode/data-persistence-indexing/
https://github.com/GreptimeTeam/greptimedb
```

Puffin file format🔗
- https://iceberg.apache.org/puffin-spec/

## 22) On performance

- [What I Learned Building a Storage Engine That Outperforms RocksDB](https://tidesdb.com/articles/what-i-learned-building-a-storage-engine-that-outperforms-rocksdb/)
- [Performance Hints](https://abseil.io/fast/hints.html)
- [What Does a Database for SSDs Look Like?](https://brooker.co.za/blog/2025/12/15/database-for-ssd.html)
