# Storage layer specification (OTLP-native TSDB with PromQL)

This document specifies the **local / SSD-friendly storage layer** for an OTLP-native metrics TSDB that supports **PromQL querying**.

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
  source_timestamp_ms: i64,  // Kafka/source metadata; not trusted policy time
  captured_at_ms: i64,       // local Chronoxide wall clock at capture/accept
  payload: bytes,            // raw OTLP ExportMetricsServiceRequest
}
```

`captured_at_ms` is required. It is the trusted replay anchor used by event-time
validation, future-skew policy, lag diagnostics, and any replay clock that wants
to reproduce the original ingestion safety decision.

The source/Kafka timestamp is metadata only. It can be useful for diagnostics or
for transports that do not carry datapoint timestamps, but it must not be used
as trusted policy time because a producer or broker timestamp can be wrong or
malicious.

Segment placement remains event-time based: datapoint timestamps decide the
head window, segment range, compression deltas, and query time range. Capture
time must not decide where samples are stored.

Replay from capture should preserve per-partition record order and evaluate
event-time policy using `captured_at_ms` (or future explicit capture watermark
records). Replaying a file later with the current wall clock must not make
previously future-dated datapoints appear safe.

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

**Operational note (FD / mmap scaling)**  
Implementations must not keep every segment mmapped/open. Use a bounded “open segment cache” (LRU by recent query use) and rely on the manifest’s time range inventory so you only touch segments that overlap the query window.

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
- `symbols.bin`: **segment-local** sorted string dictionary (metric names, label keys/vals) used by `series.bin` and `indexes.puffin` within this segment. `symbol_id == sorted_dictionary_ordinal`. Query-time resolution maps query strings -> this segment’s `symbol_id`s by binary search over the sorted dictionary; an optional embedded FST/hash accelerator may be added later only if profiling shows the binary search is hot.
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

2. Early routing and symbol resolution

   symbols.bin                    -> "__name__", "http_request_duration_seconds",
                                     "route", "/api" become segment-local symbol_ids
   indexes.puffin routing blob    -> skip segment if required equality values are absent
                                     or their time ranges miss the query window

3. Selector planning

   indexes.puffin metric ranges   -> series_ref ranges for native metric name
   indexes.puffin postings/FSTs   -> series_refs matching route="/api"
   intersection                   -> candidate series_ref set

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

footer.bin is not on the hot path by default; `open_validated` or explicit
validation reads it to check tracked file sizes/checksums before querying.
```

`symbols.bin` v2 byte layout:

```
SymbolsHeader:
  u32 magic          // 'SYMB'
  u16 version        // 2
  u16 flags          // 0 for v2
  u32 symbol_count

SymbolOffsets:
  u64 offsets[symbol_count + 1]

SymbolBytes:
  u8 strings[offsets[symbol_count]]
```

Each symbol `i` is `strings[offsets[i]..offsets[i + 1]]`. Offsets are relative
to the start of `SymbolBytes`, `offsets[0]` must be zero, and
`offsets[symbol_count]` must end exactly at EOF. Symbols must be valid UTF-8,
strictly sorted by raw UTF-8 bytes, and unique. A reader resolves
`symbol_id -> string` by offset slicing and resolves `string -> symbol_id` by
binary search over the sorted offset table. Readers must reject unsorted,
duplicate, out-of-bounds, or trailing-byte payloads as corrupt.

#### Index container (Greptime-inspired)
- `indexes.puffin`: a container holding multiple index blobs:
  - segment routing metadata for early equality/time pruning
  - postings index
  - label-value FSTs
  - label-value time ranges
  - metric-series ranges
  - bitmap dictionaries / roaring containers
  - optional bloom filters and min/max stats per series

#### Integrity
- `footer.bin`: per-file sizes + checksums + segment schema version
- `meta.json`: human-readable summary

Current implementation note: segment schema version `6` stores routing metadata
and required metric-series ranges inside `indexes.puffin`; there is no separate
`routing_index.bin`. This is a breaking format change from previous
experimental layouts. Old smoke segments must be regenerated instead of read
through a compatibility path.

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
series-to-chunk-index routing pointer in `series.bin` so the read path can avoid
reading the global chunk-index offset directory for selective queries.

#### 6.4.3 Series metadata TLVs

`meta[]` stores segment-local, series-level semantics that are required to decode and query typed chunks correctly. It is a sequence of TLVs:

```
SeriesMetaTlv:
  u8  tag
  u8  len
  u8  value[len]
```

Required tags for non-gauge cumulative/delta data:
```
tag=1 effective_temporality   // u8: 0=unspecified, 1=cumulative, 2=delta
tag=2 original_temporality    // u8: OTLP aggregation temporality before normalization
tag=3 monotonicity            // u8: 0=not_monotonic, 1=monotonic, 2=unknown
tag=4 source_metric_kind      // SourceMetricKind enum, distinct from ChunkHeader.kind
tag=5 normalization_mode      // u8: 0=raw, 1=store_cumulative, 2=store_delta
```

`SourceMetricKind` values:
```
0 = unknown
1 = gauge_number
2 = sum_number
3 = histogram
4 = exponential_histogram
5 = summary
```

This enum is metadata about the OTLP source metric family. It is not the same namespace as `ChunkHeader.kind`; numeric collisions between the two enums have no meaning.

Rules:
- Histogram and ExponentialHistogram series MUST persist effective temporality and normalization mode.
- Delta and cumulative samples MUST NOT mix within one canonical series identity unless the change is represented as a new typed stream boundary; query code must treat a mid-series temporality change as a reset/type boundary, not as continuous samples.
- Summary samples do not use aggregation temporality. Their metadata records `source_metric_kind=SUMMARY` and any known monotonicity for `_count`/`_sum` projections, but summary quantile values are gauges.
- A `kind_mask` with multiple sample kinds for the same labelset is allowed only to preserve conflicting source data. Query merge and dedupe are always kind-aware (§14, §16.5).

### 6.5 `chunk_index.bin` format (v1)

`chunk_index.bin` is optimized for:
- fast “chunks for (`series_ref`, time range)” lookups without scanning unrelated series
- predictable mmap access patterns (fixed-size chunk entries)

The file keeps its own `series_offsets` directory so it is self-describing and
can be validated independently. Query readers that already loaded
`SeriesEntryV2` SHOULD use `series.bin`'s `chunk_index_offset/chunk_index_len`
and read that exact span directly, avoiding a second directory lookup and the
cold-cache random reads that come with it.

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
WAL is append-only records:

```
| magic u32 | version u16 | type u16 | len u64 | payload[len] | crc32c u32 |
```

Record types:
- `OTLP_BATCH`: raw OTLP `ExportMetricsServiceRequest` bytes plus capture metadata (`CaptureRecord` fields), not lossy normalized points
- `CHECKPOINT`: (partition -> next_offset) map + wal_lsn + wall clock
- `SEGMENT_SEALED`: segment id + range + wal_lsn boundary

Replay contract:
- WAL replay re-runs the same normalization, event-time policy, temporality handling, reset detection, and chunk-building code as live ingestion.
- Raw OTLP bytes preserve flags, exemplars, start_time, temporality, and future OTLP fields even before the segment layer persists every field.
- Deterministic replay requires the same writer config, segment duration, event-time policy, deterministic segment id seed, and per-partition record order (§4.5).

### 7.2 Checkpoint file (fast startup)
`checkpoint.meta` is a small atomically replaced snapshot of the latest checkpoint:
- offsets per transport partition
- wal_lsn of the checkpoint record
- checksum

On startup:
1. read `checkpoint.meta`
2. seek/validate WAL to last checkpoint lsn
3. resume transport consumption from recorded offsets
4. replay WAL tail if needed to rebuild head

---

## 8) Checksums and corruption handling

Use checksums at three layers:

- **WAL record**: crc32c (detect torn writes and corruption)
- **Chunk frames**: each frame carries a crc32c
- **Segment footer**: strong checksum per file (xxhash64 or blake3)

Behavior:
- WAL replay stops at first invalid record; earlier records remain valid.
- Segment read:
  - if footer checksum fails => quarantine segment
  - if a chunk frame fails CRC => ignore frame tail (stop scan) and rely on WAL/other segments for recovery
  - if a chunk has an unknown `(kind, encoding)` pair => do not decode it; return an unsupported-encoding error for queries that require it, or skip only when the caller explicitly allows partial results
  - if `chunk_index.bin` kind disagrees with `ChunkHeader.kind` => treat the segment index as corrupt and quarantine or rebuild the index

---

## 9) mmap vs explicit I/O

Use both, by file class:

### mmap (read-mostly, random access)
- `symbols.bin`
- `series.bin`
- `chunk_index.bin`
- `indexes.puffin` blobs that are compact and pointer-friendly (FST nodes, roaring containers)

### explicit I/O (large data)
- `chunks.bin`
- `ooo_chunks.bin`

Reader uses `pread`/`io_uring` to batch reads of required frames and avoid page-fault storms from mmapping huge chunk files.
Implementation note: use `io_uring` on Linux when available; on macOS fall back to standard `pread`.

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
If SSD retention is large enough to produce hundreds/thousands of segments, do not mmap all per-segment metadata up-front. Keep only an inventory in memory (from the manifest) and mmap segment metadata lazily with an LRU to cap VMAs and open-file pressure.

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
  u16 flags
  u32 num_chunks
  ... chunk payloads ...
```

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
1 = RESERVED_INT64   // reserved; current PromQL path stores OTLP ints as FLOAT/f64
2 = HIST
3 = EXPHIST
4 = SUMMARY
```

Per-kind encoding ids:
```
FLOAT:
  0 = GORILLA
  1 = RAW_F64

HIST:
  0 = SCHEMA_VARLEN
  1 = RAW_VARLEN
  2 = SCHEMA_COLUMNAR       // optional future codec

EXPHIST:
  0 = SCHEMA_VARLEN
  1 = RAW_VARLEN
  2 = SCHEMA_COLUMNAR       // optional future codec

SUMMARY:
  0 = SCHEMA_VARLEN
  1 = RAW_VARLEN
```

Unknown `(kind, encoding)` pairs are treated as unreadable data: the reader must not attempt best-effort decoding. It may skip the chunk, quarantine the segment, or return a typed "unsupported chunk encoding" error depending on query policy (§8).

`ChunkHeader.flags`:
```
bit 0      SINGLE_SCHEMA              // schema_id omitted when num_schemas == 1
bit 1      HAS_START_TIME             // start_time_ms lane is present
bit 2      HAS_PER_SAMPLE_FLAGS       // OTLP DataPointFlags lane is present
bit 3      HAS_COUNTER_RESET_HINTS    // HIST/EXPHIST reset hints are present or uniform
bit 4      TEMPORALITY_DELTA          // set=delta, unset=cumulative for HIST/EXPHIST
bit 5      HAS_EXEMPLARS              // optional exemplar sidecar is present
bit 6      ALL_SUM_PRESENT            // optional sum present for every sample
bit 7      ALL_MIN_PRESENT            // optional min present for every sample
bit 8      ALL_MAX_PRESENT            // optional max present for every sample
bit 9      RESET_HINT_UNIFORM         // bits 10..11 contain one reset hint for all samples
bits 10-11 RESET_HINT_UNIFORM_VALUE   // 2-bit CounterResetHint when bit 9 is set
bit 12     DOWNSCALED                 // EXPHIST was downscaled before storage/projection
bits 13-15 reserved
```

`ChunkEntryV1.kind` is an index hint for planning. Readers must validate it against `ChunkHeader.kind` after reading the chunk bytes.

Flag invariants:
- `RESET_HINT_UNIFORM` MUST be 0 unless `HAS_COUNTER_RESET_HINTS` is set. A chunk with `RESET_HINT_UNIFORM=1` and `HAS_COUNTER_RESET_HINTS=0` is corrupt.
- `RESET_HINT_UNIFORM_VALUE` is meaningful only when both `HAS_COUNTER_RESET_HINTS` and `RESET_HINT_UNIFORM` are set; otherwise readers ignore bits 10..11.
- `HAS_COUNTER_RESET_HINTS` is defined for `HIST` and `EXPHIST` chunks. FLOAT/Sum reset handling remains query-time value-based until a scalar counter-reset lane is specified.
- `HAS_EXEMPLARS` MUST be 0 in v1 chunks. Exemplar sidecar storage is deferred until §13.8 defines a byte-level sidecar/index format.
- `DOWNSCALED` MUST be 0 in v1 native chunks. Query-time downscale does not mutate chunk bytes. Future materialized projection chunks that set this bit must also persist original scale and target scale in projection metadata.

### 11.3 Common payload lanes

Every chunk payload starts with a time lane:
```
u64      t0_ms
uLEB128  dt_ms[num_points]       // timestamp_ms - t0_ms
```

If `HAS_START_TIME` is set, this follows the time lane:
```
u64      start_time0_ms
zLEB128  start_time_delta_ms[num_points]  // start_time_ms - start_time0_ms
```

`start_time_ms` is mandatory for `store_delta` Histogram/ExponentialHistogram chunks and for cumulative counter chunks when the source provides it. If the source omits it, the writer records no lane and must set counter reset hints to `UnknownCounterReset` at ambiguous boundaries.

If `HAS_PER_SAMPLE_FLAGS` is set, this follows the start-time lane:
```
u8       flags_present_bitmap[ceil(num_points / 8)]
uLEB128  otlp_datapoint_flags[popcount(flags_present_bitmap)]
```

Only samples with non-zero OTLP `DataPointFlags` have an entry in the varint list. `FLAG_NO_RECORDED_VALUE` (bit 0) means the point is a semantic gap. Query-time scalar projections map it to the Prometheus stale NaN bit pattern `0x7ff0000000000002` for every derived series. A stale marker participates in OOO/dedupe and must not be dropped as an empty point.

For native typed chunks, a sample with `FLAG_NO_RECORDED_VALUE` MUST still have a byte-present, num_points-aligned value body. Its typed body is canonical zero:
- `optional_field_mask = 0` for HIST/EXPHIST
- `count = 0`
- `zero_count = 0` for EXPHIST
- all bucket arrays have the schema/per-sample declared length and every count is `0`
- SUMMARY stores `count = 0`, `sum = 0.0`, and each quantile value as `0.0`

Readers reconstruct staleness from the `DataPointFlags` lane, never from the zero body. The zero body exists only to keep validators and byte offsets deterministic.

If `HAS_COUNTER_RESET_HINTS` is set and `RESET_HINT_UNIFORM` is unset, this follows the flags lane:
```
u8 counter_reset_hint_bits[ceil(num_points * 2 / 8)]
```

Counter reset hint values:
```
0 = UnknownCounterReset
1 = CounterReset
2 = NotCounterReset
3 = GaugeType
```

The persisted order matches Prometheus `CounterResetHint` for the non-unknown reset/not-reset values, so code that bridges to native Prometheus histograms does not transpose reset semantics.

If `RESET_HINT_UNIFORM` is set, no reset-hint byte lane is stored; the uniform value is read from `RESET_HINT_UNIFORM_VALUE`.

All chunk kinds then store a value payload after the time and optional lanes. `SCHEMA_VARLEN` and future schema-based typed encodings start that value payload with a chunk-local schema table:
```
TypedChunkSchemaTable:
  u32 num_schemas
  repeated num_schemas times:
    u32 schema_len
    u8  schema[schema_len]
```

Schema ids are dense `0..num_schemas-1` in first-seen order. Unless `SINGLE_SCHEMA` is set, each sample payload starts with `schema_id` as unsigned LEB128. A decoded `schema_id >= num_schemas` is a corrupt chunk (§8). The schema table is included in `payload_len` and covered by `chunk_crc32c`.

### 11.3.1 Typed scalar projection lane

Typed HIST/EXPHIST/SUMMARY chunks may store a compact scalar projection lane immediately after `ChunkHeader` and before the native typed payload. This makes `_count`/`_sum` projection a single contiguous `ChunkHeader + TypedScalarLane` read. The lane is outside `ChunkHeader.payload_len` and outside `chunk_crc32c`; it is inside `ChunkEntryV1.length` and covered by its own CRC. `ChunkHeader.header_len` points to the start of the native typed payload, so when the scalar lane is present `header_len = sizeof(ChunkHeader) + scalar_lane_len`.

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

`TypedSampleMetadata` uses the same wire encoding as the native typed payload: `flags`, `temporality`, `reset_hint`, and optional `start_time_ms`.

Readers use `chunk_index.bin` `scalar_lane_offset/scalar_lane_len` to fetch only `ChunkHeader + TypedScalarLane` for `<metric>_count` and `<metric>_sum`. The reader must validate lane magic, version, body length, CRC, and that `ChunkHeader.kind` is one of HIST/EXPHIST/SUMMARY with `SCHEMA_VARLEN` encoding. If the scalar lane is absent, a reader may fall back to scanning the full native typed payload.

### 11.4 Native typed value formats

Histogram, ExponentialHistogram, and Summary data are persisted as native typed chunks, not expanded into Prometheus-compatible scalar series on the ingestion path.

Rationale:
- Expanding histograms into `_bucket` / `_sum` / `_count` series at write time multiplies cardinality and index size.
- OTLP carries native shape information that would be lost or made ambiguous by eager scalar projection.
- Query-time projection keeps storage faithful to the source and lets compaction/materialization policies evolve later.

#### HIST/SCHEMA_VARLEN

Schema bytes:
```
u32      num_bounds
f64le    explicit_bounds[num_bounds]   // finite, strictly ascending
u32      bucket_count                  // MUST equal num_bounds + 1
```

Per-sample bytes:
```
schema_id?                         // omitted when SINGLE_SCHEMA is set
u8       optional_field_mask        // always present; bit0=sum, bit1=min, bit2=max
uLEB128  count                      // u64
f64le    sum?                       // raw IEEE bits when present
f64le    min?                       // raw IEEE bits when present
f64le    max?                       // raw IEEE bits when present
uLEB128  bucket_counts[bucket_count]
```

`optional_field_mask` is always present for every HIST sample, regardless of `ALL_SUM_PRESENT`, `ALL_MIN_PRESENT`, or `ALL_MAX_PRESENT`. The `ALL_*` flags are validation/fast-path hints only: writers set them when the corresponding mask bit is set for every sample in the chunk, and readers must still read presence from `optional_field_mask`.

Validation:
- `explicit_bounds` are finite, non-NaN values and strictly ascending by numeric value. `+Inf`, `-Inf`, and `NaN` bounds are rejected so they cannot collide with the synthetic Prometheus `le="+Inf"` bucket.
- `len(bucket_counts) == len(explicit_bounds) + 1`.
- `sum(bucket_counts) == count` for classic OTLP histograms. Overflow in the accumulator is a corrupt-chunk error.
- Bucket counts and `count` decode to `u64`; no `u32` narrowing is allowed.
- Present NaN/Inf values round-trip as raw IEEE bits and are distinct from absent fields and from stale NaN.

`HIST/RAW_VARLEN` emits no schema table and no `schema_id`; `SINGLE_SCHEMA` MUST be 0. It is for compatibility, tests, and unstable schemas. Per-sample bytes are:
```
u32      num_bounds
f64le    explicit_bounds[num_bounds]   // finite, strictly ascending
u32      bucket_count                  // MUST equal num_bounds + 1
u8       optional_field_mask           // always present; bit0=sum, bit1=min, bit2=max
uLEB128  count                         // u64
f64le    sum?
f64le    min?
f64le    max?
uLEB128  bucket_counts[bucket_count]
```

#### EXPHIST/SCHEMA_VARLEN

Schema bytes:
```
zLEB128  scale
f64le    zero_threshold
```

Per-sample bytes:
```
schema_id?                         // omitted when SINGLE_SCHEMA is set
u8       optional_field_mask        // always present; bit0=sum, bit1=min, bit2=max
uLEB128  count                      // u64
f64le    sum?
f64le    min?
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

`optional_field_mask` is always present for every EXPHIST sample, regardless of `ALL_SUM_PRESENT`, `ALL_MIN_PRESENT`, or `ALL_MAX_PRESENT`. The `ALL_*` flags are validation/fast-path hints only.

Validation:
- `zero_threshold` is part of the schema. Same `scale` and bucket counts with different `zero_threshold` are different schemas.
- `zero_count + sum(positive_counts) + sum(negative_counts) == count`; overflow is a corrupt-chunk error.
- `count`, `zero_count`, and bucket counts decode to `u64`.

`EXPHIST/RAW_VARLEN` emits no schema table and no `schema_id`; `SINGLE_SCHEMA` MUST be 0. It is the non-schema fallback. Per-sample bytes are:
```
zLEB128  scale
f64le    zero_threshold
u8       optional_field_mask        // always present; bit0=sum, bit1=min, bit2=max
uLEB128  count                      // u64
f64le    sum?
f64le    min?
f64le    max?
uLEB128  zero_count                 // u64
zLEB128  positive_offset
uLEB128  positive_len
uLEB128  positive_counts[positive_len]
zLEB128  negative_offset
uLEB128  negative_len
uLEB128  negative_counts[negative_len]
```

`EXPHIST/SCHEMA_VARLEN` is the preferred v1 format when `scale` and `zero_threshold` are stable. RAW fallback is used when schema churn policy chooses it or when a writer cannot intern a stable schema.

#### SUMMARY/SCHEMA_VARLEN

Schema bytes:
```
u32   num_quantiles
f64le quantiles[num_quantiles]      // strictly ascending quantile positions
```

Per-sample bytes:
```
schema_id?                         // omitted when SINGLE_SCHEMA is set
uLEB128  count                     // u64
f64le    sum
f64le    values[num_quantiles]
```

`SUMMARY/RAW_VARLEN` emits no schema table and no `schema_id`; `SINGLE_SCHEMA` MUST be 0. Per-sample bytes are:
```
u32      num_quantiles
f64le    quantile[num_quantiles]    // strictly ascending quantile positions
uLEB128  count                      // u64
f64le    sum
f64le    values[num_quantiles]
```

Summary semantics:
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

Current implementation note: scalar `rate(selector[range])` and `increase(selector[range])` are implemented over vector/projection query results with counter reset handling and range-boundary extrapolation. Native Histogram/ExponentialHistogram count/sum/bucket projections preserve stored `CounterResetHint` metadata and consume it during scalar range evaluation; scalar series without reset metadata still use counter-decrease reset handling. Stale/non-finite samples inside the selected counter range act as stream boundaries for scalar and native typed range evaluation: the evaluator uses only finite samples after the last boundary marker, clamps extrapolation to that marker, and slices aligned reset hints to the same suffix. Scoped instant-vector aggregations (`sum`, `count`, `avg`, `min`, `max` with `by`/`without`) are implemented over the latest sample in each input series; selector children of instant-vector operators are read through a 5-minute lookback window ending at the evaluation timestamp, and a latest stale/non-finite sample makes that series absent from the aggregation input. Top-level selector queries still use the caller's explicit read range for smoke/readback compatibility. `histogram_quantile(q, ...)` is implemented for classic `_bucket` vectors, including production-shaped `histogram_quantile(q, sum by (le, route)(rate(<metric>_bucket[range])))` inputs. A first native classic Histogram path is implemented for sealed and active-head `histogram_quantile(q, rate(metric[range]))` and `histogram_quantile(q, sum by/without (...)(rate(metric[range])))`: it reads typed Histogram samples directly, computes a native histogram `rate`/`increase` for compatible cumulative samples with identical explicit bounds, supports native Histogram `sum` aggregation over compatible bucket layouts, and converts only the final quantile result back to scalar output without materializing `_bucket` series. A first sealed and active-head native ExponentialHistogram path is implemented for `histogram_quantile(q, rate(metric[range]))` and `histogram_quantile(q, sum by/without (...)(rate(metric[range])))`: it reads typed ExponentialHistogram samples directly, downscales compatible cumulative samples to a common coarser scale, supports native ExponentialHistogram `sum` aggregation over compatible zero thresholds, consumes reset hints, applies exponential interpolation for positive and negative exponential buckets, and clamps one-sided zero-bucket interpolation to the observed side of zero. Delta-temporality scalar projections and native Histogram/ExponentialHistogram range paths carry decoded `start_time_ms` in memory when available; `rate()`/`increase()` sum selected delta intervals whose `[start_time_ms, time_ms)` windows intersect the evaluation range, so a single complete delta interval can produce a valid range result without fabricating a second endpoint sample. If native delta start times are unavailable, native Histogram/ExponentialHistogram range execution falls back to converting selected delta samples into the same in-range cumulative sequence exposed by virtual `_count`/`_sum`/`_bucket` projections, then applies the existing reset-aware `rate`/`increase` math.

Delta virtual scalar projections (`_count`, `_sum`, `_bucket`) may be accumulated in chunk-local, segment-local, or head-local fragments before query merge. Range evaluation records those fragments as internal boundaries and stitches them into one in-range cumulative sequence before applying `rate`/`increase`; these boundaries are not exposed as PromQL counter resets.

Projected selector rewrite:
- A selector for `<metric>_bucket{le="..."}` is rewritten to native `<metric>` with kind `HIST` or configured EXPHIST classic projection, then `le` is applied after decoding/projection.
- A selector for `<metric>_count` or `<metric>_sum` is rewritten to matching native histogram/exphist/summary kinds and may also match real scalar metrics with that exact name. If real and virtual series produce the same final labelset, the query layer must return a conflict error or use a documented precedence policy; it must not silently dedupe them.
- Selector indexes remain label-based over native series. Optional per-kind bitmaps may be added in `indexes.puffin` to reduce planning work, but correctness comes from `series.bin.kind_mask` and chunk-header validation.

### 11.6 Chunk sizing and logical fragmentation

In high-cardinality OTLP workloads, it is common to have millions of sparse series. If you flush a new chunk per series per head window unconditionally, you can create many tiny chunks and a large `chunk_index.bin`.

Recommendations:
- `chunk_target_bytes` is the primary trigger for all kinds.
- `chunk_target_points` is a hard cap, not the primary sizing signal for wide histogram payloads.
- A chunk always accepts at least one sample; a frame may contain exactly one oversized chunk.
- Enforce `max_schemas_per_chunk` and a per-series schema-change rate limit. On breach, split the chunk, fall back to `RAW_VARLEN`, reject the series, or emit an explicit ingestion error according to policy.
- If sparse series dominate, use block-in-progress or segment packing (§6.1) rather than reducing `segment_duration` until most chunks are single-sample.
- Query budgets (§16.6) must charge post-projection fan-out, not only native chunk reads.

---

## 12) Write flow: Sum

Note: FLOAT chunks are implemented for Gauge/Sum number datapoints. Histogram/ExponentialHistogram/Summary native chunk persistence and first-pass scalar projections are implemented. Typed value payloads currently persist start time, OTLP datapoint flags, temporality, and reset hints; typed chunks also append compact scalar lanes for `_count`/`_sum`. The fully separated common-lane byte layout and exemplar sidecars remain forward-looking.

Sums are stored as float chunks (plus sum metadata in `series.bin`).

### 12.1 Input handling
For each OTLP Sum datapoint:
- identify series_id (labels)
- read temporality and monotonic flag
- use `(start_time, time)` to detect resets/gaps

### 12.2 Normalization choices
Config:
- `sum_mode = store_cumulative | store_delta`

Notes:
- storing cumulative is friendlier for PromQL `rate()` and counter semantics
- storing delta preserves raw semantics but requires more work in query/compaction

### 12.3 Chunk encoding
FLOAT chunk payload uses the common lanes from §11.3:
- `t0_ms, dt_ms[]`
- optional `HAS_START_TIME`, `HAS_PER_SAMPLE_FLAGS`, and future reset-hint lanes when specified
- value encoding:
  - xor-f64 (Gorilla-style) for float

### 12.4 Flush
At head window close or size threshold:
- emit chunk(s) for each series
- append to `chunks.bin` frames
- add entry to `chunk_index.bin`

---

## 13) Write flow: Histogram, ExponentialHistogram, Summary

Note: native chunk persistence and first-pass scalar projections for these types are implemented. Start time, OTLP datapoint flags, temporality, cumulative reset hints, stale projection, DELTA Histogram count/sum/bucket projection, deterministic query-configured ExponentialHistogram bucket projection, compact `_count`/`_sum` scalar lanes, and reusable ExponentialHistogram downscale/merge helpers are implemented in the current schema-varlen path. A sealed/head native classic Histogram path exists for `histogram_quantile(q, rate(<metric>[range]))` and native Histogram `sum` aggregation over compatible cumulative or delta Histogram samples. A sealed/head native ExponentialHistogram path exists for `histogram_quantile(q, rate(<metric>[range]))` and native ExponentialHistogram `sum` aggregation over compatible cumulative or delta ExponentialHistogram samples. Exemplar sidecars and the fully separated common-lane byte layout remain future work.

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

Config:
- `hist_mode = store_cumulative | store_delta`

Rules:
- The effective mode is persisted in `series.bin` metadata and reflected in `ChunkHeader.flags`.
- Delta and cumulative histogram samples MUST NOT mix within one continuous `(series_id, HIST)` stream.
- A mid-series temporality change is a type/reset boundary. The writer either starts a new logical stream boundary or rejects the input according to policy.
- `start_time_ms` is mandatory for `store_delta`; if missing, the sample cannot be safely converted to PromQL counter semantics.

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
- additive fields: bucket-wise sum `count`, `bucket_counts`, and present `sum` over the selected intervals
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

Config:
- `exphist_mode = store_cumulative | store_delta`
- `exphist_scale_policy = keep | downscale_to_max_scale(K)`

Rules:
- Effective temporality and mode are persisted in `series.bin` metadata and `ChunkHeader.flags`.
- Delta and cumulative ExponentialHistogram samples MUST NOT mix within one continuous `(series_id, EXPHIST)` stream.
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
- preferred: `SUMMARY/SCHEMA_VARLEN` (§11.4)
- fallback: `SUMMARY/RAW_VARLEN`

Projection contract:
- `<metric>_count`: scalar count series
- `<metric>_sum`: scalar sum series
- `<metric>{quantile="q"}`: quantile gauge series
- quantile gauge samples are not mergeable across series or time ranges and are not valid inputs to `rate()`/`increase()`

### 13.8 Exemplars

OTLP NumberDataPoint, HistogramDataPoint, and ExponentialHistogramDataPoint may carry exemplars. SummaryDataPoint does not.

For v1, exemplar persistence is optional and controlled by:
- `store_exemplars = false | true | sampled(N)` (future; v1 chunks set `HAS_EXEMPLARS=0` until the sidecar format is specified)

When enabled:
- v1 chunks still set `HAS_EXEMPLARS = 0`; exemplar persistence is deferred until the sidecar/index byte format is specified.
- future exemplar sidecars must be chunk-keyed, reuse `symbols.bin` for filtered attributes, and round-trip exemplar time, value, span_id, and trace_id exactly.
- disabling exemplars must not affect metric sample correctness.

### 13.9 Flush

At head window close or size threshold:
- build typed chunks using the kind and encoding rules in §11
- append in-order samples to `chunks.bin`
- append accepted OOO samples to `ooo_chunks.bin`
- add entries to `chunk_index.bin`
- persist series-level effective temporality/mode in `series.bin` metadata

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
A single file that stores multiple **index blobs** with a footer listing:
- blob kind/type
- byte offset + length
- version

This lets you add new index types later without changing the segment file list, and rebuild indexes without rewriting `chunks.bin`.
The segment footer protects `indexes.puffin` as a file; per-blob checksums may be
added in a later index-container version if partial index corruption handling is
needed.

Current implementation format:

```
SegmentIndexesHeader:
  u32 magic     // 'SIDX'
  u16 version   // 6
  u16 flags     // 0

BlobPayloads:
  byte[] blob_0
  byte[] blob_1
  ...

SegmentIndexesFooter:
  u32 magic       // 'SIDF'
  u16 version     // 6
  u16 flags       // 0
  u32 entry_count
  u32 reserved    // 0
  DirectoryEntry[entry_count]

DirectoryEntry:
  u16 kind
  u16 flags
  u32 label_name_sym
  u32 label_value_sym
  u64 offset
  u64 len
  u64 min_time_ms
  u64 max_time_ms

SegmentIndexesTrailer:
  u64 footer_len
  u32 magic       // 'SIDT'
```

Known blob kinds:
- `1`: exact postings for one `(label_name_sym, label_value_sym)`
- `2`: label-value FST for one `label_name_sym`
- `3`: label-value time ranges for one `label_name_sym`
- `4`: routing metadata for early segment pruning
- `5`: metric-series ranges for metric-name equality routing

The routing metadata blob should be physically first in `indexes.puffin` so a
reader can fetch the routing header and a small number of fixed-size lookup
buckets before deciding whether to open `symbols.bin`, `series.bin`,
`chunk_index.bin`, or chunk files. A reader may still use the footer directory to
locate it; physical order is an I/O locality optimization, not a replacement for
directory lookup.

Current segment-index version `6` requires blob kind `5`. Readers must reject a
segment whose `indexes.puffin` directory does not contain the required
metric-series ranges blob. This is a breaking format requirement for newly
written segments.

### 15.2 Required index blobs

#### (A) Postings dictionary
Mapping:
- `(label_name_sym, label_value_sym) -> postings_bitmap_id`

Postings are stored as:
- roaring bitmaps for large sets (elements are `series_ref` / `u32`)
- delta-encoded sorted lists for small sets (elements are `series_ref` / `u32`)

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
matching hash, and reads key bytes only for hash matches to verify collisions.
Strings are normalized PromQL label names/values as used by queries, not
segment-local symbols. This is intentional: it lets the read path answer "can
this equality matcher exist in this segment and overlap this query time range?"
before loading `symbols.bin`.

`exact_postings_blob_len` is the byte length of the exact-postings blob that
would be read if the segment survives pruning. Query planning uses it to order
multiple equality matchers by cheapest postings read.

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

### 15.3 Query execution plan for selectors
Given a selector `{a="x", b=~"^foo.*", c!~"bar"}`:

0. Normalize the selector and apply native projection rewrites (§11.5):
   - `<metric>_bucket{le="..."}` becomes native `<metric>` candidates with kind `HIST` or configured EXPHIST classic projection.
   - `<metric>_count` / `<metric>_sum` may map to native HIST/EXPHIST/SUMMARY projections and/or real scalar metrics with the exact name.
   - `le` and `quantile` matchers for virtual projections are not looked up in stored postings; they are applied after decoding schemas.
1. Resolve remaining **positive** equality matchers:
   - for `__name__="metric"`, expand the metric-series ranges blob into
     candidate `series_ref`s for that metric;
   - for other equality matchers, read exact postings bitmaps;
   - if an earlier matcher already produced a small candidate set, a reader may
     verify equality directly from `series.bin` instead of reading another index
     payload.
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
3. Intersect all positive postings => `base` candidate `series_ref`s.
4. If there are **no** positive matchers, set `base = all_series_bitmap` (blob D) (or the implicit range `0..num_series-1`).
5. Apply negative matchers by subtracting postings from `base` (PromQL `!=` / `!~` include series where the label is missing).
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

0. If the selector contains positive equality matchers, read the routing
   metadata blob from `segments/seg-*/indexes.puffin` and skip the segment when:
   - any equality label/value is absent from the routing blob, or
   - the label/value time range does not overlap the query time range.
   If the segment survives, reuse the same opened `indexes.puffin` reader for
   the full selector plan instead of opening the file again.
1. Resolve query strings to this segment’s symbol ids:
   - `segments/seg-*/symbols.bin` (mmap): map label names/values (including `__name__`) to `symbol_id`s
2. Build candidate `series_ref` set from label matchers:
   - `segments/seg-*/indexes.puffin` (mmap): read metric-series ranges for `__name__="..."`, postings + roaring containers for other exact matches, and per-label value FSTs
   - Use FST traversal for `=~` / `!~` to enumerate only matching label values, then union postings
   - For negative-only selectors, start from `all_series_bitmap` (blob D) and subtract negative postings
3. (Optional) materialize/verify labelsets:
   - `segments/seg-*/series.bin` + `segments/seg-*/symbols.bin` (mmap): map `series_ref -> (series_id, labelset) -> strings` (v1 flat pairs or v2 keyset-encoded) to (a) return labels to the engine, (b) verify hash-based `series_id`s if you use a fingerprint scheme, and (c) unify series across segments/head by `series_id` (or by labelset)
4. Apply kind filtering:
   - Use `series.bin.kind_mask` and query/projection requirements to keep only candidate chunks of the required kind.
   - If one canonical labelset has conflicting kinds, route each kind independently. Do not merge chunks across kinds.

### 16.4 Chunk selection and I/O (per candidate series)
For each candidate `series_ref`:

1. Locate the byte ranges that overlap the query time window:
   - `segments/seg-*/series.bin`: read `SeriesEntryV2.chunk_index_offset/chunk_index_len`
   - `segments/seg-*/chunk_index.bin`: read that exact entry span and filter entries by query time range; the embedded chunk-index directory is reserved for validation, repair, and readers that do not already have the `SeriesEntryV2`
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

### 16.6 High-cardinality query guardrails (required)

High-cardinality environments make certain “valid PromQL” queries operationally unsafe without budgets. Enforce guardrails early and deterministically to prevent accidental overload.

Recommended limits (enforced per shard, per query):
- `query_max_series_matched`: hard cap on the number of candidate series after selector evaluation (before reading chunks)
- `query_max_chunks_read` and/or `query_max_bytes_read`: cap physical I/O work (protects `chunk_index.bin` fanout and tiny-chunk amplification)
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

- `head_window_duration = 1h` (current: tied to `segment_duration`)
- `head_block_size = 256`
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
- `use_mmap_indexes = true`
- `chunk_read_mode = io_uring | pread` (Linux: `io_uring`, macOS: `pread`)
- `use_direct_io = false|true` (when true, apply §9.1 alignment rules)
- `direct_io_block_size = 4096`
- `index_container_format = puffin_like_v1`
- `name_normalization = otel_promql_v1`
- `open_segment_cache_max = 64` (cap mmaps/FDs)
- `manifest_compact_interval = 5m`
- `segment_packer_target_duration = 1h` (optional, if packing many 15m segments)
- `query_max_series_matched = 1_000_000` (recommended; protect “select all”)
- `query_max_chunks_read = 5_000_000` (recommended; protect chunk-index fanout)
- `query_max_bytes_read = 2GB` (recommended; protect I/O)
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
