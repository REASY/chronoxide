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
   - Current implementation persists only **Gauge/Sum number datapoints** as FLOAT chunks (f64).
   - Histogram, ExponentialHistogram, and Summary datapoints are tracked for stats/label interning but are **not yet persisted**.
4. **Out-of-order is normal**: collector retries, batching, and failover cause OOO points; support bounded lateness and an OOO lane.
5. **Single-writer stream assumption**: you can maintain per-stream state (temporality normalization, reset detection) inside a shard, but must tolerate duplicates/replays.
6. **Greptime-inspired improvements** (adopted here):
   - **Separate index artifacts from data artifacts** so you can evolve / rebuild indexes without rewriting chunk data.
   - Use an **inverted index with an FST + bitmaps** to accelerate high-cardinality label lookups and regex.
   - Treat indexes as a set of **blobs in a container file** (a “Puffin-like” bundle) to keep segment layouts stable and extensible.

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
- Only **FLOAT chunks** are persisted; Histogram/ExponentialHistogram/Summary data are not yet written to disk.

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
Interning during ingestion is for speed/memory in the shard/head, but **persisted symbol ids are per-segment**: when sealing a segment, build a segment-local `symbols.bin` and write `series.bin` + index blobs using that segment’s symbol ids. This keeps segments standalone and movable and avoids global/distributed symbol coordination.

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

Label values are stored as strings as-is (they do not need PromQL normalization).

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

### 6.3 Segment files

#### Data files
- `chunks.bin`: in-order chunk frames (append-only)
- `ooo_chunks.bin`: out-of-order chunk frames (append-only, optional)

#### Metadata
- `symbols.bin`: **segment-local** string dictionary (metric names, label keys/vals) used by `series.bin` and `indexes.puffin` within this segment; query-time resolution maps query strings -> this segment’s `symbol_id`s (typically via a sorted dictionary and/or an embedded lookup structure such as an FST)
- `series.bin`: SeriesRef -> SeriesID + labelset + type metadata (v2 keyset/value-code encoding recommended for high cardinality)
- `chunk_index.bin`: SeriesRef + time range -> **(file, offset, length)** of each chunk within chunk files (so readers can pread only required chunks)

#### Index container (Greptime-inspired)
- `indexes.puffin`: a container holding multiple index blobs:
  - postings index
  - label-value FSTs
  - bitmap dictionaries / roaring containers
  - optional bloom filters and min/max stats per series

#### Integrity
- `footer.bin`: per-file sizes + checksums + segment schema version
- `meta.json`: human-readable summary

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
  u8  kind_mask        // bitmask: FLOAT/HIST/EXPHIST present in this segment for this series
  u8  reserved0
  u16 reserved1
  u32 meta_len         // 0 if none
  u32 num_labels
  u8  meta[meta_len]   // optional, extensible (e.g., Sum monotonicity/temporality normalization)
  LabelPair labels[num_labels]  // sorted by key_sym

LabelPair:
  u32 key_sym
  u32 val_sym
```

`series_ref` is not stored in the entry; it is the index into `entry_offsets`.

#### 6.4.2 `series.bin` v2 (keyset/value-code encoding; recommended)

Motivation:
- Many series share the same *set of label keys* (e.g., k8s labels); only the *values* vary.
- Storing `LabelPair{key_sym,val_sym}` per series repeats the same `key_sym`s millions of times.
  - Empirical note: in a production-like 10M-message sample, only a small number of keys required 4-byte value codes (cardinality > 65k); most keys fit in 0/1/2 bytes, making fixed-width-per-keyset packing very effective.

`series.bin` v2 stores:
- a fixed-size per-series table: `series_ref -> {series_id, kind_mask, keyset_id, row, meta}`
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

SeriesEntryV2:  // fixed-size, 32 bytes
  u64 series_id
  u8  kind_mask        // FLOAT/HIST/EXPHIST present in this segment for this series
  u8  flags
  u16 reserved0
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

### 6.5 `chunk_index.bin` format (v1)

`chunk_index.bin` is optimized for:
- fast “chunks for (`series_ref`, time range)” lookups without scanning unrelated series
- predictable mmap access patterns (fixed-size chunk entries)

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
  u8  kind             // FLOAT/HIST/EXPHIST (optional hint; reader can validate via ChunkHeader)
  u16 flags
  u64 min_time_ms
  u64 max_time_ms
  u64 offset           // byte offset in the selected chunk file, points to ChunkHeader
  u32 length           // bytes to read (ChunkHeader + payload)
  u32 reserved0
  u32 reserved1
```

Ordering rules:
- Entries are sorted by `(min_time_ms, max_time_ms, offset)` within each (`series_ref`, `file_id`) lane.
- For `chunks.bin` (in-order lane), a writer should ensure chunk time ranges for a series do not overlap and are increasing by time.
- For `ooo_chunks.bin`, overlaps are allowed; queries must merge/dedupe at read time.

---

## 7) WAL + shard-local offset checkpointing (no Kafka-offset dependency)

### 7.1 WAL record format
WAL is append-only records:

```
| magic u32 | version u16 | type u16 | len u64 | payload[len] | crc32c u32 |
```

Record types:
- `OTLP_BATCH`: a batch of decoded points (or raw OTLP bytes + minimal indexing)
- `CHECKPOINT`: (partition -> next_offset) map + wal_lsn + wall clock
- `SEGMENT_SEALED`: segment id + range + wal_lsn boundary

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
When sealing a segment, assign `series_ref` densely (recommended: sort series by `series_id`, then assign `series_ref = 0..N-1`). Then write chunks in **series-major order** (sort by `series_ref`, then by time) and pack consecutive chunks into frames until `frame_target_size` is reached. This keeps bytes for a series contiguous on disk and reduces read amplification without requiring background compaction.

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
  u8  kind            // FLOAT (Gauge/Sum), HIST, EXPHIST
  u8  encoding        // per-kind encoding id (current: FLOAT/GORILLA, legacy: FLOAT/RAW_F64)
  u16 flags
  u32 series_ref
  u64 min_time_ms
  u64 max_time_ms
  u32 num_points
  u32 header_len
  u32 payload_len
  u32 chunk_crc32c
  ... payload ...
```

Time/value encoding (current FLOAT implementation):
- payload starts with `t0_ms`
- then varint `dt_ms[]` for each sample (delta from `t0_ms`)
- then Gorilla XOR bitstream for values

Legacy FLOAT/RAW_F64 encoding:
- payload starts with `t0_ms`
- then for each sample: varint `dt_ms` + raw `f64`

### 11.3 Chunk sizing and “logical fragmentation” (high cardinality)

In high-cardinality OTLP workloads, it’s common to have **millions of series** where many series are **sparse** (few points per `segment_duration`). If you flush a new chunk per series per head window unconditionally, you can create:
- **many tiny chunks** (high per-chunk header/CRC overhead; poor 4KB I/O efficiency, especially with `O_DIRECT`)
- **large `chunk_index.bin`** (many `ChunkEntryV1` records), increasing mmap pressure and query planning time

Recommendations:
- Prefer **fewer, larger chunks** over many tiny chunks:
  - use `chunk_target_points` / `chunk_target_bytes` as the primary flush trigger
  - enforce a `min_chunk_points` / `min_chunk_bytes` floor when possible
- If you choose `segment_duration=15m` and observe sparse series, treat **block-in-progress** or **segment packing** (§6.1) as mandatory so a “segment” can hold enough time to produce non-tiny chunks.
- Ensure `chunk_index.bin` supports fast per-series range scans (already true via per-series directory), and cap worst cases with query budgets (§16.6).

---

## 12) Write flow: Sum

Note: only FLOAT (Gauge/Sum) chunks are implemented today. Histogram/ExponentialHistogram/Summary sections are forward-looking.

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
Payload:
- `t0_ms, dt_ms[]`
- value encoding:
  - xor-f64 (Gorilla-style) for float
  - optional: varint zigzag for i64 if you store integer sums separately

### 12.4 Flush
At head window close or size threshold:
- emit chunk(s) for each series
- append to `chunks.bin` frames
- add entry to `chunk_index.bin`

---

## 13) Write flow: Exponential Histogram

### 13.1 Input handling
Per OTLP ExponentialHistogram point:
- extract: time (+ start_time), count, sum, scale, zero_count, buckets:
  - positive: offset + dense counts array
  - negative: offset + dense counts array

### 13.2 Scale policy
Config:
- `exphist_scale_policy = keep | downscale_to_max_scale(K)`
Downscale is allowed and predictable; it trades precision for storage and query speed.

### 13.3 Chunk encoding (ExpHist)
Payload:
- time: `t0_ms, dt_ms[]`
- per-point scalars:
  - `scale_delta` (zigzag varint, usually 0)
  - `count_delta_or_raw` (varint)
  - `sum` (xor-f64)
  - `zero_count` (varint)
- per-point ranges:
  - pos: `pos_offset` (zigzag), `pos_len` (varint), `pos_counts[]` (varint + zero-RLE)
  - neg: `neg_offset` (zigzag), `neg_len` (varint), `neg_counts[]` (varint + zero-RLE)

### 13.4 Flush
Same as Sum, but chunks have `ChunkHeader.kind=EXPHIST`.

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
- merge in-order and OOO iterators for a series over the requested time range.

**Duplicate timestamps / replays (deterministic dedupe)**  
Duplicates can happen due to retries/replays and because OOO points may overlap already-flushed in-order data.

Policy (PromQL-friendly):
- **Within a flush/chunk build**: sort points by `(timestamp, ingest_order)` and keep only the last point for each timestamp (last-write-wins).
- **At query merge time**: if multiple sources produce a sample at the same timestamp for the same logical series, return **one** sample using this deterministic precedence order:
  1. Head (newest ingestion) > sealed segments
  2. Newer sealed segment (later manifest order) > older sealed segment
  3. Within a segment: OOO lane (`ooo_chunks.bin`) > in-order lane (`chunks.bin`)
  4. Within the same lane: later chunk entry order (as stored in `chunk_index.bin`) wins

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
- blob name/type
- byte offset + length
- blob checksum
- version

This lets you add new index types later without changing the segment file list, and rebuild indexes without rewriting `chunks.bin`.

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

### 15.3 Query execution plan for selectors
Given a selector `{a="x", b=~"^foo.*", c!~"bar"}`:

1. Resolve all **positive** equality matchers via postings bitmaps.
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
Current implementation note: head querying is **not yet implemented**. The head only buffers samples for segment sealing.

Planned behavior:
- if the query time range overlaps the head window, evaluate the selector against the shard’s head first
- no files are read on the hot path (head index + chunk builders are in memory)
- the head uses shard-local interning; sealed segments use segment-local `symbols.bin` (see “String interning scope”)

### 16.2 Segment discovery (amortized)
On shard startup (or after a manifest refresh), load segment inventory:
- `manifest/CURRENT` + `manifest/MANIFEST-*`: list sealed segments and their time ranges
- `segments/seg-*/footer.bin`: validate file sizes/checksums (optional fast path: validate lazily on first access)

Keep an in-memory, time-ordered list of segments so most queries do **not** touch manifest files.

### 16.3 Selector evaluation (per query, per relevant segment)
For each segment whose `[start_ms, end_ms]` overlaps the query time range:

1. Resolve query strings to this segment’s symbol ids:
   - `segments/seg-*/symbols.bin` (mmap): map label names/values (including `__name__`) to `symbol_id`s
2. Build candidate `series_ref` set from label matchers:
   - `segments/seg-*/indexes.puffin` (mmap): read postings + roaring containers and per-label value FSTs
   - Use FST traversal for `=~` / `!~` to enumerate only matching label values, then union postings
   - For negative-only selectors, start from `all_series_bitmap` (blob D) and subtract negative postings
3. (Optional) materialize/verify labelsets:
   - `segments/seg-*/series.bin` + `segments/seg-*/symbols.bin` (mmap): map `series_ref -> (series_id, labelset) -> strings` (v1 flat pairs or v2 keyset-encoded) to (a) return labels to the engine, (b) verify hash-based `series_id`s if you use a fingerprint scheme, and (c) unify series across segments/head by `series_id` (or by labelset)

### 16.4 Chunk selection and I/O (per candidate series)
For each candidate `series_ref`:

1. Locate the byte ranges that overlap the query time window:
   - `segments/seg-*/chunk_index.bin` (mmap): find chunk entries for `(series_ref, time range)` that return `(file, offset, length)` and chunk time bounds
2. Read only the required chunks:
   - `segments/seg-*/chunks.bin` via batched `pread`/`io_uring` for in-order chunks (Linux: prefer `io_uring`, macOS: `pread`)
   - `segments/seg-*/ooo_chunks.bin` via batched `pread`/`io_uring` for OOO chunks (if present)
3. Decode and validate:
   - `ChunkHeader` is self-describing (`kind`, `encoding`) and carries a CRC, so readers can validate/decode individual chunks without reading an entire frame

### 16.5 Merge and return samples
- Merge iterators from in-order and OOO lanes for the same `series_ref` within a segment.
- Merge results across segments (and head) for the same `series_id` (or by canonical labelset) over the requested time range.
- Dedupe equal-timestamp samples using the precedence order in §14 so the PromQL engine sees at most one sample per timestamp per series.
- Return samples (and the series labelset) to the PromQL engine for range functions/aggregations.

### 16.6 High-cardinality query guardrails (required)

High-cardinality environments make certain “valid PromQL” queries operationally unsafe without budgets. Enforce guardrails early and deterministically to prevent accidental overload.

Recommended limits (enforced per shard, per query):
- `query_max_series_matched`: hard cap on the number of candidate series after selector evaluation (before reading chunks)
- `query_max_chunks_read` and/or `query_max_bytes_read`: cap physical I/O work (protects `chunk_index.bin` fanout and tiny-chunk amplification)
- `query_max_samples`: cap decoded samples processed by the PromQL engine (Prometheus-style protection)
- `regex_max_expanded_values`: cap how many distinct label values a regex is allowed to expand to via FST enumeration (fallback to series-driven filtering or return an error)

Planning/memory notes:
- **“Select all”**: if `base = all_series_bitmap` and its cardinality exceeds `query_max_series_matched`, return an error unless the caller explicitly opts in.
- **Top-N**: `topk/bottomk` should stream results and keep only a `k`-heap; do not materialize labelsets for all candidate series.
- **Label materialization**: defer reading/decoding full labelsets (`series.bin`) until the final output set is known; for aggregations, prefer reading only the grouping label values instead of full labelsets.

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
- `regex_max_expanded_values = 100_000` (recommended; fallback to series-driven or error)

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
