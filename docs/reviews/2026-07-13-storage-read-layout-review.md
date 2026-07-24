# Storage Read-Path Layout, Replay-Sealing Review, and vNext Options

- **Date:** 2026-07-13
- **Status:** Evidence-informed, non-normative design review; updated with the
  completed schema-6 paged-symbol baseline.
- **Implementation revision reviewed:**
  `ccd7adec97c784de946537f87b567d4dd2b93445`

## Authority

[storage.md](../superpowers/specs/storage.md) remains the normative storage specification and
[clock.md](../superpowers/specs/clock.md) remains the normative clock and event-time specification.
This review records measurements, implementation findings, specification drift,
and candidate improvements. Nothing in this document changes on-disk semantics.
Any accepted format change must first be specified precisely in `storage.md`
and assigned a new format/version boundary where required.

Historical reports and dated designs cited here are evidence and context, not
current normative authority.

## Executive conclusion

Chronoxide's largest demonstrated fresh-process read-path opportunity on the
measured corpus and query shapes is metadata access and label materialization,
not another chunk-payload I/O backend. This is not yet a claim about every
steady-state workload: the benchmark processes began with empty process-local
metadata caches, and only `chunks.bin` page-cache residency was explicitly
controlled.

The inspected replay corpus is unusually clear:

- it contains 47,766,209 series and exactly 47,766,209 chunks;
- each chunk holds only 10.480 datapoints on average;
- tracked metadata occupies 10.644 GB, while `chunks.bin` occupies 10.901 GB;
- exact postings alone occupy 3.923 GB;
- across nine repetitions, a selective scalar query had medians of 99.205 ms
  loading symbols and 20.954 ms reading selected chunk payloads; and
- across nine repetitions, a sparse regex query had medians of 428.036 ms
  reading/materializing series rows and labels and 95.077 ms reading selected
  chunk payloads.

The current adaptive payload scheduler is architecturally sound and improves
the portion it owns. Existing experiments show that a meaningful payload-I/O
improvement can still produce less than one percent end-to-end improvement when
metadata and result materialization dominate. The alpha-stage optimization
program can run no-format and format experiments as independent tracks. A
no-format metadata baseline remains a useful control, but it is not a
prerequisite for a focused format prototype when deterministic replay can
regenerate the corpus. The first isolated format experiment—the independently
checksummed, paged `symbols.bin` v3 design—is complete as the schema-6 A/B
baseline. Later experiments can target:

1. point-addressable metric ranges and exact directories;
2. compact and column-oriented series metadata;
3. an inline single-chunk fast path;
4. adaptive compressed postings;
5. independently readable typed scalar columns; and
6. packed frames and, later, adjacent-segment packing.

SIMD becomes attractive after the metadata is columnar. Applying SIMD only to
the present row-oriented, allocation-heavy path is unlikely to move total query
latency materially.

Replay and sealing have a separate high-value memory opportunity. In the
writer-enabled replay path, `HeadConfig` takes the writer's 15-minute segment
duration; the configured `head_buffer.window_duration_secs = 3600` is ignored
for that path. The one-hour out-of-order setting remains an acceptance window,
while accepted late samples are partitioned into 15-minute OOO head windows.
Sealing decodes and orders a complete 15-minute window, and shutdown currently
collects all normal and OOO windows from all partitions before writing them.
Footer construction also reads each completed file wholly into memory for
hashing. These effects can produce large transient RSS even though chunk
payloads are streamed to disk. Replay/sealing memory must be measured and
bounded independently of any read-layout redesign.

## Scope

This review covers:

- sealed-segment `symbols.bin`, `series.bin`, `chunk_index.bin`, `chunks.bin`,
  and `indexes.puffin` layouts;
- segment writer and footer behavior that determines those layouts;
- exact and regex selector planning;
- typed scalar and native payload projection;
- current positional-read and payload scheduling behavior;
- head-to-segment replay/sealing memory behavior that determines writer RSS;
  and
- real-corpus size and read-profile evidence already present on the review
  host.

It does not select a final vNext byte layout, authorize a migration, change
PromQL semantics, or claim latency improvements for unbenchmarked proposals.
Prior experimental formats need not remain readable unless a later design
explicitly requires compatibility.

## Constraints every proposal must preserve

The following constraints come from the normative specifications and project
policy. They are acceptance requirements, not optimization choices.

- Segment placement and query range semantics remain event-time based.
- Capture/ingest time remains control-policy time. `captured_at_ms` is the
  trusted replay anchor; a source/Kafka timestamp is diagnostic metadata only.
- Missing OTLP datapoint timestamps are rejected rather than replaced by a
  source, capture, or wall-clock timestamp.
- Typed OTLP temporality, start time, datapoint flags, reset hints, and optional
  value presence remain correctness fields.
- Stable input order, identical writer configuration, and deterministic segment
  IDs continue to produce deterministic replay output.
- Persisted symbol and dictionary IDs remain segment-scoped. Any query path
  carrying encoded labels across segments must remap them to a query-global
  identity or compare canonical values, with collision verification where
  hashes are used.
- Immutable manifest publication and head/sealed/OOO precedence remain
  unchanged.
- Individual chunks remain directly addressable even if several chunks share a
  physical frame.
- The v7 principle of immutable positional-read roots and lazy metadata remains
  the baseline. A vNext design must not reintroduce eager complete-directory
  materialization or a shared seek cursor.
- Every touched parse, checksum, bounds, ordering, or count failure propagates
  as corruption. It must never become a cache miss, a pruning result, a
  fallback, or an empty query result.
- New caches are explicitly byte-bounded and do not defeat the open-segment
  budget.
- A faster layout must not weaken segment sealing or checksum coverage.

## Evidence and method

### Sources reviewed

- [Storage layer specification](../superpowers/specs/storage.md)
- [Clock and event-time specification](../superpowers/specs/clock.md)
- [Segment index v7 design](../superpowers/specs/archive/storage/2026-07-10-segment-index-v7-design.md)
- [Shared segment index directory design](../superpowers/specs/archive/storage/2026-07-10-shared-segment-index-directory-design.md)
- [General chunk-payload read scheduler design](../superpowers/specs/archive/benchmarks/2026-07-12-chunk-payload-read-scheduler-design.md)
- current segment writer, chunk codecs, series reader, v7 index reader, selector
  lowering, query-store, and footer implementation at the revision above
- existing real-corpus query and io_uring reports

### Selected corpus

The read-only inventory used this host-local corpus:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/
  segments-replay-20260711-141105
```

This path is not portable. It identifies the evidence exactly; it is not a
repository dependency. The corpus has 18 approximately 15-minute segment
directories.

The corpus fingerprint recorded by the query benchmark is:

```text
b9c1470b99726c3f6a53591bf5ec7fb8f96b0691f474e6935a27fce6de145891
```

The inventory parsed fixed headers and the v7 index trailer without changing
the corpus. Byte-saving numbers below are arithmetic estimates unless an
experiment is explicitly cited. They are not latency predictions and are not
necessarily additive.

The inventory and deterministic 1/16 postings sample used for this review do
not yet have a saved machine-readable result and checked-in reproducer. Their
figures are suitable for prioritizing experiments, not for accepting a format.
Before a design relies on them, preserve the inventory command/tool revision,
raw output, corpus fingerprint, sample rule, and codec-estimate output together
in a run-specific experiment directory.

## Corpus layout measurements

### Aggregate files

| Artifact | Bytes | Share of tracked bytes |
|---|---:|---:|
| `chunks.bin` | 10,901,453,553 | 50.6% |
| `chunk_index.bin` | 2,292,778,392 | 10.6% |
| `series.bin` | 3,197,605,493 | 14.8% |
| `symbols.bin` | 235,189,457 | 1.1% |
| `indexes.puffin` | 4,918,064,629 | 22.8% |
| **Total** | **21,545,091,524** | **100.0%** |

Metadata excluding `chunks.bin` is 10,643,637,971 bytes, or approximately
49.4% of all tracked bytes. It is 97.6% of the physical `chunks.bin` size.

The corpus contains:

- 47,766,209 series;
- 47,766,209 chunks;
- 500,600,784 datapoints;
- 10.480 datapoints per chunk on average; and
- 214.225 indexed chunk bytes per chunk on average.

Every observed series has exactly one chunk in its segment. This does not prove
that production data is universally single-chunk, but it makes an inline
single-chunk representation a high-value target with an overflow path for the
general case.

### Chunk kinds

| Kind | Chunks | Indexed bytes | Average bytes/chunk |
|---|---:|---:|---:|
| Float | 40,956,759 | 4,109,378,997 | 100.3 |
| Histogram | 1,886,600 | 1,343,689,643 | 712.2 |
| ExponentialHistogram | 4,886,746 | 4,736,420,519 | 969.2 |
| Summary | 36,104 | 43,237,468 | 1,197.6 |
| **Total** | **47,766,209** | **10,232,726,627** | **214.2** |

The difference between `chunks.bin` and indexed chunk bytes is exactly
668,726,926 bytes: 14 frame-header bytes for every chunk. The fixed 40-byte
chunk header consumes another 1,910,648,360 bytes. Frame plus chunk headers are
therefore approximately 2.58 GB. For the average Float chunk, those fixed 54
bytes are more than half of its physical record footprint.

### `series.bin`

The measured components are approximately:

| Component | Bytes |
|---|---:|
| Fixed 40-byte series table | 1,910,648,360 |
| Keyset directory/data | 3,990,604 |
| Value dictionaries | 14,519,608 |
| Keyset row blocks | 1,268,445,769 |

The final eight bytes of every current fixed series record are written as zero,
and current readers reject non-zero metadata. Those eight bytes alone account
for 382,129,672 bytes in this corpus.

### `chunk_index.bin`

Every observed series pays for:

- an eight-byte global directory slot; and
- a fixed 40-byte chunk index entry.

The entries consume 1,910,648,360 bytes. The directory consumes approximately
382.1 MB and duplicates a level of location information already reachable from
the series table.

### `indexes.puffin`

| v7 component | Bytes | Share of `indexes.puffin` |
|---|---:|---:|
| Routing | 671,166,785 | 13.6% |
| Metric ranges | 8,260,740 | 0.2% |
| Exact directory | 280,256 | <0.1% |
| Exact pages | 142,901,248 | 2.9% |
| Exact postings | 3,922,789,036 | 79.8% |
| Auxiliary directory | 1,334,032 | <0.1% |
| Auxiliary payload | 171,327,636 | 3.5% |

Exact postings are currently a count followed by raw sorted `u32` series
references. A deterministic 1/16 sample covered 222,677 posting lists. Encoding
the sampled gaps with unsigned LEB128 used an estimated 26.8% of the raw posting
bytes, a 73.2% reduction. The distribution is highly skewed:

- median posting length: 6;
- lists longer than 1,024 references: 1.77% of lists; and
- those long lists' share of posting bytes: 91.61%.

This supports adaptive per-list codecs rather than one universal representation.
The simple LEB128 result is a size probe only; it does not establish the best
decode/intersection codec. Applied arithmetically to all current posting bytes,
the sampled ratio suggests about 2.87 GB of possible reduction before codec
tags and alignment. That estimate must be reproduced from a saved
machine-readable run before it is used as a design acceptance result.

Routing contains 9,254,912 40-byte open-addressed buckets for 3,563,222 exact
keys, an observed load factor of approximately 0.385. Bucket bytes account for
about 370.2 MB and repeated key strings for about 301.0 MB. A probabilistic
filter can drastically reduce negative-check metadata, but it cannot replace
the authoritative exact directory or be allowed to produce false negatives.

## Query-profile evidence

The following values are medians across the nine `default-pread` repetitions
labelled `evicted` in the scheduler matrix. Each repetition used a fresh query
process. The runner applied `POSIX_FADV_DONTNEED` and verified residency only
for `chunks.bin`; it did not evict or measure `symbols.bin`, `series.bin`,
`chunk_index.bin`, or `indexes.puffin`. These are therefore fresh-process,
payload-page-evicted measurements, not fully cold metadata measurements. They
are representative evidence for these query shapes, not universal constants.

### Selective scalar `rate`

Raw reports:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/
  io-uring-promql-shapes-20260712-155258/
  scalar_count_rate-default-pread-evicted-{1..9}.md
```

| Stage/result | Measurement |
|---|---:|
| Query wall time | 166.243 ms |
| Surviving `symbols.bin` files loaded | 233.39 MB |
| Symbol load/validation | 99.205 ms |
| Whole metric-range payload | 8.19 MB |
| Metric-range read/decode | 14.856 ms |
| Series entries | 3.626 ms |
| Chunk index | 1.161 ms |
| Logical chunk bytes used | 5.022 MB |
| Coalesced chunk bytes read | 19.216 MB |
| Payload read amplification | 3.826x |
| Chunk payload stage | 20.954 ms |

The full symbol load alone consumed about 60% of query wall time. Because
metadata page residency was uncontrolled, this is evidence of the combined
whole-file read, validation, allocation, and copy cost rather than a measured
cold-storage latency. Payload I/O was important, but it was not the primary
cost.

### Sparse scalar regex

Raw reports:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/
  io-uring-promql-shapes-20260712-155258/
  sparse_scalar-default-pread-evicted-{1..9}.md
```

| Stage/result | Measurement |
|---|---:|
| Query wall time | 2.518 s |
| Regex values examined | 112,236 |
| Matched/projected series | 254,219 |
| Exact page reads | 1,044 x 16 KiB |
| Exact page bytes read | 17.105 MB |
| Exact posting bytes read | 2.048 MB |
| Series entry/label stage | 428.036 ms |
| Chunk index stage | 43.301 ms |
| Logical chunk bytes used | 38.042 MB |
| Coalesced chunk bytes read | 133.521 MB |
| Payload read amplification | 3.510x |
| Chunk payload stage | 95.077 ms |

The point-lookup pattern read much more exact-page data than posting data, and
series/label work was almost five times the payload stage.

### Payload scheduler limit

The tracked real-corpus io_uring report found that a roughly 16.4% sparse
payload-stage improvement changed end-to-end latency by approximately 0.64%:
[io_uring real-corpus sparse query report](../experiments/iouring/io_uring_real_corpus_sparse_query_20260711.md).

The later scheduler matrix reached the same general conclusion and also found a
warm sparse-payload regression in one comparison:
[chunk scheduler experiment](../experiments/iouring/io_uring_chunk_scheduler_20260712.md).

Conversely, a full native read already achieved approximately 1.010x payload
read/used amplification, showing that the existing series-major layout and
coalescing can be excellent when the query consumes the full native records:
[cross-segment flow report](../experiments/iouring/io_uring_cross_segment_flow_20260711.md).

## Current implementation findings

### 1. Symbols are eager whole-file metadata

`chronoxide-core/src/storage/series.rs:256-303` reads the entire symbols file,
validates it, constructs native offsets, and copies/drains bytes. A surviving
`SegmentQueryContext` obtains the complete symbols value at construction time
(`segment/query_context.rs:47-88`). The value is cached, but the cold cost is
paid per surviving segment and the cache lifetime is not governed by the
storage spec's explicit open-segment byte budget.

A zero-copy mapping could remove allocation and copying within the current
format, but current v2 structural validation still touches the entire
dictionary. Truly lazy symbol lookup requires independently checksummed pages
or an equivalent authenticated subdivision.

### 2. Metric-range lookup reads the whole blob

`storage/index/v7/reader.rs:362-375` reads and parses the complete metric-range
payload for a lookup. The corpus payload is only about 8.3 MB in total, but the
selective query spent 14.6 ms reading it across segments. Current writers often
produce one series range per metric, making a point-addressable inline-range
fast path especially attractive.

### 3. Series decoding copies and hashes row data

The series reader coalesces adjacent fixed-table reads, which is good, but then
copies each selected keyset row into a `Vec<u8>` stored in a `HashMap`
(`storage/series.rs:692-763`). It subsequently performs value dictionary lookup
and creates complete labels (`storage/series.rs:821-980`). For dictionaries
above 1,024 values, an unseen code can cause an individual four-byte positional
read.

This path should instead stream or tile row blocks, deduplicate requested
dictionary locations, and keep symbol/value codes intact until labels are
actually needed.

### 4. Labels are materialized before payload evaluation

The generic selector path materializes labels before payload planning and
decode (`segment/query_reader/generic.rs:146-245`). Projection creates owned
strings (`segment/query_reader/projection.rs:237`), while many aggregation paths
only need a subset of grouping labels and a stable series identity.

Materializing only output/grouping labels would reduce reads, allocation,
hashing, peak RSS, and result-construction CPU. Matching and grouping should
remain on compact IDs as long as possible.

### 5. Sparse dictionary and metadata reads are under-coalesced

Fixed series table rows are batched only when exactly adjacent. Dictionary
codes, keyset rows, chunk-index ranges, and exact pages can still become many
small positional reads. The same bounded-gap scheduler principle used for
chunk payloads can be applied to immutable metadata without changing its
format or error semantics.

### 6. `ChunkPayloadBatch::slice` performs a linear search

`storage/chunk/reader.rs:211` searches physical spans linearly for each logical
slice. Requests are ordered, so a monotonic cursor is normally sufficient; a
binary search is a safe fallback for out-of-order access. This is a no-format
CPU fix, not a layout redesign.

### 7. Metadata caches are not consistently bounded

Per-segment symbols, metric ranges, locators, entries, and chunk-entry caches
include unbounded `HashMap` storage (`segment/query_types.rs:10` and related
contexts). Adding more page caches without a unified byte budget could improve
latency while causing unbounded RSS and file-descriptor retention. Cache design
must satisfy the bounded LRU/open-segment policy in `storage.md`.

### 8. Regex lookup repeats exact-page work

Regex lowering enumerates matching strings, maps them through symbols, resolves
each exact record, and repeatedly allocates/unions decoded postings
(`segment/promql_lowering.rs:355` and the v7 exact reader). The measured sparse
query performed 1,044 reads of 16 KiB exact pages to obtain about 2.05 MB of
postings.

The path should batch values by exact-directory page, cache validated touched
pages, use direct FST values instead of string round trips, and use the
specified series-driven fallback when an existing candidate set is already
selective.

### 9. Hot series and chunk metadata are large for the corpus shape

The fixed series table is 40 bytes per series. The final eight bytes are always
zero in the current writer and non-zero metadata is rejected by the reader
(`storage/series.rs:780-831`, `1246-1257`). `chunk_index.bin` then adds an
eight-byte directory slot plus a 40-byte entry (`storage/chunk/index.rs:3-65`,
`storage/chunk/types.rs:70-95`).

This is 88 bytes of fixed hot metadata for the observed one-chunk series before
keyset rows, symbols, postings, or chunk headers.

### 10. The writer does not implement configured chunk targets

The segment writer emits one whole series/window chunk in the ordinary and
typed paths (`segment/writer.rs:532-684`). `storage.md` specifies
`chunk_target_bytes` and `chunk_target_points`, but core
`SegmentWriterConfig` does not carry those targets (`segment/layout.rs:210-245`).

The current corpus is too sparse for splitting to help, but dense series need
bounded chunks so narrow time queries do not decode an entire segment-window
record.

### 11. Typed scalar and native records duplicate common data

Native typed values contain timestamps, start-time/flags/reset metadata, count,
and optional sum. The compact scalar lane then encodes aligned timestamps,
metadata, count, and sum again (`storage/chunk/writer.rs:86-135`,
`storage/chunk/codec.rs:99-147`). The scalar decoder reads both count and sum
data even when the projection needs only one (`storage/chunk/codec.rs:169-204`).

The scalar lane sits between each chunk header and the native payload. Bounded
gap coalescing can therefore pull native-payload gaps into a scalar-only query,
which is consistent with the measured 3.5-3.8x amplification.

### 12. Float encoding selection is not adaptive

Sealed Float chunks are written with Gorilla encoding in the normal path
(`storage/chunk/writer.rs:147-216`). Application configuration exposes float,
integer, and variable-length encoding choices, but `to_core_config()` does not
forward them (`chronoxide-ingester/src/app_config.rs:267-333`).

Existing capture-driven head benchmarks found Gorilla decoding approximately
5.7-7.2 times slower than raw decoding for a 13-20% payload reduction:
[head buffer benchmark results](../stats/head_buffer_bench_results.md).
Those head results do not prove the best sealed-segment codec, but they justify
an A/B test of deterministic per-chunk raw/Gorilla selection and more
SIMD-friendly block codecs.

### 13. Postings are raw and decoded eagerly

`storage/index/v7.rs:315-379` writes exact posting lists as raw `u32`
references. The reader validates and materializes a decoded vector
(`storage/index/v7/reader.rs:736-788`). This is simple and correct, but consumes
3.923 GB in the measured corpus and prevents encoded-domain intersections or
unions.

### 14. Exact routing repeats strings in a low-load hash table

Routing writes exact label name/value bytes into a 40-byte open-addressed table
(`storage/index.rs:904-1018`, `1191-1234`). Capacity is more than twice the
entry count in the observed corpus. This makes negative equality pruning fast
but expensive in storage and cache footprint.

### 15. One chunk is one frame

The writer and reader currently enforce one chunk per frame
(`storage/chunk/writer.rs:487-504`, `storage/chunk/reader.rs:278-310`), despite
the specification's target of tens-of-KiB frames. This costs 14 bytes per chunk
and provides little amortization for the corpus's approximately 214-byte
average indexed chunk.

### 16. Footer checksumming reads whole files into memory

`storage/segment/footer.rs:84-106` uses `fs::read` for every tracked segment
file before hashing it. This is a sealing-time issue rather than a query-path
issue, but it increases memory pressure and rereads all newly written data. The
files are processed one at a time, so their allocations are not cumulative, but
peak memory can still include the largest segment file while the decoded head
window remains live. Checksums should be accumulated while writing or computed
by bounded streaming reads.

### 17. Replay sealing materializes a complete active head window

The application exposes independent head-buffer and writer durations, but the
writer-enabled construction path does not use both. When a segment writer is
configured, `chronoxide-ingester/src/main.rs` constructs `HeadConfig` with the
writer's `segment_duration`; it consults
`head_buffer.window_duration_secs` only when no writer is configured. The
measured replay configuration sets those values to 900 and 3,600 seconds,
respectively, so its normal head windows and output segments are both 15
minutes. The configured 3,600-second OOO value remains the per-series lateness
acceptance window, and accepted late samples are stored in 15-minute-aligned
OOO windows.

`HeadWindow::into_series_samples` seals and decodes every encoded series in one
normal or OOO window into one `Vec`, and the ingestion pipeline sorts that
complete vector before passing it to `SegmentWriter`
(`storage/head/window.rs:23-43`, `processor/otlp/pipeline.rs:197-216`). During
decode, the arena-backed encoded 15-minute window and the growing decoded
result coexist. During segment writing, the complete decoded window remains
live while segment metadata is built and footer files may be read for hashing.
Typed raw variable-length values, especially Histogram and
ExponentialHistogram buckets, can make the decoded representation materially
larger than the encoded head payload.

Shutdown adds a multi-partition amplification risk: `flush_head` first drains
all normal and OOO windows from every partition into one `Vec<HeadWindow>`, then
writes them (`processor/otlp/pipeline.rs:177-193`). The selected capture has one
partition, but the implementation must remain bounded when more partitions or
OOO windows are present.

The segment writer itself streams chunk payload bytes to file-backed buffered
writers and retains primarily per-segment symbols, series entries, chunk index
entries, and other metadata. It does not intentionally retain every completed
segment payload in heap.

### 18. Replay has lifetime-wide retained state

The ingestion processor retains the labelset interner and Histogram and
ExponentialHistogram reset-state maps across the replay. Their memory does not
fall when a head window seals, so they can create a rising RSS baseline on a
high-cardinality corpus independently of transient seal peaks. Allocator high
water marks may also keep process RSS high after Rust values are dropped.

Replay profiling must distinguish at least:

- encoded head capacity and used bytes;
- decoded seal bytes and series count;
- segment-writer retained metadata;
- footer-hashing buffers;
- label interner and reset-state entries; and
- allocator-retained versus live heap bytes.

## Near-term improvements without a new format

These changes should establish a stronger baseline before committing to a vNext
layout.

### A. Bound replay and sealing memory

- Record live-heap/RSS checkpoints before head decode, after decode/order,
  during each segment seal, after footer creation, and after release.
- Process drained partition/OOO windows one at a time instead of first retaining
  every drained window in a collection.
- Prototype a bounded seal flow that decodes and writes one deterministic tile
  or series group at a time rather than materializing a complete active or OOO
  window.
- Keep logical head/query/OOO policy independent from physical segment duration;
  matching the two durations is a useful replay experiment, not a required
  semantic coupling.
- Stream footer checksums and measure lifetime-wide label interner and reset
  state separately from transient seal memory.
- Require identical semantic fingerprints, `QueryStats`, segment IDs, and
  deterministic output bytes for an identical writer configuration.

### B. Keep labels encoded through planning and aggregation

- Carry segment-local symbol/value codes through matching and payload decode.
  Before cross-segment grouping or comparison, map selected values into a
  query-global identity or compare canonical bytes with collision verification.
- Resolve only labels needed for output or grouping where PromQL semantics allow
  it. Raw selectors, `without`, label functions, and vector matching may still
  require most or all labels.
- Use borrowed or symbol-backed label views where ownership is not required.
- Preserve collision verification when stable series IDs are hash-derived.

### C. Batch immutable metadata reads

- Decode all selected series rows first.
- Sort and deduplicate dictionary offsets, then read pages or bounded-gap spans.
- Coalesce nearby fixed table, keyset-row, chunk-index, and exact-page ranges.
- Batch regex matches by exact page rather than doing one logical lookup at a
  time.
- Add a validated exact-page cache under one explicit byte budget.

### D. Bound and unify caches

- Put symbols, metric ranges, exact pages, series rows, and chunk metadata under
  a session/open-segment memory budget.
- Charge actual retained bytes, not only entry counts.
- Ensure eviction does not turn a previously detected corruption into a miss.
- Cap open files and mapped regions consistently with the manifest-driven
  segment inventory.

### E. Reduce current-format CPU overhead

- Replace `ChunkPayloadBatch::slice` linear scans with a monotonic cursor or
  binary search.
- Stream keyset row decode instead of building a per-row `Vec`/`HashMap` graph.
- Avoid repeated postings unions where an encoded or bitset accumulator can be
  reused.
- Implement the series-driven regex fallback when the candidate base is
  already selective.

### F. Implement existing chunk targets

Honor deterministic byte and point targets already described by the storage
spec. Preserve stable series/chunk order, typed metadata, and deterministic
replay bytes. Add coverage for chunks that split exactly at, before, and after
the thresholds.

### G. Honor and measure existing codec choices

Forward application encoding configuration into the core writer. Benchmark one
identical release binary where runtime configuration is sufficient. If an
adaptive choice is added, make it deterministic from the input bytes and
writer configuration.

### H. Stream footer checksums and implement required durability

Accumulate per-file checksums during writes where practical, or hash in bounded
blocks after close. Add the file and directory synchronization required by the
chosen sealing policy before manifest publication.

## Candidate storage vNext changes

These are design options, not an approved combined format. Each should be
prototyped and measured independently because their savings overlap and their
CPU/cache tradeoffs differ.

Every prototype must name its primary objective and report cold latency, warm
latency, peak and retained RSS, disk footprint, replay/sealing throughput, and
corruption-surface complexity separately. A disk-size estimate is not evidence
of a latency improvement. In particular, the current sparse query reads only
about 2.05 MB of exact postings despite the corpus containing 3.923 GB of them;
postings compression may be an excellent capacity improvement while adding CPU
or having little query-latency effect.

### 1. Paged `symbols.bin v3`

This option was implemented as storage schema 6 and passed the deterministic
two-million-message prefix and same-binary semantic-equivalence gates. It is a
preserved comparison baseline rather than the final combined vNext format; see
[the focused design](../superpowers/specs/archive/storage/2026-07-13-storage-vnext-paged-symbols-design.md) and
[prefix report](../experiments/storage_vnext/2026-07-13-prefix-results.md).

Use independently checksummed symbol pages with a small root directory. A page
descriptor should contain enough authenticated information to route both
directions without scanning the entire file, for example:

- symbol-ID base and count;
- page offset and length;
- first/last string or equivalent search fences;
- page checksum; and
- any restart/offset-table metadata needed for bounded decoding.

Desired operations:

- `string -> symbol_id` touches the root and one or a few candidate pages;
- `symbol_id -> string` touches the root and one page;
- a batch of output symbol IDs is sorted/grouped by page;
- full validation remains available as an explicit footer/full-scan pass; and
- every touched malformed page fails the query.

The root and page boundaries must be deterministic. Prefix compression is
reasonable within a page if random ID lookup remains bounded.

### 2. Point-addressable metric ranges and exact values

Replace the variable whole metric-range blob with a fixed or page-framed
directory:

```text
metric_symbol_id -> inline single range | offset + count
```

Keep overflow range records separately checksummed. For exact label values,
prefer an `fst::Map` whose value is a symbol ID, exact-directory ordinal, or
other verified locator. Regex automaton traversal can then return locators
without repeated string-to-symbol round trips.

Test smaller exact pages, such as 4 or 8 KiB, against the current 16 KiB pages.
Smaller pages reduce point-lookup amplification but increase root descriptors
and may hurt scans. Cache only pages that were fully validated.

### 3. Compact, column-oriented series metadata

Split hot routing fields from cold or optional metadata:

```text
SeriesHot {
    series_id
    min/max time relative to segment
    keyset_id + row
    inline chunk tag or overflow locator
}
```

Candidate properties:

- 32-byte hot series records where bounds prove that size sufficient;
- segment-relative `u32` time deltas and offsets only where segment duration and
  file-size bounds are checked;
- optional metadata in a bitmap-addressed sidecar rather than eight zero bytes
  in every hot record;
- 128- or 256-row keyset tiles with one bitpacked column per label key;
- page-local dictionaries or stable dictionary locators; and
- independent checksums for every touched tile/page.

Column stripes let queries read only grouping/output labels and create useful
SIMD work: vector comparisons, bitset combination, and block dictionary gather.

### 4. Inline single-chunk descriptor with overflow

Encode the common one-chunk case directly in `SeriesHot`. A compact descriptor
can use segment-relative offsets/times and implicit fields derived from the
series or file. Multi-chunk, dense, OOO, or future exceptional series use a
separate overflow table.

The overflow representation remains fully general and independently
checksummed. Readers must not infer that all corpora share the current 100%
single-chunk distribution.

Estimated footprint effects on this corpus:

- removing the separate eight-byte chunk directory: about 382 MB;
- removing the unused eight series bytes: about 382 MB; and
- shrinking a 40-byte descriptor to 24 bytes: about 764 MB more.

The combined approximately 1.53 GB is a size estimate before alignment, roots,
overflow records, and new checksum metadata. It is not a latency guarantee.

### 5. Adaptive postings codecs

Give each posting list a deterministic codec tag. Candidate policy:

| Shape | Candidate representation |
|---|---|
| Empty/singleton/tiny | Inline refs or tiny delta list |
| Short sparse | Delta ULEB128 or StreamVByte |
| Medium sparse | StreamVByte or SIMD-BP128 |
| Dense or run-heavy | Roaring/run containers |

The writer may choose the smallest candidate from a fixed deterministic set, or
use a deterministic threshold policy proven by benchmarks. Required operations
include:

- exact iteration;
- intersection with an already-selective candidate set;
- union across regex values;
- complement under the correct metric/time universe; and
- corruption-safe count/order/bounds validation.

Where possible, intersections and unions should operate on encoded containers
instead of first allocating a second full `Vec<u32>`. SIMD-BP128 and
StreamVByte should be evaluated for decode throughput, branch behavior, and
tail handling; Roaring should not be forced on tiny lists.

### 6. Compact routing

Keep exact metric-name routing because it gives high-value early segment
pruning. For arbitrary equality absence checks, evaluate a two-tier design:

1. a compact exact metric router; and
2. a checksummed Bloom, Xor, or similar no-false-negative filter as a negative
   hint for other label pairs.

A filter hit means only "maybe present." The authoritative exact directory and
time metadata must still be consulted. A 10-bit/key filter for the observed
3.56 million exact keys would be about 4.45 MB before the exact metric router,
but its CPU cost, construction determinism, checksum scheme, and adversarial
behavior require measurement.

An exact compact hash/MPHF alternative is also viable if it verifies the full
key and has a deterministic construction/fallback path.

### 7. Shared typed columns or a scalar sidecar

The ideal vNext layout should avoid duplicating aligned common data while
allowing narrow PromQL projections:

```text
common: timestamp, start_time, flags, temporality/reset metadata
count:  independently addressable count column
sum:    independently addressable signed IEEE optional-sum column
native: bounds/buckets/scale/zero/extrema/quantiles/exemplars
```

Count, sum, and native projections should be independently locatable and
checksummed. A count query must not read or decode sum/native bytes. Negative or
non-finite delta sums must remain valid signed interval values and must not
invalidate count/bucket results.

A lower-risk experiment can first place the current duplicated scalar lanes in
a metric/series-major `typed_scalars.bin`. That isolates scalar reads and tests
the amplification benefit without immediately changing native decoding. If it
wins, a later design can decide whether shared common columns justify the extra
read indirection for native queries.

Every sidecar/column locator must identify the authoritative source chunk and
be protected against cross-chunk substitution or partial corruption.

### 8. Packed multi-chunk frames

Pack consecutive series-major chunks into approximately 64 KiB frames, with
32-256 KiB evaluated as a range. Preserve:

- direct per-chunk locators;
- stable series/chunk order;
- an independently validated checksum covering each chunk header, scalar/common
  lanes, and native payload;
- an outer frame checksum for full-frame scans/recovery; and
- bounded reads for an individual chunk.

Packing could reduce the current approximately 669 MB frame-header cost to a
few MiB, but its main benefits may be storage, sealing, and scan efficiency
rather than random query latency. Do not page-align approximately 214-byte
chunks individually.

### 9. Adaptive sample and timestamp codecs

Evaluate raw versus Gorilla per Float block using a deterministic selection
rule. Also evaluate block-oriented timestamp representations for regular
streams, such as base + fixed step + exception bitmap, and SIMD-friendly delta
blocks for irregular streams.

Codec experiments must measure:

- encoded bytes;
- scalar and full decode throughput;
- random range startup cost;
- branch misses and cycles/sample;
- cold and warm end-to-end queries; and
- interaction with chunk size.

The best microbenchmark codec is not automatically the best query codec.

### 10. Adjacent-segment packing

After per-segment metadata is fixed, evaluate a deterministic packer that
combines adjacent immutable 15-minute segments into a 1-2 hour physical block
with shared symbols/series/postings. Retain smaller event-time subgroups and an
active-series bitmap so narrow ranges do not scan the whole packed block.

The packer must preserve manifest, head, and OOO precedence exactly. Repeated
series across subgroups may share metadata, but samples must retain deterministic
ordering and segment/source identity for corruption reporting and replay
equivalence.

Physical ordering alternatives such as time-subgroup -> metric -> series should
be compared against the current series-major order. Do not select a new order
from intuition alone; full native reads already demonstrate excellent locality
with the current order.

## Correctness and specification gaps found during review

These findings should be resolved independently of whether a vNext performance
layout proceeds.

### Chunk header integrity on indexed reads

The frame checksum covers the chunk header, scalar lane, and payload when the
writer emits a frame (`storage/chunk/writer.rs:487-504`). The chunk index,
however, points after the 14-byte frame header. The indexed hot reader decodes
the 40-byte chunk header and validates `chunk_crc32c` over the native payload
only (`storage/chunk/codec.rs:25-85`). The scalar lane has its own checksum, but
header fields such as kind, encoding, series reference, times, counts, lengths,
and flags are not independently authenticated by that indexed read.

Some corruptions will be caught by index/header comparisons or bounds checks,
but the integrity boundary is incomplete. A vNext chunk checksum should cover
the header with the checksum field zeroed, every directly associated lane, and
the native payload, or provide equivalent authenticated subrecords.

### Strong-sealing durability gap

`storage.md` requires file and directory `fsync` before strong segment
publication. The reviewed writer flushes Rust file buffers, writes the footer,
renames the temporary directory, and appends the manifest record
(`storage/segment/writer.rs:974-1091`), but the path does not visibly synchronize
all segment files and the containing directory.

This must be covered by explicit implementation and crash/recovery tests. A
performance change must not treat `flush()` as equivalent to durability.

### Missing timestamp fallback conflicts with project semantics

`chronoxide-core/src/otlp.rs:11-19` accepts `fallback_ts_ms` when an OTLP
datapoint timestamp is zero, and WAL replay passes the stored fallback through
for all supported datapoint kinds (`storage/wal_replay.rs:170-306`). This
conflicts with the project invariant that missing OTLP datapoint timestamps are
rejected and that source timestamps must not become event time.

The storage prose near `CaptureRecord` also says source timestamps may be useful
for transports without datapoint timestamps, which is ambiguous against the
stricter invariant. The normative specifications and implementation must be
made unambiguous before relying on replay equivalence.

### Raw postings versus compressed-postings prose

The normative v7 byte layout describes raw `u32` posting references, matching
the implementation. Later prose says large sets use Roaring and small sets use
delta-encoded sorted lists. The current format must be documented as raw, and
compressed postings must be introduced only through an explicit codec-tagged
format revision.

### Head and segment duration drift

`storage.md` describes a one-hour default. Application
`SegmentWriterConfig::default_segment_duration_secs()` returns 15 minutes
(`chronoxide-ingester/src/app_config.rs:300-302`). Documentation, configuration,
and benchmark interpretation must name the actual default rather than silently
assuming one.

The early implementation-status prose says head window duration is tied to
`segment_duration`; that is true specifically for the writer-enabled
application path. Although application configuration exposes independent head
and writer duration fields, `chronoxide-ingester/src/main.rs` ignores
`head_buffer.window_duration_secs` when the writer is enabled and constructs
the head with the writer's 15-minute duration. It uses the configured head
duration only in the no-writer head path. The OOO acceptance duration is still
applied in both paths and must not be confused with physical OOO window size.
The normative specification and configuration surface should make those
effective rules explicit so memory and benchmark interpretations use the
actual window shape.

### Encoding ID drift

`storage.md` describes per-kind encoding IDs, including Float Gorilla as 0 and
raw f64 as 1. The implementation uses one global `ChunkEncoding` enum where 0
is schema-varlen, 1 is raw f64, and 3 is Gorilla
(`storage/chunk/types.rs:55-68`). The normative byte layout and current emitted
bytes must be reconciled before another codec is assigned an ID.

### Schema table and `SINGLE_SCHEMA` drift

The specification shows fixed `u32` schema counts and lengths and permits
omitting per-sample schema IDs under `SINGLE_SCHEMA`. The implementation encodes
counts and lengths as unsigned LEB128 and always writes a schema ID per sample
(`storage/encoding/schema_varlen.rs:54-145`). The actual on-disk contract must
be specified exactly and covered by deterministic golden bytes.

### Series metadata drift

The specification assigns `meta_off`/`meta_len` to optional typed series TLVs.
The current writer emits zero metadata and the reader rejects non-zero metadata.
Do not reclaim or reinterpret these bytes in a compact format until the
authoritative location of temporality, monotonicity, reset policy, and original
OTLP type is settled.

### Chunk/frame target drift

The storage specification provides `frame_target_size`,
`chunk_target_points`, and `chunk_target_bytes`. The writer emits one chunk per
series/window and one chunk per frame, and core writer configuration lacks those
targets. The prose should distinguish current implementation from the intended
policy.

### Encoding configuration is dropped

The application config exposes float, integer, and variable-length encoding
choices but does not copy them into `CoreSegmentWriterConfig`. Configuration
that has no effect should not be presented as an operational tuning control.

### Stale mmap guidance

Some query walkthrough prose still labels symbols, series, and indexes as
`mmap` inputs. Current v7 index policy deliberately uses lazy immutable
positional reads and bounded state. Update that prose when the current format is
next revised; do not use the stale wording to reintroduce eager complete-index
mapping.

### Head status drift

Early `storage.md` implementation-status sections say the head is not queryable,
while the current query store contains explicit sealed+head selector and PromQL
paths (`storage/segment/query_store.rs` and `query_store/head.rs`). The status
section should be updated separately so layout decisions use the actual query
surface.

## Priorities and independent experiment tracks

### Priority 0: correctness/specification alignment

Before adopting a prototype as the current storage contract:

1. settle chunk/header checksum coverage;
2. implement and test the selected sealing durability policy;
3. remove missing-datapoint timestamp fallback;
4. make current encoding IDs and schema bytes normative;
5. document independent head/segment durations and current head-query status;
6. define the query-global identity used when segment-local label codes cross
   segment boundaries; and
7. distinguish current raw postings from future compressed codecs.

### Priority 1: bounded replay/sealing baseline

Implement and measure:

1. phase-specific live-heap and RSS telemetry for head decode, ordering, segment
   writing, footer hashing, and release;
2. one-at-a-time partition/OOO window draining;
3. bounded or streaming head-to-segment decode/write flow;
4. bounded streaming footer checksums;
5. label-interner and typed reset-state growth; and
6. semantic and deterministic-byte equivalence under an identical writer
   configuration.

This addresses the observed high replay RSS without conflating it with a sealed
read-layout change.

### Priority 2: no-format metadata baseline

Implement and measure:

1. delayed/partial label materialization;
2. batched dictionary and exact-page reads;
3. bounded validated metadata caches;
4. streaming keyset decode;
5. regex page grouping and series-driven fallback; and
6. `ChunkPayloadBatch` cursor/binary lookup.

This establishes how much of the measured metadata cost is algorithmic versus
fundamentally caused by the byte layout. It may proceed in parallel with the
focused vNext work and is a comparison baseline, not an experiment gate.

### Priority 3: focused vNext prototypes

Prototype separately, in this order:

1. paged `symbols.bin` v3 (completed schema-6 A/B baseline);
2. point-addressable metric ranges;
3. inline single-chunk series metadata;
4. adaptive postings codecs;
5. separate typed scalar storage/shared typed columns; and
6. packed frames.

Avoid combining all changes into one first experiment. Independent variants
make latency, space, RSS, and complexity effects attributable.

Because the project is alpha and old corpora can be regenerated by
deterministic replay, a focused format prototype does not wait for completion
of the no-format baseline. Adoption still requires a replayed real-corpus A/B
comparison with matching query semantics and explicitly attributed latency,
space, RSS, and complexity effects. Space-only wins remain valid, but must be
presented as capacity/sealing improvements rather than query-latency claims.

### Priority 4: broader physical reorganization

Only after fresh profiles:

- adaptive sample/timestamp block codecs;
- columnar SIMD keyset scans beyond the selected-label path; and
- adjacent-segment packing or a changed physical sort order.

## Required validation before adoption

### Byte-level correctness

- deterministic golden bytes for every root, descriptor, page, codec, and
  overflow variant;
- byte-for-byte deterministic replay under identical input order and writer
  configuration;
- round trips for minimum, maximum, empty, singleton, and overflow cases;
- explicit integer overflow and segment-relative bound tests;
- truncation at every structural boundary;
- checksum corruption in roots, directories, pages, headers, columns,
  postings, frames, and footers;
- malformed counts, ordering, offsets, lengths, codec tags, and page fences;
- proof through tests that touched corruption never becomes a miss or prune;
  and
- crash tests around file sync, directory sync, rename, manifest publication,
  and restart.

### Semantic equivalence

- replay/readback equivalence for every OTLP kind;
- event-time placement and age/lead policy equivalence;
- typed temporality, flags, start time, reset hints, stale markers, optional
  values, and signed delta sum equivalence;
- exact, negative, regex, absent-label, and empty-label matcher equivalence;
- cross-segment grouping and vector matching when identical canonical labels
  have different segment-local symbol/value codes;
- `by`, `without`, raw-selector, label-function, and collision-verification
  equivalence under delayed label materialization;
- head/sealed/OOO duplicate and precedence equivalence;
- deterministic segment IDs; and
- independent `chronoxide-query --verify-readbacks` coverage with executed and
  skipped diagnostics inspected.

### Performance methodology

Every A/B comparison must use:

- the same host and isolated workload window;
- the same Rust toolchain and release settings;
- a fingerprinted corpus;
- the same query schedule, query limits, and cache budgets;
- one identical binary for runtime-flag comparisons;
- recorded binary hashes for code-version comparisons;
- matching semantic fingerprints and explained `QueryStats` differences; and
- separate footer validation outside timed query runs.

Cache state must be named per artifact rather than described by one global
`cold` label. At minimum distinguish:

- fresh process/session versus a long-lived API process;
- metadata page-cache residency for symbols, series, chunk indexes, and
  `indexes.puffin`;
- chunk-payload page-cache residency; and
- retained application-cache bytes under the configured budget.

Page-cache eviction claims require per-artifact eviction and residency evidence.
Use medians across the complete repetition schedule for headline comparisons;
individual runs remain diagnostic evidence.

Report at minimum:

- cold first-expression latency in a fresh query session;
- warm repeated latency in both fresh-process and long-lived API schedules;
- peak RSS and bounded-cache charges;
- opened and logically read metadata bytes;
- logical payload-used bytes;
- coalesced payload-read bytes;
- read/used amplification;
- result series/samples and semantic fingerprints; and
- profiler evidence for CPU-oriented changes.

Format experiments that affect writing must additionally report replay
throughput, seal latency, output bytes by artifact, head/decode/footer peak
memory, retained post-seal RSS, and deterministic output fingerprints.

Microbenchmarks should report cycles/value, branches, branch misses, and bytes,
but real replay/smoke data remains the performance authority.

### Engineering checks

Run focused tests during development, then the repository-required formatting,
workspace tests, clippy checks, `git diff --check`, corruption suites, and the
independent readback oracle. Any unavailable command must be recorded with its
reason.

## Recommended next work and design document

The selected narrow `symbols.bin` v3 experiment described in the
[paged-symbol design](../superpowers/specs/archive/storage/2026-07-13-storage-vnext-paged-symbols-design.md) is
complete and retained as the schema-6 A/B baseline. The active isolated
follow-up is the
[schema-7 inline-series/v8 design](../superpowers/specs/archive/storage/2026-07-13-storage-schema7-inline-series-design.md).
Its [v8-aware materiality model](../experiments/storage_vnext/2026-07-13-schema7-layout-model.md)
projects a gross 2,286,642,112-byte series/chunk-index saving, a
28,764,752-byte index-v8 charge, and a net 2,257,877,360-byte saving: 10.48% of
all modeled standard artifacts and 21.21% of modeled metadata. Rust segment
integration, deterministic prefix replay, and the same-binary schema-6/schema-7
A/B remain pending; these modeled bytes are not measured latency or replay
evidence.

The bounded replay/sealing and no-format metadata work can continue in
parallel. Point-addressable metric ranges, compressed postings, and the broader
physical reorganizations remain separate follow-up designs so their byte and
latency effects are independently reviewable.

## Related documents

- [Storage layer specification](../superpowers/specs/storage.md)
- [Clock and event-time specification](../superpowers/specs/clock.md)
- [Paged `symbols.bin` v3 design](../superpowers/specs/archive/storage/2026-07-13-storage-vnext-paged-symbols-design.md)
- [Schema-7 inline-series/v8 design](../superpowers/specs/archive/storage/2026-07-13-storage-schema7-inline-series-design.md)
- [Schema-7 v8-aware layout model](../experiments/storage_vnext/2026-07-13-schema7-layout-model.md)
- [Segment index v7 design](../superpowers/specs/archive/storage/2026-07-10-segment-index-v7-design.md)
- [Shared segment index directory design](../superpowers/specs/archive/storage/2026-07-10-shared-segment-index-directory-design.md)
- [General chunk-payload read scheduler design](../superpowers/specs/archive/benchmarks/2026-07-12-chunk-payload-read-scheduler-design.md)
- [io_uring real-corpus sparse query report](../experiments/iouring/io_uring_real_corpus_sparse_query_20260711.md)
- [io_uring chunk scheduler experiment](../experiments/iouring/io_uring_chunk_scheduler_20260712.md)
- [io_uring cross-segment flow report](../experiments/iouring/io_uring_cross_segment_flow_20260711.md)
- [Head buffer benchmark results](../stats/head_buffer_bench_results.md)
