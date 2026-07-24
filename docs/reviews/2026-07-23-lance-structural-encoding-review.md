# Lance structural-encoding ideas for Chronoxide

- **Date:** 2026-07-23
- **Status:** Exploratory, non-normative design review
- **Source paper:** [Lance: Efficient Random Access in Columnar Storage through Adaptive Structural Encodings](https://arxiv.org/html/2504.15247v1)
- **Current authority:** [storage.md](../superpowers/specs/storage.md), [clock.md](../superpowers/specs/clock.md), and
  [PromQL coverage](../promql-coverage.md)

This document records potentially useful ideas from the Lance paper and maps
them to Chronoxide's current storage and query architecture. It is experiment
selection evidence, not authorization for a new storage schema, a current
backlog, or a replacement for the normative storage specification.

## Conclusion

The paper is useful primarily as a design framework. Chronoxide should not
adopt Lance wholesale or copy its thresholds directly. Lance studies arbitrary
row retrieval from general nested columnar data, whereas Chronoxide normally
performs:

1. label-postings and metric-range selection;
2. routing to many often-correlated series;
3. time-range reads from metric-query-ordered payloads; and
4. PromQL projection, merge, and aggregation.

The paper nevertheless reinforces three principles that apply directly:

- choose structural encoding according to the access pattern rather than
  forcing one layout on every value;
- use small, independently decodable units to bound random-read and decode
  amplification; and
- distinguish the logical encoding unit from the physical I/O and packing unit.

Chronoxide already applies several of these principles. The strongest new
experimental directions are page-aware physical read planning and restartable
blocks for dense sample streams. Neither currently justifies an immediate
on-disk version change.

## What Chronoxide already does well

### Adaptive inline and overflow metadata

Schema 8 retains Schema 7's adaptive series layout:

- the common single-chunk case packs label location, kind, time bounds, payload
  location, length, scalar-lane length, and prefix integrity into one fixed
  40-byte hot record;
- hot records are grouped in independently checksummed pages; and
- multi-chunk, mixed-kind, mixed-lane, and width-exception series use a complete
  overflow representation.

This is structurally similar to Lance's adaptive choice between a cheap common
representation and a more general fallback. It has already produced material
results: the two-million-message corpus was 11.94% smaller and
`chunk_index.bin` fell from 535,686,024 bytes to 384 bytes. See the
[inline-series design](../superpowers/specs/archive/storage/2026-07-13-storage-schema7-inline-series-design.md), the
[Schema 7 result](../experiments/storage_vnext/2026-07-14-schema7-prefix-results.md),
and the [live performance status](../../improve_new.md).

### Projection-oriented typed scalar lanes

Typed Histogram, ExponentialHistogram, and Summary chunks already use a packed
projection-specific representation. `_count` and `_sum` can read the common
chunk header and scalar lane without reading or decoding native bucket,
boundary, scale, quantile, or exemplar data.

This is the same trade-off described by the paper's struct-packing discussion:
put fields that are commonly consumed together in one access, while preserving
another representation for broader projection. The current scalar lane
deliberately duplicates some timestamps and typed metadata to retain one
contiguous read.

### Separate metadata and statistics

Chronoxide keeps routing summaries, exact postings, FSTs, label-value time
ranges, and metric ranges in `indexes.puffin`, rather than repeating statistics
inside every payload chunk. This agrees with the paper's observation that
inline page statistics can become expensive when pages are small.

### Direct chunk addressability

Chunk locators identify exact payload ranges. Physical coalescing does not turn
gap bytes into selected chunks or corruption authority. This clean separation
between logical selection and physical read planning should be retained by
every experiment below.

## Recommended experiments

### 1. Remove the payload-span lookup confounder

This is a code-only prerequisite, not a format change.

`ChunkPayloadBatch::slice()` currently scans physical spans from the beginning
for every logical locator lookup:

- [current implementation](../../../chronoxide-core/src/storage/chunk/reader.rs)
- [Phase 3 payload-coalescing result](../experiments/storage_vnext/2026-07-21-phase3-payload-coalescing.md)

The lookup is effectively quadratic within a batch when gap zero produces one
physical span per logical request. This confounds the previous coalescing
experiment: fewer spans reduce both I/O submissions and an avoidable in-memory
search cost.

Implement and compare:

- a monotonic cursor for the normal ordered decode path; and
- binary search as a correctness-preserving fallback for out-of-order lookup.

Then rerun the same fixed-gap matrix. Do not revisit adaptive coalescing,
activate a scalar sidecar, or infer an optimal read-amplification point until
this comparator is complete.

### 2. Add a sector/page-aware read planner

This can also be tested without changing stored bytes.

The Lance paper reports that unaligned small reads touched multiple disk
sectors and that 8 KiB pages performed best on its tested Samsung NVMe. Those
results are hardware- and workload-specific, but they provide a credible
hypothesis for Chronoxide.

Add same-binary runtime comparators for:

1. exact selected ranges;
2. the current 4 KiB maximum-gap coalescer;
3. the union of touched 4 KiB-aligned pages;
4. the union of touched 8 KiB-aligned pages; and
5. aligned page unions followed by a separately bounded gap merge.

For a page size `P`, a selected range `[offset, end)` becomes:

```text
page_start = floor(offset / P) * P
page_end   = ceil(end / P) * P
```

Merge duplicate and adjacent page ranges, issue the resulting reads, and expose
only the exact selected subranges to decoders.

Report:

- logical selected requests and bytes;
- process-issued spans and bytes;
- page-union amplification;
- actual block-device requests, sectors, and completion latency where the
  environment permits;
- cold and warm query latency;
- read/decode CPU and span-lookup CPU;
- peak in-flight and process RSS; and
- semantic fingerprints, `QueryStats`, and corruption/error equivalence.

`fincore` residency and process-issued bytes are not block-device measurements.
Use Linux block tracepoints or an equivalent device-level observer on an
otherwise quiet device. Treat buffered I/O and `O_DIRECT` as separate
experiments; do not mix the alignment policy with a backend change.

Do not align individual Chronoxide chunks on disk. The current corpus contains
millions of small chunks, and per-chunk sector padding would overwhelm useful
payload bytes. Align physical read units or packed groups instead.

### 3. Add restartable blocks for dense sample streams

The current segment writer has no implemented `chunk_target_bytes` or
`chunk_target_points` setting and normally emits one complete series/window
chunk:

- [writer configuration](../../../chronoxide-core/src/storage/segment/layout.rs)
- [current record path](../../../chronoxide-core/src/storage/segment/writer/record.rs)

This is efficient for the current sparse corpus, which averages roughly nine
samples per chunk, but a dense series can force a narrow time-range query to
read and decode a much larger one-hour record.

Prototype compressed block targets of 4, 8, and 16 KiB. Each independent block
should contain or be bound to:

```text
BlockDescriptor:
  first_timestamp_ms
  last_timestamp_ms
  point_count
  offset
  encoded_length
  timestamp_codec
  value_codec
  block_crc32c
```

The encoded block must also carry sufficient restart state to decode it without
decoding a predecessor. For Gorilla this includes an initial value and reset
window state. Timestamp encodings similarly require an explicit base and any
step or delta state.

Two possible physical designs should be modeled before implementation:

- multiple ordinary chunks, using the existing overflow locator path; or
- one logical chunk containing a protected mini-block directory and
  independently checksummed blocks.

The first design is simpler and preserves one-stage payload reads after
metadata planning, but makes dense series use overflow metadata. The second can
preserve one logical series locator, but introduces a directory read and a
potential dependent I/O phase. Compare total metadata, number of I/O phases,
range bytes, and corruption surface rather than selecting by intuition.

Keep the current flat representation for sparse or small encoded streams. The
writer should select blocked form only from a deterministic complete encoded
size and access-policy rule; point count alone is not a sufficient proxy for
wide typed values.

Start with Float and Int64. Typed Histogram and ExponentialHistogram blocks add
schema dictionaries, scalar-lane binding, and PromQL projection semantics and
should follow only if scalar or native typed payload work remains material.

### 4. Evaluate block size and timestamp codec together

The accepted timestamp fit screen found:

- fixed-step residual bitpacking would save approximately 3.93% of the complete
  corpus;
- delta-of-delta ZigZag ULEB128 would save approximately 3.82%; and
- the difference between them is only about 0.108% of the complete corpus.

See the [timestamp fit screen](../experiments/storage_vnext/2026-07-23-phase6-timestamp-fit-screen.md).

That narrow capacity difference can easily be reversed by decode cost, SIMD
width, branch behavior, restart overhead, and range selectivity. The runtime
prototype should therefore measure the cross-product:

```text
block target:     4 KiB | 8 KiB | 16 KiB
timestamp codec:  fixed-step residual | delta-of-delta
value codec:      current Gorilla/Int encoding
```

Report full scans and narrow range startup separately. Do not finalize a
timestamp encoding independently from the chosen structural block size.

The formal dense-range gate needs at least 24 hours of dense event-time data.
The current sparse real corpus is valid capacity evidence but cannot establish
the range-startup benefit of independently addressable blocks.

### 5. Add 8 KiB to the packed-frame experiment

Every current chunk carries its own 14-byte single-chunk frame header. The
current four-million-message corpus has a theoretical upper-bound saving of
230.8 MiB, or 4.345% of the complete corpus, if most of those headers can be
amortized. See the
[Phase 7 activation audit](../experiments/storage_vnext/2026-07-21-phase7-format-activation-audit.md).

When packed frames are reactivated, compare at least 8, 16, and 64 KiB physical
targets. Preserve:

- exact per-chunk locators;
- per-chunk header and body integrity;
- an outer frame check for full validation and recovery;
- deterministic series-major construction;
- bounded reads of one selected chunk; and
- oversized single-chunk frames.

Frame size must not force the query reader to fetch the complete frame. The
primary claim should remain capacity and sealing efficiency unless device-level
and end-to-end query measurements establish a latency benefit.

### 6. Model metadata page sizes from real access traces

Schema 8 inherits fixed 16 KiB hot and cold series pages. Before changing them,
capture page/record access traces from the committed query matrix and replay
the traces against hypothetical 4, 8, 16, and 32 KiB page boundaries.

For each page size, calculate:

- pages and bytes touched;
- selected records and logical bytes used;
- read/used amplification;
- descriptor/root bytes;
- final-page padding;
- CRC computations;
- governed retained/in-flight charge; and
- predicted batching opportunities.

Then implement only a materially promising candidate as a versioned A/B.
Metric-contiguous broad queries may prefer larger pages, while sparse random
selectors may prefer smaller pages. The paper's 8 KiB result must not be
treated as a universal default.

## Ideas not justified by the paper

### Do not copy the 128-byte full-zip threshold

Lance's threshold was measured for embeddings, tensors, images, strings, and
general nested values. Chronoxide's dominant access units, projection rules,
and record-size distribution differ. A histogram record being wider than 128
bytes does not imply that row-style full zip is best for `_count`, `_sum`,
bucket, or native projection.

### Do not introduce generic repetition/definition levels

Chronoxide stores known OTLP metric types with explicit semantics. Generic
nested-column shredding would add complexity without evidence that it improves
the selector-to-time-range workload. Existing schemas and typed metadata must
remain explicit and independently decodable.

### Do not activate a typed scalar sidecar yet

The existing scalar lane already avoids native typed payload decode, and
current profiles do not show its remaining I/O or decode work dominating
end-to-end latency. A separate file also adds dependent I/O, locator binding,
checksum, replay, and typed-semantic surface.

Reconsider shared common/count/sum/native columns only after the span-lookup
fix, page-aware planner, and production-safe one-pass range execution leave a
measured scalar bottleneck.

### Do not infer an `io_uring` or `O_DIRECT` rewrite

The current Phase 3 comparisons found similar `pread` and forced-`io_uring`
end-to-end results once requests were coalesced. The paper's syscall and buffer
observations identify profiling questions, not proof of a Chronoxide
bottleneck. Fixed buffers, registered files, direct I/O, and decode/I/O
pipelining require isolated evidence.

### Do not prioritize a new schema over one-pass execution

The existing diagnostic one-pass executor improved warm latency by 68.28% for
`sum(rate())` and 89.37% for `count(rate())` on the admitted dense 30-minute
queries. Its missing allocation governance, finite-limit behavior, public
statistics contract, and dense 24-hour gate are correctness work, not evidence
against the approach.

See the
[Phase 4 result](../experiments/storage_vnext/2026-07-23-phase4-range-one-pass-results.md).
Closing those gates has stronger current end-to-end evidence than introducing
another storage format.

## Recommended sequence

1. Replace linear payload-span lookup with a cursor and binary-search fallback.
2. Repeat the fixed-gap matrix and add 4/8 KiB page-aligned read-plan arms.
3. Finish the production governance and correctness contract for one-pass
   multi-step range execution.
4. Build a dense 24-hour corpus and run the block-size by timestamp-codec
   matrix.
5. Model 4/8/16/32 KiB metadata pages from captured access traces.
6. Run packed frames as an isolated capacity/sealing experiment after the codec
   result.
7. Reprofile the resulting defaults before considering typed sidecars, compact
   routing, or adjacent-segment packing.

## Acceptance requirements

Any code-only query optimization must preserve:

- exact and portable semantic fingerprints;
- result shape, values, and ordering;
- ordinary `QueryStats`, unless a difference is specified before measurement;
- touched-corruption and resource-refusal behavior;
- finite-limit and error precedence; and
- independent readback coverage with no unexplained skip.

Any on-disk experiment additionally requires:

- an update to [storage.md](../superpowers/specs/storage.md) before emitting changed bytes;
- explicit component and segment versions;
- deterministic golden bytes and replay output;
- round-trip and exhaustive corruption coverage;
- footer and same-generation locator binding;
- independent readback and direct PromQL equivalence;
- replay/seal CPU, wall time, RSS, and output-size evidence; and
- cold/warm real-corpus query evidence with logical, process-issued, and
  device-level I/O clearly distinguished.
