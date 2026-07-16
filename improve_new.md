# Next performance improvements

## Conclusion

Schema 8 has already captured the clear postings win. The next meaningful
improvements are primarily code-side: repeated OTLP label work, millions of
small head allocations, and owned query-label strings. A future schema should
target a measured read pattern rather than changing indexing generally.

The latency results below were collected on an intentionally noisy shared
machine. Treat CPU profiles, byte counts, semantic fingerprints, and read
amplification as the reliable signals; small latency differences are
directional only.

## Current evidence

| Path | Current evidence | Best opportunity |
| --- | ---: | --- |
| Four-million-message replay | 795.33 s wall, 787.17 s user CPU, 12.19 GiB peak RSS | Prepared labels and compact head storage |
| Replay allocator | 29.50% of sampled CPU | Eliminate millions of tiny head allocations |
| Label construction outside interning | Approximately 279.9 s | Cache resource/metric prefixes and merge datapoint labels |
| Broad PromQL raw output | 4.39 s warm, 2.37 GiB RSS, 8.13 million materialized label pairs | Query-local symbol IDs instead of owned strings |
| Scalar range query | 3.89x process-issued read amplification | Adaptive coalescing, then a scalar sidecar if needed |
| Schema 8 postings | 72.90% fewer postings bytes and 15.60% smaller total corpus | Complete as a capacity optimization; measured latency is neutral |

The Schema 8 corpus is approximately 5.569 GB:

| Artifact | Bytes | Share |
| --- | ---: | ---: |
| `chunks.bin` | 3,578,303,589 | 64.25% |
| `series.bin` | 1,154,153,445 | 20.72% |
| `indexes.puffin` | 754,231,284 | 13.54% |
| `symbols.bin` | 82,618,420 | 1.48% |

## 1. Code-only ingest-path improvements

These optimizations apply to the shared Kafka and capture-replay processing
path. Capture replay provides the deterministic performance and correctness
A/B; it is not a separate optimized implementation.

### 1.1 Prepared resource and metric label plans

This is the best immediate replay CPU experiment.

The four-million-message replay accepted approximately 155.1 million
datapoints but produced only 6.61 million unique label sets. For every
datapoint, the current path still rebuilds resource, metric-name, and datapoint
labels; formats scalar attribute values; sorts the complete label set; hashes
it; and reinterns its symbols.

Prepare the repeated portions instead. This is not a general
"allocation-free repeated lookup" optimization: warmed all-string label sets
already reuse their canonical and encoded scratch buffers. The expected win is
less repeated sorting, scalar formatting, and symbol-table probing within one
OTLP request.

1. Format, canonicalize, and sort resource labels once per `ResourceMetrics`
   input without mutating the label store before a datapoint is accepted.
2. Add the metric name once per metric and cache resource/metric symbol pairs
   after their first successful use in the current label store.
3. Canonicalize only datapoint attributes for each datapoint.
4. Merge the prepared sorted prefix and sorted datapoint attributes while
   preserving the current duplicate-resolution rules.
5. Normalize and hash the final canonical sequence as before, and copy
   permanent label-set state only for a new series.

The equivalence implementation must preserve:

- resource, datapoint, and metric-name precedence;
- last-value-wins behavior within an equal rank;
- label-key and label-value normalization;
- skipped non-scalar accounting;
- deterministic `SeriesRef` assignment;
- collision verification; and
- byte-identical sealed output.

#### Measured result

Implemented as request-local prepared resource/metric label plans in the
shared Kafka and capture-replay processor. The FlatInterned path caches
resource and metric symbol pairs after first use; other stores consume the
same prepared canonical sequence without changing their on-disk semantics.

On the real four-million-message capture, using the same Schema 8 config and
separately preserved release binaries:

| Metric | Baseline | Prepared plans | Change |
| --- | ---: | ---: | ---: |
| Wall time | 821.54 s | 734.48 s | -10.60% |
| User CPU | 810.51 s | 726.75 s | -10.33% |
| System CPU | 9.22 s | 8.27 s | -10.30% |
| Measured ingest processing time | 489.176 s | 406.613 s | -16.88% |
| Label-store interning time | 196.682 s | 138.964 s | -29.35% |
| Processing-minus-interning residual | 292.493 s | 267.649 s | -8.49% |
| Peak RSS | 12,194,900 KiB | 12,313,056 KiB | +0.97% |
| Corpus bytes | 5,569,314,896 | 5,569,314,896 | identical |

The processing residual is not a pure label-construction timer: it includes
head recording, value conversion, and synchronous segment sealing. Aggregate
head-window write time was effectively unchanged (174.641 s versus 175.560 s,
+0.53%), while the total user-CPU reduction closely matched the 82.563-second
reduction in measured ingest processing time.

Correctness gates passed:

- all 66 generated artifact hashes are byte-identical;
- all logical ingestion counters match, including 155,197,127 observed,
  155,073,601 accepted, and 154,902,724 recorded datapoints;
- full footer and exact-postings verification passed;
- the independent readback oracle executed 38 of 38 cases with zero skips and
  zero mismatches; and
- focused prepared-versus-legacy, event-time, live/WAL, capture/direct, and
  warmed-allocation tests passed, followed by the complete workspace suite.

This is a CPU win, not a memory optimization. A one-million-message post-change
profile still attributes the largest self-time to allocator work, final
label-set hashing/equality, symbol interning, and head maps. Prepared-prefix
merge itself is approximately 1.13% self-time. The next replay experiment
should therefore target compact head ownership rather than add more prepared
label state. Eager resource preparation can also regress empty or fully
rejected requests, so those shapes should remain a focused follow-up gate.

### 1.2 Dense head maps and timestamp lookup

Use a deterministic lightweight hasher for maps keyed by `SeriesRef` values.
Its `u32` path must retain enough mixing for partition-local maps, whose global
series references may be sparse or strided; raw integer identity performs
poorly for those shapes. Fuse the repeated `last_timestamps` validation,
out-of-order routing, and accepted-value updates into one entry-state
operation.

This is a small, low-risk A/B before changing head ownership. The head-related
SipHash samples alone account for approximately 2.9% of the replay profile.

#### Measured result

Implemented a shared `SeriesRefHashMap` using the existing cheap deterministic
`u32` FNV fallback, and changed each sample to acquire one timestamp entry for
validation, out-of-order routing, and the accepted maximum-timestamp update.
The timestamp entry is changed only after the sample is accepted. The same map
is also used for per-window series state.

On the real four-million-message capture, comparing separately preserved
release binaries on the same Schema 8 configuration:

| Metric | Prepared-label baseline | Dense head maps | Change |
| --- | ---: | ---: | ---: |
| Wall time | 726.99 s | 689.22 s | -5.20% |
| User CPU | 718.97 s | 680.10 s | -5.41% |
| Task clock | 727,100 ms | 688,279 ms | -5.34% |
| CPU cycles | 4.062 trillion | 3.844 trillion | -5.36% |
| Instructions | 7.087 trillion | 7.005 trillion | -1.15% |
| Instructions per cycle | 1.745 | 1.822 | +4.45% |
| Head-call mean | 599 ns | 417 ns | -30.38% |
| Head-call p50 | 526 ns | 341 ns | -35.17% |
| Head-call p95 | 920 ns | 692 ns | -24.78% |
| Measured ingest processing time | 402.115 s | 373.608 s | -7.09% |
| Peak RSS | 12,245,196 KiB | 12,088,580 KiB | -1.28% |
| Corpus bytes | 5,569,314,896 | 5,569,314,896 | identical |

The full run was one baseline-then-candidate pair on a noisy host, so the RSS
change is not treated as a memory result and the exact wall-time magnitude
needs replication. A 250,000-message ABBA screen independently confirmed the
head-latency direction and produced byte-identical outputs. Effective CPU
frequency was unchanged in the full pair; fewer instructions and 4.45% higher
IPC support the CPU result.

Correctness gates passed:

- all 66 files and 5,569,314,896 bytes matched byte-for-byte;
- every ingestion, event-time, per-type, series, symbol, and head-structure
  counter matched;
- exhaustive footer, series, chunk, and exact-postings verification passed;
- the independent readback oracle executed 38 of 38 cases with zero skips and
  zero mismatches; and
- focused out-of-order, drain, type-mismatch, partial-batch, WAL, head-query,
  processor, and source-level tests passed, followed by the workspace suite.

This is a CPU optimization, not a capacity redesign. The next ingest item is
compact short-series head storage.

### 1.3 Compact short-series head storage

The measured corpus produced 17.29 million per-window series occurrences:

- average: 8.96 samples;
- p99: 30 samples; and
- single-sample series: 16.8%.

Each active series currently owns an `EncodedSeries` and normally a boxed
`BlockBuilder` with small codec buffers. Large windows retain approximately
5.4-5.5 million concurrent series. A slab/arena-backed short-series path can
avoid millions of boxes and small heap buffers, promoting only longer series
to the existing general representation.

#### Measured result

Implemented an inline four-sample staging representation for the default
Gorilla float and Delta-ZigZag integer codecs. It stores exact timestamps and
value bits inside the existing 96-byte `EncodedSeries`. Series with four or
fewer samples avoid the boxed block builder and its timestamp/value buffers;
the fifth sample promotes by replaying the four values through the existing
codec in append order. Other numeric codecs and all typed Histogram,
ExponentialHistogram, and Summary paths are unchanged.

The behavior is controlled by
`ingestion.head_buffer.compact_numeric_series`, which defaults to `true`. The
comparison used one identical release binary and changed only that flag and
the fresh output directory.

On the real four-million-message capture with Schema 8:

| Metric | General head series | Inline short series | Change |
| --- | ---: | ---: | ---: |
| Wall time | 732.99 s | 697.10 s | -4.90% |
| User CPU | 723.63 s | 685.79 s | -5.23% |
| Task clock | 732.700 s | 695.752 s | -5.04% |
| CPU cycles | 4.058 trillion | 3.838 trillion | -5.44% |
| Instructions | 7.006 trillion | 7.004 trillion | -0.03% |
| Instructions per cycle | 1.726 | 1.825 | +5.72% |
| Measured ingest processing time | 395.439 s | 383.294 s | -3.07% |
| Head-call mean | 442 ns | 414 ns | -6.33% |
| Head-call p50 | 350 ns | 321 ns | -8.29% |
| Head-call p95 | 743 ns | 729 ns | -1.88% |
| Peak RSS | 12,087,876 KiB | 11,907,392 KiB | -1.49% |
| Corpus bytes | 5,569,314,896 | 5,569,314,896 | identical |

Instructions are effectively unchanged while cycles fall by 5.44%, raising
IPC by 5.72%. This is consistent with removing allocator and cache stalls
rather than removing equivalent logical work. Individual synchronous seal
times moved in both directions on the noisy host, so the evidence supports a
whole-replay CPU improvement, not a separate deterministic seal-time claim.

The RSS result is modest and not yet independently replicated. The four-million
pair peaked 176 MiB lower, but a one-million-message ABBA screen had effectively
identical peaks and comparable mid-run four-million checkpoints differed by
only about 25 MiB. Treat capacity as directional; CPU is the promotion reason.

The one-million-message ABBA screen independently confirmed the CPU direction:
mean wall time fell from 177.315 to 155.655 seconds (-12.22%) and mean user CPU
fell from 174.11 to 152.28 seconds (-12.54%). That prefix result overstates the
full-capture magnitude, so the four-million result above is the scale estimate.

Correctness gates passed:

- focused equivalence tests cover block sizes 1, 2, 3, 4, 5, and 1024; exact
  float bits including stale NaN and ordinary NaN; integer extrema; the 4-to-5
  promotion boundary; duplicate out-of-order timestamps; rejected samples;
  live-head queries; rotation; and drain;
- all 66 generated files and 5,569,314,896 bytes are byte-identical;
- all logical ingestion counters match, including 155,197,127 observed,
  155,073,601 accepted, and 154,902,724 recorded datapoints;
- exhaustive footer, chunk, series, and exact-postings verification passed for
  17,286,077 chunks and 154,902,724 samples; and
- the independent readback oracle executed 38 of 38 cases with zero skips and
  zero mismatches.

This completes the compact-head experiment. The next ingest capacity item is
paged label-pair storage.

### 1.4 Paged label-pair storage

`FlatInternedLabelSetStore.key_values` has 149,615,407 live entries but a
capacity of 251,658,240. At eight bytes per entry, the unused capacity is about
778.5 MiB. Total label-store accounting reports approximately 892 MB more
allocated than used.

Replace the geometrically growing monolithic vector with stable pages or
chunks. Prevent an individual label-set row from crossing a page, or teach the
visitor/equality path to consume a bounded two-slice representation. Do not
use `reserve_exact` for every insertion because that risks repeated copying
and substantially worse CPU time.

## 2. Query code improvements

### 2.1 Keep query-local symbol IDs through evaluation

The broad-regex raw-output query takes approximately 4.39 seconds warm and
peaks near 2.37 GiB RSS while materializing 8.13 million label pairs. It issues
only about 52 MiB of payload spans, and measured warm read time is only a few
milliseconds. The dominant cost is label ownership, evaluator work, and result
construction rather than storage I/O.

Keep labels as query-local `(name_id, value_id)` pairs through selection,
grouping, matching, and evaluation. Resolve or serialize strings only at the
public API boundary. A lower-risk intermediate experiment can replace repeated
owned strings with query-local `Arc<str>` values.

Raw selectors still require complete output labels. The optimization removes
duplicate ownership; it must not omit observable labels.

### 2.2 Extend demand-driven labels to native histograms

The generic scalar terminal-aggregation path can request only matcher and
grouping labels. Native Histogram and ExponentialHistogram planning still
visits and owns complete labels.

Propagate terminal label demand into native Histogram and
ExponentialHistogram range/aggregation planning. Continue integrity-checking
the complete touched metadata pages while allocating only the labels consumed
by the expression.

Implemented for the proved root scalar-output native shapes: `count` and
`group`, with `All` or `by(...)` grouping, over a direct pure native selector
or native `rate()`/`increase()`. Histogram, ExponentialHistogram, and scalar
branches use the same normalized demand; mixed-kind rows, `without`, native
`sum`/`avg`, nested, raw, binary, ranking, and other uncertain shapes remain
full. Full and demand-driven execution retain identical touched-row integrity
checking and ordinary `QueryStats`; real-corpus latency measurement remains
part of the refreshed profile run below.

The last Schema 7/8 promotion query gate explicitly used
`label_materialization=full`. Production defaults to demand-driven, so rerun
eligible scalar and native terminal aggregations with the production default.
The broad raw-output query remains a useful full-label stress case.

### 2.3 Close query profiling gaps

Add separate durations and cache/governor deltas for:

- series-row integrity checks;
- full versus selective label materialization;
- symbol lookup and resolution;
- matcher verification;
- locator planning;
- payload decoding;
- PromQL grouping and evaluation;
- result construction; and
- metadata-cache hits, misses, evictions, admission refusals, and class
  charges.

Profile these current Schema 8 shapes separately from timed benchmark runs:

- broad raw regex output;
- demand-driven scalar range aggregation;
- native Histogram range aggregation; and
- native ExponentialHistogram range aggregation.

### 2.4 Avoid redundant postings copies when proven relevant

The Schema 8 runtime decodes a posting into a governed `Vec<u32>`, copies it
into a candidate vector, and may allocate another vector for intersection or
union. Regex expansion repeatedly unions growing vectors.

Possible improvements are borrowed posting views, decode-directly-to-final
candidates, and multiway merge/bitset union. Encoded-domain operations should
remain lower priority until a fresh profile demonstrates a postings-bound
query: the current matrix spends milliseconds or less in postings inside
queries that take hundreds of milliseconds or seconds.

## 3. Code and layout co-design

### 3.1 Adaptive payload-read coalescing

The planner currently merges requests separated by gaps of up to 4 KiB. This
produces the following process-issued amplification in representative shapes:

| Query shape | Read/used amplification |
| --- | ---: |
| Broad regex | 5.27x |
| Negative matcher | 4.18x |
| Metric-range control | 3.98x |
| Scalar range | 3.89x |
| Summary | 3.45x |
| Sparse regex | 3.16x |
| Native Histogram | 1.02x |
| Native ExponentialHistogram | 1.14x |

Benchmark gaps such as 0, 256 B, 1 KiB, and 4 KiB while recording both read
count and physical bytes. Then introduce an adaptive policy based on request
size, density, backend, and an explicit maximum-amplification budget.

These counters describe process-issued file spans, not operating-system cache
misses or storage-device traffic.

### 3.2 Typed scalar sidecar and complete Number/Sum metadata

If smaller coalescing gaps merely exchange excess bytes for too many reads,
place scalar count/sum lanes in a dense series- or metric-major
`typed_scalars.bin`. This would make narrow scalar reads independently
addressable without disturbing the already efficient full native reads.

This should be a natural future schema boundary combined with fixing the known
Number Gauge/Sum correctness gap. The new representation must preserve:

- Gauge versus Sum kind;
- Sum temporality and monotonicity;
- start time and datapoint flags;
- reset hints where applicable;
- signed, non-finite optional delta sums;
- independently locatable and checksummed count/sum/native data; and
- binding of every sidecar locator to its authoritative source chunk.

Do not introduce a performance-only schema while continuing to discard these
typed Number/Sum semantics.

### 3.3 Adaptive sample and timestamp codecs

`chunks.bin` occupies 3.578 GB, or 64.25% of the Schema 8 corpus. Evaluate raw
versus Gorilla and suitable block timestamp encodings using deterministic
per-block selection. The format already identifies chunk encodings, but any
new selection policy must be specified and deterministic.

Measure encoded bytes, cycles per sample, branch misses, range-startup cost,
seal CPU, scalar/full decode, and cold/warm end-to-end latency. Historical
microbenchmarks suggest raw values decode much faster for a modest size cost;
that is not sufficient evidence to change the default without a sealed-corpus
A/B.

## 4. Capacity-oriented format experiments

These remain worthwhile, but they do not currently have a demonstrated query
latency case.

### Packed multi-chunk frames

The corpus contains 17,286,077 one-chunk frames. At 14 header bytes per frame,
the exact header cost is 242,005,078 bytes. Approximately 64 KiB packed frames
could remove most of that overhead and may improve sealing or scan throughput.
They must retain direct chunk locators, bounded individual reads, per-chunk
integrity, and an outer frame integrity check.

### Compact routing

Current exact-index routing occupies approximately 244 MB. A deterministic,
checksummed no-false-negative filter or compact exact structure could reduce
capacity and write bytes while retaining authoritative exact directories.
The promotion query matrix issued no routing bytes, so this is not currently a
query-latency optimization.

### Adjacent-segment packing

Packing adjacent immutable segments could amortize repeated symbols, series,
and postings and create more cross-segment I/O work. It has substantially more
manifest, recovery, compaction, and time-pruning complexity and should follow
the lower-risk per-segment work.

## 5. Recommended execution order

1. Add the missing query stage timers and cache/governor counters.
2. Reprofile current Schema 8 using production demand-driven labels.
3. Implement the prepared resource/metric label plan and run a four-million-
   message A/B.
4. Apply the dense `SeriesRef` map/timestamp quick win as an isolated A/B.
5. Prototype compact short-series head storage and paged label-pair storage.
6. Query-local shared label atoms cut high-cardinality peak RSS by 54.9%, but
   still regressed that selector by 25.6% cold and 10.7% warm after redundant
   final re-interning was removed. Keep owned strings as the default. A future
   compact-ID experiment must avoid hashing every materialized UTF-8 pair and
   must add aggregate session memory governance. Native demand-driven
   materialization is implemented for the proved terminal `count`/`group`
   shapes.
7. Benchmark adaptive coalescing.
8. If scalar I/O remains material, design the typed scalar/common-column
   schema together with complete Number/Sum metadata.
9. Evaluate packed frames, compact routing, and adaptive codecs as separate
   capacity experiments.

Do not prioritize another postings codec, sealing micro-optimizations, or an
io_uring redesign before the planner exposes enough useful concurrent work.
Sealing is now approximately 8% of replay time, and Schema 8 postings latency
is already neutral.

## 6. Verification gates

Every code-only replay optimization must preserve:

- all four-million-message ingest counters;
- deterministic segment IDs;
- byte-identical segment artifacts where the format is unchanged;
- independent readback fingerprints; and
- footer validation.

Every query optimization must preserve semantic and portable fingerprints.
Ordinary `QueryStats` must match unless an intended counter change, such as
fewer materialized labels, is explicitly named and reviewed.

Every on-disk change additionally requires deterministic byte/round-trip
tests, corruption and error-propagation tests, replay/readback equivalence, and
real-corpus performance evidence.

Durable publish work must not be omitted to improve benchmark numbers. Once
crash-safe file and directory synchronization is implemented, report its seal
and replay cost separately from the pre-sync baseline.

## Evidence

- Four-million-message allocation-free label interning A/B:
  `/run/media/user/8c0c2e73-2c76-4cfb-bc59-36559b9bfb10/data/chronoxide/storage-schema7-perf-4m-label-intern-20260714-234047/replay-ab-analysis.md`
- Schema 8 promotion query summary:
  `/run/media/user/8c0c2e73-2c76-4cfb-bc59-36559b9bfb10/data/chronoxide/schema8-promotion-4m-20260715-164941/query-ab-final-20260715-103547/summary.tsv`
- Schema 8 adaptive-postings results:
  `docs/experiments/storage_vnext/2026-07-15-schema8-adaptive-postings-results.md`
- Normative storage contract:
  `docs/superpowers/specs/storage.md`
