# Compact writer-only series-entry result

**Status:** promoted as a bounded writer-memory improvement. The sealed
segment writer now retains a 40-byte writer-only series row instead of the
56-byte public/read-side `SeriesEntry`. Schema 6 keeps chunk-index ranges in a
parallel vector; Schema 7/8 no longer retain that unused field.

## Decision

Promote the candidate as a memory-layout improvement, not a speedup.

On the accepted 250,000-message replay prefix, the candidate:

- reduced mean unprofiled ingester high-water RSS by 69,037 KiB
  (67.419 MiB, 2.350%);
- reduced mean monitored process-tree peak RSS by 69,161 KiB
  (67.540 MiB, 2.314%);
- put every candidate RSS observation below every control observation;
- reduced Heaptrack's exact sampled requested-live maximum by 70,521,688
  bytes (67.255 MiB, 2.150%);
- shrank the live largest-window series-entry allocation from 246,826,160
  bytes to 176,304,400 bytes;
- changed mean instructions by +0.033%, task clock by +0.093%, cycles by
  +0.124%, and wall time by +0.205%;
- changed the largest-window writer-flush mean by +39.5 ms (+0.382%) and
  largest-window elapsed mean by +101 ms (+0.493%), with overlapping run
  ranges for both;
- preserved all 34 storage files and 972,969,365 corpus bytes exactly;
- passed complete footer validation; and
- passed 40/40 independent readbacks with zero skips, isolation skips, or
  mismatches.

The CPU and elapsed movements are neutral at this schedule's dispersion and
do not support a speedup claim. The memory result is stronger: both RSS
measures have non-overlapping ranges, both counterbalanced blocks reproduce
the reduction, and exact peak-stack attribution explains all but 72 bytes of
the Heaptrack peak delta.

## Change under test

The public/read-side `SeriesEntry` contains `series_id`, `kind_mask`, labels,
and a Schema 6 `ChunkIndexRange`. On 64-bit targets it occupies 56 bytes.
Schema 7 and Schema 8 encode chunk locations independently and intentionally
ignore the embedded Schema 6 range, yet the writer retained that 16-byte field
for every series through segment sealing.

The candidate introduces a private writer-only row containing only
`series_id`, `kind_mask`, and labels. Its asserted 64-bit layout is 40 bytes
with eight-byte alignment.

A crate-private `SeriesEntryView` abstraction lets cold-series planning,
Schema 7 assembly, label-value FST and metric-range construction, metric-query
ordering, and authenticated v8/v9 index sealing consume either row
representation. These paths remain statically monomorphized; the change
introduces no trait-object dispatch.

Schema handling remains explicit:

- Schema 7/8 carry only the compact writer row.
- Schema 6 computes chunk-index ranges after the final joint series/chunk
  permutation, retains them in a positional parallel vector, and supplies them
  to the canonical Schema 6 encoder.
- The public reader representation and APIs remain unchanged.
- No conversion to a complete public `Vec<SeriesEntry>` occurs at seal time,
  so the saved allocation is not replaced by a later overlapping copy.

## Exact-byte and failure proof

Focused differential tests establish that the abstraction does not change
encoded output:

- Schema 6 compact rows plus positional chunk ranges produce exactly the same
  `series.bin` bytes as public rows with embedded ranges, including empty
  labels, multiple keysets, and maximum-width integer fields.
- Schema 6 range-count mismatches and compact rows with unsorted or duplicate
  label keys fail before writing output.
- The Schema 6 integration path reorders series whose chunk counts differ,
  compares every embedded range with its exact chunk-index directory pair,
  decodes every payload, and queries the reordered multi-chunk series.
- Schema 7 compact and public rows produce identical `series.bin`,
  `chunk_index.bin`, roots, and assembly statistics for mixed inline,
  multi-chunk overflow, out-of-order, and mixed-kind rows.
- Schema 7 noncanonical compact labels fail before either output is written.
- Authenticated v8 and v9 index sealing produces identical bytes from compact
  and public rows.
- Malformed compact authenticated inventories fail closed before writing v8
  or v9 output.

The real Schema 8 replay strengthens the unit-level proof: the profiled
candidate and all eight formal A/B arms reproduced the same complete 34-file
SHA-256 manifest. Consequently, the change affects no persisted byte, root,
offset, checksum, version, reader behavior, or query result.

## Allocation-profile evidence

The profiling control is the retained exact direct-routing candidate binary
and trace from the immediately preceding experiment. The new candidate uses
the same Rust system allocator, Schema 8 configuration, accepted capture
prefix, and deterministic segment seed.

Both exact peak flame-stack exports sum byte-for-byte to their corresponding
Massif sampled maxima.

| Exact live allocation at sampled peak | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Writer series-entry outer vector | 246,826,160 B | 176,304,400 B | -70,521,760 B |
| Element capacity | 4,407,610 x 56 B | 4,407,610 x 40 B | -16 B/series |
| All other live allocations | 3,032,558,608 B | 3,032,558,680 B | +72 B |
| Whole-process requested-live peak | 3,279,384,768 B | 3,208,863,080 B | -70,521,688 B |

The control peak occurred at 78.062 seconds and the candidate peak at 77.650
seconds. Subtracting only the targeted series vector leaves a 72-byte
difference across all other live allocations. Thus the exact
70,521,760-byte vector shrink explains 70,521,688 bytes of the observed total
peak reduction; the residual is negligible and explicitly recorded.

Whole-process Heaptrack statistics were:

| Measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Allocation calls | 240,150,870 | 240,150,875 | +5 |
| Temporary allocations | 40,534,767 | 40,534,750 | -17 |
| Leaked bytes | 414,748 | 414,748 | 0 |
| GNU-time maximum RSS | 2,951,592 KiB | 2,879,576 KiB | -72,016 KiB (-70.328 MiB, -2.440%) |
| Heaptrack runtime | 92.09 s | 91.98 s | -0.11 s |

Heaptrack runtime is diagnostic only and is not substituted for the unprofiled
gate.

## Formal eight-run ABBA evidence

The schedule used two `control, candidate, candidate, control` blocks. Every
arm used all CPUs, the same frozen binary per side, the same configuration
except output path, a freshly advised-away capture cache, writeback-quiescence
gates, and identical replay-counter and byte-manifest gates.

| Run | Side | Wall | Ingester RSS | Process-tree RSS | Writer flush |
| --- | --- | ---: | ---: | ---: | ---: |
| 01 | Control | 41.43 s | 2,937,212 KiB | 2,988,432 KiB | 10,392 ms |
| 02 | Candidate | 41.38 s | 2,866,812 KiB | 2,918,792 KiB | 10,373 ms |
| 03 | Candidate | 41.58 s | 2,868,272 KiB | 2,918,688 KiB | 10,394 ms |
| 04 | Control | 41.45 s | 2,937,928 KiB | 2,988,720 KiB | 10,386 ms |
| 05 | Control | 41.16 s | 2,936,404 KiB | 2,987,668 KiB | 10,289 ms |
| 06 | Candidate | 41.44 s | 2,869,556 KiB | 2,920,152 KiB | 10,343 ms |
| 07 | Candidate | 41.70 s | 2,868,092 KiB | 2,919,192 KiB | 10,436 ms |
| 08 | Control | 41.72 s | 2,937,336 KiB | 2,988,648 KiB | 10,321 ms |

Arithmetic means of four observations per binary are:

| Measure | Control mean | Candidate mean | Change |
| --- | ---: | ---: | ---: |
| Wall time | 41.440 s | 41.525 s | +0.085 s (+0.205%) |
| Task clock | 41,420.520 ms | 41,458.985 ms | +38.465 ms (+0.093%) |
| Cycles | 230,114,999,077 | 230,400,499,322 | +0.124% |
| Instructions | 669,022,390,661 | 669,244,922,769 | +0.033% |
| Branches | 124,337,688,584 | 124,209,130,713 | -0.103% |
| Branch misses | 479,648,424 | 476,538,378 | -0.648% |
| Cache references | 7,915,077,983 | 7,862,477,118 | -0.665% |
| Cache misses | 1,191,464,176 | 1,183,263,462 | -0.688% |
| Ingester high-water RSS | 2,937,220 KiB | 2,868,183 KiB | -69,037 KiB (-67.419 MiB, -2.350%) |
| Process-tree peak RSS | 2,988,367 KiB | 2,919,206 KiB | -69,161 KiB (-67.540 MiB, -2.314%) |
| Largest-window elapsed | 20,472.5 ms | 20,573.5 ms | +101.0 ms (+0.493%) |
| Largest-window writer flush | 10,347.0 ms | 10,386.5 ms | +39.5 ms (+0.382%) |

Ingester RSS ranges do not overlap: controls were
2,936,404..2,937,928 KiB and candidates were
2,866,812..2,869,556 KiB. Even the smallest control exceeds the largest
candidate by 66,848 KiB (65.281 MiB). Counterbalanced block effects were
-70,028 KiB and -68,046 KiB.

Process-tree ranges also do not overlap: controls were
2,987,668..2,988,720 KiB and candidates were
2,918,688..2,920,152 KiB. The minimum separation was 67,516 KiB
(65.934 MiB).

Writer-flush and largest-window elapsed ranges overlap. The writer-flush block
effects also changed direction, from -0.053% to +0.820%. Their positive
aggregate means are recorded, but they do not establish a reproducible local
regression.

Raw tables remain in:

- `metadata/abba-8run-summary.tsv`;
- `metadata/abba-8run-means.tsv`; and
- `metadata/abba-8run-process-tree-rss.tsv`.

## Correctness evidence

The profiled candidate and every formal arm reproduced:

- 250,000 messages;
- 9,659,074 observed datapoints;
- 9,655,365 time-policy-accepted datapoints;
- 9,634,809 recorded samples;
- every event-time and storage acceptance/rejection counter;
- 4 deterministic segments;
- 34 files and 972,969,365 bytes;
- replay-correctness SHA-256
  `2917c9d0957df96f21cb006382357fa5f97cd15a636bc4230f8a0b96990ff388`;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
  and
- every individual segment-file SHA-256 byte-for-byte.

The profiled corpus separately passed complete footer validation across all
four segments and 40/40 independent readback-oracle cases with zero
mismatches, ordinary skips, isolation skips, or multi-step skips.

## Measurement contract

- Base source:
  `b722e1af40100a9899d65cf7f69d70dceb486228`
- Frozen measured source-only patch SHA-256:
  `912ca13bc05f882f2d6dbe8667766185823a2f87bf65ef43f51ee568ace04c1f`
- Control ingester SHA-256:
  `2213193a75a81cf3e4d380aaa97ceebfc082e11c3aaf8f723f79f6bed7e42b53`
- Candidate ingester SHA-256:
  `9f2be83161cc801b137002e08abf9c318cc2c6c9efb82bf9241765b4accfa994`
- Candidate query SHA-256:
  `baf5cb2acc916cccdec00dd9f5b8b4980eb77a1825e388b90a8369c8ad3899d9`
- Control Heaptrack trace SHA-256:
  `713191d9ff95a1f96ba4a76614c956d831f362cf20afd220ff3c59c4bfd65a8e`
- Candidate Heaptrack trace SHA-256:
  `a28b1088595765b7002a09ba2347749ebf2416a1b65ab9cec4952f4316095133`
- Capture-file SHA-256:
  `1ecebab16fc68b984949810f32c2778857940530336554872d775215fdd28dc4`
- Workload: exact accepted 250,000-message capture prefix
- Storage schema: Schema 8
- Writer seed: 42
- Allocator: Rust system allocator
- CPU set: all 32 logical CPUs
- Toolchain: Rust/Cargo 1.97.0

The candidate evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/compact-writer-series-entry-memory-20260724T073241-WiQOyP`

The reused profiling-control root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/routing-direct-encode-memory-20260724T064026-hTADrG`

## Artifact cleanup

Immediately before deletion, all 306 generated storage files were rehashed
against their retained per-run manifests. All 306 matched.

Cleanup removed only:

- 9 regenerated segment trees containing 306 files and 8,756,724,285 logical
  bytes; and
- 1 redundant query binary containing 502,113,504 bytes.

The cleanup reclaimed 307 files and 9,258,837,789 logical bytes in total
(8.623 GiB). Both frozen ingester binaries, measured patches, hashes,
Heaptrack trace and Massif export, perf/RSS data, logs, manifests, reports,
and cleanup records remain. No capture, accepted corpus, unrelated
experiment, or user-owned artifact was removed.

## Verification

The exact measured candidate source passed:

- focused Schema 6 compact-row byte-equivalence, reordered unequal-chunk, and
  fail-before-write tests;
- focused Schema 7 mixed inline/overflow/OOO byte-equivalence and
  fail-before-write tests;
- focused authenticated v8/v9 byte-equivalence and malformed-input tests;
- strict all-feature `chronoxide-core` library and test Clippy;
- `cargo fmt --all -- --check`;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed workspace-wide all-feature Clippy gates for libraries,
  binaries, tests, and benches with warnings denied;
- complete segment-footer validation;
- 40/40 independent readback verification; and
- `git diff --check`.

Because persisted bytes, versions, public semantics, and reader behavior are
unchanged, this candidate requires no storage-version or `storage.md` update.
