# Compact tagged chunk-entry row result

**Status:** promoted as a bounded writer-memory improvement. The sealed
segment writer now retains each empty or one-entry chunk row in a 40-byte safe
tagged value instead of a 56-byte `SmallVec<[ChunkIndexEntry; 1]>`. Rows with
two or more entries promote to `Vec<ChunkIndexEntry>`.

## Decision

Promote the candidate as a memory-layout improvement, not a speedup.

On the accepted 250,000-message replay prefix, the candidate:

- reduced mean unprofiled ingester high-water RSS by 69,808 KiB
  (68.172 MiB, 2.434%);
- reduced mean monitored process-tree peak RSS by 69,425 KiB
  (67.798 MiB, 2.376%);
- put every candidate RSS observation below every control observation;
- reduced Heaptrack's exact sampled requested-live maximum by 70,521,856
  bytes (67.255 MiB, 2.198%);
- shrank the live 4,407,610-row chunk-entry outer allocation from
  246,826,160 bytes to 176,304,400 bytes;
- changed mean instructions by -0.231%, task clock by -0.001%, cycles by
  +0.014%, and wall time by +0.114%;
- changed the largest-window writer-flush mean by -25.5 ms (-0.245%) and
  largest-window elapsed mean by +71.25 ms (+0.346%);
- preserved all 34 storage files and 972,969,365 corpus bytes exactly;
- passed complete footer validation; and
- passed 40/40 independent readbacks with zero skips, isolation skips, or
  mismatches.

Wall, task clock, cycles, largest-window elapsed, and writer-flush effects
changed direction between adjacent pairs. They do not support a speedup or
regression claim. Instructions fell in all four adjacent pairs, but the
candidate is promoted for the stronger memory result: the RSS ranges do not
overlap, and exact peak-stack attribution explains the entire requested-live
reduction within 96 bytes.

## Change under test

The previous production store used one `SmallVec<[ChunkIndexEntry; 1]>` per
series. That correctly kept the common one-chunk case inline, but each outer
row occupied 56 bytes on the measured 64-bit target. The accepted prefix had
4,407,610 live outer rows at the process peak, so the outer array alone
retained 246,826,160 bytes.

The candidate introduces a crate-private safe enum:

```rust
enum InlineOneChunkEntries {
    Empty,
    One(ChunkIndexEntry),
    Many(Vec<ChunkIndexEntry>),
}
```

Rust's existing enum representation packs that value into 40 bytes with
eight-byte alignment on the measured 64-bit target. A focused layout contract
also asserts that `Option<InlineOneChunkEntries>` remains 40 bytes. There is
no `unsafe`, trait-object dispatch, locator side table, or seal-time conversion
to another complete row array.

The existing statically dispatched `SeriesChunkEntries` abstraction continues
to support `Vec`, `SmallVec`, and the production compact enum. The production
row:

- preserves empty, one-entry, and arbitrary multi-entry series;
- exposes the same ordered immutable and mutable slices;
- promotes `Empty -> One -> Many` without changing entry order;
- allocates the two-entry vector before moving the inline entry, so allocation
  failure leaves the old row intact; and
- delegates later growth to the normal `Vec` path.

No persisted type, byte layout, checksum, root, reader API, or query behavior
changes.

## Exact-byte and behavior proof

Focused tests exercise the representation independently and through the
writers:

- backend conformance covers empty rows, mutable inline access, promotion,
  a third push after promotion, row order, and `into_rows` round trips;
- the 64-bit layout test fixes `ChunkIndexEntry`, the compact row, and its
  `Option` at 40 bytes and eight-byte alignment;
- the Schema 6 differential test builds the actual production compact store
  and compares exact chunk-index bytes and positional ranges against nested
  vectors for empty, one-entry, multi-entry, out-of-order, and scalar-lane
  rows;
- the Schema 7 differential test compares exact `series.bin`,
  `chunk_index.bin`, roots, and assembly statistics for mixed inline and
  overflow rows; and
- the full Schema 7/8 writer test promotes a row, reorders the segment, checks
  the footer, reads every sample back, and queries the result.

The real Schema 8 replay strengthens that proof. The profiled candidate and
all eight formal A/B arms reproduced the same complete 34-file SHA-256
manifest.

## Allocation-profile evidence

The profiling control is the retained exact compact-writer-row candidate binary
and trace from the immediately preceding experiment. The new candidate uses
the same system allocator, Schema 8 configuration, capture prefix, and
deterministic segment seed.

Both exact peak flame-stack exports sum byte-for-byte to their corresponding
Massif sampled maxima.

| Exact live allocation at sampled peak | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Chunk-entry outer array | 246,826,160 B | 176,304,400 B | -70,521,760 B |
| Element capacity | 4,407,610 x 56 B | 4,407,610 x 40 B | -16 B/series |
| All other live allocations | 2,962,036,920 B | 2,962,036,824 B | -96 B |
| Whole-process requested-live peak | 3,208,863,080 B | 3,138,341,224 B | -70,521,856 B |

The control peak occurred at 77.650 seconds and the candidate peak at 77.740
seconds. Subtracting only the target outer array leaves a 96-byte favorable
difference across all other live allocations. Thus the exact structural
70,521,760-byte shrink explains the whole 70,521,856-byte peak reduction
within 96 bytes.

Whole-process Heaptrack statistics were:

| Measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Allocation calls | 240,150,875 | 240,150,883 | +8 |
| Temporary allocations | 40,534,750 | 40,534,836 | +86 |
| Leaked bytes | 414,748 | 414,748 | 0 |
| GNU-time maximum RSS | 2,879,576 KiB | 2,810,428 KiB | -69,148 KiB (-67.527 MiB, -2.401%) |
| Heaptrack runtime | 91.98 s | 92.01 s | +0.03 s |

Heaptrack runtime is diagnostic only and is not substituted for the unprofiled
gate.

## Formal eight-run ABBA evidence

The schedule used two `control, candidate, candidate, control` blocks. Every
arm used all 32 logical CPUs, the same frozen binary per side, the same
configuration except output path, a freshly advised-away capture cache,
writeback-quiescence gates, and identical replay-counter and byte-manifest
gates.

| Run | Side | Wall | Ingester RSS | Process-tree RSS | Writer flush |
| --- | --- | ---: | ---: | ---: | ---: |
| 01 | Control | 41.86 s | 2,868,440 KiB | 2,919,572 KiB | 10,480 ms |
| 02 | Candidate | 42.06 s | 2,801,740 KiB | 2,851,084 KiB | 10,471 ms |
| 03 | Candidate | 41.43 s | 2,797,680 KiB | 2,849,984 KiB | 10,337 ms |
| 04 | Control | 41.59 s | 2,868,064 KiB | 2,919,548 KiB | 10,439 ms |
| 05 | Control | 41.47 s | 2,869,228 KiB | 2,929,724 KiB | 10,427 ms |
| 06 | Candidate | 41.50 s | 2,797,188 KiB | 2,860,172 KiB | 10,396 ms |
| 07 | Candidate | 41.48 s | 2,798,124 KiB | 2,849,056 KiB | 10,384 ms |
| 08 | Control | 41.36 s | 2,868,232 KiB | 2,919,152 KiB | 10,344 ms |

Arithmetic means of four observations per binary are:

| Measure | Control mean | Candidate mean | Change |
| --- | ---: | ---: | ---: |
| Wall time | 41.5700 s | 41.6175 s | +0.0475 s (+0.114%) |
| Task clock | 41,579.040 ms | 41,578.688 ms | -0.353 ms (-0.001%) |
| Cycles | 231,677,918,505 | 231,710,602,829 | +0.014% |
| Instructions | 669,398,054,099 | 667,852,309,088 | -0.231% |
| Branches | 124,141,461,762 | 124,196,398,370 | +0.044% |
| Branch misses | 481,708,757 | 478,538,331 | -0.658% |
| Cache references | 7,958,695,939 | 7,910,200,608 | -0.609% |
| Cache misses | 1,194,087,610 | 1,195,681,683 | +0.133% |
| Ingester high-water RSS | 2,868,491 KiB | 2,798,683 KiB | -69,808 KiB (-68.172 MiB, -2.434%) |
| Process-tree peak RSS | 2,921,999 KiB | 2,852,574 KiB | -69,425 KiB (-67.798 MiB, -2.376%) |
| Largest-window elapsed | 20,610.0 ms | 20,681.25 ms | +71.25 ms (+0.346%) |
| Largest-window writer flush | 10,422.5 ms | 10,397.0 ms | -25.5 ms (-0.245%) |

Ingester RSS ranges do not overlap: controls were
2,868,064..2,869,228 KiB and candidates were
2,797,188..2,801,740 KiB. Even the smallest control exceeds the largest
candidate by 66,324 KiB (64.770 MiB).

Process-tree ranges also do not overlap: controls were
2,919,152..2,929,724 KiB and candidates were
2,849,056..2,860,172 KiB. The minimum separation was 58,980 KiB
(57.598 MiB).

Instructions fell in every adjacent pair by 0.174% to 0.283%. Wall, task
clock, and writer flush changed direction between pairs, so their aggregate
means are recorded without interpreting them as a broad runtime change.

Raw tables remain in:

- `metadata/abba-8run-summary.tsv`;
- `metadata/abba-8run-means.tsv`; and
- `metadata/abba-adjacent-pairs.tsv`.

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
  `410ce3929eda619f82ce06f7c4eae7fa7d9eb3b0`
- Frozen measured source-only patch SHA-256:
  `e291890359d90580a2c6425bba1730a4deffb510fd4a0fb0491ca2f2e04efbc7`
- Control ingester SHA-256:
  `9f2be83161cc801b137002e08abf9c318cc2c6c9efb82bf9241765b4accfa994`
- Candidate ingester SHA-256:
  `86bfb10871beece66b696df5e5cf4c6c4de322c0c18be14299799d07996d4ef9`
- Candidate query SHA-256:
  `38c57af02a4d67b0f66579812e37853209bf120790981a5084853d5339585dc0`
- Control Heaptrack trace SHA-256:
  `a28b1088595765b7002a09ba2347749ebf2416a1b65ab9cec4952f4316095133`
- Candidate Heaptrack trace SHA-256:
  `3d00a898c8aa283a57b78e6282a20ca6ad223ed32e58b88dbd0c024c326b3bdc`
- Capture-file SHA-256:
  `1ecebab16fc68b984949810f32c2778857940530336554872d775215fdd28dc4`
- Workload: exact accepted 250,000-message capture prefix
- Storage schema: Schema 8
- Writer seed: 42
- Allocator: Rust system allocator
- CPU set: all 32 logical CPUs
- Toolchain: Rust/Cargo 1.97.0

The candidate evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/compact-chunk-row-memory-20260724TXXXXXXXX-z9knse`

The reused profiling-control evidence is retained in the immediately preceding
compact writer-row result root:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/compact-writer-series-entry-memory-20260724T073241-WiQOyP`

## Artifact cleanup

Immediately before deletion, all 306 generated storage files were rehashed
against their retained per-run manifests. All 306 matched.

Cleanup removed only:

- 9 regenerated segment trees containing 306 files and 8,756,724,285 logical
  bytes;
- 1 redundant query binary containing 501,931,656 bytes; and
- 1 reproducible temporary peak-stack export containing 381,568,595 bytes.

The cleanup reclaimed 307 files and 9,258,655,941 logical bytes (8.623 GiB)
from the experiment filesystem, plus the temporary stack export. Across both
filesystems it reclaimed 308 files and 9,640,224,536 logical bytes
(8.978 GiB).

Both frozen ingester binaries, measured patches, hashes, Heaptrack trace and
Massif export, perf/RSS data, logs, manifests, reports, and cleanup records
remain. No capture, accepted corpus, unrelated experiment, or user-owned
artifact was removed.

## Verification

The exact measured candidate source passed:

- focused compact-row layout, backend-conformance, Schema 6 exact-byte, Schema
  7 exact-byte, and Schema 7/8 reorder/readback tests;
- all storage chunk, series-v3 writer, and segment writer/flush test groups;
- `cargo test -p chronoxide-core --all-targets --all-features`;
- strict all-feature `chronoxide-core` library Clippy;
- `cargo fmt --all -- --check`;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed workspace-wide all-feature Clippy gates for libraries,
  binaries, tests, and benches with warnings denied;
- complete segment-footer validation;
- 40/40 independent readback verification; and
- `git diff --check`.

Because persisted bytes, versions, public semantics, and reader behavior are
unchanged, this candidate requires no storage-version or `storage.md` update.
