# Active-segment record-state lifetime result

**Status:** promoted as a bounded seal-memory lifetime improvement. The
segment writer now releases recording-only source-series lookup,
metadata-presence, normalized-name-cache, and metadata scratch state before
seal-time indexes and metadata raise the allocation crest.

## Decision

Promote the candidate as a memory-lifetime improvement, not a speedup.

On the accepted 250,000-message replay prefix, the candidate:

- reduced mean unprofiled ingester high-water RSS by 80,389 KiB
  (78.505 MiB, 2.873%);
- reduced mean monitored process-tree peak RSS by 82,637 KiB
  (80.700 MiB, 2.897%);
- put every candidate RSS observation below every control observation;
- reduced Heaptrack's event-exact requested-live peak by 82,793,590 bytes
  (78.958 MiB, 2.638%);
- removed a predicted 82,793,602-byte recording-only allocation family from
  the peak, explaining the event-exact reduction within 12 bytes;
- changed mean instructions by -0.006%, task clock by +0.183%, cycles by
  +0.155%, and wall time by +0.295%;
- changed the largest-window writer-flush mean by -29.75 ms (-0.287%) and
  largest-window elapsed mean by +136.5 ms (+0.665%);
- preserved all 34 storage files and 972,969,365 corpus bytes exactly;
- passed complete footer validation; and
- passed 40/40 independent readbacks with zero skips, isolation skips, or
  mismatches.

Wall, task clock, cycles, and largest-window elapsed changed direction between
adjacent pairs. Instructions were effectively flat, and writer flush improved
in three pairs but regressed slightly in one. The result supports neither a
speedup nor a runtime regression. The memory result is stronger: the RSS
ranges do not overlap, and event-exact peak attribution accounts for the
target allocation family within 12 bytes.

## Change under test

`SegmentWriter::flush` takes the active segment and moves the state needed for
sealing into local variables. The previous destructuring pattern ended with
`..`. Rust retained the unbound, recording-only fields in the partially moved
`ActiveSegment` until `flush` returned.

The candidate exhaustively destructures the active segment and explicitly
drops only:

- the source-series lookup map;
- the per-series metadata-presence vector;
- the normalized label/metric-name cache;
- the metadata hash scratch buffer; and
- the metadata label scratch buffer.

Symbols, writer series rows, chunk-entry rows, chunk payload state, the
temporary segment directory, and the trusted metric-order flag remain live.
The explicit drop occurs after the series/chunk count check but before
`SegmentFlushProfile::total` starts. That preserves the previous internal
flush-timer boundary while the caller's whole-flush timer continues to include
the destructor work on both sides.

The exhaustive pattern is intentional: adding a future `ActiveSegment` field
now causes a compile error until the field is classified as a seal input or
recording-only state.

No persisted type, byte layout, checksum, root, reader API, query behavior, or
public storage semantic changes.

## Lifetime and correctness proof

The released fields are used only by recording:

- `series_map` maps a source series reference to a segment-local reference;
- `metadata_present` prevents repeated metadata construction;
- `NormalizedNameCache` owns reusable normalized `Arc<str>` values while
  metadata is encoded; and
- the two scratch vectors are cleared and reused between metadata records.

Before sealing, normalized strings have been copied into `SegmentSymbols`, and
`WriterSeriesEntry` rows retain only the resulting symbol IDs. Dropping the
cache therefore cannot invalidate a label. The segment-local rows, symbols,
chunks, temporary directory, and ordering state own every input used by the
seal path.

None of the released collection types has a custom or panicking destructor.
The chunks writer is still explicitly flushed and dropped before publication,
and failure paths retain the same temporary-directory and error-propagation
behavior.

Existing Schema 6/7/8 writer tests cover count mismatch, ordering, chunk
rewrite failures, typed chunks, footer publication, deterministic IDs, and
readback. A synthetic drop-sentinel test would duplicate ownership facts
already enforced by the compiler without exercising a production behavior.
The real replay, complete byte manifest, footer scan, and independent
readbacks provide the external proof.

## Allocation-profile evidence

The profiling control is the retained exact compact tagged chunk-row candidate
from the immediately preceding experiment. The new candidate uses the same
system allocator, Schema 8 configuration, capture prefix, and deterministic
segment seed.

The event-exact peak flame-stack exports sum to the following totals:

| Live allocation at event-exact peak | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Recording-only active state | 82,793,602 B | 0 B | -82,793,602 B |
| All other live allocations | 3,055,547,622 B | 3,055,547,634 B | +12 B |
| Whole-process requested-live peak | 3,138,341,224 B | 3,055,547,634 B | -82,793,590 B |

The released family was:

| Recording-only family | Bytes |
| --- | ---: |
| Source-series lookup map | 75,497,488 |
| Metadata-presence vector | 4,407,610 |
| Normalized-name cache maps and strings | 2,879,800 |
| Metadata scratch buffers | 8,704 |
| Total | 82,793,602 |

Subtracting only that family leaves a 12-byte unfavorable difference across
all other live allocations. The structural lifetime change therefore explains
the event-exact reduction essentially byte-for-byte.

Massif's approximately 10 ms snapshots recorded a lower candidate maximum of
3,052,920,236 bytes at 78.563 seconds. The event-exact peak export was
2,627,398 bytes higher because that brief crest fell between snapshots. Both
artifacts are retained; the report uses like-for-like event-exact peak-stack
totals for allocation attribution and records unprofiled RSS separately.

Whole-process Heaptrack statistics were:

| Measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Allocation calls | 240,150,883 | 240,150,859 | -24 |
| Temporary allocations | 40,534,836 | 40,534,753 | -83 |
| Leaked bytes | 414,748 | 414,748 | 0 |
| GNU-time maximum RSS | 2,810,428 KiB | 2,728,832 KiB | -81,596 KiB (-79.684 MiB, -2.903%) |
| Heaptrack runtime | 92.01 s | 92.84 s | +0.83 s |

Heaptrack runtime and profiler-inclusive RSS are diagnostic only and are not
substituted for the unprofiled gate.

## Formal eight-run ABBA evidence

The schedule used two `control, candidate, candidate, control` blocks. Every
arm used all 32 logical CPUs, the same frozen binary per side, the same
configuration except output path, a freshly advised-away capture cache,
writeback-quiescence gates, and identical replay-counter and byte-manifest
gates.

| Run | Side | Wall | Ingester HWM | Process-tree RSS | Writer flush |
| --- | --- | ---: | ---: | ---: | ---: |
| 01 | Control | 41.57 s | 2,796,660 KiB | 2,860,972 KiB | 10,384 ms |
| 02 | Candidate | 41.44 s | 2,717,396 KiB | 2,771,272 KiB | 10,303 ms |
| 03 | Candidate | 41.63 s | 2,717,104 KiB | 2,768,536 KiB | 10,335 ms |
| 04 | Control | 41.49 s | 2,799,084 KiB | 2,850,104 KiB | 10,373 ms |
| 05 | Control | 41.53 s | 2,797,264 KiB | 2,848,348 KiB | 10,333 ms |
| 06 | Candidate | 41.68 s | 2,718,328 KiB | 2,769,784 KiB | 10,347 ms |
| 07 | Candidate | 41.75 s | 2,718,436 KiB | 2,769,324 KiB | 10,318 ms |
| 08 | Control | 41.42 s | 2,799,812 KiB | 2,850,040 KiB | 10,332 ms |

Arithmetic means of four observations per binary are:

| Measure | Control mean | Candidate mean | Change |
| --- | ---: | ---: | ---: |
| Wall time | 41.5025 s | 41.6250 s | +0.1225 s (+0.295%) |
| Task clock | 41,496.750 ms | 41,572.625 ms | +75.875 ms (+0.183%) |
| Cycles | 231,063,366,051 | 231,422,100,589 | +0.155% |
| Instructions | 667,817,652,718 | 667,777,182,051 | -0.006% |
| Branches | 124,218,318,929 | 124,233,722,923 | +0.012% |
| Branch misses | 477,974,746 | 478,298,424 | +0.068% |
| Cache references | 7,887,539,388 | 7,856,026,827 | -0.400% |
| Cache misses | 1,186,652,043 | 1,185,153,879 | -0.126% |
| Ingester high-water RSS | 2,798,205 KiB | 2,717,816 KiB | -80,389 KiB (-78.505 MiB, -2.873%) |
| Process-tree peak RSS | 2,852,366 KiB | 2,769,729 KiB | -82,637 KiB (-80.700 MiB, -2.897%) |
| Largest-window elapsed | 20,520.25 ms | 20,656.75 ms | +136.5 ms (+0.665%) |
| Largest-window writer flush | 10,355.5 ms | 10,325.75 ms | -29.75 ms (-0.287%) |

Ingester HWM ranges do not overlap: controls were
2,796,660..2,799,812 KiB and candidates were
2,717,104..2,718,436 KiB. Even the smallest control exceeds the largest
candidate by 78,224 KiB (76.391 MiB).

Process-tree ranges also do not overlap: controls were
2,848,348..2,860,972 KiB and candidates were
2,768,536..2,771,272 KiB. The minimum separation was 77,076 KiB
(75.270 MiB).

Adjacent wall effects were -0.313%, +0.337%, +0.361%, and +0.797%.
Task-clock effects were -0.296%, -0.021%, +0.182%, and +0.869%. Writer-flush
effects were -0.780%, -0.366%, +0.135%, and -0.136%. Those direction changes
support a neutral runtime classification.

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
  `210c20926cfdc606be14d1f90947e9dab2ec4814`
- Frozen measured source-only patch SHA-256:
  `060c17480b3067e9a7f298916b440d435dccd01421e6adef3251a47a09d2e0d2`
- Control ingester SHA-256:
  `86bfb10871beece66b696df5e5cf4c6c4de322c0c18be14299799d07996d4ef9`
- Candidate ingester SHA-256:
  `4aaaefe7bdcf1da573d1eb9ccc5cdcd3e107a3abfa4614376624026256a1a202`
- Candidate query SHA-256:
  `df9e8d0741bdc62ce0952f684d03eeea0af711be5a899dcfbf968bb4027e351a`
- Control Heaptrack trace SHA-256:
  `3d00a898c8aa283a57b78e6282a20ca6ad223ed32e58b88dbd0c024c326b3bdc`
- Candidate Heaptrack trace SHA-256:
  `d1bbe63a9d811350ddf1da73ce06b0207580f302d80ab4d53a6bbd308ad38abb`
- Capture-file SHA-256:
  `1ecebab16fc68b984949810f32c2778857940530336554872d775215fdd28dc4`
- Workload: exact accepted 250,000-message capture prefix
- Storage schema: Schema 8
- Writer seed: 42
- Allocator: Rust system allocator
- CPU set: all 32 logical CPUs
- Toolchain: Rust/Cargo 1.97.0

The candidate evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/active-seal-lifetime-final-20260724T013825Z-SeswCa`

The reused profiling-control evidence is retained in:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/compact-chunk-row-memory-20260724TXXXXXXXX-z9knse`

## Artifact cleanup

Immediately before deletion, all 306 generated storage files were rehashed
against their retained per-run manifests. All 306 matched.

Cleanup removed only:

- 9 regenerated segment trees containing 306 files and 8,756,724,285 logical
  bytes;
- 1 redundant query binary containing 511,634,344 bytes; and
- 1 reproducible temporary peak-stack export containing 359,977,309 bytes.

That cleanup reclaimed 308 files and 9,628,335,938 logical bytes
(8.967 GiB). The corrected run also superseded the first measurement attempt,
whose drop occurred inside the internal total timer. After the corrected
binary passed every gate, that complete 614-file, 10,626,519,677-byte
(9.897 GiB) root was removed. Total reclaimed space was 20,254,855,615 bytes
(18.864 GiB).

Both final frozen ingester binaries, measured patches, hashes, Heaptrack trace
and Massif export, perf/RSS data, logs, manifests, reports, and cleanup records
remain. No capture, accepted corpus, unrelated experiment, or user-owned
artifact was removed.

## Verification

The exact measured candidate source passed:

- all 30 focused segment writer/flush tests;
- strict all-feature `chronoxide-core` library Clippy;
- `cargo fmt --all -- --check`;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed workspace-wide all-feature Clippy gates for libraries,
  binaries, tests, and benches with warnings denied;
- complete segment-footer validation;
- 40/40 independent readback verification; and
- `git diff --check`.

Because persisted bytes, versions, public semantics, profile boundaries, and
reader behavior are unchanged, this candidate requires no storage-version or
`storage.md` update.
