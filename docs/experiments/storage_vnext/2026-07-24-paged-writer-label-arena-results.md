# Paged writer-label arena result

**Status:** promote as a bounded writer-memory improvement. The segment writer
now stores compact series rows and their canonical label pairs in a 64 KiB
paged arena instead of retaining one independently allocated label vector per
series.

## Decision

Promote the paged candidate as a memory improvement, not as a speedup.

On the accepted 250,000-message replay prefix, the final candidate:

- reduced mean unprofiled ingester high-water RSS by 111,532 KiB
  (108.918 MiB, 4.105%);
- reduced mean monitored process-tree peak RSS by 109,364 KiB
  (106.801 MiB, 3.949%);
- put both candidate RSS observations below both control observations;
- reduced Heaptrack's event-exact requested-live peak by 69,197,304 bytes
  (65.992 MiB, 2.265%);
- reduced whole-process allocation calls by 4,439,342 (1.848%);
- explained the event-exact peak change within 280 bytes from the exact row
  and label-page allocations;
- preserved all 34 storage files and 972,969,365 corpus bytes exactly;
- passed complete footer validation; and
- passed 40/40 independent readbacks with zero skips or mismatches.

The noisy-host A/B recorded instructions +1.685%, task clock -2.112%, wall
-2.102%, cache misses -8.379%, and minor faults -5.240%. QEMU continuously
used approximately 1.7 host cores during the schedule. Those counters prove
there was no gross runtime regression, but they do not support a fine-grained
speedup claim. The memory decision does not depend on CPU quietness.

## Change under test

The control retained 4,407,610 writer rows. Each row used the read-side
compatible 40-byte writer shape and owned a separate `Vec<(u32, u32)>` for
its canonical label IDs. The label payload itself was 88,864,686 pairs, or
710,917,488 logical bytes.

The candidate introduces a writer-only store:

- each `WriterSeriesRow` is 24 bytes and contains the series ID, packed
  page/offset location, label count, kind mask, and metadata state;
- label pairs are appended directly into 64 KiB pages;
- one logical row never crosses a page boundary;
- an oversized row receives one oversized page;
- zero-label rows require no sentinel;
- readers and seal consumers access the store through immutable
  `SeriesEntryStore` views;
- symbol remapping mutates the pages in place;
- metric-query ordering supplies exact canonical label counts; and
- the flat-interned shutdown path defers label-page construction until sample
  buffers and recording-only lookup state have been consumed and released.

The 64 KiB page size is deliberate. It replaces millions of tiny allocations
without creating one 710.9 MB allocation that glibc services as a separate
mapping. Payload pages are allocated fallibly on demand; the earlier aggregate
reservation now reserves only the page directory and is named accordingly.

No persisted type, component version, byte layout, checksum, root, reader API,
query behavior, or storage semantic changed.

## Why the contiguous variants failed

Three layouts were measured before the final decision.

### Immediate contiguous arena

The first candidate built one contiguous label arena while the decoded head
sample inventory was still live. Its high-water RSS regressed by roughly
260 MiB. The 710.9 MB label payload overlapped the large outer sample vector
and remaining decoded sample allocations.

### Deferred contiguous arena

Deferring metadata construction removed that logical overlap, but peak RSS
still regressed:

| Measure | Control | Deferred contiguous | Change |
| --- | ---: | ---: | ---: |
| Mean ingester HWM | 2,716,604 KiB | 2,933,922 KiB | +217,318 KiB (+212.225 MiB) |

Heaptrack showed why. The exact 710,917,488-byte arena became one large glibc
mapping. Freed decoded-sample allocations remained resident in the ordinary
heap, while the control's millions of small label allocations could reuse
that heap. The contiguous candidate had excellent post-flush release because
`munmap` returned the large mapping immediately, but its peak was worse.

### Deferred paged arena

Paging preserves the correct deferred lifetime and lets the allocator reuse
the freed ordinary heap. It also avoids the control's per-series allocation
metadata and replaces the 40-byte writer row with a 24-byte row. This is the
only tested layout that improved the governing high-water mark.

The result is an allocator-topology lesson: equal logical live bytes do not
imply equal RSS. Allocation size class and lifetime overlap both matter.

## Allocation-profile evidence

The profiling control is the exact current baseline binary from the promoted
active-segment lifetime experiment. The final candidate used the same system
allocator, Schema 8 configuration, capture prefix, and deterministic segment
seed.

Event-exact peak-stack exports give:

| Writer allocation at the process peak | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Series-row outer allocation | 176,304,400 B | 105,782,640 B | -70,521,760 B |
| Canonical label storage | 710,917,488 B | 712,241,664 B | +1,324,176 B |
| Target total | 887,221,888 B | 818,024,304 B | -69,197,584 B |
| Whole-process requested-live peak | 3,055,547,634 B | 2,986,350,330 B | -69,197,304 B |

The candidate's 1,324,176-byte label overhead is page-boundary slack. The
observed whole-process reduction is only 280 bytes less favorable than the
exact structural prediction.

Heaptrack attributed the candidate label pages to 10,913 allocations totaling
712,241,664 bytes. The control made one label allocation per non-empty series.
Whole-process statistics were:

| Measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Allocation calls | 240,150,859 | 235,711,517 | -4,439,342 (-1.848%) |
| Temporary allocations | 40,534,753 | 38,767,476 | -1,767,277 (-4.360%) |
| Leaked bytes | 414,748 | 414,748 | 0 |
| GNU-time maximum RSS | 2,728,832 KiB | 2,626,624 KiB | -102,208 KiB (-99.813 MiB) |
| Heaptrack runtime | 92.84 s | 89.47 s | -3.37 s |

Profiler-inclusive RSS and runtime are diagnostic only. The unprofiled
counterbalanced A/B is the RSS promotion gate.

The pages are ordinary heap allocations. Freeing them does not promise an
immediate 712 MB return to the operating system. In the formal runs, the
pre-exit resident plateau was still about 43 MiB lower than the control, but
the contiguous candidate's immediate `munmap` behavior was intentionally
traded for the much lower governing peak.

## Formal four-run ABBA evidence

The schedule was `control, candidate, candidate, control`. Every arm used all
32 logical CPUs, one frozen binary per side, the same configuration except
for output path, an advised-away capture cache, writeback-quiescence gates,
and exact replay-counter and corpus-manifest gates.

| Run | Side | Wall | Ingester HWM | Process-tree RSS | Largest writer flush |
| --- | --- | ---: | ---: | ---: | ---: |
| 01 | Control | 42.09 s | 2,718,060 KiB | 2,769,924 KiB | 10,410 ms |
| 02 | Candidate | 41.67 s | 2,605,992 KiB | 2,663,548 KiB | 9,827 ms |
| 03 | Candidate | 40.76 s | 2,604,592 KiB | 2,656,228 KiB | 9,697 ms |
| 04 | Control | 42.11 s | 2,715,588 KiB | 2,768,580 KiB | 10,402 ms |

Arithmetic means are:

| Measure | Control mean | Candidate mean | Change |
| --- | ---: | ---: | ---: |
| Wall time | 42.100 s | 41.215 s | -0.885 s (-2.102%) |
| Task clock | 42,105.055 ms | 41,215.645 ms | -2.112% |
| Instructions | 667,741,068,202 | 678,990,267,253 | +1.685% |
| Cache misses | 1,199,320,262 | 1,098,834,076 | -8.379% |
| Minor faults | 886,543 | 840,084 | -5.240% |
| Ingester high-water RSS | 2,716,824 KiB | 2,605,292 KiB | -111,532 KiB (-108.918 MiB, -4.105%) |
| Process-tree peak RSS | 2,769,252 KiB | 2,659,888 KiB | -109,364 KiB (-106.801 MiB, -3.949%) |
| Largest-window elapsed | 20,921 ms | 19,731 ms | -5.688% |
| Largest-window writer flush | 10,406 ms | 9,762 ms | -6.189% |

Ingester HWM ranges do not overlap. The smallest control exceeds the largest
candidate by 109,596 KiB (107.027 MiB). Process-tree ranges also do not
overlap; their minimum separation is 105,032 KiB (102.570 MiB).

The instruction increase makes the runtime classification deliberately
conservative despite favorable elapsed, cache, fault, and flush observations.
A quiet-host rerun is needed only if a runtime claim becomes important.

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
four segments. Independent verification executed 40 readback queries with
zero ordinary skips, isolation skips, or mismatches.

Focused coverage includes:

- compact-row size and alignment;
- every small row permutation;
- page growth and row-boundary spill;
- a row larger than the default page;
- invalid page index and offset propagation;
- exact-count mismatch rollback;
- unfinished, invalid, and cross-window deferred batches;
- Schema 6/7/8 byte equivalence across every chunk kind;
- FlatInterned, Naive, and KeySetDictEncoded processor output equivalence,
  including manifests and a PromQL readback; and
- metadata-batch profile accounting.

## Measurement contract

- Base source:
  `2cb36d3a019d1f29c21f4d919574b1b6c7b943cf`
- Control ingester SHA-256:
  `4aaaefe7bdcf1da573d1eb9ccc5cdcd3e107a3abfa4614376624026256a1a202`
- Candidate ingester SHA-256:
  `b85379a471c4ef08283b0e1ccfb28d273e689118dd66a6965207ee6e702cb538`
- Candidate query SHA-256:
  `0e055d62f39f94e5bbb22c51b4f62c03c71fee8683e6b1067073c7fbff371fdd`
- Frozen source-manifest SHA-256:
  `d4e3fd7d204a81da6e53908998044eef7d732a937b23983638844b0ad748a341`
- Frozen tracked-patch SHA-256:
  `106662cded71cc3926f42018697086cfa3a26a8d01e63c6a7f576cf77b6c5ccb`
- Frozen new writer-store patch SHA-256:
  `df37f34bd34ce1d41c164218ce2e7d1abb6d753f5bed5901526d305477e546f7`
- Profiling-control Heaptrack trace SHA-256:
  `d1bbe63a9d811350ddf1da73ce06b0207580f302d80ab4d53a6bbd308ad38abb`
- Candidate Heaptrack trace SHA-256:
  `277de2b6c19b62b28fff6f374be37be288c8f309d113ff8d1b9f389e58f6a0b4`
- Workload: exact accepted 250,000-message capture prefix
- Storage schema: Schema 8
- Writer seed: 42
- Allocator: Rust system allocator
- CPU set: all 32 logical CPUs
- Toolchain: Rust/Cargo 1.97.0
- Host caveat: explicitly accepted QEMU ambient load; memory and gross
  regression decision only

The final evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/flat-writer-paged-arena-final-20260724TXXXXXX-nPb6A3`

The profiling control is retained in:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/active-seal-lifetime-final-20260724T013825Z-SeswCa`

## Artifact cleanup

Before deletion, all 578 generated storage files across the failed contiguous,
deferred-contiguous, provisional paged, and final paged runs were rehashed
against their retained manifests. Every file matched.

Cleanup removed only:

- 17 reproducible segment trees containing 16,540,479,205 logical bytes;
- 9 superseded or redundant frozen binaries; and
- 3 reproducible temporary event-exact peak-stack exports.

The 590 files totaled 22,968,584,848 logical bytes (21.391 GiB). Final control
and candidate ingester binaries, both Heaptrack traces, Massif and summary
exports, exact manifests, source patches and hashes, run logs, perf/RSS data,
reports, and cleanup records remain. No capture, accepted calibration,
unrelated experiment, or user-owned artifact was removed.

## Verification

The exact measured candidate source passed:

- focused writer, flush, ordering, label-encoding, and processor tests;
- `cargo fmt --all -- --check`;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed workspace-wide all-feature Clippy gates for libraries,
  binaries, tests, and benches with warnings denied;
- complete segment-footer validation;
- 40/40 independent readback verification; and
- `git diff --check`.

Because persisted bytes, versions, public semantics, and reader behavior are
unchanged, this candidate requires no storage-version or `storage.md` update.

## Follow-up

Do not retry the single contiguous arena on glibc. The strongest remaining
writer-label memory hypothesis is to intern each canonical `(key, value)` pair
once and store a `u32 PairId` per row occurrence. This corpus has 88,864,686
label occurrences but only 313,963 exact postings keys. The structural payload
would fall from 710,917,488 bytes to approximately 355,458,744 bytes plus a
small pair dictionary.

That follow-up is not free: it adds one dictionary lookup per occurrence and
requires writer consumers to resolve IDs without rematerializing all pairs.
It must be implemented and measured separately, with exact bytes, readbacks,
peak memory, and CPU as independent gates.
