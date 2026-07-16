# Paged label-pair storage replay result

- **Date:** 2026-07-16
- **Status:** Correctness passed. In the authoritative guarded three-million-
  message C-P-P-C block, paged was directionally 0.27% slower by task-clock
  and 0.28% slower by wall time. One block cannot resolve a sub-percent
  regression. Paging reduced estimated store allocation by 39.91% but did not
  reduce peak RSS. Keep contiguous storage as the default and retain paging
  only as an explicitly named experimental comparator.
- **Authoritative guarded output:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/paged-labels-guarded-20260716-130406`
- **Earlier two-million-message output:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/paged-labels-ab-20260716-101357`
- **Earlier three-million-message output:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/paged-labels-threshold-20260716-103024`

## Decision

The normal `flat_interned` store remains contiguous. The bounded-page layout is
available only through `experimental_flat_interned_paged`. It is an internal
performance comparator, not a compatibility promise or a second preferred
production backend.

Retaining the comparator has one demonstrated benefit: it prevents a large
geometric capacity jump in the contiguous label-pair vector. It has no
demonstrated resident-memory or throughput benefit. Remove it before promoting
storage vNext to main unless a planned real-corpus or allocator experiment
shows a material RSS, allocation-reliability, or throughput advantage while
preserving exact output bytes.

## Candidate layout

The candidate stores interned label pairs in fixed 65,536-entry pages. A row
never crosses a page. Its eight-byte `SeriesLoc` packs a 16-bit page index and
16-bit page offset into the existing `u32` offset and retains the existing
`u32` row length. The contiguous comparator uses the same eight-byte locator;
the interning, hashing, collision, and series-assignment algorithms are shared.

At three million messages the contiguous vector crossed a growth boundary:
its capacity doubled to 251,658,240 label pairs while only 136,720,575 were
used. Paging held capacity to 136,773,632 pairs and reduced estimated allocated
store bytes by 918,978,560 bytes (876.4 MiB). The unused contiguous capacity
was predominantly reserved rather than resident.

This is an ingest-memory layout only. It does not change segment schemas,
writers, readers, WAL semantics, or PromQL behavior.

## Authoritative guarded three-million-message ABBA

The accepted schedule was contiguous, paged, paged, contiguous. Every run used
one frozen release binary with SHA-256
`489ce5a059a9d4d6834b3409b4e2fdb766069ab8894a5c2f47ba28bfc1b9da54`,
Git head `e5d642d1b282d88db29b661d27c4d1b0166cd5e8`, Schema 8, deterministic
segment seed 42, and the same three-million-message prefix of the
20,589,025,986-byte production capture.

Before every accepted observation, the harness issued
`POSIX_FADV_DONTNEED` and required `fincore` to report zero resident capture
pages and bytes. A ten-sample quiet gate rejected active compiler/build
processes and sustained host or I/O pressure. Accepted runs averaged about
94.95% host idle, detected no build process, and had empty post-run disruptive-
process scans. The two contiguous controls contained isolated one-second noise
threshold excursions, but none met the declared two-consecutive-sample
rejection rule. Neither paged observation had an excursion.

Balanced arithmetic means follow. Deltas are paged minus contiguous.

| Measure | Contiguous mean | Paged mean | Delta |
| --- | ---: | ---: | ---: |
| Wall time | 485.105 s | 486.480 s | +0.283% |
| Task-clock | 485.291245 s | 486.601160 s | +0.270% |
| User CPU | 479.185 s | 480.685 s | +0.313% |
| CPU cycles | 2.711327 T | 2.717898 T | +0.242% |
| Instructions | 5.150127 T | 5.151378 T | +0.024% |
| Branches | 906.434618 B | 906.689624 B | +0.028% |
| Branch misses | 4.757819 B | 4.745745 B | -0.254% |
| Label processing | 244.545517 s | 245.304500 s | +0.310% |
| Label interning | 102.332590 s | 103.267361 s | +0.913% |
| Label building | 142.212927 s | 142.037139 s | -0.124% |
| Peak RSS | 10,922,700 KiB | 10,923,678 KiB | +978 KiB / +0.009% |
| Estimated allocated store bytes | 2,302,673,672 | 1,383,695,112 | -39.909% |
| Reported live-element store bytes | 1,291,186,814 | 1,291,186,814 | 0% |
| Label-pair capacity | 251,658,240 | 136,773,632 | -45.651% |
| Label-pair pages | 1 | 2,087 | +2,086 |

The frozen benchmark binary's used-byte estimator omitted the live
`Vec<InternedKeyValue>` header for each paged page. The committed estimator
includes it. At 2,087 pages that is 50,088 bytes, or about 0.0039% of the
reported 1.291 GB live store size. It does not affect the allocation-capacity
or RSS conclusions.

Actual run midpoints were slightly uneven because the harness preserved and
excluded rejected attempts. Linear interpolation between the two contiguous
endpoints gives 0.275% task-clock, 0.248% cycles, 0.286% wall, and 0.909%
intern-time deltas, effectively the same result. With only two observations per
layout there is no useful confidence interval. The sub-percent E2E difference
is directional evidence, not a proven regression.

## Correctness and byte equality

Every accepted run exited zero after exactly three million messages and
reported identical stable totals:

- 116,228,134 observed datapoints;
- 116,128,092 accepted datapoints;
- 115,989,915 recorded samples;
- 6,153,489 series; and
- 503,948 symbols.

All four output trees contain exactly 50 files and 3,965,280,759 bytes. Their
sorted relative-path and per-file SHA-256 manifests are byte-identical, with
manifest SHA-256
`f38340c98826591c9447e571f5894e576d2418fc44f891293c37550bee18e455`.
Thus the comparison covers names and every emitted byte, not only aggregate
counts and sizes.

## Historical selector names

The frozen benchmark binary predates the final disposition and used these
historical runtime names:

- `flat_interned` selected the paged candidate; and
- `flat_interned_contiguous` selected the contiguous control.

The committed source intentionally reverses that experimental default:

- `flat_interned` is the existing contiguous default; and
- `experimental_flat_interned_paged` selects the paged comparator.

Reproductions must select layouts according to the binary hash and recorded
configuration rather than assuming the historical names still apply.

## Earlier noisy evidence

Earlier completed pairs used the same frozen binary and exact output checks,
but ran on a noisier shared host and did not form a complete balanced block.
They remain supporting historical evidence, not the primary estimate.

| Prefix | Task-clock delta | Intern-time delta | Peak-RSS delta | Allocated-byte delta |
| --- | ---: | ---: | ---: | ---: |
| 2M | +0.99% | +1.47% | -26,472 KiB | -11,485,184 |
| 3M | +0.80% | +2.34% | -140 KiB | -918,978,560 |

The two-million RSS difference was only 0.25% and did not reproduce at the
more favorable three-million vector-growth boundary. The guarded ABBA instead
measured +978 KiB, also effectively zero. None is evidence of an RSS benefit.

The superseded one-million-message prototype at
`paged-labels-ab-20260716-094515` expanded `SeriesLoc` from eight to twelve
bytes in both variants. Its performance and memory measurements do not apply
to the final packed-locator candidate, although its output equality passed.

## Code support cost and remaining measurement gap

Against `e5d642d`, the retained comparator is localized to five Rust files.
The current code delta is 678 insertions and 14 deletions: roughly 320
production lines for locators, page storage, accounting, configuration, and
selection, plus roughly 360 lines of focused boundary and equivalence tests.
It does not duplicate the interning algorithm or add a segment/query
compatibility matrix.

The guarded ABBA compared paged and contiguous layouts inside the same
enum-backed binary. It therefore does not measure the common cost of replacing
the old direct `Vec<InternedKeyValue>` field with runtime layout dispatch. A
code-version A/B between the pre-experiment direct-Vec binary and the current
enum-contiguous binary, with both binary hashes recorded, is required before
claiming that retaining the comparator has zero overhead for the default.

## Disposition

The bounded pages demonstrably prevent geometric reserved-capacity growth.
They have no demonstrated E2E or resident-memory win; interning time was
directionally 0.91% higher in the guarded block. Contiguous remains the normal
store. Keep the paged layout only while it serves a named experiment, and
remove it before vNext promotion unless new controlled evidence satisfies the
promotion gate above.
