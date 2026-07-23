# Bounded postings-growth result

**Status:** promoted as a segment-seal memory improvement. Large exact-postings
lists now add one bounded midpoint capacity between adjacent powers of two
instead of always jumping directly to the next power of two.

## Decision

Promote the single-pass bounded-growth builder and reject both measured
exact-capacity two-pass builders.

On the accepted 250,000-message replay prefix, the promoted candidate:

- reduced the official Heaptrack process requested-live maximum by
  66,354,924 bytes (63.281 MiB, 1.7955%);
- reduced complete live postings-builder storage by 66,355,200 bytes
  (63.281 MiB, 12.439%);
- reduced retained postings-vector slack by 41.130%;
- added 789 postings-vector growth allocations over the complete replay and
  777 whole-process allocation calls;
- changed mean ABBA instructions by +0.153%, task clock by -0.095%, and wall
  time by -0.179%;
- preserved all 34 storage files and 972,969,365 corpus bytes exactly;
- passed complete footer validation; and
- passed 40/40 independent readbacks with zero skips, isolation skips, or
  mismatches.

The two-pass alternatives removed more slack, but their repeated traversal and
cardinality accounting produced stable CPU and writer-flush regressions. The
bounded-growth candidate captures a smaller but process-wide memory win without
paying that cost.

## Change under test

The segment writer still builds the same
`BTreeMap<(u32, u32), Vec<u32>>` exact-postings index in one pass and returns
the same `ExactPostingsIndex`. Only the transient capacity policy changes.

For lists below 16,384 `u32` references, ordinary `Vec` growth is unchanged.
When a list at or above that threshold is full, the builder selects:

```text
midpoint = ceil(3 * capacity / 2)
legacy_ceiling = next_power_of_two(capacity + 1)
next_capacity = min(midpoint, legacy_ceiling)
```

This produces:

```text
16,384 -> 24,576 -> 32,768 -> 49,152 -> 65,536 -> ...
```

The legacy ceiling matters. It guarantees that no individual list receives
more capacity than ordinary power-of-two `Vec` growth would have selected.
The midpoint only reduces tail slack between those ceilings.

The builder:

- checks an equal trailing reference before attempting growth;
- preserves the existing monotonic append fast path;
- preserves sorted insertion and deduplication for decreasing references;
- calls fallible `try_reserve_exact` only when a large list is full; and
- maps capacity overflow or allocation failure to `io::ErrorKind::OutOfMemory`.

Final symbol remapping, postings ordering, index encoding, checksums, and
persisted bytes are unchanged. This is an in-memory allocation-policy change,
so it requires no storage version or `storage.md` update.

## Why this shape

The accepted corpus contains 313,963 postings lists and 89,285,049 references.
Its nearest-rank list lengths are:

| Quantile | References |
| --- | ---: |
| p50 | 8 |
| p90 | 63 |
| p95 | 174 |
| p99 | 2,195 |
| maximum | 3,116,435 |

Only 541 aggregate lists exceed 16,384 references, but they contain
62,257,344 references. The threshold therefore leaves nearly every small list
and its hot insertion behavior alone while targeting the small number of large
lists responsible for most retained slack.

In the dominant segment, 311,863 lists contain 88,864,686 references. Their
logical `u32` payload is 355,458,744 bytes, while legacy vector backing retained
516,788,496 bytes. The 161,329,752-byte difference was 153.856 MiB of capacity
slack. This exact decoded inventory predicted a 66,355,200-byte reduction from
the bounded policy before the candidate was profiled.

## Alternatives measured and rejected

Both exact-capacity designs performed a complete cardinality pass and then
allocated every postings list at its final length before the normal fill pass.
They were isolated, correctness-gated, and measured against the same control.

| Candidate | Memory evidence | Formal CPU/runtime evidence | Decision |
| --- | --- | --- | --- |
| Ordered `BTreeMap` cardinality pass | Heaptrack requested-live maximum 3,695,665,350 B to 3,528,350,674 B: -159.563 MiB/-4.527% | Instructions about +7.12%, replay wall about +4.08%, largest-window writer flush about +13.44% | Rejected |
| Keyed `AHashMap` cardinality pass | Mean GNU-time maximum RSS -85.150 MiB | Instructions +2.080%, task clock +1.387%, wall +1.557%, largest-window elapsed +2.466%, writer flush +4.001% | Rejected |
| Single-pass bounded midpoint growth | Heaptrack requested-live maximum -63.281 MiB/-1.7955% | Instructions +0.153%, task clock -0.095%, wall -0.179%, writer flush -0.103% | **Promoted** |

The AHash variant removed most ordered-map comparison overhead, but the
remaining second pass and capacity bookkeeping still regressed every formal
CPU/time measure. Exact capacity is therefore closed for this corpus unless a
future writer architecture obtains final list cardinalities without adding a
second label traversal.

## Heaptrack memory evidence

The control is the frozen packed-cold-row binary and trace. The candidate uses
the Rust system allocator.

| Requested-live measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Process maximum | 3,695,665,350 B | 3,629,310,426 B | -66,354,924 B (-63.281 MiB, -1.7955%) |
| Complete postings builder at process peak | 533,430,528 B | 467,075,328 B | -66,355,200 B (-63.281 MiB, -12.439%) |
| Postings vector backing | 516,788,496 B | 450,433,296 B | -66,355,200 B |
| Logical `u32` references | 355,458,744 B | 355,458,744 B | unchanged |
| Retained vector slack | 161,329,752 B | 94,974,552 B | -66,355,200 B (-41.130%) |
| `BTreeMap` nodes | 16,642,032 B | 16,642,032 B | unchanged |

The allocation-site model differs from the official whole-process result by
only 276 bytes. Non-target live memory at the process maximum was therefore
effectively identical.

| Allocation measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Complete postings target calls | 958,971 | 959,760 | +789 |
| Complete postings cumulative requested bytes | 1,050,083,104 B | 1,281,064,736 B | +230,981,632 B |
| Whole-process allocation calls | 240,159,535 | 240,160,312 | +777 |
| Temporary allocations | 40,535,897 | 40,535,727 | -170 |

The cumulative-byte increase is expected: tighter growth retains less final
slack but copies a few large vectors through one additional midpoint. The
formal CPU gate below verifies that this extra copying is immaterial for the
complete replay.

The valid candidate trace recorded its official requested-live maximum at
74.099 seconds. The frozen control reached its corresponding maximum at
74.345 seconds. GNU `time` observed maximum RSS move from 3,261,260 KiB to
3,243,476 KiB (-17.37 MiB); that allocator/OS measure agrees directionally but
is not substituted for exact requested-live accounting.

The first candidate profiling command accidentally applied `env -i` to the
profiled command instead of to Heaptrack itself. That cleared Heaptrack's
preload environment and profiled only the `env` wrapper, producing a 9.6 KiB,
806-allocation trace. It is quarantined under
`heaptrack-invalid-env-wrapper` and excluded from every result above. The valid
rerun contains 240,160,312 allocation calls and an 89 MiB compressed trace.

## Formal ABBA runtime evidence

The accepted schedule was control A, candidate A, candidate B, control B. All
four arms reproduced the accepted replay counters and exact corpus. Means are
arithmetic means of the two arms for each binary.

| Measure | Control mean | Candidate mean | Change |
| --- | ---: | ---: | ---: |
| Wall time | 41.945 s | 41.870 s | -0.075 s (-0.179%) |
| Task clock | 41,924.355 ms | 41,884.625 ms | -39.730 ms (-0.095%) |
| Cycles | 233,104,804,385 | 232,876,886,876 | -0.098% |
| Instructions | 689,337,958,595 | 690,390,712,069 | +0.153% |
| Maximum RSS | 3,249,930 KiB | 3,237,414 KiB | -12,516 KiB (-12.223 MiB, -0.385%) |
| Largest-window elapsed | 21,219 ms | 21,166 ms | -53 ms (-0.250%) |
| Largest-window writer flush | 11,123 ms | 11,111.5 ms | -11.5 ms (-0.103%) |

Control instruction dispersion was 0.0039%; candidate dispersion was 0.0348%.
The +0.153% mean instruction movement is much smaller than the rejected
two-pass variants and did not translate into worse task clock, wall time, or
flush time. The candidate is classified as runtime-neutral, not as a speedup.

## Measurement contract

- Base source:
  `b9602d3c27fb46513c15600f323333efe2ec20a0`
- Control ingester SHA-256:
  `0ebcc522df19eb1add7ff16a3fea6f34fec021228321858aa2e772b7b1b295ac`
- Candidate patch SHA-256:
  `60e09f4edf24dc8a692230ce89d7c05798ad5f6ed92c8622b54a1bee1f4a61b5`
- Candidate ingester SHA-256:
  `6c953b91f25e926b6237c7e294312d08a7745a8b19d3b9d218c199eee2532d33`
- Candidate query SHA-256:
  `76ca6106e318829159b26770eeabf7c69a7a26f501797d2240e8b53ff7367a2c`
- Frozen control Heaptrack trace SHA-256:
  `0ea87f6a5c0cd15023df0c494b0ca9d8d7e260cafdd0f54238ba84c09fc04fbe`
- Valid candidate Heaptrack trace SHA-256:
  `2ab55181616264452b1fdfbf3033d583c26180233ba69a28b6680346a60e817b`
- Workload: exact accepted 250,000-message capture prefix
- Writer configuration: identical except for run-specific output paths;
  deterministic segment seed 42
- Storage schema: Schema 8
- Allocator: Rust system allocator

The promoted evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/compact-postings-growth-memory-20260723T205908Z-yNQEXy`

Rejected exact-capacity evidence roots are:

- ordered cardinality:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/exact-capacity-postings-memory-20260723T201918Z-JrIajo`;
- keyed AHash cardinality:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/exact-capacity-postings-ahash-memory-20260723T204027Z-sy1Pj7`.

## Correctness evidence

The real replay reproduced:

- 250,000 accepted replay messages and 9,634,809 recorded samples;
- 4 segments, 34 files, and 972,969,365 bytes;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
- replay-correctness JSON, corpus summary, complete file inventory, and every
  segment SHA-256 byte-for-byte;
- complete segment-footer validation; and
- all 40 independent readback-oracle cases with zero skips, isolation skips,
  or mismatches.

Focused tests compare candidate insert, ordering, deduplication, decreasing-ref
fallback, threshold capacity, and power-of-two ceiling behavior with the
independent legacy builder. A separate finalization test remaps a deliberately
non-canonical symbol table and verifies the complete finalized postings index
against a freshly constructed legacy index.

## Artifact cleanup

After the A/B, profile, byte-equivalence, footer, readback, and analysis gates
completed, only regenerated segment trees and two redundant candidate query
binaries were removed from the three postings experiment roots. The exact
cleanup manifest validated and reclaimed:

- 15 segment trees containing 510 files and 14,594,540,475 logical bytes;
- 2 query binaries containing 1,014,609,496 bytes;
- 512 files and 15,609,149,971 logical bytes in total (14.54 GiB).

Frozen ingester binaries, patches, hashes, Heaptrack traces, perf data, logs,
manifests, reports, analysis, and cleanup records remain. No capture, unrelated
corpus, or user-owned artifact was removed.

## Verification

The exact measured candidate source passed:

- the compact-builder differential and capacity-boundary tests;
- the independent finalized-postings remap test;
- targeted storage tests and strict `chronoxide-core` Clippy;
- complete segment-footer validation;
- `chronoxide-query --verify-readbacks` with 40/40 executed and zero skipped;
- `cargo fmt --all -- --check`;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed workspace-wide all-feature Clippy gates for libraries,
  binaries, tests, and benches with warnings denied; and
- `git diff --check`.
