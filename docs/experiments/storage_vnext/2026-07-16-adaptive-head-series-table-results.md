# Adaptive Head Series Table Results

Date: 2026-07-16

## Decision

Use the adaptive paged series table as the default in-memory `HeadWindow`
lookup. Keep `adaptive_series_table = false` as a runtime comparator and safe
fallback.

This is an in-memory ingest optimization. It does not change the segment
format or query semantics.

## Design

The previous head stored every `SeriesRef -> EncodedSeries` entry in one
deterministically hashed `HashMap`. The adaptive table divides refs below `2^24` into
4,096-ref pages:

- a page starts in the shared sparse hash map;
- at 128 occupied slots it promotes to an 8 KiB slot-to-packed-index table;
- `EncodedSeries` values and reverse slots stay packed;
- refs at or above `2^24` always remain sparse;
- the disabled path retains the plain hash map for same-binary comparisons.

Promotion moves values; it does not clone them. New-series insertion remains
transactional: the first sample is encoded successfully before the series is
inserted. Iteration is allocation-free, including consuming iteration during
window sealing.

The 128-entry threshold is a locality hypothesis, not a universal memory
break-even. A 64-partition strided-ref test demonstrates that pages with only
64 entries stay sparse. Live multi-partition Kafka remains an important
follow-up workload even though the fallback is exact.

## Method

Raw artifacts:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/head-series-table-ab-20260716-220817`

The comparison used:

- the real `kafka-capture-001` capture;
- one frozen release binary for both modes;
- binary SHA-256
  `3a5ae7e03524495c04eeaec87f6254054f248e211c7469a6b4ed1681e2d8f4c6`;
- build ID `a95ac43168c669f067d33643e37732b4d61bf73b`;
- `flat_interned`, compact numeric series, Schema 8, 900-second segments,
  and deterministic segment seed 42;
- explicit capture-file `POSIX_FADV_DONTNEED` before every run;
- `/usr/bin/time -v` and `perf stat` for task clock, cycles, instructions,
  branches, branch misses, faults, and context switches;
- a 250k control/adaptive/adaptive/control screen;
- an adjacent 1M control/adaptive gate.

No build, footer validation, or query verification overlapped a measured run.
Host load was mild and similar across the alternating screen. Both adaptive
replicates beat both controls.

## Results

### 250k alternating screen

The values are the mean of two runs per mode.

| Metric | Plain | Adaptive | Delta |
|---|---:|---:|---:|
| Wall time | 57.560 s | 48.675 s | -15.44% |
| Task clock | 57,548.5 ms | 48,597.4 ms | -15.55% |
| Cycles | 320.183 B | 269.986 B | -15.68% |
| Instructions | 773.991 B | 768.088 B | -0.76% |
| Branches | 141.392 B | 140.529 B | -0.61% |
| Branch misses | 571.129 M | 494.896 M | -13.35% |
| Page faults | 2,169,467 | 1,633,294 | -24.71% |
| Peak RSS | 5,478,350 KiB | 5,240,324 KiB | -4.34% |
| Instructions/cycle | 2.417 | 2.845 | +17.69% |

The large cycle reduction with only a small instruction reduction is
consistent with fewer cache/memory stalls and branch mispredictions. Direct
cache-miss and pipeline-stall counters were not collected, so this does not
establish their individual contributions. The result is not explained by
removing ingest work.

### 1M adjacent gate

| Metric | Plain | Adaptive | Delta |
|---|---:|---:|---:|
| Wall time | 133.31 s | 114.26 s | -14.29% |
| Task clock | 133,325.15 ms | 114,351.74 ms | -14.23% |
| Cycles | 742.683 B | 635.512 B | -14.43% |
| Instructions | 1,627.869 B | 1,620.393 B | -0.46% |
| Branches | 299.395 B | 298.533 B | -0.29% |
| Branch misses | 1,430.480 M | 1,324.863 M | -7.38% |
| Page faults | 2,868,339 | 2,246,600 | -21.68% |
| Peak RSS | 8,575,448 KiB | 8,204,264 KiB | -4.33% |
| Instructions/cycle | 2.192 | 2.550 | +16.33% |

The 1M adaptive run saved 19.05 seconds and 362.5 MiB of peak RSS.

## Observed Structure

At 1M messages, the four flushed adaptive windows contained:

- 5,326,810 series total;
- 5,321,333 direct series (99.8972%);
- 5,477 residual sparse series;
- 1,363 direct pages and 329 non-empty sparse pages;
- no refs at or above the `2^24` bounded-directory limit.

The plain run retained a maximum sparse-map capacity of 7,340,032 entries.
The adaptive run's maximum residual sparse-map capacity was 7,032 entries.

## Correctness Gates

- The 250k plain/adaptive/repeat size and SHA-256 manifests are identical:
  34 files and 972,976,604 bytes.
- The 1M plain/adaptive manifests are identical. The checksum-list digest is
  `c57bd2970b615958820edced252694180bede6d57ab898d4e864cefff5b70bfd`.
- Replay counters, typed datapoint counts, drop counts, watermarks, and series
  counts match exactly.
- Footer validation was effective for all four 1M adaptive segments.
- Independent readback verification executed 38 of 38 expected queries with
  zero skips and zero mismatches.
- All 67 focused head tests pass, including plain/adaptive equivalence across
  promotion, repeated lookup, rotation, out-of-order routing, sealing,
  high-ref fallback, first-insert failure atomicity, and 64 strided
  partition-local heads.

## Caveats And Follow-up

- The real capture has one Kafka partition. Before changing the 128-entry
  threshold, measure a live or synthetic workload with the production
  partition count and report direct/sparse coverage per partition.
- The runtime comparator uses the same wrapper and transactional insertion as
  the adaptive mode. Against the preceding frozen AHash-symbol binary, the
  adaptive 250k mean is still 14.84% faster in wall time and 15.14% lower in
  cycles; the wrapper is not the source of the win.
- Container capacities are reported separately from encoded sample payloads;
  they are not allocator-exact byte accounting.
