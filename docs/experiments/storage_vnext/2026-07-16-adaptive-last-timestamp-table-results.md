# Adaptive last-timestamp table results

Date: 2026-07-16

## Decision

Keep the adaptive paged last-timestamp table.

On the 1,000,000-message real-capture gate it reduced task-clock, cycles,
instructions, page faults, and peak RSS while producing a byte-identical
schema-8 segment tree. The improvement is deliberately adaptive: partition-
local series-ref pages below 50% occupancy retain the previous hash
representation instead of always allocating one dense page per Kafka
partition.

## Motivation

The post-label-hash replay profile attributed about 6.96 CPU-seconds to
hash-table lookup/tag work below `HeadBuffer::record_samples`. The real corpus
has 5,214,871 globally dense series refs and 38.7 million accepted datapoints,
so most last-timestamp checks are repeated lookups.

The previous representation was one `HashMap<SeriesRef, u64>` per partition
head. At this cardinality its likely 8,388,608 buckets require roughly 136 MiB
before allocator metadata.

## Implementation

- Split refs below `2^24` into 4,096-ref pages.
- Keep a page in the existing sparse hash map until it contains 2,048 accepted
  refs.
- At the 50% threshold, move that page into a direct `u64` array plus an
  occupancy bitmap. The bitmap preserves both timestamp zero and `u64::MAX`.
- Keep refs at or above `2^24`, including `u32::MAX`, in the sparse fallback;
  they cannot expand the flat page directory.
- Retain a mutable occupied slot across validation and recording, then update
  it only after the sample is accepted. A rejected first sample does not
  allocate a page or create timestamp state.

A dense 4,096-ref page is 33,280 bytes. At the observed cardinality, 1,273
dense pages, one 663-entry sparse tail page, and the observed page-directory
capacity account for 40.434 MiB before the residual sparse hash table and
allocator metadata. This is roughly 95 MiB below the old hash table. Uniform
four-way and sparser strided pages remain below the promotion threshold;
skewed pages may still promote independently in more than one partition.

## Method

Raw artifacts:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/last-timestamp-table-ab-20260716-185700`

Input:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001/partition-1.capture`

Both variants used the same schema-8 configuration, deterministic seed 42,
release-with-debuginfo builds, explicit capture-cache eviction, `perf stat`,
and `/usr/bin/time -v`. No build or profiler overlapped a measured process.

| Variant | Binary SHA-256 |
| --- | --- |
| Hash-map control | `d8b0f102900569c3999ade924b2778600921de8fdafe8546bd86eee8225b4d36` |
| Adaptive candidate | `f0cb9f85ac6e2a75174fb0af3c278708edd76eb704f2cbfed6d583a687f74883` |

The 250,000-message screen used C-A-A-C order. The 1,000,000-message gate used
one control followed by one adaptive run. The host was noisy, so instructions
and output identity are the stable primary signals; single-pair wall time is
supporting evidence.

## Results

### 250,000-message alternating screen

| Metric | Control mean | Adaptive mean | Delta |
| --- | ---: | ---: | ---: |
| Task-clock | 61,334.91 ms | 61,995.47 ms | +1.077% |
| Instructions | 800,769,251,278 | 800,041,849,874 | -0.091% |
| Peak RSS | 5,581,940 KiB | 5,479,238 KiB | -100.30 MiB (-1.840%) |

The screen established the capacity and instruction reduction, but its
task-clock result was negative/noisy. Promotion cost is front-loaded, while
the intended benefit is repeated lookups, so the larger gate was required.

### 1,000,000-message real-capture gate

| Metric | Control | Adaptive | Delta |
| --- | ---: | ---: | ---: |
| Wall time | 154.31 s | 153.45 s | -0.56% |
| Task-clock | 154,190.59 ms | 153,473.57 ms | -0.465% |
| Cycles | 849,497,480,152 | 844,102,678,219 | -0.635% |
| Instructions | 1,731,864,863,440 | 1,730,778,790,436 | -0.063% |
| Branches | 310,414,535,417 | 310,370,264,293 | -0.014% |
| Branch misses | 1,554,018,072 | 1,538,537,031 | -0.996% |
| Page faults (`perf stat`) | 2,918,355 | 2,867,882 | -1.730% |
| Peak RSS | 8,670,624 KiB | 8,571,524 KiB | -96.78 MiB (-1.143%) |

The control task-clock was about 5.8% slower than an earlier run of the same
binary while its instruction count was stable. That confirms meaningful host
noise; the result supports CPU-neutral to modestly better behavior, not a
large latency claim. `/usr/bin/time` recorded 117 major faults for the
candidate versus zero for the control even though total and minor faults fell,
so the page-fault reduction is not evidence of less device I/O.

## Correctness gates

- Both 250,000-message C-A-A-C runs produced identical manifests and file
  hashes.
- Both 1,000,000-message runs produced the expected 34-file,
  1,584,337,371-byte tree.
- The 1,000,000-message manifest digest matched exactly:
  `c57bd2970b615958820edced252694180bede6d57ab898d4e864cefff5b70bfd`.
- Footer validation was enabled and effective for all four segments.
- Independent readback verification executed 38 expected queries with zero
  skips and zero mismatches.
- Focused tests cover the exact sparse-to-dense transition, strided sparse
  pages, page boundaries, zero and `u64::MAX` timestamps, `u32::MAX`, mutable
  dense/sparse updates, and a deterministic differential trace against
  `HashMap`.

## Interpretation

This is primarily a capacity improvement with a small CPU benefit. It removes
about 97 MiB from peak RSS on the full replay and avoids fixed dense storage on
sparse partition heads. It does not change the storage format or segment bytes.
Removed entries do not immediately shrink the sparse HashMap allocation, and
refs at or above `2^24` remain hash-backed, so future multi-partition and
long-lived ingestion evidence should record per-partition dense-page counts,
sparse length/capacity, overflow count, and RSS.
