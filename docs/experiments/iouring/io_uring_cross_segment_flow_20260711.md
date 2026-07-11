# Experimental cross-segment io_uring flow

## Outcome

Planning native Histogram payload reads across segment boundaries can make
io_uring materially faster when one selector evaluation exposes enough
independent spans. On a real-corpus instant query with a six-hour PromQL
lookback over about 1.5 hours of populated metric data, the first experimental
flow:

- reduced median payload-read time from **15.806 ms to 10.616 ms** (**32.8%**);
- reduced median end-to-end time from **244.959 ms to 237.357 ms** (**3.1%**);
- preserved the semantic fingerprint, results, every `QueryStats` field,
  logical payload bytes, coalesced physical bytes, and physical span count.

It does not help every query shape. The original 15-minute range-function
window usually overlaps only one or two 15-minute segments per evaluation.
There, the experimental flow increased median end-to-end time from
1083.764 ms to 1104.014 ms (**1.9% slower**).

The result supports an adaptive design: submission batching is useful when a
single selector evaluation has enough independent segment reads, but should
not be enabled unconditionally for shallow batches.

## Implementation under test

The experiment is behind
`--experimental-cross-segment-chunk-reads` and is disabled by default. For
native Histogram and ExponentialHistogram selectors it separates:

1. per-segment index and payload planning;
2. bounded multi-file payload submission;
3. stable per-segment decode and result merge.

Groups are bounded to 32 segments, 256 physical spans, and 256 MiB. Read
results and errors are restored to request order before decode so completion
order cannot change query error precedence. Per-segment coalescing remains
unchanged.

Histogram was implemented and measured first. ExponentialHistogram was then
added using the same shared typed planning representation and bounded read
flow.

## Environment

- Base commit: `d3275bd`
- Experimental binary SHA-256:
  `7c72c899d5f6fc26e20926b253a08666dbb09037833cb85a726db140abc50e02`
- Corpus:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/segments-replay-20260711-141105`
- Corpus fingerprint:
  `b9c1470b99726c3f6a53591bf5ec7fb8f96b0691f474e6935a27fce6de145891`
- Filesystem: ext4 on NVMe
- io_uring queue depth: 8
- Same release binary for every backend/flow comparison
- Before every measured process, all `chunks.bin` pages were advised with
  `POSIX_FADV_DONTNEED`
- Raw artifacts:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/io-uring-cross-segment-20260711-204000`

## Positive-control workload

```promql
histogram_quantile(
  0.95,
  sum by (service_name_x55e50a58f9befba7)(
    rate(http_client_duration_xf5f33b0f6bbd8257[6h])
  )
)
```

This is one instant evaluation at `1782985800000`. The `[6h]` range is the
PromQL lookback, not the populated data duration: the selected metric contains
about 1.5 hours of actual data. It selected 14,103 logical chunks (19,780,449
used bytes) and issued 24 coalesced physical spans (19,978,557 bytes).

With Histogram batching alone, full syscall tracing showed one QD8 and one QD7
submission for Histogram plus nine single-read ExponentialHistogram
submissions. With both typed paths enabled, the same 24 spans are issued as two
QD8 submissions, one QD7 submission, and one QD1 submission.

### Incremental ExponentialHistogram result

The Histogram-only and both-types binaries were compared using identical
corpus, query, cache eviction, queue depth, and interleaved three-run schedules.

| Flow | Payload median | End-to-end median |
|---|---:|---:|
| Default per-segment io_uring | 23.038 ms | 250.993 ms |
| Cross-segment Histogram only | 16.847 ms | 245.284 ms |
| Cross-segment Histogram + ExponentialHistogram | **14.818 ms** | **244.587 ms** |

Adding ExponentialHistogram batching improves the Histogram-only payload phase
by another **12.0%**. Relative to the default path, the combined flow improves
payload time by **35.7%** and end-to-end time by **2.6%**. Fingerprints,
results, every `QueryStats` field, logical bytes, physical bytes, and physical
span counts match.

### Three-run cold A/B

| Backend and flow | End-to-end median | End-to-end mean | Payload median |
|---|---:|---:|---:|
| pread, default | 245.105 ms | 246.670 ms | 15.743 ms |
| pread, cross-segment | 243.986 ms | 244.538 ms | 16.724 ms |
| io_uring QD8, default | 244.959 ms | 244.869 ms | 15.806 ms |
| io_uring QD8, cross-segment | **237.357 ms** | **239.050 ms** | **10.616 ms** |

All runs produced fingerprint
`84d8d11ddbfb89d35141f79ab0e1369b03818625de6d4729b0d0e0e114c6f302`,
11 result series, 11 result samples, and identical serialized `QueryStats`.

The pread payload result is a useful control: merely deferring decode does not
make reads faster. The gain comes from concurrent io_uring submission.

## Original shallow-window workload

The original query used the same metric and aggregation but a `[15m]` range,
evaluated every minute over one hour. Although the full query touches many
segments, each individual range evaluation normally overlaps only one or two.

The experimental trace contained 345 physical spans in 186 submissions:

| Reads submitted | Calls |
|---:|---:|
| 1 | 114 |
| 2 | 14 |
| 3 | 41 |
| 4 | 12 |
| 5 | 1 |
| 6 | 2 |
| 7 | 1 |
| 8 | 1 |

Average submission depth was only 1.85. Three-run medians were:

| Backend and flow | End-to-end median | Payload median |
|---|---:|---:|
| io_uring QD8, default | 1083.764 ms | 26.337 ms |
| io_uring QD8, cross-segment | 1104.014 ms | 40.172 ms |

The extra planning lifetime and simultaneous buffers cost more than the
limited storage overlap saves for this shape.

## Perf counters

A single cold `perf stat` pair on the six-hour io_uring workload showed nearly
identical instruction counts (3.811 billion default vs 3.810 billion
experimental). The experimental run had fewer context switches (146 to 102),
fewer cache misses (12.64 million to 11.67 million), and lower task-clock time
(250.18 ms to 245.00 ms). These are supporting observations; the three-run
timings above are the primary comparison.

## Correctness gates

- Focused three-segment native Histogram and ExponentialHistogram tests compare
  exact results, all `QueryStats`, logical bytes, physical spans, physical
  bytes, byte-limit errors, and sample-limit errors against the default flow.
- Split payload plans reject missing or short backend results.
- io_uring completions are materialized in original request order.
- `cargo test -p chronoxide-core` passed (400 unit tests plus integrations).
- `cargo test -p chronoxide-ingester --bin chronoxide-query` passed (60 tests).
- The focused io_uring feature test passed.

## Next decision

Do not make this flow the default yet. The next useful experiment is an
adaptive threshold based on planned cross-segment physical span count and
bytes. A conservative first policy should retain the current per-segment flow
for one- or two-span groups and use cross-segment io_uring only once the batch
is deep enough to amortize simultaneous buffer allocation and ring overhead.

The adaptive threshold should cover both Histogram and ExponentialHistogram.
The predictor must be planned physical spans and bytes, not nominal PromQL
lookback duration: the positive query has a six-hour lookback but only about
1.5 hours of populated metric data.
