# LabelSetStore: from naive strings to dictionary encoding

When you ingest OTLP metrics, every datapoint carries a labelset (metric name plus label pairs).
The LabelSetStore is the hot path that maps a canonical labelset to a `SeriesRef` and deduplicates
the series. The storage layout you choose here determines both memory footprint and ingestion CPU.

Inspired by https://habr.com/ru/companies/flant/articles/878282/ (in Russian).

This article walks through three LabelSetStore implementations in Chronoxide:

1) `NaiveLabelSetStore` - intentionally inefficient, uses owned strings per series.
2) `FlatInternedLabelSetStore` - interns keys/values and stores label pairs in a flat arena.
3) `KeySetDictEncodedLabelSetStore` - groups by keyset and dictionary-encodes values.

The benchmark and allocator data comes from:

- `docs/experiments/labelset_store/results/bench_results.log`
- `docs/experiments/labelset_store/results/memory_results.log`

## TL;DR

- Naive is easy to reason about, but it explodes memory and allocator pressure.
- FlatInterned is faster and far more memory efficient with almost no fragmentation.
- KeySetDictEncoded uses the least memory, but labelset reads are much slower.

## Baseline: NaiveLabelSetStore

The naive store keeps each labelset as its own vector of owned strings:

```
Vec<Vec<OwnedKeyValue>>
OwnedKeyValue { key: String, value: String }
```

Each series allocates:

- a separate `Vec` header (ptr/len/cap),
- per-label heap allocations for `String` keys and values,
- and hash map bookkeeping for series lookup.

This is correct but hostile to memory: millions of small allocations amplify allocator overhead
and internal fragmentation.

### Memory blowup in practice

On a 400k-message ingest run, RSS rises to ~35 GiB in under a minute:

![NaiveLabelSetStore RSS](../experiments/labelset_store/results/naive.png)

This is not a pathological input. The naive layout stores owned strings per series, and its
`encode` path allocates `OwnedKeyValue` (new `String` key/value) before it can even check whether
the labelset already exists. That means every intern attempt allocates, even on cache hits, which
amplifies allocator churn and keeps RSS high.

### Criterion results (median time)

| Metric        |     Value |
|---------------|----------:|
| Intern unique | 15.042 ms |
| Visit 50k     | 331.69 us |

### Naive allocation profile (100k series)

TrackingAllocator output:

| Metric                 |       Value |
|------------------------|------------:|
| Alloc Calls            |   1,100,017 |
| Req Current            | 38,573,968B |
| Usable Current         | 55,782,208B |
| Internal Fragmentation |      30.85% |
| Estimate Used Bytes    | 37,200,128B |

## Why we need a better layout

Naive storage is correct but too expensive. We need to:

- avoid per-series `Vec` allocations,
- intern repeated strings instead of storing `String` for every series,
- and ideally reuse keysets to compress labelsets further.

## Improved: FlatInternedLabelSetStore

FlatInternedLabelSetStore fixes the two big problems:

1) It interns keys and values using `SymbolTable`, so repeated strings are stored once.
2) It stores all label pairs in a single flat `Vec<InternedKeyValue>`, with per-series
   offsets (`SeriesLoc`) pointing to slices inside the flat array.

This removes the per-series `Vec` and per-string heap allocations, and it preserves fast
labelset reads by slicing the flat array.

It is a drop-in performance win without changing query semantics. See the comparison section
for the numbers.

## Maximum compression: KeySetDictEncodedLabelSetStore

KeySetDictEncodedLabelSetStore takes the memory optimization further by separating keys from
values and dictionary-encoding values per keyset:

- A **keyset** is a sorted list of label keys (by symbol ID).
- For each key, we keep a **value dictionary**: `SymbolId -> ValueCode`.
- Each series row stores only `ValueCode` entries, one per key in the keyset.

This means:

- keys are stored once per keyset,
- values are stored once per key dictionary,
- series rows are dense arrays of compact codes.

It is highly effective when you have many series that share the same keyset and repeated values.
It produces the smallest memory footprint in the experiment.

The tradeoff is CPU on reads: to reconstruct a labelset you have to:

1) resolve the keyset,
2) resolve each value code via the per-key dictionary,
3) and map back to string symbols.

That is why `visit_labelset` is ~8x slower than FlatInterned in the benchmarks.

## Benchmarking and allocator analysis

### Criterion results (median time)

| Store                          | Intern unique (ms) | Visit 50k (us) |
|--------------------------------|-------------------:|---------------:|
| NaiveLabelSetStore             |             15.042 |         331.69 |
| FlatInternedLabelSetStore      |             10.706 |         258.63 |
| KeySetDictEncodedLabelSetStore |             17.561 |         2073.4 |

### Allocation and fragmentation (100k series)

TrackingAllocator output:

| Store                          | Alloc Calls | Req Current | Usable Current | Internal Frag | Estimate Used Bytes |
|--------------------------------|------------:|------------:|---------------:|--------------:|--------------------:|
| NaiveLabelSetStore             |   1,100,017 | 38,573,968B |    55,782,208B |        30.85% |         37,200,128B |
| FlatInternedLabelSetStore      |     100,036 | 13,828,128B |    13,832,248B |         0.03% |         10,003,323B |
| KeySetDictEncodedLabelSetStore |     200,073 | 12,913,536B |    12,913,672B |         0.00% |          9,205,207B |

## Results on 11 million OTLP messages

### Workload summary

These results are from 11,376,766 OTLP messages captured over a ~3h30m window and replayed from
`/tmp` (RAM-backed) to minimize storage I/O:

| Metric                            |        Value |
|-----------------------------------|-------------:|
| Total Messages                    |   11,376,766 |
| Total OTLP Metric Records         |   81,825,901 |
| Total Unique Metrics (`__name__`) |       19,953 |
| Total Series (unique label sets)  |   79,005,309 |
| Total Datapoints                  |  413,593,326 |
| Overall Window                    | 03:29:57.479 |

### RSS comparison across stores

RSS over time for FlatInterned and KeySetDictEncoded stores (same workload, same host):

![LabelSetStore RSS Comparison](../experiments/labelset_store/results/comparison_plot.png)

### Latency on real workload

Metric definitions:

| Metric        | Meaning                                                                                  |
|---------------|------------------------------------------------------------------------------------------|
| Message Total | End-to-end time to process one OTLP message (decode + iterate + intern + build + stats). |
| DP Total      | Per-message average time per datapoint (Message Total / datapoints).                     |
| DP Intern     | Per-message average time per datapoint spent in labelset interning.                      |
| DP Build      | Per-message average time per datapoint spent building datapoint records.                 |

Latency summary (mean / P50 / P95 / P99):

| Metric          | Stat | FlatInterned, µs | KeySetDictEncoded, µs |
|-----------------|------|-----------------:|----------------------:|
| Message Total   | Mean |           57.128 |                77.866 |
|                 | P50  |            6.523 |                 8.707 |
|                 | P95  |          474.659 |               653.083 |
|                 | P99  |         1048.633 |              1448.757 |
| DP Total        | Mean |            1.433 |                 1.938 |
|                 | P50  |            1.439 |                 1.952 |
|                 | P95  |            2.142 |                 2.919 |
|                 | P99  |              2.9 |                 3.961 |
| ** DP Intern ** | Mean |            1.154 |                 1.651 |
|                 | P50  |            1.164 |                 1.677 |
|                 | P95  |            1.741 |                 2.490 |
|                 | P99  |            2.337 |                 3.366 |
| DP Build        | Mean |            0.279 |                 0.286 |
|                 | P50  |            0.270 |                 0.273 |
|                 | P95  |            0.420 |                 0.431 |
|                 | P99  |            0.579 |                 0.612 |

### `/usr/bin/time -pv`

End-of-run stats from `/usr/bin/time -pv` (pinned to CPU cores 10-16):

| Metric                                 | FlatInterned | KeySetDictEncoded |
|:---------------------------------------|:-------------|:------------------|
| User time (seconds)                    | 1045.78      | 1378.54           |
| System time (seconds)                  | 4.55         | 3.71              |
| Percent of CPU this job got            | 101%         | 101%              |
| Elapsed (wall clock) time              | 17:17.47     | 22:36.16          |
| Maximum resident set size (kbytes)     | 18928756     | 12366240          |
| Minor (reclaiming a frame) page faults | 5296890      | 3656262           |
| Voluntary context switches             | 178          | 755               |
| Involuntary context switches           | 19186        | 24622             |

### Store statistics

Store size from the Markdown reports:

| Metric                 |   FlatInterned | KeySetDictEncoded |
|------------------------|---------------:|------------------:|
| Series Count           |     79,005,309 |        79,005,309 |
| Allocated Bytes        | 21,881,684,216 |    14,621,358,208 |
| Used Bytes             | 16,899,455,295 |     9,538,832,603 |
| Allocated Bytes/Series |         276.96 |            185.07 |
| Used Bytes/Series      |         213.90 |            120.74 |
| Symbols                |      2,621,843 |         2,621,843 |

From buffer stats: `sum_per_key_cardinality=3,101,759`.
This is the sum of per-key dictionary sizes across all keys. If a value appears under multiple
keys, it is counted once per key.

## Summary

If you need a safe baseline, `NaiveLabelSetStore` is simple but too expensive for real workloads.

If you want a default that is fast and memory efficient, `FlatInternedLabelSetStore` is the
best balanced choice.

If you are chasing the lowest memory possible and can tolerate slower labelset reads,
`KeySetDictEncodedLabelSetStore` wins on memory by a large margin.

In practice:

- Use FlatInterned for ingestion + query hot paths.
- Use KeySetDictEncoded for memory-constrained scenarios or background compaction paths.

## Appendix: Bench Environment

- Ubuntu 25.10
- Kernel `6.17.0-8-generic`
- CPU: AMD Ryzen 9 9950X (16-core), x86_64
- Build flags: `-C target-cpu=native` (via `.cargo/config.toml`)
- Note: CPU frequency scaling/turbo can shift small deltas; keep clocks stable when comparing close results.
