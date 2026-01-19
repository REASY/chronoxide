# LabelSetStore: from naive strings to dictionary encoding

When you ingest OTLP metrics, every datapoint carries a labelset (metric name plus label pairs).
The LabelSetStore is the hot path that maps a canonical labelset to a `SeriesRef` and deduplicates
the series. The storage layout you choose here determines both memory footprint and ingestion CPU.

Inspired by https://habr.com/ru/companies/flant/articles/878282/ (in Russian).

This article walks through three LabelSetStore implementations in Chronoxide, plus two sealed snapshots of the KeySet
store for maximum density:

1) [NaiveLabelSetStore](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/chronoxide-core/src/labels/interners.rs#L105) –
   intentionally inefficient, uses owned strings per series.
2) [FlatInternedLabelSetStore](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/chronoxide-core/src/labels/interners.rs#L321) –
   interns keys/values and stores label pairs in a flat arena.
3) [KeySetDictEncodedLabelSetStore](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/chronoxide-core/src/labels/interners.rs#L622) –
   groups by keyset and dictionary-encodes values.
4) `FixedWidthPackedKeySetLabelSetStore` – read-only, byte-aligned packing (1/2/4 bytes per key).
5) `BitPackedKeySetLabelSetStore` – read-only, bit-packed storage for maximum compression.

## TL;DR

- Naive is easy to reason about, but it explodes memory and allocator pressure.
- FlatInterned is faster and far more memory efficient with almost no fragmentation.
- KeySetDictEncoded minimizes memory by sharing keys and dictionary-encoding values.
- **FixedWidthPackedKeySet** and **BitPackedKeySet** (sealed, read-only snapshots produced at report time) win on memory:
  ~67/52 bytes per series (Allocated/Used) for fixed-width, and ~58/43 for bit-packed (vs ~233/210 for FlatInterned).

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

This uses the same capture as the 11M-message workload below; the chart shows the first ~400k messages.

This is not a pathological input. The naive layout stores owned strings per series, and its
`encode` path allocates `OwnedKeyValue` (new `String` key/value) before it can even check whether
the labelset already exists. That means every intern attempt allocates, even on cache hits, which
amplifies allocator churn and keeps RSS high.

### Criterion results

| Metric        |     Value |
|---------------|----------:|
| Intern unique | 15.084 ms |
| Visit 50k     | 324.03 us |

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
values and dictionary-encoding values per key (global across keysets):

- A **keyset** is a sorted list of label keys (by symbol ID).
- For each key, we keep a **value dictionary** (global across keysets): `SymbolId -> ValueCode`.
- Each series row stores only `ValueCode` entries, one per key in the keyset.

This means:

- keys are stored once per keyset,
- values are stored once per key dictionary,
- series rows are dense arrays of compact codes.

It is highly effective when you have many series that share the same keyset and repeated values.
It produces the smallest memory footprint among the **mutable** stores in the experiment.

To minimize memory further, this store supports two **sealed** read-only layouts:

- **FixedWidthPackedKeySetLabelSetStore** stores `ValueCode` entries in byte-aligned widths (1/2/4 bytes), chosen per key
  based on dictionary cardinality. Rows remain directly indexable, so this is a good balance of speed and memory.
- **BitPackedKeySetLabelSetStore** stores `ValueCode` entries in a bitstream using the exact number of bits per key. This
  removes the last few bytes of overhead at the cost of bit-level unpacking on reads.

Both are snapshots of the mutable KeySet store (the vectors are shrunk to fit), so they are immutable and efficient for
scan-heavy workloads.

**This is the game changer.** On the 11M-message workload, the unpacked KeySet store uses ~118.75 bytes per series (Used).
Fixed-width packing drops that to ~52.06 bytes, and bit-packing pushes it to ~43.07 bytes per series.

### Visualization

To see how the "Keyset -> Dictionary -> Row" structure looks in practice, here is a dump of a small store with 3 series.

Notice how `namespace` and `pod` (which have higher cardinality) reuse values via codes `0` and `1`, while `__name__` is
stored just once in the keyset and has a single-entry dictionary.

```text
KeySetLabelSetStore
  series=3 keysets=1 value_dicts=5 sum_per_key_cardinality=7 symbols=12
  estimate_size_bytes=2300 estimate_used_bytes=1474
Symbols (first 200):
  SymbolId(0) "__name__"
  SymbolId(1) "pod_cpu_usage_seconds_total"
  SymbolId(2) "cluster"
  SymbolId(3) "prod"
  SymbolId(4) "container"
  SymbolId(5) "web"
  SymbolId(6) "namespace"
  SymbolId(7) "payments"
  SymbolId(8) "pod"
  SymbolId(9) "backend-123"
  SymbolId(10) "backend-1231"
  SymbolId(11) "payments2"
KeySets (first 200):
  KeySetId(0): [SymbolId(0)="__name__", SymbolId(2)="cluster", SymbolId(4)="container", SymbolId(6)="namespace", SymbolId(8)="pod"]
Value Dictionaries (first 200):
  Key SymbolId(0)="__name__": cardinality=1
    ValueCode(0) -> SymbolId(1) "pod_cpu_usage_seconds_total"
  ...
  Key SymbolId(6)="namespace": cardinality=2
    ValueCode(0) -> SymbolId(7) "payments"
    ValueCode(1) -> SymbolId(11) "payments2"
  Key SymbolId(8)="pod": cardinality=2
    ValueCode(0) -> SymbolId(9) "backend-123"
    ValueCode(1) -> SymbolId(10) "backend-1231"
Rows per KeySet (first 200):
  KeySetId(0): key_count=5 rows=3
    row 0: "__name__"="pod_cpu_usage_seconds_total", ... "pod"="backend-123"
    row 1: "__name__"="pod_cpu_usage_seconds_total", ... "pod"="backend-1231"
    row 2: "__name__"="pod_cpu_usage_seconds_total", ... "pod"="backend-1231"
Series (first 200):
  SeriesRef(0): KeySetId(0) row=0
  SeriesRef(1): KeySetId(0) row=1
  SeriesRef(2): KeySetId(0) row=2
```

The tradeoff is CPU on reads. To reconstruct a labelset from a `SeriesRef`, the store must:

1) **Fetch the Keyset**: Resolve `KeySetId` to the list of `SymbolId` keys.
2) **Fetch the Row**: Retrieve the `ValueCode` entries for this series.
3) **Resolve Values**: Fetch the per-key dictionary (hash lookup by key), then index into `code_to_value` to map each
   `ValueCode` to a `SymbolId`.
4) **Resolve Strings**: Finally, map the key/value `SymbolId`s back to strings via the SymbolTable.

This extra indirection (per-key hash lookup + code unpacking) explains why `visit_labelset` is ~8x slower than
FlatInterned in the benchmarks (2081us vs 262us). The packed variants add ~6% (fixed-width) to ~17% (bit-packed) on
top of that due to extra unpacking.

## Benchmarking and allocator analysis

### Criterion results

| Store                               |     Intern unique (ms) | Visit 50k (us) |
|-------------------------------------|-----------------------:|---------------:|
| NaiveLabelSetStore                  |                 15.084 |         324.03 |
| FlatInternedLabelSetStore           |                 10.724 |         262.27 |
| KeySetDictEncodedLabelSetStore      |                 16.984 |        2081.20 |
| FixedWidthPackedKeySetLabelSetStore | can't intern, readonly |        2205.40 |
| BitPackedKeySetLabelSetStore        | can't intern, readonly |        2436.10 |

The benchmarks show that **FlatInternedLabelSetStore** is the performance leader, providing the lowest latency for both interning new series and visiting existing ones. **KeySetDictEncodedLabelSetStore** introduces significant CPU overhead (interning is ~1.6x slower, and visiting is ~8x slower) due to the multiple layers of indirection required for dictionary encoding and value unpacking. The packed variants, while extremely memory-efficient, further increase read latency because they must unpack fixed-width (byte-aligned) or bit-packed values during reads.


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

| Metric                            |           Value |
|-----------------------------------|----------------:|
| Total Messages                    |      11,376,766 |
| Total OTLP Metric Records         |      84,143,299 |
| Total Unique Metrics (`__name__`) |          20,042 |
| Total Series (unique label sets)  |      75,294,581 |
| Total Datapoints                  |     427,040,038 |
| Overall Window                    | 00h:54m:18s.881 |
| Sum per-key cardinality           |       2,515,927 |
| Global distinct values            |       2,099,126 |

Sum per-key cardinality is the sum of per-key dictionary sizes across all keys (values counted once per key).
Global distinct values is the number of unique values across all keys.

### RSS comparison across stores

RSS over time for FlatInterned and KeySetDictEncoded stores (same workload, same host):

![LabelSetStore RSS Comparison](../experiments/labelset_store/results/comparison_plot.png)

### Latency on real workload

DP Intern is a per-message average time per datapoint spent in labelset interning.

| DP Intern         | Count    | Mean, us | StdDev, us | Min, ns | Max, ms | P50, us | P75, us | P95, us | P99, us |
|-------------------|----------|----------|------------|---------|---------|---------|---------|---------|---------|
| FlatInterned      | 11376766 | 1.106    | 7.002      | 177     | 23.514  | 1.133   | 1.307   | 1.674   | 2.218   |
| KeySetDictEncoded | 11376766 | 1.653    | 7.377      | 290     | 24.727  | 1.697   | 1.965   | 2.493   | 3.303   |

### `/usr/bin/time -pv`

End-of-run stats from `/usr/bin/time -pv` (pinned to CPU cores 10-16):

| Metric                                 | FlatInterned | KeySetDictEncoded |
|:---------------------------------------|:-------------|:------------------|
| User time (seconds)                    | 1045.91      | 1421.49           |
| System time (seconds)                  | 3.53         | 3.94              |
| Percent of CPU this job got            | 101%         | 101%              |
| Elapsed (wall clock) time              | 17:17.60     | 23:21.86          |
| Maximum resident set size (kbytes)     | 17911312     | 15408400          |
| Minor (reclaiming a frame) page faults | 5042629      | 5798237           |
| Voluntary context switches             | 236          | 848               |
| Involuntary context switches           | 14337        | 20873             |

### Store statistics

Store size from the Markdown reports (packed variants are sealed snapshots of the KeySet store at report time):

| Metric                 |   FlatInterned | KeySetDictEncoded | FixedWidthPackedKeySet | BitPackedKeySet |
|------------------------|---------------:|------------------:|------------------------|-----------------|
| Series Count           |     75,294,581 |        75,294,581 | 75,294,581             | 75,294,581      |
| Allocated Bytes        | 17,569,939,704 |    13,219,956,068 | 5,054,119,968          | 4,377,054,821   |
| Used Bytes             | 15,852,702,341 |     8,941,045,645 | 3,919,960,556          | 3,242,895,409   |
| Allocated Bytes/Series |         233.35 |            175.58 | 67.12                  | 58.13           |
| Used Bytes/Series      |         210.54 |            118.75 | 52.06                  | 43.07           |
| Symbols                |      2,100,662 |         2,100,662 | 2,100,662              | 2,100,662       |


These statistics confirm that dictionary encoding and packing deliver massive memory savings on real-world datasets.
**BitPackedKeySet** is the clear winner for density, requiring only **~58 bytes per series** (Allocated) or **~43 bytes**
per series (Used), which is a ~4x reduction compared to **FlatInternedLabelSetStore** (~233/210 bytes). **FixedWidth**
already gets you to ~67/52 bytes per series, while the unpacked **KeySetDictEncoded** layout lands at ~176/119 bytes.


## Summary

If you need a safe baseline, `NaiveLabelSetStore` is simple but too expensive for real workloads.

If you want a default that is fast and memory efficient, `FlatInternedLabelSetStore` is the
best balanced choice.

If you are chasing the lowest memory possible and can tolerate slower labelset reads,
`KeySetDictEncodedLabelSetStore` wins on memory by a large margin.

In practice:

- Use FlatInterned for ingestion + query hot paths.
- Use KeySetDictEncoded for memory-constrained scenarios or background compaction paths.
- Seal to FixedWidthPacked or BitPacked when you want a read-only snapshot with maximum density.

## Appendix

### Bench Environment

- Ubuntu 25.10
- Kernel `6.17.0-8-generic`
- CPU: AMD Ryzen 9 9950X (16-core), x86_64
- Build flags: `-C target-cpu=native` (via `.cargo/config.toml`)
- Note: CPU frequency scaling/turbo can shift small deltas; keep clocks stable when comparing close results.

### Detailed Latency Statistics

Metric definitions:

| Metric        | Meaning                                                                                  |
|---------------|------------------------------------------------------------------------------------------|
| Message Total | End-to-end time to process one OTLP message (decode + iterate + intern + build + stats). |
| DP Total      | Per-message average time per datapoint (Message Total / datapoints).                     |
| DP Intern     | Per-message average time per datapoint spent in labelset interning.                      |
| DP Build      | Per-message average time per datapoint spent building datapoint records.                 |

#### FlatInterned

| Metric        | Count    | Mean     | StdDev    | Min   | Max         | P50     | P75      | P95       | P99        |
|---------------|----------|----------|-----------|-------|-------------|---------|----------|-----------|------------|
| Message Total | 11376766 | 57.571us | 253.754us | 400ns | 471.93244ms | 6.399us | 11.238us | 484.575us | 1.031024ms |
| DP Total      | 11376766 | 1.384us  | 7.007us   | 255ns | 23.515182ms | 1.407us | 1.623us  | 2.068us   | 2.764us    |
| DP Intern     | 11376766 | 1.106us  | 7.002us   | 177ns | 23.51452ms  | 1.133us | 1.307us  | 1.674us   | 2.218us    |
| DP Build      | 11376766 | 278ns    | 133ns     | 52ns  | 31.91us     | 268ns   | 320ns    | 414ns     | 574ns      |

#### KeySetDictEncoded

| Metric        | Count    | Mean    | StdDev    | Min   | Max          | P50     | P75      | P95       | P99        |
|---------------|----------|---------|-----------|-------|--------------|---------|----------|-----------|------------|
| Message Total | 11376766 | 80.94us | 321.601us | 520ns | 476.307601ms | 8.838us | 15.576us | 676.739us | 1.464529ms |
| DP Total      | 11376766 | 1.941us | 7.387us   | 365ns | 24.728815ms  | 1.969us | 2.286us  | 2.92us    | 3.943us    |
| DP Intern     | 11376766 | 1.653us | 7.377us   | 290ns | 24.727919ms  | 1.697us | 1.965us  | 2.493us   | 3.303us    |
| DP Build      | 11376766 | 287ns   | 228ns     | 52ns  | 125.661us    | 271ns   | 327ns    | 428ns     | 611ns      |

### Data sources

- [docs/experiments/labelset_store/results/bench_results.log](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/docs/experiments/labelset_store/results/bench_results.log)
- [docs/experiments/labelset_store/results/memory_results.log](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/docs/experiments/labelset_store/results/memory_results.log)
- [docs/experiments/labelset_store/results/naive.png](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/docs/experiments/labelset_store/results/naive.png)
- [docs/experiments/labelset_store/results/comparison_plot.png](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/docs/experiments/labelset_store/results/comparison_plot.png)
- [docs/experiments/labelset_store/results/time_results.md](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/docs/experiments/labelset_store/results/time_results.md)
- [docs/experiments/labelset_store/results/key_set_dict_encoded.md](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/docs/experiments/labelset_store/results/key_set_dict_encoded.md)
- [docs/experiments/labelset_store/results/flat_interned.md](https://github.com/REASY/chronoxide/blob/0210fdec5582b31d6743a921522b511df7f0ab28/docs/experiments/labelset_store/results/flat_interned.md)
