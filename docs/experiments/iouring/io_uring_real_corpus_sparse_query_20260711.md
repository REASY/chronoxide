# Real-corpus sparse-query io_uring experiment

## Outcome

The real replay corpus can produce query shapes that fill the io_uring queue and improve cold payload I/O. For the selected typed scalar-lane query:

- Cold payload median: **97.800 ms pread → 81.757 ms io_uring QD8**
- Cold payload improvement: **16.4%**
- Cold end-to-end median: **2650.447 ms → 2633.586 ms**
- Cold end-to-end improvement: **0.64%**
- Cold end-to-end mean improvement: **1.88%**
- Semantic fingerprints, result counts, and every `QueryStats` field matched.

The I/O improvement is real, but decoding and materializing 983,583 samples across 254,219 series dominate total latency.

## Environment

- Raw artifacts: `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/io-uring-real-sparse-20260711-193000`
- Commit: `b1bb617`
- Binary SHA-256: `d162d92829bb19fa84feba315c82c1400744db288cadd6471c4806395b869ae9`
- Corpus: `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/segments-replay-20260711-141105`
- Filesystem: ext4 on NVMe `/dev/nvme2n1p5`
- Same release binary for both modes
- End time: `1782980100000`
- io_uring queue depth: 8

## Workload search

Sparse metric-name regexes were tested because metric-major chunk layout makes alternating metric selections produce independent physical ranges within each segment.

| Query | Result series | Logical chunks | Physical spans | Mean reads/submission | QD8 submissions |
|---|---:|---:|---:|---:|---:|
| `{__name__=~".*0"}` | 43,566 | 150,088 | 171 | 7.125 | 20/24 |
| `{__name__=~".*[02468]"}` | 254,119 | 510,699 | 445 | 7.417 | 54/60 |
| `{__name__=~".*_total"}` | 797,884 | 885,727 | 385 | 7.700 | 46/50 |
| `{__name__=~".*_seconds"}` | 196,795 | 222,840 | 190 | 7.600 | 23/25 |

These probes demonstrate that the real corpus is not inherently limited to one-read submissions. The earlier dense histogram query was limited by its selector/layout shape.

## Selected query

```promql
{__name__=~".*[02468]_count"}
```

This selects typed Histogram, ExponentialHistogram, and Summary count projections whose underlying metric names end in an even decimal digit. It exercises normal corpus opening, regex/FST expansion, postings, series matching, scalar-lane planning, coalescing, payload reads, typed scalar decoding, label projection, and PromQL result construction.

The experiment used `--regex-max-expanded-values 1000000`. Its 112,236 examined regex values exceed the production default of 100,000, so this is an explicit experimental workload rather than a production-default query.

## Submission shape

Full `io_uring_enter` argument capture showed:

```text
to_submit=2 calls=1
to_submit=7 calls=1
to_submit=8 calls=285
calls=287 submitted=2289 avg=7.976
```

The real corpus therefore supplies nearly ideal QD8 batches for this selector: 285 of 287 submissions are full.

## Query semantics and work

Every measured cold execution produced:

- Semantic fingerprint: `4c5b7cdbb766b54db4e00a7d52ce2aee8c01477aeff4e2c83cf72d41fc2bf2e7`
- Result series: 254,219
- Result samples: 983,583
- Segments considered: 36
- Segments queried: 12
- Matched/projected series: 254,219
- Logical chunk reads: 254,222
- Logical bytes read: 38,042,105
- Samples decoded: 983,583
- Typed scalar chunks decoded: 254,222
- Regex values examined: 112,236
- Index postings reads: 1,044
- Index postings bytes: 2,047,504
- Process-issued coalesced spans: 2,289 reads / 133,520,847 bytes

All serialized statistics were identical between backends.

## Cold A/B

All `chunks.bin` pages in the 21 GiB corpus were evicted with `POSIX_FADV_DONTNEED` before every fresh process. NVMe partition counters were captured immediately around each query.

| Backend | Run | Query | Chunk payload | NVMe sectors | NVMe cumulative read ms |
|---|---:|---:|---:|---:|---:|
| pread | a1 | 2650.447 ms | 99.689 ms | 315,776 | 151 |
| pread | a2 | 2736.778 ms | 97.800 ms | 315,776 | 150 |
| pread | a3 | 2638.101 ms | 95.832 ms | 315,776 | 149 |
| io_uring QD8 | b1 | 2654.006 ms | 83.056 ms | 310,896 | 310 |
| io_uring QD8 | b2 | 2633.586 ms | 80.423 ms | 310,640 | 306 |
| io_uring QD8 | b3 | 2586.517 ms | 81.757 ms | 310,384 | 305 |

Pread's median device traffic was 161,677,312 bytes; io_uring's was 159,047,680 bytes. The small difference is consistent with different readahead behavior and does not explain the latency result.

The NVMe `read_ms` counter is cumulative across concurrent requests. Its approximately 2× value under io_uring, together with lower wall time, confirms request overlap rather than less storage work.

## Warm control

Across four warm samples per backend:

| Backend | Warm query median | Warm query mean | Warm payload median |
|---|---:|---:|---:|
| pread | 1128.295 ms | 1141.635 ms | 12.104 ms |
| io_uring QD8 | 1096.803 ms | 1095.703 ms | 12.427 ms |

Total warm query variance is much larger than the payload phase. Within the payload phase, io_uring is 2.7% slower with resident pages, matching the synthetic positive-control result: ring bookkeeping only pays off when it hides storage latency.

## Interpretation

The real-data experiment validates three separate claims:

1. **Real Chronoxide layout can fill io_uring batches.** Sparse metric regexes reach 7–8 reads per submission without synthetic storage files.
2. **io_uring improves cold real-corpus payload reads.** The selected query's payload phase improves by 16.4%.
3. **Payload speedup alone may not move the query.** Payload I/O is only about 3–4% of this query's cold wall time; decoding, label/result materialization, and evaluator work dominate.

The larger 3.56× end-to-end positive-control result used tiny selected chunks and deliberately minimized decoding work. This real query performs much more CPU work per fetched range, so only 0.64% reaches median total latency.

## Next experiment

Cross-segment batching remains valuable for dense selectors that currently average one read per submission. However, the next implementation should be evaluated with at least two gates:

- **I/O-phase gate:** sparse real-data queries like this one must preserve the 10%+ cold payload improvement.
- **End-to-end gate:** identify production-safe selectors with lower decode/result cost, or pipeline decoding with the next I/O batch, so storage concurrency overlaps CPU work and reaches total latency.

An adaptive backend remains appropriate: use pread for tiny/resident-page batches and io_uring for sufficiently large cold candidates. Page residency is not directly known at query time, so batch size is the safe first signal; more advanced policies would need measured feedback rather than assumptions.
