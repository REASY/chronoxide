# Schema-7 two-million-message prefix result

- **Date:** 2026-07-14
- **Status:** Focused real-corpus replay/readback correctness gate passed.
  The strict paired PromQL gate also passed after batched Schema-7 metadata
  materialization. Schema 7 is selected as the vNext read-layout candidate;
  final writer promotion gates remain open.
- **Raw output:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-schema7-prefix-20260714-140255`
- **Schema-6 reference:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-prefix-20260713-173713/runs/vnext-a`

## Outcome

Schema 7 encoded the same deterministic six-segment replay in 527,684,944
fewer bytes, a reduction of 11.94%. Replay counters, segment IDs, unchanged
artifacts, and a deterministic real-corpus decoded-series fingerprint matched.
The original same-binary storage readback measured Schema 7 as 8.77 times
faster, but later profiling proved that result was dominated by a linear-scan
metadata-cache LRU interacting pathologically with Schema 6's finer cache-key
granularity. After replacing that LRU and rerunning the production PromQL
facade, Schema 6 was faster in every paired timed query. Batched Schema-7
metadata selection/materialization, no-copy cold ranges, and prehashed cache
keys then removed the redundant per-series work: the latest paired run made
Schema 7 4.5 to 6.6 times faster than Schema 6 across all measured cold/warm
query shapes. The 11.94% size reduction and correctness fingerprints remain
durable evidence; both earlier latency results are retained below as the
profiling history that led to the current reader.

## Post-LRU PromQL correction

The corrected same-binary run is preserved at
`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-schema7-promql-lru-20260714-162104`.
It used binary SHA-256
`ddcc49d41e54974c8730d1c954b63823092fd3a3e48c30e8084219eb5a593769`,
the unchanged six-segment corpora, two alternating outer repetitions, three
in-session runs, and four query shapes. All 24 cross-layout pairs matched exact
and portable semantic fingerprints, result shapes, complete `QueryStats`, and
payload counters.

| Query | Run kind | Schema 6 | Schema 7 | Schema 7 / Schema 6 |
| --- | --- | ---: | ---: | ---: |
| Scalar rate aggregation | Cold | 321.31 ms | 539.85 ms | 1.68x |
| Scalar rate aggregation | Warm | 246.15 ms | 505.69 ms | 2.05x |
| Native Histogram quantile | Cold | 792.85 ms | 1,499.06 ms | 1.89x |
| Native Histogram quantile | Warm | 810.95 ms | 1,489.54 ms | 1.84x |
| Metric-name regex selector | Cold | 284.85 ms | 524.62 ms | 1.84x |
| Metric-name regex selector | Warm | 237.61 ms | 500.00 ms | 2.10x |
| Native ExponentialHistogram quantile | Cold | 647.87 ms | 1,157.08 ms | 1.79x |
| Native ExponentialHistogram quantile | Warm | 568.68 ms | 1,138.66 ms | 2.00x |

Schema 6 was faster in all 24 pairs. The paired median made Schema 6 2.00x
faster, with individual Schema-7/Schema-6 ratios from 1.53x to 2.33x. Relative
to the flawed-LRU run, median per-shape Schema-6 latency improved 9.2x to
49.7x; Schema 7 improved 1.3x to 1.8x. Median peak RSS was 45.23 MiB for
Schema 6 and 35.65 MiB for Schema 7, so Schema 7 retained a 21.2% RSS benefit.

Whole-process wall time does not represent steady-state query latency here.
The post-fix medians were 14.77 seconds for Schema 6 and 13.11 seconds for
Schema 7, but the median timed PromQL phases were 5.79 and 11.02 seconds,
respectively. Schema 6 spent another approximately 7.15 seconds in teardown.
A post-fix `perf` sample attributed 60.42% of Schema 6 whole-process cycles to
`retire_artifacts_after_inventory_removal`, which repeatedly scans the much
larger fine-grained resident set. That control-plane cleanup is the next
aggregate-cache fix; it is outside the reported query timer.

In the isolated native-Histogram profile, Schema 6 reported 742.24 ms and
about 3.86 billion query-window cycles, versus 1.504 seconds and about 7.98
billion cycles for Schema 7. The former linear `touch_lru` scan was absent.
Schema 7's largest remaining self-cycle buckets were authenticated cold-page
reads, value-dictionary decoding and validation, cold-page loading,
allocation, and aggregate governor/hash bookkeeping. Because unrelated builds
and tests were active on the host, latency remains directional; the paired
correctness and counter results are authoritative. Raw `perf.data`, query JSON,
and query reports are preserved under
`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-schema7-promql-lru-perf-20260714-162457`.

## Batched Schema-7 PromQL result

The current same-binary paired run is preserved at
`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-schema7-promql-batched-20260714-171150`.
It used binary SHA-256
`55bdd892552f894b11a7cba491563b177e293d525fa459486832392a36074e04`,
the same six-segment corpora and query schedule as the post-LRU run, two
alternating outer repetitions, and three in-session runs. All 24 cross-layout
pairs matched exact and portable semantic fingerprints, result shapes,
complete `QueryStats`, and payload counters.

| Query | Run kind | Schema 6 | Schema 7 | Schema-7 speedup |
| --- | --- | ---: | ---: | ---: |
| Scalar rate aggregation | Cold | 264.07 ms | 48.46 ms | 5.45x |
| Scalar rate aggregation | Warm | 230.77 ms | 38.63 ms | 5.97x |
| Native Histogram quantile | Cold | 702.29 ms | 115.26 ms | 6.09x |
| Native Histogram quantile | Warm | 693.22 ms | 115.01 ms | 6.03x |
| Metric-name regex selector | Cold | 245.51 ms | 54.18 ms | 4.53x |
| Metric-name regex selector | Warm | 229.70 ms | 37.52 ms | 6.12x |
| Native ExponentialHistogram quantile | Cold | 521.61 ms | 78.85 ms | 6.62x |
| Native ExponentialHistogram quantile | Warm | 501.93 ms | 76.34 ms | 6.57x |

Schema 7 was faster in all 24 paired timed queries. Its process peak RSS was
about 37.5 MiB, versus 45.9 to 46.9 MiB for the two Schema-6 processes. This
run establishes the current end-to-end ordering but is not a component
ablation: the binary combines batched facade materialization, no-copy
authenticated cold ranges, prehashed metadata keys, and the O(1) LRU. The host
was noisy, so correctness, exact counters, and bytes are authoritative while
the latency ratios remain directional.

## Strict reader and Prometheus API integration

The strict Schema-7 production reader, independent readback oracle, and HTTP
API were exercised against the same six-segment corpus. The successful API run
is preserved at
`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-schema7-api-e2e-20260714-195647`.
It used one warm-up and three measured requests per query, `pread`, queue depth
256, one concurrent query, and a disabled range-scalar cache. The exact query,
HTTP-client, and API binary hashes are recorded in that directory.

The internal CLI result was the expected-value source for each request. Every
HTTP response matched its portable semantic fingerprint, result-series count,
and result-sample count. Latency is directional because the host was noisy.

| Query | Internal CLI median | HTTP median | Series | Samples |
| --- | ---: | ---: | ---: | ---: |
| Scalar count/rate instant | 39.47 ms | 45.86 ms | 10 | 10 |
| Native Histogram quantile | 111.30 ms | 120.56 ms | 10 | 10 |
| Regex `last_over_time` | 19.28 ms | 42.92 ms | 1,659 | 1,659 |
| Native ExponentialHistogram quantile | 76.14 ms | 78.03 ms | 55 | 55 |
| Scalar count/rate, 30-minute range | 141.75 ms | 141.04 ms | 10 | 38 |

The high-cardinality regex case returned a 1.83 MB HTTP body; its extra wall
time therefore includes result serialization, transfer, JSON parsing, and
portable-fingerprint construction. Both `/api/v1/query` and
`/api/v1/query_range` are covered.

The query suite deliberately uses an explicit `last_over_time(...[5m])` for
the high-cardinality selector. A bare selector is not currently comparable
through this harness: the historical `chronoxide-query` no-step mode returns
all samples in its storage interval, while Prometheus `/api/v1/query` applies
instant lookback and returns one live sample per series. On this corpus those
shapes are 1,760 series/26,863 samples versus 1,659/1,659. This is a CLI mode
distinction, not a Schema-7 or HTTP serialization mismatch. A future bare
selector gate needs an explicit true-instant-at CLI mode rather than silently
changing the interval benchmark.

The separate real-corpus readback run is preserved at
`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-schema7-reader-e2e-20260714-195541`.
It independently checked 14 readback queries with zero mismatches. Eight
additional projections were reported as isolation-check skips and were not
counted as passes. Focused synthetic coverage executes all five stored kinds,
inline and overflow metadata, and both chunk payload files without skips.

## Scope and provenance

The replay consumed exactly 2,000,000 source messages from
`kafka-capture-001`, using the schema-6 reference configuration, deterministic
segment-ID seed 42, 900-second segments, and the 3,600-second out-of-order
window. The only format switch was `experimental_schema7 = true`.

The preserved schema-7 ingester binary has SHA-256
`5f88c254e8b072c43922e9afe1054bb850c9fd039619445fcbcd47e4f08de469`.
Its config has SHA-256
`61574e6affee02aa23f3677f71084e2eeceb321d6c068928922cfd7ee5ba6d16`.
The schema-6 reference binary has SHA-256
`a758a8767e29389d42f8836674255920dd5c615c7b6054107af7312313f63eaf`.
The exact verifier binary used by the final matched read pair is preserved as
`chronoxide-storage-verify-final` in the raw-output root with SHA-256
`83a3017de739d3bd8084134c9454ae75dafd1734ed9d2c3487ccbd13fb291e5a`.

## Correctness evidence

- Both replays emitted the same six deterministic segment IDs.
- The two ingestion reports have no semantic counter differences: messages,
  records, unique metrics, series, observed/accepted/recorded datapoints,
  policy drops, missing values, and every per-type/storage/cardinality table
  match.
- All corresponding `meta.json`, `symbols.bin`, `chunks.bin`,
  `ooo_chunks.bin`, and manifest files are byte-identical.
- Complete schema-7 footer checksum validation passed for all six segments.
- The focused synthetic cross-schema fixture fully decodes both layouts and
  produces the same verified-selection fingerprint.
- The real-corpus gate selected up to 4,096 evenly spaced series refs from
  every segment. It verified canonical labels and series IDs, authenticated
  schema-7 chunk prefixes, compared exact flags/offsets/lengths/scalar-lane
  locators, and decoded the selected chunks.

The real-corpus selection covered 23,962 series, 23,962 chunks, and 88,439
samples. It covered every chunk kind present in the corpus:

| Kind | Corpus chunks | Selected chunks |
| --- | ---: | ---: |
| Float | 9,738,559 | 22,823 |
| Int64 | 0 | 0 |
| Histogram | 401,735 | 324 |
| ExponentialHistogram | 1,011,046 | 807 |
| Summary | 8,783 | 8 |

Both layouts produced fingerprint
`338919ed1450f3e2fc0e3981db8729f1c53761330758aba5ecd6547a9bcca93e`
and identical selected counts and logical chunk bytes.

This hash uses the explicit `chronoxide-verified-storage-selection-v1` domain;
it is not the normative full-replay fingerprint. This is a deterministic
sampled structural/readback equivalence result, not an all-11,160,123-series
proof or a PromQL query oracle. The first exhaustive verifier attempted to
decode all 77.8 million samples even though the complete chunk files were
already byte-identical; it was stopped because that duplicated work would take
roughly an hour per layout. The remaining exhaustive gate should walk every
changed metadata record, batch schema-7 prefix authentication, and decode only
a small per-kind sample.

## Encoded size

| Artifact | Schema 6 bytes | Schema 7 bytes | Delta |
| --- | ---: | ---: | ---: |
| `chunk_index.bin` | 535,686,024 | 384 | -535,685,640 |
| `series.bin` | 740,273,158 | 741,712,206 | +1,439,048 |
| `indexes.puffin` | 1,130,545,535 | 1,137,107,183 | +6,561,648 |
| `chunks.bin` | 1,963,849,451 | 1,963,849,451 | 0 |
| `symbols.bin` | 49,617,837 | 49,617,837 | 0 |
| `meta.json` | 4,117 | 4,117 | 0 |
| `ooo_chunks.bin` | 0 | 0 | 0 |
| `footer.bin` | 984 | 984 | 0 |
| Manifest files | 616 | 616 | 0 |
| **Total** | **4,419,977,722** | **3,892,292,778** | **-527,684,944 (-11.94%)** |

Every series in this prefix used the inline path; no overflow series were
emitted. The 535.7 MB external chunk-index saving dominates the 8.0 MB combined
growth in `series.bin` and `indexes.puffin`.

## Replay and sealing

| Measure | Schema 6 reference | Schema 7 | Change |
| --- | ---: | ---: | ---: |
| Wall time | 464.27 s | 594.62 s | +28.08% |
| User CPU | 458.07 s | 557.01 s | +21.60% |
| System CPU | 5.89 s | 36.74 s | +523.77% |
| Peak RSS | 10,783,540 KiB | 10,527,944 KiB | -2.37% |

A second preserved schema-6 replay took 549.31 seconds, putting schema 7 only
8.25% above that noisy comparison. Do not interpret either wall-time delta as
a stable throughput estimate.

The per-stage logs do identify displaced CPU. Across the two large segments,
schema-6 sealing took 69.42 seconds versus 191.29 seconds for schema 7.
Schema-7 `series.bin` assembly took 48.44 seconds versus 8.90 seconds, and v8
index construction/validation took 86.29 seconds versus 1.11 seconds for v7.
Those two paths are the next write-side optimization targets.

## Focused storage readback latency

> **Superseded latency interpretation:** the 8.77x ratio below was measured
> with the linear-scan metadata LRU. It is not evidence that Schema 7 is
> intrinsically faster. The post-LRU run first reversed the timed-query
> ordering; the later batched production-PromQL result independently
> establishes the current Schema-7 advantage while preserving correctness.

Footer validation was excluded from the timed reads. Each process selected the
same deterministic refs, verified canonical labels and locators, read and
decoded the same chunk payloads, and emitted the same verified-selection
fingerprint.

| Order | Schema 6 | Schema 7 | Ratio |
| --- | ---: | ---: | ---: |
| schema 7, then schema 6 | 78.42 s | 8.95 s | 8.77x |

Two earlier exploratory pairs showed the same direction, but they used the
pre-final fingerprint encoder and are excluded from this comparison. The row
above uses one identical preserved release binary for both layouts.

The result is explained by metadata request shape rather than fewer logical
results:

| Measure | Schema 6 | Schema 7 |
| --- | ---: | ---: |
| Metadata read calls | 139,397 | 21,181 |
| Metadata bytes issued | 40,254,862 | 366,250,546 |
| Peak retained metadata | 29,675,462 | 67,108,851 |
| First-round process RSS | 65,352 KiB | 78,312 KiB |

Schema 7 issued 6.58 times fewer reads but fetched 9.10 times more metadata,
largely through page-granular reads and the 64 MiB aggregate cache. Those
request-shape counters remain useful, but the apparent latency gain was later
attributed primarily to the cache bug. This benchmark is an end-to-end
sealed-storage readback through the schema-neutral metadata facade and
production chunk decoder. It bypasses postings/FST selection and is not a
PromQL query-latency or independent-oracle result.

## Decision and next iteration

Select schema 7 as the vNext read-layout foundation. The measured format is
11.94% smaller, uses less query-process RSS, and the optimized strict reader is
faster for every query shape in the current paired gate without semantic or
`QueryStats` differences. Keep writer emission opt-in until the remaining
promotion work is complete:

1. remove the v8 same-seal construction bottleneck and the schema-7 two-pass
   prefix work, then rerun this exact 2M replay gate;
2. add the fast exhaustive all-series metadata-only comparison;
3. complete corruption/error-propagation coverage for every changed on-disk
   boundary;
4. port the range-scalar cache into the schema-7 facade and expose aggregate
   cache charges plus read/used amplification in the paired report; and
5. repeat the PromQL gate with more alternating rounds on an isolated host
   before treating the measured ratios as stable capacity numbers.
