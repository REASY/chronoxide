# Four follow-up storage and query optimization experiments

- **Date:** 2026-07-16
- **Status:** All four candidates were implemented or profiled against real
  data. Native Histogram/ExponentialHistogram selective label materialization
  is the only candidate promoted to the normal execution path, and only for
  its proved whitelist. Paged ingest label pairs and query-session shared atoms
  remain explicit comparators.

## Outcome

| Experiment | Result | Disposition |
| --- | --- | --- |
| Paged ingest label-pair storage | Prevented a large contiguous-capacity jump, but did not reduce peak RSS and made replay about 0.8% slower at 3M messages | Keep contiguous storage as the default; retain paging only as an opt-in comparator |
| Fresh post-compact replay profile | Allocator, label, hashing, and equality work dominate; the earlier event-skew hypothesis was not supported | Optimize the measured label/ownership path rather than event-skew statistics |
| Query-session shared label atoms | Cut peak RSS by as much as 55%, but regressed the high-cardinality full-label query by 11–26% after removing redundant final re-interning | Keep owned strings as the default; use shared atoms only as an experiment until atom lookup is cheaper and governed |
| Native typed selective label materialization | Saved 30–31% of owned label pairs and improved every eligible cold and warm median | Keep demand-driven materialization enabled for the exact proved native whitelist |

The experiments used the same real Schema 8 corpus or real capture prefix for
each A/B pair. Runtime comparators used one identical release binary. Semantic
and portable fingerprints, ordinary `QueryStats`, payload-read counters, full
touched-row integrity counters, and corpus inventories were required to match.
Footer validation and the independent readback pass ran separately from timed
queries.

The host was shared and noisy. Repeated alternating query results are suitable
for directional decisions, not sub-percent claims. Byte equality, accounting,
and correctness gates are authoritative.

## 1. Paged ingest label pairs

The final candidate used 65,536-pair pages and retained the existing eight-byte
series locator. At the three-million-message vector-growth boundary it reduced
estimated allocated capacity from 2,302,673,672 to 1,383,695,112 bytes, but
peak RSS changed from 10,924,756 to 10,924,616 KiB. The unused contiguous
capacity was reserved rather than resident.

Paging increased task-clock by 0.80%, CPU cycles by 0.69%, branch misses by
1.54%, and reported interning time by 2.34%. Both variants emitted the same 50
files and 3,965,280,759 bytes, with byte-for-byte identical per-file hashes.

Full evidence and the two-million-message comparison are in
[the paged-label result](2026-07-16-paged-label-pairs-results.md).

## 2. Fresh replay profile

The post-compact profile sampled a one-million-message real replay containing
38,747,141 accepted datapoints and 5,214,871 interned series. Its selected
non-overlapping self-symbol families were:

| Self-symbol family | Sampled self CPU |
| --- | ---: |
| Allocator routines | ~18.90% |
| Explicit label routines | ~17.69% |
| Two dominant SipHash rows | 7.68% |
| `memcmp` | 5.50% |
| Explicit head routines | ~5.92% |
| All explicit TDigest routines | ~3.14% |
| `record_event_time_skew` directly | 0.09% |

This rejects event-skew statistics as the next target and explains why the two
query-side label experiments below were worth measuring. Full provenance and
profile caveats are in
[the replay profile](2026-07-16-post-compact-replay-profile.md).

## 3. Query-session shared label atoms

`SharedAtoms` interns equal label names and values as session-local `Arc<str>`
atoms. Returned results own their atom references and remain valid after the
session is dropped. Equality, ordering, serialization, and fingerprints remain
content-based. `OwnedStrings` remains the default, and the selected policy is
frozen before the first query, prewarm, or prefetch attempt, including failed
attempts.

The hardened nine-repeat A/B used one frozen binary, full label
materialization, positional reads, a disabled range scalar cache, and two
queries per fresh process (cold then warm). Medians are SharedAtoms relative to
OwnedStrings:

| Query | Cold latency | Warm latency | Process wall | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| Broad full-label selector | 4,752.009 -> 5,966.757 ms (+25.56%) | 4,320.171 -> 4,782.803 ms (+10.71%) | 10.89 -> 12.07 s (+10.84%) | 2,047,060 -> 923,060 KiB (-54.91%) |
| Exact-metric range control | 53.115 -> 56.473 ms (+6.32%) | 21.114 -> 21.206 ms (+0.43%) | 0.10 -> 0.10 s | 35,336 -> 26,200 KiB (-25.85%) |
| Native ExponentialHistogram range | 344.257 -> 347.924 ms (+1.07%) | 269.995 -> 269.066 ms (-0.34%) | 0.63 -> 0.64 s (+1.59%) | 36,700 -> 33,992 KiB (-7.38%) |
| Native Histogram range | 484.738 -> 472.808 ms (-2.46%) | 416.536 -> 415.282 ms (-0.30%) | 0.92 -> 0.92 s | 40,744 -> 36,860 KiB (-9.53%) |
| Scalar count range | 211.778 -> 210.873 ms (-0.43%) | 145.872 -> 142.116 ms (-2.58%) | 0.38 -> 0.37 s (-2.63%) | 42,140 -> 35,248 KiB (-16.36%) |
| No-result control | 1.847 -> 1.833 ms (-0.80%) | 0.040 -> 0.041 ms (+2.57%) | 0.02 -> 0.02 s | 12,300 -> 12,392 KiB (+0.75%) |

The first implementation copied `last_over_time` result labels into owned
strings and then re-interned them at the public boundary. The hardened version
moves the already-shared label object through that operator, preserves its
established series identity, and performs zero warm-pass atom lookups for the
broad and exact-metric selectors.

The corrected broad cold pass still performed 22,134,776 atom lookups:
22,026,622 hits and 108,154 misses, a 99.51% hit rate, while retaining
12,786,137 unique UTF-8 bytes. The retained deduplication is real, but millions
of cold content-hash lookups still outweigh it on the most important
high-cardinality materialization workload. Shared cold latency lost in every
paired repetition; its interquartile delta was +22.57% to +27.35%. Warm noise
was higher, but the paired interquartile delta remained +6.05% to +13.92%.
The verifier also still creates transient `String` values before the facade
moves them into the interner, so this version does not remove all allocation
work.

Raw result:
`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/query-label-storage-ab-20260716-120717`

The frozen final binary SHA-256 was
`e963fa833aabb99e45f9ba2b95e821339d0665600a9a36998e35ac947cc3f500`.
The initial implementation result at `query-label-storage-ab-20260716-114052`
used binary `2df1ab5f69bb22047f1d484d77091ee0b3f114e50cda5fc19624ab5f552ecef0`
and exposed the redundant final-boundary work. A subsequent attempted rerun at
`query-label-storage-ab-20260716-120103` accidentally reused that old binary;
it is excluded rather than presented as post-fix evidence.

## 4. Native typed selective label materialization

Demand propagation is enabled only for root `count` and `group`, using `All`
or `by(...)`, over a direct pure Histogram/ExponentialHistogram selector or a
native `rate()`/`increase()` child. Mixed-kind rows, `without`, `sum`, `avg`,
nested expressions, raw selection, binary/ranking expressions, Summary, and
every uncertain shape request complete labels before execution.

The selective path still decodes and integrity-checks every pair in each
touched row and resolves all referenced symbols. It changes only which verified
label strings become owned query results. Complete source identity is carried
separately for cross-segment merging and rate/increase reset semantics.

Across nine alternating repetitions, all twelve eligible queries improved in
both cold and warm medians. Their geometric-mean change was -3.39% cold and
-4.37% warm:

| Eligible family | Cold median change | Warm median change | Owned pairs saved |
| --- | ---: | ---: | ---: |
| Histogram direct `count`/`group` | -2.11% to -2.93% | -3.62% to -4.27% | 30.04% |
| Histogram `rate`/`increase` `count`/`group` | -3.03% to -4.16% | -3.75% to -4.88% | 30.05% |
| ExponentialHistogram direct `count`/`group` | -1.88% to -1.90% | -3.57% to -3.85% | 31.31% |
| ExponentialHistogram `rate`/`increase` `count`/`group` | -4.00% to -4.97% | -4.13% to -5.98% | 31.31% |

The four full-materialization controls changed by a geometric mean of about
-0.36% cold and -0.99% warm and materialized exactly the same number of pairs
under both policies. Peak-RSS changes for eligible queries ranged from +196 to
-2,088 KiB; these short processes do not establish a material RSS benefit.

Raw result:
`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/native-label-materialization-ab-20260716-113521`

## Correctness gates

Both query experiments passed exact cross-policy checks for:

- semantic and portable result fingerprints;
- result series/sample counts and ordinary `QueryStats`;
- logical/coalesced payload reads and symbol-page counters;
- complete touched-row pair and integrity accounting;
- zero timed footer-validation work;
- before/after corpus inventory equality;
- standalone footer validation; and
- all 38 independent readback cases, with zero skipped and zero mismatched.

The shared-atom raw-v9 counters additionally reconcile every lookup as one hit
or miss, require zero atom activity in owned mode, and require both hits and
misses in stress-query cold passes.

Focused native coverage also constructs one physical row containing both Float
and Histogram chunks. DemandDriven `count` and `group` both take the complete-
label fallback, match Full fingerprints and `QueryStats`, and report zero
selective rows.

## Next direction

Do not combine these results into a broad default switch. The safe next query
experiment is a governed, cheaper identity representation that avoids hashing
the same UTF-8 values for every returned label pair and avoids the verifier's
transient `String` ownership. It must retain the OwnedStrings one-binary
comparator and the same integrity/error precedence. For replay, the fresh
profile still points to reducing label hashing/equality and protobuf ownership;
paging untouched capacity is not an RSS optimization.
