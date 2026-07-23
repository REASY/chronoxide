# Phase 4 one-pass range-query results

## Decision

The diagnostic `one-pass-assume-scalar` executor is worth finishing, but it is
not safe to promote yet. The admitted real-corpus comparison found large,
repeatable latency reductions with exact result equivalence. The final gate
therefore records `candidate_disposition: defer` and
`production_promotion_verdict: forbidden` until the allocation, finite-limit,
statistics, and dense-long-range contracts are closed.

Raw evidence is preserved at:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase4-range-one-pass-formal-20260722T163309Z`

The canonical decision is `comparisons/result-gate.json`. The artifact was
produced from source revision
`e87cbf9e465ded639cf344d80e2cb0476eb37250`; the preserved query binary SHA-256
is `1481c9659279d6899b3ba9f2371e3a419abbdaff8b5d824f7621e669307525b1`.

## Admission and correctness

- The frozen final verifier passed independently.
- All 66 held-root guardian lifecycles completed conflict-free with zero
  identity or handshake violations. The worst edge-inclusive cadence gap was
  100.70 ms against the fixed 200 ms bound.
- The corpus was unchanged: 66 files, 5,569,314,896 bytes, canonical inventory
  digest `28547c0fc2b738eb58948400602640c017844cd57bd49917bffdf100a6e14a0b`.
- All 64 pre-query eviction observations reported zero resident corpus bytes.
- Independent readbacks executed 32 of 32 cases with zero skips or mismatches;
  both Phase 4 multi-step cases executed.
- Exact and portable fingerprints, result order, and the two-series/eight-sample
  result shape matched for every arm and repetition.
- Forty-eight ordinary `QueryStats` fields were equal. All 132 differences
  were declared union-work-versus-repeated-work accounting differences; there
  was no unexplained drift.

The matrix used four counterbalanced ABBA/BAAB blocks, eight fresh processes
per arm and query, and one cold plus two warm evaluations per process. The warm
observation unit is each process's two-run median. `cold` means the first
expression in a fresh query session after corpus page eviction; it is not a
cold device/controller-cache claim.

## Performance

| Query | Repeated cold -> one-pass | Repeated warm -> one-pass | Median process RSS |
| --- | ---: | ---: | ---: |
| `sum(rate())`, 30m dense | 2.801 s -> 0.991 s (64.61% faster) | 2.610 s -> 0.828 s (68.28% faster) | 105.30 -> 113.35 MiB (7.65% higher) |
| `count(rate())`, 30m dense | 7.879 s -> 0.973 s (87.65% faster) | 7.629 s -> 0.811 s (89.37% faster) | 123.44 -> 113.46 MiB (8.08% lower) |
| `sum(rate())`, 6h sparse control | 2.801 s -> 1.034 s (63.07% faster) | 2.604 s -> 0.872 s (66.52% faster) | 105.12 -> 113.33 MiB (7.81% higher) |
| `sum(rate())`, 24h sparse control | 2.853 s -> 1.233 s (56.79% faster) | 2.659 s -> 1.071 s (59.71% faster) | 104.83 -> 113.42 MiB (8.20% higher) |

Every candidate process beat every repeated-executor process for both cold
latency and per-process warm median at all four query coordinates. The gate did
not predeclare a confidence interval or hypothesis test, so this is strong
descriptive evidence rather than a formal inferential-confidence claim. The
6-hour and 24-hour cases are sparse scheduler controls because the corpus has
only 1.25 hours of dense event-time coverage.

At every query/run coordinate, union execution reduced logical payload-used
bytes from 17,790,188 to 7,237,340 (59.32%), coalesced process-issued bytes from
22,033,043 to 8,325,750 (62.21%), and process-issued spans from 104 to 3
(97.12%). Read/used amplification fell from 1.23849x to 1.15039x. These are
process-issued file spans, not storage-device traffic or operating-system
cache misses.

## Why the default remains repeated execution

The diagnostic intentionally cannot authorize promotion:

1. union-result preallocation is estimated after decode rather than reserved
   through the query governor before allocation;
2. finite `QueryLimits` and their error precedence are not exercised;
3. public `QueryStats` describe union work rather than repeated per-step work;
4. the pinned corpus contains no dense 24-hour event-time range.

The next Phase 4 gate should govern the retained union representation before
allocation, specify finite-limit and public-statistics semantics, add focused
error-precedence coverage, and repeat the sealed comparison on a fingerprinted
corpus with at least 24 dense event-time hours. Until then, the optimized mode
remains an explicit diagnostic comparator and unsupported shapes continue to
use repeated execution.
