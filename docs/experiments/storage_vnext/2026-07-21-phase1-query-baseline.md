# Phase 1 current-source Schema 8 query baseline

- **Measured:** 2026-07-21, final run started at 14:37 `+08:00`
- **Query binary SHA-256:**
  `52360c7b51253bd5bacbd6e1d94251c505e837a6c262a39b7aea0a0548819eb7`
- **Corpus:** 66 files, 5,569,314,896 bytes, manifest SHA-256
  `8b0789e2f6c404a144e0d2e87f152a83e9f0bedb9c5ab2c6512608056cae3289`
- **Query corpus fingerprint:**
  `7e5cf252e5df9bdb786e1b9deb9248f09667962ac559f339ba47312c5c0e3ca3`
- **Fixed query-manifest SHA-256:**
  `7da7c63e8044cc19f5b49a87890200b042527094020a26e72ffb3d3173526b8f`
- **Raw result:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase1-query-final-20260721T063715Z`
- **Status:** complete and accepted

The strict gate passed 204 fresh processes and 612 query evaluations. Exact
and portable fingerprints, result cardinality, public `QueryStats`, Full-
demand controls, and cache-on/off semantics matched. Footer validation passed
outside timing, and the independent readback oracle executed 38/38 cases with
zero skips or mismatches. The complete corpus inventory was byte-identical
before and after the schedule.

## Contract correction and discarded runs

The first complete measurement correctly failed publication because its
manifest called virtual Histogram `_count` projection and nested
`histogram_quantile(sum(rate(...)))` rows selective. The implementation was
right: virtual projections and nested p95 plans require full label ownership.
The corrected manifest:

- uses physical Float/Int64 `container_cpu_usage_seconds_total` for real
  scalar selective instant/range coverage;
- retains virtual Histogram `_count` as a separate full-demand typed-scalar
  and range-cache control; and
- classifies both nested p95 expressions as full-demand.

The invalid complete artifact at
`storage-vnext-phase1-query-20260721T060157Z` and two corrected partial
artifacts at `storage-vnext-phase1-query-corrected-20260721T061605Z` and
`storage-vnext-phase1-query-clean-20260721T062453Z` remain preserved. The
partials were fail-closed interruptions caused by unrelated `art-tracer-core`
build/test processes. No bytes or timings from those directories contribute to
this report. The final run began only after 100 continuous seconds without a
build, Rust compiler, profiler, or Chronoxide process and encountered no
conflict.

## Measurement state

Every matrix row used three four-process blocks:

```text
off, detailed, detailed, off
detailed, off, off, detailed
off, detailed, detailed, off
```

Each process created a fresh query session and executed one CLI-cold run then
two warm runs. There are six Off cold observations and twelve Off warm
observations per query. Off is the latency baseline; it performs no stage
clock reads. Detailed is deliberately observer-heavy and is used only for
stage attribution.

Before every fresh process, all 66 corpus files received
`POSIX_FADV_DONTNEED`; `fincore` reported exactly zero resident bytes in all
204 cases. Store startup and corpus-fingerprint work may touch pages before
the timed query, so “cold” here means the first expression in a fresh process
and session, with proven zero process-start residency. It does not mean a
flushed NVMe/controller cache or prove zero residency at the exact timed-query
boundary.

All runs used Schema 8, `OwnedStrings`, pread, queue depth 128, no prewarm, no
prefetch, a 64 MiB retained metadata budget, and fixed query limits. Range-
cache budgets are explicit per row; zero means disabled. RSS is the maximum
for the entire three-evaluation process, so it is not a cold-versus-warm RSS
split. Post-query semantic fingerprinting is separately timed and excluded
from query wall time. The CLI does not measure HTTP JSON serialization.

## Off latency and RSS baseline

| Query | Cold median (min–max) | Warm median (min–max) | Median peak RSS | Result series/samples |
| --- | ---: | ---: | ---: | ---: |
| Broad raw `_count` regex | 4,626.6 ms (4,480.1–4,805.6) | 4,176.2 ms (4,016.7–4,298.2) | 2,007.4 MiB | 90,569 / 119,782 |
| Equality `last_over_time` | 33.0 ms (32.8–33.2) | 7.38 ms (7.24–7.60) | 22.4 MiB | 766 / 766 |
| Sparse regex `last_over_time` | 101.9 ms (100.6–104.7) | 60.5 ms (59.8–64.3) | 43.8 MiB | 1,449 / 1,449 |
| Negative matcher `last_over_time` | 48.8 ms (48.0–50.5) | 20.1 ms (19.1–21.6) | 35.2 MiB | 1,647 / 1,647 |
| No result | 1.81 ms (1.79–1.86) | 0.035 ms (0.029–0.049) | 14.4 MiB | 0 / 0 |
| Real scalar rate/sum instant, selective | 914.4 ms (903.3–937.9) | 747.9 ms (738.8–778.2) | 115.3 MiB | 2 / 2 |
| Real scalar rate/sum range, selective | 2,864.9 ms (2,745.6–2,992.2) | 2,667.4 ms (2,534.3–2,831.3) | 112.6 MiB | 2 / 8 |
| Virtual `_count` range, cache off | 204.6 ms (199.9–207.0) | 142.5 ms (136.4–145.1) | 43.5 MiB | 10 / 38 |
| Virtual `_count` range, 16 MiB cache | 205.9 ms (202.5–211.5) | 142.5 ms (135.6–148.3) | 44.4 MiB | 10 / 38 |
| Native Histogram count range, selective | 441.5 ms (433.2–450.9) | 380.1 ms (370.8–391.2) | 42.2 MiB | 10 / 38 |
| Native Histogram p95 range, full | 466.2 ms (447.6–473.1) | 404.9 ms (385.5–409.4) | 42.0 MiB | 10 / 38 |
| Native ExponentialHistogram count range, selective | 329.7 ms (317.7–331.8) | 256.4 ms (244.9–258.4) | 36.1 MiB | 55 / 198 |
| Native ExponentialHistogram p95 range, full | 340.0 ms (331.1–345.8) | 267.6 ms (257.2–275.1) | 37.8 MiB | 55 / 198 |
| Real scalar rate/sum instant, Full | 1,130.2 ms (1,079.8–1,145.4) | 939.9 ms (894.4–950.6) | 285.8 MiB | 2 / 2 |
| Real scalar rate/sum range, Full | 3,333.7 ms (3,267.4–3,496.5) | 3,127.4 ms (3,059.4–3,313.9) | 284.2 MiB | 2 / 8 |
| Native Histogram count range, Full | 457.2 ms (444.4–464.3) | 398.8 ms (386.0–405.5) | 42.9 MiB | 10 / 38 |
| Native ExponentialHistogram count range, Full | 333.1 ms (330.7–344.7) | 258.7 ms (256.9–270.7) | 38.1 MiB | 55 / 198 |

The six-observation cold intervals and twelve-observation warm intervals are
descriptive ranges, not confidence intervals.

## Demand-driven label result

All Full-versus-demand comparisons retained identical complete row/pair
integrity checks, semantic fingerprints, result values, and `QueryStats`.

| Comparator | Cold latency | Warm latency | Peak RSS | Owned pairs omitted |
| --- | ---: | ---: | ---: | ---: |
| Real scalar instant selective vs Full | -19.10% | -20.43% | -59.65% | 1,446,604 / 1,576,708 |
| Real scalar range selective vs Full | -14.06% | -14.71% | -60.39% | 5,124,698 / 5,585,312 |
| Native Histogram count selective vs Full | -3.43% | -4.71% | -1.55% | 222,253 / 739,731 |
| Native ExponentialHistogram count selective vs Full | -1.01% | -0.91% | -5.19% | 171,872 / 548,922 |

The real scalar instant path materialized 130,104 pairs and 5,503,602 string
content bytes instead of 1,576,708 pairs and 75,653,351 bytes. The range path
materialized 460,614 pairs and 19,484,541 bytes instead of 5,585,312 pairs and
268,051,978 bytes. Native count rows include the specified full fallback for
rows outside the pure eligible family; the counters reconcile exactly.

This closes the previous real-corpus scalar-selective evidence gap and confirms
that repeated label ownership is a first-order CPU and RSS cost. It does not
relax full canonical-row decoding, all-symbol resolution, stored identity
verification, or corruption precedence.

## Detailed stage profiles

Absolute Detailed latency is not comparable with Off. The table shows median
stage share of Detailed wall time for representative rows; only the largest
leaves are listed.

| Query/state | Principal Detailed leaves |
| --- | --- |
| Broad cold | symbol resolution 55.3%; label construction 15.2%; canonical row decode 13.8%; canonical identity 6.3%; payload decode/projection/result processing 4.2% |
| Broad warm | symbol resolution 57.9%; row decode 13.9%; labels 13.2%; identity 6.7%; metadata visit 2.5%; payload decode/projection/result processing 2.5% |
| Sparse regex cold | candidate selection 44.7%; symbol resolution 31.0%; row decode 9.3%; labels 5.5%; payload decode/projection/result processing 3.8% |
| Negative matcher warm | symbol resolution 54.3%; row decode 16.0%; labels 11.4%; identity 6.5%; payload decode/projection/result processing 5.8% |
| Real scalar instant warm, selective | symbol resolution 59.7%; row decode 15.9%; identity 9.2%; labels 7.9%; source merge 1.8%; payload decode/projection/result processing 1.7% |
| Real scalar range warm, selective | symbol resolution 61.3%; row decode 16.1%; identity 9.3%; labels 8.0%; metadata visit 1.5%; PromQL grouping/evaluation 1.2% |
| Native Histogram count warm | symbol resolution 57.5%; row decode 16.8%; labels 10.3%; identity 7.4%; PromQL 3.3%; payload decode/projection/result processing 2.5% |
| Native ExponentialHistogram count warm | symbol resolution 62.3%; row decode 16.1%; labels 10.9%; identity 8.0%; metadata visit 1.5% |
| No-result cold | symbol lookup 96.0% of a 1.79 ms Detailed run |

The median unclassified remainder was below 0.3% for these material queries.
Candidate selection is material for the sparse regex control, but it is not a
broad-query bottleneck. Warm symbol resolution remains dominant after metadata
reads fall to zero, proving that this is CPU/traversal work rather than cold
disk I/O. The 30-minute scalar range executes seven evaluation steps; its
Detailed profile spends roughly 95% in repeated symbol/row/identity/label and
metadata work rather than payload reads or PromQL arithmetic. This is direct
evidence for the one-pass range experiment.

## Metadata cache and governor

All fresh processes began with approximately 1.94 MiB of retained roots and
ledger state. Representative cold/warm medians were:

| Query | Cold metadata misses / issued bytes | Warm misses / issued bytes | End retained | Peak in flight |
| --- | ---: | ---: | ---: | ---: |
| Broad selector | 2,114 / 45,558,318 | 0 / 0 | 49,031,194 B | 1,972,947 B |
| Real scalar range | 1,602 / 42,244,651 | 0 / 0 | 44,885,309 B | 1,115,955 B |
| Native Histogram range | 432 / 10,962,853 | 0 / 0 | 13,035,838 B | 789,324 B |
| Native ExponentialHistogram range | 539 / 15,324,356 | 0 / 0 | 17,411,992 B | 789,324 B |

There were no cache evictions, failed loads, retained/in-flight refusals, FD
capacity refusals, or corruption detections. Peak retained charge stayed below
the 64 MiB budget, and peak open files stayed between 40 and 44. Warm runs had
zero metadata reads. The cache/governor is behaving as intended and is not the
current broad-query limiter.

## Payload I/O and coalescing evidence

| Shape | Logical used | Physical reads | Physical bytes | Read/used amplification |
| --- | ---: | ---: | ---: | ---: |
| Broad raw selector | 10,115,253 B | 241 | 53,259,352 B | 5.265x |
| Equality | 389,688 B | 59 | 411,936 B | 1.057x |
| Sparse regex | 252,871 B | 4 | 798,921 B | 3.159x |
| Negative matcher | 641,797 B | 2 | 2,682,596 B | 4.180x |
| Real scalar instant | 7,202,558 B | 3 | 8,325,750 B | 1.156x |
| Real scalar range | 17,790,188 B | 104 | 22,033,043 B | 1.238x |
| Virtual `_count` range, cache off | 2,965,970 B | 71 | 11,534,193 B | 3.889x |
| Virtual `_count` range, 16 MiB cache | 2,965,970 B | 98 | 4,847,555 B | 1.634x |
| Native Histogram range | 11,838,144 B | 29 | 12,056,074 B | 1.018x |
| Native ExponentialHistogram range | 1,288,071 B | 35 | 1,473,772 B | 1.144x |

The 16 MiB range cache admitted 3,390 entries, produced 5,252 hits, retained a
peak 16,777,184 bytes, and finalized at zero charge. It reduced issued payload
bytes by 57.97% but increased physical spans from 71 to 98. Off cold latency
changed +0.66%, warm latency changed -0.05%, and RSS rose 2.26%. The cache is
therefore semantically sound and byte-effective but end-to-end neutral for
this 30-minute control. This is also a warning against inferring latency from
issued-byte reduction alone.

The fixed 4 KiB coalescing gap produces material amplification on broad,
negative, sparse, and virtual-scalar shapes, but Detailed payload-read time is
small for the main CPU-bound queries. The Phase 3 gap/backend sweep remains
warranted; a new sidecar or default change is not warranted from amplification
alone.

## Baseline decisions

Phase 1 query baseline is accepted. It supports the following next gates:

1. **Compact query-local IDs:** proceed as a code-only comparator. Broad output
   peaks near 2 GiB and owns 415,320,441 bytes of label content; label and
   downstream ownership are material. The comparator must not claim it can
   remove mandatory all-symbol resolution or complete identity verification.
2. **Adaptive coalescing:** run the fixed `0/256/1024/4096` gap sweep for pread
   and io_uring independently. Promote only an end-to-end Pareto improvement.
3. **One-pass range execution:** this has the strongest current query CPU
   evidence. Repeated storage verification/planning dominates the scalar and
   native range profiles; payload I/O and PromQL arithmetic do not.
4. **Another postings codec:** remain deferred. Candidate selection is large
   only in the sparse regex control and Schema 8 postings bytes are already
   compact.
5. **Conditional on-disk work:** remain inactive. The current evidence points
   first to execution ownership and repeated range planning, not an
   undecodable or oversized metadata layout.
