# Owned typed-bucket transfer results

Date: 2026-07-16

## Decision

Keep the change. Moving decoder-owned Histogram and ExponentialHistogram bucket
vectors into the head removes real work, preserves the exact segment bytes, and
has no material RSS cost. The effect is deliberately small: this is an
allocation/copy cleanup, not a large replay-latency optimization.

Summary quantiles remain on the borrowed conversion path. Prost and the
internal representation use different element types, so their vector
allocation cannot be transferred safely.

## Change

The live OTLP processor now consumes its decoded request. After event-time
acceptance and successful label-set interning, it moves these allocations into
the typed sample instead of cloning them:

- Histogram `explicit_bounds` and `bucket_counts`;
- ExponentialHistogram positive and negative `bucket_counts`.

Rejected datapoints are not mutated. Borrowed conversion helpers remain for WAL
recovery and other borrowed callers.

The million-message corpus accepted 993,620 Histogram and 2,310,956
ExponentialHistogram datapoints. The change can therefore eliminate up to
6,609,152 vector allocation/copy operations on this workload. Empty or absent
bucket vectors make the actual count lower.

## Method

- Capture:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001/partition-1.capture`
- Raw result root:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/owned-typed-buckets-ab-20260716-194731`
- Control: commit `71c575e`, binary SHA-256
  `f0cb9f85ac6e2a75174fb0af3c278708edd76eb704f2cbfed6d583a687f74883`
- Candidate binary SHA-256:
  `c95231f80d209a27017fae009bdb6a20e4f4341189d73a5fdd5a5aec2c59b3fd`
- Both variants used the same release profile, schema 8 configuration, capture,
  stop count, deterministic segment-ID seed, explicit cache eviction, and
  `perf stat` event set.
- The 250k-message schedule was control, candidate, candidate, control.
- The confirmation schedule was one control and one candidate run at one
  million messages.

No build, profiler, footer validation, or other measured process overlapped the
replay runs. The host remained subject to unrelated background noise, so stable
instruction counts carry more weight than the short-run wall time.

## Results

### 250k messages, mean of two runs per variant

| Metric | Candidate versus control |
| --- | ---: |
| Wall time | +0.972% |
| Task clock | +0.922% |
| Cycles | +0.519% |
| Instructions | **-0.165%** |
| Branches | **-0.242%** |
| Peak RSS | -842 KiB (-0.015%) |

The short wall-time and cycle result is noise in conflict with the stable work
counters. It is not evidence of a latency regression.

### One million messages

| Metric | Control | Candidate | Difference |
| --- | ---: | ---: | ---: |
| Wall time | 146.73 s | 146.38 s | **-0.239%** |
| Task clock | 146,803.90 ms | 146,435.33 ms | **-0.251%** |
| Cycles | 816,479,168,914 | 814,990,508,627 | **-0.182%** |
| Instructions | 1,730,445,294,577 | 1,728,800,377,512 | **-0.095%** |
| Branches | 310,314,412,687 | 309,876,351,536 | **-0.141%** |
| Peak RSS | 8,573,780 KiB | 8,573,020 KiB | -760 KiB (-0.009%) |

The longer run confirms a small reduction in both executed work and elapsed
time. RSS is effectively unchanged because the same bucket allocations remain
live in the head; only their duplicate allocation and copy are removed.

## Correctness gates

- Every 250k control and candidate manifest was byte-identical.
- The one-million control and candidate manifests were byte-identical, with
  digest `c57bd2970b615958820edced252694180bede6d57ab898d4e864cefff5b70bfd`.
- The candidate recorded the same 38,680,023 samples, 5,214,871 series, typed
  datapoint counts, and event-time rejection counts as the control.
- Candidate footer validation was requested and effective for all four
  segments.
- Independent readback verification executed 38 queries with zero skips and
  zero mismatches.
- Focused tests prove allocation pointer/length/capacity transfer and prove
  that accepted bucket storage is consumed while old, future, and missing-time
  datapoints remain intact.
- Processor typed-semantics tests and all 12 WAL replay tests pass.
