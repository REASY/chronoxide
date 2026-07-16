# Owned single-sample head transfer result

- **Date:** 2026-07-16
- **Status:** Promoted as a small code-only ingest optimization.
- **Raw runs:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/owned-single-sample-ab-20260716-183836`
- **Control binary SHA-256:**
  `c43ecf7b756d66e625195ae1f5f6f8d1d4878eb227c6a30634dea8964793d531`
- **Candidate binary SHA-256:**
  `d8b0f102900569c3999ade924b2778600921de8fdafe8546bd86eee8225b4d36`

## Change

`HeadBuffer::record_sample` already owned its `SampleValue`, but routed it
through the borrowed batch API. `push_sample_to_window` therefore cloned the
value before encoding it. That clone is cheap for Number samples, but it
deep-copies the bucket/bound/quantile vectors in Histogram,
ExponentialHistogram, and Summary values.

The single-sample API now transfers ownership through a shared iterator-based
inner path and into `EncodedSeries::push_sample`. The public borrowed
`record_samples` API retains its original contract and clones each input once
at that boundary. Timestamp validation, out-of-order routing, rotation, type
checking, accepted timestamp updates, datapoint accounting, and selector-cache
invalidation retain their original order.

The one-million-message corpus contains 3,342,636 accepted typed datapoints:

- 993,620 Histogram datapoints;
- 2,310,956 ExponentialHistogram datapoints; and
- 38,060 Summary datapoints.

The change therefore removes one redundant deep `SampleValue` clone for each
of those datapoints in the shared Kafka/capture-replay ingest path. It does not
change the on-disk format.

## Profile basis

The fresh post-SymbolId profile contained 7,060 samples with zero lost
samples. Ingest ended at 84.902 seconds, sealing at 138.752 seconds, and report
construction at 143.442 seconds. Allocator self-time was split between ingest
(about 18.0 seconds) and sealing (about 11.6 seconds), so this change is not
claimed to address the complete allocator family. It removes one concrete
ingest-only redundant ownership step.

## Measurement

The first screen replayed 250,000 messages in C-M-M-C order with two preserved
release binaries. Every run started after `POSIX_FADV_DONTNEED`; `perf stat`
recorded task-clock, cycles, instructions, branches, faults, and context
switches. The host was noisy: one candidate seal took 43.03 seconds while the
other three comparable seals took 35.14-36.36 seconds. Consequently wall,
task-clock, and cycle differences from this block are not usable as a latency
estimate.

The stable counter was retired instructions:

| 250k position | Control | Candidate |
| --- | ---: | ---: |
| First position | 801,593,769,601 | 800,834,012,453 |
| Second position | 801,470,786,872 | 800,819,793,333 |
| Mean | 801,532,278,237 | 800,826,902,893 |
| Mean change | - | **-0.0880%** |

Both candidate positions retired fewer instructions than both controls, and
the two candidate counts differed by only 0.0018%. A separate one-million-
message sanity run reproduced the direction:

| Metric | Prior control | Owned transfer | Change |
| --- | ---: | ---: | ---: |
| Instructions | 1.734944 T | 1.731930 T | **-0.1737%** |
| Branches | 310.859 B | 310.442 B | -0.1342% |
| Task-clock | 145.047 s | 145.716 s | +0.4612% |
| Cycles | 808.627 B | 812.151 B | +0.4358% |
| Peak RSS | 8,671,540 KiB | 8,673,448 KiB | +0.0220% |

The task-clock/cycle movement is within host-frequency and workload noise and
is opposite the instruction result, so no end-to-end latency claim is made.
This is a small, mechanistically verified CPU-work reduction.

## Correctness gates

The permanent differential test compares the owned single-sample and borrowed
batch paths across Float, Int64, Histogram, ExponentialHistogram, and Summary,
including non-finite signed sums, typed metadata, duplicate timestamps,
out-of-order routing, and window rotation. Existing focused tests retain
coverage for rejected timestamps, accepted batch prefixes, type mismatch,
last-timestamp atomicity, compact numeric promotion, live-head queries, and
typed codec round trips.

All four 250,000-message runs produced byte-identical 34-file,
972,976,604-byte segment trees. Their complete per-file hash manifests match.
The one-million-message candidate produced the exact prior 34-file,
1,584,337,371-byte tree with manifest digest
`c57bd2970b615958820edced252694180bede6d57ab898d4e864cefff5b70bfd`.

The 250,000-message corpus makes the independent oracle select two Histogram
sum `rate`/`increase` cases whose expected and actual values differ only in the
last floating-point bit. The unchanged control reports the exact same two
mismatches. The clean one-million-message gate executed all 38 expected
readbacks with zero skips and zero mismatches, with full footer validation.

## Decision

Keep the owned transfer. It is a small improvement rather than a major replay
optimization, but it removes provably redundant deep clones, preserves the
borrowed batch API and exact output, and reduces instructions consistently.
The next larger ingest experiment should target the profiled per-series
last-timestamp hash lookup.
