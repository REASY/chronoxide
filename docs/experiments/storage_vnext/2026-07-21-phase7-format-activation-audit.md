# Phase 7 on-disk activation audit

- **Date:** 2026-07-21
- **Baseline revision:** `8c0d666`
- **Status:** No on-disk candidate is activated. All four candidates remain
  deferred behind new evidence.

## Decision

The current evidence supports code- and execution-path work, not another
storage-version change. Query measurements report process-issued byte spans;
they do not establish storage-device traffic or operating-system cache misses.
The accepted runs controlled Linux page-cache residency at process start, but
did not flush device or controller caches. No measured query currently shows a
device-I/O bottleneck that a new byte layout is known to remove.

This audit therefore creates no new format version and changes no reader or
writer semantics. A deferred candidate can be reopened only by a new isolated
measurement that names the residual bottleneck and preserves all storage,
typed-OTLP, corruption, replay, and PromQL guarantees.

## Evidence boundary

The accepted four-million-message Schema 8 corpus contains 8 segments,
17,286,077 chunks, and 154,902,724 samples in 5,569,314,896 bytes.
`chunks.bin` accounts for 3,578,303,589 bytes (64.250%), so chunk layout and
codecs remain credible capacity work. Capacity share alone does not establish
query latency causality.

The Phase 1 stage profile instead attributed the representative warm queries
primarily to symbol resolution, canonical-row decode, identity verification,
and label construction. Payload decode/projection/result processing was 1.7%
of the selective scalar-instant profile and 2.5% of the native Histogram range
profile. The seven-step scalar range spent only 1.2% in PromQL grouping and
evaluation; repeated planning and verification motivated the Phase 4
execution comparator.

Phase 3 confirmed that issued-byte amplification and latency are not
interchangeable. The 16 MiB scalar range cache reduced issued payload bytes by
57.97%, but changed cold latency by +0.66%, warm latency by -0.05%, and peak
RSS by +2.26%. Across the fixed coalescing sweep, smaller gaps reduced bytes
but increased the current payload-batch lookup work. The accepted 4 KiB fixed
gap remains the measured latency/submission endpoint; no adaptive policy or
scalar sidecar was justified.

## Candidate decisions

### Typed scalar/common columns: defer

The existing typed scalar lane already avoids decoding complete native
Histogram, ExponentialHistogram, and Summary values for `_count` and `_sum`
projections. Its remaining process-issued amplification did not dominate
end-to-end latency, and a byte-effective range cache was latency-neutral.

A new sidecar would also require an explicit version boundary and the known
Number Gauge/Sum metadata repair in the same design: source kind,
temporality, monotonicity, start time, flags, reset hints, signed/non-finite
optional sums, and binding each locator to its authoritative native chunk.
There is no current benefit measurement that pays for this semantic and
corruption surface.

Reopen only after code-side payload lookup and Phase 4 one-pass execution are
measured and a scalar query still shows material payload-I/O or decode cost.

### Packed multi-chunk frames: defer; retain as the leading capacity candidate

Every current chunk has a 14-byte single-chunk frame header. The exact corpus
upper bound is therefore:

```text
17,286,077 chunks * 14 bytes = 242,005,078 bytes
```

That is 230.8 MiB, 4.345% of the complete corpus, and 6.763% of `chunks.bin`.
Real packed frames would retain some outer headers, so this is an upper bound,
not a predicted saving.

The bound is locally relevant to the Phase 4 scalar union read: 59,220
selected chunks carry 829,080 bytes of current frame headers, equal to 76.17%
of the 1,088,410-byte difference between logical used bytes and the three
coalesced issued spans. It does not explain the broad-selector amplification:
90,683 selected frame headers are 1,269,562 bytes, only 2.94% of that query's
43,144,099-byte issued-minus-used gap. Packing therefore has plausible
capacity and sealing value, but no demonstrated general query-latency value.

Reopen, after Phase 6 codec evidence, only as an isolated capacity/seal
experiment. It must retain direct per-chunk locators, bounded individual
reads, per-chunk integrity, outer-frame integrity, deterministic construction,
and separate capacity, seal-throughput, scan, and random-query results.

### Compact routing: defer

Current profiles do not isolate routing size, lookup CPU, or cache misses as a
material residual bottleneck. Exact matcher verification remains authoritative
and touched malformed routing metadata must remain corruption, never a miss or
pruning decision. No no-false-negative compact comparator is activated without
an authenticated capacity inventory and a fresh routing-specific profile.

### Adjacent-segment packing: defer and keep last

The reference corpus has only eight segments. Phase 1 observed 40--44 peak
open files, metadata peak retained charge below the 64 MiB budget, zero warm
metadata reads, and no cache evictions or governor/FD refusals. There is no
measured manifest, FD, cache, or time-pruning pressure that justifies expanding
retention, recovery, compaction, and deterministic replay semantics.

## Exit gate

Phase 7 is complete as an activation audit, not as a format implementation.
The decision is deliberately **defer**, not **reject forever**:

1. finish the execution, allocator/head, and codec gates;
2. profile the resulting defaults again on a real replay/query corpus;
3. name a remaining byte-layout bottleneck with device or end-to-end evidence;
4. design exactly one comparator and update `storage.md` before changing bytes;
5. require golden bytes, round trips, corruption/sticky-error tests,
   deterministic replay, independent readbacks, and a controlled real-corpus
   A/B.

Until those conditions hold, another on-disk version would add complexity
without evidence that it improves the workload users actually observe.
