# Live-query ingestion D/P/Q screen

**Date:** 2026-07-25

**Status:** correctness screen passed; performance and default-enablement
acceptance failed

**Follow-up:** the first optimization, an exact final empty-head publication
shortcut, passed its focused counterbalanced A/B, full 50k D/P/Q,
provenance-bound exact-prefix oracle, noisy 125k smoke, and mandatory
quiet-host 250k scale gate. See
[Live-query final empty-head shutdown fast path](2026-07-25-live-empty-head-shutdown-results.md).
A later one-active-partition owner-validation shortcut passed its noisy 250k
A-B-B-A screen, but still requires its own naturally quiet 250k candidate gate;
see
[Live at-most-one-active-partition owner-validation fast path](2026-07-25-live-owner-validation-fast-path-results.md).
Neither result by itself changes this screen's production-readiness decision.

## Decision

The versioned immutable query-view synchronization model is sound enough to
continue: requests pin a coherent generation, the publisher swaps roots
quickly, and the live and sealed storage paths produced the same selected
result.

The current generation builder is not ready for production. At only 50,000
captured messages, enabling publication reduced replay throughput by 92.63%,
raised peak process-tree RSS from 0.90 GiB to 4.18 GiB, and produced a 4.54 s
median publication pause. Queries did not materially extend the already
publication-bound wall time in this particular lean screen, but they doubled
CPU consumption, added 0.46 GiB peak RSS, and took about 0.6 s at p50.

Do not run or interpret the 4,000,000-message experiment yet. The empty-head
shutdown fast path passed its quiet-host 250k gate, and incremental live roots,
broader admission accounting, and the one-active-partition owner shortcut have
landed. The owner shortcut's distinct naturally quiet 250k candidate gate
remains the prerequisite before attempting 4M.

## What the three arms isolate

All arms replay the same ordered capture prefix with the same writer
configuration and frozen release binaries into fresh output roots:

| Arm | Live publication | HTTP queries | What the comparison measures |
|---|---:|---:|---|
| D | off | none | existing ingest and seal baseline |
| P | on | none | D → P: end-to-end cost of enabling and publishing live views |
| Q | on | continuous | P → Q: incremental interference from concurrent readers |

The intended formal experiment uses three cyclic orders (`D,P,Q`, `P,Q,D`,
and `Q,D,P`) so every arm occupies every position. It evicts the capture pages
between arms, fingerprints all inputs and binaries, uses disjoint validated
CPU sets for ingestion and clients, and runs no build, verifier, profiler, or
other measured workload concurrently. Q should use a recorded open-loop
offered rate for latency/capacity claims so overload is visible instead of
hidden by coordinated omission. The lean closed-loop pair remains useful as a
correctness screen, not as the capacity workload.

This screen ran only `Q,D,P`, did not evict the capture, and deliberately
relaxed maximum view staleness from the normal policy to 600 s so slow
publication could be observed instead of immediately becoming a stream of
`503` responses. It was not replicated, Q began near the configured I/O-PSI
limit, and live DEBUG observation is included in P/Q cost. Its performance
numbers are diagnostic, not formal acceptance evidence.

## What is measured

### Ingestion and host cost

- elapsed time and messages/s;
- user/system/total CPU time and CPU/wall utilization;
- cycles, instructions, IPC, cache misses, page faults, context switches, and
  CPU migrations;
- process-tree peak RSS, anonymous/file RSS, swap, and process count; and
- exact replay counters and immutable output size/hash.

### Publication and synchronization

- the message cut, catalog revision, manifest offset, and generation of every
  successful publication;
- ingestion pause and total publication duration;
- freeze/admission, seal, inventory, coverage, sample-root, catalog,
  owner/head, root-build, commit, old-root-drop, and post-commit durations;
- root-lock wait/held time for publisher and reader;
- sample keys/fragments, active catalog rows, retained payload, and the
  reported/estimated memory classes; peak generation-construction scratch is
  not yet measured or bounded comprehensively; and
- publication failures, admission failures, stale-view responses, and
  shutdown time.

### Query behavior

- client, server evaluation, queue, and serialization latency at
  p50/p95/p99/max;
- achieved closed-loop request rate and error/timeout counts;
- view generation, visible message sequence, revision, view age, and root-pin
  time;
- complete `QueryStats`, including matched/projected series, chunks, decoded
  samples, and index work; and
- logical payload-used bytes, coalesced payload-read bytes, physical reads,
  and read/used amplification.

### Correctness

- exact replay-counter equality and byte-identical D/P/Q segment trees;
- exhaustive Schema 8 footer, bounds, checksum, and exact-postings validation;
- independent readback-oracle execution with zero skips and mismatches;
- non-empty head-only queries and same-generation response consistency; and
- an exact-message-prefix replay that compares one live-head HTTP response
  with the same prefix after it is sealed.

## Completed 50,000-message screen

Evidence root:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-ingest-screen-lean-20260725T110340
```

The source was the first 50,000 messages of the fingerprinted 20,589,025,986 B
capture. Ingestion ran on CPUs `4-15,20-31`; the two-request closed-loop client
ran on `0-3,16-19`. The client issued `service-last` and a guaranteed-empty
exact-matcher control in parallel, then waited 500 ms.

| Arm | Elapsed | Throughput | CPU time | CPU/wall | Peak tree RSS |
|---|---:|---:|---:|---:|---:|
| D | 10.91 s | 4,582.95 msg/s | 9.97 s | 91.38% | 0.90 GiB |
| P | 148.05 s | 337.72 msg/s | 148.24 s | 100.13% | 4.18 GiB |
| Q | 148.11 s | 337.59 msg/s | 304.32 s | 205.47% | 4.64 GiB |

### D → P: live enablement is presently dominant

Compared with D, P added 137.14 s elapsed time, lost 92.63% throughput, added
3,444,608 KiB peak RSS, and executed 840.00% more instructions.

Normal message-boundary publications were already expensive. Excluding the
initial publication and shutdown, P's 12 boundaries had a 4.565 s median
duration and a 3.946–4.894 s range. Q's 13 comparable boundaries had a
4.462 s median and a 4.262–4.848 s range.

P's final shutdown publication took 76.344 s: 25.693 s sealing, 2.058 s
building the sample root, 45.222 s building the catalog, and 3.365 s
post-commit. Q's shutdown was similar at 71.350 s, including 26.011 s sealing,
2.124 s for the sample root, and 43.155 s for the catalog. The p95/p99 values
in the raw 14/15-publication summaries equal these shutdown maxima because
the sample is small; they are not steady-state percentiles.

The pointer synchronization itself is cheap. Q's publisher commit lock was
held for at most 1.27 µs, while query root-pin wait was 0.06 µs at p50,
0.19 µs at p95, and 0.75 µs at maximum. Full-generation construction happens
outside the root lock, but it is synchronous with ingestion and therefore
still pauses message processing. Replacing the pointer lock would not address
the measured bottleneck.

### P → Q: Q did not measurably extend wall time in this screen

Against P, Q changed elapsed time by only +0.06 s (+0.04%) and throughput by
-0.04%. That does **not** mean querying is free:

- CPU time increased by 156.08 s (+105.29%);
- instructions increased by 57.98%;
- cache misses increased by 190.92%;
- context switches increased by 3,283.68%; and
- peak process-tree RSS increased by 483,900 KiB (+11.04%).

The lean client completed 270 requests at 1.83 requests/s:

| Measure | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| client latency | 587.0 ms | 1,049.1 ms | 2,025.8 ms | 4,678.3 ms |
| server query time | 586.0 ms | 1,045.5 ms | 1,284.1 ms | 2,016.6 ms |
| view age | 4.58 s | 63.84 s | 71.04 s | 72.57 s |
| queue time | 0 | 1 µs | 1 µs | 2 µs |

`service-last` took 602.9/1,049.6 ms at p50/p95. The guaranteed-empty control
still took 579.2/1,025.3 ms in server time despite matching no series,
decoding no samples, and issuing no payload I/O. This rules out the
instrumented chunk-decode and storage-I/O work as the dominant cost, but the
current counters do not distinguish catalog resolution, parsing, evaluator
setup, or other pre-decode work. A sampling profile is required before
changing it.

Aggregate `QueryStats` recorded only two segment queries across 270 requests.
Across the useful query, logical payload use was 28,428 B versus 35,296 B of
coalesced reads (1.242× amplification, 30 physical reads). This screen
primarily exercises the head path and is not a sealed-query storage benchmark.

The 63.84 s p95 view age is unacceptable. With the normal approximately
10-second stale-view policy, many of these requests would have been rejected
as unavailable. The 600-second setting was an observation aid, not a proposed
production setting.

### Memory admission does not represent resident cost

At P's peak, the observer charged about 296 MB of live memory while the
process tree held 4.18 GiB RSS. It separately estimated about 2.04 GB for an
unshared catalog index and about 683 MB for the shared label snapshot. The
configured 16 GiB live-memory admission limit therefore does not currently
bound total process residence or transient generation-building scratch.

This becomes dangerous before the full corpus. A separate, non-formal
250,000-message diagnostic reached 14.96 GiB peak process-tree RSS while its
reported live charge was only about 1.93 GB.

## Correctness evidence

The accepted screen passed:

- byte-identical D, P, and Q storage trees;
- identical replay counters: 1,669,910 observed, 1,668,979 accepted, and
  1,666,808 recorded samples;
- identical ten-file, 298,045,928 B output trees;
- exhaustive validation of one Schema 8 segment, 1,437,066 series/chunks, and
  1,567,241 physical samples;
- an exact-postings fingerprint and decoded semantic fingerprint;
- all 26 expected independent readback queries, with zero skips and zero
  mismatches; and
- observed non-empty, head-only responses (`segments_queried=0`).

The ingester recorded 1,666,808 successful head writes. Storage contains
1,567,241 rows because equal-timestamp last-write-wins compaction removed
99,567 writes (5.97%). A strengthened post-run audit reconciled replay
counters, every `Head window written` count, series, chunks, physical samples,
zero dropped typed series, exhaustive verifier totals, exact postings, and the
decoded semantic fingerprint.

The frozen screen harness carried an older one-sided physical-row gate, so the
stronger audit is post-hoc. There is also no current capture-level golden for
this 50,000-message prefix. The old 4,000,000-message golden predates
last-write-wins coalescing and must be explicitly versioned and rebaselined;
it must not be silently weakened.

### Exact-prefix head-versus-sealed oracle

Evidence root:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-prefix-oracle-20260725T111950
```

The oracle selected generation 3 at exact visible message sequence 12,481.
Eight observations of that query/generation agreed.
The measured live response was non-empty, matched seven stored streams,
projected 51 Prometheus series, decoded seven head samples, and queried zero
segments, with no chunk bytes read. A fresh API-disabled replay used the same
frozen ingester and capture identity, changed only API enablement, the exact
stop cut, and the fresh segment path, and sealed exactly those 12,481
messages. The standalone API then evaluated the exact same expression and
timestamp and demonstrably read one segment, seven chunks, and 1,535 logical
payload bytes.

Both paths returned 51 series/samples and the ordering-sensitive canonical
Prometheus data hash:

```text
be32b15e9a05ed1dafe8b20352e00e4ae558f8a5b4e7567cc626ebe92067138d
```

This proves equality for that one selected instant-query
head-versus-sealed storage path. It does not cover range queries, all PromQL,
OOO/handoff cases, or every typed OTLP semantic, and it is not an independent
PromQL semantic oracle because both paths use Chronoxide's evaluator.
Canonicalization sorts object keys but retains result-array order, so equality
is exact while a future failure could be ordering-only.

The standalone API binary was built post-hoc. Its relationship to the
unchanged measured Rust source state is operator-recorded rather than
mechanically bound to the pre-timing binary manifest. The oracle records that
provenance limitation and is screening evidence rather than formal release
evidence. Its replay and sealed-query timings are correctness-only and are not
comparable with measured live latency or throughput.

The candidate-promotion runner now freezes `chronoxide-api` with the ingester,
query verifier, and storage verifier before any measured arm. The next
exact-prefix oracle must use that preserved API with post-hoc opt-in disabled,
closing this specific provenance gap.

## Larger diagnostic and stop condition

The earlier 250,000-message root is intentionally incomplete and was affected
by unrelated host load, so it supplies scaling diagnosis rather than A/B
numbers. P's 142 ordinary boundary publications were 5.090/6.832/8.176 s at
p50/p95/max. Near 4.4 million active series, late boundaries were about
6.8–7.1 s and owner/head rebuilding alone took about 5.8–5.9 s.

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-ingest-screen-noisy-20260725T022221Z
```

Its final publication completed, so this was not a deadlock, but it exposed a
deterministic shutdown pathology:

- final publication: 372.925 s;
- seal: 199.428 s;
- sample root: 15.892 s;
- catalog: 144.396 s; and
- post-commit: 13.056 s.

The broad Q workload also hit its 30-second client timeout before completing
the 250,000-message prefix. It must be treated as a capacity failure, not
folded into the lean latency distribution.

The next implementation must pass these fail-fast gates before a 4M run:

- 125,000 messages as a smoke screen, then 250,000 as the mandatory scale
  gate;
- ordinary-boundary publication p95 no greater than 10 s and maximum no
  greater than 15 s; a 30 s publication is an abort watchdog, not acceptance;
- post-seal shutdown cleanup (`publication_duration_ns - seal_ns`) no greater
  than 60 s;
- at 250,000 messages, `sample_root_ns + catalog_ns` no greater than 10 s and
  `post_commit_ns` no greater than 30 s; and
- scaling comparisons must name active series, activation/retirement churn,
  sample keys, and fragment count. Prefix count alone is not a valid
  complexity proxy.

## Work to do before repeating the formal experiment

The code and timings support a more specific diagnosis. `Arc` pin/swap
synchronization is cheap; synchronous construction and validation of each
immutable generation on the ingestion thread is expensive. During high
new-series churn, catalog reconciliation and per-label persistent-map updates
dominate. As cardinality matures, rebuilding the scratch owner map and
re-validating the full active set dominate. Final empty-head publication also
retires handed-off fragments through repeated full-map scans and removes
active catalog rows/postings one by one. These are code-derived complexity
risks, not fitted scaling laws.

1. Add a validated empty-head shutdown path that publishes empty sample and
   catalog roots directly while old pinned generations survive through
   `Arc`.
2. Make the owner root persistent/incremental and remove the redundant full
   active-set validation through a private proof-bearing constructor.
3. Maintain persistent active-series reference counts and birth/retirement
   deltas, and cache validated canonical row IDs, so catalog reconciliation
   does not rescan and sort every key.
4. Replace one-full-scan-per-fragment retirement with a bulk or indexed path.
5. Make memory admission cover retained catalog/index state, query-retained
   generations, and peak construction scratch—not only frozen payload.
6. Profile the empty-control matcher path; its zero-result latency shows that
   chunk decoding and storage I/O are not the current query bottleneck.
7. Only after those changes should the per-membership persistent postings-map
   representation itself be batched or redesigned.
8. The empty-head shortcut's mandatory quiet 250k gate passed. The later
   owner-validation shortcut passed a noisy 250k A-B-B-A screen; run its
   distinct naturally quiet 250k candidate gate before the formal
   counterbalanced 4M experiment.
