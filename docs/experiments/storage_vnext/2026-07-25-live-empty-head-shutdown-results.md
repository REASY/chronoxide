# Live-query final empty-head shutdown fast path

**Date:** 2026-07-25

**Status:** focused A/B, full 50k D/P/Q, exact-prefix oracle, noisy 125k
smoke, and mandatory quiet-host 250k scale gate passed; 4M not attempted

## Decision

Keep the exact, proof-gated empty-head shutdown fast path.

In a counterbalanced 50,000-message `A,B,B,A` replay, it reduced mean final
publication time from 75.371 s to 29.349 s (-61.06%). Non-seal publication
time, calculated as total publication time minus `seal_ns`, fell from 49.692 s
to 3.521 s (-92.91%), and sample-root plus catalog construction fell from
46.247 s to 0.141 s (-99.70%). Segment sealing did not materially change.

The candidate produced byte-identical sealed storage and the same decoded
semantic and exact-postings fingerprints in all four runs. Every exhaustive
Schema 8/footer/postings check and all 26 independent readbacks passed with
zero skips and zero mismatches.

This accepts one narrow optimization for scale promotion. It is not yet
production acceptance for the live-query feature and is not evidence that
ordinary publication became faster. The full 50,000-message D/P/Q and
provenance-bound exact-prefix gates described below also passed. The noisy
125,000-message smoke and mandatory quiet-host 250,000-message scale gate also
passed. This satisfies the scale prerequisite before any 4M experiment; it
does not by itself promote the entire live-query feature.

## What changed

At final shutdown, after sealing has made the immutable segment inventory
cover the exact publication cut, the previous implementation still retired
every handed-off fragment from the persistent sample map and removed every
inactive row and posting from the catalog one at a time. That work rebuilt an
empty live view from roughly 1.4 million sample keys in this corpus.

The candidate may publish empty sample and catalog roots directly only when
all of the following are proven:

- publication is the final shutdown publication;
- the candidate sealed reader is bound to the exact current manifest cut;
- exact coverage contains no head-owned order;
- every pending fragment has been handed off and no seal attempt remains;
- every mutable head has zero publishable fragments;
- the committed pending fragment identities exactly equal the immutable
  sample-root fragment certificate; and
- catalog lineage, revision, and next-generation invariants remain valid.

The root swap remains the single atomic visibility transition. Pending
bookkeeping, expected-order replacement, and guard retirement occur only after
a successful swap, as on the normal path. A reader that already pinned the
predecessor retains its complete old roots through `Arc`; new readers see the
empty live roots plus the manifest-bound sealed reader. Any failed proof or
root construction fails closed before the swap.

The certificate is currently an `Arc<BTreeSet<FrozenFragmentIdentity>>`.
Fragment count was tiny relative to sample-map cardinality in this corpus, so
its copy-on-write mutation cost was retained for this measurement. If future
ordinary publications regress as fragment count grows, replace it with a
dedicated persistent set rather than weakening the proof.

## Experiment

The diagnostic used four fresh P-only replays in `A1,B1,B2,A2` order:

- A: live publication before the empty-head shortcut;
- B: the candidate with the shortcut; and
- P-only: publication enabled, with no concurrent HTTP client.

This isolates final publication cost without making D/P/Q or query-interference
claims. Each arm replayed the same first 50,000 captured messages using the
same configuration, CPU allocation, release build mode, capture-page eviction,
storage verifier, and readback schedule. No build, verifier, profiler, or
other measured Chronoxide process was intentionally overlapped. Conflict
checks were clean immediately before and after every arm, and no overlap was
observed.

Frozen ingester A/B root:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-empty-shutdown-binaries-20260725T045407Z
```

| Binary | SHA-256 |
|---|---|
| baseline ingester | `c5a9897c7abc5b5bcd8b29fd2af6397e5d3455ad49b1d30a8cbdf6a3a0dfbe0b` |
| candidate ingester | `d4044c5566606f214ebd824f708b8d8ebc8bba20aa58d2af17a6fe193d63256b` |
| query verifier | `64bc9a32af78dfce7ca51485a6429a6abfc77e789b7757a2e499d918a6e4a235` |
| storage verifier | `8934816bcd6335828d47bfbd4c67b3c82b524b8e46928cc17ef1dac2b2527453` |

The shared root contains only the two ingester binaries. The query and storage
verifier binaries were preserved independently in every accepted result root;
their listed hashes are identical across all four roots.

The baseline ingester binary is co-preserved with the pre-shortcut source
snapshot under:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-ingest-screen-lean-20260725T110340
```

That root contains the same baseline binary and its captured Rust source
state, whose publisher/sample/catalog sources lack the shortcut. Each new A/B
root captures the candidate working tree because code version is selected by
the frozen ingester binary rather than by changing worktrees between arms;
therefore the new A-arm source snapshots alone are not baseline provenance.

Accepted evidence roots:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-empty-shutdown-ab-20260725T131000-A1
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-empty-shutdown-ab-20260725T131500-B1
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-empty-shutdown-ab-20260725T132500-B2
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-empty-shutdown-ab-20260725T133500-A2
```

The fail-closed four-arm acceptance output is:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-empty-shutdown-comparison-20260725T133748/shutdown-ab-gate-v2.json
```

That gate independently re-hashes every frozen binary, reconstructs each
publication summary from the raw log, verifies every current segment file
against its safe exact-tree manifest, and checks the required preserved
correctness summaries and fingerprints. Each B observation must have lower
final, non-seal, and sample-root-plus-catalog time than both A observations.
Each B ordinary-publication p95 and maximum must be at most 110% of the worse A
observation, and each B peak RSS must be at most 105% of the worse A
observation.

## Results

### Individual arms

| Metric | A1 | B1 | B2 | A2 |
|---|---:|---:|---:|---:|
| elapsed | 149.23 s | 101.10 s | 100.84 s | 147.69 s |
| peak process-tree RSS | 4,317.35 MiB | 4,235.85 MiB | 4,281.90 MiB | 4,301.02 MiB |
| final publication | 75.210 s | 29.548 s | 29.151 s | 75.532 s |
| seal | 25.909 s | 26.029 s | 25.626 s | 25.450 s |
| non-seal publication | 49.302 s | 3.519 s | 3.524 s | 50.082 s |
| sample root | 1.895 s | 0.000013 s | 0.000012 s | 2.098 s |
| catalog | 43.967 s | 0.156 s | 0.126 s | 44.533 s |
| post-commit | 3.435 s | 3.355 s | 3.393 s | 3.444 s |
| ordinary publication p50 | 4.242 s | 4.537 s | 4.493 s | 4.547 s |
| ordinary publication p95/max | 4.822 s | 4.928 s | 4.964 s | 4.941 s |

The candidate shortcut reported the expected final base scale:

| Arm | sample keys | fragments | active series |
|---|---:|---:|---:|
| B1 | 1,387,744 | 25 | 1,365,448 |
| B2 | 1,398,839 | 25 | 1,376,543 |

The scale differs slightly because the time-driven publication boundary can
fall at a different message cut. The exact final proof applies to each arm's
own committed predecessor and manifest cut; it does not compare these counts
across arms as a correctness oracle.

### A/B means

| Metric | A mean | B mean | Candidate change |
|---|---:|---:|---:|
| final publication | 75.371 s | 29.349 s | -61.06% |
| seal | 25.679 s | 25.828 s | +0.58% |
| non-seal publication | 49.692 s | 3.521 s | -92.91% |
| sample root + catalog | 46.247 s | 0.141 s | -99.70% |
| post-commit | 3.440 s | 3.374 s | -1.90% |
| elapsed | 148.46 s | 100.97 s | -31.99% |
| user CPU | 147.38 s | 99.81 s | -32.28% |
| peak process-tree RSS | 4,309.19 MiB | 4,258.88 MiB | -1.17% |
| ordinary publication p50 | 4.395 s | 4.515 s | +2.74% |
| ordinary publication p95/max | 4.882 s | 4.946 s | +1.31% |

The ordinary distributions overlap. A1 happened to contain 14 ordinary
boundaries while the other three arms contained 13. Because B1, B2, and A2
each contained 13 boundaries, comparing the B mean with A2 shows the
candidate's ordinary p50 was about 0.7% lower and its p95 was about 0.1%
higher. Treat ordinary publication as neutral within this diagnostic's noise;
do not attribute the total elapsed improvement to a steady-state ingest
change.

Hardware counters agree with deleting final teardown work: candidate task
clock fell 32.04%, cycles 32.14%, instructions 26.69%, and cache misses 44.20%.
These whole-run differences are supporting evidence, not isolated
microarchitectural costs for the shortcut.

## Correctness evidence

All four accepted arms had:

- the same ten-file, 298,045,928-byte sealed corpus;
- byte-identical `segments.sha256`;
- byte-identical replay-correctness JSON;
- corpus manifest SHA-256
  `ce710100ab7fea4d4c313446ec1fc574de32fc39c12fba10cd251a3510edbfe9`;
- decoded semantic fingerprint
  `db19b218a560c7a20ed8b2c018d0d538fdb54b2f16c14a33fcab8668402906b2`;
- exact-postings fingerprint
  `7365038beb6412688d423fd39c9e55bded633f6db4147659f53969d64ff1b86c`;
- successful exhaustive Schema 8 footer, bounds, checksum, and postings
  validation; and
- 26 expected and 26 executed independent readbacks, with zero skips,
  isolation skips, or mismatches.

Focused tests additionally cover:

- a reader pinned before the root swap observing the complete predecessor;
- exact final publication exposing only the sealed successor to new readers;
- injected commit-descriptor preparation failure retaining fragments, order
  ownership, and kind guards for an exact retry;
- certificate drift failing closed;
- no-data shutdown;
- mixed already-committed and newly frozen final fragments;
- pre-seal OOO/last-write-wins and post-seal OOO through final shutdown; and
- catalog generation, revision, lineage, postings, and predecessor-lifetime
  invariants.

## Full 50k D/P/Q promotion

The candidate then passed a fresh `Q,D,P` run with the same 50,000-message
prefix and lean two-query workload:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-candidate-50k-20260725T140315
```

The host's pressure was low, but an idle development environment remained, so
the run is explicitly marked noisy. Its correctness gates and large shutdown
effect remain usable; fine performance differences are diagnostic. Capture
pages were evicted before each arm. The 600-second observation staleness
matched the earlier screen and prevented controlled final shutdown from
turning into a client-availability test.

| Arm | Elapsed | Throughput | CPU time | Peak tree RSS |
|---|---:|---:|---:|---:|
| D | 10.83 s | 4,616.81 msg/s | 9.87 s | 941,472 KiB |
| P | 104.44 s | 478.74 msg/s | 104.72 s | 4,420,816 KiB |
| Q | 106.33 s | 470.23 msg/s | 198.00 s | 4,866,984 KiB |

P and Q both passed the ordinary-publication scale limits:

| Arm | Ordinary p50 | Ordinary p95/max | Final total | Seal | Non-seal |
|---|---:|---:|---:|---:|---:|
| P | 4.259 s | 4.706 s | 30.527 s | 26.981 s | 3.547 s |
| Q | 4.330 s | 4.702 s | 31.434 s | 27.904 s | 3.530 s |

P's final sample root took 0.013 ms, catalog 86.339 ms, and post-commit
3.455 s at a base of 1,419,059 sample keys, 27 fragments, and 1,396,759 active
series. Q's corresponding values were 0.011 ms, 152.285 ms, and 3.371 s at
1,389,217 sample keys, 27 fragments, and 1,366,921 active series. Both final
publications reported the exact empty-head fast path.

All D/P/Q arms produced the same ten-file, 298,045,928-byte segment tree and
replay counters. Exhaustive validation again produced the same semantic,
selection, and exact-postings fingerprints and reconciled 1,666,808 head
writes to 1,567,241 physical rows. All 26 independent readbacks executed with
zero skips or mismatches.

Q completed 240 requests. Overall query time was 317.851/998.509 ms at
p50/p95; the guaranteed-empty control alone was 316.571/799.285 ms despite
zero matched series, chunks, samples, or I/O. Root pin wait and held time
remained sub-microsecond. This reinforces that the current query bottleneck is
pre-decode head planning, not the publication pointer lock.

### Provenance-bound exact-prefix oracle

The promotion runner froze `chronoxide-api` before any timed arm:

```text
371e96c1b407a673fdda52afe21e5973e5f6ff78801c75ba25098e73893e20a9
```

The subsequent oracle verified that exact preserved binary and closed the
earlier post-hoc API provenance limitation:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-prefix-oracle-candidate-20260725T141306
```

At generation 3 and exact visible message sequence 12,477, the live path
matched seven stored streams, projected 51 series, decoded seven head samples,
and queried zero segments. A fresh API-disabled replay sealed exactly those
12,477 messages; the standalone API then read one segment, seven chunks, and
1,535 logical payload bytes. Both paths produced 51 samples and the same
ordering-sensitive response-data hash:

```text
be32b15e9a05ed1dafe8b20352e00e4ae558f8a5b4e7567cc626ebe92067138d
```

### Noisy 125k scale smoke

The next P-only smoke used the same frozen candidate binaries and exact capture
contract:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-candidate-125k-20260725T142300
```

This run was deliberately admitted as noisy evidence. Maven/JDTLS overlapped,
and the pre-run full I/O PSI `avg10` was 6.19. Its absolute throughput and
latency therefore remain diagnostic rather than quiet-host performance
claims. The hardened scale gate nevertheless passed every 125k acceptance
condition:

| Measurement | 125k result | Limit |
|---|---:|---:|
| Ordinary publication p50 | 4.529 s | observed |
| Ordinary publication p95 | 5.946 s | <= 10 s |
| Ordinary publication maximum | 6.272 s | <= 15 s |
| Final post-seal work | 10.510 s | <= 60 s |
| Peak process-tree RSS | 11.92 GiB | observed |
| Process-tree swap | 0 B | required |

The final publication took 109.236 s, of which 98.726 s was immutable segment
sealing. The exact empty-head shortcut then completed sample-root plus catalog
work in 25.890 ms and post-commit cleanup in 10.452 s. Those two measurements
are recorded but are acceptance conditions only at 250k. The pre-clear head
held 3,806,581 sample keys in 105 fragments and 3,783,681 active series; the
published final live roots contained zero of each.

The run produced 53 successful ordinary publications and exactly 53 successful
message-boundary observations, with no failures. Ordinary stage p95 values
were 4.015 s for catalog construction, 2.466 s for owner/head validation, and
0.198 s for the sample root. Publication pauses consumed about 243.3 s of the
418.2 s replay. Root-lock wait and hold maxima remained below 0.6 us and
1.4 us respectively, so the scale cost is immutable-state construction rather
than mutex contention.

All 80 sealed result-artifact entries, all ten segment files, and all four
frozen binaries rehashed. Exhaustive Schema 8/footer/exact-postings validation
reported 3,801,959 series/chunks and 4,611,914 physical samples. Its selection,
decoded-semantic, and exact-postings fingerprints were:

```text
b4228be42648fa48b6b43575307cd4dc02385bae11f77b53bed7ddab4693cfeb
ca079d390b264d37b495d21f6510c13f039d5bafbd5dbb64a0568497addf4725
0bb44dde01d72b195f4a3f885d2f24bb6e48fedf6e41be726b7b61c110df7876
```

All 26 independent readbacks executed with zero skips or mismatches. The
hardened validator now binds the accepted capture, template, CPU sets, API and
memory controls, four binary hashes, raw perf/GNU-time evidence, exact artifact
inventory, recomputed corpus bytes, raw verifier/readbacks, and the 125k/250k
threshold mode. Independent mutation review additionally proved that
publication counts alone were insufficient: a result could otherwise report
ten trivial early roots and defer the full-scale root to shutdown. The gate now
requires strictly advancing ordinary message cuts, requires the last ordinary
250k cut to reach at least 90% of the prefix, and validates the fixed quiet-host
pressure limits both before and after the mandatory run. Because endpoint
snapshots still leave the replay interval blind, the future runner now stops a
fresh measured session at a pre-start barrier, starts a frozen 250 ms `/proc`
sampler, records monotonic start/end boundaries, and only then releases replay.
The monitor first proves that `/proc` has `hidepid=0`, exposes one PID-namespace
level, and exposes PID 1; every scan must retain that exact PID 1 identity. The
gate reparses the raw header/sample/footer stream, rejects overlapping or
regressing scans and gaps above 500 ms, binds the measured session leader to
its kernel start time throughout replay, and requires a complete scan after
the runner's stop observation. A one-way leader disappearance is allowed only
inside the predeclared cadence-plus-boundary tail because process exit
necessarily precedes recording the end boundary. Recognized build, profiler,
database, QEMU, or other Chronoxide processes outside the measured session
fail the run.

A `/proc` entry can disappear between enumeration and parsing. The evidence
therefore records that uncertainty and rejects more than eight such entries in
one scan or more than 1,000 per million listed-PID observations over the run.
This remains bounded polling evidence, not a claim that a process shorter than
the sampling interval could never execute. The separately pinned RSS sampler
now binds the same leader PID and kernel start time, verifies that identity
while sampling, and must reconcile every raw row, peak, sampling gap, and the
full measured duration. The runner supervises both observers in isolated
sessions and aborts if either exits before replay. A five-second implementation
probe consumed 0.19 s user plus 0.08 s system CPU, peaked at 35,216 KiB RSS,
and emitted 2,321,650 bytes over 5.31 s. Separate GNU time evidence makes that
observer overhead explicit rather than attributing it to replay.

The sealed 125k root retains two provenance-label limitations. Its
`validated-inputs.json` names the 4M capture-capacity expectation while the
settings, rendered configuration, and replay correctly bind the 125k cut.
Also, its P-only coverage note incorrectly claimed writer-row reconciliation
even though the normalized live-handoff gate correctly reports that no
per-window writer evidence exists. The runner now records the selected
noisy-host/readback controls, binds root summaries, labels P-only validation
accurately, and uses an explicit coverage-gap statement. These corrections
apply to the future 250k root; the sealed 125k evidence was not rewritten.

### Rejected 250k attempt

The first mandatory attempt is preserved only as noisy diagnostic evidence:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-candidate-250k-20260725T163207
```

An unrelated long-running Codex session began a Codehop `cargo` workload
177.142 seconds after the measured boundary and continued through shutdown.
The 250 ms sampler observed `cargo` or `rustc` in 1,437 samples. Process churn
also produced a maximum of 20 vanished entries in one scan, exceeding the
predeclared limit of eight. The runner then rejected the root at its endpoint
process check with exit 70, before storage verification or readbacks. This root
must not satisfy the mandatory gate or support a quiet-host performance claim.

For diagnosis only, replay took 1,016.15 seconds and peaked at 15,630,036 KiB
process-tree RSS with no measured swap. Ordinary publication p50/p95/max was
4.736/11.688/12.362 seconds. Owner/head validation was the dominant ordinary
stage at 10.241 seconds p95, followed by catalog construction at 3.751 seconds
p95. The final publication spent 169.704 seconds sealing, then 13.604 seconds
after sealing; sample-root plus catalog construction took 22.785 ms and
post-commit cleanup took 13.444 seconds. Thus every mandatory shutdown limit
would pass, but ordinary p95 would miss its 10-second limit. Because external
compilation overlapped the final 839 seconds, the quiet retry—not this
observation—must decide whether owner/head validation is the next optimization.
The monitor now exits on the first recognized external conflict, and the
runner's existing two-child supervisor terminates replay immediately; a future
interfered retry will preserve a partial root without wasting the full prefix.
Because contaminated evidence has no graceful-shutdown value, this path sends
`SIGKILL` to the measured process group rather than entering Chronoxide's
expensive final seal. A TERM-ignoring fake-leader probe observed monitor status
2, launcher status 137, and completed the abort in 202.9 ms.

### Accepted mandatory 250k scale gate

After temporarily suspending the explicitly authorized development workloads,
a 180.52-second preflight observed no conflict. The fresh P-only replay then
completed without an external process conflict:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-candidate-250k-20260725T114802Z
```

The runner recorded 3,413 host-process scans across the 852.83-second measured
interval. The sampler, RSS observer, replay, verifier, and readbacks all exited
successfully; no measured swap occurred. All temporarily suspended processes
were identity-checked and resumed after the run.

| Measurement | 250k result | Mandatory limit |
|---|---:|---:|
| Ordinary publication p50 | 4.621 s | observed |
| Ordinary publication p95 | 5.867 s | <= 10 s |
| Ordinary publication maximum | 6.039 s | <= 15 s |
| Final post-seal work | 13.595 s | <= 60 s |
| Final sample root + catalog | 21.671 ms | <= 10 s |
| Final post-commit cleanup | 13.456 s | <= 30 s |
| Peak process-tree RSS | 15,677,912 KiB (14.95 GiB) | observed |
| Process-tree swap | 0 B | required |

The 118 ordinary publications and 118 message-boundary observations matched
exactly, with no failure. Their message cuts strictly advanced, and the last
ordinary cut reached message 248,824, or 99.53% of the prefix. Owner/head
validation was the largest ordinary stage at 4.569 s p95, followed by catalog
construction at 3.898 s p95 and the sample root at 0.231 s p95. The total
ordinary p95 still cleared the mandatory limit by 4.133 s.

Final publication took 168.851 s, including 155.256 s of segment sealing. The
proof-gated empty-head path then reduced sample-root construction to 0.099 ms
and catalog construction to 21.572 ms at a predecessor scale of 4,432,950
sample keys, 237 fragments, and 4,395,484 active series. The published live
roots contained zero of each.

The sealed corpus contains 26 files and 965,568,698 bytes. Exhaustive Schema 8
footer, bounds, checksum, and exact-postings verification decoded 4,409,680
series/chunks and 9,166,720 physical samples. Its decoded-semantic,
exact-postings, and verified-selection fingerprints are:

```text
ad6cfd0a81465eb9a8b64d9bca3036c30d4b6d250bc2577c4b3616cdac87919e
0d12cb61389dd518bd35f712cef97c1063f88edb0878253ea11556ac6732b400
3e00d77c14756da00cde7d6802ede875ee0a471fd829a132d123bf91318de41d
```

The independent readback oracle reported 30 expected, 30 executed, and 30
checked queries, with zero skips, isolation skips, or mismatches. This count is
larger than the 125k count of 26 because the larger corpus gives some sampled
typed identities multiple merged samples, enabling additional derived oracle
checks. Both prefixes still contain 14 visible sampled PromQL table rows.

The producer-frozen v1 admission validator incorrectly used one global
26-query golden for both prefix sizes, so it rejected this otherwise complete
root after the run. That was a validator-contract defect, not a readback
failure. The v2 validator uses exact prefix-specific cardinalities—26 at 125k
and 30 at 250k—and rejects both missing and inflated counts. No performance
threshold changed, the defect could not influence measured execution, and the
sealed result root was not rewritten.

The post-hoc v2 admission ran under isolated Python (`-I -S -B`) from a
read-only sibling bundle. Its certificate binds the exact source bytes loaded
for the bootstrap, gate, and Phase 1 helper; the original frozen producer
harness; Python executable; complete CLI expectation tuple; empty `COMPLETE`
marker; 90-entry result-artifact manifest; 26-file segment manifest; raw and
canonical gate output; stdout, stderr, and exit status. Both manifests were
fully rechecked after admission:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-candidate-250k-20260725T114802Z-scale-gate-v2-20260725T123447Z
```

The certificate SHA-256 is
`6ddaa57cd41abed7626df267bd4aca36e874ff09e9ec3da2dcbd56f520652431`;
the external artifact-manifest SHA-256 is
`4c267b209418e44080a87b0b215ffc99f3b2729d5f1f6ffb8f8fcc02770c2c2c`.
This is explicitly post-hoc sealed-root validation, not a claim that v2 was
the producer-time gate.

## Remaining limits

- The full 50k run has one `Q,D,P` order and a closed-loop lean client; it is
  not the formal position-counterbalanced capacity experiment.
- Its 600-second staleness policy is an observation aid. The default 10-second
  policy remains valid for the measured ordinary publications but was not
  asserted across the 27-second controlled final seal.
- Peak live-memory admission still does not comprehensively charge catalog
  roots, reader-pinned generations, or construction scratch.
- At 250k, owner/head validation is now the largest ordinary stage at 4.569 s
  p95, with catalog construction at 3.898 s p95. Profile this path before
  choosing the next persistent-root or retirement optimization.
- The guaranteed-empty query still scales with the live head and needs a
  separately measured catalog-negative fast path after publication scaling is
  addressed.
- The quiet-host 250,000-message prerequisite has passed. The formal
  4,000,000-message counterbalanced run has not yet been attempted.
