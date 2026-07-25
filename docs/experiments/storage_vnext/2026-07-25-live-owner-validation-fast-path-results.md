# Live at-most-one-active-partition owner-validation fast path

Date: 2026-07-25

Status: keep the hardened shortcut. It passed a non-invasive, noisy-host
250,000-message A-B-B-A mechanism, CPU, memory-regression, and persisted-corpus
correctness screen after the precursor's 50,000-message diagnostic. The
implementation-level effect is clear, but the observed whole-run percentage is
not a formal quiet-host estimate. A naturally quiet, provenance-bound
250,000-message candidate gate remains required before the formal 4M run.

## Question

Ordinary live-view publication spent 4.569 seconds at p95 in the combined
owner/head stage of the accepted 250,000-message scale run. Was that work
necessary for this corpus, and can it be removed without weakening
cross-partition ownership correctness?

The profiled implementation rebuilt this temporary structure at every
publication:

```text
canonical series ID -> [(raw SeriesRef, active PartitionKey), ...]
```

It revisited every series run in every non-handed-off fragment, performed a
persistent-catalog lookup and ordered-map operation for each run, allocated
nearly one small bucket per canonical series, compared colliding canonical
rows, and then destroyed the whole tree.

The accepted capture prefix has only one active full source identity:
`(topic="otlp_metrics", partition=1)`. The forbidden condition is one
canonical series simultaneously owned by two *distinct* full partition keys.
With zero or one non-handed-off partition, that condition is impossible.

The screened candidate first inspected the partition attached to each
non-handed-off fragment. Zero or one distinct full partition key returned
success without building the per-series tree. Two or more partitions retained
the existing collision-safe validation unchanged.

Independent correctness review then tightened the proof before the scale run.
The current candidate first derives and sorts every active pending fragment
identity, rejects duplicates, and proves exact equality with the candidate
sample store's independent fragment certificate. It derives partitions only
from that validated identity set. Handed-off fragments remain excluded, and
exact catalog/sample active-series binding still runs before the root swap.
This adds fragment-scale work, not the eliminated retained-run scan.

## Instrumented baseline

The baseline adds only substage and work counters. It does not take the
shortcut.

```text
binary SHA-256:
e4323e839820de75706a1a00deb5d31e4dcfc236bccefcf4cc4869e87acb604f

result root:
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-owner-fastpath-baseline-50k-20260725T125253Z
```

At the last ordinary publication, message cut 49,737:

| Work | Observation |
|---|---:|
| Active partitions, capped at two | 1 |
| Retained run keys examined | 1,483,009 |
| Owner-ID buckets built | 1,431,579 |
| Canonical identity comparisons | 51,430 |
| Owner validation | 851.471 ms |
| Exact catalog/sample binding | 160.949 ms |
| Combined owner/head stage | 1.012 s |

Across all 16 ordinary publications, rebuilding owner state consumed 6.424
seconds. Exact catalog/sample binding consumed another 1.344 seconds.

## Candidate

```text
precursor binary SHA-256:
e61a8f3eb6554457e6b291db72d8dcdf59f819be11155cbf05ed6656b5d2807c

result root:
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-owner-fastpath-candidate-50k-20260725T125253Z
```

The precursor candidate reported the at-most-one-partition fast path at every
applicable ordinary publication. It examined zero run keys and built zero owner
buckets. At its last ordinary publication, message cut 46,163, owner validation
took 1.140 microseconds while exact catalog/sample binding still took 140.858
milliseconds.

The closest publication cuts in the two schedules were 36,352 and 36,362:

| Stage | Baseline | Candidate |
|---|---:|---:|
| Owner validation | 506.282 ms | 0.001 ms |
| Combined owner/head stage | 614.651 ms | 107.851 ms |

This is mechanism evidence only: the frozen precursor binary removed the
retained-run owner scan on the one-partition corpus while the later exact
head/catalog validation remained.

## Hardened scale candidate

The post-review binary includes the exact pending-fragment/sample-certificate
proof described above:

```text
binary SHA-256:
98a0c3cf77f54e2b2466ef3bf35eea6497e9b1cbb8dc9101588dff3bd31018ff

frozen binary:
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/live-query-owner-fastpath-hardened-binaries-20260725T133257Z/chronoxide-ingester
```

The A-B-B-A screen below preserved and executed this exact binary in both
candidate positions.

## Diagnostic 50k A/B

Both arms used the same host, release profile/toolchain, fingerprinted capture,
50,000-message prefix, configuration template, CPU sets, 1-second publication
interval, 16 GiB live admission value, disabled range-scalar cache, supporting
binaries, perf event set, footer/postings verifier, and independent readback
oracle. The ingester binary hashes differed.

The following are raw observations, not attributable performance effects:

| Observation | Baseline | Candidate | Difference |
|---|---:|---:|---:|
| Elapsed | 115.54 s | 97.09 s | -15.97% |
| Process task-clock | 115.896 s | 97.340 s | -16.01% |
| CPU cycles | 632.395 B | 540.746 B | -14.49% |
| Instructions | 1,265.533 B | 1,201.911 B | -5.03% |
| Cache misses | 1.687 B | 1.074 B | -36.32% |
| Peak process-tree RSS | 4,492,596 KiB | 4,217,052 KiB | -275,544 KiB (-6.13%) |
| Process-tree swap | 0 B | 0 B | unchanged |
| Sum of ordinary publication time | 62.225 s | 49.443 s | -12.782 s |
| Sum of owner validation | 6.424 s | 0.000013 s | -6.424 s |

The baseline completed 16 ordinary publications and reached message cut 49,737
before shutdown; the candidate completed 13 and reached cut 46,163. Their
shutdown tails therefore differed substantially. Total ordinary publication
p50 changed from 4.007 seconds to 4.167 seconds, while p95 was effectively
flat because other publication stages remained noisy. Neither aggregate sums
nor whole-run counters are schedule-controlled estimators.

Every 250-millisecond host-monitor sample contained at least one recognized
conflict: 465/465 baseline samples recorded 2,588 conflict observations across
15 identities, and 392/392 candidate samples recorded 1,649 across 9
identities. Observed conflicts included Cargo, Docker, Java/QEMU, and perf. The
screen explicitly used `ALLOW_NOISY_HOST=1`, disabled capture-cache eviction,
and ran only the publication arm, without concurrent API query traffic.

Peak live-memory charges were effectively equal (`+0.022%` in the candidate).
The noisy RSS difference is not evidence of a memory improvement.

Finally, both roots froze the same current candidate source snapshot. The
different binaries are preserved and hashed, but the precise source diff that
produced them cannot be reconstructed from those roots. The 250k screen below
preserves the hardened candidate source and binary; its A-source authority is
the separately preserved accepted baseline root.

## 50k correctness

The complete persisted output was byte-identical:

```text
files: 10
bytes: 298,045,928
segment-tree SHA-256:
ce710100ab7fea4d4c313446ec1fc574de32fc39c12fba10cd251a3510edbfe9
```

Both full-corpus storage verifiers reported:

```text
segments: 1
series/chunks: 1,437,066
physical samples: 1,567,241
decoded-semantic fingerprint:
db19b218a560c7a20ed8b2c018d0d538fdb54b2f16c14a33fcab8668402906b2
exact-postings fingerprint:
7365038beb6412688d423fd39c9e55bded633f6db4147659f53969d64ff1b86c
verified-selection fingerprint:
ede65a47e259155ac51cad0a9595553375a3dab68bda53bb3e431f661cd5037d
```

All 26 expected independent readbacks executed in both arms with zero skips,
isolation skips, or mismatches.

Focused tests additionally cover:

- normalized-equivalent raw refs in one partition taking the shortcut without
  losing samples;
- exact pending/sample fragment-certificate drift failing closed even when
  catalog/sample active-series equality still passes;
- equal numeric partition IDs in different topics taking the full path;
- disjoint multi-partition series;
- canonical conflicts across distinct full partition keys failing closed; and
- ownership transfer after handoff while an old generation remains pinned.

The P-only screen did not independently exercise queries against intermediate
live generations. Those semantics remain covered by focused unit,
multi-threading, and live/sealed equality tests, but the 50,000-message result
root establishes only final persisted-corpus equivalence.

## Hardened 250k noisy-host A-B-B-A

The completed sequence used the accepted baseline as A and the hardened
candidate as B:

```text
A1  live-query-owner-fastpath-abba-250k-20260725T141258Z-A1
B1  live-query-owner-fastpath-abba-250k-20260725T143336Z-B1
B2  live-query-owner-fastpath-abba-250k-20260725T144732Z-B2
A2  live-query-owner-fastpath-abba-250k-20260725T150047Z-A2
```

All roots are under:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/
```

The executed ingester hashes were:

```text
A: d4044c5566606f214ebd824f708b8d8ebc8bba20aa58d2af17a6fe193d63256b
B: 98a0c3cf77f54e2b2466ef3bf35eea6497e9b1cbb8dc9101588dff3bd31018ff
```

The supporting API, query, and storage-verifier binaries, capture, config
template, frozen runner, 250,000-message prefix, CPU sets, 16 GiB live-memory
admission value, one-second publication interval, disabled range-scalar cache,
capture eviction, perf event set, 100 ms RSS sampling, and 250 ms host-process
sampling were identical. After excluding timestamp, result-directory, and
run-note fields, all four settings files have SHA-256
`1e0751727c619db9301c889d6e7262e9ed617e22fe463938cda7616dd5120096`.

The frozen `validated-inputs.json` was reused from the 4M input preflight and
contains a stale 4M `stop_after_messages` value. That is a metadata defect in
these diagnostic roots. The settings, rendered configuration, replay totals,
and final visible message sequence independently bind every arm to 250,000
messages. The quiet gate must regenerate this metadata with the selected 250k
prefix.

### Observed whole-run and CPU result

These are means of the two A and two B positions:

| Observation | Baseline A | Candidate B | Observed change |
|---|---:|---:|---:|
| Elapsed | 1,008.030 s | 668.765 s | -33.66% |
| Process task-clock | 1,011.634 s | 671.584 s | -33.61% |
| CPU cycles | 5.568 T | 3.669 T | -34.12% |
| Instructions | 7.478 T | 6.527 T | -12.71% |
| Cache misses | 39.387 B | 13.062 B | -66.84% |
| Page faults | 10.666 M | 11.254 M | +5.51% |
| Peak process-tree RSS | 15,638,654 KiB | 15,516,706 KiB | -0.78% |

Both adjacent contrasts point in the same direction: B1 was 38.05% faster than
A1, and B2 was 28.40% faster than A2. Candidate elapsed times were 679.80 and
657.73 seconds; baseline elapsed times were 1,097.38 and 918.68 seconds.

The instruction and cache-miss reductions are especially useful mechanism
evidence: B completed 277 ordinary publications versus A's 264 while executing
fewer instructions and avoiding most of the pointer-heavy temporary-owner-map
traffic. Total page faults increased modestly in both replicas: +4.41% in
A1/B1 and +6.70% in A2/B2. They remain a repeated follow-up signal. Major
faults moved inconsistently, from 432 to 298 and from 402 to 526. Candidate
peak RSS was lower in both pairings with zero swap, which establishes no
observed peak-RSS regression; the sub-1% difference is not claimed as a memory
optimization. Peak live-admission charges were slightly higher in both
candidate pairings (+0.041% and +0.0039%), which is immaterial but reinforces
that the RSS difference is not an accounting-based memory win.

### Publication mechanism

| Ordinary-publication stage | A1 | B1 | B2 | A2 |
|---|---:|---:|---:|---:|
| Total p50 | 4.894 s | 1.724 s | 1.746 s | 4.579 s |
| Total p95 | 7.011 s | 4.126 s | 4.093 s | 6.118 s |
| Owner + head p50 | 3.250 s | 0.460 s | 0.414 s | 2.832 s |
| Owner + head p95 | 5.793 s | 0.522 s | 0.561 s | 4.815 s |
| Catalog p50 | 0.959 s | 0.972 s | 0.996 s | 0.944 s |
| Catalog p95 | 3.635 s | 3.757 s | 3.771 s | 3.662 s |
| Sample root p50 | 0.151 s | 0.159 s | 0.156 s | 0.165 s |
| Sample root p95 | 0.243 s | 0.241 s | 0.227 s | 0.233 s |

The neighboring catalog and sample-root stages remained comparable. At
nearest publication cuts for fixed input-prefix targets, the combined
owner/head stage was as follows. Selected cuts differed by at most 1,381
messages, or about 0.55% of the full prefix:

| Target cut | A1 | B1 | B2 | A2 |
|---|---:|---:|---:|---:|
| about 50k | 0.804 s | 0.167 s | 0.156 s | 0.859 s |
| about 100k | 2.215 s | 0.356 s | 0.329 s | 1.949 s |
| about 150k | 3.494 s | 0.464 s | 0.410 s | 2.943 s |
| about 200k | 3.917 s | 0.489 s | 0.439 s | 3.726 s |
| about 249k | 5.356 s | 0.522 s | 0.483 s | 5.070 s |

Across all 277 candidate boundary publications:

- the exact fragment certificate proved one active full partition;
- the at-most-one-partition fast path was always selected;
- zero retained run keys or owner-ID buckets were examined, and zero canonical
  owner-row comparisons were performed;
- owner validation consumed 23.692 milliseconds in total;
- B1/B2 owner-validation p50 was 64/61 microseconds and p95 was 115/123
  microseconds; and
- nearly all remaining combined time was the unchanged exact head/catalog
  binding.

This repeatedly removes the scale-dependent owner scan at comparable logical
prefixes. That result does not depend on treating total wall time as a clean
estimator.

### Host-load limitation

`ALLOW_NOISY_HOST=1` deliberately kept every external workload untouched.
Inside the measured start/end boundaries, the frozen classifier observed:

| Arm | Samples with recognized conflicts | Conflict observations | Process identities |
|---|---:|---:|---:|
| A1 | 4,179 / 4,390 (95.2%) | 50,289 | 4,352 |
| B1 | 2,654 / 2,719 (97.6%) | 13,171 | 657 |
| B2 | 2,224 / 2,631 (84.5%) | 30,070 | 773 |
| A2 | 1,168 / 3,675 (31.8%) | 6,784 | 585 |

A1 overlapped a qualitatively heavier compiler/build storm: 11,604 recognized
runnable or uninterruptible observations, versus 278, 49, and 59 in B1, B2,
and A2. A1 also exceeded the monitor's uncertainty bound with 30 vanished PIDs
in one scan; the other arms remained at or below seven.

The recognized table intentionally excludes classifier gaps. Unrecognized
`memgraph` processes contributed 363 observations / 16 identities / 308
runnable-or-uninterruptible observations in A1; 348 / 16 / 294 in B1; 2,137 /
44 / 789 in B2; and none in A2. Unrecognized `qemu-aarch64` processes
contributed 2,638 observations / 10 identities in A1, 2,389 / 14 in B1, 303 /
1 in B2, and none in A2. B2 therefore had substantial active database work
that the frozen conflict classifier did not count. Process presence is still
not CPU-time measurement.

ABBA brackets time drift, but it did not balance these structured workloads.
The -33.66% whole-run mean is therefore a noisy screening observation, not a
certifiable effect size. The stronger conclusion is narrower: A2 was much
cleaner than A1 yet remained slower than both B arms, candidate results
replicated tightly, matched-scale owner/head work fell by roughly 87%, and
hardware counters moved in the direction expected from deleting the temporary
map scan.

### 250k persisted-corpus correctness

All four arms produced the same byte manifest:

```text
files: 26
bytes: 965,568,698
segment-manifest SHA-256:
62515f250a36a24d3f435e68b705befeaf4c3cbed0fab7e25e4fe418cd9256eb
```

The exhaustive footer and exact-postings verifier reported identically:

```text
schema: 8
segments: 3
series/chunks: 4,409,680
physical samples: 9,166,720
decoded-semantic fingerprint:
ad6cfd0a81465eb9a8b64d9bca3036c30d4b6d250bc2577c4b3616cdac87919e
exact-postings fingerprint:
0d12cb61389dd518bd35f712cef97c1063f88edb0878253ea11556ac6732b400
verified-selection fingerprint:
3e00d77c14756da00cde7d6802ede875ee0a471fd829a132d123bf91318de41d
```

All 30 expected independent readbacks executed in every arm with zero normal
skips, isolation skips, or mismatches. The replay-correctness JSON is also
byte-identical across all four roots.

These roots do not close the pre-existing capture-level physical-row golden
gap. Every storage gate records
`capture_level_physical_sample_golden_gated=false` and
`writer_to_verifier_counts_reconciled=false`; the preserved
`PHYSICAL_SAMPLE_COUNT_COVERAGE_GAP` states that no version-matched
capture-level physical-row oracle exists. Byte identity, semantic
fingerprints, exact postings, and independent readbacks prove A/B equivalence
and internal persisted-corpus correctness, but not an independent expected
total for every physical row.

The roots froze the same current candidate source snapshot. Candidate source
provenance is therefore present in each root (`live_publisher.rs` SHA-256
prefix `1403f0d7`, working-tree patch prefix `da5c2c8d`), but reconstructing
A's precise source depends on the accepted baseline authority at
`live-query-candidate-250k-20260725T114802Z` (`live_publisher.rs` prefix
`03d1e3f4`). The executed A and B binary identities themselves are unambiguous
and preserved in every arm.

Like the earlier 50k diagnostic, this was P-only. It proves final persisted
corpus equivalence and the publication mechanism, not concurrent live-query
latency under Q.

## Decision

Keep the hardened candidate. The exact-certificate shortcut removes the
structurally unnecessary `O(runs)` owner scan on the reference one-partition
corpus, substantially reduces instructions and cache misses, has no observed
RSS regression, and produces byte-identical final storage and semantic
readbacks.

Do not publish the noisy ABBA's -33.66% as the formal throughput result. Before
the 4M experiment, run the same frozen candidate through the naturally quiet
mandatory 250,000-message gate. A later Q arm must measure concurrent query
latency and interference.

The next, separate generalization would be a generation-versioned persistent
owner root for truly multi-partition workloads. It is not part of this
candidate.
