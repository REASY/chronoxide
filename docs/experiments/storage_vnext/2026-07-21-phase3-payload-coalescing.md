# Phase 3 payload-coalescing result

**Decision:** promote the bounded runtime-selectable fixed policy, retain
`4096` bytes as the default, and reject an adaptive selector from this phase's
evidence. Gap `0` remains available when minimizing process-issued bytes is
more important than latency.

This is a code/runtime change only. It changes no stored bytes, locator
authority, checksum boundary, corruption precedence, or public `QueryStats`.

## Accepted evidence

The accepted evidence artifacts are:

- `pread`:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase3-pread-final-20260721T120349Z`
- forced `io_uring`:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase3-io-uring-final-20260721T122308Z`
- hardened cross-backend v3 gate:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase3-backend-compare-v3-final-20260721T132919Z`
- planner-cap-audited v12 Detailed attribution:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase3-attribution-final-20260721T131510Z`
- current-source v13 provenance smoke:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase3-v13-provenance-smoke-20260721T133754Z`

Both backend artifacts used the same release binary, SHA-256
`d734fab9ac288945acb86460e592dda52fda8a772ff72eadfa3839732e075035`,
the same 66-file, 5,569,314,896-byte Schema 8 corpus, inventory SHA-256
`28547c0fc2b738eb58948400602640c017844cd57bd49917bffdf100a6e14a0b`,
and query-corpus fingerprint
`7e5cf252e5df9bdb786e1b9deb9248f09667962ac559f339ba47312c5c0e3ca3`.
The sealed eleven-query manifest SHA-256 was
`3420740cc3e5eb38e82ca53b58d6d1a075b9007380b8745e0193ec18236a07e7`.

Each backend ran 352 fresh processes and 1,056 evaluations. For every query,
an eight-block Williams schedule balanced gaps `0`, `256`, `1024`, and `4096`
at every order position; each process recorded one CLI-cold and two warm
evaluations. Headline warm observations are the median of the two warm runs
inside each process, followed by the median across eight processes. The
latency matrix used `--query-instrumentation off`, compact query-label IDs,
demand-driven labels, Schema 8, and a zero-byte range scalar cache.

Before each process the runner issued `POSIX_FADV_DONTNEED` for every corpus
file and required `fincore` to report zero resident bytes. This proves the
recorded Linux page-cache condition; it does not prove a cold SSD or cold
device/controller cache. A CLI-cold row is also a fresh process and query
session. Warm rows reuse that process's application metadata state.

Footer validation passed outside timing. The independent readback oracle
executed 38/38 cases with zero skips, isolation skips, or mismatches in each
backend artifact. A real forced-`io_uring`, queue-depth-8 setup preflight
passed in both artifacts. The host soft `RLIMIT_MEMLOCK` was 8 MiB, below the
64 MiB benchmark recommendation; successful forced setup is therefore the
actual availability evidence, not the limit recommendation.

## Latency result

The table reports process-clustered medians. Percentages compare the retained
4 KiB default with no coalescing; negative is faster.

| Query | Backend | gap 0 cold / warm | gap 1 KiB cold / warm | gap 4 KiB cold / warm | 4 KiB vs 0 cold / warm |
| --- | --- | ---: | ---: | ---: | ---: |
| Broad raw selector | `pread` | 6,959 / 6,744 ms | 4,314 / 4,077 ms | 3,869 / 3,622 ms | -44.4% / -46.3% |
| Broad raw selector | `io_uring` | 6,948 / 6,738 ms | 4,220 / 3,998 ms | 3,818 / 3,578 ms | -45.0% / -46.9% |
| Scalar rate instant | `pread` | 1,486 / 1,319 ms | 903 / 744 ms | 908 / 746 ms | -38.9% / -43.5% |
| Scalar rate instant | `io_uring` | 1,492 / 1,321 ms | 908 / 744 ms | 914 / 753 ms | -38.7% / -43.0% |
| Scalar rate range | `pread` | 4,160 / 3,975 ms | 2,754 / 2,556 ms | 2,855 / 2,646 ms | -31.4% / -33.4% |
| Scalar rate range | `io_uring` | 4,211 / 4,006 ms | 2,845 / 2,653 ms | 2,802 / 2,610 ms | -33.5% / -34.8% |

For the broad selector, 4 KiB beat 1 KiB in every one of eight paired blocks,
cold and warm, on both backends. It was about 9.3-10.4% faster and reduced
physical spans another 97.8%. The scalar instant 1 KiB movements relative to
4 KiB were only 0.2-1.3%. Scalar range preferred 1 KiB under `pread` by about
3.4% warm but preferred 4 KiB under `io_uring` by about 1.6%.

Native Histogram and ExponentialHistogram median movements among 256 B,
1 KiB, and 4 KiB were generally within 1-2%. Equality, sparse, negative, and
no-result controls showed no material regression attributable to the setting.
The gap policy did not materially change broad-query RSS. The largest notable
RSS trade-off was scalar instant: 4 KiB was about 9-10 MiB above 1 KiB.

Schedule-position bias was at most about 0.12% cold/0.10% warm for `pread` and
0.05% cold/0.29% warm for `io_uring`. Median second-square drift was
-0.12%/-0.23% for `pread` and +0.06%/+0.05% for `io_uring`. The drift IQR was
roughly 1-2%, so smaller apparent winners are directional rather than a sound
classification rule.

## Read and submission trade-off

Physical payload planning is backend-independent; backend scheduler counters
differ as expected. Representative cold accounting was stable across all
processes and cold/warm runs.

| Query | Gap | Logical bytes | Physical spans | Physical bytes | Read/used |
| --- | ---: | ---: | ---: | ---: | ---: |
| Broad raw selector | 0 | 10,115,253 | 90,683 | 10,115,253 | 1.000x |
| Broad raw selector | 256 | 10,115,253 | 42,419 | 16,815,909 | 1.662x |
| Broad raw selector | 1,024 | 10,115,253 | 10,883 | 35,270,389 | 3.487x |
| Broad raw selector | 4,096 | 10,115,253 | 241 | 53,259,352 | 5.265x |
| Scalar rate instant | 0 | 7,202,558 | 58,621 | 7,202,558 | 1.000x |
| Scalar rate instant | 256 | 7,202,558 | 430 | 8,131,979 | 1.129x |
| Scalar rate instant | 1,024 | 7,202,558 | 20 | 8,301,127 | 1.153x |
| Scalar rate instant | 4,096 | 7,202,558 | 3 | 8,325,750 | 1.156x |
| Scalar rate range | 0 | 17,790,188 | 147,728 | 17,790,188 | 1.000x |
| Scalar rate range | 256 | 17,790,188 | 2,702 | 20,240,775 | 1.138x |
| Scalar rate range | 1,024 | 17,790,188 | 535 | 21,275,506 | 1.196x |
| Scalar rate range | 4,096 | 17,790,188 | 104 | 22,033,043 | 1.238x |

At 4 KiB, native Histogram range amplification was 1.018x and native
ExponentialHistogram range amplification was 1.144x. The negative control
reached 4.180x and the sparse regex control 3.159x, but their latency changes
were small because payload I/O was not the dominant end-to-end cost.

Forced `pread` issued one syscall submission per physical span. Forced
`io_uring` used queue depth 8; the broad 4 KiB point reduced 241 spans to 31
backend submissions. Its maximum observed session in-flight high-water was
about 6.37 MB. The cross-backend v3 gate validated all 44 query/gap points,
every accounting run and scheduler invariant, exact nonphysical semantics, and
identical backend-independent physical span/byte plans.

## Diagnostic attribution and the CPU mechanism

The sealed attribution artifact used the planner-cap-audited v12 binary,
SHA-256
`27db586fdef697451f5ede60e198712e58a742c5c310a2efc03fba1a5432fc87`.
It ran 24 fresh processes: four representative queries, both forced backends,
and gaps 0, 1024, and 4096. Each process recorded one fresh-session/page-cache-
evicted evaluation and one application-warm evaluation with observer-heavy
`Detailed` instrumentation. The gate passed, every semantic fingerprint and
logical accounting field matched, the before/after corpus inventories matched,
and the complete artifact checksum manifest verifies. These stage values are
diagnostics only; they are not comparable with the instrumentation-off latency
matrix above.

The table keeps the fresh-session/page-cache-evicted and application-warm
observations separate. Each cell is `cold / warm`; no midpoint across cache
states is used. The combined decode leaf honestly includes payload lookup,
decode, projection, and result processing.

| Query | Backend | Read pipeline, gap 0 -> 4 KiB, cold / warm | Decode/projection/result, gap 0 -> 4 KiB, cold / warm |
| --- | --- | ---: | ---: |
| Broad raw selector | `pread` | 33.5 / 25.9 -> 24.9 / 5.1 ms | 3,073.5 / 3,027.8 -> 124.6 / 77.3 ms |
| Broad raw selector | `io_uring` | 39.8 / 28.4 -> 30.1 / 6.1 ms | 3,091.9 / 3,051.4 -> 123.1 / 79.9 ms |
| Scalar rate instant | `pread` | 14.9 / 9.4 -> 5.9 / 1.3 ms | 561.6 / 557.0 -> 28.9 / 26.5 ms |
| Scalar rate instant | `io_uring` | 16.8 / 11.4 -> 5.8 / 1.2 ms | 574.8 / 562.9 -> 28.3 / 25.7 ms |
| Scalar rate range | `pread` | 49.4 / 23.9 -> 32.5 / 2.0 ms | 1,416.4 / 1,437.9 -> 66.0 / 63.0 ms |
| Scalar rate range | `io_uring` | 56.7 / 27.7 -> 23.8 / 1.9 ms | 1,391.2 / 1,374.1 -> 64.1 / 61.4 ms |

Every cold and warm broad/scalar observation moved in the same direction. The
combined decode-leaf reduction accounts for roughly 87-112% of the corresponding
exclusive-stage reduction; values above 100% are offset by movement in other
stages. Native Histogram count did not show the same scale. Its combined leaf
fell from 20.8 / 25.2 to 16.7 / 15.9 ms under `pread` and from 19.8 / 19.1 to
16.8 / 15.8 ms under `io_uring`. The evidence therefore does not support a
claim that saved syscalls or device traffic alone caused the headline win.

The frozen artifact's v1 aggregate also contains a midpoint over its one cold
and one warm observation. This report does not use that mixed-cache midpoint;
the checked-in v2 attribution gate preserves explicit `cold` and `warm` stage
records instead.

Code audit exposes a concrete mechanism consistent with the result.
`ChunkPayloadBatch::slice()` scans `self.spans` from the beginning for every
locator lookup. Its lookup cost is therefore worst-case
`O(sum(batch lookups * batch spans))`, degenerating toward quadratic work in a
batch when one physical span is retained per logical request. Schema 7 full-
record paths may search once for prefix authentication and again for decode.
At gap 0, the broad point has 90,683 logical requests and 90,683 physical spans
in aggregate; at 4 KiB it has the same logical work but only 241 spans.

The combined diagnostic leaf cannot prove how much of its reduction belongs to
span search rather than decode, projection, or result processing, so this is a
leading code-audited hypothesis rather than causal attribution. It should be
isolated with a sorted lookup/cursor comparator and focused lookup evidence.

This strengthens the current fixed-policy promotion, because the measured
implementation is decisively faster with bounded 4 KiB coalescing. It also
limits the conclusion: 4 KiB is not proven to remain optimal after span lookup
is indexed or cursor-driven. Before training an adaptive gap policy, activating
a scalar sidecar, or treating read amplification as a byte-layout defect, run
an isolated sorted-span lookup comparator and repeat the fixed gap matrix. A
successful lookup fix could move the Pareto frontier toward lower amplification.

## Correctness and authority

Across gaps and backends, the strict gates required exact equality for:

- semantic and portable fingerprints, result series, and result samples;
- every public `QueryStats` field and logical payload request/byte count;
- full canonical-row integrity, compact label-arena accounting, symbol and
  metadata non-timing accounting, and range-cache accounting; and
- the sealed query matrix, binary, corpus inventory, and corpus fingerprint.

Focused tests cover exact gap thresholds, both Schema 6 and Schema 8 planners,
selected corruption at gaps 0 and 4096, and a corrupt unselected middle chunk
that is physically over-read but never decoded or made authoritative. Invalid
configuration is rejected before backend setup. The final audit also bounds
the directly callable planner itself to `0..=4096`.

The planner-cap audit added only an out-of-range rejection before valid plan
construction. It produced final release binary SHA-256
`27db586fdef697451f5ede60e198712e58a742c5c310a2efc03fba1a5432fc87`;
the accepted matrices predate that defensive boundary check. A full repeat
with the audited binary was attempted, but the quiet-host gate correctly
aborted it when an unrelated `artracer` build began. No samples from that
partial run are used. Valid-gap production behavior and the measured
coalescing mechanism are unchanged; the direct rejection has focused test
coverage.

After the sealed v12 matrices and attribution run, the checked-in source made
one additional observability-only correction: the scheduler profile field and
raw JSON key that count cumulative physical bytes are now
`total_physical_bytes_executed`, and the raw schema is v13. Neither sealed
binary contains that rename. It changes no planner, read, decode, query result,
`QueryStats`, or valid-gap behavior; current-source tests cover the v13 shape.
The checked-in per-backend and cross-backend output schemas are correspondingly
v2 and v4; the accepted backends remain immutable v1 artifacts and the cited
v3 artifact remains their immutable comparison.

The current release binary SHA-256 is
`72d8bc69242c2f633dc5ac0cc1650063c35cc2fe39a718c90df2ef7aa0bc2c70`.
The untimed v13 provenance smoke ran the same scalar instant query as the v12
attribution artifact and matched its semantic and portable fingerprints,
result shape, every public `QueryStats` field, payload accounting, and every
scheduler value after applying the one field rename. Its artifact checksum
manifest verifies. This is serialization/provenance evidence, not another
latency sample.

## Why adaptive was rejected

There is no universal Pareto winner when physical bytes are an objective:
larger gaps monotonically trade more bytes for fewer submissions. The 4 KiB
point is the latency/submission endpoint; gap 0 is the minimum-byte endpoint;
256 B and 1 KiB are intermediate choices.

The observed small-query winner changed by backend and often fell inside the
1-2% drift band. Request count, aggregate density, amplification, and backend
did not yield a stable threshold that explains both the decisive broad result
and the scalar/native reversals. Training such a rule on this single roughly
75-minute corpus would be overfitting, not adaptation.

An adaptive policy remains rejected until multiple independent corpora, a
declared latency/bytes/RSS objective, and holdout validation produce a stable
rule with an explicit maximum-amplification budget. It should also follow the
span-lookup comparator above so the learned rule does not encode an avoidable
linear-search cost. This phase does not activate a scalar sidecar or any new
on-disk format.

## Promoted contract

- `chunk_payload_coalesce_max_gap_bytes` is immutable per query session.
- Accepted values are `0..=4096`; the default remains `4096`.
- The same value drives Schema 6 and Schema 7/8 payload planning and both
  `pread` and `io_uring` backends.
- Coalesced gap bytes are over-read only. They do not add selected chunks,
  logical charges, decoding, or corruption authority.
- The accepted latency artifacts use raw query schema v12. Current raw schema
  v13 retains the configured gap and complete scheduler accounting while
  renaming the cumulative byte counter to `total_physical_bytes_executed`.
  Submission depth and peak in-flight bytes are session high-water gauges, not
  subtractable counters.
- Lower fixed gaps, including zero, remain explicit operational choices for
  byte-sensitive environments.
