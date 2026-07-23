# Chronoxide performance program — live status

- **Audit date:** 2026-07-24
- **Accepted Phase 1 audited baseline:**
  `a8bd6d44d6c06375a09104a4a9c58ecbe6268021`
  (`2026-07-17`, `chore(lint): satisfy strict workspace checks`)
- **Latest measured comparator baseline:**
  `bf51a8e65b1b57639eb131a62a14291646372d86`
  (`2026-07-24`, `perf(storage): flatten cold series rows`)
- **Latest promoted candidate evidence:** packed cold-series rows, frozen as
  the `bf51a8e` control plus patch SHA-256
  `fe164ee845c88bc2f27f0ecef8fb1801c6d85f69f8e510829b9368edc70d24ca`
- **Current sealed-store contract:** Schema 8
- **Normative authorities:**
  [storage.md](docs/superpowers/specs/storage.md),
  [clock.md](docs/superpowers/specs/clock.md), and
  [PromQL coverage](docs/promql-coverage.md)

This file is the live performance status and experiment queue. Dated reports
are evidence, not current authority or an automatic backlog. A candidate moves
from open to promoted only after its correctness and real-corpus performance
gates pass. A failed or superseded experiment stays recorded so it is not
accidentally repeated.

The accepted performance baseline uses the audited commit plus the frozen
tracked instrumentation/harness patch whose digest and binary hashes are
recorded in the Phase 1 reports. It includes a fresh four-million-message
replay and complete Schema 8 query matrix. Phase 2 is promoted from its own
same-binary gate, whose exact candidate binary and tracked source patch are
sealed with the result. Historical percentages below came from different
sequential baselines, prefix sizes, binaries, and schedules; they must not be
added together or presented as one cumulative speedup.

## Executive status

Schema 8 has already captured the material on-disk metadata wins. The current
baseline and query-stage profiles are complete, governed compact query label
IDs have passed their promotion gate, and bounded fixed payload coalescing has
been retained after the Phase 3 backend/gap matrix. The current sequence of
seal-layout changes has removed the owned metric-order clone, avoidable
index/series lifetime overlap, one-chunk nested vectors, per-series cold-row
vectors, and fixed-width `u32` cold-code backing. The last two changes lower
later Series-stage crests rather than the earlier process-wide maximum. The
next credible improvements are code-side and measurement-led:

1. test one-pass multi-step range execution;
2. tune allocator/head behavior against realistic partition layouts; and
3. evaluate sample/timestamp codecs against a sealed real corpus.

Do not start a new disk schema merely because a component is large. A new
format requires a measured read or capacity bottleneck that code-only work
cannot resolve, an explicit decodable layout, a new version boundary, and an
update to [storage.md](docs/superpowers/specs/storage.md) before code changes.

## Current production defaults

- Schema 8 is the writer, reader, CLI, and HTTP default. Schema 7 is an
  explicit prior-format comparator; Schema 6 is readable only through the
  complete-footer-validated `schema6-ab` policy.
- Schema 8 uses `symbols.bin` v3, `series.bin` v3, overflow-only
  `chunk_index.bin` v2, and `indexes.puffin` v9 with deterministic RAW32 versus
  delta-ULEB128 exact postings.
- Metadata access is immutable positional I/O under one aggregate byte and
  file-descriptor governor. Complete-directory materialization, shared seek
  cursors, and ungoverned per-segment caches are not valid shortcuts.
- Query label materialization defaults to `DemandDriven`. Non-empty Schema 7/8
  query sessions and `chronoxide-query` default to governed `CompactIds`;
  Schema 6, empty stores, and standalone interners retain `OwnedStrings`.
  Because the benchmark CLI flag itself defaults to compact IDs, an explicit
  `schema6-ab` run must also select `--query-label-storage owned-strings`.
  `OwnedStrings` and `SharedAtoms` remain explicit same-binary comparators.
- Compact four-sample numeric head staging and the adaptive head-series table
  default to enabled.
- Query payload planning uses one immutable fixed gap per session. The default
  remains 4096 bytes, the accepted range is `0..=4096`, and lower values are
  explicit byte-sensitive comparators. Phase 3 rejected a learned/adaptive
  selector as unsupported by the available corpus.
- The normal flat-interned label-pair store remains contiguous and uses
  interned-symbol-ID hashing with exact row equality. The paged pair store is
  diagnostic only.

## Promoted work in the current baseline

### On-disk and read architecture

| Change | Evidence-backed result | Current disposition |
| --- | --- | --- |
| Paged symbols v3 | `symbols.bin` -4.45%, complete prefix corpus -0.052%; cold process-issued symbol bytes -85.1% and retained symbol charge -97.4% | Retained as the Schema 8 symbol access architecture, not advertised as a standalone capacity win |
| Schema 7 inline series metadata and overflow-only chunk index | Two-million-message corpus -11.94%; `chunk_index.bin` 535,686,024 bytes to 384 bytes; semantic fingerprint and `QueryStats` equivalence passed | Foundation retained by Schema 8; Schema 7 itself is now the comparator |
| Schema 8 adaptive exact postings | Exact postings -72.90%, `indexes.puffin` -57.71%, complete four-million-message corpus -15.60%; exhaustive decoded membership matched | Production default; measured cached postings latency classified as neutral |
| Aggregate metadata/FD governance | Immutable generation-bound positional reads, sticky corruption, aggregate charges and hard descriptor bounds | Normative architecture, not open performance work |

The original paged-symbol prefix changed only `symbols.bin`; `series.bin`,
postings, chunk indexes, and payloads were byte-identical by design. Its small
total-size change was therefore structurally expected. That experiment tested
selective dictionary access, not prefix compression or the later Schema 7/8
series and postings changes. See
[the prefix result](docs/experiments/storage_vnext/2026-07-13-prefix-results.md),
[the Schema 7 result](docs/experiments/storage_vnext/2026-07-14-schema7-prefix-results.md),
and
[the Schema 8 result](docs/experiments/storage_vnext/2026-07-15-schema8-adaptive-postings-results.md).

### Ingest and head

| Change | Principal measured evidence | Current disposition |
| --- | --- | --- |
| Prepared resource/metric label plans | Four-million-message wall -10.60%, user CPU -10.33%, label interning -29.35%, corpus byte-identical, readback 38/38 | Promoted |
| Cheap `SeriesRef` maps plus fused timestamp update | Four-million-message wall -5.20%, cycles -5.36%, mean head call -30.38%, byte-identical | Promoted |
| Inline four-sample numeric staging | Four-million-message wall -4.90%, cycles -5.44%, byte-identical | Promoted and enabled by default |
| Interned-symbol-ID label-set fingerprint | One-million-message task clock -6.42%, instructions -11.21% | Promoted; canonical ordered row equality remains authoritative |
| Keyed AHash label-set lookup | One-million-message task clock -2.42%, no material RSS change | Promoted; exact row equality remains authoritative |
| Keyed AHash symbol lookup | One-million-message task clock -2.50%, no material RSS change | Promoted; exact string equality remains authoritative |
| Adaptive last-timestamp table | Approximately 96.8 MiB lower one-million-message peak RSS; small directional CPU win | Promoted; sparse pages retain the hash representation |
| Adaptive head-series table | One-million-message task clock -14.23%, peak RSS about 362.5 MiB lower, readback 38/38 | Promoted and enabled by default; multi-partition gate remains open |
| Owned single-sample transfer | Small instruction/allocation reduction with exact output | Promoted as cleanup, not a broad latency claim |
| Owned typed-bucket transfer | One-million-message task clock -0.25%, exact output | Promoted as cleanup, not a broad latency claim |
| Borrowed canonical cold-series planning | Whole-process requested-live bytes at the selected large-window seal peak -845.28 MiB/-16.48%, whole-process allocation calls -4.87%, instructions -0.354%; whole replay neutral and writer flush +0.907% on the accepted noisy host; exact bytes and 40/40 readbacks | Promoted for seal-phase headroom and allocation work; the fused shape pass and scratch reuse are required |
| Indirect sealed-series metric ordering | Heaptrack requested-live maximum -845.365 MiB/-16.486%, allocation calls -4.535%, accepted ABBA wall -8.887%, exact storage/semantic fingerprints | Promoted; compact indices and shared projection plans replace complete owned ordering-key clones |
| Segment-flush lifetime ordering | Requested-live maximum -220.010 MiB/-5.137%; allocation calls neutral; exact bytes and 40/40 readbacks | Promoted; selector indexes are written and released before cold-series planning |
| Inline-one chunk-entry store | Requested-live maximum -538.038 MiB/-13.244%, allocation calls -1.787%; exact bytes and 40/40 readbacks | Promoted for the dominant one-chunk-per-series corpus shape |
| Flat cold-series rows | Largest affected Series-stage crest -145.905 MiB/-4.734%, allocation calls -1.825%; process-wide maximum unchanged; exact bytes and 40/40 readbacks | Promoted; one exact row-major `u32` buffer per keyset replaces per-series row vectors |
| Packed cold-series rows | Largest affected Series-stage crest -231.163 MiB/-7.873%; largest code payload -231.340 MiB/-68.244%; process-wide maximum unchanged; exact bytes and 40/40 readbacks | Promoted; value codes are built directly in final 0/1/2/4-byte form |

The detailed ingest reports are under
[storage_vnext](docs/experiments/storage_vnext/README.md). The current baseline
must be remeasured rather than reconstructed by summing these sequential A/Bs.
The first borrowed-only cold-plan candidate is explicitly superseded: despite
removing the clone, its four fragmented-row passes raised `series_ms` 5.05%,
writer flush 1.29%, and cache misses 2.23%. The fused locality repair and final
evidence are in
[the cold-plan result](docs/experiments/storage_vnext/2026-07-23-cold-plan-fastpath-results.md).
The later seal-memory sequence is recorded in the
[indirect-sort](docs/experiments/storage_vnext/2026-07-24-indirect-sort-results.md),
[flush-lifetime](docs/experiments/storage_vnext/2026-07-24-flush-lifetime-results.md),
[inline-one](docs/experiments/storage_vnext/2026-07-24-inline-one-chunk-entry-results.md),
[flat-row](docs/experiments/storage_vnext/2026-07-24-flat-cold-row-store-results.md),
and
[packed-row](docs/experiments/storage_vnext/2026-07-24-packed-cold-row-store-results.md)
reports. Their sequential percentages use different baselines and must not be
summed.

### Query execution

Demand-driven label ownership is promoted for the exact proved scalar terminal
aggregation path and for root native Histogram/ExponentialHistogram `count`
and `group` with `All` or `by(...)` over a direct pure native selector or
native `rate()`/`increase()`. Mixed-kind rows and all unproved shapes use full
labels.

The native A/B reduced owned label pairs by 30–31% and improved the eligible
query geometric mean by 3.39% cold and 4.37% warm on the noisy test host while
preserving complete row integrity checks, semantic fingerprints, and ordinary
`QueryStats`. The exact whitelist and escape/corruption invariants are in
[the delayed-label design](docs/superpowers/specs/2026-07-15-delayed-selective-label-materialization-design.md).

The accepted Phase 1 corpus strengthens that evidence. Demand-driven real
scalar aggregation reduced cold/warm latency by 19.10%/20.43% for the instant
shape and 14.06%/14.71% for the seven-step range, with about 60% lower process
peak RSS in both cases. Native Histogram count improved 3.43%/4.71%; native
ExponentialHistogram count improved 1.01%/0.91%. Complete canonical row/pair
integrity, result fingerprints, and public `QueryStats` remained identical to
the mandatory Full controls.

## Rejected, comparator-only, or explicitly deferred work

| Candidate | Evidence | Disposition |
| --- | --- | --- |
| Paged ingest label pairs | Estimated store allocation -39.91%, no peak-RSS reduction, authoritative task clock +0.27% | Keep contiguous default; comparator only; do not repeat the same layout |
| Query-session `SharedAtoms` | Broad selector peak RSS -54.91%, but cold +25.56% and warm +10.71%; about 22.1 million cold atom lookups | Rejected as a default; retained only as a comparator after governed `CompactIds` replaced it |
| Source payload `Vec` reuse | Instructions -0.056%, no RSS win | Removed; do not repeat |
| Persistent capture Zstd context | Task clock +1.38% and more instruction/branch work | Removed; do not repeat |
| Event-skew statistics optimization | Fresh profile did not support the presumed 7% bottleneck; allocator/label/hash/equality work dominated | Rejected hypothesis; profile again before revisiting |
| Linked jemalloc as default | One-million-message task clock -14.28%, but peak RSS +10.09% | Opt-in comparator only pending bounded arena/decay/purge tuning |
| Another postings codec | Schema 8 already removed 72.90% of postings and current query latency is not postings-bound | Defer until a fresh profile identifies postings decode/set work as material |
| Unprofiled `io_uring` redesign | No evidence that submission mechanics dominate; useful concurrency is not yet exposed | Defer; compare only inside the coalescing experiment |

The paged-pair, source-reuse, allocator, and follow-up query evidence is
recorded in the dated reports under
[docs/experiments/storage_vnext](docs/experiments/storage_vnext/README.md).

## Query-observability checkpoint

The 2026-07-21 instrumentation checkpoint adds an explicit
`QueryInstrumentationMode`. Production and latency-comparison sessions default
to `Off`; that path performs no stage clock reads and returns the established
profile-free verified-series value. Diagnostic runs select `Detailed` before
any prewarm, prefetch, or query work, and the mode then freezes with the
session. Off and Detailed have focused semantic-fingerprint, portable-
fingerprint, `QueryStats`, result-cardinality, and mode-freezing coverage.

`SegmentStoreQueryProfile` now exposes mutually exclusive leaf attribution for
the stages below. Candidate index/FST/postings/set work and schema-neutral
metadata-visit overhead have their own leaves; they are not mislabeled as
authoritative matcher verification or canonical-row decode. Existing open/read
durations remain inclusive diagnostics and are not additive with these leaves.

| Stage | Current attribution |
| --- | --- |
| Canonical identity | Complete row decode, all-symbol resolution, canonical identity hash and stored-ID verification |
| Symbols | String-to-ID lookup and ID-to-string resolution, separated where possible |
| Candidate selection | Authenticated index/FST/postings reads and series-ref set operations, excluding symbol lookup |
| Metadata visit overhead | Schema-neutral visit, cache/governor and callback-dispatch residual after explicit row leaves |
| Matcher | Equality/negative/regex verification after complete integrity checking |
| Labels | Full versus selective construction, query-label interning/ID translation, materialized and omitted bytes |
| Locator planning | Series entry, authoritative chunk-directory pair, chunk filtering, request construction |
| Payload | Logical and process-issued bytes plus a combined read-pipeline leaf; decode/projection/result processing is a second honestly combined leaf pending the Phase 3 split |
| Sealed source merge | Per-chunk/segment result merge, cross-segment merge, dedupe and projection merge |
| PromQL | Group-key construction, grouping, range-function and evaluator time |
| Results | Final identity/label construction and API/benchmark serialization time |
| Metadata runtime | Hits, misses, successful/failed loads, evictions, single-flight waits, admission/refusal counts, current/peak charges by stable class, sticky-corruption charges, and FD state |
| Range scalar cache | Hits/misses/admissions/bypasses/refusals, logical hit/miss bytes, exact peak/final charge, and process-governor leases |

`chronoxide-query --query-instrumentation off|detailed` records the stable raw
leaves, exclusive total, and unclassified remainder per run. Off runs require
every leaf to remain zero. Detailed runs fail artifact publication if the
exclusive sum exceeds the measured query wall. Detailed timing is deliberately
observer-heavy and is never the latency baseline. Payload decode currently
includes projection and result-processing work; the raw field and Markdown
name that combined boundary instead of claiming pure decoder CPU.

Benchmark semantic and portable fingerprint traversal is timed separately as
`post_query_fingerprint_ns`, outside query wall time. The CLI does not perform
Prometheus HTTP serialization. The API measures response construction plus
JSON encoding separately and exposes exact nanoseconds in
`x-chronoxide-serialize-duration-ns` as well as `Server-Timing`.

Cache counters must be sampled as deltas around a run. Current retained/FD
resources are start/end gauges and lifetime peaks are context, not per-query deltas; none
may be summed per segment or mistaken for monotonic work. Store-level reporting
must deduplicate shared reader state. `successful_loads` is a load outcome, not
a resident-admission count. The report therefore uses distinct
resident-admission, refusal, and disabled-residency bypass counters at the
actual post-validation governor handoff.

Implemented focused coverage includes:

- `add` and `delta_since` saturation for every new monotonic field;
- preservation of after-snapshot values for current resource gauges;
- zero-work/no-result queries;
- failure paths, including touched corruption and budget refusal;
- full versus demand-driven equivalence with equal integrity work; and
- report/raw-schema serialization tests with stable field names.

The touched-corruption test verifies that a CRC failure on the selected series
page remains corruption, freezes the session policy, performs no payload or
result work, and still records a nonzero exclusive Detailed leaf bounded by
the failed call's wall time. The budget-refusal test reserves all but one byte
of the store-wide in-flight metadata budget, verifies a refusal before any
metadata I/O or sticky admission, checks the same timing bound, releases the
competing reservation, and succeeds on retry. These are test-oracle gates;
failed benchmark queries intentionally publish no partial raw artifact.

The sealed-store session now attributes cross-segment scheduling, decode, and
source merge. The older `*_with_head` test-only reader surface does not return a
`SegmentStoreQueryProfile` and is not exercised by the sealed-corpus query CLI;
live-head/OOO stage attribution is therefore explicitly deferred to the Phase
5 head validation rather than being implied by the Phase 1 sealed-store report.

Both checkpoint gates are complete. The clean pre-instrumentation versus
current-Off ABBA changed broad-query cold latency by +0.874%, warm latency by
+1.305%, and peak RSS by -0.0049%; all were inside the declared 3% latency and
5% RSS limits, all semantic/read counters matched, and every Off stage leaf was
zero. See
[the observer-cost report](docs/experiments/storage_vnext/query_instrumentation_off_ab.md).

The accepted Detailed matrix covers broad, matcher, scalar, native Histogram,
and native ExponentialHistogram paths. Its mutually exclusive attribution
stays within query wall time, while exact/portable fingerprints and public
`QueryStats` match Off and Full controls. Warm broad/range work is dominated by
symbol resolution, canonical row decode/identity, and label construction;
metadata reads fall to zero. Detailed timings are diagnostic only and were not
used as the latency baseline.

## Phase 1 — establish the current baseline

**Status: complete and accepted.** Full provenance, configuration, raw artifact
locations, correctness evidence, and descriptive distributions are in
[the replay report](docs/experiments/storage_vnext/2026-07-21-phase1-replay-baseline.md)
and
[the query report](docs/experiments/storage_vnext/2026-07-21-phase1-query-baseline.md).

### Ingest baseline

Three measured replays and a separate profile replay produced byte-identical
Schema 8 corpora: 66 files, 5,569,314,896 bytes, eight deterministic segments,
and manifest SHA-256
`8b0789e2f6c404a144e0d2e87f152a83e9f0bedb9c5ab2c6512608056cae3289`.
The median replay was 510.52 seconds (7,835.15 messages/s), median task clock
was 510.980 seconds, IPC was 2.010, and process-tree peak RSS was 10.834 GiB.
All 4,000,000 messages, 155,197,127 observed datapoints, acceptance/rejection
counters, 154,902,724 stored samples, and 17,286,077 chunks matched across
runs.

Untimed exhaustive verification covered every segment, series, chunk, sample,
and exact postings list. Footer validation passed, and the independent
readback oracle executed 38/38 cases with zero skips or mismatches. A separate
24,000-sample perf profile lost no samples; glibc allocation/free entry points
accounted for more than 30% self CPU, with label/symbol interning, hashing, and
equality also material. This supports Phase 5 allocator work and does not
support speculative event-skew or protobuf rewrites.

### Query baseline

The corrected fixed matrix ran 204 fresh processes and 612 evaluations over 17
query shapes. Every process began with zero corpus residency according to
`fincore`; all runs used pread, no prewarm/prefetch, a 64 MiB retained metadata
budget, and explicit range-cache budgets. Exact/portable fingerprints, result
cardinality, public `QueryStats`, Full controls, and range-cache semantics
passed. Footer validation and 38/38 independent readbacks passed outside
timing. An earlier completed artifact was rejected because its manifest, not
the implementation, misclassified virtual `_count` and nested p95 shapes as
selective; no measurements from it were admitted.

The broad raw selector returned 90,569 series, peaked at 2,007.4 MiB RSS, and
had 4,626.6 ms cold/4,176.2 ms warm medians. It owned 8,126,970 label pairs and
415,320,441 bytes of string content. Its logical payload was only 10,115,253
bytes versus 53,259,352 process-issued bytes (5.265x amplification), yet its
Detailed CPU was dominated by symbol resolution and label/identity work. The
30-minute real-scalar range spent roughly 95% of Detailed wall in repeated
symbol/row/identity/label and metadata traversal across seven steps. This is
the strongest current evidence for Phase 2 and especially Phase 4.

A 16 MiB range scalar cache reduced issued bytes 57.97% on the virtual
`_count` control but changed cold/warm latency by +0.66%/-0.05% and raised RSS
2.26%. Phase 3 must therefore promote only an end-to-end Pareto improvement,
not a byte-count win. No current Phase 1 result activates a new disk format.

## Phase 2 — governed query-local compact label IDs

**Status: promoted on 2026-07-21.** The same-binary, counterbalanced Schema 8
gate ran 176 fresh processes and 528 evaluations over eleven query shapes.
Exact and portable fingerprints, result shapes, ordinary `QueryStats`, footer
validation, and all 38 independent readbacks passed. On the broad raw selector,
`CompactIds` improved the cold median by 14.82%, the warm median by 10.20%, and
process peak RSS by 71.48% versus `OwnedStrings`. Sparse, negative, and native
typed controls also improved; the small-query movements remained below the
predeclared materiality gates. The closest latency control was scalar instant
warm at +2.72%, below the 3% limit. See
[the Phase 2 report](docs/experiments/storage_vnext/2026-07-21-phase2-compact-query-label-ids.md).

The promoted path carries compact pairs through merge, grouping, evaluation,
fingerprinting, and API serialization without a retained owned compatibility
slice. It uses a query/session-wide modeled retained-allocation budget, stable
generation-bound source-symbol translations, and shared arena ownership so
results can outlive the session. The final gate observed zero arena refusals
and zero compatibility materializations; the maximum modeled current/peak
charge was 139,361,608/139,361,928 bytes under the 512 MiB limit. This is a
runtime representation change only: it changed no persisted byte or storage
format semantic.

### Hypothesis

Broad full-label queries are dominated by repeated string ownership, hashing,
grouping, and result construction. Carry query-local `(u32 name_id, u32
value_id)` pairs through matching, merge, grouping, and evaluation, resolving
strings only at an observable boundary.

The current broad control peaks at 2,007.4 MiB, creates 8,126,970 owned label
pairs with 415,320,441 content bytes, and attributes 13–15% of Detailed wall
directly to label construction in addition to dominant symbol traversal. The
earlier `SharedAtoms` experiment proved the RSS opportunity but regressed
latency because roughly 22.1 million hash lookups were added. This phase must
replace repeated downstream strings with dense IDs while avoiding that lookup
pattern; it cannot skip mandatory source-symbol resolution or identity checks.

### Required design

- Keep `OwnedStrings` as a same-binary runtime comparator.
- Use one query/session aggregate byte governor, not a budget per segment.
- Bind each segment-symbol translation table to the immutable segment
  generation. A physical locator hit from another generation is invalid.
- Resolve and integrity-check every canonical source pair and verify the
  complete authoritative stored series identity before any matcher result,
  cache reuse, merge, or omission.
- Hash UTF-8 only on first admission of a distinct segment symbol into the
  query arena. Downstream equality, ordering, grouping, and matching operate on
  compact IDs while preserving canonical string-byte semantics.
- Keep complete source identity distinct from selectively visible labels.
- Preserve cross-segment symbol-ID differences, hash-collision verification,
  missing versus explicit-empty labels, `__name__` transformations, and exact
  corruption/error precedence.
- Returned results must outlive the query session via explicit shared arena
  ownership, for example `Arc`; no dangling borrowed IDs are allowed.
- API and benchmark fingerprint/serialization paths resolve compact pairs
  directly and must not first construct a complete owned `(String, String)`
  copy.
- Report arena hits/misses, translations, pairs, unique strings/content bytes,
  current/peak charge, admission refusals, and final retained ownership.

### Promotion gate (passed)

Require full semantic/portable fingerprints and public `QueryStats` to match
`OwnedStrings`, complete corruption and cache-isolation tests to pass, and the
real broad-label matrix to show a repeatable CPU/latency or bounded-RSS benefit
without material regression on small/full-demand or demand-driven queries.
Counter reduction alone is not a win.

## Phase 3 — adaptive payload-read coalescing

**Status: complete on 2026-07-21.** The bounded runtime-selectable fixed
implementation is promoted; the default remains 4096 bytes. An adaptive
selector is rejected from the current evidence. See
[the Phase 3 report](docs/experiments/storage_vnext/2026-07-21-phase3-payload-coalescing.md).

Before Phase 3, the planner hard-coded a 4 KiB gap. The accepted implementation
hard-caps a runtime-selectable gap at 4 KiB. It ran one binary with runtime
gaps `0`, `256`, `1024`, and `4096` bytes on the fixed query schedule. Compare
`pread` and `io_uring` separately; do not mix backend changes with the gap A/B.

The Phase 1 gap produced 5.265x broad, 4.180x negative-matcher, 3.159x sparse-
regex, and 3.889x virtual-range amplification, while native range shapes were
near 1.0x. A range cache cut one control's issued bytes 57.97% without a
latency win. These are experiment-selection signals, not evidence for a new
default.

For each point record logical requests/bytes, physical spans/bytes, read/used
amplification, read/decode CPU, latency, RSS, scheduler decisions, and actual
OS/device-cold evidence where available. A policy may depend on request size,
density, backend, and an explicit amplification budget.

Promote a fixed or adaptive policy only if it is on the measured Pareto
frontier across broad, sparse, scalar, native, negative, and no-result shapes.
Do not infer the need for a scalar sidecar from amplification alone.

The accepted matrix ran 352 fresh processes and 1,056 evaluations per backend
with one binary, four fixed gaps, an eight-block Williams schedule, zero
post-eviction corpus residency, footer validation, and 38/38 independent
readbacks. Cross-backend semantics, public `QueryStats`, logical payload
accounting, and all non-timing metadata/label accounting matched exactly.

At 4096 bytes, broad cold/warm latency improved 44.4%/46.3% under `pread` and
45.0%/46.9% under forced `io_uring` versus no coalescing; physical spans fell
from 90,683 to 241 at 5.265x read/used amplification. Scalar instant improved
roughly 39%/43%, and scalar range roughly 31-34%, while their 4096-byte
amplification was only 1.156x and 1.238x. The broad 4096 point beat 1024 in
all paired blocks by about 9-10%.

Small 1024-versus-4096 winners reversed by backend and mostly sat inside the
observed 1-2% drift band. No stable rule based on request count, density,
backend, or amplification explained those reversals without overfitting one
corpus. Adaptation therefore requires multiple independent corpora, a declared
latency/bytes/RSS objective, and holdout validation. No scalar sidecar or disk
format work was activated.

A planner-cap-audited v12 binary ran an observer-heavy attribution gate over
the affected stages.
For broad and scalar queries, the combined payload lookup/decode/projection/
result leaf fell about 95-97% between gap 0 and 4096 in both cold and warm
observations, accounting for roughly 87-112% of the corresponding exclusive-
stage reduction after offsetting movement elsewhere.
The read-pipeline leaf itself changed by only tens of milliseconds. Code audit
found that `ChunkPayloadBatch::slice()` scans physical spans from the beginning
for every locator lookup, making lookup worst-case
`O(sum(batch lookups * batch spans))` and effectively quadratic within a batch
when gap 0 produces one span per request. The combined stage does not prove
that this lookup dominates, but the mechanism and span trend make it the
leading next hypothesis. The promoted 4 KiB default is therefore the best
measured setting for the current implementation, not proof that its 5.265x
broad read amplification is optimal after indexed or cursor-driven span
lookup. Test that lookup change as an isolated comparator before revisiting
adaptive gaps or a scalar sidecar.

## Phase 4 — one-pass multi-step range execution

The narrow `one-pass-assume-scalar` comparator for root
`sum/count by(...)(rate(selector[window]))` shapes is implemented and has one
admitted real-corpus result. On dense 30-minute queries it reduced warm median
latency by 68.28% for `sum` and 89.37% for `count`; exact/portable fingerprints,
result shape/order, independent readbacks, corpus bytes, and all declared
accounting classifications passed. Sparse 6-hour and 24-hour scheduler controls
also improved, but are not dense long-range evidence. See
[`2026-07-23-phase4-range-one-pass-results.md`](docs/experiments/storage_vnext/2026-07-23-phase4-range-one-pass-results.md).

Disposition: **defer, diagnostic comparator only**. Production promotion is
forbidden until the union representation is governed before allocation,
finite `QueryLimits` and error precedence are covered, public `QueryStats`
semantics are specified, and the sealed comparison passes on at least 24 dense
event-time hours. Repeated execution remains the default and every unproved
expression retains that path.

## Phase 5 — allocator and head topology

Run system allocator and linked jemalloc from otherwise equivalent release
builds. Sweep bounded jemalloc settings such as arena count, dirty/muzzy decay,
background threads, and explicit purge behavior. Record task clock, cycles,
allocation profile, peak and time-series RSS, retained/active allocator bytes,
page faults, and post-seal release behavior. A CPU win with unbounded or
operationally unacceptable RSS is not promotable.

The admitted
[250k allocator screen](docs/experiments/storage_vnext/2026-07-23-phase5-allocator-screen.md)
nominates J1 (`narenas:4`) for the two-stage 4M confirmation gate. J1 improved
workload CPU by 7.783% with a 1.029% HWM increase, but J2/J3 released far more
post-drop memory and partial attempts showed policy-rank/dispersion
instability. No default change is authorized until J1 passes both the
stats-enabled and plain no-stats 4M stages.

The separately profiled cold-series seal family has advanced through the
[canonical fast path](docs/experiments/storage_vnext/2026-07-23-cold-plan-fastpath-results.md),
indirect metric ordering, flush-lifetime reordering, inline-one chunk rows,
flat cold rows, and packed cold rows. The latest isolated packed-row step
reduced the affected Series-stage crest by 231.163 MiB/7.873% and the retained
code payload by 231.340 MiB/68.244% without changing one storage byte. The
process-wide requested-live maximum remained unchanged because it occurs
before cold-series planning. These are seal-phase headroom results, not
allocator-policy or formal writer-speed claims.

The packed plan still builds complete `BTreeMap<value_symbol, code>` reverse
dictionaries beside already-sorted value arrays. Fresh allocation attribution
must prove that family remains material before comparing binary search or a
more compact reverse index. Width-array arenas and other few-thousand-
allocation cleanup are lower priority and must remain separate experiments.

Exercise adaptive last-timestamp and head-series tables with realistic
multi-partition/strided `SeriesRef` layouts, skewed partitions, sparse pages,
promotion thresholds, long-lived rotations, and OOO lanes. The current real
capture evidence is effectively single-partition and does not close this gate.

After re-profiling, introduce a slab/arena or additional protobuf ownership
work only for a measured residual allocation family. Do not speculatively
rewrite protobuf decoding.

## Phase 6 — sample and timestamp codecs

`chunks.bin` was 64.25% of the historical four-million-message Schema 8 corpus,
so codec work has material capacity potential. Run real sealed-corpus A/Bs for
Raw versus Gorilla float blocks and credible timestamp block encodings.

The current-format four-million-message
[Float fit screen](docs/experiments/storage_vnext/2026-07-23-phase6-float-fit-screen.md)
retains Gorilla and defers adaptive RawF64/Gorilla selection. All-Raw adds
845,906,962 bytes, or 15.1889% of the complete corpus, while the exact
per-chunk adaptive minimum saves only 361,439 bytes, or 0.00649%. An all-Raw
runtime A/B is therefore low priority unless fresh profiling identifies Float
decode as a bottleneck capable of justifying that capacity cost. Timestamp
candidates remain a separate decision.

The separate
[timestamp fit screen](docs/experiments/storage_vnext/2026-07-23-phase6-timestamp-fit-screen.md)
selects global fixed-step residual bitpacking as the first prototype and
delta-of-delta ZigZag ULEB128 as its mandatory comparator. Fixed-step saves
218,865,331 native timestamp bytes, or 3.9299% of the complete corpus, but is
only 6,017,795 bytes ahead of delta-of-delta. Adaptive selection remains
deferred until per-block evidence, a real selector layout, and runtime results
exist. No timestamp on-disk change is authorized by the size model.

Inventory per kind/block: point count, raw bytes, encoded bytes, selected
codec, value/timestamp distributions, and schema/layout. Measure replay and
seal wall/CPU, cycles per sample, branch/cache misses, peak RSS, range-startup
cost, scalar/full decode, and cold/warm end-to-end queries. Any adaptive choice
must be deterministic, specified byte-for-byte, and select from complete
encoded sizes with a canonical tie rule.

Do not promote from a microbenchmark or byte reduction alone. Require
deterministic bytes/round trips, corruption tests, replay/readback equivalence,
and acceptable ingest and query CPU.

Formal replay timing must start behind a causal monitor barrier. Bind the held
replay root plus distinct RSS and capacity monitors by PID, PPID, and process
start time in an immutable atomic control; release only after both monitors
flush their first root-bound sample. Each monitor and the run-wide
conflict/capacity guardian must use an exact 100 ms cadence, record at least two
samples plus a terminal boundary, and reconstruct an edge-inclusive maximum gap
of at most 200 ms. Cleanup is identity-bound, deepest-first, root-before-monitor,
and bounded; PID reuse, dead states, partial/mutated controls or markers, and
loss of the guardian's captured runner-parent identity invalidate the result.

The source-bound codec gate fixes capture and corpus residency after eviction
at exactly zero bytes and requires Linux `Dirty+Writeback` to be at most
67,108,864 bytes. It records the producer's `getconf PAGESIZE`; final admission
matches every residency row to the canonical inventory and treats
`fincore --bytes` as page-granular, allowing a file no more than its logical
size rounded up to that recorded page size. It independently reconstructs the
canonical raw evidence matrix: eight capture-residency admissions, 40 query
pre-run/post-eviction corpus admissions, 40 post-run corpus observations, and
50 writeback admissions. Different ceilings, missing or extra rows, incorrect
totals or paths, and a writeback poll that continues after its first passing
sample fail formal admission.

Only the committed `phase6_codec_queries.json` is admissible, and every range
query fixes the range scalar cache at zero. A formal source-bound result also
requires `PERF_STAT_MODE=required`, effective perf `on`, and the exact ordered
counter set `task-clock`, `cycles`, `instructions`, `branches`,
`branch-misses`, `cache-references`, `cache-misses`, `page-faults`,
`context-switches`, and `cpu-migrations`; the raw preflight, replay, and query
TSVs are reparsed during final admission. One canonical `perf` path, SHA-256,
and one-line version are plan/settings authorities, and the identity is
rechecked at every seal and admission. Process-issued read calls and span
bytes are not device-I/O measurements, and `fincore` only observes operating-
system page-cache residency for the inventoried files. Neither proves a
block-device cache miss or a cold media/controller cache.

## Phase 7 — conditional format work

Only measured failure of the preceding code/runtime experiments may activate
these candidates, in this order:

1. **Typed scalar/common columns.** Consider only if adaptive coalescing and
   one-pass range execution leave material scalar I/O/decode cost. Repair the
   known Number Gauge/Sum semantics at the same version boundary: source kind,
   temporality, monotonicity, start time, flags, reset information, signed
   non-finite delta sums, and authoritative locator/checksum binding.
2. **Packed multi-chunk frames.** Preserve direct per-chunk locators, bounded
   individual reads, per-chunk integrity, and an outer frame check.
3. **Compact routing.** Preserve no-false-negative behavior and authority
   boundaries; a checksum does not prove semantic completeness.
4. **Adjacent-segment packing.** Last priority because it expands manifest,
   recovery, retention, time-pruning, and compaction complexity.

Before any changed bytes are implemented, update
[storage.md](docs/superpowers/specs/storage.md), assign new explicit component
and segment versions, define rejection/migration behavior, and add golden,
round-trip, corruption, deterministic replay, readback, and real-corpus gates.

## Phase tracker

| Phase | Status | Exit evidence |
| --- | --- | --- |
| 1. Status, instrumentation, current baseline | **Complete** | [Observer-cost](docs/experiments/storage_vnext/query_instrumentation_off_ab.md), [4M replay](docs/experiments/storage_vnext/2026-07-21-phase1-replay-baseline.md), and [Schema 8 query](docs/experiments/storage_vnext/2026-07-21-phase1-query-baseline.md) reports accepted |
| 2. Governed compact query IDs | **Complete** | [Same-binary correctness, accounting, and real-corpus promotion report](docs/experiments/storage_vnext/2026-07-21-phase2-compact-query-label-ids.md) |
| 3. Payload coalescing | **Complete** | [Bounded fixed-policy promotion and adaptive-policy rejection](docs/experiments/storage_vnext/2026-07-21-phase3-payload-coalescing.md) |
| 4. One-pass range execution | Open | Per-step oracle equivalence and 30m/6h/24h measurements |
| 5. Allocator/head topology | **250k allocator screen complete; 4M and topology gates open** | [J1 nominated for stats/no-stats 4M confirmation](docs/experiments/storage_vnext/2026-07-23-phase5-allocator-screen.md) |
| 6. Codecs | **Float/timestamp fits complete; runtime/layout work open** | [Float](docs/experiments/storage_vnext/2026-07-23-phase6-float-fit-screen.md) and [timestamp](docs/experiments/storage_vnext/2026-07-23-phase6-timestamp-fit-screen.md) screens select the next measured work |
| 7. Conditional format candidates | **Activation audit complete; all deferred** | [No current device-I/O or residual byte-layout bottleneck activates a format change](docs/experiments/storage_vnext/2026-07-21-phase7-format-activation-audit.md) |

## Global correctness and measurement gates

Every code-only ingest optimization must preserve accepted/rejected counters,
event-time policy, stable input order, deterministic segment IDs, byte-identical
artifacts where the format is unchanged, footer validation, and independent
readback equivalence.

Every query optimization must preserve exact and portable semantic
fingerprints, result shapes/values/order, corruption and limit errors, and
ordinary `QueryStats` unless an intended difference is named before the run.
Inspect readback executed/skipped diagnostics; a skip is a coverage gap.

Every A/B uses the same host, fixed workload, explicit configuration and
limits, fingerprinted corpus, alternating order, and no overlapping builds,
replay, profiler, footer scan, or unrelated workload. Runtime-flag comparisons
use one identical release binary. Code-version comparisons record both binary
hashes. Report raw per-run evidence and environmental noise; do not turn a
single noisy latency sample into a promotion claim.

## Completion criterion

This program is complete only when:

- the current-head baseline and profile exist;
- every phase above has either a promoted implementation or an explicit
  evidence-backed rejection/defer decision;
- relevant focused, integration, corruption, replay/readback, Prometheus,
  formatting, clippy, and workspace gates have been run or their unavailability
  is recorded; and
- this file, the normative specs, and the final report agree on the remaining
  bottlenecks and production defaults.
