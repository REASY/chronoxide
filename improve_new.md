# Chronoxide performance program — live status

- **Audit date:** 2026-07-21
- **Audited code baseline:** `a8bd6d44d6c06375a09104a4a9c58ecbe6268021`
  (`2026-07-17`, `chore(lint): satisfy strict workspace checks`)
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
replay and complete Schema 8 query matrix. Historical percentages below came
from different sequential baselines, prefix sizes, binaries, and schedules;
they must not be added together or presented as one cumulative speedup.

## Executive status

Schema 8 has already captured the material on-disk metadata wins, and the
current baseline plus query-stage profiles are complete. The next credible
improvements are code-side and measurement-led:

1. test governed query-local compact label IDs;
2. test adaptive payload coalescing and one-pass multi-step range execution;
3. tune allocator/head behavior against realistic partition layouts; and
4. evaluate sample/timestamp codecs against a sealed real corpus.

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
- Query label materialization defaults to `DemandDriven`; label storage
  defaults to `OwnedStrings`. `Full` and `SharedAtoms` remain same-binary
  comparators.
- Compact four-sample numeric head staging and the adaptive head-series table
  default to enabled.
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

The detailed ingest reports are under
[storage_vnext](docs/experiments/storage_vnext/README.md). The current baseline
must be remeasured rather than reconstructed by summing these sequential A/Bs.

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
| Query-session `SharedAtoms` | Broad selector peak RSS -54.91%, but cold +25.56% and warm +10.71%; about 22.1 million cold atom lookups | `OwnedStrings` remains default; do not repeat without a cheaper governed lookup design |
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

### Promotion gate

Require full semantic/portable fingerprints and public `QueryStats` to match
`OwnedStrings`, complete corruption and cache-isolation tests to pass, and the
real broad-label matrix to show a repeatable CPU/latency or bounded-RSS benefit
without material regression on small/full-demand or demand-driven queries.
Counter reduction alone is not a win.

## Phase 3 — adaptive payload-read coalescing

The current planner hard-codes a 4 KiB maximum gap. Run one binary with runtime
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

## Phase 4 — one-pass multi-step range execution

The current range executor reruns the complete instant query for every step.
Implement a narrow runtime comparator for common
`sum/count by(...)(rate(selector[window]))` shapes:

The accepted seven-step scalar range spends roughly 95% of Detailed wall in
repeated storage verification/planning work and only 1.2% in PromQL grouping
and evaluation. Its selective Off median is 2,864.9 ms cold and 2,667.4 ms
warm. This is currently the strongest query-CPU hypothesis in the program.

1. plan the union time interval once, including the required predecessor/seed;
2. read, validate, and decode each required chunk once;
3. retain a governed ordered per-series representation; and
4. advance left/right cursors through each evaluation step.

Every unproved expression uses the existing executor. Preserve left-open,
right-closed selection, logical pre-epoch duration, exact stale-NaN omission,
ordinary NaN/Inf values, reset hints, delta interval/start-time requirements,
signed delta sums, duplicate precedence, offsets, limits, and per-step output.

Verify every step against the current executor, focused explicit-value tests,
the independent readback oracle where supported, and `promtool` when
available. Measure at least 30-minute, 6-hour, and 24-hour ranges. Promote only
with material repeatable speedup and bounded governed memory.

## Phase 5 — allocator and head topology

Run system allocator and linked jemalloc from otherwise equivalent release
builds. Sweep bounded jemalloc settings such as arena count, dirty/muzzy decay,
background threads, and explicit purge behavior. Record task clock, cycles,
allocation profile, peak and time-series RSS, retained/active allocator bytes,
page faults, and post-seal release behavior. A CPU win with unbounded or
operationally unacceptable RSS is not promotable.

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

Inventory per kind/block: point count, raw bytes, encoded bytes, selected
codec, value/timestamp distributions, and schema/layout. Measure replay and
seal wall/CPU, cycles per sample, branch/cache misses, peak RSS, range-startup
cost, scalar/full decode, and cold/warm end-to-end queries. Any adaptive choice
must be deterministic, specified byte-for-byte, and select from complete
encoded sizes with a canonical tie rule.

Do not promote from a microbenchmark or byte reduction alone. Require
deterministic bytes/round trips, corruption tests, replay/readback equivalence,
and acceptable ingest and query CPU.

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
| 2. Governed compact query IDs | Open | Same-binary correctness and real-corpus promotion/rejection report |
| 3. Adaptive coalescing | Open | Gap/backend Pareto matrix and promotion/rejection report |
| 4. One-pass range execution | Open | Per-step oracle equivalence and 30m/6h/24h measurements |
| 5. Allocator/head topology | Open | Bounded jemalloc and multi-partition evidence |
| 6. Codecs | Open | Real sealed-corpus value/timestamp codec A/B |
| 7. Conditional format candidates | Blocked by evidence gates, not an execution blocker | Each candidate either remains inactive or receives a versioned design and measured gate |

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
