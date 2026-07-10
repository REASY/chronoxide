# PromQL Range Decoded Scalar Cache Design

## Goal

Reduce repeated typed-scalar-lane decoding during sealed-segment PromQL range
queries without changing query results, PromQL semantics, query-limit behavior,
error precedence, or the segment format.

This remains a measured performance experiment. The implementation is committed
only if it improves both targeted scalar range-query workloads by at least 10%
under the paired benchmark protocol below, produces bit-identical result
fingerprints and identical public `QueryStats`, stays within its cache-memory
budget, and causes no regression greater than 3% in the native-histogram control
workload.

## Profile Evidence

The current range executor evaluates every timestamp by running the complete
instant-query path again. A 61-step scalar `rate()` workload therefore repeats
selector planning, payload reads, typed scalar-lane decoding, projection,
merging, and range-function evaluation 61 times.

Five fresh baseline processes gave a mean total suite duration of 8.7707
seconds with a 0.82% coefficient of variation and a 2.12% min-to-max spread.
The targeted queries had these mean warm durations:

- `rate(go_gc_duration_seconds_count[15m])`: 308.4 ms;
- `sum by (service_name_x55e50a58f9befba7)(rate(go_gc_duration_seconds_count[15m]))`:
  295.9 ms;
- native histogram quantile control: 2.2756 s.

A 45-second scalar-only sample attributed 52.5% of CPU samples to selector
acquisition and materialization, 32.3% to range/rate evaluation, and only 3.75%
to kernel reads. `decode_varint`, decoded-metadata work, allocation, copying,
and freeing were prominent leaves. A byte-only payload cache has too little
measured headroom. Reusing decoded scalar lanes attacks repeated I/O, checksum,
varint, metadata, and decoder work while leaving all projection and evaluator
semantics unchanged.

## Scope

The first implementation caches only successfully decoded dedicated scalar
lanes used for Histogram, ExponentialHistogram, and Summary count/sum
projection. An entry is eligible only when:

- `file_id == 0` identifies the current `chunks.bin` file;
- both `scalar_lane_offset` and `scalar_lane_len` are nonzero; and
- the projection is `Count` or `Sum`.

Chunks without a dedicated lane retain the existing full-record fallback and
are never inserted into this cache. This is deliberate: V7 replay data used by
the performance workload has dedicated lanes, and compatibility with older
layouts is not needed to prove this optimization.

The session range API owns the optimized executor. The direct sealed-store
range APIs open an ephemeral session and delegate to the same executor so the
two public paths cannot drift. Head-inclusive range queries retain their current
executor.

One cache is isolated to one top-level range call. Selector branches inside the
same expression may share it; separate expressions, repeated benchmark runs,
sessions, threads, and instant queries do not.

The following remain out of scope:

- float, integer, full Histogram, full ExponentialHistogram, and full Summary
  record caching;
- cached projections, delta accumulators, rates, aggregations, or AST results;
- incremental/sliding range evaluation;
- changes to query limits or public `QueryStats` semantics;
- any on-disk format or storage-semantic change.

If this narrow cache misses the performance gate, its implementation is
discarded rather than broadened speculatively. One-time union selector
materialization becomes the next separately designed experiment.

## Configuration and API Behavior

`SegmentStoreQuerySession` has a range scalar-cache configuration with a byte
budget. Zero disables admission while retaining the identical range executor;
this is the differential-test and cache-off benchmark mode.

The initial candidate default is 16 MiB per active range call, with a hard
accepted configuration maximum of 32 MiB. The benchmark CLI exposes the exact
budget and records it in the report. A preliminary cache-on sweep measures 4,
8, 16, and 32 MiB, including admission bypasses and peak retained charge. The
committed default is the smallest budget whose scalar latency is within two
percentage points of the best accepted candidate.

The direct store APIs use the committed default. Session callers may choose any
budget from zero through 32 MiB. Values above the maximum are rejected rather
than silently clamped.

A process-wide atomic admission governor caps the sum of active cache leases at
128 MiB by default. A range call acquires its complete per-call budget lazily,
only after a step plans its first eligible scalar-lane miss and before that
step's physical batch is issued. Queries with no eligible scalar projection do
not allocate arenas or acquire a lease. If capacity is unavailable, that call
runs with streaming decode and records governor refusal; query success never
depends on cache admission. The governor limit is configured once during
process initialization, and its RAII lease is released on every return path.

`chronoxide-core::storage::segment` owns the production governor through one
`OnceLock<Arc<RangeScalarCacheGovernor>>`; all stores and sessions clone that
same immutable handle. An explicit process configuration function may initialize
the limit before the first query. Repeating it with the same value is
idempotent; a conflicting value, including one supplied after lazy default
initialization, returns a typed error containing the existing and requested
limits. Zero globally disables admission.

Production current leased bytes are atomic, peak leased bytes are monotonic for
the process lifetime, and neither has a reset API. Fresh benchmark processes
therefore start from zero. Internal constructors accept an injected
`Arc<RangeScalarCacheGovernor>` under test configuration so parallel tests use
isolated governors and cannot cause cross-test refusals or stale peak values.

## Cache Ownership, Identity, and Lifetime

The public session range method installs a non-allocating RAII call guard before
parsing. It clears the previous summary, preserves the current parse-before-
bounds-validation order, and publishes a finalized summary on every return:
parse, bounds, governor refusal, limit, I/O, decode, and success. Cache admission
and allocation occur only after parsing and bounds validation succeed.

The guard owns at most one `RangeScalarDecodeCache` for the evaluation loop.
Dropping the guard releases arena allocations and the process-wide governor
lease before publishing `retained_charge_after_finalize = 0`. The summary
remains inspectable through the mutable session after an error.

The direct store range methods preserve their current parse and bounds ordering
before opening the ephemeral session, then invoke the same parsed-query range
executor. Session creation cannot take precedence over an existing parse or
bounds error.

An entry key contains every field that can change the decoded value:

- stable segment ordinal within the sorted session;
- chunk file identifier;
- chunk payload offset and full chunk length;
- scalar-lane offset and scalar-lane length;
- scalar projection kind (`Count` or `Sum`); and
- chunk kind.

The cached value contains the validated scalar-record header plus every raw
`ChunkScalarSample`: timestamp, optional scalar value, OTLP flags, temporality,
counter-reset hint, and start time. Count and sum are never interchanged. A
partial scalar lane never satisfies a full-record request.

Reads, checksum failures, malformed headers or lane bounds, decode failures,
and unsupported file/layout entries are not cached. Sealed segment files are
immutable for the lifetime of a reader; concurrent mutation of `chunks.bin` is
outside the storage contract and is not made observable through repeated cache
validation.

## Hard Cache Allocation Budget

The byte budget is a hard bound over all cache-specific requested allocation
layouts, including metadata entries, decoded samples, and temporary cache
construction state. It does not claim to bound ordinary cache-off planner/result
allocations or allocator-internal bookkeeping outside requested layouts; paired
RSS measurement covers that distinction.

The cache uses two fixed, fallibly allocated arenas rather than growable
`Vec`/`HashMap` storage:

- an entry arena stores keys, validated record headers, and sample-arena
  `(start, len)` references;
- a `ChunkScalarSample` arena stores decoded samples in-place;
- both arenas are allocated once, never grow, and are released together;
- the entry arena is at most one quarter of the per-call budget and at most
  16,384 entries; the remaining requested layout belongs to samples; and
- lookup is binary search over the initialized, sorted entry prefix.

The already-locked `allocator-api2` crate is added as a direct dependency and
provides fallible exact-length `Box<[MaybeUninit<T>]>` allocation on stable Rust.
A small private RAII arena wrapper tracks initialized elements and rolls back
and drops partial records on decode errors. Any `MaybeUninit` unsafe operation
is confined to that wrapper. Unit tests use a failure-injecting allocator to
validate allocation refusal, initialization, sorted insertion, rollback, and
drop behavior; an optional Miri recipe is documented for toolchains that provide
the component.

The validated chunk header supplies the expected sample count. Before decoding
a miss, the cache checks that the complete sample slice fits unused arena slots.
If it does not, the existing streaming callback runs directly; no collection
allocation is attempted. Successful decoding writes directly into reserved arena
slots and advances the cursor only after complete checksum, bounds, sample-count,
and payload validation.

Cache lookup is performed twice from stack-built keys: once while rebuilding
the existing request vector with physical misses and once while processing
planned chunks. The request element remains the original `(offset, len)` layout;
no cache key is stored on the heap. No cache-specific hit bitmap, miss vector,
request clone, or classification map is allocated.

An RAII live-allocation gauge acquires the exact arena layout charges before
allocation and releases them on every failure/drop path. Cache construction is
disabled if layouts overflow, the global lease is unavailable, or either arena
allocation fails. Consequently failed admission, corrupted input, and ordinary
completion cannot exceed the per-call requested-layout budget, while the global
governor bounds simultaneous requested cache layouts process-wide.

## Two-Phase Execution and Error Ordering

Every evaluation step retains current selector lowering, matching, series and
chunk planning, projection, delta-fragment handling, filtering, merging, range
evaluation, aggregation, retimestamping, and final deduplication.

The chunk portion remains explicitly two-phase:

1. Build the existing ordered `(offset, len)` request vector unchanged and
   charge all logical chunk-read and byte limits exactly as cache-off execution
   does.
2. Before removing hits, pass that complete vector once to a new logical
   observation function. It updates `chunk_payload_bytes` and every ordered and
   sorted `ChunkPayloadLocalityProfile` field exactly as the current combined
   reader does.
3. Only after all logical charges/observations succeed, clear the vector while
   retaining its capacity, rewalk the already-planned chunk entries, derive
   cache keys on the stack, and repopulate the same vector with misses plus
   unsupported/budget bypasses. Do not charge limits again. No second request,
   enlarged request element, or hit-map allocation is created.
4. Pass the retained vector to a physical-only batch reader. It records read
   duration, physical spans, and physical bytes but does not repeat logical
   observation. Complete that batch before processing any decoded record,
   preserving current I/O-before-decode ordering.
5. Process planned chunks in their original order. Hits iterate cached raw
   samples; admissible misses insert only after complete successful validation;
   streaming misses and bypasses use the current callback.
6. Charge decoded samples and typed scalar chunks exactly as today regardless
   of hit, miss, admission, or bypass.

The implementation locks down the current precedence rather than applying one
rule to every limit:

| Competing outcomes | Required winner |
| --- | --- |
| parse error vs bounds error | parse error |
| matched-series limit vs payload read/decode | matched-series limit |
| chunk-read or byte limit vs payload read/decode | chunk/byte limit |
| any physical miss/bypass read failure vs processing an earlier cached hit | physical read failure |
| corrupt chunk before a later chunk's sample-limit failure | corruption/decode error |
| earlier successful hit/chunk exceeds sample limit before a later corrupt chunk is processed | sample limit |
| corrupt chunk itself vs its sample-limit charge | corruption/decode error |
| projected-series limit vs earlier read/decode/sample failure | earlier failure |

Series and chunk/byte limits are charged during planning. Sample limits remain
charged only after successful per-chunk decode/projection in original chunk
order, and projected-series limits remain after result construction. The full
miss/bypass physical batch completes before any cached hit or decoded miss is
processed. Tests cover both chunk orders and hit/miss permutations for every row.

## PromQL and OTLP Semantics

The cache stores raw decoded inputs, never window-specific state. Every step
creates fresh delta count/sum accumulators and calls the existing projection
logic. Different windows therefore keep their current:

- left-open/right-closed range boundaries and seed reads;
- exact stale-marker behavior and distinction from ordinary NaN/infinity;
- missing-value rejection/projection behavior;
- cumulative, delta, unspecified, and mixed temporality behavior;
- reset-hint and counter-decrease handling;
- `start_time_ms` interval handling;
- positive, negative, nested, and epoch-saturating offset behavior; and
- duplicate timestamp, branch-conflict, and keep-last ordering.

Public `QueryStats` and all query-limit checks retain logical per-step
accounting. Hits still count as chunk reads, bytes read, samples decoded, and
typed scalar chunks decoded. Cache behavior may change physical session reads,
but never public logical statistics or limit failures.

## Cache Summary and Read Observability

Cache metrics are not added to additive per-segment
`SegmentStoreQueryProfile`. The public session range-call guard instead clears
and then stores `last_range_scalar_cache_summary`. The dedicated accessor remains
usable after success, parse/bounds errors, governor/allocation refusal, and
execution errors; a call never exposes the previous range call's summary.

The summary contains:

- configured per-call budget, acquired governor lease, and governor refusal;
- entry-arena and sample-arena exact layout charges;
- hits, misses, admitted entries, streaming/budget bypasses, and unsupported
  bypasses;
- logical requested bytes served by hits and by misses/bypasses;
- peak retained charge; and
- retained charge after finalization, which must be zero.

The process governor separately exposes current and peak leased bytes for the
concurrency test and benchmark report. Its current lease count/bytes must return
to the pre-call value after every success and error path.

The existing session profile continues to count every planned range as logical
payload used bytes. Physical read batches contain miss/bypass requests only,
but coalesced spans may include gap bytes also represented by cached entries;
physical counters continue to report actual spans and bytes read.

The benchmark captures one cache summary per run rather than subtracting
current/peak gauges. Instant queries do not manufacture cache statistics.

## Independent Pre-Change Oracle

Cache-off and cache-on modes in the final binary share refactored plumbing, so
their agreement alone cannot prove that refactoring preserved old behavior.
Implementation is therefore split at a correctness checkpoint:

1. Add only the semantic fingerprint, warm-median reporting, corpus identity,
   and raw benchmark metadata. Do not change either range executor or chunk
   reader.
2. Run the existing executor against the replay workload and committed semantic
   fixtures. Record expression, bounds, step, returned fingerprint, public
   `QueryStats`, result counts, and corpus identity in a versioned baseline
   artifact under `docs/superpowers/benchmarks/`. In the same checkpoint, run a
   versioned error corpus and record typed error variant plus exact message for
   parse/bounds failures, both public direct and session APIs, and every
   row/order combination in the precedence table.
3. Commit that instrumentation checkpoint and record its hash before adding
   cache or range executor changes.
4. After implementation, require cache-off and cache-on results/errors to match
   each other and the pre-change result and error artifacts.

Unit/source-level fixtures also assert explicit labels, timestamps, bit-pattern
values, and typed metadata-derived outcomes. They are not digest-only tests and
remain an independent semantic oracle if both candidate modes share a defect.

## Bit-Exact Result Fingerprint

Result counts are not a correctness check. `chronoxide-core` therefore provides
a versioned SHA-256 semantic fingerprint for `QueryExecution` results. The
canonical encoding includes, in returned order:

- result and series counts;
- series IDs;
- label counts and length-prefixed UTF-8 key/value bytes in label order;
- sample counts, timestamps, and `f64::to_bits()` values;
- reset-hint vector length and every discriminant;
- sample-start-time vector length, presence bit, and value; and
- result temporality discriminant.

The encoding has an explicit version/domain prefix and fixed little-endian
integer representation. It hashes vector lengths as well as values so an empty
metadata vector differs from an explicitly populated one. Ordering differences,
NaN payload changes, stale-marker changes, signed-zero changes, and private
typed metadata changes therefore alter the fingerprint.

`chronoxide-query` reports this fingerprint for every benchmark run. All repeats
within a mode and every cache-off/cache-on pair must match exactly. Errors are
compared by typed variant plus exact message against both candidate modes and
the independent pre-change error corpus; the successful real-replay suite does
not substitute a digest for error testing.

## Correctness Tests

Cache-off and cache-on executions use the same range executor with budgets zero
and nonzero. Differential assertions cover results, bit-exact fingerprints,
errors, public `QueryStats`, and every query limit.

They also compare session stats and a normalized logical session profile. All
file/index counters, `chunk_payload_bytes`, and every ordered/sorted locality
field must be identical. Duration fields are reported but not equality-tested;
only physical chunk span reads/bytes are expected to decrease. A focused unit
test proves that splitting logical observation from physical reads reproduces
the complete cache-off locality profile before any cache benchmark is accepted.

The typed scalar matrix includes:

- Histogram, ExponentialHistogram, and Summary;
- count and sum for each kind;
- absent sums;
- cumulative, delta, unspecified, and mixed temporality;
- stale markers, ordinary NaNs, missing OTLP values, reset hints, and start
  times at and across chunk/segment boundaries;
- count values above the exact `f64` integer range;
- overlapping and non-overlapping range steps;
- positive, negative, nested, and epoch-saturating offsets;
- duplicate timestamps across chunks and segments; and
- dedicated scalar lanes plus proof that no-lane fallback and nonzero file IDs
  bypass the cache unchanged.

Explicit expected-value fixtures, exercised first as misses and then as hits,
cover at minimum:

- stale marker -> reset -> delta continuation;
- no-recorded-value with absent sum and with an ordinary NaN sum;
- mixed temporality inside one chunk and across chunk/segment boundaries;
- `start_time_ms` changes immediately before and after resets; and
- large count-to-`f64` projection bit patterns.

Failure and lifecycle coverage includes:

- malformed lane offsets/lengths, truncated payloads, checksum failures, and
  decode errors;
- entry-table full, byte-budget refusal, oversized-record streaming, and
  exact-arena allocation refusal through a deterministic allocator;
- every row of the precedence table in both relevant chunk orders and hit/miss
  combinations;
- success followed by parse, bounds, governor-refusal, allocation-refusal, I/O,
  decode, and limit errors, each replacing the previous summary and leaving
  retained charge at zero;
- instant, native full-histogram, and head-inclusive queries never admitting
  entries; and
- direct-store and session range APIs producing identical results, limits, and
  read/decode errors after delegation.

A barrier-controlled concurrent stress test attempts more simultaneous leases
than the configured global governor permits. Admitted calls must match cache-off
fingerprints; refused calls must stream and also match. Per-call and process
lease peaks stay within their respective budgets, simultaneous RSS high water
is recorded, no query fails because admission was refused, and all summaries and
global leases finalize at zero.

## Reproducible Performance Protocol

Build one release binary containing both cache-off and cache-on modes. Do not
compare different compiler outputs. Stop replay, builds, sampling, and other
known CPU/disk-heavy work during measurement. Record the commit, binary hash,
Rust toolchain/LLVM versions, OS build, machine model/CPU/RAM, power mode, and
complete benchmark arguments. Preserve every raw per-repeat duration.

The report includes a replay-corpus fingerprint over sorted segment directory
IDs, segment metadata, file names and lengths, and validated footer checksums.
Both modes must use the same fingerprint, and it must match the independent
pre-change baseline artifact. Do not claim the operating-system page cache is
cold.

Run nine fresh-process cache-off/cache-on pairs. Alternate order by pair
(`off/on`, then `on/off`) to reduce thermal and page-cache ordering bias. Each
process uses five benchmark repeats: the first is session-local cold and the
remaining four are warm.

For each query and process, compute the median of its four warm durations. For
each pair, compute `1 - (cache_on_median / cache_off_median)`. The reported
effect is the median of the nine paired effects. The benchmark report is
extended to show warm median as well as the existing mean/min/max.

Reject and rerun the experiment if either mode's process medians for any query
have a coefficient of variation above 3%, if result fingerprints or
`QueryStats` differ from each other or the pre-change baseline, or if an
uncontrolled workload overlaps the run.

Capture fresh-process maximum resident set size with `/usr/bin/time -l`. RSS is
reported alongside the cache's exact peak retained charge; RSS alone is not
used to infer release. For each pair, RSS delta is the signed
`cache_on_max_rss - cache_off_max_rss`; the report retains all nine deltas and
their median. The median RSS increase must not exceed the median observed cache
peak by more than 4 MiB of allocator/measurement allowance.

## Commit Gate

The implementation is committed only when all of the following hold:

- both targeted scalar queries have at least 10% median paired warm improvement
  and cache-on is faster in at least eight of nine pairs (one-sided sign-test
  probability 0.0195 under no effect);
- the native histogram control has no more than 3% median paired regression;
- all successful replay fingerprints and public `QueryStats` are identical
  across repeats/modes and match the independent pre-change artifact;
- all typed error variants/messages match the independent pre-change error
  corpus;
- all differential result, error, precedence, limit, lifecycle, and concurrency
  tests pass;
- peak cache charge stays within the configured budget and finalized retained
  charge is always zero; process-wide leased bytes stay within the governor and
  return to zero;
- physical payload reads and decode misses decrease as expected;
- paired RSS behavior satisfies the stated allowance; and
- focused tests, the full `chronoxide-core` suite, query-binary tests, and
  `git diff --check` pass.

If the gate is missed, implementation changes are reverted and not committed.
The failed experiment's timing, cache-hit, physical-byte, and memory evidence is
retained in the work report so the next opportunity starts from measured data.
