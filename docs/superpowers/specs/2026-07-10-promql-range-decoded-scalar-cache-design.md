# PromQL Range Decoded Scalar Cache Design

## Goal

Reduce repeated typed-scalar chunk decoding during sealed-segment PromQL range
queries without changing query results, PromQL semantics, query-limit behavior,
or the segment format.

This is a measured performance experiment. It is committed as an implementation
only if it improves the targeted warm range-query benchmark by at least 10%
across five fresh processes, while producing identical results and limit
behavior and causing no regression greater than 3% in the other range-suite
workloads.

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
- native histogram quantile: 2.2756 s.

A 45-second scalar-only sample attributed 52.5% of CPU samples to selector
acquisition and materialization, 32.3% to range/rate evaluation, and only 3.75%
to kernel reads. `decode_varint`, decoded-metadata work, allocation, copying,
and freeing were prominent leaves. A byte-only payload cache therefore has too
little measured headroom; caching decoded typed scalar lanes attacks the first
material part of the repeated work while leaving the evaluator unchanged.

## Scope

The first implementation caches only successful typed scalar projection-record
decodes used for Histogram, ExponentialHistogram, and Summary count/sum
projection. This includes the dedicated scalar-lane encoding and the existing
full-chunk fallback for records without a scalar lane. It is the hot path
exercised by the targeted scalar range benchmark.

The cache is active only for a single sealed-store range-query execution. It is
not shared between expressions, query sessions, threads, or instant queries.
The session range API owns the optimized path; the direct sealed-store range
API opens an ephemeral query session and delegates to it. Head-inclusive range
queries retain the current path in the first version.

The following are explicitly out of scope:

- caching float, integer, or full typed chunk records;
- caching projected series, delta accumulators, rates, aggregations, or AST
  results;
- changing range-window selection or making the evaluator incremental;
- changing query limits, public `QueryStats`, or on-disk data;
- sharing cached values beyond one range-query call.

If this narrow cache does not clear the acceptance gate, it is discarded rather
than broadened speculatively. The next experiment is one-time union selector
materialization, covered by a separate design.

## Cache Ownership and Key

`SegmentStoreQuerySession::execute_promql_range_query` owns one central cache
for its full evaluation loop and drops it before returning, including error
returns. Segment query execution receives access to that range-local cache
without adding cache state to the store-shared query cache. Central ownership
also makes the 32 MiB bound apply to the whole range call rather than once per
segment.

Entries are keyed by:

- stable segment identity;
- chunk file identifier;
- chunk payload offset;
- full chunk length;
- scalar-lane offset;
- scalar-lane length;
- scalar-lane read length; and
- scalar projection kind (`Count` or `Sum`).

The first version admits only the current `file_id == 0` `chunks.bin` path.
Other file identifiers bypass the cache until their file routing is implemented
explicitly.

The projection kind is part of the key because the same encoded lane produces
different scalar values for count and sum. No entry decoded from a partial
scalar lane may satisfy a full-record request, and count/sum entries are not
interchanged.

The cached value is the immutable decoded scalar-projection record, including
every sample timestamp, value, OTLP flags, temporality, counter-reset hint, and
start time. Failed reads, checksum failures, malformed payloads, and decode
errors are never cached.

## Execution Flow

Every evaluation step continues to perform current selector lowering,
matching, series/chunk planning, projection, delta-fragment handling, filtering,
merging, rate evaluation, aggregation, retimestamping, and final deduplication.

For each planned typed scalar projection chunk:

1. Charge the existing logical chunk-read and byte budget exactly as today.
2. Look up the exact decoded-lane cache key.
3. On a hit, iterate the cached decoded samples through the existing projection
   code.
4. On a miss, include the payload range in the physical read batch, decode and
   validate it once, insert it if the memory budget permits, and then run the
   same projection code.
5. Charge decoded-sample and typed-scalar-chunk statistics exactly as today,
   whether the decoded samples came from a hit or miss.

This intentionally retains per-step delta accumulators. A cached record is raw
input to the current projection logic, so different evaluation windows still
apply their own seed, stale-marker, reset, start-time, and temporality behavior.

## Memory Bound

The cache has a fixed 32 MiB per-range-execution admission budget. Accounting
uses the scalar-record allocation and the capacity of its decoded sample vector,
plus conservative map-entry overhead. An entry larger than the remaining budget
is used for the current step but not cached.

Cache-specific map and sample-vector growth uses fallible reservation. If a
reservation fails, the current payload is processed through the existing
streaming scalar decoder and is not cached. The optimization must not replace a
streaming query with an unconditional decoded-vector allocation. At concurrency
`N`, the cache-specific process bound is therefore `32 MiB * N` plus small,
conservatively charged map overhead; the observed peak is reported for capacity
tuning.

The first version does not evict entries. Once an entry cannot be admitted, the
query continues correctly through the existing read/decode path. This keeps the
memory bound deterministic and prevents eviction churn from changing query
behavior. The cache is an optimization only; refusal to admit an entry must not
change the result or error produced by the underlying query.

## Statistics and Observability

Public `QueryStats` and all query-limit checks retain their current logical
per-step accounting. In particular, cache hits still count as chunk reads,
bytes read, samples decoded, and typed scalar chunks decoded for compatibility.

The session read profile continues to count every requested chunk range as
logical payload used bytes. Physical read batches are built from miss requests
only, but their coalesced spans may also include gap bytes covered by cached
entries; physical counters continue to report the actual spans read. One
range-call cache summary reports:

- decoded scalar cache hits;
- decoded scalar cache misses;
- logical requested bytes served by hits and misses;
- admitted entries, current retained bytes, and peak retained bytes; and
- entries bypassed because of the memory bound.

The summary is owned centrally, so per-segment peaks are not incorrectly summed.
Current retained bytes must return to zero after successful and failed range
calls. These counters are included in the benchmark report so reduced physical
work and cache memory are visible rather than inferred from latency alone.

## Correctness Requirements

Cache-on and cache-off executions must return identical values, labels, sample
timestamps, ordering, errors, public `QueryStats`, and limit failures.
A test-only zero-budget execution option disables cache admission while
retaining the same range path so differential tests do not compare two
otherwise different query implementations.

Focused differential coverage must include:

- count and sum projections;
- cumulative and delta temporality;
- start times and counter-reset hints;
- exact stale markers and missing OTLP values;
- overlapping and non-overlapping range steps;
- positive, negative, and epoch-saturating offsets;
- duplicate timestamps across chunks or segments; and
- chunk, byte, sample, matched-series, and projected-series limits.

Instant queries, native full-histogram range evaluation, and head-inclusive
queries must have regression coverage proving that they remain on their current
paths. A lifecycle test exercises both success and error returns and proves that
the central cache is empty afterward.

## Benchmark and Commit Gate

Build the release query binary and run the exact real-replay range suite in five
fresh processes before and after the change. Compare per-process median warm
durations, result counts, `QueryStats`, logical/physical payload bytes, cache
counters, and peak resident memory. Fresh-process peak resident memory is
captured with `/usr/bin/time -l`; cache current/peak counters separately prove
per-call release because process RSS alone is not a release signal.

The implementation is committed only when all of the following hold:

- the targeted raw scalar and grouped scalar warm medians improve by at least
  10%;
- returned results, errors, and public `QueryStats` are identical;
- no other range-suite query regresses by more than 3%;
- cache memory remains within 32 MiB and is released on both successful and
  failed range calls;
- physical payload reads and decode misses decrease as expected; and
- focused tests, the full `chronoxide-core` suite, and `git diff --check` pass.

If the gate is missed, the implementation changes are reverted and not
committed. The benchmark evidence is retained in the work report so the next
opportunity starts from measured results.
