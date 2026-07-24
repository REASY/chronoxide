# Phase 4 one-pass range-execution comparator

Status: completed experimental diagnostic; the production default remains repeated
instant evaluation.

## Purpose

Chronoxide currently evaluates a PromQL range request by executing the complete
instant query independently at every evaluation timestamp. The accepted Phase 1
profile attributes roughly 95% of the seven-step scalar range query to repeated
storage planning, verification, symbol, identity, and label work rather than to
range arithmetic or grouping.

This phase tests one narrow hypothesis: scalar
`sum/count by (...)(rate(...))` range queries should be able to read and
validate their union interval once, then reuse that decoded series state across
evaluation timestamps.

The comparator is deliberately not a production optimization. The current
selector boundary decodes directly into owned `Vec<SegmentQueryResult>` values
and cannot accept a retained-byte reservation before those allocations occur.
It can report an honest post-decode estimate, but that is not memory admission.
The comparator therefore cannot be promoted until result construction is
reservation-aware or streaming.

## Runtime modes

`RangeExecutionMode` has two values:

- `Repeated`: the current executor. This is the default.
- `OnePassAssumeScalar`: diagnostic-only scalar assumption. It attempts the
  whitelist below and otherwise calls the unchanged repeated executor.

The mode is immutable after a session begins query, prefetch, or prewarm work.
HTTP/API sessions retain `Repeated` unless a future production design changes
that default explicitly.

Every range call finalizes a summary, including parse, validation, fallback,
and execution failures. The summary records at least:

- requested and effective mode;
- a stable fallback reason;
- evaluation count;
- union read bounds when one-pass executes;
- source series and source sample counts;
- whether the scalar range cache was bypassed;
- a scoped post-decode retained-byte estimate;
- `preallocation_governed = false`; and
- zero retained comparator charge after finalization.

The estimate is observability only. It must never be named or interpreted as a
governor lease, allocation limit, or proof that an allocation could not exceed
a configured budget.

## Eligibility whitelist

One-pass execution is eligible only when all of these conditions are proven
before selector I/O:

1. The requested mode is `OnePassAssumeScalar`.
2. Query limits are exactly `QueryLimits::unlimited()`. Any finite limit uses
   repeated execution so its error and accounting precedence remain unchanged.
3. The root AST is `sum by (...)(rate(selector[window]))` or
   `count by (...)(rate(selector[window]))`.
4. The range-function child is a direct selector. Offset, binary, label,
   subquery, nested, and other function shapes fall back.
5. The selector has one exact metric name, not a metric-name regex or a
   brace-only metric matcher.
6. The metric name does not end in `_count`, `_sum`, or `_bucket`; those names
   may require real-plus-virtual projection branches.
7. Lowering yields exactly one `SegmentProjection::AllPromql` selector.
8. The step is no larger than the rate window. This makes the union interval
   continuous; the comparator does not read holes that repeated evaluation
   would never touch.
9. The sealed-store session path is used. Head-inclusive range APIs remain on
   repeated execution.

`count` is eligible only under the same explicit scalar-only assumption. Native
histogram `count` has separate element-counting semantics and must not count
virtual scalar projections; observing a typed source therefore triggers the
same terminal assumption-violation error.

The exact-metric/`AllPromql` restriction admits the Phase 1 physical scalar
target while excluding projection-looking targets. Syntax alone cannot prove
that a metric has no native Histogram or ExponentialHistogram data: the normal
aggregation executor probes and combines those inputs before its scalar path.
The mode name therefore makes the caller's scalar-only assumption explicit.
The union read still decodes every overlapping source kind. If its stats show a
typed chunk, or its result has delta/mixed temporality, execution fails with an
explicit assumption-violation error. It must not silently drop typed input and
must not retry repeated after post-I/O observation, because that retry would
contaminate cache/profile evidence and could not restore corruption precedence.

## One-pass algorithm

For an outer range `[start_ms, end_ms]`, rate window `window_ms`, and step
`step_ms`:

1. Lower the selector through the existing storage-aware lowering path.
2. Apply the existing terminal-aggregation label demand. Complete canonical
   row decoding, symbol validation, and source identity verification remain
   mandatory even when only grouping labels are retained.
3. Compute the first logical window boundary with saturating arithmetic:
   `logical_start_ms = start_ms.saturating_sub(window_ms)`.
4. Compute the actual read start through the existing
   `range_selector_read_start_ms` helper. The helper remains authoritative for
   any predecessor/seed requirement.
5. Query, validate, decode, cross-segment merge, and duplicate-resolve the
   selector once over `[read_start_ms, end_ms]`.
6. For each evaluation timestamp, advance per-series bounds through the union
   result, copy the bounded samples and aligned metadata sidecars into an owned
   scratch window, and pass that window to the existing range evaluator. This
   diagnostic implementation deliberately accepts the scratch-copy cost rather
   than reimplementing rate, stale-marker, reset, or extrapolation arithmetic.
7. Feed the resulting one-sample vectors to the existing aggregation evaluator
   in the same series order as repeated execution.
8. Merge per-step outputs with the existing final range-result merger.

The cursor and owned scratch-window path must preserve:

- PromQL left-open/right-closed selection;
- timestamp-zero inclusion and logical pre-epoch duration;
- omission of only the exact stale-NaN sentinel;
- ordinary `NaN`, `+Inf`, and `-Inf` as values;
- stored reset hints across stale omission;
- deterministic cross-segment and duplicate precedence; and
- bit-exact aggregation order.

## Cache behavior

Successful one-pass execution bypasses the decoded scalar range cache because
the union result itself supplies cross-step reuse. Its finalized cache summary
must show no cache admission or retained charge.

Every fallback calls the unchanged repeated path and retains its existing
cache semantics. A fallback must be selected before storage I/O; execution may
not partially run one-pass and then retry repeated after observing data. A
post-decode scalar-assumption violation is a terminal diagnostic error, not a
fallback.

## QueryStats and limits

This experiment predeclares one intentional ordinary-`QueryStats` difference:

- repeated mode reports the sum of logical work independently charged at each
  evaluation step;
- successful one-pass mode reports the actual union selector work charged
  once.

The gate must compare values, labels, series IDs, ordering, exact and portable
semantic fingerprints, and all non-`QueryStats` correctness metadata. It must
classify the stats difference rather than claim equality. It must also reject
unexplained field movement.

Finite public limits always select repeated execution. Multiplying union stats
by the number of steps is not an exact emulation: time pruning, unique-series
sets, chunk overlap, projection fan-out, and regex work differ by step.

A promotable design must choose and specify one of these contracts:

- preserve the established per-step logical stats and limit behavior through a
  prepared logical charge trace, including corruption-versus-limit precedence;
  or
- deliberately redefine range stats and limits as optimized union work and
  update the normative storage/query contract before enabling it.

## Memory status and promotion block

The comparator retains the decoded union result and final range output. The
current storage selector API has no reservation-aware result sink, so the
comparator cannot acquire a complete byte lease before those vectors, labels,
reset hints, start times, and sidecars allocate.

Consequences:

- the post-decode estimate and process RSS are measurement evidence only;
- no configured byte value can be called a hard one-pass memory budget;
- allocation or governor refusal fallback is not implemented by this
  diagnostic comparator; and
- production enablement is forbidden regardless of a latency win.

A follow-up production design must charge before allocation or capacity growth
for result structures, samples, metadata sidecars, cursor state, step scratch,
and accumulated output. It should either thread an RAII reservation through
selector/result construction or replace the owned union with a governed
streaming/prepared representation. Final retained charge must be zero on
success and every error.

## Verification

Required focused coverage:

- whitelist and fallback AST matrix;
- finite-limit fallback with exact legacy result, stats, and error behavior;
- exact values, labels, series IDs, order, and semantic fingerprints between
  repeated and one-pass modes;
- left and right boundaries, epoch zero, stale markers, ordinary non-finite
  values, and reset hints;
- cross-segment merge and duplicate precedence;
- irregular final step, `start == end`, and timestamp overflow;
- DemandDriven and Full label materialization plus OwnedStrings and CompactIds;
- scalar-cache bypass on one-pass and unchanged cache behavior on fallback;
- explicit fallback for projection-looking names and native/delta shapes; and
- finalized summaries on success and error.

Run the real Prometheus oracle when available because this phase touches range,
staleness, reset, and extrapolation-sensitive execution.

## Measurement gate

Use one identical release binary and counterbalanced fresh processes for
`Repeated` versus `OnePassAssumeScalar`:

- Schema 8, CompactIds, DemandDriven, pread;
- 4 KiB payload coalescing and range scalar cache disabled;
- unlimited public query limits, recorded explicitly;
- `rate(...[15m])`, 5-minute step;
- outer spans of 30 minutes, 6 hours, and 24 hours;
- one CLI-cold and two warm evaluations per process;
- footer validation and independent readbacks outside timed runs; and
- exact corpus, binary, configuration, residency, schedule, and result seals.

The accepted Phase 1 corpus has only about 1.25 hours of event-time coverage.
Its 30-minute result is real-corpus evidence. Six-hour and 24-hour runs on that
corpus are sparse scheduler/step-count controls, not dense memory or latency
evidence. The result gate must say so and must not emit a promotion verdict.

A future promotion attempt needs a fingerprinted Schema 8 corpus with at least
24 hours of dense event-time coverage, plus preallocation governance and a
resolved public stats/limits contract.

## On-disk impact

None. The comparator changes no segment, chunk, index, manifest, WAL, replay,
or persisted metadata format.
