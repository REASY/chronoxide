# PromQL correctness design

> **Archived historical record:** This document is retained for provenance and is not current authority. Consult the current contracts and code before relying on it.

Date: 2026-07-08

## Purpose

Chronoxide already serves PromQL-shaped selectors, scalar `_count`/`_sum`
projections, classic `_bucket` projections, `rate()`, `increase()`, and
`histogram_quantile()` over classic bucket vectors. The next correctness step is
to make common production PromQL expressions compose correctly without changing
the sealed segment format.

The target query shape for this increment is:

```promql
histogram_quantile(
  0.95,
  sum by (le, route) (
    rate(http_request_duration_seconds_bucket[5m])
  )
)
```

This requires a broader expression tree, aggregation operators, more
Prometheus-compatible counter range evaluation, and tests that cross sealed
segments and active head data.

## Scope

In scope:

- Parse and evaluate a scoped PromQL expression tree:
  - instant vector selector
  - scalar literals and constant scalar arithmetic expressions where a function
    or aggregation parameter requires a scalar
  - `rate(selector[range])`
  - `increase(selector[range])`
  - `delta(selector[range])` for cumulative/unknown scalar gauge-like series
  - `irate(selector[range])` for cumulative/unknown scalar counter series
  - `idelta(selector[range])` for cumulative/unknown scalar gauge-like series
  - `changes(selector[range])` for scalar series
  - `resets(selector[range])` for cumulative/unknown scalar counter series
  - `last_over_time(selector[range])` for scalar series
  - `count_over_time(selector[range])` for scalar series
  - `present_over_time(selector[range])` for scalar series
  - `sum_over_time(selector[range])` for scalar series
  - `avg_over_time(selector[range])` for scalar series
  - `stddev_over_time(selector[range])` and
    `stdvar_over_time(selector[range])` for scalar series
  - `min_over_time(selector[range])` and `max_over_time(selector[range])` for
    scalar series
  - `sum`, `count`, `avg`, `min`, and `max`
  - `by (...)` and `without (...)` grouping for those aggregations
  - `absent(expr)` over instant-vector expressions
  - `absent_over_time(selector[range])`
  - `histogram_quantile(q, expr)` over classic bucket-shaped vectors
  - scalar-scalar and vector-scalar arithmetic for `+`, `-`, `*`, `/`, `%`,
    and `^`
- Keep native Histogram and ExponentialHistogram query support on the existing
  PromQL projection surface: `_count`, `_sum`, and classic `_bucket{le="..."}`
  vectors.
- Improve `rate()` and `increase()` for counters by adding range-boundary
  extrapolation over the requested range while preserving existing counter reset
  hint handling. Add scalar `irate()` over cumulative/unknown scalar counter
  series on the same selector range surface using the last two valid samples
  without range-boundary extrapolation. Add scalar `delta()`/`idelta()` over
  cumulative/unknown scalar gauge-like series using Prometheus' extrapolated and
  last-two-sample range behavior without counter-reset adjustment. Add scalar
  `changes()` over selector range vectors using Prometheus' value-transition
  rule, where consecutive ordinary IEEE `NaN` values are not counted as
  changes and exact Prometheus stale markers are skipped. Add scalar `resets()`
  over selector range vectors using counter decreases or stored
  `CounterResetHint::CounterReset` metadata after the last stale/non-finite
  boundary, dropping gauge-typed reset-hint streams. Add scalar
  `last_over_time()` over selector range vectors, preserving metric names and
  skipping Prometheus stale markers when choosing the last sample. Add scalar
  `count_over_time()` over selector range vectors, counting non-stale scalar
  samples while preserving ordinary IEEE `NaN`/`Inf` as present samples. Add
  scalar `present_over_time()` over selector range vectors, returning `1` when
  any non-stale scalar sample is present, treating ordinary IEEE `NaN`/`Inf` as
  present, and dropping metric names. Add scalar `sum_over_time()` over selector
  range vectors, summing non-stale scalar samples, preserving ordinary IEEE
  `NaN`/`Inf` as values, and dropping metric names. Add scalar
  `avg_over_time()` over selector range vectors, averaging non-stale scalar
  samples with overflow-resistant mean calculation, preserving ordinary IEEE
  `NaN`/`Inf` as values, and dropping metric names. Add scalar
  `stddev_over_time()` and `stdvar_over_time()` over selector range vectors,
  calculating population standard deviation and variance over non-stale scalar
  samples with Prometheus-compatible ordinary-NaN/Inf propagation and dropping
  metric names. Add scalar
  `min_over_time()` and `max_over_time()` over selector range vectors,
  selecting from non-stale scalar samples with Prometheus-compatible
  ordinary-NaN ordering and dropping metric names.
- Keep query limits, sealed segment merging, head+sealed merging, and the
  optimized storage selector path intact.
- Add tests for parser behavior, aggregation behavior, crossed segments,
  head+sealed data, reset hints, stale samples, and histogram bucket grouping.
- Re-run focused tests, core tests, ingester query tests, readback verification,
  and the read benchmark after implementation.

Out of scope for this increment:

- A complete native-histogram PromQL execution engine that evaluates every
  typed native Histogram and ExponentialHistogram expression directly. A later
  Histogram-first follow-up adds a sealed and active-head native classic
  Histogram path for `histogram_quantile(q, rate(metric[range]))` and native
  Histogram `sum`/`avg` aggregation before quantile, plus a sealed and
  active-head native ExponentialHistogram `histogram_quantile(q,
  rate(metric[range]))` path with native `sum`/`avg` aggregation before
  quantile. Delta native range
  execution now uses decoded start-time intervals when available and retains
  cumulative-stitch fallback behavior for older decoded samples without those
  intervals.
- Binary fill modifiers, subqueries, offsets, recording rules, remote read API
  behavior, and full staleness/lookback delta semantics.
- Direct native Histogram/ExponentialHistogram `changes()`/`resets()`,
  `irate()` execution, and `delta()`/`idelta()` execution, plus
  delta-temporality scalar projection `irate()`/`delta()`/`idelta()`. Until
  those are designed, native typed range evaluators must not silently treat
  those functions as `increase()`.
- Any on-disk segment format change.

## Current Model

PromQL syntax is parsed with the `promql-parser` crate. Chronoxide then lowers
the external AST into its storage-aware supported expression tree, with a small
compatibility rewrite for OTLP-style dotted metric and label names used by
existing callers.

The current lowered query forms are:

- `Vector(PromqlSelector)`
- `Scalar(f64)`
- `RangeFunction(PromqlRangeFunction)`
- `Aggregation(PromqlAggregation)`
- `Absent(PromqlAbsent)`
- `AbsentOverTime(PromqlAbsentOverTime)`
- `HistogramQuantile(PromqlHistogramQuantile)`
- `HistogramFraction(PromqlHistogramFraction)`
- `HistogramScalarFunction(PromqlHistogramScalarFunction)`
- `BinaryExpression(PromqlBinaryExpression)`

Scalar-only parameters for `histogram_quantile`, `histogram_fraction`, `topk`,
`bottomk`, and `quantile` accept constant scalar arithmetic expressions over
`+`, `-`, `*`, `/`, `%`, and `^`. Parameter expressions that depend on vectors
remain invalid because this increment does not execute parameter subqueries.

The current evaluator lowers each selector to one or more storage selectors,
reads `SegmentQueryResult`s from sealed segments and optional active head data,
then applies range functions or quantile evaluation over those results.

This model already has good storage separation:

- storage selectors remain leaf scans;
- results are merged across segments by stable `series_id`;
- virtual typed projections produce PromQL-shaped labels and scalar samples;
- reset hints are carried with projected counter samples where available.

The limitation is expression composition. Aggregations cannot sit between
`rate()` and `histogram_quantile()`, and `rate()` currently divides by elapsed
time between the first and last sample instead of the requested range with
Prometheus-style extrapolation.

## Proposed Architecture

Replace the flat `PromqlQuery` variants with a scoped expression tree:

```rust
enum PromqlQuery {
    Vector(PromqlSelector),
    Scalar(f64),
    RangeFunction(PromqlRangeFunction),
    Aggregation(PromqlAggregation),
    Absent(PromqlAbsent),
    AbsentOverTime(PromqlAbsentOverTime),
    HistogramQuantile(PromqlHistogramQuantile),
    HistogramFraction(PromqlHistogramFraction),
    HistogramScalarFunction(PromqlHistogramScalarFunction),
    BinaryExpression(PromqlBinaryExpression),
}

struct PromqlRangeFunction {
    kind: PromqlRangeFunctionKind,
    selector: PromqlSelector,
    range_ms: u64,
}

struct PromqlAggregation {
    op: PromqlAggregationOp,
    grouping: PromqlAggregationGrouping,
    input: Box<PromqlQuery>,
}

enum PromqlAggregationOp {
    Sum,
    Count,
    Avg,
}

enum PromqlAggregationGrouping {
    All,
    By(Vec<String>),
    Without(Vec<String>),
}

struct PromqlBinaryExpression {
    op: PromqlBinaryOp,
    left: Box<PromqlQuery>,
    right: Box<PromqlQuery>,
}
```

`RangeFunction` remains selector-based for now. This keeps range vector support
bounded and avoids pretending we have full PromQL subquery semantics. The input
to aggregation and `histogram_quantile()` can be any scoped instant-vector
expression.

## Evaluation Flow

Evaluation stays recursive:

1. Top-level `Vector(selector)` lowers to storage selectors and reads matching
   samples for `[start_ms, end_ms]` to preserve the existing range-read API used
   by smoke/readback tooling.
2. A `Vector(selector)` used as the child of an instant-vector operator
   (`sum`, `count`, `avg`, `min`, `max`, or `histogram_quantile`) reads
   `[end_ms - 5m, end_ms]` and contributes the latest sample in that window.
3. `RangeFunction(function)` reads the selector over the storage span needed for
   `[end_ms - range_ms, end_ms]`, evaluates PromQL samples in the
   left-open/right-closed range `(end_ms - range_ms, end_ms]`, emits one instant
   sample per resulting series at `end_ms`, and returns an instant vector.
4. `Aggregation(aggregation)` evaluates its input as an instant vector, groups
   by the requested labels, and emits one sample per group at `end_ms`.
5. `HistogramQuantile(function)` evaluates its input as an instant vector,
   groups classic bucket vectors by labels minus `__name__` and `le`, and emits
   one sample per group at `end_ms`.
6. `count_values("label", expr)` evaluates its instant-vector input, injects
   the sample value as the configured output label, and counts equal values per
   effective grouping. For non-`without` grouping, the generated value label is
   part of the grouping key, matching Prometheus behavior.
7. `absent(expr)` evaluates its input as an instant vector. If any input series
   has a present latest sample, it emits no result; otherwise it emits one
   sample with value `1` at `end_ms`. For direct vector selector inputs, output
   labels are derived from unique equality matchers using normalized PromQL
   label names, excluding `__name__` and excluding regex, negative, or duplicate
   matchers.
8. `absent_over_time(selector[range])` reads the selector over
   `(end_ms - range, end_ms]`. If any non-stale sample is present in that
   left-open, right-closed range, it emits no result; otherwise it emits one
   sample with value `1` at
   `end_ms`, deriving normalized labels from unique equality matchers as
   `absent()` does. Stale markers by themselves do not count as present range
   samples, while IEEE `NaN`/`Inf` values are still present samples.
9. `BinaryExpression(expression)` supports scalar-scalar, vector-scalar, and
   vector-vector arithmetic/comparison over instant-vector inputs, plus set
   operators. Arithmetic covers `+`, `-`, `*`, `/`, `%`, and `^`.
   Vector-scalar arithmetic drops `__name__` and preserves other labels on the
   vector side. Vector-vector matching uses all labels except `__name__` by
   default and supports `on(...)`, `ignoring(...)`, `group_left`, and
   `group_right` matching modifiers for arithmetic and comparison operators.
   `on(...)` matches only the listed labels, including `__name__` when it is
   explicitly listed. Arithmetic result labels follow PromQL grouping-label
   output and drop `__name__`; non-`bool` comparisons retain the left metric
   name except for one-to-one `on(...)`, which drops `__name__` even when it is
   explicitly listed, and `group_right` comparisons retain the right metric name
   to avoid collisions.
   Set operators stay on the many-to-many matching path, including
   `on(...)`/`ignoring(...)` label selection, but they do not support
   `group_left` or `group_right` modifiers.

The storage layer still sees only selector reads. Aggregations and quantile
evaluation are pure transforms over `SegmentQueryResult`.

## Aggregation Semantics

For this increment, aggregations consume instant-vector-shaped results. Each
input series contributes its latest sample at or before the evaluation time from
the result set already read by the child expression. For selector children, that
result set is bounded by the 5-minute instant lookback window. If that latest
sample is the exact Prometheus stale NaN marker, the series is absent from the
aggregation input; the evaluator must not walk backward to resurrect an older
finite sample. Other present IEEE float values, including infinities, remain
input values for the standard scalar aggregation operators.

Grouping rules:

- `sum(expr)`, `count(expr)`, `avg(expr)`, `min(expr)`, and `max(expr)` with no modifier produce one
  output series with no labels.
- `sum by (a, b)(expr)` keeps only labels `a` and `b`; if `__name__` is
  explicitly listed, it is preserved and participates in the grouping key.
- `sum without (a, b)(expr)` keeps all labels except `a`, `b`, and `__name__`.
- Group labels are sorted in canonical label order before computing the output
  `series_id`.

Operator rules:

- `sum`: sum non-stale input values using IEEE float arithmetic.
- `count`: count non-stale input values.
- `avg`: arithmetic mean of non-stale input values, using an
  overflow-resistant running mean for finite inputs so large same-signed values
  do not turn a finite average into `+Inf`/`-Inf`.
- `min`: minimum non-stale input value.
- `max`: maximum non-stale input value.
- `topk` and `bottomk`: rank non-stale input values using Prometheus ordering:
  finite and infinite values outrank ordinary IEEE `NaN` values for both
  operators; `NaN` samples may still be returned when `k` exceeds the number of
  finite/infinite candidates.
- `quantile`: sort ordinary IEEE `NaN` values before finite/infinite values,
  matching Prometheus vector quantile ordering. The exact stale marker remains
  absent before sorting.
- `count_values`: counts non-stale input values by sample value label,
  normalizing the configured output label name with the same PromQL label-name
  normalization used for stored labels, and formatting sample values with the
  same Go `strconv.FormatFloat(v, 'g', -1, 64)` style used for PromQL float
  labels, including IEEE specials as `+Inf`, `-Inf`, and `NaN`.
- Empty groups emit no result.

This is enough for `sum by (le, route)(rate(..._bucket[5m]))` while keeping
unsupported PromQL behavior explicit.

## Counter Range Semantics

The existing reset handling should stay:

- If reset hints are present and aligned with samples, use them.
- If reset hints are absent, detect resets by counter decreases.
- `GaugeType` reset hints make counter functions return no result.
- A stale/non-finite sample inside the selected range is a stream boundary:
  counter functions ignore samples at and before the last such marker, then
  evaluate the finite run after it. If fewer than two finite samples remain,
  the function returns no result.
- Extrapolation after such a boundary starts at the stale marker, not at the
  original range start; native Histogram and ExponentialHistogram range paths
  follow the same rule as scalar counters.

The improvement is extrapolation. For each counter/gauge range function,
compute adjusted increase or delta across samples in
`(range_start_ms, eval_time_ms]`, then extrapolate the observed interval to the
requested range following Prometheus' counter range behavior:

- Require at least two valid samples.
- Use first and last sample timestamps as the observed interval.
- Estimate average spacing between samples.
- Allow extrapolation to the range boundaries when samples are close enough to
  the boundaries.
- Avoid extrapolating counters below zero by considering the time to a
  theoretical zero point when the first value is positive and the adjusted
  increase is positive.
- `increase()` returns the extrapolated increase.
- `rate()` returns extrapolated increase divided by `range_ms` seconds.
- `delta()` returns the extrapolated scalar difference between the first and
  last valid samples, without counter reset handling or zero-point clamping.
- `irate()` uses only the last two valid scalar counter samples after the last
  stale/non-finite boundary, applies the same reset-hint/decrease handling to
  that pair, and divides by the observed seconds between those two samples.
- `idelta()` returns the scalar difference between the last two valid samples
  after the last stale/non-finite boundary, without counter reset handling.
- `changes()` counts value transitions between non-stale scalar samples in the
  selected range, treats consecutive ordinary IEEE `NaN` values as unchanged,
  and drops `__name__`.
- `resets()` counts counter resets after the last stale/non-finite boundary,
  using stored reset hints when aligned and otherwise falling back to value
  decreases. `CounterResetHint::GaugeType` makes the function return no result.
- `last_over_time()` returns the last non-stale scalar sample in the selected
  range and preserves `__name__`, matching Prometheus' metric-name behavior for
  this range function.
- `count_over_time()` returns the count of non-stale scalar samples in the
  selected range, drops `__name__`, and treats ordinary IEEE `NaN`/`Inf` values
  as present samples.
- `sum_over_time()` sums non-stale scalar samples in the selected range, drops
  `__name__`, and preserves ordinary IEEE `NaN`/`Inf` values as values rather
  than treating them as stale markers.
- `avg_over_time()` averages non-stale scalar samples in the selected range,
  drops `__name__`, preserves ordinary IEEE `NaN`/`Inf` values as values, and
  avoids overflowing the result when large same-signed finite samples have a
  finite mean.
- `stddev_over_time()` and `stdvar_over_time()` calculate population standard
  deviation and variance over non-stale scalar samples in the selected range,
  drop `__name__`, preserve ordinary IEEE `NaN`/`Inf` propagation, and use a
  compensated Welford-style update matching Prometheus' range functions.
- `min_over_time()` and `max_over_time()` select the minimum or maximum
  non-stale scalar sample in the selected range, drop `__name__`, preserve
  infinities, and let ordinary IEEE `NaN` win only when no later comparable
  value replaces an already-NaN candidate.

This does not yet implement the full Prometheus query engine. It is a targeted
counter-range correction for the storage results Chronoxide already produces.

## Histogram Quantile Semantics

`histogram_quantile(q, expr)` remains classic-bucket based in this increment.
The input expression must produce bucket-shaped series with an `le` label from
either real classic `_bucket` float series or virtual native histogram bucket
projections. The typical input will be an aggregation over bucket rates:

```promql
sum by (le, route)(rate(http_request_duration_seconds_bucket[5m]))
```

Selectors over real classic `_bucket` float series use normal PromQL label
matching, including regex and negative `le` matchers. Native virtual bucket
projection applies absent, equality, inequality, regex, and negative regex
`le` matchers to the synthetic bucket label after native decoding. Multiple
`le` matchers are evaluated as a conjunction, matching normal PromQL label
matcher behavior.

The existing classic quantile rules remain:

- Parse `le="+Inf"` as positive infinity.
- Ignore vectors that are not bucket-shaped.
- Group by labels excluding `__name__` and `le`.
- Sort buckets by upper bound.
- Compact duplicate bucket bounds by summing their non-negative counts before
  monotonic repair, matching Prometheus bucket coalescing.
- Force monotonic bucket counts before interpolation.
- Require a `+Inf` bucket.
- If a classic bucket group has fewer than two buckets or lacks a `+Inf`
  bucket, emit a NaN result sample for that group, matching Prometheus
  `BucketQuantile` special cases.

Implementation update: a Histogram-first follow-up adds direct sealed and
active-head native classic Histogram evaluation for
`histogram_quantile(q, rate(metric[range]))` and
`histogram_quantile(q, sum by/without (...)(rate(metric[range])))` or
`histogram_quantile(q, avg by/without (...)(rate(metric[range])))` when the
selected cumulative Histogram samples have identical explicit bounds. Classic
`_bucket` vectors still use the rules above. This native classic Histogram path
works across sealed segments and active head. A sealed and active-head native
ExponentialHistogram path evaluates `histogram_quantile(q,
rate(metric[range]))` over compatible cumulative ExponentialHistogram samples
with downscaling to a common coarser scale and exponential interpolation for
positive and negative exponential buckets, including native `sum by`/`sum without`
and `avg by`/`avg without` aggregation before quantile. Native
Histogram/ExponentialHistogram `sum`/`avg` aggregation treats stale input
samples as absent and averages over the remaining compatible inputs. Native
`histogram_quantile` also evaluates physical scalar bucket samples with an
`le` label from the same input expression, returning classic bucket quantile
results alongside native histogram quantile results while excluding virtual
native projections from that scalar side. Native
Histogram/ExponentialHistogram `count` and `group` aggregation returns scalar
PromQL aggregation results over native histogram elements directly, rather than
counting virtual bucket/count/sum projections. If the input expression also
contains physical Float/Int64 scalar elements, the same `count`/`group`
aggregation combines those scalar elements with the native Histogram/
ExponentialHistogram elements under the requested grouping labels without
materializing virtual histogram projections for the scalar side. One-sided zero
buckets are clamped to the observed side of zero before linear interpolation,
and bucket bounds adjacent to a non-zero zero threshold are trimmed before
interpolation.
Delta-temporality native Histogram/ExponentialHistogram range execution uses decoded
`[start_time_ms, time_ms)` intervals when they are available, sums selected
intervals that intersect the evaluation range, and can therefore produce a
valid `rate()`/`increase()` result from one complete delta interval. If decoded
start times are unavailable, it falls back to converting selected delta samples
into the same in-range cumulative sequence exposed by virtual `_count`/`_sum`/
`_bucket` projections, then applies the existing reset-aware
`rate`/`increase` math.
For virtual scalar projections over delta Histogram/ExponentialHistogram data,
range evaluation also stitches sealed/head or chunk-local cumulative fragments
before applying `rate`/`increase`; fragment starts are internal boundaries, not
PromQL-visible counter resets. Scalar delta projections also retain decoded
`start_time_ms` in the in-memory query result. When that metadata is present,
`rate()` and `increase()` aggregate selected `[start_time_ms, time_ms)` delta
intervals directly, which makes one complete delta interval inside the range a
valid range-function input.
`histogram_fraction(lower, upper, expr)` is implemented for native
Histogram/ExponentialHistogram instant-vector results, including `rate()` and
native `sum by`/`sum without` and `avg by`/`avg without` aggregation inputs.
Bounds may be finite or `-Inf`/`Inf`, but not `NaN`. Classic bucket vectors are
ignored by this native function. Inside native histogram functions, selector
metric names ending in `_count`, `_sum`, or `_bucket` remain literal native
metric names rather than virtual projection rewrites.
For ordinary PromQL selectors, real scalar series and virtual native
`_count`/`_sum`/`_bucket` projections may be matched by the same query rewrite.
If they produce the same final PromQL labelset, the evaluator rejects the query
with an invalid conflict error rather than silently merging or applying
precedence.

## Staleness And Lookback

This increment keeps staleness handling conservative:

- Prometheus stale NaN samples are not finite and therefore do not contribute to
  scalar aggregations, `rate`, `increase`, or `histogram_quantile`. Present
  IEEE float values such as `+Inf` and `-Inf` are still aggregation inputs.
- For binary instant-vector arithmetic, comparison, and set operators, the exact
  Prometheus stale marker makes that input series absent. Other present IEEE
  float values, including infinities, remain input values.
- For `rate` and `increase`, a stale/non-finite sample splits the counter
  stream; evaluation uses only the finite samples after the last split marker.
- For instant-vector aggregations, a latest stale NaN removes that series from
  the aggregation input instead of falling back to an older finite sample.
- Selector children of instant-vector operators use a fixed 5-minute lookback
  delta ending at the evaluation timestamp.
- Top-level selector reads still use the explicit query time range passed to the
  storage layer.

Full Prometheus lookback/staleness behavior should be a later design because it
requires configurable lookback, step/range evaluation, API metadata, and
staleness handling beyond this scoped instant evaluation path.

## Error Handling

Unsupported PromQL should fail explicitly with `PromqlQueryError::Unsupported`.

Initial unsupported cases include:

- aggregation parameter clauses outside the supported `topk`, `bottomk`,
  `quantile`, and `count_values` forms;
- vector-valued parameter expressions where a function or aggregation requires a
  constant scalar parameter;
- binary fill modifiers;
- nested range functions;
- `rate()`, `increase()`, `delta()`, `irate()`, `idelta()`, `changes()`,
  `resets()`,
  `last_over_time()`, `count_over_time()`, `present_over_time()`,
  `sum_over_time()`, `avg_over_time()`, `stddev_over_time()`,
  `stdvar_over_time()`, `min_over_time()`, or `max_over_time()` over arbitrary
  expressions;
- unsupported aggregation operators.

Invalid syntax should continue to return `PromqlQueryError::Invalid`.

## Testing

Add tests in layers:

Parser tests:

- `sum(rate(metric[5m]))`
- `delta(metric[5m])`
- `irate(metric[5m])`
- `idelta(metric[5m])`
- `changes(metric[5m])`
- `resets(metric[5m])`
- `last_over_time(metric[5m])`
- `count_over_time(metric[5m])`
- `present_over_time(metric[5m])`
- `sum_over_time(metric[5m])`
- `avg_over_time(metric[5m])`
- `stddev_over_time(metric[5m])`
- `stdvar_over_time(metric[5m])`
- `min_over_time(metric[5m])`
- `max_over_time(metric[5m])`
- `sum by (le, route)(rate(metric_bucket[5m]))`
- `sum without (instance)(metric)`
- `count by (route)(metric)`
- `avg(metric)`
- `min by (route)(metric)`
- `max without (instance)(metric)`
- `histogram_quantile(0.95, sum by (le, route)(rate(metric_bucket[5m])))`
- unsupported operators and malformed grouping clauses.

Evaluator tests:

- aggregation over sealed scalar vectors;
- aggregation over active head vectors;
- `sum by (le, route)(rate(..._bucket[5m]))` feeding
  `histogram_quantile`;
- `count by (route)(rate(native_histogram[5m]))` counts native histogram
  elements directly under a projected-series budget that would fail if the
  evaluator materialized bucket projections;
- crossed-segment counter range with reset hints;
- head+sealed duplicate timestamp precedence remains unchanged;
- latest stale samples make a series absent from aggregation input;
- stale samples do not contribute to counter functions;
- `count`, `avg`, `min`, and `max` skip stale marker values while preserving
  present IEEE infinities.

Verification commands:

```sh
cargo test -p chronoxide-core --test promql_selector -- --nocapture
cargo test -p chronoxide-core --test promql_query -- --nocapture
cargo test -p chronoxide-core
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
cargo fmt --check
git diff --check
```

Real data verification:

```sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query && \
  ./target/release/chronoxide-query \
    --segments-dir data/smoke/segments-replay-001 \
    --sample-limit-per-kind 2 \
    --verify-readbacks
```

Current readback verification samples decoded chunk data and checks exact
PromQL selectors/projections. For sampled scalar chunks with enough finite
points at the sampled chunk end, it also verifies `rate()` and `increase()`
using independently computed Prometheus-style extrapolated counter math. For
sampled cumulative or unspecified Histogram and ExponentialHistogram chunks, it
also verifies projected `_count`, `_sum`, and sampled `_bucket` `rate()`/
`increase()` readbacks when decoded reset hints make the independent expected
counter math well-defined and the exact projection query is isolated to the
sampled chunk over that verification range. Overlapping chunks with the same
labelset are exact-readback checked but skipped for these derived range
readbacks. Delta-temporality typed histogram range readbacks remain covered by
focused query tests because those paths use decoded `[start_time_ms, time_ms)`
intervals rather than pure projected counter samples.

Perf verification:

```sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query && \
  ./target/release/chronoxide-query \
    --segments-dir data/smoke/segments-replay-001 \
    --benchmark-repeats 3 \
    --query '{__name__="go_gc_duration_seconds_count"}' \
    --query '{__name__="definitely_missing_metric"}'
```

## Performance Constraints

The optimized storage read path must remain the source of raw samples. The
expression evaluator should avoid forcing full label materialization earlier
than existing selector reads do.

Expected cost:

- Parser overhead is negligible.
- Aggregation cost is linear in returned instant-vector series.
- Histogram quantile cost is linear in bucket series plus per-group bucket sort.
- Existing exact selector read benchmarks should not regress materially because
  plain vector queries should follow the same lowering path.

If the exact-metric read benchmark regresses by more than noise, profile before
keeping the change.

## Implementation Order

1. Parser AST expansion and tests.
2. Recursive evaluator plumbing with no behavior change for existing query
   forms.
3. Aggregation evaluation and tests.
4. Prometheus-style counter range extrapolation and tests.
5. `histogram_quantile(... sum by (...)(rate(...)))` integration tests.
6. Real-data verifier and perf benchmark.
