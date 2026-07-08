# PromQL correctness design

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
  - `rate(selector[range])`
  - `increase(selector[range])`
  - `sum`, `count`, and `avg`
  - `by (...)` and `without (...)` grouping for those aggregations
  - `histogram_quantile(q, expr)` over classic bucket-shaped vectors
- Keep native Histogram and ExponentialHistogram query support on the existing
  PromQL projection surface: `_count`, `_sum`, and classic `_bucket{le="..."}`
  vectors.
- Improve `rate()` and `increase()` for counters by adding range-boundary
  extrapolation over the requested range while preserving existing counter reset
  hint handling.
- Keep query limits, sealed segment merging, head+sealed merging, and the
  optimized storage selector path intact.
- Add tests for parser behavior, aggregation behavior, crossed segments,
  head+sealed data, reset hints, stale samples, and histogram bucket grouping.
- Re-run focused tests, core tests, ingester query tests, readback verification,
  and the read benchmark after implementation.

Out of scope for this increment:

- A complete native-histogram PromQL execution engine that evaluates every
  typed native Histogram and ExponentialHistogram expression directly. A later
  Histogram-first follow-up adds a sealed-segment direct native classic
  Histogram path for `histogram_quantile(q, rate(metric[range]))`; native
  aggregation, active-head native execution, delta native range execution, and
  ExponentialHistogram native execution remain separate work.
- PromQL binary operators, subqueries, offsets, recording rules, remote read
  API behavior, and full staleness/lookback delta semantics.
- Any on-disk segment format change.

## Current Model

The current parser has three top-level query forms:

- `Vector(PromqlSelector)`
- `RangeFunction(PromqlRangeFunction)`
- `HistogramQuantile(PromqlHistogramQuantile)`

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
    RangeFunction(PromqlRangeFunction),
    Aggregation(PromqlAggregation),
    HistogramQuantile(PromqlHistogramQuantile),
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
   (`sum`, `count`, `avg`, or `histogram_quantile`) reads
   `[end_ms - 5m, end_ms]` and contributes the latest sample in that window.
3. `RangeFunction(function)` reads the selector over
   `[end_ms - range_ms, end_ms]`, evaluates one instant sample per resulting
   series at `end_ms`, and returns an instant vector.
4. `Aggregation(aggregation)` evaluates its input as an instant vector, groups
   by the requested labels, and emits one sample per group at `end_ms`.
5. `HistogramQuantile(function)` evaluates its input as an instant vector,
   groups classic bucket vectors by labels minus `__name__` and `le`, and emits
   one sample per group at `end_ms`.

The storage layer still sees only selector reads. Aggregations and quantile
evaluation are pure transforms over `SegmentQueryResult`.

## Aggregation Semantics

For this increment, aggregations consume instant-vector-shaped results. Each
input series contributes its latest sample at or before the evaluation time from
the result set already read by the child expression. For selector children, that
result set is bounded by the 5-minute instant lookback window. If that latest
sample is a Prometheus stale NaN or another non-finite value, the series is
absent from the aggregation input; the evaluator must not walk backward to
resurrect an older finite sample.

Grouping rules:

- `sum(expr)`, `count(expr)`, and `avg(expr)` with no modifier produce one
  output series with no labels.
- `sum by (a, b)(expr)` keeps only labels `a` and `b`, excluding `__name__`.
- `sum without (a, b)(expr)` keeps all labels except `a`, `b`, and `__name__`.
- Group labels are sorted in canonical label order before computing the output
  `series_id`.

Operator rules:

- `sum`: sum finite input values.
- `count`: count finite input values.
- `avg`: sum finite input values divided by count.
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

The improvement is extrapolation. For each range function, compute adjusted
increase across samples in `[range_start_ms, eval_time_ms]`, then extrapolate
the observed interval to the requested range following Prometheus' counter
range behavior:

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

This does not yet implement the full Prometheus query engine. It is a targeted
counter-range correction for the storage results Chronoxide already produces.

## Histogram Quantile Semantics

`histogram_quantile(q, expr)` remains classic-bucket based in this increment.
The input expression must produce bucket-shaped series with an `le` label. The
typical input will be an aggregation over bucket rates:

```promql
sum by (le, route)(rate(http_request_duration_seconds_bucket[5m]))
```

The existing classic quantile rules remain:

- Parse `le="+Inf"` as positive infinity.
- Ignore vectors that are not bucket-shaped.
- Group by labels excluding `__name__` and `le`.
- Sort buckets by upper bound.
- Compact duplicate bucket bounds by keeping the maximum count.
- Force monotonic bucket counts before interpolation.
- Require a `+Inf` bucket.

Implementation update: a Histogram-first follow-up adds direct sealed-segment
native classic Histogram evaluation for
`histogram_quantile(q, rate(metric[range]))` when the selected cumulative
Histogram samples have identical explicit bounds. Classic `_bucket` vectors
still use the rules above. Native Histogram aggregation, active-head native
Histogram execution, delta-temporality native Histogram range execution, and
native ExponentialHistogram quantile evaluation remain future work.

## Staleness And Lookback

This increment keeps staleness handling conservative:

- Prometheus stale NaN samples are not finite and therefore do not contribute to
  `sum`, `count`, `avg`, `rate`, `increase`, or `histogram_quantile`.
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

- aggregation parameter clauses;
- binary operators;
- nested range functions;
- `rate()` or `increase()` over arbitrary expressions;
- native-histogram direct quantile evaluation;
- unsupported aggregation operators.

Invalid syntax should continue to return `PromqlQueryError::Invalid`.

## Testing

Add tests in layers:

Parser tests:

- `sum(rate(metric[5m]))`
- `sum by (le, route)(rate(metric_bucket[5m]))`
- `sum without (instance)(metric)`
- `count by (route)(metric)`
- `avg(metric)`
- `histogram_quantile(0.95, sum by (le, route)(rate(metric_bucket[5m])))`
- unsupported operators and malformed grouping clauses.

Evaluator tests:

- aggregation over sealed scalar vectors;
- aggregation over active head vectors;
- `sum by (le, route)(rate(..._bucket[5m]))` feeding
  `histogram_quantile`;
- crossed-segment counter range with reset hints;
- head+sealed duplicate timestamp precedence remains unchanged;
- latest stale samples make a series absent from aggregation input;
- stale samples do not contribute to counter functions;
- `count` and `avg` skip stale/non-finite values.

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
