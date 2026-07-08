# Native Histogram PromQL design

Date: 2026-07-08

## Purpose

Chronoxide persists OTLP Histogram and ExponentialHistogram samples as native
typed chunks, but the current PromQL evaluator flattens them into scalar
`_count`, `_sum`, and `_bucket` projections before functions run. That is enough
for classic bucket expressions such as:

```promql
histogram_quantile(0.9, sum by (job, le)(rate(http_request_duration_seconds_bucket[10m])))
```

It is not a true native-histogram PromQL engine. Prometheus native histogram
syntax should work without the `_bucket` suffix and without `le` grouping:

```promql
histogram_quantile(0.9, sum by (job)(rate(http_request_duration_seconds[10m])))
```

The goal of this increment is to add the internal execution model needed for
native classic OTLP Histogram samples first, without changing the sealed segment
format. A first ExponentialHistogram native slice now follows the same
interfaces for sealed `histogram_quantile(q, rate(metric[range]))`, because
schema reconciliation and interpolation rules are broader there.

## Current Model

The current query path returns scalar `SegmentQueryResult` values:

```rust
pub struct SegmentQueryResult {
    pub series_id: u64,
    pub labels: QueryLabels,
    pub samples: Vec<(u64, f64)>,
    pub counter_reset_hints: Vec<CounterResetHint>,
}
```

This has two important consequences:

- `histogram_quantile()` can only see scalar vectors with an `le` label.
- `rate()` and `increase()` can only return scalar samples.

That shape is not sufficient for native histograms. A native histogram range
function produces fractional count and bucket values after extrapolation, so the
internal PromQL value cannot reuse `HistogramValue`, whose `count` and
`bucket_counts` are `u64`.

## Scope

In scope for the first native-histogram slice:

- Add an internal PromQL value model that can carry scalar series and native
  classic Histogram series.
- Add a storage leaf mode that reads native Histogram chunks from sealed
  segments and active head without projecting them to `_bucket` series.
- Evaluate `rate(selector[range])` and `increase(selector[range])` over native
  classic Histogram samples with identical explicit bounds.
- Evaluate `sum`, `count`, and `avg` over scalar vectors as today; evaluate
  `sum` over native Histogram instant vectors when all grouped histograms have
  identical explicit bounds.
- Evaluate `histogram_quantile(q, expr)` over native classic Histogram instant
  vectors, while keeping the existing classic bucket-vector fallback.
- Keep top-level selector behavior unchanged for smoke/readback compatibility.
- Keep query limits, head+sealed merge, stale markers, reset hints, and crossed
  segment merging explicit in tests.

Out of scope for the first native-histogram slice:

- Complete native ExponentialHistogram execution beyond sealed
  `histogram_quantile(q, rate(metric[range]))`.
- Native Histogram binary operators.
- Full Prometheus range-query step execution.
- Subqueries, offsets, `@`, annotations, recording rules, and remote API result
  typing.
- Any on-disk segment format change.

## Internal Value Model

Introduce a private evaluator value type in the segment query layer:

```rust
enum PromqlEvalVector {
    Scalar(Vec<SegmentQueryResult>),
    Histogram(Vec<PromqlHistogramSeries>),
}

struct PromqlHistogramSeries {
    series_id: u64,
    labels: QueryLabels,
    samples: Vec<PromqlHistogramSample>,
}

struct PromqlHistogramSample {
    timestamp_ms: u64,
    count: f64,
    sum: Option<f64>,
    explicit_bounds: Arc<[f64]>,
    bucket_counts: Vec<f64>,
    reset_hint: CounterResetHint,
    stale: bool,
}
```

`PromqlHistogramSample` intentionally uses `f64` for additive components.
Instant-vector reads convert native `u64` counts into `f64`; range functions
then subtract, add reset corrections, and extrapolate by multiplying those
values by a fractional factor.

`min` and `max` are not carried in the first PromQL histogram value. They are
not additive and are not needed for `rate()`, `increase()`, `sum()`, or
`histogram_quantile()`. Future functions can add explicit gauge-histogram
handling if they need extrema.

## Storage Leaf Reads

Add a native Histogram projection mode for evaluator-internal use:

```rust
enum SegmentProjection {
    None,
    AllPromql {
        exponential_histogram_boundaries: Vec<f64>,
    },
    Count,
    Sum,
    HistogramBucket {
        le: Option<String>,
        exponential_histogram_boundaries: Vec<f64>,
    },
    SummaryQuantile {
        quantile: Option<String>,
    },
    NativeHistogram,
}
```

This mode:

- matches only `ChunkKind::Histogram`;
- decodes full typed chunks from sealed segments;
- reads active head `SeriesSamples::Histogram` directly;
- filters samples by the requested time range;
- preserves the native labelset and native `series_id`;
- converts stale OTLP `NO_RECORDED_VALUE` points into `stale = true` samples
  instead of scalar stale NaN;
- charges query budgets as one projected native series per matched native
  series, not as `N + 1` bucket series.

The existing scalar projection modes remain unchanged:

- `None`
- `AllPromql`
- `Count`
- `Sum`
- `HistogramBucket`
- `SummaryQuantile`

Top-level vector selectors continue to use the current scalar projection
behavior so the existing CLI smoke/readback output remains stable. Native reads
are used when a PromQL function requests histogram-aware input.

## Range Function Semantics

For scalar inputs, `rate()` and `increase()` keep the current scalar logic.

For native classic Histogram inputs, the evaluator computes one histogram sample
per input series at the evaluation timestamp.

Input eligibility:

- Require at least two finite histogram samples after the last stale marker.
- All finite samples in the selected run must have identical explicit bounds.
- `GaugeType` reset hints make counter range functions return no result.
- Any missing or incompatible bucket layout returns no result for that series.

Counter increase:

- For each adjacent sample pair, consume the current sample's reset hint.
- `CounterReset`: add the current count, buckets, and present sum.
- `NotCounterReset`: require count, buckets, and present sum to be
  non-decreasing; add component deltas.
- `Unknown`: add component deltas when non-decreasing, otherwise add current
  component values.
- If either side lacks `sum`, the output `sum` is absent.

Extrapolation:

- Reuse the current scalar extrapolation factor calculation.
- Use histogram `count` as the non-negative counter value for the
  duration-to-zero adjustment.
- `increase()` returns extrapolated component increases.
- `rate()` divides extrapolated component increases by the requested range
  seconds.
- The output reset hint is `GaugeType`, because a rate/increase result is a
  derived histogram, not an input counter stream.

Delta temporality:

- Delta Histogram native range functions are deferred to a second slice. The
  current scalar projection path already exposes cumulative-shaped projections
  for delta histograms. Native delta execution must use
  `[start_time_ms, timestamp_ms)` intervals and only sum intervals that
  intersect the query range. It must not pretend raw delta points are cumulative
  counter samples.

## Aggregation Semantics

Scalar `sum`, `count`, and `avg` keep their current behavior.

For native Histogram values:

- `sum` groups by `by` or `without` labels just like scalar aggregation.
- `sum` requires identical explicit bounds inside each group for the first
  native classic Histogram slice.
- Additive components are summed as `f64`: count, bucket counts, and present
  sum.
- If any input lacks `sum`, the aggregate output `sum` is absent.
- Latest stale samples are absent from aggregation input, matching current
  scalar aggregation behavior.
- Incompatible explicit bounds drop that aggregation group until the query API
  has a warning channel.

`count` and `avg` over native Histogram values are unsupported in this slice.
Prometheus supports histogram-aware aggregation more broadly, but the first
Chronoxide slice only implements operators needed for:

```promql
histogram_quantile(q, sum by (...)(rate(metric[range])))
```

Unsupported native histogram aggregation falls back to the existing scalar
evaluation path. It must not silently claim native execution and flatten to
bucket vectors.

## Histogram Quantile Semantics

`histogram_quantile(q, expr)` evaluates `expr` as a mixed instant vector:

- Native Histogram samples are treated individually as histograms.
- Scalar samples with an `le` label are grouped through the existing classic
  bucket-vector path.
- Scalar samples without `le` are ignored by the quantile function.

For native classic Histogram samples:

- Convert per-bucket counts into cumulative bucket counts over each explicit
  upper bound.
- Append the implicit `+Inf` bucket using `count`.
- Reuse the existing `classic_histogram_quantile()` interpolation logic for
  custom finite bucket boundaries.
- Drop `__name__` from result labels. No `le` label exists on the native path.
- Preserve ordinary grouping labels, e.g. `job`, `route`, and `namespace`.

This deliberately matches the Prometheus distinction:

- classic histogram query:
  `histogram_quantile(0.9, sum by (job, le)(rate(metric_bucket[10m])))`
- native histogram query:
  `histogram_quantile(0.9, sum by (job)(rate(metric[10m])))`

## Head And Sealed Merge

Native Histogram series must merge across:

- multiple sealed segments;
- active head plus sealed segments;
- equal timestamp duplicates using the same precedence policy as scalar
  results.

Merge identity remains `(series_id, kind)` for native reads. A native Histogram
series and a scalar series with the same labelset are not the same value type.
If a future mixed evaluator exposes both for one expression, it must keep them
separate until a function explicitly defines mixed behavior.

## Error Handling

For this slice, prefer "drop this output series" over a broad query failure when
a single native histogram series is incompatible, matching Prometheus'
element-level behavior. Still use explicit `Unsupported` errors for syntax or
operator forms Chronoxide cannot evaluate at all.

Examples:

- `histogram_quantile(q, rate(metric[5m]))` with compatible Histogram samples:
  return scalar quantiles.
- `sum by (job)(rate(metric[5m]))` with mixed explicit bounds in one group:
  omit that group.
- `avg(rate(metric[5m]))` where `rate(metric[5m])` is native Histogram:
  return `Unsupported` until native `avg` is deliberately specified.

Chronoxide does not currently expose Prometheus-style annotations. The first
implementation should add tests for omission behavior and leave annotation
plumbing as future work.

## Tests

Add tests before implementation:

1. Native Histogram quantile over cumulative sealed chunks:
   `histogram_quantile(0.5, rate(metric[5s]))` returns the same value as the
   equivalent `_bucket` query, but succeeds with `max_projected_series = 1` to
   prove it did not materialize bucket series.
2. Native Histogram quantile over `sum by (route)(rate(metric[5s]))` aggregates
   two instances without requiring `le` in the grouping labels.
3. Crossed sealed segments feed one native histogram range function before
   quantile evaluation.
4. Active head plus sealed samples feed one native histogram range function.
5. Stored reset hints drive native histogram counter reset handling.
6. A stale native Histogram sample splits the range stream.
7. Incompatible explicit bounds in a native `sum` group omit that group.
8. The existing classic `_bucket` quantile tests still pass and still require
   `le` grouping for aggregation.

## Performance Verification

The first implementation should improve query work for native histogram
expressions by avoiding bucket fan-out inside the evaluator. After tests pass,
measure at least:

```sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query
./target/release/chronoxide-query \
  --segments-dir data/smoke/segments-replay-001 \
  --benchmark-repeats 3 \
  --query 'histogram_quantile(0.9, rate(<native_histogram_metric>[5m]))'
```

Use a real native Histogram metric name from the smoke report. Compare:

- result series and sample counts;
- projected series;
- chunk reads;
- bytes read;
- warm mean;
- cold time.

If smoke data has no suitable native Histogram metric, run the focused unit and
integration tests first and defer real-data perf until a replay containing such
a metric is available.

## Follow-Up Slice: ExponentialHistogram

A first sealed and active-head native ExponentialHistogram slice is implemented
for `histogram_quantile(q, rate(metric[range]))` using a separate sample
variant.
It:

- reconcile compatible schemas by downscaling finer samples to a common
  coarser scale;
- drops `zero_threshold` mismatches from the native path;
- implements exponential interpolation rules for positive and negative exponential buckets;
- clamps one-sided zero-bucket interpolation to `[0, zero_threshold]` or
  `[-zero_threshold, 0]` when observations exist only on the positive or
  negative side;
- trims positive and negative bucket bounds adjacent to a non-zero
  `zero_threshold` before interpolation;
- preserve the current deterministic configured-boundary `_bucket` projection
  path as the compatibility fallback.

Remaining ExponentialHistogram work:

- broader quantile coverage for mixed positive/negative bucket distributions;
- Prometheus-style annotations for omitted incompatible series.
