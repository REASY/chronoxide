# Native Histogram PromQL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Histogram-first native PromQL execution path so `histogram_quantile(q, sum by (route)(rate(metric[range])))` can consume typed Histogram samples directly instead of requiring `_bucket` scalar fan-out.

**Architecture:** Keep the existing scalar `SegmentQueryResult` path intact for top-level selectors and classic bucket queries. Add an internal `PromqlEvalVector` value model with scalar and native Histogram variants, plus native Histogram leaf reads for sealed segments and active head. Route histogram-aware functions through the typed path, then convert only final quantile results back to scalar `SegmentQueryResult`.

**Tech Stack:** Rust 2024, `chronoxide-core::promql`, existing segment reader/query session/store APIs, `HeadBuffer`, `HistogramValue`, `CounterResetHint`, current query limits and smoke benchmark tooling.

---

## File Structure

- Modify `chronoxide-core/src/storage/segment/query_types.rs`
  - Add `PromqlEvalVector`, `PromqlHistogramSeries`, and `PromqlHistogramSample`.
  - Add merge/dedupe helpers for native Histogram series.
- Modify `chronoxide-core/src/storage/segment/query_helpers.rs`
  - Extend projection/kind matching for a private `NativeHistogram` projection.
- Modify `chronoxide-core/src/storage/segment/query_reader.rs`
  - Add sealed-segment native Histogram reads that reuse current selector planning, label loading, chunk index batching, and chunk payload batching.
- Modify `chronoxide-core/src/storage/head/buffer.rs`
  - Add active-head native Histogram reads using `SeriesSamples::Histogram`.
- Modify `chronoxide-core/src/storage/segment/query_context.rs`
  - Add session-level native Histogram selector execution and recursive native-aware PromQL execution.
- Modify `chronoxide-core/src/storage/segment/query_store.rs`
  - Add store-level and head+sealed native-aware PromQL execution.
- Modify `chronoxide-core/src/storage/segment/query_promql.rs`
  - Add native Histogram range functions, native Histogram `sum`, and native Histogram quantile conversion.
- Modify `chronoxide-core/src/storage/segment/promql_lowering.rs`
  - Add a helper that lowers a selector to a native Histogram storage selector only when the selector names the native metric, not `_bucket`, `_count`, or `_sum`.
- Test `chronoxide-core/tests/promql_query.rs`
  - Add all behavior and regression coverage.
- Modify docs after behavior changes:
  - `docs/superpowers/specs/storage.md`
  - `docs/superpowers/specs/2026-07-08-promql-correctness-design.md`

## Task 1: Red Test For Native Histogram Quantile Without Bucket Fan-Out

**Files:**
- Modify: `chronoxide-core/tests/promql_query.rs`

- [ ] **Step 1: Write the failing sealed native query test**

Add a test near the existing histogram quantile tests:

```rust
#[test]
fn promql_query_native_histogram_quantile_does_not_project_bucket_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(204),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(20.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![2, 5, 3, 0],
                    },
                ),
                (
                    6_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(40.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata {
                            reset_hint: CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0, 2.0, 4.0],
                        bucket_counts: vec![4, 10, 6, 0],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.native.duration");
                visit("route", "/native-quantile");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let execution = store
        .query_promql_with_limits(
            r#"histogram_quantile(0.5, rate(http.request.native.duration{route="/native-quantile"}[5s]))"#,
            0,
            6_000,
            QueryLimits {
                max_projected_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples.len(), 1);
    assert_eq!(execution.results[0].samples[0].0, 6_000);
    assert!((execution.results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(execution.stats.projected_series, 1);
    assert_eq!(execution.stats.typed_full_chunks_decoded, 1);
}
```

- [ ] **Step 2: Run the test and verify red**

Run:

```sh
cargo test -p chronoxide-core --test promql_query promql_query_native_histogram_quantile_does_not_project_bucket_series -- --nocapture
```

Expected before implementation: the test fails because native histogram syntax
does not yet produce one quantile result. On the current code this fails at
`assert_eq!(execution.results.len(), 1)` with `left: 0`; if a later intermediate
change routes through scalar bucket fan-out first, it may instead fail with a
`projected_series` limit error.

## Task 2: Add Internal Native Histogram Value Types

**Files:**
- Modify: `chronoxide-core/src/storage/segment/query_types.rs`
- Modify: `chronoxide-core/src/storage/segment/mod.rs`

- [ ] **Step 1: Add the private eval value structs**

Add the following to `query_types.rs` after `SegmentQueryResult`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PromqlEvalVector {
    Scalar(Vec<SegmentQueryResult>),
    Histogram(Vec<PromqlHistogramSeries>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PromqlHistogramSeries {
    pub(crate) series_id: u64,
    pub(crate) labels: QueryLabels,
    pub(crate) samples: Vec<PromqlHistogramSample>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PromqlHistogramSample {
    pub(crate) timestamp_ms: u64,
    pub(crate) count: f64,
    pub(crate) sum: Option<f64>,
    pub(crate) explicit_bounds: Arc<[f64]>,
    pub(crate) bucket_counts: Vec<f64>,
    pub(crate) reset_hint: CounterResetHint,
    pub(crate) stale: bool,
}
```

- [ ] **Step 2: Add constructors and conversion helpers**

Add methods:

```rust
impl PromqlHistogramSeries {
    pub(crate) fn new(series_id: u64, labels: QueryLabels) -> Self {
        Self {
            series_id,
            labels,
            samples: Vec::new(),
        }
    }

    pub(crate) fn push_sample(&mut self, sample: PromqlHistogramSample) {
        self.samples.push(sample);
    }

    pub(crate) fn extend_from(&mut self, mut other: PromqlHistogramSeries) {
        self.samples.append(&mut other.samples);
    }
}

impl PromqlHistogramSample {
    pub(crate) fn from_histogram_value(timestamp_ms: u64, value: HistogramValue) -> Self {
        let stale = value.metadata.is_stale();
        Self {
            timestamp_ms,
            count: value.count as f64,
            sum: value.sum,
            explicit_bounds: Arc::from(value.explicit_bounds.into_boxed_slice()),
            bucket_counts: value
                .bucket_counts
                .into_iter()
                .map(|count| count as f64)
                .collect(),
            reset_hint: value.metadata.reset_hint,
            stale,
        }
    }
}
```

- [ ] **Step 3: Add merge/dedupe helpers**

Add a `merge_histogram_query_results` helper modeled on `merge_query_results`.
It must sort by `series_id`, merge equal series, sort samples by timestamp, and
keep the last sample for duplicate timestamps.

- [ ] **Step 4: Run focused compile check**

Run:

```sh
cargo test -p chronoxide-core --test promql_query promql_query_native_histogram_quantile_does_not_project_bucket_series -- --nocapture
```

Expected: still red for behavior, not compile errors.

## Task 3: Add Native Histogram Selector Lowering And Leaf Reads

**Files:**
- Modify: `chronoxide-core/src/storage/segment/query_types.rs`
- Modify: `chronoxide-core/src/storage/segment/query_helpers.rs`
- Modify: `chronoxide-core/src/storage/segment/promql_lowering.rs`
- Modify: `chronoxide-core/src/storage/segment/query_reader.rs`
- Modify: `chronoxide-core/src/storage/head/buffer.rs`
- Modify: `chronoxide-core/src/storage/segment/query_store.rs`
- Modify: `chronoxide-core/src/storage/segment/query_context.rs`

- [ ] **Step 1: Add private native projection**

Extend `SegmentProjection`:

```rust
NativeHistogram,
```

Update helper matches so it:

- matches `ChunkKind::Histogram`;
- uses `SERIES_KIND_HISTOGRAM`;
- does not match scalar `Float`, `Int64`, Summary, or ExponentialHistogram.

- [ ] **Step 2: Add native selector lowering**

In `promql_lowering.rs`, add:

```rust
pub(super) fn native_histogram_selector_from_promql(
    selector: PromqlSelector,
) -> Result<Option<SegmentSelector>, PromqlQueryError> {
    if let Some(metric_name) = selector.metric_name.as_deref()
        && (metric_name.ends_with("_bucket")
            || metric_name.ends_with("_count")
            || metric_name.ends_with("_sum"))
    {
        return Ok(None);
    }
    if selector
        .matchers
        .iter()
        .any(|matcher| matcher.name == "le" || matcher.name == "quantile")
    {
        return Ok(None);
    }
    storage_selector_from_promql_parts(
        selector.metric_name,
        selector.matchers,
        SegmentProjection::NativeHistogram,
    )
    .map(Some)
}
```

- [ ] **Step 3: Add sealed native Histogram query**

Add this method on `SegmentReader`:

```rust
pub(super) fn query_native_histogram_with_budget(
    &self,
    selector: &SegmentSelector,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
) -> io::Result<Vec<PromqlHistogramSeries>>
```

It should reuse the existing selector planning shape from
`query_normalized_with_context` but return `Vec<PromqlHistogramSeries>`. It
must:

- read series metadata first unless full label entries are required;
- filter by `series_kind_mask_matches_projection`;
- batch chunk index reads;
- batch chunk payload reads;
- decode only `ChunkSamples::Histogram`;
- convert in-range samples with `PromqlHistogramSample::from_histogram_value`;
- call `budget.observe_matched_series`, `budget.observe_chunk_read`,
  `budget.observe_typed_full_chunk_decoded`, `budget.observe_samples_decoded`,
  and `budget.observe_projected_series`.

- [x] **Step 4: Add active-head native Histogram query**

Add this method on `HeadBuffer`:

```rust
pub(super) fn query_native_histogram_with_budget<R>(
    &self,
    labels: &R,
    selector: &SegmentSelector,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
) -> io::Result<Vec<PromqlHistogramSeries>>
where
    R: SeriesLabelResolver,
```

It should mirror `query_window_selector_with_budget`, but for
`SegmentProjection::NativeHistogram` and `SeriesSamples::Histogram`.

- [x] **Step 5: Add store/head native selector execution**

Add methods on `SegmentStoreReader`:

```rust
fn query_native_histogram_selector_with_limits(
    &self,
    selector: &SegmentSelector,
    start_ms: u64,
    end_ms: u64,
    limits: QueryLimits,
) -> Result<(Vec<PromqlHistogramSeries>, QueryStats), PromqlQueryError>
```

and equivalent head+sealed variants. Return merged native series and the same
`QueryStats` budget counters. Session-native execution remains future work.

- [x] **Step 6: Run red test**

Run:

```sh
cargo test -p chronoxide-core --test promql_query promql_query_native_histogram_quantile_does_not_project_bucket_series -- --nocapture
```

Expected: still red because the evaluator is not using the native selector yet.

## Task 4: Evaluate Native Histogram Range Functions

**Files:**
- Modify: `chronoxide-core/src/storage/segment/query_promql.rs`
- Test: `chronoxide-core/tests/promql_query.rs`

- [ ] **Step 1: Add range function unit tests**

Add tests for:

- cumulative no-reset histogram increase;
- cumulative reset-hint histogram increase;
- stale marker splitting the range;
- incompatible explicit bounds returning no result.

- [ ] **Step 2: Add helpers**

Add:

```rust
pub(super) fn evaluate_histogram_range_function(
    function: &PromqlRangeFunction,
    series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<PromqlHistogramSeries>
```

It should:

- use `range_function_start_ms`;
- drop samples at and before the last stale marker;
- require at least two finite samples;
- require identical explicit bounds;
- compute component increases using reset hints;
- apply the same extrapolation factor as scalar counter ranges;
- divide by range seconds for `rate`;
- emit one `PromqlHistogramSample` at `eval_time_ms`.

- [ ] **Step 3: Extract scalar extrapolation factor**

Refactor `extrapolated_counter_increase` so scalar and histogram range
functions share a helper:

```rust
fn counter_extrapolation_factor(
    sample_count: usize,
    first_ts: u64,
    first_value: f64,
    last_ts: u64,
    raw_increase: f64,
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<f64>
```

Scalar behavior must remain byte-for-byte equivalent in existing tests.

- [ ] **Step 4: Verify targeted range tests**

Run:

```sh
cargo test -p chronoxide-core --test promql_query native_histogram -- --nocapture
```

Expected: new native range tests pass; existing scalar tests still pass.

## Task 5: Add Native Histogram-Aware PromQL Evaluation

**Files:**
- Modify: `chronoxide-core/src/storage/segment/query_context.rs`
- Modify: `chronoxide-core/src/storage/segment/query_store.rs`
- Modify: `chronoxide-core/src/storage/segment/query_promql.rs`
- Test: `chronoxide-core/tests/promql_query.rs`

- [ ] **Step 1: Add recursive native evaluation methods**

Add evaluator methods that return `PromqlEvalVector`:

```rust
fn execute_promql_histogram_aware_instant_query(
    &self,
    query: &PromqlQuery,
    end_ms: u64,
    limits: QueryLimits,
) -> Result<(PromqlEvalVector, QueryStats), PromqlQueryError>
```

and head/session equivalents.

Supported native branches:

- `Vector(selector)` -> native Histogram selector read over instant lookback.
- `RangeFunction(function)` -> native Histogram selector read over
  `[end_ms - range_ms, end_ms]`, then `evaluate_histogram_range_function`.
- `Aggregation(sum)` -> recurse and `evaluate_histogram_sum_aggregation`.
- Any unsupported native branch -> `PromqlQueryError::Unsupported`.

- [ ] **Step 2: Add native quantile evaluation**

Add:

```rust
pub(super) fn evaluate_native_histogram_quantile(
    function: &PromqlHistogramQuantile,
    series: Vec<PromqlHistogramSeries>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult>
```

It should convert each latest non-stale histogram sample into cumulative bucket
pairs and call `classic_histogram_quantile`.

- [ ] **Step 3: Route histogram_quantile through native first**

In `execute_promql_query` and `execute_promql_query_with_head`, change the
`PromqlQuery::HistogramQuantile` arm:

1. Try native histogram-aware execution of `function.input`.
2. If it returns `PromqlEvalVector::Histogram(series)`, run native quantile.
3. If the input is unsupported for native histograms, fall back to the existing
   scalar bucket-vector path.

Do not fall back when native execution returns an explicit operator error such
as `avg` over native histograms.

- [ ] **Step 4: Verify Task 1 green**

Run:

```sh
cargo test -p chronoxide-core --test promql_query promql_query_native_histogram_quantile_does_not_project_bucket_series -- --nocapture
```

Expected: pass with `projected_series == 1`.

- [ ] **Step 5: Commit the first vertical slice**

Run:

```sh
cargo fmt
git diff --check
git add chronoxide-core/src/storage/segment/query_types.rs \
  chronoxide-core/src/storage/segment/query_helpers.rs \
  chronoxide-core/src/storage/segment/promql_lowering.rs \
  chronoxide-core/src/storage/segment/query_reader.rs \
  chronoxide-core/src/storage/head/buffer.rs \
  chronoxide-core/src/storage/segment/query_context.rs \
  chronoxide-core/src/storage/segment/query_store.rs \
  chronoxide-core/src/storage/segment/query_promql.rs \
  chronoxide-core/tests/promql_query.rs
git commit -m "feat: evaluate native histogram quantiles"
```

## Task 6: Native Histogram Sum Aggregation

**Files:**
- Modify: `chronoxide-core/src/storage/segment/query_promql.rs`
- Test: `chronoxide-core/tests/promql_query.rs`

- [x] **Step 1: Add red aggregation test**

Add:

```rust
#[test]
fn promql_query_native_histogram_quantile_over_sum_by_rate() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(205), "a"), (SeriesRef::new(206), "b")] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[
                    (
                        1_000,
                        HistogramValue {
                            count: 10,
                            sum: Some(20.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![2, 5, 3, 0],
                        },
                    ),
                    (
                        6_000,
                        HistogramValue {
                            count: 20,
                            sum: Some(40.0),
                            min: None,
                            max: None,
                            metadata: TypedSampleMetadata {
                                reset_hint: CounterResetHint::NotCounterReset,
                                ..TypedSampleMetadata::default()
                            },
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![4, 10, 6, 0],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.native.aggregate");
                    visit("route", "/native-quantile-agg");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, sum by (route)(rate(http.request.native.aggregate{route="/native-quantile-agg"}[5s])))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(
        results[0].labels.as_ref(),
        &[("route".to_string(), "/native-quantile-agg".to_string())]
    );
}
```

The expected value should match the current classic bucket aggregation test:
`1.6`.

- [x] **Step 2: Implement `evaluate_histogram_sum_aggregation`**

Group by the existing aggregation grouping rules. For each group:

- let non-`Sum` native aggregation fall back to the existing scalar path;
- use the latest non-stale sample per input series;
- require identical explicit bounds across the group;
- sum count, buckets, and present sum;
- output one `PromqlHistogramSeries` per group at `eval_time_ms`.
- drop incompatible groups until the query API has a warning channel.

- [x] **Step 3: Verify aggregation test**

Run:

```sh
cargo test -p chronoxide-core --test promql_query native_histogram -- --nocapture
```

Expected: pass.

- [ ] **Step 4: Commit**

Run:

```sh
cargo fmt
git diff --check
git add chronoxide-core/src/storage/segment/query_promql.rs chronoxide-core/tests/promql_query.rs
git commit -m "feat: aggregate native histogram rates"
```

## Task 7: Cross-Segment, Head, Reset, Stale, And Incompatibility Coverage

**Files:**
- Modify: `chronoxide-core/tests/promql_query.rs`
- Modify: production files only if tests expose bugs.

- [ ] **Step 1: Add crossed sealed segment test**

Write two sealed segment windows for the same native Histogram series and query:

```promql
histogram_quantile(0.5, rate(http.request.native.cross[15s]))
```

Expected: samples from both segments feed one range calculation.

- [x] **Step 2: Add head+sealed test**

Write the first native Histogram sample to a sealed segment and the second to
active head. Query:

```promql
histogram_quantile(0.5, rate(http.request.native.head[5s]))
```

Expected: one quantile result from the merged range.

- [x] **Step 3: Add reset-hint test**

Use `CounterResetHint::CounterReset` on the second sample. Expected: the range
function adds the second histogram component values, not `current - previous`.

- [x] **Step 4: Add stale split test**

Put a stale native Histogram sample between two finite samples. Expected: only
the finite samples after the stale marker can contribute; if fewer than two
remain, no quantile result is returned.

- [x] **Step 5: Add incompatible bounds test**

Use two input series in one native `sum by` group with different
`explicit_bounds`. Expected: the group is omitted, and the query returns no
result.

- [x] **Step 6: Verify all PromQL query tests**

Run:

```sh
cargo test -p chronoxide-core --test promql_query -- --nocapture
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

Run:

```sh
cargo fmt
git diff --check
git add chronoxide-core/tests/promql_query.rs chronoxide-core/src/storage/segment/query_promql.rs
git commit -m "test: cover native histogram promql edge cases"
```

## Task 8: Documentation And Full Verification

**Files:**
- Modify: `docs/superpowers/specs/storage.md`
- Modify: `docs/superpowers/specs/2026-07-08-promql-correctness-design.md`

- [ ] **Step 1: Update docs**

Update the current implementation notes to say:

- native classic Histogram `histogram_quantile(q, rate(metric[range]))` is
  implemented without `_bucket` fan-out;
- native classic Histogram `sum by (route)(rate(metric[range]))` is supported for
  identical explicit bounds;
- native ExponentialHistogram PromQL execution remains future work;
- top-level selector/readback scalar projection behavior remains unchanged.

- [ ] **Step 2: Run focused and full tests**

Run:

```sh
cargo test -p chronoxide-core --test promql_query -- --nocapture
cargo test -p chronoxide-core --test promql_selector -- --nocapture
cargo test -p chronoxide-core
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
```

Expected: all pass.

- [ ] **Step 3: Run release build and smoke readback**

Run:

```sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query
./target/release/chronoxide-query \
  --segments-dir data/smoke/segments-replay-001 \
  --sample-limit-per-kind 2 \
  --verify-readbacks
```

Expected: build succeeds, readback reports zero mismatches.

- [ ] **Step 4: Run native histogram benchmark if data exists**

Find a native Histogram metric in the smoke report or sample series. Then run:

```sh
./target/release/chronoxide-query \
  --segments-dir data/smoke/segments-replay-001 \
  --benchmark-repeats 3 \
  --query 'histogram_quantile(0.9, rate(<native_histogram_metric>[5m]))'
```

If no native Histogram metric exists in smoke data, record that explicitly and
run the existing read query benchmark instead:

```sh
./target/release/chronoxide-query \
  --segments-dir data/smoke/segments-replay-001 \
  --benchmark-repeats 3 \
  --query '{__name__="go_gc_duration_seconds_count"}' \
  --query '{__name__="definitely_missing_metric"}'
```

- [ ] **Step 5: Commit docs and verification updates**

Run:

```sh
cargo fmt --check
git diff --check
git add docs/superpowers/specs/storage.md \
  docs/superpowers/specs/2026-07-08-promql-correctness-design.md
git commit -m "docs: update native histogram promql status"
```
