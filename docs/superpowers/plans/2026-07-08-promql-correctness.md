# PromQL Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add scoped PromQL expression composition, aggregation, and more Prometheus-compatible counter range semantics without changing the sealed segment format.

**Architecture:** Keep storage selectors as leaf scans and evaluate PromQL as a recursive expression tree over `SegmentQueryResult`. Add `Aggregation` to the parser AST, route it through existing sealed/head query dispatch, and implement aggregation plus counter extrapolation as pure result transforms.

**Tech Stack:** Rust 2024, existing `chronoxide-core::promql`, existing `SegmentStoreReader`, `SegmentStoreQuerySession`, `HeadBuffer`, `SegmentQueryResult`, and current PromQL projection path.

---

## File Structure

- Modify `chronoxide-core/src/promql/mod.rs`: add aggregation AST types and parser support for `sum`, `count`, `avg`, `by (...)`, and `without (...)`.
- Modify `chronoxide-core/src/storage/segment/query_promql.rs`: add aggregation evaluation and replace simple counter rate elapsed logic with extrapolated counter range logic.
- Modify `chronoxide-core/src/storage/segment/query_context.rs`: route `Aggregation` through prewarm, prefetch, and execution recursion for session queries.
- Modify `chronoxide-core/src/storage/segment/query_store.rs`: route `Aggregation` through sealed-only and head+sealed recursive execution.
- Modify `chronoxide-core/tests/promql_selector.rs`: parser red/green coverage.
- Modify `chronoxide-core/tests/promql_query.rs`: evaluator correctness coverage.
- No storage format files should change. Do not modify `docs/superpowers/specs/storage.md` unless implementation reveals an on-disk semantic change.

## Task 1: Parser AST For Aggregations

**Files:**
- Modify: `chronoxide-core/src/promql/mod.rs`
- Test: `chronoxide-core/tests/promql_selector.rs`

- [ ] **Step 1: Write failing parser tests**

Add tests to `chronoxide-core/tests/promql_selector.rs`:

```rust
use chronoxide_core::promql::{
    PromqlAggregation, PromqlAggregationGrouping, PromqlAggregationOp,
};

#[test]
fn parse_sum_by_over_rate_query() {
    let query = parse_query(r#"sum by (le, route)(rate(http_request_duration_bucket[5m]))"#)
        .unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Sum,
            grouping: PromqlAggregationGrouping::By(vec![
                "le".to_string(),
                "route".to_string(),
            ]),
            input: Box::new(PromqlQuery::RangeFunction(PromqlRangeFunction {
                kind: PromqlRangeFunctionKind::Rate,
                selector: PromqlSelector {
                    metric_name: Some("http_request_duration_bucket".to_string()),
                    matchers: Vec::new(),
                },
                range_ms: 300_000,
            })),
        })
    );
}

#[test]
fn parse_sum_without_query() {
    let query = parse_query(r#"sum without (instance)(cpu_usage{job="api"})"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Sum,
            grouping: PromqlAggregationGrouping::Without(vec!["instance".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: vec![PromqlMatcher {
                    name: "job".to_string(),
                    op: PromqlMatcherOp::Eq,
                    value: "api".to_string(),
                }],
            })),
        })
    );
}

#[test]
fn parse_count_by_query() {
    let query = parse_query(r#"count by (route)(http_requests_total)"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Count,
            grouping: PromqlAggregationGrouping::By(vec!["route".to_string()]),
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("http_requests_total".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_avg_without_grouping_query() {
    let query = parse_query(r#"avg(cpu_usage)"#).unwrap();

    assert_eq!(
        query,
        PromqlQuery::Aggregation(PromqlAggregation {
            op: PromqlAggregationOp::Avg,
            grouping: PromqlAggregationGrouping::All,
            input: Box::new(PromqlQuery::Vector(PromqlSelector {
                metric_name: Some("cpu_usage".to_string()),
                matchers: Vec::new(),
            })),
        })
    );
}

#[test]
fn parse_histogram_quantile_over_sum_by_rate_query() {
    let query = parse_query(
        r#"histogram_quantile(0.95, sum by (le, route)(rate(http_request_duration_bucket[5m])))"#,
    )
    .unwrap();

    assert!(matches!(query, PromqlQuery::HistogramQuantile(_)));
    let PromqlQuery::HistogramQuantile(function) = query else {
        unreachable!("matched above");
    };
    assert!(matches!(*function.input, PromqlQuery::Aggregation(_)));
}
```

- [ ] **Step 2: Run parser tests to verify red**

Run:

```sh
cargo test -p chronoxide-core --test promql_selector -- --nocapture
```

Expected: compile failure because `PromqlAggregation`, `PromqlAggregationGrouping`, and `PromqlAggregationOp` do not exist.

- [ ] **Step 3: Add aggregation AST types**

In `chronoxide-core/src/promql/mod.rs`, extend the AST:

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum PromqlQuery {
    Vector(PromqlSelector),
    RangeFunction(PromqlRangeFunction),
    Aggregation(PromqlAggregation),
    HistogramQuantile(PromqlHistogramQuantile),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlAggregation {
    pub op: PromqlAggregationOp,
    pub grouping: PromqlAggregationGrouping,
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromqlAggregationOp {
    Sum,
    Count,
    Avg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromqlAggregationGrouping {
    All,
    By(Vec<String>),
    Without(Vec<String>),
}
```

- [ ] **Step 4: Parse aggregation calls**

In `SelectorParser::parse_query`, after the `histogram_quantile` branch and before the range-function branch, parse aggregation names:

```rust
if let Some(op) = match name.as_str() {
    "sum" => Some(PromqlAggregationOp::Sum),
    "count" => Some(PromqlAggregationOp::Count),
    "avg" => Some(PromqlAggregationOp::Avg),
    _ => None,
} {
    return self.parse_aggregation(op);
}
```

Add helper methods:

```rust
fn parse_aggregation(&mut self, op: PromqlAggregationOp) -> Result<PromqlQuery, PromqlQueryError> {
    let grouping = self.parse_aggregation_grouping()?;
    self.skip_ws();
    let input_start = self.pos;
    let input_end = self.find_current_call_end()?;
    let input = self.input[input_start..input_end].trim();
    if input.is_empty() {
        return Err(self.invalid("aggregation input is empty"));
    }
    let input = parse_query(input)?;
    self.pos = input_end;
    if self.peek_char() != Some(')') {
        return Err(self.invalid("expected ')'"));
    }
    self.bump_char();
    self.skip_ws();
    if !self.is_eof() {
        return Err(self.invalid("unexpected trailing input"));
    }
    Ok(PromqlQuery::Aggregation(PromqlAggregation {
        op,
        grouping,
        input: Box::new(input),
    }))
}
```

`parse_aggregation_grouping` must accept:

- no grouping: `sum(expr)` -> `All`
- `by (label, label)`: `By`
- `without (label, label)`: `Without`

It must reject duplicate labels and empty grouping labels with `Invalid`.

- [ ] **Step 5: Run parser tests to verify green**

Run:

```sh
cargo test -p chronoxide-core --test promql_selector -- --nocapture
```

Expected: parser tests pass.

- [ ] **Step 6: Commit parser AST work**

Run:

```sh
cargo fmt
git diff --check
git add chronoxide-core/src/promql/mod.rs chronoxide-core/tests/promql_selector.rs
git commit -m "feat: parse promql aggregations"
```

## Task 2: Recursive Query Dispatch For Aggregation

**Files:**
- Modify: `chronoxide-core/src/storage/segment/query_context.rs`
- Modify: `chronoxide-core/src/storage/segment/query_store.rs`
- Test: `chronoxide-core/tests/promql_query.rs`

- [ ] **Step 1: Write failing evaluator smoke test**

Add this test to `chronoxide-core/tests/promql_query.rs`:

```rust
#[test]
fn promql_query_sum_aggregation_over_sealed_vectors() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(1),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("route".to_string(), "/api".to_string()),
            ("instance".to_string(), "a".to_string()),
        ],
        &[(5_000, 1.5)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(2),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
            ("route".to_string(), "/api".to_string()),
            ("instance".to_string(), "b".to_string()),
        ],
        &[(5_000, 2.5)],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"sum by (route)(cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(10_000, 4.0)]);
    assert_eq!(results[0].labels.as_ref(), &[("route".to_string(), "/api".to_string())]);
}
```

- [ ] **Step 2: Run query test to verify red**

Run:

```sh
cargo test -p chronoxide-core --test promql_query promql_query_sum_aggregation_over_sealed_vectors -- --nocapture
```

Expected: compile failure or non-exhaustive match failure because `PromqlQuery::Aggregation` is not handled by query dispatch.

- [ ] **Step 3: Add aggregation match arms to dispatch**

In both `query_context.rs` and `query_store.rs`, add `PromqlQuery::Aggregation(aggregation)` match arms for:

- `prewarm_promql_query`: recurse into `aggregation.input`
- `prefetch_promql_data_query`: recurse into `aggregation.input`
- `execute_promql_query`: execute input recursively, then call `evaluate_aggregation`
- `execute_promql_query_with_head`: execute input recursively, then call `evaluate_aggregation`

The execution arm should look like:

```rust
PromqlQuery::Aggregation(aggregation) => {
    let mut execution = self.execute_promql_query(
        &aggregation.input,
        start_ms,
        end_ms,
        limits,
    )?;
    execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
    Ok(execution)
}
```

Use the corresponding `execute_promql_query_with_head` recursion in the head path.

- [ ] **Step 4: Add a temporary empty evaluator**

In `query_promql.rs`, add:

```rust
pub(super) fn evaluate_aggregation(
    _aggregation: &PromqlAggregation,
    _results: Vec<SegmentQueryResult>,
    _eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    Vec::new()
}
```

- [ ] **Step 5: Run query test to verify failure changes**

Run:

```sh
cargo test -p chronoxide-core --test promql_query promql_query_sum_aggregation_over_sealed_vectors -- --nocapture
```

Expected: test compiles and fails at assertion because the result vector is empty.

## Task 3: Aggregation Evaluator

**Files:**
- Modify: `chronoxide-core/src/storage/segment/query_promql.rs`
- Test: `chronoxide-core/tests/promql_query.rs`

- [ ] **Step 1: Add aggregation edge tests**

Add tests:

```rust
#[test]
fn promql_query_count_and_avg_skip_stale_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    write_series(
        &mut writer,
        SeriesRef::new(10),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, 2.0)],
    );
    write_series(
        &mut writer,
        SeriesRef::new(11),
        vec![(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
        &[(5_000, prometheus_stale_nan())],
    );
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let count = store.query_promql(r#"count(cpu.usage)"#, 0, 10_000).unwrap();
    let avg = store.query_promql(r#"avg(cpu.usage)"#, 0, 10_000).unwrap();

    assert_eq!(count.len(), 1);
    assert_eq!(count[0].samples, vec![(10_000, 1.0)]);
    assert_eq!(avg.len(), 1);
    assert_eq!(avg[0].samples, vec![(10_000, 2.0)]);
}

#[test]
fn promql_query_sum_without_drops_named_labels() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (idx, instance, value) in [(1, "a", 1.0), (2, "b", 2.0)] {
        write_series(
            &mut writer,
            SeriesRef::new(idx),
            vec![
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("route".to_string(), "/api".to_string()),
                ("instance".to_string(), instance.to_string()),
            ],
            &[(5_000, value)],
        );
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(r#"sum without (instance)(cpu.usage)"#, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].labels.as_ref(), &[("route".to_string(), "/api".to_string())]);
    assert_eq!(results[0].samples, vec![(10_000, 3.0)]);
}
```

- [ ] **Step 2: Run aggregation tests to verify red**

Run:

```sh
cargo test -p chronoxide-core --test promql_query -- --nocapture
```

Expected: tests fail because aggregation returns no results.

- [ ] **Step 3: Implement aggregation groups**

Replace the temporary evaluator in `query_promql.rs` with:

```rust
pub(super) fn evaluate_aggregation(
    aggregation: &PromqlAggregation,
    results: Vec<SegmentQueryResult>,
    eval_time_ms: u64,
) -> Vec<SegmentQueryResult> {
    let mut groups = BTreeMap::<Vec<(String, String)>, AggregationAccumulator>::new();
    for result in results {
        let Some((_, value)) = result.samples.iter().rev().copied().find(|(_, value)| value.is_finite()) else {
            continue;
        };
        let labels = aggregation_group_labels(&aggregation.grouping, result.labels.as_ref());
        groups.entry(labels).or_default().observe(value);
    }

    let mut out = Vec::new();
    for (labels, accumulator) in groups {
        let Some(value) = accumulator.value(aggregation.op) else {
            continue;
        };
        let mut result = SegmentQueryResult::new(segment_series_id(&labels), labels);
        result.push_sample(eval_time_ms, value);
        out.push(result);
    }
    merge_query_results(out)
}
```

Add helpers:

```rust
#[derive(Default)]
struct AggregationAccumulator {
    sum: f64,
    count: u64,
}

impl AggregationAccumulator {
    fn observe(&mut self, value: f64) {
        self.sum += value;
        self.count = self.count.saturating_add(1);
    }

    fn value(&self, op: PromqlAggregationOp) -> Option<f64> {
        match op {
            PromqlAggregationOp::Sum => (self.count > 0).then_some(self.sum),
            PromqlAggregationOp::Count => (self.count > 0).then_some(self.count as f64),
            PromqlAggregationOp::Avg => (self.count > 0).then_some(self.sum / self.count as f64),
        }
    }
}
```

`aggregation_group_labels` must:

- exclude `__name__` always;
- for `All`, return `Vec::new()`;
- for `By`, keep labels whose keys are in the grouping list;
- for `Without`, keep labels whose keys are not in the grouping list;
- sort labels by key/value before returning.

- [ ] **Step 4: Run aggregation tests to verify green**

Run:

```sh
cargo test -p chronoxide-core --test promql_query -- --nocapture
```

Expected: tests pass.

- [ ] **Step 5: Commit aggregation evaluator**

Run:

```sh
cargo fmt
git diff --check
git add chronoxide-core/src/storage/segment/query_context.rs chronoxide-core/src/storage/segment/query_store.rs chronoxide-core/src/storage/segment/query_promql.rs chronoxide-core/tests/promql_query.rs
git commit -m "feat: evaluate promql aggregations"
```

## Task 4: Prometheus-Style Counter Extrapolation

**Files:**
- Modify: `chronoxide-core/src/storage/segment/query_promql.rs`
- Test: `chronoxide-core/tests/promql_query.rs`

- [ ] **Step 1: Write failing counter extrapolation test**

Add:

```rust
#[test]
fn promql_query_rate_extrapolates_counter_to_requested_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let series = SeriesRef::new(91);
    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "http.requests.total".to_string()),
        ("route".to_string(), "/extrapolate".to_string()),
    ];
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_with_labels(series, &raw_labels, &[(1_000, 1.0), (4_000, 4.0)])
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"rate(http.requests.total{route="/extrapolate"}[5s])"#,
            0,
            5_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 5_000);
    assert!((results[0].samples[0].1 - 0.6).abs() < 1e-9);
}
```

This fails under the old implementation because it returns `1.0` per second from observed elapsed time instead of extrapolating to the 5-second range.

- [ ] **Step 2: Run counter test to verify red**

Run:

```sh
cargo test -p chronoxide-core --test promql_query promql_query_rate_extrapolates_counter_to_requested_range -- --nocapture
```

Expected: test fails with actual value near `1.0`.

- [ ] **Step 3: Implement extrapolated counter range**

Change `evaluate_range_function` to pass the range start and range length:

```rust
let range_start_ms = range_function_start_ms(eval_time_ms, function.range_ms);
let Some(increase) = extrapolated_counter_increase(
    &result.samples,
    result.counter_reset_hints(),
    range_start_ms,
    eval_time_ms,
) else {
    continue;
};
```

Add `extrapolated_counter_increase` in `query_promql.rs`:

```rust
pub(super) fn extrapolated_counter_increase(
    samples: &[(u64, f64)],
    counter_reset_hints: Option<&[CounterResetHint]>,
    range_start_ms: u64,
    range_end_ms: u64,
) -> Option<f64> {
    if samples.len() < 2 || range_end_ms <= range_start_ms {
        return None;
    }
    let raw_increase = counter_increase(samples, counter_reset_hints)?;
    let (first_ts, first_value) = samples.first().copied()?;
    let (last_ts, _) = samples.last().copied()?;
    if last_ts <= first_ts || !first_value.is_finite() {
        return None;
    }

    let sampled_interval = (last_ts - first_ts) as f64 / 1_000.0;
    let range_seconds = (range_end_ms - range_start_ms) as f64 / 1_000.0;
    let mut duration_to_start = first_ts.saturating_sub(range_start_ms) as f64 / 1_000.0;
    let mut duration_to_end = range_end_ms.saturating_sub(last_ts) as f64 / 1_000.0;
    let average_between_samples = sampled_interval / (samples.len() - 1) as f64;
    let extrapolation_threshold = average_between_samples * 1.1;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_between_samples / 2.0;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_between_samples / 2.0;
    }

    if raw_increase > 0.0 && first_value >= 0.0 {
        let duration_to_zero = sampled_interval * (first_value / raw_increase);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let extrapolated_interval = sampled_interval + duration_to_start + duration_to_end;
    Some(raw_increase * (extrapolated_interval / sampled_interval).min(range_seconds / sampled_interval))
}
```

Use the extrapolated increase directly for `increase()` and divide by `function.range_ms` seconds for `rate()`.

- [ ] **Step 4: Run counter reset regression tests**

Run:

```sh
cargo test -p chronoxide-core --test promql_query -- --nocapture
```

Expected: all listed tests pass. If old expected values conflict with correct extrapolated semantics, update only after confirming the new value follows the documented extrapolation rule.

- [ ] **Step 5: Commit counter extrapolation**

Run:

```sh
cargo fmt
git diff --check
git add chronoxide-core/src/storage/segment/query_promql.rs chronoxide-core/tests/promql_query.rs
git commit -m "fix: extrapolate promql counter ranges"
```

## Task 5: Histogram Quantile Over Aggregated Bucket Rates

**Files:**
- Modify: `chronoxide-core/tests/promql_query.rs`
- Modify if needed: `chronoxide-core/src/storage/segment/query_promql.rs`

- [ ] **Step 1: Write failing integration test**

Add:

```rust
#[test]
fn promql_query_histogram_quantile_over_sum_by_bucket_rate() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    for (series_ref, instance) in [(SeriesRef::new(201), "a"), (SeriesRef::new(202), "b")] {
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
                            metadata: TypedSampleMetadata::default(),
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
                            metadata: TypedSampleMetadata::default(),
                            explicit_bounds: vec![1.0, 2.0, 4.0],
                            bucket_counts: vec![4, 10, 6, 0],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "http.request.duration");
                    visit("route", "/quantile-agg");
                    visit("instance", instance);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = store
        .query_promql(
            r#"histogram_quantile(0.5, sum by (le, route)(rate(http.request.duration_bucket{route="/quantile-agg"}[5s])))"#,
            0,
            6_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].0, 6_000);
    assert!((results[0].samples[0].1 - 1.6).abs() < 1e-9);
    assert_eq!(results[0].labels.as_ref(), &[("route".to_string(), "/quantile-agg".to_string())]);
}
```

- [ ] **Step 2: Run integration test to verify red or green**

Run:

```sh
cargo test -p chronoxide-core --test promql_query promql_query_histogram_quantile_over_sum_by_bucket_rate -- --nocapture
```

Expected before Tasks 1-4: red. Expected after Tasks 1-4: green. If it is red after Tasks 1-4, inspect whether aggregation labels kept `le` and whether quantile input sees the bucket-shaped labels.

- [ ] **Step 3: Add crossed-segment rate aggregation test**

Add a test that writes the same counter series into two segment windows and queries `sum by (route)(rate(metric[15s]))` across both windows. The expected value must prove merged samples from both segments feed one range function before aggregation.

- [ ] **Step 4: Run focused histogram and crossed-segment tests**

Run:

```sh
cargo test -p chronoxide-core --test promql_query -- --nocapture
```

Expected: both tests pass.

- [ ] **Step 5: Commit histogram integration**

Run:

```sh
cargo fmt
git diff --check
git add chronoxide-core/tests/promql_query.rs chronoxide-core/src/storage/segment/query_promql.rs
git commit -m "test: cover aggregated histogram quantile queries"
```

## Task 6: Full Verification And Perf

**Files:**
- Modify if needed: `chronoxide-core/src/promql/mod.rs`
- Modify if needed: `chronoxide-core/src/storage/segment/query_promql.rs`
- Modify if needed: `chronoxide-core/src/storage/segment/query_context.rs`
- Modify if needed: `chronoxide-core/src/storage/segment/query_store.rs`
- Modify if needed: `chronoxide-core/tests/promql_selector.rs`
- Modify if needed: `chronoxide-core/tests/promql_query.rs`

- [ ] **Step 1: Run focused parser and query tests**

Run:

```sh
cargo test -p chronoxide-core --test promql_selector -- --nocapture
cargo test -p chronoxide-core --test promql_query -- --nocapture
```

Expected: both commands pass.

- [ ] **Step 2: Run full core and ingester query tests**

Run:

```sh
cargo test -p chronoxide-core
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
```

Expected: both commands pass.

- [ ] **Step 3: Run formatting and diff checks**

Run:

```sh
cargo fmt --check
git diff --check
```

Expected: both commands pass.

- [ ] **Step 4: Run readback verifier on replay data**

Run:

```sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query && \
  ./target/release/chronoxide-query \
    --segments-dir data/smoke/segments-replay-001 \
    --sample-limit-per-kind 2 \
    --verify-readbacks
```

Expected: verifier completes with zero mismatches.

- [ ] **Step 5: Run exact selector read benchmark**

Run:

```sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query && \
  ./target/release/chronoxide-query \
    --segments-dir data/smoke/segments-replay-001 \
    --benchmark-repeats 3 \
    --query '{__name__="go_gc_duration_seconds_count"}' \
    --query '{__name__="definitely_missing_metric"}'
```

Expected: exact selector warm and cold timings stay in the same rough band as the latest baseline because plain vector queries still use the same storage path.

- [ ] **Step 6: Commit any verification fixes**

If Step 1-5 required fixes, commit them with a focused message:

```sh
git add chronoxide-core/src/promql/mod.rs \
  chronoxide-core/src/storage/segment/query_promql.rs \
  chronoxide-core/src/storage/segment/query_context.rs \
  chronoxide-core/src/storage/segment/query_store.rs \
  chronoxide-core/tests/promql_selector.rs \
  chronoxide-core/tests/promql_query.rs
git commit -m "fix: harden promql expression evaluation"
```

If no fixes were needed, do not create an empty commit.
