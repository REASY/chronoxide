# PromQL Read Benchmark Range Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `chronoxide-query` with real-segment PromQL range benchmarks and prominent payload read-versus-used byte reporting.

**Architecture:** Keep the existing repeatable query runner and add one global evaluation mode: instant by default or range when `--step-ms` is present. Reuse the core session range API and the existing result/profile pipeline. Derive byte efficiency exclusively from measured session profile deltas.

**Tech Stack:** Rust, Clap, Chronoxide segment query sessions, Markdown reports, built-in Rust tests.

---

### Task 1: Benchmark mode, CLI normalization, and validation

**Files:**
- Modify: `chronoxide-ingester/src/bin/chronoxide-query.rs`
- Test: `chronoxide-ingester/src/bin/chronoxide_query/tests.rs`

- [ ] **Step 1: Write failing CLI and validation tests**

Add tests that parse omitted and explicit bounds, normalize instant defaults,
select range mode, and reject invalid range configurations before opening a
store. The assertions must cover these concrete cases:

```rust
let instant = Args::parse_from(["chronoxide-query", "--query", "cpu.usage"]);
assert_eq!(
    benchmark_request_from_args(&instant).unwrap(),
    (0, u64::MAX, QueryBenchmarkMode::Instant),
);

let range = Args::parse_from([
    "chronoxide-query", "--query", "time()", "--start-ms", "1000",
    "--end-ms", "5000", "--step-ms", "2000",
]);
assert_eq!(
    benchmark_request_from_args(&range).unwrap(),
    (1000, 5000, QueryBenchmarkMode::Range { step_ms: 2000 }),
);
```

Also assert errors for missing start, missing end, zero step, end before start,
more than 1,000,000 scheduled evaluations, range prewarm, and range prefetch.

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```sh
cargo test -p chronoxide-ingester --bin chronoxide-query benchmark_request -- --nocapture
```

Expected: compilation fails because the mode, step argument, and normalization
helper do not exist.

- [ ] **Step 3: Add the mode and normalization helper**

Change the CLI bounds to optional values so explicit range bounds can be
distinguished from instant defaults, add `step_ms`, and define the mode:

```rust
#[arg(long)]
start_ms: Option<u64>,
#[arg(long)]
end_ms: Option<u64>,
#[arg(long)]
step_ms: Option<u64>,

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryBenchmarkMode {
    Instant,
    Range { step_ms: u64 },
}
```

Implement `benchmark_request_from_args` so instant mode normalizes to
`0..u64::MAX`, while range mode requires explicit bounds and validates nonzero
step and ordered bounds. Add `mode` to `QueryBenchmarkConfig`; normalize smoke
bounds independently in `main`. Extend runner validation to reject range mode
combined with prewarm or prefetch. Update existing test configurations with
`QueryBenchmarkMode::Instant`.

- [ ] **Step 4: Run the focused CLI and validation tests**

Run:

```sh
cargo test -p chronoxide-ingester --bin chronoxide-query benchmark_request -- --nocapture
cargo test -p chronoxide-ingester --bin chronoxide-query rejects_range -- --nocapture
```

Expected: all matching tests pass.

### Task 2: Range execution and report metadata

**Files:**
- Modify: `chronoxide-ingester/src/bin/chronoxide-query.rs`
- Test: `chronoxide-ingester/src/bin/chronoxide_query/tests.rs`

- [ ] **Step 1: Write a failing inclusive-range execution test**

Create a benchmark configuration over a test segment store with
`start_ms=1000`, `end_ms=5000`, `Range { step_ms: 2000 }`, one repeat, and the
query `time() + 1`. Assert one result series, three result samples, and report
lines for range mode, a 2000 ms step, and three scheduled evaluations.

- [ ] **Step 2: Run the range execution test and verify it fails**

Run:

```sh
cargo test -p chronoxide-ingester --bin chronoxide-query run_query_benchmark_executes_range -- --nocapture
```

Expected: the assertion fails because the runner still calls the instant API.

- [ ] **Step 3: Dispatch through the existing session range API**

Branch only the measured execution call:

```rust
let execution = match config.mode {
    QueryBenchmarkMode::Instant => query_session.query_promql_with_limits(
        query, config.start_ms, query_end_ms, config.limits,
    ),
    QueryBenchmarkMode::Range { step_ms } =>
        query_session.query_promql_range_with_limits(
            query, config.start_ms, config.end_ms, step_ms, config.limits,
        ),
};
```

Only instant mode may resolve an omitted end from the newest segment window.
Render `Evaluation Mode`, the range step, scheduled evaluations, and a note
that cold refers to a fresh Chronoxide query session rather than a cold OS
cache.

- [ ] **Step 4: Run the range execution test**

Run:

```sh
cargo test -p chronoxide-ingester --bin chronoxide-query run_query_benchmark_executes_range -- --nocapture
```

Expected: the test passes with three inclusive evaluation points.

### Task 3: Payload read-versus-used efficiency

**Files:**
- Modify: `chronoxide-ingester/src/bin/chronoxide-query.rs`
- Test: `chronoxide-ingester/src/bin/chronoxide_query/tests.rs`

- [ ] **Step 1: Write failing byte-efficiency tests**

Assert the formatter returns an em dash for no selected payload and a stable
three-decimal ratio otherwise:

```rust
assert_eq!(format_payload_read_amplification(0, 0), "—");
assert_eq!(format_payload_read_amplification(150, 100), "1.500x");
```

Extend an existing data-reading benchmark test to assert positive payload used
and read bytes, `read >= used`, and Markdown columns/total rows named
`payload_used_bytes`, `payload_read_bytes`, and `payload_read_over_used`.

- [ ] **Step 2: Run the byte tests and verify they fail**

Run:

```sh
cargo test -p chronoxide-ingester --bin chronoxide-query payload_read -- --nocapture
```

Expected: compilation or assertions fail because the formatter and report
fields do not exist.

- [ ] **Step 3: Aggregate and render measured byte efficiency**

Extend `QueryBenchmarkTotals` with `payload_used_bytes` and
`payload_read_bytes`. Sum `session_profile_delta.chunk_payload_bytes` and
`session_profile_delta.chunk_payload_physical_bytes` for measured results only.
Implement:

```rust
fn format_payload_read_amplification(read_bytes: u64, used_bytes: u64) -> String {
    if used_bytes == 0 {
        return "—".to_string();
    }
    format!("{:.3}x", read_bytes as f64 / used_bytes as f64)
}
```

Add payload-used, payload-read, and ratio rows to query totals and the three
corresponding columns to each measured query result. Explain in the Markdown
that read bytes are coalesced process-issued spans and do not measure physical
storage-device traffic.

- [ ] **Step 4: Run byte-efficiency and full binary tests**

Run:

```sh
cargo test -p chronoxide-ingester --bin chronoxide-query payload_read -- --nocapture
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
```

Expected: all tests pass.

### Task 4: Review and real-replay verification

**Files:**
- Verify: `chronoxide-ingester/src/bin/chronoxide-query.rs`
- Verify: `chronoxide-ingester/src/bin/chronoxide_query/tests.rs`
- Create runtime report: `data/perf/segment-index-v7/full-replay-no-record-index/reports/promql-instant-suite.md`
- Create runtime report: `data/perf/segment-index-v7/full-replay-no-record-index/reports/promql-range-suite.md`

- [ ] **Step 1: Format and inspect the focused diff**

Run:

```sh
cargo fmt --all -- --check
git diff --check
git diff -- chronoxide-ingester/src/bin/chronoxide-query.rs chronoxide-ingester/src/bin/chronoxide_query/tests.rs
```

Expected: formatting and whitespace checks pass; the diff is confined to the
benchmark binary and tests apart from the approved spec and plan.

- [ ] **Step 2: Build the release query binary**

Run:

```sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query
```

Expected: release build succeeds.

- [ ] **Step 3: Run the validated instant suite**

Run:

```sh
./target/release/chronoxide-query \
  --segments-dir data/perf/segment-index-v7/segments-replay-v7-no-record-index \
  --output data/perf/segment-index-v7/full-replay-no-record-index/reports/promql-instant-suite.md \
  --benchmark-repeats 3 \
  --query '{__name__="go_gc_duration_seconds_count"}' \
  --query 'go_gc_duration_seconds_count{service_name_x55e50a58f9befba7="gitlab-runner"}' \
  --query '{__name__="definitely_missing_metric"}' \
  --query 'http_client_duration_xf5f33b0f6bbd8257_count{http_method_xd03b88c71267d28a=~"GET|POST"}' \
  --query 'sum by (service_name_x55e50a58f9befba7)(go_gc_duration_seconds_count)' \
  --query 'rate(go_gc_duration_seconds_count[15m])' \
  --query 'increase(go_gc_duration_seconds_count[15m])' \
  --query 'rate(go_gc_duration_seconds_sum[15m]) / rate(go_gc_duration_seconds_count[15m])' \
  --query 'rate(go_gc_duration_seconds_sum[15m]) / on(service_name_x55e50a58f9befba7) group_left sum by (service_name_x55e50a58f9befba7)(rate(go_gc_duration_seconds_count[15m]))' \
  --query 'go_gc_duration_seconds{quantile="0"}' \
  --query 'histogram_quantile(0.95, sum by (le,service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257_bucket[15m])))' \
  --query 'histogram_quantile(0.95, sum by (service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257[15m])))' \
  --query 'histogram_quantile(0.95, sum by (service_name_x55e50a58f9befba7)(rate(ag_consul_request_x0f4a28dca7d2d184[15m])))'
```

Expected: 39 successful runs are written to `promql-instant-suite.md`.

- [ ] **Step 4: Run the final-hour range suite**

Run:

```sh
./target/release/chronoxide-query \
  --segments-dir data/perf/segment-index-v7/segments-replay-v7-no-record-index \
  --output data/perf/segment-index-v7/full-replay-no-record-index/reports/promql-range-suite.md \
  --start-ms 1782982800000 \
  --end-ms 1782986400000 \
  --step-ms 60000 \
  --benchmark-repeats 3 \
  --query 'rate(go_gc_duration_seconds_count[15m])' \
  --query 'sum by (service_name_x55e50a58f9befba7)(rate(go_gc_duration_seconds_count[15m]))' \
  --query 'histogram_quantile(0.95, sum by (service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257[15m])))'
```

Expected: nine successful runs, 61 scheduled evaluations per run, and no
query-limit failure.

- [ ] **Step 5: Compare timings and byte amplification**

Read both reports and summarize cold/warm duration, result samples, selected
payload bytes, issued payload bytes, and read/used amplification for every
workload. Flag the largest duration and amplification rather than inferring a
bottleneck from duration alone.

- [ ] **Step 6: Commit the focused implementation**

Stage only the query binary, its tests, and this plan. Do not stage runtime
reports or unrelated user changes.

```sh
git add chronoxide-ingester/src/bin/chronoxide-query.rs \
  chronoxide-ingester/src/bin/chronoxide_query/tests.rs \
  docs/superpowers/plans/2026-07-10-promql-read-benchmark-range.md
git commit -m "perf(query): benchmark PromQL range reads"
```
