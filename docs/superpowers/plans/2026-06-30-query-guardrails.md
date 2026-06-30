# Query Guardrails Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded PromQL selector execution for matched series, chunk reads, bytes read, decoded samples, and regex value expansion.

**Architecture:** Keep existing selector and PromQL APIs unlimited for compatibility. Add explicit `*_with_limits` APIs that thread a single query budget through sealed segments and active head, returning both query results and execution stats. Convert storage quota errors into a typed PromQL limit error.

**Tech Stack:** Rust, existing segment/head query path, `std::io::ErrorKind::QuotaExceeded`, existing integration tests.

---

### Task 1: Guardrail API and Budget

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`
- Modify: `chronoxide-core/src/promql/mod.rs`
- Test: `chronoxide-core/src/storage/segment.rs`

- [x] **Step 1: Write failing unit tests**

Add budget tests that require `QueryLimits`, unique matched-series accounting, and quota errors for chunk reads, bytes, samples, and regex values.

- [x] **Step 2: Run unit tests to verify red**

Run: `cargo test -p chronoxide-core storage::segment::tests::query_budget`

Expected: compile failure because the budget types do not exist.

- [x] **Step 3: Implement minimal budget**

Add `QueryLimits`, `QueryStats`, `QueryExecution`, `QueryLimit`, `QueryLimitExceeded`, and `QueryBudget`. Enforce limits before counters advance beyond the configured maximum.

- [x] **Step 4: Run unit tests to verify green**

Run: `cargo test -p chronoxide-core storage::segment::tests::query_budget`

Expected: both budget unit tests pass.

### Task 2: PromQL Integration Guardrails

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`
- Modify: `chronoxide-core/src/storage/head.rs`
- Test: `chronoxide-core/tests/promql_query.rs`

- [x] **Step 1: Write failing integration tests**

Add tests for successful stats, `max_matched_series`, `max_chunk_reads`, `max_bytes_read`, `max_samples_decoded`, `max_regex_values_examined`, and active-head sample limits.

- [x] **Step 2: Run integration tests to verify red**

Run: `cargo test -p chronoxide-core --test promql_query`

Expected: compile failure because `query_promql_with_limits`, `query_promql_with_head_with_limits`, and `PromqlQueryError::LimitExceeded` do not exist.

- [x] **Step 3: Wire budget through query execution**

Thread `QueryBudget` through segment store, segment reader, regex postings expansion, chunk reads, decoded samples, and head query matching.

- [x] **Step 4: Run integration tests to verify green**

Run: `cargo test -p chronoxide-core --test promql_query`

Expected: all PromQL query integration tests pass.

### Task 3: Regression Verification

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`

- [x] **Step 1: Format**

Run: `cargo fmt -- --check`

Expected: success.

- [x] **Step 2: Run core storage and selector regressions**

Run: `cargo test -p chronoxide-core storage`

Run: `cargo test -p chronoxide-core --test promql_selector --test promql_query --test head_query --test segment_query --test promql_normalization --test series_bin --test postings_index --test segment_publish`

Expected: success.

- [x] **Step 3: Run ingester regression**

Run: `cargo test -p chronoxide-ingester processor_writes`

Expected: success.

- [x] **Step 4: Commit**

Stage only intended files and commit with `feat: add PromQL query guardrails`.
