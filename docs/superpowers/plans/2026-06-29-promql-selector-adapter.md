# PromQL Selector Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let callers query the current storage path with a PromQL vector selector string instead of manually constructing `SegmentSelector`.

**Architecture:** Add a small parser in `chronoxide-core::promql` for the PromQL subset storage can execute today: metric selectors, brace selectors, and `=` / `!=` label matchers. Keep regex, functions, aggregations, and binary expressions explicit unsupported errors. Add storage adapter methods on `SegmentStoreReader` that parse the query string, convert it to the existing `SegmentSelector`, and route to sealed-only or sealed-plus-head query paths.

**Tech Stack:** Rust, existing `chronoxide-core::promql` module, existing `SegmentSelector` / `LabelMatcher`, existing `SegmentStoreReader` and `HeadBuffer`.

---

### Task 1: Parser Tests

**Files:**
- Modify: `chronoxide-core/src/promql/mod.rs`
- Test: `chronoxide-core/tests/promql_selector.rs`

- [x] **Step 1: Write failing parser tests**

Create tests for:
- metric shorthand: `cpu_usage`
- metric plus labels: `cpu_usage{pod="backend-1",namespace!="kube-system"}`
- brace-only metric: `{__name__="cpu_usage",pod!="backend-2"}`
- escaped strings: `route="\\/api\\n"`
- unsupported regex: `pod=~"backend-.*"`
- unsupported function: `rate(cpu_usage[5m])`
- invalid syntax: `cpu_usage{pod=}`

- [x] **Step 2: Run parser tests to verify red**

Run: `cargo test -p chronoxide-core --test promql_selector`

Expected: compile failure because the parser types/functions do not exist.

- [x] **Step 3: Implement minimal selector parser**

Add:
- `PromqlSelector`
- `PromqlMatcher`
- `PromqlMatcherOp`
- `PromqlQueryError`
- `parse_vector_selector`

Parser scope:
- trim surrounding whitespace,
- accept PromQL identifiers for metric and label names,
- parse double-quoted label values with common escapes,
- reject regex operators with `Unsupported`,
- reject obvious expression syntax with `Unsupported`,
- reject malformed selectors with `Invalid`.

- [x] **Step 4: Run parser tests to verify green**

Run: `cargo test -p chronoxide-core --test promql_selector`

Expected: all parser tests pass.

### Task 2: Storage Adapter Tests

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`
- Test: `chronoxide-core/tests/promql_query.rs`

- [x] **Step 1: Write failing adapter tests**

Create tests that:
- write sealed segment samples and active head samples,
- call `query_promql_with_head("cpu.usage{pod.name=\"backend-1\"}", ...)`,
- verify sealed and head samples are merged,
- call brace-only `{__name__="cpu.usage",pod.name!="backend-2"}`,
- verify unsupported regex returns a PromQL error without scanning.

- [x] **Step 2: Run adapter tests to verify red**

Run: `cargo test -p chronoxide-core --test promql_query`

Expected: compile failure because the adapter methods do not exist.

- [x] **Step 3: Implement storage adapter**

Add:
- conversion from `PromqlSelector` to `SegmentSelector`,
- `SegmentStoreReader::query_promql`,
- `SegmentStoreReader::query_promql_with_head`.

Only `=` and `!=` matchers are converted. Regex matchers return `PromqlQueryError::Unsupported`.

- [x] **Step 4: Run adapter tests to verify green**

Run: `cargo test -p chronoxide-core --test promql_query`

Expected: all adapter tests pass.

### Task 3: Verification and Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`

- [x] **Step 1: Update milestone status**

Record that a PromQL vector-selector adapter exists for metric selectors plus `=` / `!=`, with regex and expression evaluation still open.

- [x] **Step 2: Run focused verification**

Run:
- `cargo fmt -- --check`
- `cargo test -p chronoxide-core storage`
- `cargo test -p chronoxide-core --test promql_selector --test promql_query --test head_query --test segment_query --test promql_normalization --test series_bin --test postings_index --test segment_publish`
- `cargo test -p chronoxide-ingester processor_writes`

Expected: all commands exit successfully.

- [x] **Step 3: Commit scoped changes**

Stage only the parser, adapter, tests, and roadmap/plan docs.

Commit message: `feat: add PromQL selector query adapter`
