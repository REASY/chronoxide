# Regex Matchers Value Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support PromQL `=~` and `!~` matchers over sealed segments and the active head using a segment-local value enumeration index.

**Architecture:** Extend the existing selector model with regex matcher variants and compile regexes at query planning time. For sealed segments, build a lightweight in-memory label-value index from `series.bin` label symbol pairs, enumerate matching value symbols for each regex matcher, then union/subtract exact postings for those values. For head queries, evaluate regex matchers against canonical labels directly. Keep the value index API isolated so a persisted FST can replace the in-memory builder later.

**Tech Stack:** Rust, `regex` crate, existing PromQL parser, existing `SegmentSelector` / `LabelMatcher`, existing exact postings index.

---

### Task 1: Parser Regex Tests

**Files:**
- Modify: `chronoxide-core/src/promql/mod.rs`
- Test: `chronoxide-core/tests/promql_selector.rs`

- [x] **Step 1: Write failing parser tests**

Add tests that parse:
- `cpu_usage{pod=~"backend-.*"}`
- `cpu_usage{pod!~"backend-.*"}`
- `{__name__=~"http_.*"}`

Expected parsed matcher ops are `Regex` and `NotRegex`, not `Unsupported`.

- [x] **Step 2: Run parser tests to verify red**

Run: `cargo test -p chronoxide-core --test promql_selector`

Expected: failures because regex operators currently return `Unsupported`.

- [x] **Step 3: Implement parser support**

Remove the parser-level regex unsupported check. Keep invalid syntax and expression unsupported behavior unchanged.

- [x] **Step 4: Run parser tests to verify green**

Run: `cargo test -p chronoxide-core --test promql_selector`

Expected: all parser tests pass.

### Task 2: Value Enumeration Index Tests

**Files:**
- Modify: `chronoxide-core/src/storage/index.rs`
- Test: `chronoxide-core/tests/postings_index.rs`

- [x] **Step 1: Write failing value-index tests**

Add tests for a `LabelValueIndex` that:
- dedupes values for a label name,
- keeps value symbols sorted,
- returns empty for missing label names.

- [x] **Step 2: Run tests to verify red**

Run: `cargo test -p chronoxide-core --test postings_index`

Expected: compile failure because `LabelValueIndex` does not exist.

- [x] **Step 3: Implement value-index type**

Add `LabelValueIndex` with:
- `insert(label_name_sym, label_value_sym)`
- `values(label_name_sym) -> &[u32]`
- `build_label_value_index(series_entries: &[SeriesEntry]) -> LabelValueIndex`

- [x] **Step 4: Run tests to verify green**

Run: `cargo test -p chronoxide-core --test postings_index`

Expected: all postings/value-index tests pass.

### Task 3: Storage Regex Tests

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`
- Modify: `chronoxide-core/src/storage/head.rs`
- Test: `chronoxide-core/tests/promql_query.rs`
- Test: `chronoxide-core/tests/head_query.rs`

- [x] **Step 1: Write failing regex query tests**

Add tests for:
- sealed segment positive regex: `pod.name=~"backend-[12]"`
- sealed segment negative regex includes missing labels: `pod.name!~"backend-1"`
- regex combined with equality matcher: `namespace="default",pod.name=~"backend-.*"`
- invalid regex returns `PromqlQueryError::Invalid`
- active head regex through `query_promql_with_head`

- [x] **Step 2: Run tests to verify red**

Run: `cargo test -p chronoxide-core --test promql_query --test head_query`

Expected: failures because storage conversion still rejects regex matchers.

- [x] **Step 3: Implement regex storage matching**

Add:
- `LabelMatcher::regex` and `LabelMatcher::not_regex`
- `NormalizedMatcher::Regex` and `NormalizedMatcher::NotRegex`
- regex compilation in storage query paths
- positive regex union of exact postings for enumerated matching values
- negative regex subtraction of matching postings from the base candidate set
- head regex evaluation against canonical labels

- [x] **Step 4: Run tests to verify green**

Run: `cargo test -p chronoxide-core --test promql_query --test head_query`

Expected: all regex query tests pass.

### Task 4: Verification and Docs

**Files:**
- Modify: `Cargo.toml`
- Modify: `chronoxide-core/Cargo.toml`
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`

- [x] **Step 1: Add direct regex dependency**

Declare `regex = { workspace = true }` in `chronoxide-core` and workspace dependencies.

- [x] **Step 2: Update milestone status**

Mark regex and negative regex matchers complete with the caveat that value enumeration is currently in-memory from `series.bin`; persisted FST remains open.

- [x] **Step 3: Run focused verification**

Run:
- `cargo fmt -- --check`
- `cargo test -p chronoxide-core storage`
- `cargo test -p chronoxide-core --test promql_selector --test promql_query --test head_query --test segment_query --test promql_normalization --test series_bin --test postings_index --test segment_publish`
- `cargo test -p chronoxide-ingester processor_writes`

Expected: all commands exit successfully.

- [x] **Step 4: Commit scoped changes**

Stage only regex matcher implementation, value-index tests/code, dependency/docs changes, and this plan.

Commit message: `feat: add PromQL regex matchers`
