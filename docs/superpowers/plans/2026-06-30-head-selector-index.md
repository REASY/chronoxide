# Head Selector Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add cached active-head selector indexes so PromQL head queries use postings/value enumeration instead of scanning every head series.

**Architecture:** Build a normalized `HeadSelectorIndex` from the active `HeadWindow` and external label resolver on first query, cache it behind the `HeadBuffer`, and invalidate it on head mutation or drain. The index stores exact postings, per-label value lists for regex expansion, and canonical labelsets keyed by `SeriesRef`; query execution intersects positive postings, subtracts negative postings, and then decodes samples only for candidate series.

**Tech Stack:** Rust, existing `HeadBuffer`, `SegmentSelector` normalized matchers, existing `QueryBudget`, in-memory sorted postings over `SeriesRef`.

---

### Task 1: Head Selector Index Tests

**Files:**
- Modify: `chronoxide-core/src/storage/head.rs`
- Modify: `chronoxide-core/tests/head_query.rs`
- Modify: `chronoxide-core/tests/promql_query.rs`

- [x] **Step 1: Write failing unit tests**

Add unit tests for exact/negative matcher resolution, regex matcher resolution with regex value expansion accounting, and selector-index cache invalidation on head mutation.

- [x] **Step 2: Write failing integration tests**

Add head negative-regex missing-label coverage and PromQL head regex expansion guardrail coverage.

- [x] **Step 3: Run focused tests to verify red**

Run: `cargo test -p chronoxide-core storage::head::tests::head_selector_index`

Run: `cargo test -p chronoxide-core --test promql_query`

Expected: compile failure for missing `HeadSelectorIndex`/cache and a failing PromQL regex expansion limit on the old scan path.

### Task 2: Cached Index Implementation

**Files:**
- Modify: `chronoxide-core/src/storage/head.rs`

- [x] **Step 1: Add cached index field**

Add `selector_index: Mutex<Option<CachedHeadSelectorIndex>>` to `HeadBuffer`, initialize it in `new`, and clear it when recording samples or draining.

- [x] **Step 2: Build normalized postings**

Add `HeadSelectorIndex` with sorted `SeriesRef` postings, canonical labelsets, and per-label value lists.

- [x] **Step 3: Use index for selector evaluation**

Replace per-series label matcher scans in `query_selector_with_budget` with indexed candidate resolution and keep sample decode/result behavior unchanged.

- [x] **Step 4: Count head regex expansion**

Call `QueryBudget::observe_regex_value` for head regex value enumeration so head and segment paths share the same guardrail semantics.

- [x] **Step 5: Run focused tests to verify green**

Run: `cargo test -p chronoxide-core storage::head::tests::head_selector_index`

Run: `cargo test -p chronoxide-core storage::head::tests::head_query_populates_and_invalidates_selector_index_cache`

Run: `cargo test -p chronoxide-core --test head_query`

Run: `cargo test -p chronoxide-core --test promql_query`

Expected: success.

### Task 3: Regression Verification

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`

- [x] **Step 1: Format**

Run: `cargo fmt -- --check`

Expected: success.

- [x] **Step 2: Run core storage and integration regressions**

Run: `cargo test -p chronoxide-core storage`

Run: `cargo test -p chronoxide-core --test metadata_discovery --test promql_selector --test promql_query --test head_query --test segment_query --test promql_normalization --test series_bin --test postings_index --test segment_publish`

Expected: success.

- [x] **Step 3: Run ingester regression**

Run: `cargo test -p chronoxide-ingester processor_writes`

Expected: success.

- [x] **Step 4: Commit**

Stage only intended files and commit with `feat: cache head selector indexes`.
