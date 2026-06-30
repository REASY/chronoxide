# Head OOO Query Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make live PromQL-style head queries see accepted out-of-order samples before they are flushed to overlapping segments.

**Architecture:** `HeadBuffer` keeps active in-order samples in `window` and accepted late samples in `ooo_windows`. Query and metadata paths will iterate both lanes, reuse the existing selector index per `HeadWindow`, then merge matching results by PromQL `series_id` with samples sorted by timestamp. Deterministic same-timestamp precedence remains the next milestone item.

**Tech Stack:** Rust 2024, `HeadBuffer`, `SegmentStoreReader::query_selector_with_head`, existing selector/index metadata code, cargo test.

---

### Task 1: HeadBuffer Query Sees OOO Windows

**Files:**
- Modify: `chronoxide-core/src/storage/head.rs`

- [x] Write a failing unit test proving `HeadBuffer::query_selector` returns active and OOO samples for the same series before flush.
- [x] Write a failing unit test proving head metadata discovery includes labels that only have samples in OOO windows.
- [x] Run `cargo test -p chronoxide-core storage::head::tests::head_query_merges_active_and_ooo_windows_before_flush --quiet` and verify it fails before implementation.
- [x] Run `cargo test -p chronoxide-core storage::head::tests::head_metadata_includes_ooo_only_series_before_flush --quiet` and verify it fails before implementation.
- [x] Implement window iteration, per-window selector query, per-window metadata collection, and head result merge.
- [x] Run both focused tests and verify they pass.

### Task 2: Store Query With Head Sees OOO Windows

**Files:**
- Modify: `chronoxide-core/tests/head_query.rs`

- [x] Write an integration test proving `SegmentStoreReader::query_selector_with_head` returns an accepted late head sample before flush.
- [x] Run `cargo test -p chronoxide-core --test head_query store_query_selector_with_head_merges_ooo_head_samples_before_flush --quiet` and verify it passes with Task 1.
- [x] Keep the integration test focused on query behavior; persistence was covered by the previous overlapping segment slice.

### Task 3: Docs, Regression, Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-head-ooo-query-merge.md`

- [x] Mark `Query merge of in-order and OOO lanes` complete.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo test -p chronoxide-core --quiet`.
- [x] Run `cargo test -p chronoxide-core --test head_query --quiet`.
- [x] Run `git diff --check`.
- [x] Commit as `feat: query out-of-order head windows`.
