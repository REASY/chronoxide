# Head Overlapping OOO Segments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist accepted bounded late samples without rotating the active in-order head window backward.

**Architecture:** `HeadBuffer` keeps the existing active in-order window plus a small map of OOO windows keyed by time range. Samples older than the active in-order window, or older than the per-series last accepted timestamp, are routed to the OOO window map after the existing `out_of_order_time_window` validation passes. Flush drains both lanes as independent `HeadWindow`s; the existing `SegmentWriter` ULID segment ids make overlapping immutable segments queryable after reopen.

**Tech Stack:** Rust 2024, `HeadBuffer`, `HeadWindow`, existing `SegmentWriter`, ingester processor tests, cargo test.

---

### Task 1: HeadBuffer OOO Window Lane

**Files:**
- Modify: `chronoxide-core/src/storage/head.rs`

- [x] Write a failing unit test proving a bounded late sample does not rotate the active in-order window backward and drains as a separate older window.
- [x] Run `cargo test -p chronoxide-core storage::head::tests::head_buffer_routes_late_samples_to_ooo_window_without_rotating_active --quiet` and verify it fails before implementation.
- [x] Add `ooo_windows` to `HeadBuffer`, route late samples into that map, and expose `drain_windows()` for callers that need all pending windows.
- [x] Run `cargo test -p chronoxide-core storage::head::tests::head_buffer_routes_late_samples_to_ooo_window_without_rotating_active --quiet` and verify it passes.

### Task 2: Flush OOO Windows As Overlapping Segments

**Files:**
- Modify: `chronoxide-ingester/src/processor.rs`

- [x] Write a failing processor integration test proving a late sample does not publish a segment before explicit flush, then survives flush/reopen as an overlapping segment.
- [x] Run `cargo test -p chronoxide-ingester processor_flushes_bounded_late_sample_as_overlapping_segment --quiet` and verify it fails before implementation.
- [x] Update `flush_head()` to drain all pending head windows and write each window to the segment writer.
- [x] Run `cargo test -p chronoxide-ingester processor_flushes_bounded_late_sample_as_overlapping_segment --quiet` and verify it passes.

### Task 3: Docs, Regression, Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-head-overlapping-ooo-segments.md`

- [x] Mark `OOO-only chunks or overlapping OOO segments` complete.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo test -p chronoxide-core --quiet`.
- [x] Run `cargo test -p chronoxide-ingester processor_writes --quiet`.
- [x] Run `git diff --check`.
- [x] Commit as `feat: persist out-of-order head windows`.
