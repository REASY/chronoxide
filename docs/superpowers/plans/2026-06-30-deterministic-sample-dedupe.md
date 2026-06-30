# Deterministic Sample Dedupe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deduplicate duplicate-timestamp PromQL samples with a deterministic last-write-wins precedence across active head, OOO head, and store+head merge paths.

**Architecture:** Preserve the existing source ordering as write precedence: persisted segment results are lower precedence than head results, and within head the active window is lower precedence than an OOO window for the same time range because the OOO sample was accepted later. Merge helpers will collapse samples by timestamp using the last sample seen for that timestamp and return sorted timestamps.

**Tech Stack:** Rust 2024, `HeadBuffer`, `SegmentStoreReader`, existing `SegmentQueryResult`, cargo test.

---

### Task 1: Head Query Dedupe

**Files:**
- Modify: `chronoxide-core/src/storage/head.rs`

- [x] Write failing unit tests proving active-window duplicates keep the later sample and a late OOO duplicate overrides the earlier active-window sample.
- [x] Run `cargo test -p chronoxide-core storage::head::tests::head_query_dedupes_duplicate_timestamps_with_active_last_write --quiet` and verify it fails before implementation.
- [x] Run `cargo test -p chronoxide-core storage::head::tests::head_query_dedupes_duplicate_timestamps_with_ooo_last_write --quiet` and verify it fails before implementation.
- [x] Update head query window ordering and head merge dedupe to keep the last sample by explicit source order.
- [x] Run both focused head tests and verify they pass.

### Task 2: Store Plus Head Dedupe

**Files:**
- Modify: `chronoxide-core/tests/head_query.rs`
- Modify: `chronoxide-core/src/storage/segment.rs`

- [x] Write an integration test proving a late OOO head duplicate wins over both a sealed segment sample and an earlier active head sample at the same timestamp.
- [x] Run `cargo test -p chronoxide-core --test head_query store_query_selector_with_head_prefers_late_ooo_head_duplicate --quiet` and verify it passes with the head precedence implementation.
- [x] Make store-level sample dedupe explicit by timestamp while preserving source precedence.
- [x] Run the focused integration test and verify it passes.

### Task 3: Docs, Regression, Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-deterministic-sample-dedupe.md`

- [x] Mark `Last-write-wins dedupe using deterministic precedence` complete.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo test -p chronoxide-core --quiet`.
- [x] Run `cargo test -p chronoxide-core --test head_query --quiet`.
- [x] Run `git diff --check`.
- [x] Commit as `feat: dedupe samples with deterministic precedence`.
