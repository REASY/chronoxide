# Metadata Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add PromQL metadata discovery APIs for metric names, label names, and label values.

**Architecture:** Use the same canonical PromQL labelsets already written to `series.bin` and resolved for active head queries. Segment discovery reads `series.bin` and `chunk_index.bin` only, using chunk time overlap to avoid chunk data reads. Store-level APIs merge sealed segment metadata with optional active-head metadata through a shared sorted/deduping accumulator.

**Tech Stack:** Rust, existing segment metadata files, active `HeadBuffer`, integration tests over sealed segments and head overlay.

---

### Task 1: Discovery Tests

**Files:**
- Create: `chronoxide-core/tests/metadata_discovery.rs`
- Modify: `chronoxide-core/src/storage/segment.rs`

- [x] **Step 1: Write failing integration tests**

Add tests for `metric_names`, `label_names`, `label_values`, time-range filtering, and sealed+head overlay discovery.

- [x] **Step 2: Write failing unit test**

Add a `MetadataAccumulator` unit test for sorted dedupe and `__name__` metric tracking.

- [x] **Step 3: Run tests to verify red**

Run: `cargo test -p chronoxide-core --test metadata_discovery`

Expected: compile failure because discovery APIs do not exist.

### Task 2: Segment and Head APIs

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`
- Modify: `chronoxide-core/src/storage/head.rs`

- [x] **Step 1: Implement accumulator**

Add `MetadataAccumulator` with `add_labelset`, `metric_names`, `label_names`, and `label_values`.

- [x] **Step 2: Implement segment discovery**

Add `SegmentReader` and `SegmentStoreReader` APIs that collect metadata from `series.bin` and filter by overlapping `chunk_index.bin` entries.

- [x] **Step 3: Implement active-head discovery**

Add `HeadBuffer` APIs and store `*_with_head` APIs that collect canonical head labelsets with samples in the requested range.

- [x] **Step 4: Run focused tests**

Run: `cargo test -p chronoxide-core storage::segment::tests::metadata_accumulator`

Run: `cargo test -p chronoxide-core --test metadata_discovery`

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

Stage only intended files and commit with `feat: add metadata discovery APIs`.
