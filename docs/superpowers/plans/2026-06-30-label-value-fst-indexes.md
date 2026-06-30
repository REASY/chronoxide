# Label Value FST Indexes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist per-label value FSTs in `indexes.puffin` and use them for regex value enumeration.

**Architecture:** Keep the existing exact postings format as a section inside a new segment-index container. Add a second section containing one FST set per label-name symbol; each FST stores sorted label values as UTF-8 strings. Segment query regex expansion streams persisted FST values and resolves them back through segment symbols to retrieve postings.

**Tech Stack:** Rust, `fst` crate, existing segment-local symbols, exact postings, and chunk index files.

---

### Task 1: FST and Container Tests

**Files:**
- Modify: `chronoxide-core/tests/postings_index.rs`
- Modify: `chronoxide-core/tests/segment_publish.rs`

- [x] **Step 1: Write failing unit tests**

Add tests for `LabelValueFstIndex::from_series`, sorted value enumeration, and combined `SegmentIndexes` roundtrip.

- [x] **Step 2: Write failing integration test**

Add a segment-publish test proving `indexes.puffin` contains persisted value FSTs for metric and label values.

- [x] **Step 3: Run focused tests to verify red**

Run: `cargo test -p chronoxide-core --test postings_index --test segment_publish`

Expected: compile failure because FST index APIs do not exist.

### Task 2: FST Index Implementation

**Files:**
- Modify: `Cargo.toml`
- Modify: `chronoxide-core/Cargo.toml`
- Modify: `chronoxide-core/src/storage/index.rs`
- Modify: `chronoxide-core/src/storage/segment.rs`

- [x] **Step 1: Add dependency**

Add `fst = "0.4"` to workspace dependencies and `chronoxide-core`.

- [x] **Step 2: Implement FST value index**

Add `LabelValueFstIndex`, `write_label_value_fst_index`, and `read_label_value_fst_index`.

- [x] **Step 3: Implement segment index container**

Add `SegmentIndexes`, `write_segment_indexes`, and `read_segment_indexes`, preserving legacy exact-postings-only reads.

- [x] **Step 4: Wire segment write/read path**

Write exact postings and value FSTs together from `SegmentWriter::flush`, and use the persisted FSTs for regex expansion in `SegmentReader`.

- [x] **Step 5: Run focused tests to verify green**

Run: `cargo test -p chronoxide-core --test postings_index --test segment_publish`

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

Stage only intended files and commit with `feat: persist label value FST indexes`.
