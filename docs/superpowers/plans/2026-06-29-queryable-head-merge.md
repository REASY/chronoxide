# Queryable Head Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make fresh samples in the active `HeadBuffer` visible to PromQL selector queries and merge them with sealed segment results by stable `series_id`.

**Architecture:** Keep `HeadBuffer` as the encoded write buffer and add a read overlay that borrows a read-only label resolver. The overlay canonicalizes each head series labelset with the same PromQL rules used by sealed segments, evaluates the existing selector matcher types, decodes only matched numeric samples, converts integer samples to `f64`, and returns the same query result type as sealed segments. A shared merge helper combines segment and head results, sorts samples, and resolves duplicate timestamps with later sources winning; callers pass sealed segments first and head last so head samples take precedence.

**Tech Stack:** Rust, `chronoxide-core` storage modules, existing `LabelSetStore`, existing `HeadBuffer`, existing segment selector/result APIs.

---

### Task 1: Head Selector Tests

**Files:**
- Modify: `chronoxide-core/src/storage/head.rs`
- Test: `chronoxide-core/tests/head_query.rs`

- [x] **Step 1: Write failing tests**

Create tests that:
- build a small `HeadBuffer` with a label store,
- query the active head by metric shorthand and equality matcher,
- query `!=` and include missing labels,
- convert head integer samples to PromQL `f64`.

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p chronoxide-core --test head_query`

Expected: compile failure for missing head query API.

- [x] **Step 3: Implement minimal head query overlay**

Add:
- a read-only label resolver trait with a blanket implementation for `LabelSetStore`,
- `HeadBuffer::query_selector`,
- label canonicalization through `canonicalize_labelset`,
- selector matching over normalized labels,
- numeric sample decoding only.

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p chronoxide-core --test head_query`

Expected: all new head query tests pass.

### Task 2: Sealed Segment + Head Merge Tests

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`
- Test: `chronoxide-core/tests/head_query.rs`

- [x] **Step 1: Write failing tests**

Add a test that:
- writes one sealed segment sample,
- keeps a newer matching sample in the active head,
- queries one API across both,
- receives a single merged series with sorted samples.

Add a duplicate timestamp test where sealed and head both contain the same `series_id` and timestamp; expect the head value to win.

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p chronoxide-core --test head_query`

Expected: compile failure for missing merged query API or assertion failure before duplicate precedence is implemented.

- [x] **Step 3: Implement shared merge helper and store API**

Add:
- `merge_query_results` that groups by `series_id`, sorts samples, and keeps the last value for duplicate timestamps,
- `SegmentStoreReader::query_selector_with_head` that merges segment results first and head results second.

- [x] **Step 4: Run tests to verify they pass**

Run: `cargo test -p chronoxide-core --test head_query`

Expected: all head and merge tests pass.

### Task 3: Verification and Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`

- [x] **Step 1: Update milestone status**

Mark the head query and merge portions of Milestone 3 complete, while leaving persistent head postings, WAL/recovery, regex, and guardrails open.

- [x] **Step 2: Run focused verification**

Run:
- `cargo fmt -- --check`
- `cargo test -p chronoxide-core storage`
- `cargo test -p chronoxide-core --test head_query --test segment_query --test promql_normalization --test series_bin --test postings_index --test segment_publish`
- `cargo test -p chronoxide-ingester processor_writes`

Expected: all commands exit successfully.

- [x] **Step 3: Commit scoped changes**

Stage only the head query implementation, segment merge API, tests, and milestone docs.

Commit message: `feat: query head with sealed segments`
