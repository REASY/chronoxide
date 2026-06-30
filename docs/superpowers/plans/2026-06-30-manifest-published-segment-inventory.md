# Manifest Published Segment Inventory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make segment queries optionally load their segment list from `manifest/CURRENT` plus `MANIFEST-*` instead of scanning every `seg-*` directory.

**Architecture:** Keep the existing `SegmentStoreReader::open` directory scan as a compatibility path, and add a manifest-authoritative constructor for startup/recovery paths that have a manifest directory. The manifest path reads `ManifestInventory`, opens only those segment directories, validates `meta.json` against each manifest entry, and returns an empty store when `CURRENT` is absent.

**Tech Stack:** Rust 2024, existing `chronoxide-core::storage::manifest` module, `SegmentReader`, tempfile-backed integration tests.

---

### Task 1: Manifest Segment Meta Validation

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`

- [x] Add unit tests for accepting matching manifest segment metadata and rejecting a mismatched `meta.json`.
- [x] Implement a private `validate_manifest_segment_meta` helper.
- [x] Verify `cargo test -p chronoxide-core storage::segment::tests::manifest_segment_meta`.

### Task 2: Manifest-Authoritative Store Open

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`
- Modify: `chronoxide-core/tests/segment_query.rs`

- [x] Add integration tests proving manifest-published stores ignore orphan segment directories, return empty without `CURRENT`, and fail when a published segment directory is missing.
- [x] Implement `SegmentStoreReader::open_manifest_published` and `SegmentStoreReader::open_manifest_inventory`.
- [x] Verify `cargo test -p chronoxide-core --test segment_query manifest_published`.

### Task 3: Docs And Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-manifest-published-segment-inventory.md`

- [x] Mark only `Manifest-published segment inventory` complete.
- [x] Run focused and regression tests.
- [x] Commit as `feat: use manifest segment inventory`.
