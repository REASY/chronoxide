# Manifest Retention Tombstones Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manifest-authoritative retention step that tombstones expired segments without moving or deleting segment directories.

**Architecture:** The manifest already stores `SegmentDeleted` records and builds live inventory by applying deletes. This slice adds a helper that appends synced delete tombstones for live inventory segments with `end_ms <= retain_after_ms`. Query readers that use `open_manifest_published` will ignore tombstoned segments even while files remain on SSD; physical trash movement stays in the next slice.

**Tech Stack:** Rust 2024, manifest record log, `ManifestWriter`, `SegmentStoreReader::open_manifest_published`, cargo test.

---

### Task 1: Manifest Retention Helper

**Files:**
- Modify: `chronoxide-core/src/storage/manifest.rs`

- [x] Write a failing unit test proving retention appends delete tombstones for segments ending at or before the cutoff and leaves newer segments live.
- [x] Run `cargo test -p chronoxide-core storage::manifest::tests::manifest_retention_tombstones_segments_at_or_before_cutoff --quiet` and verify it fails before implementation.
- [x] Implement `RetentionTombstoneReport` and `append_retention_tombstones`.
- [x] Run the focused unit test and verify it passes.

### Task 2: Manifest-Published Query Integration

**Files:**
- Modify: `chronoxide-core/tests/segment_query.rs`

- [x] Write an integration test proving `open_manifest_published` stops querying a tombstoned segment while its directory still exists.
- [x] Run `cargo test -p chronoxide-core --test segment_query manifest_retention_tombstones_hide_expired_segments_without_deleting_files --quiet` and verify it passes with Task 1.

### Task 3: Docs, Regression, Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-manifest-retention-tombstones.md`

- [x] Mark `SSD retention tombstones in the manifest` complete.
- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo test -p chronoxide-core --quiet`.
- [x] Run `cargo test -p chronoxide-core --test segment_query --quiet`.
- [x] Run `git diff --check`.
- [x] Commit as `feat: add manifest retention tombstones`.
