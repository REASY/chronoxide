# Manifest CURRENT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add crash-safe shard manifest primitives: append-only `MANIFEST-*` records plus atomic `CURRENT` pointer updates.

**Architecture:** Implement a new `chronoxide-core::storage::manifest` module separate from segment reading. The module owns checksummed manifest record encoding, append/read APIs, `CURRENT` validation and atomic replacement, and reconstruction of a time-ordered live segment inventory from sealed/tombstone records.

**Tech Stack:** Rust 2024, crc32c, little-endian binary records, `std::fs` atomic rename and directory fsync.

---

### Task 1: Manifest Record Codec

**Files:**
- Create: `chronoxide-core/src/storage/manifest.rs`
- Modify: `chronoxide-core/src/storage/mod.rs`

- [x] Add failing unit tests for sealed record roundtrip, deleted record roundtrip, crc mismatch, truncated payload, and unsafe manifest file names.
- [x] Implement `ManifestSegment`, `ManifestRecord`, record encode/decode helpers, and filename validation.
- [x] Verify `cargo test -p chronoxide-core storage::manifest`.

### Task 2: Manifest Inventory

**Files:**
- Modify: `chronoxide-core/src/storage/manifest.rs`

- [x] Add failing unit test for sealed plus deleted records producing a live segment inventory.
- [x] Implement `ManifestInventory::from_records` sorted by `(start_ms, end_ms, segment_id)`.
- [x] Verify `cargo test -p chronoxide-core storage::manifest`.

### Task 3: Manifest File And CURRENT APIs

**Files:**
- Modify: `chronoxide-core/src/storage/manifest.rs`
- Create: `chronoxide-core/tests/manifest.rs`

- [x] Add integration tests for append/read inventory, atomic `CURRENT` replacement, missing `CURRENT`, and ignoring orphan segment dirs.
- [x] Implement `ManifestWriter`, `manifest_file_name`, `write_current`, `read_current`, and `read_manifest_inventory`.
- [x] Verify `cargo test -p chronoxide-core --test manifest`.

### Task 4: Docs And Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-manifest-current.md`

- [x] Mark only `Manifest CURRENT and MANIFEST-*` complete.
- [x] Run focused and regression tests.
- [x] Commit as `feat: add manifest current log`.
