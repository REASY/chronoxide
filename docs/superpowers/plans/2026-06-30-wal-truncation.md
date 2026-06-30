# WAL Truncation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a safe WAL prefix deletion primitive driven by manifest-published segment `wal_lsn_boundary` values.

**Architecture:** Keep truncation policy in `chronoxide-core::storage::wal`. Encode WAL file sequence into the high bits of the existing `u64` WAL LSN so old single-file offsets remain sequence 0; compute the safe boundary from the manifest inventory only when every live segment has a boundary; delete only whole WAL files with sequence lower than both the safe boundary sequence and the active WAL sequence.

**Tech Stack:** Rust 2024, existing manifest inventory, filesystem-backed WAL tests, crc32c WAL module.

---

### Task 1: WAL LSN And Manifest Policy

**Files:**
- Modify: `chronoxide-core/src/storage/wal.rs`

- [x] Add failing unit tests for WAL file name parsing, global WAL LSN encoding, and manifest safe-boundary calculation.
- [x] Implement `wal_file_name`, `parse_wal_file_name`, `wal_lsn`, `wal_lsn_sequence`, `wal_lsn_offset`, and `safe_wal_truncation_lsn`.
- [x] Verify `cargo test -p chronoxide-core storage::wal::tests::wal_truncation --quiet`.

### Task 2: Filesystem Prefix Truncation

**Files:**
- Modify: `chronoxide-core/src/storage/wal.rs`
- Create: `chronoxide-core/tests/wal_truncation.rs`

- [x] Add integration tests proving manifest-safe truncation deletes old closed WAL files, preserves the active WAL file, preserves same-sequence partial files, ignores non-WAL files, and does nothing when any manifest segment lacks a boundary.
- [x] Implement `WalTruncationReport`, WAL file discovery, `truncate_wal_prefix`, and `truncate_wal_prefix_from_manifest`.
- [x] Verify `cargo test -p chronoxide-core --test wal_truncation --quiet`.

### Task 3: Docs And Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-wal-truncation.md`

- [x] Mark only `WAL truncation once manifest-published segments cover the data` complete.
- [x] Run focused and regression tests.
- [x] Commit as `feat: truncate manifest-covered WAL files`.
