# Checkpoint Records Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add checkpoint WAL payloads and an atomic `checkpoint.meta` snapshot for fast restart boundaries.

**Architecture:** Keep checkpoint durability primitives in `chronoxide-core::storage::wal` next to the WAL record codec. Checkpoint payloads are a storage-native binary format with sorted `(topic, partition) -> next_offset` entries, a WAL LSN, and wall-clock timestamp; `checkpoint.meta` wraps that payload in its own checksummed file frame and is atomically replaced.

**Tech Stack:** Rust 2024, `std::fs` atomic rename, little-endian binary encoding, `crc32c`.

---

### Task 1: Checkpoint Payload Codec

**Files:**
- Modify: `chronoxide-core/src/storage/wal.rs`

- [x] Add failing unit tests for checkpoint payload roundtrip, sorted offsets, duplicate partition rejection, and truncated payload rejection.
- [x] Implement `TransportOffset`, `WalCheckpoint`, `encode_checkpoint_payload`, and `decode_checkpoint_payload`.
- [x] Verify focused WAL tests pass.

### Task 2: Checkpoint WAL Records

**Files:**
- Modify: `chronoxide-core/src/storage/wal.rs`
- Modify: `chronoxide-core/tests/wal.rs`

- [x] Add integration test for appending a checkpoint record whose `wal_lsn` matches the record offset.
- [x] Implement `WalWriter::append_checkpoint` and `decode_checkpoint_record`.
- [x] Verify focused WAL tests pass.

### Task 3: Atomic `checkpoint.meta`

**Files:**
- Modify: `chronoxide-core/src/storage/wal.rs`
- Modify: `chronoxide-core/tests/wal.rs`

- [x] Add integration tests for missing metadata, latest atomic replacement, and corruption rejection.
- [x] Implement `write_checkpoint_meta` and `read_checkpoint_meta`.
- [x] Verify focused and regression tests.

### Task 4: Docs And Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-checkpoint-records.md`

- [x] Mark only checkpoint records and `checkpoint.meta` complete.
- [x] Commit as `feat: add WAL checkpoint metadata`.
