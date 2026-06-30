# WAL Replay Into Head Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replay durable WAL `OTLP_BATCH` records into a fresh `HeadBuffer` so restart recovery can restore PromQL-visible head data.

**Architecture:** Keep the OTLP batch payload codec in `chronoxide-core::storage::wal` and add a focused `chronoxide-core::storage::wal_replay` module for scan/replay behavior. Replay reads `checkpoint.meta` when present, validates the checkpoint record at its LSN, replays later valid batches into a caller-provided `HeadBuffer` and label store, and stops without discarding prior samples at the first invalid/torn record.

**Tech Stack:** Rust 2024, `prost` protobuf encode/decode, OpenTelemetry protobuf types, existing `HeadBuffer`, `LabelSetStore`, and OTLP label/value helpers.

---

### Task 1: OTLP Batch WAL Payload

**Files:**
- Modify: `chronoxide-core/src/storage/wal.rs`

- [x] Add failing unit tests for OTLP batch payload roundtrip and truncated protobuf rejection.
- [x] Implement `OtlpWalBatch`, `encode_otlp_batch_payload`, `decode_otlp_batch_payload`, and `WalWriter::append_otlp_batch`.
- [x] Verify `cargo test -p chronoxide-core storage::wal`.

### Task 2: WAL Replay Scanner

**Files:**
- Create: `chronoxide-core/src/storage/wal_replay.rs`
- Modify: `chronoxide-core/src/storage/mod.rs`

- [x] Add unit test for replay stop error classification.
- [x] Implement replay report types and file scan loop that stops at invalid/torn records.
- [x] Verify `cargo test -p chronoxide-core storage::wal_replay`.

### Task 3: Replay Into Head

**Files:**
- Modify: `chronoxide-core/src/storage/wal_replay.rs`
- Create: `chronoxide-core/tests/wal_replay.rs`

- [x] Add integration tests for replaying OTLP number datapoints into queryable head data.
- [x] Add integration test for checkpoint-aware replay that starts after the checkpoint record.
- [x] Add integration test proving a torn record stops replay while preserving earlier samples.
- [x] Implement OTLP metric iteration, label interning, and `HeadBuffer::record_sample` calls.

### Task 4: Docs And Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-wal-replay-head.md`

- [x] Mark only WAL replay into head complete.
- [x] Run focused and regression tests.
- [x] Commit as `feat: replay WAL batches into head`.
