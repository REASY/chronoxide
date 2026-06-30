# Segment Footer Checksums Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace placeholder `footer.bin` files with checksummed segment footer metadata and validate manifest-published segments before they become queryable.

**Architecture:** Keep the footer format local to `chronoxide-core::storage::segment` because it describes segment-local files. `SegmentWriter` writes `footer.bin` last with schema version, tracked file sizes, per-file xxhash64 checksums, and a footer crc32c trailer; manifest-authoritative open validates footer metadata before returning a `SegmentStoreReader`.

**Tech Stack:** Rust 2024, binary little-endian footer records, in-repo xxhash64 implementation, existing crc32c dependency, tempfile-backed integration tests.

---

### Task 1: Footer Codec Unit Tests

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`

- [x] Add failing unit tests for footer encode/decode roundtrip and footer crc mismatch.
- [x] Implement `SegmentFooter`, `SegmentFooterFile`, footer encode/decode helpers, file-id mapping, and footer checksum trailer.
- [x] Verify `cargo test -p chronoxide-core storage::segment::tests::segment_footer --quiet`.

### Task 2: Footer Write And Validate

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`

- [x] Add failing unit test for validating tracked file size/checksum against a footer.
- [x] Implement `write_segment_footer`, `read_segment_footer`, `validate_segment_footer`, and file xxhash64 helpers.
- [x] Replace placeholder footer write in `SegmentWriter::flush`.
- [x] Verify `cargo test -p chronoxide-core storage::segment::tests::segment_footer --quiet`.

### Task 3: Manifest Path Validation

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`
- Create: `chronoxide-core/tests/segment_footer.rs`

- [x] Add integration tests proving manifest-published segments open when footer validation passes, reject a corrupted tracked file, and reject a corrupted footer.
- [x] Add `SegmentReader::open_validated` and use it from `SegmentStoreReader::open_manifest_inventory`.
- [x] Verify `cargo test -p chronoxide-core --test segment_footer --quiet`.

### Task 4: Docs And Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-segment-footer-checksums.md`

- [x] Mark only `Segment footer checksums and validation` complete.
- [x] Run focused and regression tests.
- [x] Commit as `feat: validate segment footers`.
