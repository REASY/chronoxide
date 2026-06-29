# PromQL Segment Milestone 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first queryable sealed segment path for exact PromQL selectors over in-order float samples.

**Architecture:** Keep the existing `HeadBuffer -> SegmentWriter -> chunks.bin/chunk_index.bin` path and add the missing segment metadata around it. Normalization and stable series identity live in `chronoxide-core`; the segment writer receives labelsets or a catalog snapshot, remaps head refs to segment-local refs, writes segment-local symbols/series metadata, then writes a small exact-match postings index. Query reading starts at segment scope and returns labels plus decoded samples, not full PromQL expression results.

**Tech Stack:** Rust 2024, existing `chronoxide-core` storage modules, crc32c for existing chunk integrity, little-endian binary formats, serde only for existing `meta.json`.

---

## File Structure

- Create `chronoxide-core/src/promql/mod.rs`: PromQL-facing normalization and canonical labelset helpers.
- Modify `chronoxide-core/src/lib.rs`: expose the `promql` module.
- Create `chronoxide-core/tests/promql_normalization.rs`: normalization and canonical identity tests.
- Create `chronoxide-core/src/storage/series.rs`: `symbols.bin` and `series.bin` v1 read/write helpers.
- Create `chronoxide-core/tests/series_bin.rs`: binary metadata roundtrip tests.
- Create `chronoxide-core/src/storage/index.rs`: minimal exact postings index read/write helpers.
- Create `chronoxide-core/tests/postings_index.rs`: exact matcher postings tests.
- Modify `chronoxide-core/src/storage/mod.rs`: expose `series` and `index`.
- Modify `chronoxide-core/src/storage/segment.rs`: pass/persist series metadata, keep existing simple APIs working for tests.
- Modify `chronoxide-ingester/src/processor.rs`: supply labelsets to segment writing.

## Task 1: PromQL Name Normalization

- [x] Write failing tests in `chronoxide-core/tests/promql_normalization.rs` for metric normalization, label normalization, reserved label prefixing, collision suffixes, canonical sort order, and stable `series_id`.
- [x] Run `cargo test -p chronoxide-core --test promql_normalization` and verify it fails because `chronoxide_core::promql` does not exist.
- [x] Create `chronoxide-core/src/promql/mod.rs` with:
  - `normalize_metric_name(original: &str) -> String`
  - `normalize_label_name(original: &str) -> String`
  - `canonicalize_labelset(metric_name: &str, labels: &[(&str, &str)]) -> CanonicalLabelSet`
  - `series_id(canonical: &CanonicalLabelSet) -> u64`
- [x] Expose `pub mod promql;` from `chronoxide-core/src/lib.rs`.
- [x] Run `cargo test -p chronoxide-core --test promql_normalization` and verify it passes.

## Task 2: Segment Metadata Files

- [x] Write failing tests in `chronoxide-core/tests/series_bin.rs` for a segment-local symbol table and `series.bin` v1 roundtrip.
- [x] Run `cargo test -p chronoxide-core --test series_bin` and verify it fails because `storage::series` does not exist.
- [x] Implement `chronoxide-core/src/storage/series.rs` with `SegmentSymbols`, `SeriesEntry`, `write_symbols_bin`, `read_symbols_bin`, `write_series_bin_v1`, and `read_series_bin_v1`.
- [x] Expose `pub mod series;` from `chronoxide-core/src/storage/mod.rs`.
- [x] Run `cargo test -p chronoxide-core --test series_bin` and verify it passes.

## Task 3: Exact Postings Index

- [x] Write failing tests in `chronoxide-core/tests/postings_index.rs` for `(label_name_sym, label_value_sym) -> sorted series_ref list`.
- [x] Run `cargo test -p chronoxide-core --test postings_index` and verify it fails because `storage::index` does not exist.
- [x] Implement a simple `indexes.puffin` v0 container in `chronoxide-core/src/storage/index.rs` for exact postings only.
- [x] Expose `pub mod index;` from `chronoxide-core/src/storage/mod.rs`.
- [x] Run `cargo test -p chronoxide-core --test postings_index` and verify it passes.

## Task 4: Write Self-Describing Segments

- [x] Write a failing integration test that ingests two labeled OTLP gauge series, flushes a segment, and asserts non-empty `symbols.bin`, `series.bin`, and `indexes.puffin`.
- [x] Run the new test and verify it fails because the files are placeholders.
- [x] Modify the processor/segment writer boundary so flushed head windows carry enough labelset metadata to write segment-local symbols, series entries, and exact postings.
- [x] Preserve existing `SegmentWriter::record_samples` tests by adding a minimal legacy path or test helper for unlabeled series.
- [x] Run the new integration test and existing storage tests.

## Task 5: Read Exact Selectors From One Segment

- [x] Write a failing test that opens one segment and queries `{__name__="cpu_usage",pod="backend-1"}` over a time range.
- [x] Run the test and verify it fails because no selector reader exists.
- [x] Implement a segment-level exact selector reader that:
  - loads `symbols.bin`, `series.bin`, and exact postings,
  - intersects positive equality postings,
  - filters `chunk_index.bin` by requested time range,
  - reads `chunks.bin`,
  - returns decoded samples and materialized labels.
- [x] Run the selector test and storage tests.

## Task 6: Verification

- [x] Run `cargo test -p chronoxide-core storage`.
- [x] Run `cargo test -p chronoxide-core --test promql_normalization`.
- [x] Run `cargo test -p chronoxide-core --test series_bin`.
- [x] Run `cargo test -p chronoxide-core --test postings_index`.
- [x] Run the new segment query integration test.
- [x] Run `cargo test -p chronoxide-ingester processor_writes_segment_meta`.

## Self-Review Notes

- Scope is limited to sealed in-order segments and exact positive matchers.
- WAL, manifest, regex, OOO, retention, and queryable head are intentionally deferred.
- Each production change starts with a failing test.
