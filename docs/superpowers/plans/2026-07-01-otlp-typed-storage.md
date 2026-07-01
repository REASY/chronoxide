# OTLP Typed Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist OTLP Histogram, ExponentialHistogram, and Summary datapoints in segment chunks and expose the first PromQL-compatible scalar projections.

**Architecture:** Reuse the existing `HeadBuffer` typed value models and `SchemaVarLenCodec` as the first native chunk encoding. Extend `chunks.bin`, `chunk_index.bin`, and `series.bin.kind_mask` to carry typed chunks, then add query-time projection for scalar views instead of expanding typed metrics during ingest.

**Tech Stack:** Rust, `chronoxide-core` storage chunks/segments/head codecs, `chronoxide-ingester` OTLP processor tests, existing `cargo test` suite.

---

### Task 1: Native Typed Chunk Roundtrip

**Files:**
- Modify: `chronoxide-core/src/storage/chunk.rs`
- Test: `chronoxide-core/src/storage/chunk.rs`

- [x] Add failing unit tests for `ChunkWriter` roundtripping one chunk each of `HistogramValue`, `ExponentialHistogramValue`, and `SummaryValue`.
- [x] Verify the tests fail because typed append methods and `ChunkSamples` variants do not exist.
- [x] Add chunk kinds `Histogram = 2`, `ExponentialHistogram = 3`, and `Summary = 4`.
- [x] Add schema-varlen typed append helpers that write `t0_ms`, per-sample `dt_ms`, and a schema-varlen value payload.
- [x] Decode typed chunks back into `ChunkSamples::{Histogram, ExponentialHistogram, Summary}`.
- [x] Run the focused chunk tests and keep existing float/int tests green.

### Task 2: Segment Writer Typed Persistence

**Files:**
- Modify: `chronoxide-core/src/storage/series.rs`
- Modify: `chronoxide-core/src/storage/segment.rs`
- Test: `chronoxide-core/src/storage/segment.rs`

- [x] Add failing segment writer tests proving typed samples create segment files, typed chunk index entries, typed `kind_mask`, and roundtrip through `ChunkReader`.
- [x] Verify the tests fail because segment writer has no typed record methods.
- [x] Add typed `SERIES_KIND_*` bits and make `ensure_local_series` OR in kind bits per written chunk.
- [x] Add typed record methods using label visitors and the typed chunk writer methods.
- [x] Keep scalar query paths skipping native typed chunks unless a projection asks for them.
- [x] Run focused segment tests.

### Task 3: Ingester Segment Path

**Files:**
- Modify: `chronoxide-ingester/src/processor.rs`
- Test: `chronoxide-ingester/src/processor.rs`

- [x] Add a failing processor integration test with Histogram, ExponentialHistogram, and Summary datapoints plus a segment writer.
- [x] Verify it fails because non-number samples are suppressed or dropped.
- [x] Record non-number samples into head when a head exists, even when a segment writer is configured.
- [x] Replace typed drop warnings in `write_head_window_samples` with typed segment writer calls.
- [x] Assert head write profile typed-drop counters stay zero and segment chunks contain typed records.
- [x] Run focused ingester tests.

### Task 4: First PromQL Projections

**Files:**
- Modify: `chronoxide-core/src/storage/segment.rs`
- Modify: `chronoxide-core/src/storage/head.rs`
- Test: `chronoxide-core/tests/promql_query.rs`

- [x] Add failing integration tests for classic histogram projections: `<metric>_count`, `<metric>_sum`, and `<metric>_bucket{le="...", le="+Inf"}`.
- [x] Add failing integration tests for summary projections: `<metric>_count`, `<metric>_sum`, and `{quantile="..."}`.
- [x] Verify failures are missing results, not parser failures.
- [x] Extend `SegmentSelector` with an internal projection mode derived from PromQL metric suffixes and virtual `le`/`quantile` matchers.
- [x] Rewrite projected selectors to native metric candidates and synthesize projected labels after decoding typed chunks.
- [x] Apply the same projection rules to active head queries.
- [x] Run PromQL and storage query tests.

### Deferred Follow-Ups

- Add exemplar sidecars.
- Wire native histogram query functions to consume ExponentialHistogram downscale/merge and stored reset hints.

### Completed Follow-Ups

- [x] Persist OTLP `start_time_unix_nano`, `DataPointFlags`, temporality, and counter reset hints for native typed values; chunk index flags now advertise present metadata.
- [x] Map `FLAG_NO_RECORDED_VALUE` typed samples to Prometheus stale NaN in virtual scalar projections.
- [x] Compute cumulative Histogram/ExponentialHistogram reset hints in the single-writer ingester path.
- [x] Project DELTA Histogram count/sum/bucket samples as cumulative-shaped virtual PromQL series.
- [x] Project ExponentialHistogram `_bucket` samples through deterministic query-configured classic boundaries, including active-head DELTA accumulation and sealed segment chunks.
- [x] Add reusable native ExponentialHistogram downscale/merge helpers and use the same downscale path for ingester reset detection.
- [x] Add first scalar PromQL range functions: `rate(selector[range])` and `increase(selector[range])` over vector/projection query results.
