# Spec Chunk IO Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Linux-friendly CLI benchmark that generates spec-shaped `chunks.bin`, `ooo_chunks.bin`, and `chunk_index.bin` artifacts, then compares serial `pread` with batched `io_uring` chunk reads.

**Architecture:** Implement the benchmark as `chronoxide-core/examples/spec_chunk_io_bench.rs` so it can use existing dev dependencies such as `clap`, `rand`, and `tempfile` without expanding production dependencies. Keep request planning, dataset generation, reader execution, and reporting separated inside the example file, with unit tests for deterministic planning and byte accounting.

**Tech Stack:** Rust 2024, `clap` derive, existing `chronoxide_core::storage::chunk` index format helpers, existing `chronoxide_core::storage::io::{PreadReader, IoUringReader, ReadRequest}`.

---

### Task 1: Planning Tests

**Files:**
- Create: `chronoxide-core/examples/spec_chunk_io_bench.rs`

- [ ] Add tests for candidate series planning:
  - contiguous candidates must be dense and sorted
  - strided candidates must be sorted, unique, and within `total_series`
  - random candidates must be deterministic for the same seed

- [ ] Add tests for chunk artifact planning:
  - generated index entries must point at `chunks.bin` when `ooo_percent = 0`
  - generated index entries must include both `chunks.bin` and `ooo_chunks.bin` when `ooo_percent > 0`
  - logical bytes must equal `request_count * chunk_size`

- [ ] Verify RED:

```bash
cargo test -p chronoxide-core --example spec_chunk_io_bench
```

Expected: fail because benchmark planning types/functions are not implemented.

### Task 2: Core Benchmark Implementation

**Files:**
- Modify: `chronoxide-core/examples/spec_chunk_io_bench.rs`

- [ ] Implement `CandidatePattern`, `BenchConfig`, `SegmentPlan`, and `PlannedChunk`.
- [ ] Implement candidate generation.
- [ ] Implement segment artifact generation:
  - create `seg-<n>/chunks.bin`
  - create `seg-<n>/ooo_chunks.bin`
  - create `seg-<n>/chunk_index.bin`
  - write frame-header padding plus fixed-size logical chunk bytes
  - write chunk index entries with `file_id` 0 or 1
- [ ] Implement read request planning from candidate series and in-memory chunk entries.
- [ ] Verify GREEN:

```bash
cargo test -p chronoxide-core --example spec_chunk_io_bench
```

Expected: pass.

### Task 3: CLI and Reader Modes

**Files:**
- Modify: `chronoxide-core/examples/spec_chunk_io_bench.rs`

- [ ] Implement `clap::Parser` args:
  - `--dir`
  - `--keep-files`
  - `--segments`
  - `--total-series`
  - `--candidate-series`
  - `--chunks-per-series`
  - `--chunk-size-kb`
  - `--ooo-percent`
  - `--pattern`
  - `--iterations`
  - `--warmup-iters`
  - `--queue-depths`
  - `--mode`
  - `--seed`
  - `--sparse`
- [ ] Implement `pread`, `io-uring`, and `both` modes.
- [ ] On non-Linux or builds without `--features io_uring`, return a clear error if `io-uring` is requested.
- [ ] Print one metrics row per mode/queue-depth with request count, logical MiB, wall time, throughput, and latency percentiles.
- [ ] Verify CLI smoke:

```bash
cargo run -p chronoxide-core --example spec_chunk_io_bench -- \
  --segments 1 \
  --total-series 16 \
  --candidate-series 4 \
  --chunks-per-series 2 \
  --chunk-size-kb 4 \
  --iterations 1 \
  --warmup-iters 0 \
  --mode pread \
  --sparse
```

Expected: prints a `pread` metrics row.

### Task 4: Final Verification

**Files:**
- Modify: `chronoxide-core/examples/spec_chunk_io_bench.rs`

- [ ] Format:

```bash
cargo fmt
```

- [ ] Run focused tests:

```bash
cargo test -p chronoxide-core --example spec_chunk_io_bench
```

- [ ] Run existing IO tests:

```bash
cargo test -p chronoxide-core storage::io::tests
```

- [ ] Run CLI smoke command from Task 3.
