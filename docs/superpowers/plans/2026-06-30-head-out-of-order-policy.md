# Head Out Of Order Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-series last timestamp tracking and a configurable bounded out-of-order acceptance policy to `HeadBuffer`.

**Architecture:** Keep this slice at head admission only. `HeadConfig` gets `out_of_order_time_window`; `HeadBuffer` tracks the max timestamp accepted per series and rejects samples older than `last_timestamp - window`. Accepted bounded OOO samples remain in the current head storage path for now; OOO lane/chunk/query merge work stays in later slices.

**Tech Stack:** Rust 2024, existing `HeadBuffer`, ingester TOML config, unit tests in `head.rs`, config integration tests in `chronoxide-ingester`.

---

### Task 1: HeadBuffer OOO Policy

**Files:**
- Modify: `chronoxide-core/src/storage/head.rs`

- [x] Add failing unit tests for default zero-window rejection, configured bounded OOO acceptance, too-late rejection, and per-series isolation.
- [x] Implement `HeadConfig::with_out_of_order_time_window`, per-series last timestamp tracking, and admission validation.
- [x] Verify `cargo test -p chronoxide-core storage::head::tests::head_buffer_out_of_order --quiet`.

### Task 2: Ingester Config Wiring

**Files:**
- Modify: `chronoxide-ingester/src/app_config.rs`
- Modify: `chronoxide-ingester/src/main.rs`

- [x] Add failing config test for `head_buffer.out_of_order_time_window_secs`.
- [x] Add the config field with default `0` and pass it into `HeadConfig` for both standalone head buffer and segment-writer-backed head buffer.
- [x] Verify `cargo test -p chronoxide-ingester head_buffer_config --quiet`.

### Task 3: Docs And Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-06-29-promql-storage-milestones.md`
- Modify: `docs/superpowers/plans/2026-06-30-head-out-of-order-policy.md`

- [x] Mark `Per-series last timestamp tracking` and `out_of_order_time_window acceptance policy` complete.
- [x] Run focused and regression tests.
- [x] Commit as `feat: add head out-of-order policy`.
