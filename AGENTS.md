# AGENTS.md

## Project Goal

Chronoxide is an OTLP-native metrics TSDB. It ingests OTLP metrics from Kafka
or captured replay files, persists typed native segments, and exposes
PromQL-compatible query semantics.

Correctness comes before performance. For storage, histograms, summaries,
replay, WAL, recovery, and PromQL projection semantics, do not guess. Read the
normative specs and current coverage first.

## Read First

Before changing storage, ingest, replay, or query behavior, read:

- `docs/superpowers/specs/storage.md`
- `docs/superpowers/specs/clock.md`
- `docs/promql-coverage.md` for PromQL changes
- the current design or plan for the subsystem being changed

Dated context exports, benchmark reports, and completed plans are historical
evidence, not current authority or backlog.

## Core Semantics

- Storage is event-time based.
- Control policy is ingest/capture-time based.
- `captured_at_ms` is the trusted replay anchor.
- Kafka/source timestamps are diagnostics only; do not use them as fallback
  event time.
- Missing OTLP datapoint timestamps must be rejected.
- Missing OTLP number values must not be stored as zero.
- `max_event_age_secs` and `max_event_lead_secs` must be non-negative.
- Replay determinism requires preserved `captured_at_ms`, stable input order,
  identical writer configuration, and deterministic segment IDs.
- Typed OTLP metadata matters: temporality, flags, start time, and reset hints
  are correctness fields, not optional decoration.
- Classic histogram PromQL buckets are cumulative prefix sums, with synthetic
  `le="+Inf"` equal to `_count`.

## PromQL And Typed OTLP Semantics

- Only the exact Prometheus stale-NaN sentinel is stale. Ordinary IEEE `NaN`,
  `+Inf`, and `-Inf` remain values.
- `rate()` and `increase()` omit exact stale markers without shortening the
  original logical range or inventing a reset. Stored cumulative reset hints
  and reset hints on unknown-temporality streams remain authoritative across
  stale omission.
- A `rate()` or `increase()` range that logically starts before epoch zero
  includes a timestamp-zero sample and retains the pre-epoch duration for
  extrapolation.
- OTLP delta Histogram and ExponentialHistogram count/bucket projections are
  cumulative-shaped at the PromQL surface; never expose raw deltas as counter
  samples.
- Every selected non-stale delta Histogram or ExponentialHistogram interval
  requires a present `start_time_ms < timestamp_ms`. Stale no-recorded-value
  gaps are exempt.
- Delta optional sums are signed IEEE interval values. Negative or non-finite
  sums must not invalidate otherwise valid count/bucket results.
- A virtual delta projection may retain one aligned pre-range cumulative
  sample only as a subtraction seed. That predecessor is not a selected
  interval and contributes no value by itself.
- Native and virtual signed delta sums must agree. Do not assume native and
  virtual multi-sample count/bucket evaluation use identical algorithms.
- `chronoxide-query --verify-readbacks` is an intentionally independent
  expected-value oracle for supported, isolation-safe decoded-chunk cases.
  When query semantics change, update its focused tests; do not make it call
  production evaluator helpers merely to force agreement. Inspect its
  executed/skipped diagnostics: a skipped case is a coverage gap, not a pass.

## Storage Format And Index Policy

- Do not add or change on-disk semantics without updating
  `docs/superpowers/specs/storage.md`.
- Backward compatibility with prior experimental segment formats is not
  required unless explicitly requested. Readers may reject old versions, and
  old corpora may need deterministic replay.
- Prefer explicit, decodable byte layouts over prose-only formats.
- Segment-index metadata is intentionally lazy and accessed through immutable
  positional-read state. Do not reintroduce eager complete-directory
  materialization or shared seek cursors without correctness proof and fresh
  measurement.
- Touched malformed index metadata must propagate as corruption. Never turn a
  parse, checksum, bounds, ordering, or count failure into a cache miss,
  pruning decision, or empty query result.
- On-disk changes require deterministic byte/round-trip tests, corruption and
  error-propagation tests, replay/readback equivalence, and real-corpus
  performance evidence where relevant.

## Engineering Workflow

- Use `rg` first for search.
- Use `apply_patch` for manual file edits.
- Preserve unrelated user changes in a dirty working tree.
- Do not delete, rewrite, stage, or commit local smoke/runtime artifacts unless
  explicitly requested.
- Keep commits focused and logical.
- Before committing, run relevant tests, formatting checks, and
  `git diff --check`; inspect the staged file list.
- For behavior changes, add focused unit tests plus integration, source-level,
  or external-oracle coverage as appropriate.
- Update the storage spec and PromQL coverage matrix when their stated
  semantics or support level changes.

## Verification Commands

Run targeted checks while developing, then broaden before committing. Common
commands once the Rust workspace exists:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --lib --bins --all-features -- -D warnings -D unreachable_pub
cargo clippy --workspace --tests --benches --all-features -- -D warnings -A clippy::expect_used -A clippy::unwrap_used -A clippy::panic -A unreachable_pub
```

Use `cargo llvm-cov` for coverage-sensitive changes. Use `cargo deny check`
when configured, or `cargo audit` as the fallback dependency security check.
Call out any verification command that cannot run and why.

For PromQL rate, staleness, reset, or histogram-semantic changes, run the real
Prometheus oracle when `promtool` is available:

```sh
CHRONOXIDE_PROMTOOL=/path/to/promtool \
  cargo test -p chronoxide-core --test prometheus_golden \
    prometheus_golden_suite_matches_current_promql_surface \
    -- --ignored --exact --nocapture
```

Query smoke/readback verification must use an explicitly selected corpus:

```sh
cargo build --release -p chronoxide-query-cli --bin chronoxide-query
./target/release/chronoxide-query \
  --segments-dir "$SEGMENTS_DIR" \
  --sample-limit-per-kind 2 \
  --verify-readbacks \
  --validate-segment-footers
```

Footer validation is a separate correctness pass. Do not include its full-file
reads in timed query benchmarks.

For Kafka smoke runs, use a run-specific configuration with new output and
capture paths. Never delete or reuse an existing corpus automatically:

```sh
cargo build --release
RUST_LOG=chronoxide_ingester=info,chronoxide_core=warn \
  CONFIG_FILE="$CONFIG_FILE" \
  ./target/release/chronoxide-ingester 2>&1 | tee "$LOG_FILE"
```

## Performance Work

- Profile before optimizing. On macOS use `sample`; on Linux use `perf` or
  equivalent sampling outside measured benchmark processes.
- Do not infer bottlenecks only from noisy logs.
- Real replay/smoke data is the performance reference; microbenchmarks are
  supporting evidence.
- A/B comparisons must use the same host, toolchain, build mode, fingerprinted
  corpus, query schedule, limits, and configuration. Runtime-flag comparisons
  must use one identical release binary; code-version comparisons must record
  both binary hashes. Cache budgets must be explicit;
  `--range-scalar-cache-max-bytes 0` disables the range scalar cache.
- Require semantic fingerprints to match, not only series/sample counts.
  Intended `QueryStats` changes must be named and reviewed; unexplained stats
  differences fail the comparison.
- Report cold and warm latency, peak RSS, cache charges, logical payload-used
  bytes, coalesced payload-read bytes, and read/used amplification.
  Payload-read bytes describe process-issued file spans, not storage-device
  traffic or operating-system cache misses.
- A CLI `cold` run means the first expression run in a fresh query session; it
  does not imply a cold operating-system page cache.
- Do not overlap measured processes with builds, replay, profilers, footer
  validation, or unrelated workloads.
- Keep human-readable reports, raw machine data, environment metadata, and
  logs together in one run-specific untracked output directory.

## Reliability Work

Do not assume WAL, checkpoints, Ctrl+C shutdown, or replay recovery are
complete unless verified by tests.

For recovery-related changes, cover:

- normal shutdown
- interrupted shutdown
- replay from capture
- deterministic segment IDs
- event-time policy under replay
- query equivalence after recovery

## Working Tree Rules

The repo may contain user/runtime artifacts such as:

- `data/`
- Kafka smoke/replay logs
- `ingestion_stats_*.md`
- benchmark reports and raw outputs
- dated context exports
- ad-hoc or modified smoke configurations

Treat these as user-owned. Do not stage, delete, rewrite, move, or include them
in a cleanup unless explicitly requested. Stage task files by explicit path and
inspect the index before committing.

## When Asked "What Is Next?"

Consult `docs/promql-coverage.md`, current specs/plans, and verified test gaps.
Prefer the highest-leverage correctness or recovery gap before performance or
polish. Do not treat dated context exports or completed plans as the current
backlog; profile again before proposing performance work.

## Git Hygiene

- Do not revert user changes or unrelated dirty work.
- Use Conventional Commits: `<type>[optional scope]: <description>`.
- Common types: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `chore`,
  `ci`, `build`, `revert`.
- Mark breaking changes with `!` or a `BREAKING CHANGE:` footer.
  migration/backfill path is documented.
- Update specs/notes when changing API semantics, schema labels/properties,
  uncertainty semantics, indexing passes, query policy, or benchmark
  interpretation.

