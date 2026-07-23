# Crate Boundaries

Status: current workspace ownership and Rust API migration reference.

This document defines where process-neutral TSDB behavior and process-facing
integration code belong. It does not define storage, replay-clock, or PromQL
semantics; those remain normative in `storage.md`, `clock.md`, and
`docs/promql-coverage.md`.

## Ownership

### `chronoxide-core`

`chronoxide-core` owns process-neutral, performance-sensitive TSDB behavior:

- typed OTLP metric semantics and label normalization;
- event-time policy primitives;
- the mutable head and immutable segment storage engine;
- segment metadata, indexes, payload codecs, caches, and deterministic hashes;
- PromQL normalization and evaluation.

Storage/query hot paths remain together in this crate. They must not acquire
Kafka clients, process configuration, telemetry exporters, capture compression,
or ingestion-reporting dependencies merely as a convenience for binaries.

### `chronoxide-capture`

`chronoxide-capture` is a leaf crate that owns the OTLP capture reader, writer,
manifest, partition format, compression selection, and capture-specific errors.
It does not depend on `chronoxide-core`.

Moving the codec did not change capture version 2 bytes, record ordering,
`captured_at_ms`, compression behavior, or malformed-input handling. Capture
errors retain their IO-versus-JSON classification. The ingester converts them
to the corresponding `ChronoxideError` kind at its boundary.

### `chronoxide-ingester`

`chronoxide-ingester` owns process-facing ingestion behavior:

- Kafka and capture-file message sources;
- capture wrapping and replay orchestration;
- process configuration and cancellation-aware sleeps;
- process-level errors and log-rate limiting;
- telemetry setup and exporters;
- ingestion statistics and distribution reporting.

`MessageSource` remains a generic, statically dispatched interface. Moving it
does not introduce trait-object dispatch or change source order, source
metadata, or the trusted `captured_at_ms` replay anchor.

### `chronoxide-api`

`chronoxide-api` remains a query-serving shell over `chronoxide-core`. It must
not depend on ingestion transports or telemetry-export setup through the core
crate.

### `chronoxide-query-cli`

`chronoxide-query-cli` owns read-side operational binaries and their
independent verification/reporting code:

- `chronoxide-query`;
- `chronoxide-storage-verify`;
- the intentionally independent decoded-chunk readback oracle;
- query benchmark execution and report generation.

The package depends directly on `chronoxide-core`; it does not depend on
`chronoxide-ingester`. The storage engine and PromQL evaluator remain in core.

## Dependency Direction

The intended workspace edges are:

- `chronoxide-api -> chronoxide-core`;
- `chronoxide-query-cli -> chronoxide-core`;
- `chronoxide-ingester -> chronoxide-core`;
- `chronoxide-ingester -> chronoxide-capture`;
- `chronoxide-core -[dev only]-> chronoxide-capture`, currently for the
  `head_buffer` benchmark.

`chronoxide-capture` has no dependency on another Chronoxide crate.

## Performance And Correctness Invariants

A crate-boundary change alone must not:

- change any storage or capture byte layout;
- change deterministic segment IDs, hashes, replay order, or event-time policy;
- change decoded query values, semantic fingerprints, or `QueryStats`;
- introduce dynamic dispatch, locking, allocation, copying, or serialization
  into an existing hot path;
- move the storage engine or PromQL evaluator across a crate boundary;
- turn malformed capture or index data into an empty result or cache miss.

The internal XXH64 implementation therefore remains in `chronoxide-core`; only
its module name changed from `util` to private `hash`. Ingestion distribution
code was moved without changing its generic implementations, so existing
monomorphization remains available.

## Rust API Migration

This boundary cleanup is a breaking source-level change. Update imports as
follows:

| Previous path | Current path |
| --- | --- |
| `chronoxide_core::otlp_capture::*` | crate root `chronoxide_capture::*` |
| `chronoxide_core::error::*` | `chronoxide_ingester::error::*` |
| `chronoxide_core::source::*` | `chronoxide_ingester::source::*` |
| `chronoxide_core::statistics::*` | `chronoxide_ingester::statistics::*` |
| `chronoxide_core::telemetry::*` | `chronoxide_ingester::telemetry::*` |
| `chronoxide_core::util::get_env_default` | `chronoxide_ingester::runtime::get_env_default` |
| `chronoxide_core::util::load_config` | `chronoxide_ingester::runtime::load_config` |
| `chronoxide_core::util::sleep_for` | `chronoxide_ingester::runtime::sleep_for` |
| `chronoxide_core::prelude::Result` | `chronoxide_ingester::prelude::Result` |

Capture callers now receive `chronoxide_capture::CaptureError`. Ingester
callers may continue to use `chronoxide_ingester::error::ChronoxideError`;
conversion from `CaptureError` preserves the underlying error classification.
Direct capture callers that inspect errors must match
`CaptureErrorKind::{IoError, SerdeJsonError}`.

The `headbuffer_replay` and `schema_stats` examples, and the `stats_u32`
benchmark, now belong to the `chronoxide-ingester` package. Invoke them with
`-p chronoxide-ingester`.

| Previous command | Current command |
| --- | --- |
| `cargo run -p chronoxide-core --example headbuffer_replay -- ...` | `cargo run -p chronoxide-ingester --example headbuffer_replay -- ...` |
| `cargo run -p chronoxide-core --example schema_stats -- ...` | `cargo run -p chronoxide-ingester --example schema_stats -- ...` |
| `cargo bench -p chronoxide-core --bench stats_u32 -- ...` | `cargo bench -p chronoxide-ingester --bench stats_u32 -- ...` |
| `cargo test -p chronoxide-core otlp_capture` | `cargo test -p chronoxide-capture` |

The `head_buffer` benchmark remains in `chronoxide-core`. No binary target
name changed.

The read-side binaries changed package ownership:

| Previous command | Current command |
| --- | --- |
| `cargo run -p chronoxide-ingester --bin chronoxide-query -- ...` | `cargo run -p chronoxide-query-cli --bin chronoxide-query -- ...` |
| `cargo run -p chronoxide-ingester --bin chronoxide-storage-verify -- ...` | `cargo run -p chronoxide-query-cli --bin chronoxide-storage-verify -- ...` |
| `cargo test -p chronoxide-ingester --bin chronoxide-query` | `cargo test -p chronoxide-query-cli --bin chronoxide-query` |

This is a Cargo package/build-target migration only. Executable names, command
line arguments, output schemas, query behavior, and verifier behavior are
unchanged.

## Data And Operational Compatibility

This is a Rust source/API migration, not a storage-format migration. Existing
capture and segment corpora require no replay, conversion, or backfill.

Moved logging events intentionally retain their previous
`chronoxide_core::otlp_capture`, `chronoxide_core::source`,
`chronoxide_core::util`, and `chronoxide_core::telemetry::logger` tracing
targets. Existing `RUST_LOG` filters therefore continue to select them.
