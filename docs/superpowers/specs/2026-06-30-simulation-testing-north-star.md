# Simulation Testing North Star

This document captures the long-term testing direction for Chronoxide:
FoundationDB-style deterministic simulation adapted to an OTLP-native TSDB.

The goal is not to copy FoundationDB's architecture. The goal is to borrow the
core principle: correctness-critical code should run under a deterministic
simulator where time, I/O, scheduling, randomness, source messages, crashes,
and external services are controlled by a seed.

## Why This Matters

Chronoxide is moving toward a durable metrics database with:

- Kafka ingestion and file replay.
- Trusted capture timestamps.
- WAL checkpoints and recovery.
- Windowed head buffers.
- Segment publish and manifest authority.
- Query correctness through PromQL selectors.

Most serious failures will not be simple unit-test bugs. They will be timing,
ordering, crash, replay, or partial-I/O bugs:

- Crash after WAL append but before checkpoint.
- Crash after segment files are written but before manifest publish.
- Torn WAL records.
- Corrupt segment indexes or footers.
- Duplicate or replayed source records.
- Out-of-order datapoints around segment boundaries.
- Future-dated datapoints poisoning watermarks.
- Kafka partition stalls and cancellation races.

Simulation gives us a way to test these states deliberately and repeatedly.

## Core Principle

All external and nondeterministic behavior must be injectable at the boundary.

Production implementations use real OS, Kafka, wall clock, random IDs, and
Tokio scheduling. Simulation implementations use deterministic, seeded models.

The same storage and ingestion logic should run in both modes.

## Existing Good Direction

We already have some of the right seams:

- `MessageSource` abstracts Kafka vs file/capture sources.
- `CaptureRecord` now carries trusted `captured_at_ms`.
- `SegmentIdProvider` abstracts random vs deterministic segment IDs.
- Storage already has many format-level tests for WAL, segments, indexes, and
  query behavior.

These are the first steps toward simulation. They should remain small,
explicit, and dependency-injected.

## Required Deterministic Boundaries

### Clock

Clock access must be explicit:

- Wall time for capture and policy decisions.
- Monotonic time for latency measurements.
- Sleep/timer behavior for loops and cancellation.

Simulation needs a virtual clock that can advance deterministically and trigger
timers without real waiting.

### Randomness and IDs

All random or unique IDs must come from injectable providers:

- Segment IDs.
- Future shard/run IDs.
- Any randomized test data generation.

Production can use OS randomness. Simulation must use a seed and produce stable
replayable output.

### File System

Storage correctness depends on file-system behavior. Any path covered by
simulation should access files through a small storage I/O boundary:

- create/write/flush/sync
- rename
- read
- list
- delete
- metadata
- corruption injection
- partial write and short read behavior

Production uses the real local file system. Simulation uses an in-memory or
recording file system with fault injection.

### Runtime and Scheduling

The ingestion loop currently mixes real sleeps, cancellation, and source polling.
Simulation should control:

- task scheduling order
- cancellation points
- timer delivery
- source availability
- pause/resume behavior

This does not require rewriting everything at once. Start by isolating storage
and single-threaded ingestion, then widen the runtime model.

### Source Input

Kafka should be modeled as a deterministic source stream:

- topic/partition/offset
- source timestamp metadata
- trusted `captured_at_ms`
- payload
- delivery order
- duplicates
- partition stalls
- EOF/pause/resume

The simulator should be able to generate streams and also replay captured files.

## First Simulation Target

Start with shard-local storage, not a distributed cluster.

The first simulator should drive this pipeline:

```text
SimSource -> OTLP decode -> label normalization -> WAL -> Head -> SegmentWriter -> Manifest
```

Then it should restart from the simulated persisted state and query through the
normal segment/head query path.

This gives high value early because it covers the core durability and query
contracts without requiring multi-node membership, Kafka group rebalancing, or
network simulation.

## Oracle Model

The simulator needs a simple in-memory oracle independent from the storage
implementation.

For the initially supported scope:

- Only Gauge/Sum number datapoints are persisted as PromQL float samples.
- Histogram, ExponentialHistogram, and Summary datapoints may be counted but are
  not yet part of query equality.
- The oracle maps canonical PromQL labelsets to samples by event timestamp.
- Duplicate timestamp behavior must match the production query merge rule.
- Rejected/quarantined datapoints must not appear in the oracle.

Every simulated query compares production results to this oracle.

## Fault Model

Faults should be deterministic and seed-driven. The harness should be able to
inject faults at named cut points:

- Before/after WAL append.
- Before/after WAL checkpoint.
- Before/after head record.
- Before/after segment temp file write.
- Before/after chunk index write.
- Before/after footer write.
- Before/after temp directory publish/rename.
- Before/after manifest update.
- Before/after WAL truncation.

Fault types:

- crash/restart
- partial write
- torn WAL record
- file corruption
- missing file
- failed rename
- failed sync
- duplicate source record
- source stall
- cancellation

The first version can implement only crash/restart and torn WAL records. The
cut-point design should leave room for the rest.

## Invariants

Simulation tests should assert invariants, not incidental implementation
details.

Core invariants:

- Accepted durable samples are queryable after restart.
- Recovery returns a valid prefix or complete durable state; it never invents
  samples.
- A manifest-published segment is either readable and footer-valid, or detected
  as corrupt/missing.
- WAL replay stops at the first invalid record and preserves earlier valid
  records.
- WAL truncation never removes data not covered by manifest-published segments.
- Segment folder names are repeatable when deterministic segment IDs are enabled
  with the same seed, config, and record order.
- Query results match the oracle for supported PromQL selectors.
- Future-skew rejection never advances ingest watermarks.
- Replaying a capture with `captured_at_ms` preserves the original policy
  decision; current wall clock must not change it.
- Out-of-order samples within the supported window are queryable in timestamp
  order.

## Reproducibility Contract

Every simulation failure must print enough information to reproduce it:

```text
scenario=<name>
seed=<u64>
steps=<n>
fault=<cut-point/fault>
config=<short stable summary>
```

Example command shape:

```text
cargo test -p chronoxide-sim -- --scenario wal_crash_publish --seed 12345
```

If the simulator shrinks failing cases later, it should print both the original
seed and the minimized trace.

## Milestones

### Milestone 1: Deterministic Storage Harness

- Add a small simulation crate or module.
- Add seeded source generation for simple OTLP Gauge/Sum payloads.
- Use deterministic `SegmentIdProvider`.
- Compare query results against an in-memory oracle.
- Run without real Kafka or wall clock.

### Milestone 2: Crash/Recovery Cut Points

- Add named cut points around WAL, segment publish, manifest publish, and WAL
  truncation.
- Inject crash/restart at each cut point.
- Verify recovery and query oracle invariants.

### Milestone 3: Simulated File System Faults

- Introduce a storage I/O abstraction where needed.
- Add torn WAL records, missing files, corrupt segment files, and failed rename.
- Verify detection and recovery behavior.

### Milestone 4: Source and Replay Simulation

- Model Kafka-like partitions, offsets, duplicates, stalls, and cancellation.
- Replay captured records with `captured_at_ms`.
- Verify watermark and future-skew invariants.

### Milestone 5: Broader Scheduler Simulation

- Add deterministic task/timer scheduling for ingestion loops.
- Stress shutdown, flush, pause, cancellation, and source exhaustion races.

## Non-Goals For The First Version

- No full distributed cluster simulation.
- No Kafka protocol simulation.
- No byte-for-byte segment equality requirement.
- No complete PromQL engine oracle.
- No attempt to model every file-system behavior from day one.

The first version should prove the storage durability and query correctness
contracts. Once that is stable, widen the simulator.

## Design Bias

Prefer small explicit traits over a large framework:

- `Clock`
- `IdProvider`
- `Source`
- `StorageIo`
- `FaultInjector`

Each abstraction should be introduced only when a simulator test needs it. The
production path must remain simple and visible.

Simulation is not a replacement for unit tests, integration tests, smoke tests,
or real Kafka ingestion. It is the layer that makes rare crash and ordering
states cheap to test every day.
