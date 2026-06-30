# Clock Model: Event-Time Storage, Ingest-Time Control

This document describes how the system should reason about time when ingesting
and querying OTLP metrics. The guiding model is:

**Event-time storage, ingest-time control.**

Event time is the timestamp inside the datapoint. Ingest time is the wall-clock
time observed by the system while ingesting. These serve different roles.

## Definitions

- Event time: `timestamp_ms` on each datapoint. This is the time the sample
  represents and determines where it belongs in storage.
- Ingest time: `now_ms` from a `Clock` (wall clock or test clock). This is used
  for orchestration and operational behavior.
- Capture time: `captured_at_ms` on a captured source record. This is local
  wall-clock time observed by Chronoxide when the transport message was
  accepted/captured. It is the trusted replay anchor for recorded traffic.
- Source timestamp: timestamp metadata provided by Kafka or another transport.
  This can be useful for diagnostics or fallback event-time extraction, but it
  is not trusted for replay safety or future-skew validation.
- Ingest watermark: the maximum event time accepted so far, tracked per
  partition and optionally aggregated (e.g., `min` across partitions). Used for
  read horizon.
- Persistence watermark (durable watermark): the maximum event time that is
  durably persisted (WAL checkpoint or flushed chunk/segment) per partition.
- Read horizon: the latest event time that is considered "complete enough" to
  answer queries. Typically `min(active_partition_watermarks) -
  lateness_tolerance_ms`.
- Partition activity: last ingest time per partition (`last_ingest_ms`),
  used to detect idle partitions.
- Wall clock: epoch time used for policy checks and watermark/lateness logic.
- Monotonic clock: used only for measuring durations/latency; never mix with
  event timestamps.

## Why Separate Event and Ingest Time

Event time and ingest time often diverge:

- Backfills and replays: ingest time is far ahead of event time.
- Delayed producers: event time may lag behind ingest time.
- Clock skew: event time can be slightly ahead of ingest time.

If we use ingest time to place samples or make retention decisions, we can
incorrectly evict valid data during backlog replay or return empty results for
queries. Storing by event time while using ingest time only for control avoids
that.

## Storage Rules

Event time must be used for all storage decisions:

- Head/segment assignment: based on event time boundaries.
- Timestamp compression: always uses event time deltas.
- Segment sealing: based on event time windows.

Ingest time must not be used to place or evict samples.

## Control Rules

Ingest time is used for control and operational decisions:

- Late data handling: compare `event_ms` against ingest watermark and allowed
  lateness.
- Ingest throughput control: time-based throttling or timeouts.
- Observability: measure lag as `now_ms - ingest_watermark` or
  `now_ms - read_horizon`.

## Clock Design

- `Clock::now_ms()` returns epoch milliseconds for policy and watermark logic.
- Use a monotonic clock (e.g., `Instant`) only for timing metrics, not for
  comparing to event timestamps.
- Keep clock logic outside storage. Storage takes event timestamps; control
  logic takes `now_ms`.

## Event-Time Validation Policy

Event time should be validated against ingest time to avoid poisoned watermarks
from bad clocks. The system should define bounds and a policy for out-of-window
events:

```
now_ms = trusted_policy_time_ms(record)

if event_ms > now_ms + max_future_skew_ms:
    reject/quarantine (do NOT advance ingest watermark)
elif event_ms < now_ms - max_backfill_ms:
    policy-dependent
else:
    accept and allow ingest watermark advance
```

`trusted_policy_time_ms(record)` is mode-specific:

- Live ingestion: the local wall clock at accept time.
- Captured replay: `record.captured_at_ms`, or an explicit trusted capture
  watermark stored with the recording.
- Synthetic/test replay: a test-controlled clock.

Kafka/source timestamps are not trusted policy time. They may be forged or
derived from a producer with a bad clock.

Policy choices for data older than `now_ms - max_backfill_ms`:

- Strict drop: reject and count as invalid. This protects resources but loses
  data during long downtime or backfill.
- Soft accept: ingest and store, but do not advance ingest watermark. This
  preserves data without breaking query completeness.
- Backfill mode: widen/disable `max_backfill_ms` while replaying a known
  capture; still keep the future-skew check anchored to trusted capture time or
  explicit capture watermarks to avoid poisoned clocks.

Watermarks must only advance on accepted, in-window event times. Expose metrics
for out-of-window events and optionally track a separate "raw max event time"
for debugging.

Future-skew handling should not silently drop data by default. Prefer to park
or quarantine out-of-window future samples (e.g., a future buffer or a
dead-letter stream) so they can be inspected or re-ingested when clocks are
corrected.

## Policy Abstraction (Optional)

If you want configurable behavior per mode (online vs replay vs backfill), a
small policy trait is a clean option:

```
enum PolicyDecision {
    AcceptAdvance,
    AcceptNoAdvance,
    Reject(RejectReason),
}

trait EventTimePolicy {
    fn evaluate(&self, event_ms: i64, now_ms: i64) -> PolicyDecision;
}
```

Example implementations:

- BoundsPolicy: apply `max_future_skew_ms` and `max_backfill_ms`.
- ReplayPolicy: accept all events or only enforce future skew.
- BackfillPolicy: accept old data but do not advance ingest watermark.

This keeps the ingest pipeline generic while allowing different replay modes in
tools like `chronoxide-core/examples/headbuffer_replay.rs`.

## Watermarks and Read Horizon

Track watermarks per partition:

```
ingest_watermark[p] = max(ingest_watermark[p], event_ms)  // accepted only
```

Define a read horizon for query completeness:

```
read_horizon = min(active_partition_watermarks) - lateness_tolerance_ms
```

Queries that ask for data beyond the read horizon should be treated as
potentially incomplete. Two policy options:

- Partial: clamp query end to `read_horizon` and mark response as partial.
- Strict: reject the request with a "lagging" error if it exceeds horizon.

### Idle Partitions

Using `min(ingest_watermarks)` can stall the read horizon if a partition
goes idle. Track `last_ingest_ms` per partition and define an idle timeout:

```
if now_ms - last_ingest_ms > idle_timeout_ms:
    mark partition inactive
```

Options to avoid stalled horizons:

- Active-only horizon: compute `min(ingest_watermark)` over active partitions
  only.
  If any partition is inactive, mark queries as partial.
- Synthetic watermark: for inactive partitions, use
  `now_ms - idle_skew_ms` as an effective watermark.

Both approaches should be explicit in query responses so clients understand
completeness guarantees.

Idle recovery behavior must be explicit. If a previously idle partition resumes
and emits events with `event_ms < synthetic_watermark`, the system must route
those samples to the late-arrival path. Do not "rewind" global horizons to
accommodate a straggler; that would block reads for all partitions.

Synthetic watermarks affect read horizon only. They must not force ingestion
rejections or advance ingest watermarks for that partition.

### Watermark Update Frequency

Updating watermarks on every sample can be expensive at high throughput.
Prefer batching:

- Per-batch: update with `batch_max_event_ms` once per ingest batch.
- Sampling: update every N samples.

This keeps watermarks accurate enough for control logic without hot-path
contention.

## Retention and Sealing

Retention should be anchored to event time:

```
cutoff_ms = persistence_watermark - head_retention_ms
head.evict_older_than(cutoff_ms)
```

The persistence watermark must advance frequently enough to enforce short head
retention. Do not tie it only to full segment sealing. Prefer a finer-grained
durability signal, such as:

- WAL checkpoints, or
- per-chunk/partial segment flush markers.

This allows `cutoff_ms` to move forward even when large segments are still
open.

Head window duration is aligned with `segment_duration` and currently set to
**1h** (see `docs/spec/storage_spec.md`).

Segment sealing should be based on event time progress, not ingest time:

```
if ingest_watermark >= segment_end_ms + lateness_tolerance_ms:
    seal segment
```

This allows the system to ingest historical data correctly after downtime while
keeping head bounded.

### Late Arrivals After Sealing

The sealing rule must define where late data goes. Do not reopen sealed
segments on the hot path. Options:

- Late buffer / WAL: write late samples into a separate buffer keyed by segment
  window; queries read both sealed segments and late buffers; background
  compaction merges them.
- Backfill segments: write late data into dedicated backfill segments and let
  compaction merge by time range.
- Reject with metrics: only if strict data loss is acceptable.

The chosen path should be explicit; otherwise late data either vanishes or
forces unbounded head ranges.

## Recommended Clock Usage

Storage layers should not require a clock for correctness; they operate on event
timestamps. The ingest pipeline should be responsible for:

- reading `now_ms` from a `Clock`,
- tracking watermarks,
- applying lateness policy,
- making sealing/retention decisions.

This separation keeps storage deterministic and makes replay/testing easier by
swapping in a test clock.

## Clock Trait and Modes

A minimal clock abstraction keeps ingest logic testable and allows replay mode
to operate on virtual time:

```
trait Clock {
    fn now_ms(&self) -> i64;
}
```

### Normal Mode (Wall Clock)

- `now_ms` comes from a wall clock.
- Policies enforce both `max_future_skew_ms` and `max_backfill_ms`.
- Lag metrics use `now_ms - ingest_watermark` and are meaningful for ops.

### Replay Mode (Captured Traffic)

Captured Kafka/file replay must use the trusted capture timeline, not the
current wall clock and not event time:

```
now_ms   = record.captured_at_ms
event_ms = datapoint_time_ms(...)
decision = policy.evaluate(event_ms, now_ms)
```

Replay preserves the safety decision that would have been made when the
message was originally captured. A future-dated datapoint cannot become valid
just because replay observes its event time.

If a capture file spans multiple partitions, replay should either:

- preserve each partition's captured record order and use per-record
  `captured_at_ms`, or
- drive policy from explicit capture watermark records if the capture format
  adds them later.

### Replay Mode (Synthetic/Event-Driven Clock)

Wall-clock `now_ms` will make replayed data look too old. Use a replay clock
that advances with event time only for trusted synthetic replays or
non-adversarial backfills where the replay input is already validated:

```
struct ReplayClock {
    now_ms: i64,
}

impl ReplayClock {
    fn observe(&mut self, event_ms: i64) {
        if event_ms > self.now_ms {
            self.now_ms = event_ms;
        }
    }
}

impl Clock for ReplayClock {
    fn now_ms(&self) -> i64 {
        self.now_ms
    }
}
```

Replay mode guidance:

- Do not use event-driven replay clocks for captured production traffic unless
  a separate trusted future-skew policy has already been applied.
- For synthetic/offline backfills, call `ReplayClock::observe(event_ms)` before
  policy evaluation if that is the intended model.
- Keep `max_future_skew_ms` enforced to avoid poisoned clocks.
- Widen/disable `max_backfill_ms`, or use a replay policy that accepts old data.
- If replay input can be out of order, either drive `now_ms` from explicit
  watermark markers (preferred) or disable "past" checks in replay so older
  events do not spuriously fail validation.
- For parallel replay, avoid a single global mutexed clock. Use per-partition
  clocks in the ingest loop and advance a global replay horizon only when all
  partitions reach a time window (barrier-style coordination).

## Example Flow (per partition)

```
now_ms      = clock.now_ms()
event_ms    = datapoint_time_ms(...)

decision = policy.evaluate(event_ms, now_ms)

if decision == Reject:
    reject/quarantine
else:
    if decision == AcceptAdvance:
        ingest_watermark = max(ingest_watermark, event_ms)

if event_ms falls into a sealed segment range:
    write to late-arrival path (late buffer / backfill segment)
else:
    head.record_sample(event_ms, value)

cutoff_ms = persistence_watermark - head_retention_ms
head.evict_older_than(cutoff_ms)

if ingest_watermark >= segment_end_ms + lateness_tolerance_ms:
    seal segment
```

## Operational Signals

Expose these metrics to detect lag and partial reads:

- `ingest_watermark_ms` (per partition and global min)
- `persistence_watermark_ms` (per partition and global min)
- `ingest_lag_ms = now_ms - ingest_watermark`
- `read_horizon_ms`
- `read_lag_ms = now_ms - read_horizon_ms`
- `partition_last_ingest_ms` and `partition_active` (idle detection)

These allow alerting when the system is behind and explain partial responses.

## Query Semantics for Partial Coverage

When some partitions are inactive or lagging, "partial" responses can mislead
aggregate queries. Recommendations:

- For SUM/COUNT aggregations, treat incomplete time slices as unknown (null/NaN)
  rather than returning a partial sum.
- For RANGE queries, optionally clamp results to `read_horizon` and mark the
  response partial; clients should surface the partial flag.
