# General chunk-payload read scheduler design

> **Archived historical record:** This document is retained for provenance and is not current authority. Consult the current contracts and code before relying on it.

## Status and scope

This document is the current implementation plan for a backend-independent
chunk-payload scheduler. It supersedes the Histogram-only experimental-flow
description as architecture authority; the dated reports under
`docs/experiments/iouring/` remain measurement evidence.

The first milestone covers payload reads from `chunks.bin` and
`ooo_chunks.bin` for Float, Int64, Histogram, ExponentialHistogram, Summary,
and compact typed scalar lanes. It does not change the on-disk format and does
not move symbols, series metadata, chunk indexes, postings, routing indexes, or
footers to io_uring. Those remain immutable positional reads.

The production default remains unchanged until the correctness and performance
acceptance gates in the implementation goal pass.

## Current read inventory

All hot query payload reads ultimately use a `ChunkPayloadRead { offset, len }`
and `ChunkPayloadBatch`:

| Consumer | Planning today | Fetch today | Decode today |
|---|---|---|---|
| Float and Int64 selectors | `query_normalized_with_context` filters chunk-index entries and charges the query budget | one coalesced batch per segment | full `ChunkRecord`, then kind/range filtering |
| Histogram and ExponentialHistogram virtual `_count` / `_sum` | the same generic selector planner, with scalar-lane read length and range-cache classification | one coalesced cache-miss batch per segment | scalar lane, or full typed fallback, with cache admission |
| Histogram and ExponentialHistogram virtual buckets | the generic selector planner | one coalesced batch per segment | full typed record and projection |
| Summary `_count` / `_sum` | the generic selector planner, including scalar-lane selection | one coalesced cache-miss batch per segment | scalar lane or full Summary fallback |
| Summary quantiles | the generic selector planner | one coalesced batch per segment | full Summary record and projection |
| Native Histogram | `plan_native_typed_cross_segment_with_context` when the experimental flag is set | bounded batches spanning segment files | stable per-segment full-record decode |
| Native ExponentialHistogram | the same typed planner | a second, duplicated bounded cross-segment executor | stable per-segment full-record decode |
| Smoke/readback sampling | direct individual record reads | positional seek/read | full record |
| Explicit prefetch | chunk-index-derived byte range | positional range prefetch | no decode |

Smoke/readback sampling and explicit prefetch are not PromQL payload consumers
and are not migrated in the first milestone. Every PromQL payload consumer is.

The existing cross-segment implementation proves the useful separation, but
its plan metadata is native-histogram-specific and its group/execution code is
duplicated. The current `ChunkReadMode::Auto` also chooses a concrete backend
when the session reader is created. It cannot use planned span depth and can
therefore select io_uring for one-span work.

## Required ordering and error contract

The scheduler must not change observable query behavior:

1. Selector branches, segments, series, chunks, and decoded samples retain
   their existing logical order.
2. Query limits are charged during planning in exactly the existing order.
   Fetching never charges `QueryStats` and scheduler telemetry is profile-only.
3. Per-file ranges are coalesced with one immutable query-session maximum-gap
   setting. The production default and current experimental upper bound are
   both 4 KiB; values from 0 through 4 KiB are valid. A zero gap still merges
   overlapping or exactly contiguous ranges. The same setting governs the
   legacy and schema-7/8 planners and every forced backend.
4. A backend result is restored to request order before any decoder sees it.
5. Planning stops at the first error. Already planned work may be fetched and
   decoded before returning a later planning error only where that is the
   existing cross-segment contract. An earlier decode error takes precedence
   over a deferred later planning/read error.
6. No result group is published until every planned payload needed by that
   group has fetched and decoded successfully.
7. Missing, excess, short, or otherwise malformed backend results are errors.
   Touched chunk corruption is never converted into absence or a cache miss.
8. The range scalar cache retains its current eligibility, lookup, admission,
   allocation-failure, and lifetime behavior. Cache hits produce no physical
   request but retain their existing logical budget charge and profile
   classification.

## Architecture

The payload path is explicitly split into three phases:

```text
query/type planner
    -> ordered logical chunk requests + stable decode metadata
    -> per-file coalescing
    -> bounded scheduler group
    -> pread | io_uring decision and fetch
    -> ordered per-file ChunkPayloadBatch values
    -> type-specific stable decode
```

### Physical plan

A scheduler item owns:

- a stable item/segment ordinal;
- one immutable file handle;
- one existing `ChunkPayloadBatchPlan` produced by per-file coalescing.

A scheduler group owns ordered items and enforces:

- at most 32 segment/file items;
- at most 256 physical spans;
- at most 256 MiB of physical in-flight bytes.

Limits are checked before adding an item. A single item larger than a group
limit is executed alone, so progress is guaranteed without violating
correctness. It remains subject to query byte limits already charged during
logical planning.

The scheduler returns one ordered `ChunkPayloadBatch` per input item. It does
not decode chunks and does not know OTLP kinds or PromQL projections.

### Backend modes

- **pread** executes the same ordered physical plan with positional reads and
  is the semantic reference.
- **io_uring** uses the session's persistent ring, submits independent spans up
  to configured queue depth, collects completions, and restores request order.
  Forced io_uring initialization and I/O failures are never hidden.
- **auto** keeps both a pread executor and, when supported, a persistent
  io_uring executor. It chooses per scheduler execution from physical plan
  dimensions. It never guesses page-cache residency.

The initial conservative auto threshold is eight physical spans with a
configured queue depth of at least eight. Below either threshold, auto uses
pread. At or above both, auto uses io_uring when available. Boundary tests
cover seven, eight, and nine spans. The threshold is experimental and may be
adjusted only from real-corpus evidence. If auto cannot initialize io_uring, it
records unavailability and uses pread; forced io_uring returns the
initialization error.

The payload coalescing gap is validated before backend initialization and is
fixed for the lifetime of the shared `ChunkReader`. This prevents per-segment,
cross-segment, pread, and io_uring plans from silently using different physical
layouts. Increasing the experimental cap above 4 KiB requires a new explicit
amplification bound and real-corpus evidence; it is not an unrestricted tuning
knob.

Before paying the cross-segment planning-lifetime cost, auto also requires at
least eight time-overlapping segment candidates. This is a conservative
precheck, not the backend decision: the scheduler still chooses from the
coalesced physical span count. The precheck keeps known one- or two-segment
range evaluations on the established per-segment path, while a deep candidate
set that coalesces to fewer than eight spans still uses pread.

### Memory bounds

Ordinary `Vec<u8>` buffers are used initially. One scheduler execution owns no
more than the configured group byte limit plus small plan/result metadata. The
io_uring backend further limits submitted SQEs to queue depth. No registered
buffers or files are introduced.

The Linux benchmark recommendation is a finite 64 MiB `RLIMIT_MEMLOCK`. Rapid
creation of multiple rings previously produced `io_uring_setup(8) = ENOMEM`
under an 8 KiB limit. The query session must reuse one persistent ring instead
of creating a ring per request or scheduler group.

## Profile-only observability

Scheduler measurements live in `SegmentStoreQueryProfile`, not `QueryStats`:

- pread, io_uring, and auto decision counts;
- logical request and physical span counts;
- scheduler executions and backend submissions;
- SQEs submitted;
- submission-depth buckets, sum, and maximum;
- total physical bytes executed and peak bytes in one concurrent backend
  submission (one span for pread, up to queue depth for io_uring);
- payload wait/read duration;
- logical used and physical read bytes (existing fields);
- read/used amplification derived by reporting code.

The profile implements saturating accumulation and `delta_since` for every new
monotonic counter. Submission depth and peak in-flight bytes are session
high-water gauges, not subtractable counters: a delta reports the current
session high-water only when the interval contains a new scheduler execution,
and otherwise reports zero. Raw query schema v13 therefore names them
`session_submission_depth_high_water` and
`session_peak_in_flight_bytes_high_water`; consumers must not sum them across
runs or interpret them as interval increments. The separate monotonic counter
is named `total_physical_bytes_executed`, not `in_flight_bytes`, so consumers do
not mistake cumulative work for current memory. CLI reporting must expose the
fields without changing serialized `QueryStats`.

## Migration sequence

1. Introduce the shared scheduler and replace both duplicated native typed
   executors. Preserve the existing experimental flag and exact native tests.
2. Extract the generic selector's planning state from
   `query_normalized_with_context` into an ordered per-segment plan whose decode
   method contains the current cache and projection logic unchanged.
3. At session level, form bounded scheduler groups from those generic plans.
   This migrates Float, Int64, Summary, scalar lanes, and virtual Histogram /
   ExponentialHistogram projections together because they already share the
   generic planner and decoder.
4. Retain the legacy per-segment flow when the experiment is disabled. Forced
   backend modes remain available for A/B measurement.
5. Add conservative auto selection. Auto affects scheduler executions only;
   shallow work remains on the established per-segment/pread path until the
   acceptance benchmarks justify broader opt-in.
6. Consider bounded double-buffer pipelining only after batching correctness
   and performance are independently established.

Batching across PromQL range evaluation timestamps is explicitly excluded from
this milestone. Selector branches may be combined only after tests prove that
budget and error order are unchanged.

## Verification plan

Focused tests cover every payload kind, scalar-lane cache enabled/disabled,
exact results and `QueryStats`, byte/chunk/series/projected-series/sample
limits, empty and one-span plans, queue-depth and group-limit boundaries,
identical offsets in different files, overlapping/coalesced ranges, short and
missing results, out-of-order completion, initialization failure, and error
precedence between earlier decode corruption and later planning/read errors.

Broad gates are:

```sh
cargo test -p chronoxide-core
cargo test -p chronoxide-ingester --bin chronoxide-query
cargo test -p chronoxide-ingester --test source_level_e2e -- --nocapture
cargo fmt --all -- --check
git diff --check
```

Linux io_uring tests are also run explicitly with the feature enabled.

Real-corpus A/B uses one release binary and the corpus documented by the active
goal. The runner records page residency with `fincore` after
`POSIX_FADV_DONTNEED`, separates payload-page-evicted and warm schedules, and
states that neither operation flushes controller/NVMe cache. Raw artifacts stay
outside the repository; conclusions are recorded under
`docs/experiments/iouring/`.
