# Arc-Shared Segment Index Directory Design

**Date:** 2026-07-10  
**Status:** Approved for implementation

## Context

`SegmentIndexReader::open` reads the complete version-6 `indexes.puffin`
footer and materializes its immutable directory into three `BTreeMap`s plus
the metric-series-range and routing locators. `SegmentReader` caches that
reader, but the first index use in each query session calls
`try_clone_reader`, which currently deep-clones the three maps.

The current replay corpus contains approximately 3.60 million footer entries
occupying 150.9 MiB on disk. Rebuilding those immutable maps for each acquired
reader adds allocation, memory-copy, and transient-RSS cost without changing
query results.

## Goals

- Parse and validate a segment index directory once per cached segment reader.
- Share all immutable directory metadata across cloned readers in O(1).
- Preserve every existing on-disk byte and version-6 validation rule.
- Preserve routing, exact-postings, FST, label-time-range, and metric-range
  behavior.
- Measure the change on the real replay corpus before proceeding to the next
  optimization.

## Non-goals

- Redesigning the `indexes.puffin` footer or bumping its format version.
- Making footer parsing lazy.
- Changing postings or routing encodings.
- Changing query semantics, result ordering, limits, or statistics.
- Changing the underlying file-access strategy. `File::try_clone` remains in
  place; positional I/O or independently opened file descriptions are separate
  concurrency work.

## Design

Introduce one immutable directory object:

```rust
struct SegmentIndexDirectory {
    exact_postings: BTreeMap<(u32, u32), SegmentIndexDirectoryEntry>,
    label_value_fsts: BTreeMap<u32, SegmentIndexDirectoryEntry>,
    label_value_time_ranges: BTreeMap<u32, SegmentIndexDirectoryEntry>,
    metric_series_ranges: SegmentIndexDirectoryEntry,
    routing_index: Option<SegmentIndexDirectoryEntry>,
}
```

`SegmentIndexReader` owns its I/O handle separately from an
`Arc<SegmentIndexDirectory>`:

```rust
pub struct SegmentIndexReader<R> {
    reader: R,
    directory: Arc<SegmentIndexDirectory>,
}
```

`SegmentIndexReader::open` continues to read and validate the complete footer.
It builds `SegmentIndexDirectory` once and wraps it in an `Arc`.

`SegmentIndexReader<File>::try_clone_reader` continues to clone the file handle
but uses `Arc::clone` for the directory. All directory lookup methods read
through `self.directory`; no public API or result changes.

One `Arc` is preferred over independently wrapping each map because the five
directory components form one validated snapshot. It also creates a clean
boundary for a future lazy/on-disk directory implementation.

## Correctness and concurrency

The directory has no mutation after `open`, so sharing it does not add locking
or observable state. Required metric-range validation still occurs before the
directory is published. Unknown blob kinds remain ignored exactly as before.

The change does not claim to make seek-based reads concurrently safe across
cloned `File` handles. On Unix, `File::try_clone` may share a file offset, while
the reader uses `seek` plus `read_exact`. That pre-existing issue is explicitly
outside this performance change and should later be addressed with positional
reads or independently opened file descriptions.

## Test strategy

Add a unit test that fails with the current implementation and proves:

1. A cloned file-backed reader shares the exact same directory allocation via
   `Arc::ptr_eq`.
2. The original and clone return identical exact postings, routing metadata,
   label values, label time ranges, and metric-series ranges.

Retain and run the existing segment-index round-trip/corruption tests and the
PromQL query integration suite. Benchmark acceptance additionally requires
identical result-series and result-sample counts before and after the change.

## Benchmark strategy

Use `data/smoke/segments-replay-001` and the release
`chronoxide-query` binary. Measure queries independently:

- exact count-projection workload:
  `{__name__="go_gc_duration_seconds_count"}`
- non-projected gauge control:
  `{__name__="go_goroutines"}`
- routing-only miss:
  `{__name__="definitely_missing_metric"}`

Record first-run duration, warm mean/median, result counts, and peak RSS. The
existing index-open profile counter stops before `try_clone_reader`, so it is an
unchanged footer-parse control rather than a measurement of this optimization.

For the direct signal, pass the same missing selector multiple times with one
benchmark repeat. The CLI creates a new query session for each explicit query:
the first query fills the store cache, while subsequent queries acquire the
cached index reader without opening symbols, series, or chunks. Warm repeats
inside one query session are only a no-regression control because they already
reuse that session's reader.

Run the real-corpus readback verifier as the value-level correctness guard; the
benchmark's series/sample counts alone are not sufficient to detect wrong
labels, timestamps, values, or metadata.

## Expected outcome

First-run and fresh-session latency plus transient memory should improve. The
initial footer read and initial `BTreeMap` construction remain, so this change
cannot deliver the larger cold-open gains expected from a future fixed-locator,
lazy on-disk directory. Warm sample decoding should remain statistically flat.
