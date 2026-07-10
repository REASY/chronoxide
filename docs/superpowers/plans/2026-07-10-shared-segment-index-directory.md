# Arc-Shared Segment Index Directory Implementation Plan

> **For Codex:** Execute each task in order. Preserve the existing dirty
> worktree and stage only the paths named in this plan.

**Goal:** Eliminate deep cloning of immutable version-6 segment-index directory
maps whenever a cached `SegmentIndexReader<File>` is acquired.

**Architecture:** Split the reader's immutable directory metadata from its I/O
handle. Construct one `SegmentIndexDirectory`, store it behind `Arc`, and clone
only the `Arc` when cloning a file-backed reader. Keep all disk bytes, parsing,
validation, lookups, public APIs, and query results unchanged.

**Tech stack:** Rust standard library (`Arc`, `BTreeMap`, `File`), Cargo tests,
the release `chronoxide-query` benchmark, and macOS `/usr/bin/time -l`.

---

## Task 1: Capture the pre-change baseline

**Files:**

- Read only: `data/smoke/segments-replay-001`
- Output only: `/tmp/chronoxide-index-arc-before-*`

**Step 1: Verify the focused index tests are green before modification**

Run:

```sh
cargo test -p chronoxide-core storage::index::tests -- --nocapture
cargo test -p chronoxide-core --test postings_index -- --nocapture
```

Expected: PASS.

**Step 2: Build the canonical release query binary**

Run:

```sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query
```

Expected: exit 0.

**Step 3: Warm the OS page cache without recording the result**

Run the exact-count and missing-metric queries once with
`--benchmark-repeats 2`, writing reports under `/tmp`.

**Step 4: Capture repeated fresh-process baselines**

Run each query in seven separate processes with `--benchmark-repeats 31`:

```promql
{__name__="go_gc_duration_seconds_count"}
{__name__="definitely_missing_metric"}
```

Write reports to `/tmp/chronoxide-index-arc-before-{count,missing}-NN.md`.
Capture `/usr/bin/time -l` for at least one representative invocation. Record
session-first duration, warm mean/median, result counts, and maximum RSS.

Expected: all repeated runs return identical series/sample counts. Preserve the
raw reports for the after comparison.

**Step 5: Capture the direct cache-hit acquisition baseline**

Run one process with the missing-metric `--query` argument repeated eleven
times and `--benchmark-repeats 1`. The CLI opens one query session per explicit
query, so the first result includes footer parsing and the following ten results
measure cached-reader acquisition plus routing lookup. Record each detailed
duration and peak RSS.

## Task 2: Add a failing shared-ownership regression test

**Files:**

- Modify: `chronoxide-core/src/storage/index.rs` test module

**Step 1: Add `segment_index_reader_clones_share_immutable_directory`**

Build a small index containing:

- one exact-postings entry;
- one label-value FST;
- one label-value time range;
- one metric-series range;
- routing metadata.

Write it to `tempfile::tempfile`, open `SegmentIndexReader<File>`, clone it, and
assert:

```rust
assert!(Arc::ptr_eq(&reader.directory, &clone.directory));
```

Then query both readers and assert identical exact postings, routing metadata,
label values, label time ranges, and metric-series ranges.

**Step 2: Run the new test and observe RED**

Run:

```sh
cargo test -p chronoxide-core \
  storage::index::tests::segment_index_reader_clones_share_immutable_directory \
  -- --exact --nocapture
```

Expected: compilation fails because `SegmentIndexReader` has no shared
`directory` field yet. Confirm that this is the only relevant failure.

## Task 3: Implement the smallest Arc-sharing refactor

**Files:**

- Modify: `chronoxide-core/src/storage/index.rs`

**Step 1: Add the immutable directory type**

Import `std::sync::Arc` and move these fields into private
`SegmentIndexDirectory`:

```rust
exact_postings
label_value_fsts
label_value_time_ranges
metric_series_ranges
routing_index
```

Do not derive `Clone` for the directory; cloning must occur only through `Arc`.

**Step 2: Construct the directory once in `open`**

Keep the existing footer decoding loop and required-metric-range error exactly
as written. Wrap the validated fields in one `Arc<SegmentIndexDirectory>`.

**Step 3: Route all lookups through the directory**

Mechanically replace direct field access with `self.directory.<field>`. Do not
change return types, lookup order, byte reads, or error messages.

**Step 4: Make reader cloning O(1) for metadata**

Retain `self.reader.try_clone()?` and replace all map clones with:

```rust
directory: Arc::clone(&self.directory),
```

**Step 5: Run the new test and observe GREEN**

Run the exact command from Task 2. Expected: PASS.

## Task 4: Verify correctness and review the diff

**Files:**

- Verify: `chronoxide-core/src/storage/index.rs`
- Verify: `chronoxide-core/tests/postings_index.rs`
- Verify: `chronoxide-core/tests/promql_query.rs`
- Verify: `chronoxide-ingester/src/bin/chronoxide-query.rs`

**Step 1: Run formatter and focused tests**

```sh
cargo fmt --all -- --check
cargo test -p chronoxide-core storage::index::tests -- --nocapture
cargo test -p chronoxide-core --test postings_index -- --nocapture
```

Expected: PASS.

**Step 2: Run query regression suites**

```sh
cargo test -p chronoxide-core --test promql_query -- --nocapture
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
```

Expected: PASS. If an unrelated pre-existing dirty-tree failure appears, stop
and report it rather than normalizing it as part of this change.

**Step 3: Inspect only the scoped diff**

```sh
git diff -- chronoxide-core/src/storage/index.rs \
  docs/superpowers/specs/2026-07-10-shared-segment-index-directory-design.md \
  docs/superpowers/plans/2026-07-10-shared-segment-index-directory.md
git diff --check
```

Expected: only the intended ownership refactor, test, and documentation; no
on-disk constants or encoding functions change.

## Task 5: Repeat the benchmark and decide whether item 1 helped

**Files:**

- Output only: `/tmp/chronoxide-index-arc-after-*`

**Step 1: Rebuild release**

```sh
cargo build --release -p chronoxide-ingester --bin chronoxide-query
```

**Step 2: Repeat the identical warmup and seven fresh-process runs**

Use the exact query order, repeat count, time range, limits, and page-cache
state from Task 1. Write reports under
`/tmp/chronoxide-index-arc-after-{count,missing}-NN.md`.

**Step 3: Repeat the direct cache-hit acquisition measurement**

Repeat Task 1 Step 5 with the same eleven missing selectors. Compare the median
of queries 2 through 11. Do not use `routing_open_delta` as the optimized timer:
that counter ends before `try_clone_reader` and should remain approximately
flat.

**Step 4: Enforce result equivalence**

Compare every before/after run's result-series and result-sample counts. Any
difference is a correctness regression and blocks acceptance regardless of
latency.

Also run the real-corpus verifier:

```sh
./target/release/chronoxide-query \
  --segments-dir data/smoke/segments-replay-001 \
  --sample-limit-per-kind 2 \
  --verify-readbacks \
  --output /tmp/chronoxide-index-arc-after-readback.md
```

Require zero mismatches. This covers labels, timestamps, values, and typed
projection metadata that equal result counts would not detect.

**Step 5: Report performance**

For session-first and warm durations, report median, min/max, and percentage
change across fresh processes. Compare representative maximum RSS and relevant
read-profile counters. Explicitly distinguish:

- improvement from avoiding the deep clone;
- unchanged initial footer parsing;
- unchanged warm decoding.

## Task 6: Review and commit the implementation

**Files:**

- Commit: `chronoxide-core/src/storage/index.rs`
- Commit: `docs/superpowers/plans/2026-07-10-shared-segment-index-directory.md`

Request a focused code review for correctness, scope, and test adequacy. Resolve
all findings, rerun the affected verification, and commit only the named files:

```sh
git add -- chronoxide-core/src/storage/index.rs \
  docs/superpowers/plans/2026-07-10-shared-segment-index-directory.md
git commit -m "perf: share segment index directories"
```

Do not begin projected-label caching until the item-1 benchmark result has been
reported and accepted.
