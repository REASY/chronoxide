# Segment Index v7 Lazy Directory Implementation Plan

> **For Codex:** Execute each task in order with red-green tests. Preserve the
> existing v6 corpus, user smoke artifacts, and unrelated dirty files.

**Goal:** Replace the version-6 millions-entry segment-index footer with a fixed
root trailer and lazy, checksummed exact-directory pages, then verify the change
on a deterministic full replay.

**Architecture:** Keep every existing index payload encoding. Add a version-7
container with direct root locators, 16 KiB sorted exact-directory pages, a
compact auxiliary directory, and immutable positional-read backing shared by
query sessions. Lazy metadata failures propagate as I/O errors.

**Tech stack:** Rust 2024, `crc32c`, platform `FileExt` positional reads,
`Arc`/`OnceLock`, Cargo tests, deterministic capture replay, and the release
`chronoxide-query` benchmark.

---

## Task 1: Preserve the v6 baseline

**Files/artifacts:**

- Read: `data/smoke/segments-replay-001`
- Read: `chronoxide-ingester/config/dc/sg/metric_smoke_replay.toml`
- Create only under: `data/perf/segment-index-v7/`

**Step 1: Build and test the current v6 reader**

```sh
cargo test -p chronoxide-core --test postings_index -- --nocapture
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
cargo build --release -p chronoxide-ingester \
  --bin chronoxide-query --bin chronoxide-ingester
```

Expected: PASS.

**Step 2: Preserve matching binaries and provenance**

Copy the release binaries into `data/perf/segment-index-v7/bin-v6/`. Record the
current commit, binary SHA-256 values, corpus path, corpus size, and index header
version in a benchmark manifest under the same artifact directory. Because the
baseline intentionally includes the user's tracked dirty changes, also record
the SHA-256 of `git diff --binary`; after committing v7, require the remaining
dirty diff hash to match before building the v7 binaries.

**Step 3: Freeze the replay configuration**

Copy the current dirty replay configuration into the artifact directory and
change only `segments_dir` for the future v7 corpus. Convert both `replay_from`
and `segments_dir` to canonical absolute paths, record the command working
directory, and record the resulting SHA-256. Do not edit or stage the user's
existing smoke configuration.

## Task 2: Add the v7 codec module and failing golden tests

**Files:**

- Create: `chronoxide-core/src/storage/index/v7.rs`
- Modify: `chronoxide-core/src/storage/index.rs`

**Step 1: Add format constants and test-only expected-byte builders**

Declare the exact constants from storage specification section 15.1: header,
trailer, locator, exact header/descriptor/page/record, and auxiliary
header/record sizes and magic/version values.

**Step 2: Add failing tests**

- `segment_index_v7_minimal_golden_bytes`
- `segment_index_v7_is_deterministic_across_insertion_order`
- `segment_index_v7_writes_routing_first`
- `segment_index_v7_codec_rejects_v6_header`

The minimal golden test must inspect all header/trailer fields, zero padding,
CRC fields, required locators, and exact final file length. The version-rejection
test targets the low-level v7 header decoder; end-to-end reader rejection is
added after the reader exists.

**Step 3: Run RED**

```sh
cargo test -p chronoxide-core segment_index_v7_ -- --nocapture
```

Expected: tests fail because the writer still emits version 6.

## Task 3: Implement deterministic v7 writing

**Files:**

- Modify: `chronoxide-core/src/storage/index.rs`
- Modify: `chronoxide-core/src/storage/index/v7.rs`

**Step 1: Implement checked layout planning**

Compute routing, metric, exact-postings, auxiliary-payload, exact-directory,
exact-page, auxiliary-directory, and trailer regions before writing. Reject all
count, multiplication, addition, and platform-size overflow.

**Step 2: Implement exact page construction**

Use one reusable 16 KiB page buffer. Encode 409 records per full page, strict
key order, zero padding, and a CRC32C over the complete page. Build only the
small descriptor vector; never retain every page.

**Step 3: Implement auxiliary directory construction**

Encode sorted unique FST and time-range records and a CRC over the complete
directory with its CRC field zeroed. Reject every zero-length auxiliary payload
so a non-empty auxiliary directory always has a non-empty payload region. Cover
an empty FST supplied through the public builder API with a focused test.

**Step 4: Write the fixed trailer**

Encode validated locators/counts, zero reserved bytes, file length, terminal
magic, and trailer CRC. Assert the actual emitted length equals the planned
length.

**Step 5: Run GREEN for writer tests**

Run the Task 2 command. Expected: PASS.

## Task 4: Implement immutable positional-read backing

**Files:**

- Modify: `chronoxide-core/src/storage/index.rs`
- Modify: `chronoxide-core/src/storage/index/v7.rs`
- Modify: `chronoxide-core/src/storage/segment/mod.rs`
- Modify: `chronoxide-core/src/storage/segment/query_types.rs`

**Step 1: Add `SegmentIndexReadAt`**

Implement `len` and `read_exact_at` for `File` using platform positional I/O
and for `Cursor<T: AsRef<[u8]>>` using checked slicing. Handle short reads,
interrupted reads, offset overflow, and EOF explicitly.

**Step 2: Share source and parsed root state**

Store the source and directory state in `Arc`. Keep `try_clone_reader` as a
compatibility method, but make it clone only Arcs and perform no file cloning.
Track physical index read calls and bytes by root, routing, exact-directory,
exact-page, auxiliary-directory, and payload category. Expose query-session
deltas in the CLI report without charging these metadata bytes to the existing
query payload budget.

**Step 3: Add a failing positional-source concurrency test**

`segment_index_read_at_supports_concurrent_file_ranges` shares one source across
sixteen threads and repeatedly reads different known ranges. Add it against the
new positional-source API before implementing that API, observe the expected
compile failure, and then make it pass without timing-based race assertions.

## Task 5: Implement fast open and lazy directories

**Files:**

- Modify: `chronoxide-core/src/storage/index.rs`
- Modify: `chronoxide-core/src/storage/index/v7.rs`
- Modify: `chronoxide-core/tests/postings_index.rs`

**Step 1: Add a counting random-access test source**

Write failing tests proving:

- `open` reads exactly the header and fixed trailer;
- routing miss does not read either directory;
- first exact lookup reads the exact header/descriptors and one page;
- later exact lookups read only their selected page;
- label discovery initializes only the auxiliary directory.

**Step 2: Implement root validation**

Validate all header/trailer fields, CRC, required/optional locator rules,
checked bounds, non-overlap, exact counts, and reserved zeros before publishing
the reader.

**Step 3: Implement exact-directory lazy initialization and lookup**

Use `OnceLock` for the parsed descriptor table. Validate the complete directory
CRC, descriptor ordering/ranges/counts, page CRC/header/padding, record ordering,
descriptor first/last agreement, time bounds, and posting-region containment.

**Step 4: Implement auxiliary lazy initialization**

Validate its CRC, sizes, supported kinds, strict ordering, uniqueness, time
bounds, and auxiliary-payload containment.

**Step 5: Add the end-to-end cloned-reader concurrency test**

`segment_index_v7_cloned_readers_support_concurrent_random_access` opens one
v7 file-backed reader, creates sixteen clones, and repeatedly reads different
routing, metric, and exact payloads.

**Step 6: Implement full decode for tests/tools**

Make `read_segment_indexes` enumerate all v7 pages and auxiliary records so
round-trip tests continue to compare the full logical `SegmentIndexes` value.

## Task 6: Propagate fallible lazy metadata through queries

**Files:**

- Modify: `chronoxide-core/src/storage/segment/query_context.rs`
- Modify: `chronoxide-core/src/storage/segment/query_helpers.rs`
- Modify: `chronoxide-core/src/storage/segment/query_reader.rs`
- Modify: `chronoxide-core/src/storage/segment/promql_lowering.rs`
- Modify: relevant tests under `chronoxide-core/tests/`

**Step 1: Add failing corruption-propagation tests**

Cover equality, regex, negative matcher, prewarm, prefetch, and label discovery.
A corrupt touched page must return `InvalidData`, never an empty result or a
segment-prune statistic.

**Step 2: Make lazy APIs fallible**

Change exact metadata, label-time-range, label-name, and label-value-presence
APIs to return `io::Result`. Replace ambiguous pruning results with an explicit
query-vs-skip plan inside an outer I/O result.

**Step 3: Retain validated posting locators**

Avoid a second directory-page lookup when reading a posting selected during
planning. Keep query budgeting and profile accounting unchanged.

**Step 4: Run query-focused suites**

```sh
cargo test -p chronoxide-core --test postings_index -- --nocapture
cargo test -p chronoxide-core --test metadata_discovery -- --nocapture
cargo test -p chronoxide-core --test segment_query -- --nocapture
cargo test -p chronoxide-core --test promql_query -- --nocapture
```

Expected: PASS.

## Task 7: Complete corruption and version coverage

**Files:**

- Modify: `chronoxide-core/src/storage/index/v7.rs`
- Modify: `chronoxide-core/tests/postings_index.rs`
- Modify: `chronoxide-core/src/storage/segment/layout.rs`
- Modify: `chronoxide-core/src/storage/segment/tests.rs`

**Step 1: Add the corruption matrix**

Test truncation at every structural boundary; bad magic/version/flags/reserved;
CRC mismatch; file-length mismatch; missing/partial/overlapping locators;
count-size overflow; malformed descriptor/page/record counts; unsorted and
duplicate keys; key-range disagreement; non-zero padding; invalid time ranges;
and payload ranges outside their declared region.

**Step 2: Bump the tracked segment schema**

Change segment footer schema from 4 to 5 and assert old schema rejection.

**Step 3: Run full verification**

```sh
cargo fmt --all -- --check
cargo test -p chronoxide-core
cargo test -p chronoxide-ingester --bin chronoxide-query -- --nocapture
cargo test -p chronoxide-ingester --test source_level_e2e -- --nocapture
git diff --check
```

Expected: PASS, aside from explicitly ignored external-tool golden tests.

## Task 8: Preserve v7 binaries and prepare the replay handoff

**Files/artifacts:**

- Create only under: `data/perf/segment-index-v7/`

**Step 1: Build matching v7 binaries**

Build release query and ingester binaries, copy them to `bin-v7`, and record
commit/binary hashes.

**Step 2: Verify a small generated segment before full replay**

Run a focused source-level fixture or bounded replay into an artifact-only v7
directory. Validate its footer, verify readbacks, and inspect the encoded
header/trailer version and lazy-read counters.

**Step 3: Give the user the full replay command**

The command must use the frozen config, write only to the new v7 artifact
directory, and leave the v6 corpus untouched. Estimated runtime is 40–60
minutes. Do not start the expensive replay until code and focused validation
are green and the user has been told exactly what will run.

Run the ingester from a dedicated directory below the v7 artifact root and
capture stdout/stderr there. The ingester writes timestamped ingestion-stat
reports relative to its process working directory, independent of
`segments_dir`; running from the repository root would create unrelated root
artifacts.

Before handoff, require at least 50 GiB free on the target filesystem, verify
the capture manifest hash and 13,000,000-message count, record the expected v6
segment-ID list, and abort if the v7 output path already exists or aliases the
v6 corpus.

## Task 9: Full replay equivalence and A/B benchmark

After the user authorizes/runs the replay:

1. Match deterministic segment IDs and metadata totals.
2. Hash-compare every non-index payload and each manifest file for matching
   segments.
3. Stream both index formats and compare a keyed digest inventory for every
   retained payload. Use `(kind, label_name_sym, label_value_sym, len, hash)`
   for exact postings and the corresponding logical key for routing,
   metric-range, FST, and label-time-range payloads. Require payload bytes to
   match; only container headers, directories, and absolute locators may differ.
4. Run both matching query binaries with footer validation and real readback
   verification; require zero mismatches.
5. Run timing separately with footer validation disabled; whole-file footer
   validation reads and hashes `indexes.puffin` and would mask fast-open costs.
6. Warm each corpus, then alternate v6/v7 fresh processes across seven runs per
   query.
7. Measure missing metric, `go_goroutines`, count projection, regex, repeated
   fresh sessions, warm controls, read bytes, and RSS.
   Use v7 physical-read counters and calculate v6 footer bytes directly from its
   trailer because the preserved v6 CLI reports total file size, not directory
   bytes actually read.
8. Report medians, min/max, percent changes, and any regressions before keeping
   the format.
