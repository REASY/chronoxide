# Segment-flush lifetime ordering result

**Status:** promoted. Schema 7/8 segment sealing now serializes and releases
the complete selector-index inventory before building the cold-series plan.
The default Schema 8 writer also skips the legacy v1 chunk-range scratch that
its v3 series writer does not consume.

## Decision

Promote the lifetime reordering.

On the accepted 250,000-message replay prefix:

- Heaptrack requested-live peak fell by 230,697,426 bytes
  (220.010 MiB, 5.1374%);
- all 34 storage files and 972,969,365 corpus bytes were byte-identical;
- replay counters matched the accepted calibration;
- footer validation passed; and
- independent readbacks executed 40/40 with zero skips, isolation skips, or
  mismatches.

The global peak moved earlier in the flush rather than falling by the complete
size of the released index inventory. This result therefore removes one
material overlap and exposes the next allocation owner; it is not evidence
that seal memory work is complete.

## Change under test

The former seal sequence retained exact postings, label-value FSTs,
label-value time ranges, metric ranges, and routing metadata while
`SeriesColdV2Plan` assembled `series.bin`. The replacement:

1. finalizes symbols, series rows, postings, and time ranges;
2. derives all remaining selector indexes;
3. writes `indexes.puffin`, consuming and releasing those indexes;
4. writes `symbols.bin`; and
5. assembles `series.bin` and `chunk_index.bin`.

The files are independent siblings. The authenticated index writer validates
against immutable finalized symbols and series rows; it does not read
`series.bin`. The v3 series writer reads the same immutable rows and chunk
locators; it does not read `indexes.puffin`.

Schema 7/8 series assembly explicitly ignores the legacy
`SeriesEntry::chunk_index` range and emits inline or v2-overflow locators.
Those schemas now avoid `chunk_index_ranges()` entirely. Schema 6 still
computes, validates, and copies its required v1 ranges inside a bounded scope.

This changes neither persisted bytes nor storage semantics, so no storage
format version or `storage.md` update is required.

## Memory evidence

The control is the frozen current-HEAD trace from the promoted indirect-sort
result.

| Heaptrack measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Requested-live maximum | 4,490,537,696 B | 4,259,840,270 B | -230,697,426 B (-5.1374%) |
| Peak time | 85.818 s | 78.930 s | shifted earlier |
| Allocation calls | 249,066,672 | 249,066,783 | +111 (immaterial) |
| Final leaked bytes | 414,748 B | 414,748 B | unchanged |

The change is principally a lifetime win, so it is expected not to remove a
large allocation-call family. The candidate still retains the source label
store, segment series labels, and per-series chunk-entry storage at its new
peak.

## Measurement contract and limits

- Control source:
  `f2847d7` (`perf(ingest): sort sealed series indirectly`)
- Control binary SHA-256:
  `66b4af46b2fd33f08dbe2fffcabba4546ed04db9458f4ca774691c087000eaea`
- Candidate binary SHA-256:
  `2c5a2a9c043ebc7c1640ea7406dc29181f0efb074770fe749cbe5ff6fc98f43e`
- Workload: the exact accepted 250,000-message capture prefix
- Storage schema: Schema 8
- Allocator: Rust system allocator
- Candidate trace SHA-256:
  `e4a265cd98d9a2c7d82040d23f30c25a7ce5b342e6a5f1405f925f160551464b`

The host was not CPU-quiet: QEMU and an Android build were active. A formal
CPU/RSS ABBA was therefore not admitted. The decision uses Heaptrack
requested-live bytes, which count application allocation lifetimes rather
than page residency, plus exact storage and semantic gates. Candidate CPU,
wall time, and GNU RSS are not promotion evidence.

The Heaptrack debuggee reached clean Chronoxide shutdown and emitted a complete
trace. The wrapper subsequently attempted to launch the GUI and exited because
the profiling environment had no display; non-GUI `heaptrack_print` decoded
the complete trace successfully.

The evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/flush-lifetime-ab-20260724-012400`

## Correctness and publication safety

The candidate reproduced:

- 250,000 accepted replay messages and 9,634,809 recorded samples;
- 4 segments, 34 files, and 972,969,365 bytes;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
- the accepted replay-correctness JSON and corpus summary byte-for-byte; and
- all 40 independent readback oracle cases with zero skips or mismatches.

A focused failure test corrupts a Schema 8 chunk locator so index
serialization succeeds and later series assembly fails. It proves that the
already-written index remains only under `.tmp`: no footer, final segment
directory, or manifest is published. File creation order cannot affect the
footer because footer inventory order is fixed and hashes file content and
length, not creation metadata.

## Verification

The candidate passed:

- the focused post-index series-failure atomicity test;
- all 28 segment writer/flush tests;
- `cargo test -p chronoxide-core -p chronoxide-ingester --lib --all-features`
  (1,174 core and 91 ingester tests);
- strict core library and test Clippy gates;
- `cargo fmt --all -- --check`;
- `git diff --check`;
- complete segment-footer validation; and
- `chronoxide-query --verify-readbacks` with 40/40 executed and zero skipped.
