# Flat cold-series row store result

**Status:** promoted as a segment-seal memory improvement. The cold-series
writer now keeps one exact-capacity, row-major `u32` code buffer per keyset
instead of one `Vec<u32>` per series row.

## Decision

Promote the flat row store.

On the accepted 250,000-message replay prefix:

- the maximum whole-process requested-live memory during the affected
  largest-segment Series stage fell by 152,992,892 bytes
  (145.905 MiB, 4.7340%);
- whole-process allocation calls fell by 4,463,864 (1.8248%);
- the code-buffer allocation site fell from 4,450,272 allocations to 3,709
  (-99.9167%);
- all 34 storage files and 972,969,365 corpus bytes were byte-identical;
- replay counters matched the accepted calibration byte-for-byte;
- footer validation passed; and
- independent readbacks executed 40/40 with zero skips, isolation skips, or
  mismatches.

This is deliberately not reported as a run-wide peak win. The control's
process-wide peak occurred at 77.581 seconds, before its cold-series plan
started at 82.829 seconds. The candidate changed that peak by only -5,784
bytes (-0.00016%), which is immaterial. The optimization removes a distinct
later crest and millions of allocations; it cannot lower memory that was
allocated before this code ran.

## Change under test

The former plan used:

```text
Vec<Vec<Vec<u32>>>
    keyset
        row
            value codes
```

Every series row owned a `Vec<u32>`. Each keyset also owned a geometrically
grown `Vec` of those 24-byte row headers. On the largest segment, that meant
4,407,610 occupied row headers plus capacity slack.

The replacement uses one `ColdKeysetRows` per keyset:

```text
row_count: u32
row_width: u32
codes: Vec<u32>  // exact-capacity, row-major
```

The shape pass now counts rows in the same lexicographically ordered
`BTreeMap` that determines canonical keyset IDs. Plan construction reserves
`row_count * row_width` codes once per keyset and appends each row directly.
The writer walks that flat buffer in row-width chunks and retains the existing
0/1/2/4-byte on-disk encoding.

An explicit row count is required: empty keysets and keysets whose every value
dictionary has cardinality one have rows but zero encoded data bytes. The new
tests freeze those cases, interleaved row ordinals, all four value-code
widths, exact little-endian row-major bytes, malformed code counts, and
overflow handling.

Checked multiplication, fallible exact reservation, final expected-row
validation, and writer-side width/count validation preserve failure behavior
instead of relying on unchecked indexing or capacity assumptions.

This is an in-memory representation change only. Persisted ordering, offsets,
widths, bytes, checksums, and query semantics are unchanged, so no storage
version or `storage.md` update is required.

## Memory evidence

The control is the frozen binary and trace from the promoted inline-one
chunk-entry result. Both runs use the Rust system allocator.

| Heaptrack measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Process requested-live maximum | 3,695,666,562 B | 3,695,660,778 B | -5,784 B (-0.00016%; unchanged) |
| Maximum during largest-segment code-buffer lifetime | 3,231,779,430 B | 3,078,786,538 B | -152,992,892 B (-145.905 MiB, -4.7340%) |
| Allocation calls | 244,616,384 | 240,152,520 | -4,463,864 (-1.8248%) |
| Temporary allocations | 40,528,925 | 40,528,990 | +65 (immaterial) |
| Final leaked bytes | 414,748 B | 414,748 B | unchanged |

The stage comparison is not an arbitrary wall-clock window. A replayable raw
Heaptrack event parser identifies the exact lifetime of the largest
segment's old per-row code buffers and the candidate's consolidated code
buffers:

- control: 84.238-90.432 seconds;
- candidate: 80.704-85.539 seconds.

For each run, the table reports the highest official Heaptrack Massif
requested-live value inside that allocation-site-defined lifetime. The
candidate first reached its stage maximum at 80.825 seconds and retained that
value on a flat plateau; the control reached its maximum at 86.694 seconds.

Allocation-site accounting explains the result:

| Cold-plan owner | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Code-buffer allocations | 4,450,272 | 3,709 | -4,446,563 (-99.9167%) |
| Code bytes requested over the replay | 357,140,196 B | 357,140,196 B | unchanged |
| Largest-segment code bytes live | 355,458,744 B | 355,458,744 B | unchanged |
| Per-keyset row-header backing at peak | 153,433,632 B | removed | -153,433,632 B |

The control's 4,407,610 occupied row headers account for 105,782,640 bytes.
Geometric per-keyset capacities retained another 47,650,992 bytes
(45.444 MiB) of slack, for 153,433,632 requested-live bytes at the allocation
site. That is only 440,740 bytes more than the observed whole-stage
reduction; the residual includes the candidate's larger per-keyset records
and exact-row-count bookkeeping.

The candidate intentionally still holds value codes as `u32`. It removes
allocation and row-header overhead but does not yet compact the 355,458,744
bytes of largest-segment code payload to the final 0/1/2/4-byte widths.
An independent parser validated all 3,663 on-disk keyset blocks and counted
112,880,900 packed data bytes for the same 88,864,686 logical codes. Replacing
only the `u32` backing therefore has a structural payload differential of
242,577,844 bytes (231.340 MiB, 68.244%) on this segment. That is a model, not
a measured process-memory result: packing logic, metadata, scratch, and
lifetimes can offset it. Directly building packed rows is the next separate
experiment.

GNU `time` observed maximum RSS move from 3,262,880 KiB to 3,266,008 KiB
(+3,128 KiB, +0.096%). This is noise relative to the allocation-site result
and is not decision evidence.

## Measurement contract and limits

- Control source:
  `d96a7cc4257f3d7928ae1b362a21145d9f3741b6`
- Control binary SHA-256:
  `42b2b2f10e40c6f2bbc7f51b5e398130f55eb6036aa2f42711e2698ec7aa88a8`
- Control trace SHA-256:
  `582df2befeebdae177a3ae3871f632f1909b566cabde26ca1b5a86f53c379418`
- Candidate base source: the same control source plus the recorded patch
- Candidate patch SHA-256:
  `a1683377eb9ec551ce77ff271d7ca12a87ee373d55d9fd26d6a07495aecdf1b3`
- Candidate ingester binary SHA-256:
  `9aa66e40ea33c9e7122622d8ca96d5e4891cbda34000a0b58d8cfe2bc40a7337`
- Candidate query binary SHA-256:
  `1cf7d8db157f933cdb9209c59800737432f856ad3986a12d6702da146e29f671`
- Candidate trace SHA-256:
  `4426d43118a05317e132d395660339d9046f103d91e11ad42faed1e03881d8b9`
- Workload: exact accepted 250,000-message capture prefix
- Writer configuration: identical except for the run-specific output path;
  deterministic segment seed 42
- Storage schema: Schema 8
- Allocator: Rust system allocator

The host was not CPU-quiet. CPU time, wall time, and RSS are non-authoritative
for this run. The decision uses Heaptrack requested-live memory and exact
allocation-site event accounting plus byte and semantic gates. This result
does not claim a CPU speedup or prove the absence of a small CPU-time change.

The candidate evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/flat-cold-rows-memory-20260723T183311Z`

The frozen control evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/chunk-entry-store-memory-20260724-015515`

Both roots retain the traces, binary and patch hashes, configs, exact
manifests, logs, Massif exports, and reproducible allocation-site parser
sources and results. Raw-parser process totals include tiny allocations
suppressed by Heaptrack's built-in reporting; official Massif values are used
for process comparisons, while raw events are used only for exact target-site
causality.

## Correctness evidence

The real replay reproduced:

- 250,000 accepted replay messages and 9,634,809 recorded samples;
- 4 segments, 34 files, and 972,969,365 bytes;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
- replay-correctness JSON, corpus summary, and the complete segment SHA-256
  manifest byte-for-byte;
- complete segment-footer validation; and
- all 40 independent readback oracle cases with zero skips, isolation skips,
  or mismatches.

Focused tests cover exact cold-section bytes, row ordinals, zero-width rows,
width boundaries, malformed shapes, and overflow. Existing series, segment
publication, postings-index, and query-readback coverage remained green.

## Verification

The final candidate passed:

- all 14 `cold_v2` unit tests;
- all 137 `storage::series` library tests;
- the `series_bin`, `segment_publish`, and `postings_index` integration
  suites;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed strict all-feature Clippy passes;
- `cargo fmt --all -- --check`;
- `git diff --check`;
- complete segment-footer validation; and
- `chronoxide-query --verify-readbacks` with 40/40 executed and zero skipped.

## Artifact cleanup

After every correctness and analysis gate completed, the generated candidate
segment tree and redundant candidate query-binary copy were removed. This
reclaimed 1,475,076,181 logical bytes. The evidence root retains the candidate
ingester, trace, hashes, exact segment manifest, reports, logs, configuration,
Massif data, and reproducible analysis programs and results.
