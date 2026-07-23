# Packed cold-series row store result

**Status:** promoted as a segment-seal memory improvement. The cold-series
plan now stores value codes directly in their final 0/1/2/4-byte row-major
representation instead of retaining an intermediate `u32` for every label.

## Decision

Promote the packed row store.

On the accepted 250,000-message replay prefix:

- the maximum whole-process requested-live memory during the affected
  largest-segment Series stage fell by 242,392,397 bytes
  (231.163 MiB, 7.8730%);
- the largest segment's code-buffer backing fell by 242,577,844 bytes
  (231.340 MiB, 68.2436%);
- the observed stage reduction captured 99.9236% of the independently modeled
  payload reduction;
- all 34 storage files and 972,969,365 corpus bytes were byte-identical;
- replay counters and the complete file inventory matched byte-for-byte;
- footer validation passed; and
- independent readbacks executed 40/40 with zero skips, isolation skips, or
  mismatches.

The run-wide requested-live maximum changed by only +4,572 bytes
(+0.00012%), which is immaterial. As in the flat-row experiment, that maximum
occurs before the cold-series plan exists. This change removes a distinct
later seal-stage crest; it cannot lower memory that peaks before its code
runs.

## Change under test

The control retained one exact flat `Vec<u32>` per keyset. That removed the
former per-series `Vec` headers, but every logical code still occupied four
bytes until the writer revisited the complete plan and encoded it.

The replacement uses:

```text
row_count: u32
expected_rows: u32
row_len: u32
widths: Box<[u8]>
data: Vec<u8>  // exact-capacity final bytes
```

The shape pass first finalizes canonical value dictionaries and exact row
counts. Each keyset then derives its immutable canonical widths, checks the
on-disk `u32` row/data bounds, and reserves exactly
`row_len * expected_rows` packed bytes. The entry pass resolves the same
dictionary codes and appends their little-endian 0/1/2/4-byte forms directly.
It does not retain a second `u32` representation.

Appending a row:

- rejects overfill before mutation;
- requires the exact logical key count;
- requires zero-width codes to be zero and checks `u8`/`u16` narrowing;
- rolls packed data back to the row start on every lookup or encoding error;
- requires exactly `row_len` new bytes before incrementing `row_count`; and
- retains explicit counts for empty keysets and nonempty all-zero-width
  keysets, where any number of rows has no data bytes.

Final validation checks expected row count, legal widths, exact row length,
exact packed data length, and widths against the canonical dictionaries.
Missing or empty dictionaries are errors instead of implicit width-zero
shapes.

The writer emits the stored widths followed by one bulk packed-data write per
keyset. It no longer walks every code or reconstructs the width arrays during
serialization. The existing cold-page writer still applies the same page
boundaries and CRCs.

This is an in-memory representation change only. Persisted ordering, widths,
row/data lengths, offsets, bytes, checksums, and query semantics are
unchanged, so no storage version or `storage.md` update is required.

## Memory evidence

The control is the frozen `bf51a8e` flat-row binary and trace. Both runs use
the Rust system allocator.

| Heaptrack measure | Flat-`u32` control | Packed candidate | Change |
| --- | ---: | ---: | ---: |
| Process requested-live maximum | 3,695,660,778 B | 3,695,665,350 B | +4,572 B (+0.00012%; unchanged) |
| Maximum during largest-segment code-buffer lifetime | 3,078,786,538 B | 2,836,394,141 B | -242,392,397 B (-231.163 MiB, -7.8730%) |
| Allocation calls | 240,152,520 | 240,159,535 | +7,015 (+0.0029%) |
| Temporary allocations | 40,528,990 | 40,535,897 | +6,907 (+0.0170%) |
| Final leaked bytes | 414,748 B | 414,748 B | unchanged |

The small call-count increase is consistent with retaining one immutable
width array per keyset and is immaterial beside the payload reduction.

The comparison windows come from the exact allocation sites, not arbitrary
wall-clock ranges:

- flat `u32` control: 80.704-85.539 seconds;
- packed candidate: 80.752-85.236 seconds.

The table selects the highest official Heaptrack Massif requested-live value
inside each allocation-site-defined lifetime. The control first reached its
stage maximum at 80.825 seconds; the candidate reached its stage maximum at
80.893 seconds.

Allocation-site accounting matches the structural model:

| Code-buffer measure | Flat-`u32` control | Packed candidate | Change |
| --- | ---: | ---: | ---: |
| Allocation calls | 3,709 | 3,709 | unchanged |
| Bytes requested over the replay | 357,140,196 B | 113,285,124 B | -243,855,072 B |
| Largest-segment bytes live | 355,458,744 B | 112,880,900 B | -242,577,844 B (-68.2436%) |

The prior report independently decoded all 3,663 largest-segment keyset
blocks: 88,864,686 logical codes occupied 355,458,744 bytes as `u32`, while
their authenticated on-disk packed data lengths summed to 112,880,900 bytes.
The measured packed allocation is exactly 112,880,900 bytes. The
whole-stage reduction is only 185,447 bytes smaller than the payload model;
the residual covers the retained width arrays, larger per-keyset records, and
nearby phase composition.

GNU `time` observed maximum RSS move from 3,266,008 KiB to 3,261,260 KiB
(-4,748 KiB, -0.145%). This is noisy-host context, not promotion evidence.

## Runtime observations

The host was not CPU-quiet, so runtime data is directional only. It does not
show a large regression:

| Diagnostic | Flat-`u32` control | Packed candidate | Change |
| --- | ---: | ---: | ---: |
| Heaptrack replay runtime | 92.945 s | 92.652 s | -0.293 s |
| Largest-window elapsed | 37,750 ms | 37,229 ms | -521 ms (-1.38%) |
| Largest-window `writer_flush_ms` | 16,112 ms | 15,639 ms | -473 ms (-2.94%) |

Packing still resolves every code once, but moves encoding into plan
construction and replaces millions of tiny serialization calls with one bulk
data write per keyset. The observations are consistent with that design, but
they are not a formal CPU/latency claim.

## Measurement contract and limits

- Control source:
  `bf51a8e65b1b57639eb131a62a14291646372d86`
- Control ingester binary SHA-256:
  `9aa66e40ea33c9e7122622d8ca96d5e4891cbda34000a0b58d8cfe2bc40a7337`
- Control trace SHA-256:
  `4426d43118a05317e132d395660339d9046f103d91e11ad42faed1e03881d8b9`
- Candidate base source: the same control source plus the recorded patch
- Candidate patch SHA-256:
  `fe164ee845c88bc2f27f0ecef8fb1801c6d85f69f8e510829b9368edc70d24ca`
- Candidate ingester binary SHA-256:
  `0ebcc522df19eb1add7ff16a3fea6f34fec021228321858aa2e772b7b1b295ac`
- Candidate query binary SHA-256:
  `ac3baf51145c87eb6365e6b7dc4be9a594c8aaf3a0da842cb0e6717b8e50d69c`
- Candidate trace SHA-256:
  `0ea87f6a5c0cd15023df0c494b0ca9d8d7e260cafdd0f54238ba84c09fc04fbe`
- Workload: exact accepted 250,000-message capture prefix
- Writer configuration: identical except for the run-specific output path;
  deterministic segment seed 42
- Storage schema: Schema 8
- Allocator: Rust system allocator

The complete 13-million-message capture (20.59 GB physical; 142.30 GB
uncompressed payload) and every frozen control input were SHA-256 verified
before the candidate build and replay. CPU time, wall time, and RSS are
non-authoritative because the host was not quiet. The decision uses Heaptrack
requested-live bytes and exact allocation-site accounting plus byte and
semantic gates.

The candidate evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/packed-cold-rows-memory-20260723T192245Z-tdUWIy`

The frozen control evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/flat-cold-rows-memory-20260723T183311Z`

Raw-parser process totals include tiny allocations suppressed by Heaptrack's
built-in reporting. Official Massif values are used for process comparisons;
raw events are used only to identify exact target-site bytes and lifetimes.

## Correctness evidence

The real replay reproduced:

- 250,000 accepted replay messages and 9,634,809 recorded samples;
- 4 segments, 34 files, and 972,969,365 bytes;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
- replay-correctness JSON, corpus summary, complete inventory, and every
  segment SHA-256 byte-for-byte;
- complete segment-footer validation; and
- all 40 independent readback oracle cases with zero skips, isolation skips,
  or mismatches.

The focused test oracle contains a test-only copy of the pre-pack encoder that
does not call the production width or packing helpers. A 65,537-cardinality
fixture builds the real plan with widths 0/1/2/4 and compares the complete
cold sections against that independent encoder at offsets zero and 4,096.
A separate frozen mixed-width golden checks exact headers, width-zero
omission, boundary values, little-endian bytes, and row-major ordering.

Additional tests cover interleaved keyset ordinals, empty and all-zero-width
multirow blocks, short/long/invalid packed shapes, canonical-width mismatch,
row/data overflow before allocation, missing/empty dictionaries, partial-row
rollback after bytes were appended, overfill, sink failure propagation, and
immutable-plan retry.

## Artifact cleanup

After all byte-equivalence, footer, readback, memory-analysis, and report
gates completed, the generated candidate segment tree and redundant candidate
query binary were removed. This reclaimed 1,475,329,581 logical bytes
(1.374008 GiB):

- candidate segment tree: 972,969,365 bytes;
- candidate query binary: 502,360,216 bytes.

The frozen candidate ingester, Heaptrack trace, logs, manifests, hashes, raw
analysis, and cleanup records remain under the evidence root. No capture,
control artifact, or unrelated experiment output was removed.

## Verification

The exact measured candidate source passed:

- all 17 `cold_v2` unit tests;
- all 142 `storage::series` library tests;
- the `series_bin`, `segment_publish`, and `postings_index` integration
  suites;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed workspace-wide all-feature Clippy gates for libraries,
  binaries, tests, and benches with warnings denied;
- `cargo fmt --all -- --check`;
- `git diff --check`;
- complete segment-footer validation; and
- `chronoxide-query --verify-readbacks` with 40/40 executed and zero skipped.
