# Inline-one chunk-entry store result

**Status:** promoted. The active segment writer now stores the common
one-chunk-per-series case inline and retains the former nested-`Vec` shape as a
statically dispatched differential backend.

## Decision

Promote the inline-one store.

On the accepted 250,000-message replay prefix:

- Heaptrack requested-live peak fell by 564,173,708 bytes
  (538.038 MiB, 13.2440%);
- allocation calls fell by 4,450,399 (1.7868%);
- all 34 storage files and 972,969,365 corpus bytes were byte-identical;
- replay counters matched the accepted calibration byte-for-byte;
- footer validation passed; and
- independent readbacks executed 40/40 with zero skips, isolation skips, or
  mismatches.

The result is unusually close to the layout estimate. On this target,
`ChunkIndexEntry` is 40 bytes. The first push into an empty
`Vec<ChunkIndexEntry>` reserves four entries, so a one-chunk row retains
24 outer bytes plus 160 requested heap bytes. The inline
`SmallVec<[ChunkIndexEntry; 1]>` row is 56 bytes and makes no inner allocation.
The largest segment contained 4,407,610 one-chunk rows:

`4,407,610 * (24 + 160 - 56) = 564,174,080 bytes`

That is only 372 bytes away from the observed peak reduction.

## Change under test

`ChunkEntryStore<L>` owns the outer series vector. Its crate-private
`SeriesChunkEntries` trait requires `Default`, `AsRef`, and `AsMut`, and adds
the append operation needed by the hot record path. There are two
implementations:

- `Vec<ChunkIndexEntry>`, retained as the nested-vector differential backend;
- `SmallVec<[ChunkIndexEntry; 1]>`, selected monomorphically by
  `ActiveSegment`.

There is no trait object, enum branch, runtime flag, or vtable in the ingest
path. Recording, chunk rewriting, symbol finalization, legacy chunk-index
encoding, and schema-7/8 series assembly are generic and monomorphized over
the row type. Per-series and final-series sorting comparators are unchanged.

The pre-existing public `write_chunk_index` and `chunk_index_ranges` APIs keep
their concrete nested-`Vec` signatures. Trusted crate-private row-generic
helpers serve the inline backend. This avoids exposing a generic `AsRef`
implementation that could return inconsistent row lengths between the
directory and body encoding passes.

This is an in-memory representation change only. Persisted layouts, ordering,
checksums, roots, and query semantics are unchanged, so no storage version or
`storage.md` update is required.

## Memory evidence

The control is the frozen candidate from the promoted segment-flush lifetime
result. Both runs use the system allocator.

| Heaptrack measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Requested-live maximum | 4,259,840,270 B | 3,695,666,562 B | -564,173,708 B (-13.2440%) |
| Peak time | 78.930 s | 77.581 s | shifted earlier |
| Allocation calls | 249,066,783 | 244,616,384 | -4,450,399 (-1.7868%) |
| Temporary allocations | 40,528,969 | 40,528,925 | -44 |
| Final leaked bytes | 414,748 B | 414,748 B | unchanged |

GNU `time` observed maximum RSS fall from 3,872,328 KiB to 3,262,880 KiB
(-609,448 KiB). This agrees directionally with the requested-live result, but
RSS is not promotion evidence because QEMU and other host activity were
present.

The optimization is corpus-shape dependent. Empty rows cost 32 more bytes
than an empty `Vec`, and rows that exceed one entry spill. The accepted corpus
is dominated by exactly one chunk per series, which is why this layout is a
strong fit. No global `smallvec` feature was enabled.

## Measurement contract and limits

- Control source: `9344be9a50de`
- Control binary SHA-256:
  `2c5a2a9c043ebc7c1640ea7406dc29181f0efb074770fe749cbe5ff6fc98f43e`
- Control trace SHA-256:
  `e4a265cd98d9a2c7d82040d23f30c25a7ce5b342e6a5f1405f925f160551464b`
- Candidate base source: `9344be9a50de` plus the recorded candidate patch
- Candidate patch SHA-256:
  `3849df33eac24416f0b1aac924749d5dd1fe23e5256cc69dd9b6060d107679d8`
- Candidate binary SHA-256:
  `42b2b2f10e40c6f2bbc7f51b5e398130f55eb6036aa2f42711e2698ec7aa88a8`
- Candidate trace SHA-256:
  `582df2befeebdae177a3ae3871f632f1909b566cabde26ca1b5a86f53c379418`
- Workload: exact accepted 250,000-message capture prefix
- Storage schema: Schema 8
- Allocator: Rust system allocator

The host was not CPU-quiet. CPU time, wall time, and RSS are therefore
non-authoritative observations. The decision uses Heaptrack requested-live
bytes and allocation counts, which describe application allocation
lifetimes, plus exact storage and semantic gates.

The evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/chunk-entry-store-memory-20260724-015515`

## Correctness evidence

Focused coverage proves:

- nested-`Vec` and inline-one store operations agree, including the
  inline-to-spilled transition;
- an out-of-range append preserves the former invariant diagnostic;
- schema-6 chunk-index ranges and bytes agree for empty, one-entry, and
  unsorted spilled rows;
- schema-7 `series.bin`, v2 chunk-index bytes, authenticated roots, and full
  writer stats agree for nested-`Vec` and `SmallVec` inputs; and
- Schema 7 and Schema 8 both preserve a spilled two-chunk row through a
  non-identity metric-order permutation, series-major payload rewrite,
  footer validation, and exact query readback.

The real replay reproduced:

- 250,000 accepted replay messages and 9,634,809 recorded samples;
- 4 segments, 34 files, and 972,969,365 bytes;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
- the accepted replay-correctness JSON and complete segment SHA-256 manifest
  byte-for-byte; and
- all 40 independent readback oracle cases with zero skips or mismatches.

## Verification

The candidate passed:

- all 1,169 `chronoxide-core` library tests;
- the all-feature core/ingester library gate (1,180 core and 91 ingester
  tests);
- strict core library and test Clippy gates;
- `cargo fmt --all -- --check`;
- `git diff --check`;
- complete segment-footer validation; and
- `chronoxide-query --verify-readbacks` with 40/40 executed and zero skipped.
