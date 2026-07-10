# Segment Index v7 Lazy Directory Design

**Date:** 2026-07-10
**Status:** Approved for implementation

## Context

The version-6 `indexes.puffin` footer contains one 44-byte directory record per
index blob. The current replay corpus has approximately 3.60 million records
and 150.9 MiB of footer bytes. A reader must read and expand the complete footer
before it can locate the routing blob. Arc-sharing removed repeated directory
clones, but fresh-process routing still spends approximately 270 ms parsing the
footer and retains hundreds of megabytes of directory maps.

## Goals

- Make segment-index open independent of exact-postings cardinality.
- Preserve all existing index payload encodings and query semantics.
- Read at most one fixed-size metadata page for a selective exact lookup.
- Preserve ordered access for regex and negative matchers.
- Make lazy corruption a hard error rather than a false missing-label result.
- Eliminate shared seek cursors across cloned query readers.
- Keep serialization deterministic and streamable.
- Verify the format on a full replay of the existing 13-million-message capture.

## Non-goals

- Changing postings compression, FST encoding, routing-index version 2, or
  metric-series-range encoding.
- Supporting version-6 segments in the version-7 reader.
- Adding an open-addressed hash directory or a general B-tree.
- Adding mmap in the first implementation. The fixed page layout supports a
  future mmap backend, while the first reader uses position-independent reads.
- Reworking chunk payload I/O or PromQL projection behavior.

## On-disk architecture

The normative byte layout is specified in `docs/superpowers/specs/storage.md`
section 15.1. The main components are:

1. A 16-byte version-7 header.
2. Existing routing and metric-range payloads at the front of the file.
3. Contiguous exact-postings and auxiliary-payload regions.
4. A compact exact-page index followed by fixed 16 KiB directory pages.
5. A compact auxiliary directory for FST and time-range locators.
6. A fixed 256-byte trailer containing all top-level locators and counts.

Exact records remain 40 bytes and sorted by symbol pair. A page contains at
most 409 records and is protected by a CRC32C stored in its descriptor. Page
offsets are derived from the page number, preventing arbitrary child pointers.

The fixed trailer is intentionally small and direct. It locates routing and
metric ranges without reading either directory. Exact-page descriptors and the
auxiliary directory are initialized independently on first use.

## Reader architecture

Introduce a small random-access source abstraction:

```rust
trait SegmentIndexReadAt: Send + Sync {
    fn len(&self) -> io::Result<u64>;
    fn read_exact_at(&self, offset: u64, dst: &mut [u8]) -> io::Result<()>;
}
```

Implement it for `File` with the platform positional-read API and for
`Cursor<T: AsRef<[u8]>>` by bounds-checked slicing. `SegmentIndexReader` stores
the source in an `Arc`, so cloned readers share immutable bytes and directory
state without cloning a file description or sharing a mutable cursor.

Open performs only:

1. file-length lookup;
2. header read and validation;
3. fixed-trailer read and validation;
4. checked validation of top-level locator ranges.

The shared directory contains `OnceLock` caches for parsed exact-page
descriptors and auxiliary records. Errors are not converted to empty caches.

Lazy metadata operations become fallible:

```rust
exact_postings_metadata(...) -> io::Result<Option<ExactPostingsMetadata>>
label_time_range(...)         -> io::Result<Option<LabelValueTimeRange>>
label_name_symbols(...)       -> io::Result<Vec<u32>>
has_label_values(...)         -> io::Result<bool>
```

Query planning must distinguish `Skip(SegmentPruneReason)` from `io::Error`.
Malformed directory pages, FST/postings inconsistencies, and invalid locators
propagate through direct queries, sessions, prewarm, prefetch, discovery, and
the CLI verifier.

Exact metadata should retain its validated posting locator internally so
reading the posting payload does not repeat a directory-page lookup.

## Writer architecture

The writer computes region lengths with checked arithmetic before emitting
bytes. Existing sorted `BTreeMap` iteration is the canonical order.

1. Encode or size routing and metric payloads.
2. Compute exact-postings and auxiliary region offsets in a first pass.
3. Write the header and payload regions.
4. Build one 16 KiB exact page at a time to compute descriptors and page CRCs.
5. Write the exact-directory header and descriptors.
6. Rebuild and write the pages in a deterministic second pass, reusing one page
   buffer rather than retaining the complete directory.
7. Write the auxiliary directory and fixed trailer.

The writer must reject arithmetic overflow, counts exceeding their encoded
width, invalid time ranges, zero-length auxiliary payloads, and output lengths
inconsistent with the planned layout.

## Validation model

Fast open validates root structure only. A touched lazy directory validates:

- magic, version, flags, lengths, counts, and reserved zeros;
- CRC32C;
- checked section lengths and locator bounds;
- strict ordering and duplicate absence;
- descriptor first/last keys against page contents;
- exact posting ranges within the exact-postings region;
- auxiliary ranges within the auxiliary-payload region;
- `min_time_ms <= max_time_ms`.

Untouched directory-page corruption may remain latent during the default fast
path. `open_validated` first validates `footer.bin` and therefore protects the
complete `indexes.puffin`; focused directory validation tests additionally scan
all pages.

## Versioning and compatibility

- `SEGMENT_INDEX_VERSION` becomes `7`.
- The segment footer schema becomes `5` because a tracked segment artifact has
  breaking semantics.
- Version-7 readers reject version-6 containers.
- Existing v6 replay data remains untouched for A/B comparison.
- New v7 replay data is generated into a separate directory.

## Test strategy

Follow red-green TDD in these slices:

1. Golden minimal header/trailer bytes and deterministic reverse insertion.
2. Multi-page exact-directory round trip and one-page lazy-read accounting.
3. Root corruption: truncation, magic/version/flags, CRC, file length, required
   locator, overflow, and top-level overlap.
4. Exact corruption: header/descriptor/page CRC, record count, ordering,
   duplicates, key-range mismatch, padding, time bounds, and posting bounds.
5. Auxiliary corruption: CRC, ordering, duplicates, unsupported kind, and
   payload bounds.
6. Query equivalence for equality, regex, negative matchers, time pruning,
   routing hits/misses, and label discovery.
7. Error propagation through store/session/PromQL/prewarm/prefetch.
8. Concurrent cloned-reader positional lookups.

Run the complete core and query CLI suites before replay.

## Replay and benchmark

Preserve the current v6 corpus and matching reader binary. Freeze the current
dirty replay configuration, changing only the v7 output directory. Replay the
same capture with deterministic seed 42.

Acceptance requires:

- identical segment IDs and aggregate metadata;
- byte-identical `symbols.bin`, `series.bin`, `chunk_index.bin`, chunk, and OOO
  files for matching segments;
- byte-identical manifest files: manifests contain segment metadata, not index
  file checksums, so the deterministic replay must not change them;
- a keyed streaming digest comparison proving every retained routing,
  metric-range, exact-posting, FST, and label-time-range payload is
  byte-identical between v6 and v7; only container headers, directories, and
  absolute locator bytes may differ within `indexes.puffin`;
- expected whole-file differences limited to `indexes.puffin` and `footer.bin`;
- zero real-corpus readback mismatches with footer validation enabled.

Benchmark v6 and v7 on the same filesystem with alternating fresh processes.
Measure missing metric, physical gauge, count projection, regex, cached fresh
sessions, warm controls, read-profile bytes, and peak RSS. The expected primary
signal is removal of the approximately 270 ms / 150.9 MiB footer-open cost; no
specific speedup is accepted without measured evidence.
