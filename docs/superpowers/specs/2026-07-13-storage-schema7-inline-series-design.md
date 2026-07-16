# Storage schema 7: paged inline-series metadata

- **Date:** 2026-07-13
- **Status:** Implemented prior-format contract and explicit comparator.
  Schema 8 supersedes it as the default while retaining the same symbol,
  series, chunk-index, chunk, and metadata-facade semantics.
- **Baseline:** storage schema 6 at `4e428b0`
- **Normative storage contract:** [storage.md](storage.md), which incorporates
  this focused schema-7 contract by reference for the explicit prior-format
  comparator.
- **Materiality evidence:**
  [schema-7 layout model](../../experiments/storage_vnext/2026-07-13-schema7-layout-model.md)

## Decision

Storage schema 7 retains the schema-6 `symbols.bin` v3 dictionary and
changes the common series-to-chunk routing path and the index payload-integrity
boundary:

- `series.bin` advances from version 2 to version 3;
- `chunk_index.bin` advances from version 1 to overflow-only version 2;
- `indexes.puffin` advances from version 7 to version 8 so touched exact
  postings, label FSTs, and label-value time ranges carry directory-protected
  expected counts and CRCs;
- a fixed 40-byte hot series record contains label location and the usual
  single-chunk descriptor;
- hot records live in independently checksummed fixed 16 KiB pages;
- multi-chunk, mixed-lane, and width-exception series use a complete
  checksummed overflow blob; and
- every indexed chunk locator integrity-checks the exact 40-byte `ChunkHeader`
  plus the 16-byte typed-scalar header when one is present, before the reader
  interprets either header.

The schema-7 writer advances the segment footer schema from 6 to 7. This
is not a schema-6 extension: a changed `series.bin` or `chunk_index.bin` is
never published under schema 6. Schema 7 is selected explicitly through
`storage_schema = "schema7"` or the exact `schema7` query/API policy. One
explicit read-only `schema6-ab` policy may accept a homogeneous schema-6 corpus
so the same binary can perform an attributable A/B; it validates complete
footers outside timed queries and is not a production fallback.

## Why the experiment proceeds

Read-only model v4 scanned all 18 segments, all 47,766,209 series/chunk
locators, and the protected v7 index roots/directories in a schema-5 corpus.
Schema 5 and schema 6 share `series.bin` v2 and `chunk_index.bin` v1, so those
two reference byte counts transfer directly; the all-artifact total retains the
corpus's `symbols.bin` v2 and is not a measured schema-6 total. Because schema 6
and schema 7 both use symbols v3, the modeled delta transfers to their A/B.
Every series has exactly one chunk and every selected field fits the inline
representation. The v8-aware model projects:

| Measure | Reference corpus | Schema-7/v8 model | Net change |
| --- | ---: | ---: | ---: |
| `series.bin + chunk_index.bin` | 5,490,383,885 | 3,203,741,773 | -2,286,642,112 (41.64%) |
| `indexes.puffin` | 4,918,064,629 | 4,946,829,381 | +28,764,752 |
| Metadata artifacts | 10,643,653,467 | 8,385,776,107 | -2,257,877,360 (21.21%) |
| All standard artifacts | 21,545,107,020 | 19,287,229,660 | -2,257,877,360 (10.48%) |

These exact projections include 78,558 cold-page CRC descriptors. The observed
3,563,222 exact entries require 8,722 v7 pages and 10,458 v8 pages; 33,322
auxiliary records grow from 40 to 48 bytes. The resulting 28,764,752-byte v8
charge has already been subtracted from the gross series/chunk saving. The
overflow path remains mandatory even though the reference corpus does not
exercise it.

This is capacity and access-layout evidence, not encoded schema-7 bytes or a
latency/CPU claim. After implementation, measure encoded v8 artifacts, checksum
CPU, and replay/query costs on the deterministic prefix; do not substitute
modeled values for measured evidence.

## Preserved semantics

Schema 7 does not change:

- event-time segment placement or query range semantics;
- capture-time control policy or the trusted `captured_at_ms` replay anchor;
- stable series identity, deterministic series-ref ordering, or deterministic
  segment IDs;
- symbol ordering, IDs, or `symbols.bin` v3 bytes;
- keyset/value-code label encoding;
- logical postings, FST, label-value time-range, metric-range, or routing
  contents; v8 changes only their index-container root/directory integrity
  metadata, while routing-v2 and metric-series-ranges-v1 payload bytes remain
  unchanged;
- chunk frames, native/scalar payload encodings, or typed OTLP semantics;
- stable-NaN, reset, temporality, start-time, flags, or PromQL projection
  semantics; or
- direct addressability of every chunk.

Typed sample metadata remains authoritative in each Histogram,
ExponentialHistogram, and Summary native value and in its scalar lane:
`start_time_ms`, OTLP flags, temporality, and reset hint are not replaced by or
inferred from the hot record. Chunk-header flags are integrity-checked aggregate
hints only.

`indexes.puffin` v8 locally integrity-checks every lazily read payload used by
ordinary equality membership, regex completeness, or label-value time pruning.
It deliberately does not change or locally integrity-check routing v2 or
metric-series-ranges v1. Governed decoding proves those two summaries' touched
structure and resource bounds, not their semantic truth. Ordinary schema-7
query sessions therefore use integrity-checked exact postings as the authoritative
membership source and do not prune from routing or metric-range
absence/time/kind summaries. Those summaries become authoritative only while
holding opaque, same-generation capabilities minted by the complete semantic
validator described below. Footer checksum validation alone does not mint
either capability.

## Explicit non-goals

This experiment does not add columnar label tiles, postings compression,
separate scalar files, series-level metadata sidecars, packed frames,
adjacent-segment packing, or a new chunk payload codec. It does not run four
full-capture replays. It replays only the same two-million-message prefix used
by the schema-6 baseline unless the user separately authorizes a full capture.

## Binary conventions

All fixed-width integers are little-endian. All arithmetic and host-size
conversions are checked. Offsets are absolute file offsets unless stated
otherwise. Reserved bits and bytes are zero. Files contain no unaccounted
trailing bytes.

## `indexes.puffin` v8

Footer schema 7 requires index-container version 8. Version 7 remains frozen
for the explicit schema-6 benchmark adapter and is never accepted as a
schema-7 production fallback. Version 8 preserves the region order and the
existing routing-v2, metric-series-ranges-v1, exact-postings body, raw FST body,
and label-value time-range body bytes. It changes only the root and directory
metadata needed to integrity-check correctness-affecting touched payloads.

The exact physical order is:

```text
SegmentIndexesHeaderV8
RoutingPayloadV2?
MetricSeriesRangesPayloadV1
ExactPostingsPayloadV2Region
AuxiliaryPayloadV2Region
ExactDirectoryV2
ExactDirectoryPageV2[page_count]
AuxiliaryDirectoryV2
SegmentIndexesTrailerV8
EOF
```

The 16-byte header retains magic `SIDX`, zero flags, length 16, and a zero
reserved word; its version is 8. Locators remain 16-byte absolute
`{u64 offset, u64 len}` pairs. Unlike the frozen v7 reader's non-overlap-only
rule, v8 is gap-free: each present region begins exactly at the end of the
previous present region, the first present region begins at offset 16, and the
last directory ends exactly where the trailer begins. An absent optional or
canonically empty region uses `{0, 0}`, consumes no bytes, and does not interrupt
that adjacency rule. Any unaccounted byte between the header and trailer is
corruption.

The trailer remains exactly 256 bytes:

| Offset | Type | Field | Required value or meaning |
| ---: | --- | --- | --- |
| 0 | `u32` | magic | `SIDT` |
| 4 | `u16` | version | `8` |
| 6 | `u16` | flags | `0` |
| 8 | `u32` | trailer_len | `256` |
| 12 | `u32` | reserved0 | `0` |
| 16 | `u64` | file_len | exact physical length |
| 24 | locator | routing | optional |
| 40 | locator | metric_ranges | required |
| 56 | locator | exact_directory | required, even when empty |
| 72 | locator | exact_pages | empty iff no exact entries |
| 88 | locator | exact_postings | empty iff no exact entries |
| 104 | locator | auxiliary_directory | required, even when empty |
| 120 | locator | auxiliary_payloads | empty iff no auxiliary entries |
| 136 | `u64` | exact_entry_count | exact directory cardinality |
| 144 | `u32` | exact_page_count | `ceil(exact_entry_count / 341)` |
| 148 | `u32` | exact_record_len | `48` |
| 152 | `u32` | exact_page_len | `16,384` |
| 156 | `u32` | auxiliary_entry_count | auxiliary directory cardinality |
| 160 | `u32` | trailer_crc32c | CRC of all 256 bytes with this field zero |
| 164 | `u32` | series_count | authoritative dense series-ref bound |
| 168 | `u32` | symbol_count | authoritative symbol-id bound |
| 172 | `u32` | exact_directory_crc32c | exact directory's expected CRC |
| 176 | `u32` | auxiliary_directory_crc32c | auxiliary directory's expected CRC |
| 180 | `u8[72]` | reserved1 | all zero |
| 252 | `u32` | terminal_magic | `S8ND` |

Top-level locator arithmetic, presence, and region ordering otherwise retain
the v7 rules, with the stronger canonical adjacency requirement above. Before
following any non-root locator, a schema-7 metadata session binds
`series_count` and `symbol_count` to the validated same-generation series and
symbol roots. The directory CRC at offset 172 or 176 must equal both the
directory's encoded CRC and a fresh computation with that directory field
zeroed. Caller-provided counts cannot mint this binding.

### Exact postings v2

`ExactDirectoryHeaderV2` remains 64 bytes and retains the v1 field offsets. It
uses magic `EXD8`, version 2, zero flags, descriptor length 32, page length
16,384, record length 48, and `records_per_page == 341`. Its descriptor region
starts at 64 and is exactly `page_count * 32` bytes; its CRC remains at offset
56 with that field zeroed. The 32-byte page descriptor layout is unchanged and
stores ordered first/last `(label_name_sym, label_value_sym)` fences, non-zero
record count, complete-page CRC, and zero reserved words.

Each exact page uses magic `XPG8`, version 2, zero flags, its page ordinal, and
record count in the unchanged 16-byte header. Each non-final page contains 341
records; `16 + 341 * 48 == 16,384`. Final-page padding is zero and is included
in the descriptor CRC.

```text
ExactDirectoryRecordV2:             // 48 bytes
  u32 label_name_sym                // offset 0
  u32 label_value_sym               // offset 4
  u64 postings_offset               // offset 8
  u64 postings_len                  // offset 16
  u64 min_time_ms                   // offset 24
  u64 max_time_ms                   // offset 32
  u32 ref_count                     // offset 40; non-zero
  u32 payload_crc32c                // offset 44
```

The v2 payload body remains:

```text
u32 ref_count
u32 series_ref[ref_count]
```

`postings_len == 4 + 4 * ref_count`. The CRC covers the complete payload,
including its count. A touched reader checks CRC before allocation, requires
the body and directory counts to agree, validates the exact length and strict
increasing uniqueness, and requires every ref to be less than the root-bound
`series_count`. The page CRC binds expected count and payload checksum to the
label key, locator, and time summary.

### Auxiliary payloads v2

`AuxiliaryDirectoryHeaderV2` remains 64 bytes. It uses magic `AUX8`, version 2,
zero flags, record length 48, records offset 64, records length
`entry_count * 48`, the existing CRC field at offset 40, and 20 zero reserved
bytes.

```text
AuxiliaryDirectoryRecordV2:         // 48 bytes
  u16 kind                          // 2 = FST, 3 = label-value time ranges
  u16 flags                         // 0
  u32 label_name_sym                // offset 4
  u64 payload_offset                // offset 8
  u64 payload_len                   // offset 16; non-zero
  u64 min_time_ms                   // offset 24
  u64 max_time_ms                   // offset 32
  u32 item_count                    // offset 40; non-zero
  u32 payload_crc32c                // offset 44
```

Records retain v7 ordering, uniqueness, locator, symbol, time-summary, and
canonical unconstrained-range rules. The CRC covers every byte of the exact
payload and is verified before parsing or allocation.

A kind-2 payload remains the raw deterministic FST. `item_count` equals the
non-zero `fst::Set::len()`. The reader validates the FST and UTF-8 after its CRC
passes. Every value emitted to selector planning must resolve through the bound
symbol root; an unresolved value is corruption rather than a skipped regex
candidate.

A kind-3 payload remains:

```text
u32 item_count
repeated item_count times:
  u32 label_value_sym
  u64 min_time_ms
  u64 max_time_ms
```

Its exact length is `4 + 20 * item_count`; body and directory counts agree,
value symbols are strictly increasing, unique, and below the bound symbol
count, each range is ordered, and the aggregate equals the directory summary.
When kind 2 and kind 3 both exist for a label, their counts and summaries
match. The writer derives both from one complete sealed label inventory.

Opaque selections and cached values retain the complete root, protected record,
expected count/CRC, and segment-generation provenance. A cache hit rechecks
that context. Any checksum, count, root, symbol-resolution, ordering, or context
failure becomes sticky artifact corruption and cannot become a miss, empty
regex expansion, time prune, skipped series, or partial result.

Routing v2 and metric-series-ranges v1 remain structurally validated but
summaries without local integrity checks. Without the corresponding authority minted
by complete semantic validation, schema-7 queries may use them only for
ordering or prefetch. Exact postings, FSTs, and label-value ranges become
ordinary byte-integrity authorities only through the v8 chain and the strict
same-seal writer; a CRC does not prove a buggy derivation semantically correct.

## `series.bin` v3

The exact physical order is:

```text
SeriesHeaderV3                     // 176 bytes
SeriesHotPageDescriptorV1[P]       // 16 bytes each
SeriesColdPageDescriptorV1[C]      // 16 bytes each
RootAlignmentPadding               // canonical zero bytes
SeriesHotPageV1[P]                 // 16,384 bytes each
KeySetsSectionV2
ValueDictsSectionV2
KeySetBlocksSectionV2
EOF
```

For `N = num_series`:

```text
P = ceil(N / 409)
COLD = keysets_len + value_dicts_len + keyset_blocks_len
C = ceil(COLD / 16,384)
directory_offset = 176
directory_len = 16 * (P + C)
hot_pages_offset = align_up(directory_offset + directory_len, 4096)
hot_pages_len = 16,384 * P
```

`C` must fit `u32`; equivalently `COLD <= u32::MAX * 16,384`. The writer
rejects a larger cold stream, and the reader verifies the checked ceiling,
combined descriptor count, multiplication, alignment, and every derived file
offset before allocation or I/O.

The bytes between the combined descriptor directory and `hot_pages_offset` are
zero and are covered by the root CRC. There are no gaps between the hot-page
region and the three cold label sections. The v2 cold-section byte encodings
remain unchanged; their absolute offsets are deterministically rebased. The
cold descriptors integrity-check consecutive 16 KiB physical ranges over the
concatenated cold bytes without introducing a second copy or per-section
padding.

### `SeriesHeaderV3`

The header is exactly 176 bytes:

| Offset | Type | Field | Required value or meaning |
| ---: | --- | --- | --- |
| 0 | `u32` | magic | `SERI` |
| 4 | `u16` | version | `3` |
| 6 | `u16` | flags | `0` |
| 8 | `u32` | header_len | `176` |
| 12 | `u32` | descriptor_len | `16` |
| 16 | `u32` | hot_page_len | `16,384` |
| 20 | `u32` | hot_page_header_len | `24` |
| 24 | `u32` | hot_record_len | `40` |
| 28 | `u32` | records_per_page | `409` |
| 32 | `u32` | num_series | dense series-ref count |
| 36 | `u32` | page_count | `ceil(num_series / 409)` |
| 40 | `u32` | num_keysets | cold-section keyset count |
| 44 | `u32` | num_value_dicts | cold-section dictionary count |
| 48 | `u32` | chunk_index_root_crc32c | exact v2 root CRC |
| 52 | `u32` | root_crc32c | CRC of `[0, hot_pages_offset)` with this field zero |
| 56 | `u32` | cold_page_len | `16,384` |
| 60 | `u32` | cold_page_count | `ceil(cold_bytes_len / 16,384)` |
| 64 | `u64` | directory_offset | `176` |
| 72 | `u64` | directory_len | `16 * (page_count + cold_page_count)` |
| 80 | `u64` | hot_pages_offset | canonical 4 KiB alignment formula |
| 88 | `u64` | hot_pages_len | `16,384 * page_count` |
| 96 | `u64` | keysets_offset | first byte after hot pages |
| 104 | `u64` | keysets_len | exact v2 keyset section length |
| 112 | `u64` | value_dicts_offset | follows keysets exactly |
| 120 | `u64` | value_dicts_len | exact v2 dictionary section length |
| 128 | `u64` | keyset_blocks_offset | follows dictionaries exactly |
| 136 | `u64` | keyset_blocks_len | exact v2 packed-block section length |
| 144 | `u64` | segment_start_ms | integrity-checked segment start |
| 152 | `u64` | segment_end_ms | integrity-checked exclusive segment end |
| 160 | `u64` | chunk_index_file_len | exact v2 file length |
| 168 | `u64` | file_len | exact `series.bin` length |

`segment_start_ms < segment_end_ms`. Both values must match `meta.json` and the
segment ID, and must also match manifest inventory when the segment is opened
through a manifest. `chunk_index_file_len` and
`chunk_index_root_crc32c` must match the opened overflow root before any series
record is used.

For the canonical empty table, `num_series == num_keysets ==
num_value_dicts == page_count == 0`. Each retained v2 cold section is one
8-byte zero-entry offset table whose sole absolute terminal offset equals that
section's EOF. The three little-endian `u64` values are respectively `4,104`,
`4,112`, and `4,120`. Therefore `COLD == 24`, `cold_page_count == 1`,
`directory_len == 16`, `hot_pages_offset == keysets_offset == 4,096`,
`hot_pages_len == 0`, and `file_len == 4,120`. The sole cold descriptor has
`page_index == 0`, `page_len == 24`, and integrity-checks those bytes. Non-empty
tables have no empty hot page. In general, `cold_page_count == 0` exactly when
the combined cold-section length is zero.

### `SeriesHotPageDescriptorV1`

Each descriptor is exactly 16 bytes:

```text
u32 first_series_ref
u32 record_count
u32 page_crc32c
u32 reserved0
```

Descriptor ordinal is the page index. `first_series_ref == page_index * 409`.
Every non-final page has 409 records. The final page has `1..=409` records.
The ranges are contiguous and end exactly at `num_series`. Page offsets are
implicit: `hot_pages_offset + page_index * 16,384`.

### `SeriesColdPageDescriptorV1`

Each cold descriptor is exactly 16 bytes:

```text
u32 page_index
u32 page_len
u32 page_crc32c
u32 reserved0
```

Cold descriptor ordinal after the hot descriptors is `page_index`.
`page_len == 16,384` for every non-final page; the final page has
`1..=16,384` bytes. Its physical offset is implicit:
`keysets_offset + page_index * 16,384`. Ranges are contiguous and end exactly
at `file_len`; there is no final cold-page padding. The CRC covers exactly
`page_len` bytes. A logical keyset, dictionary, or packed-row read validates
and pins every complete cold page intersecting its byte range before returning
any slice, including when a record crosses a page boundary. This is touched-
range integrity check around the unchanged v2 cold byte stream, not a
columnar-label layout.

### `SeriesHotPageV1`

Every page is exactly 16,384 bytes:

```text
SeriesHotPageHeaderV1              // 24 bytes
SeriesHotV3[record_count]           // 40 bytes each
ZeroPadding                         // exact remainder of the page
```

The page header is:

```text
u32 magic              // 'SHP7'
u16 version            // 1
u16 flags              // 0
u32 page_index
u32 first_series_ref
u32 record_count
u32 reserved0          // 0
```

All unused bytes, including all unused final-page record space, are zero. The
descriptor CRC covers the complete 16 KiB page, including padding. A touched
page is CRC-checked before its header or records are interpreted.

### `SeriesHotV3`

Each record is exactly 40 bytes:

| Offset | Type | Field |
| ---: | --- | --- |
| 0 | `u64` | `series_id` |
| 8 | `u32` | `keyset_id` |
| 12 | `u32` | `row` |
| 16 | `u32` | `control` |
| 20 | `u32[5]` | tag-specific payload |

The record's ordinal is its `series_ref`; it is never stored redundantly.
`keyset_id` and `row` must identify one canonical row in the retained v2 cold
label sections. `series_id` plus the decoded full labelset retains the existing
collision-verification contract.

`control` is packed exactly as follows:

| Bits | Meaning |
| --- | --- |
| 0..4 | `kind_mask` for Float, Int64, Histogram, ExponentialHistogram, Summary |
| 5..7 | inline `chunk_kind` |
| 8 | inline `file_id` (`0=chunks.bin`, `1=ooo_chunks.bin`) |
| 9..10 | record tag (`1=INLINE`, `2=OVERFLOW`) |
| 11..31 | inline `scalar_lane_len` (21 bits) |

Tags 0 and 3 are invalid. For an overflow record, `chunk_kind`, `file_id`, and
`scalar_lane_len` are zero. No chunk flags are stored in an inline record; the
integrity-checked chunk header is authoritative.

### Inline payload

An inline record's five payload words are:

```text
u32 min_time_delta_ms
u32 max_time_delta_ms
u32 file_offset
u32 chunk_length
u32 indexed_prefix_crc32c
```

Inline is canonical if and only if:

- the series has exactly one chunk;
- `kind_mask == 1 << chunk_kind`;
- `segment_start_ms + min/max_delta` is checked, ordered, and strictly before
  `segment_end_ms`;
- the chosen chunk-file offset fits `u32` and the complete range lies inside
  that footer-inventoried file;
- `chunk_length` is at least 40 and covers the exact header, optional scalar
  lane, and native payload;
- `scalar_lane_len <= 2,097,151` and is either zero or at least 16;
- the scalar-lane offset is derived as zero when the length is zero and 40
  otherwise; and
- a nonzero scalar lane belongs only to Histogram, ExponentialHistogram, or
  Summary with `SCHEMA_VARLEN` encoding and satisfies the global scalar-lane
  invariants below.

A single `ooo_chunks.bin` chunk may be inline. Any field-width failure uses
overflow; it is never truncated or saturated. A one-chunk record satisfying
every inline predicate must not be encoded as overflow.

### Overflow payload

An overflow record's five payload words are interpreted as:

```text
u64 blob_offset       // absolute in chunk_index.bin v2
u32 blob_len          // exact 32 + body_len
u32 chunk_count
u32 reserved0         // 0
```

The locator must lie wholly inside the root-declared blob region. Its count and
lengths must exactly match the touched blob header. Checked arithmetic requires
`44 * chunk_count <= u32::MAX - 32`. A series uses overflow when it has
multiple chunks, mixed chunk lanes/kinds, or any inline-width exception.
Per-sample typed metadata inside a native/scalar chunk does not force overflow.
Zero chunks is a noncanonical dead series and is rejected.

## `chunk_index.bin` v2

Version 2 contains only overflow blobs. It has no per-series offset directory:

```text
ChunkOverflowRootV2                // 64 bytes
ChunkOverflowBlobV1[blob_count]    // contiguous, variable length
EOF
```

### `ChunkOverflowRootV2`

| Offset | Type | Field | Required value or meaning |
| ---: | --- | --- | --- |
| 0 | `u32` | magic | `CHIX` |
| 4 | `u16` | version | `2` |
| 6 | `u16` | flags | `0` |
| 8 | `u32` | header_len | `64` |
| 12 | `u32` | blob_header_len | `32` |
| 16 | `u32` | overflow_entry_len | `44` |
| 20 | `u32` | series_count | must match `series.bin` |
| 24 | `u32` | blob_count | number of overflow series |
| 28 | `u32` | reserved0 | `0` |
| 32 | `u64` | blobs_offset | `64` |
| 40 | `u64` | blobs_len | exact concatenated blob bytes |
| 48 | `u64` | file_len | `64 + blobs_len` |
| 56 | `u32` | root_crc32c | CRC of 64 bytes with this field zero |
| 60 | `u32` | reserved1 | `0` |

With no overflow series the file is exactly 64 bytes. The root CRC and file
length must equal the values integrity-checked by `series.bin` and the footer.

### `ChunkOverflowBlobV1`

The exact 32-byte header is:

```text
u32 magic              // 'COF7'
u16 version            // 1
u16 flags              // 0
u32 header_len         // 32
u32 series_ref
u32 chunk_count
u32 reserved0          // 0
u32 body_len           // 44 * chunk_count
u32 blob_crc32c        // header with this field zero, then exact body
```

The exact body has no padding:

```text
OverflowChunkEntryV1[chunk_count]  // 44 bytes each
```

Blobs are concatenated without gaps in increasing `series_ref` order. The hot
record, blob header, and computed lengths must agree. `chunk_count >= 1`. Full
validation proves that every blob is referenced exactly once and that the
final blob ends at EOF.

Each overflow entry is:

```text
u8  file_id
u8  kind
u16 reserved0          // 0; integrity-checked ChunkHeader flags are authoritative
u64 min_time_ms
u64 max_time_ms
u64 offset
u32 length
u32 scalar_lane_offset
u32 scalar_lane_len
u32 indexed_prefix_crc32c
```

This preserves the complete schema-6 locator widths and adds header
integrity checking without duplicating aggregate chunk flags. Entries are strictly
ordered and unique by
`(file_id, min_time_ms, max_time_ms, offset)`. Readers validate kind, the
integrity-checked header flags, time ordering, file ID, file bounds, scalar-lane
shape, and header agreement.
Every entry also satisfies
`segment_start_ms <= min_time_ms <= max_time_ms < segment_end_ms`; violating
segment time bounds is corruption, not an overflow escape hatch.
For a non-empty blob, `kind_mask` equals the OR of all entry-kind bits.

Schema 7 defines no series-level metadata sidecar. Schema 6 never implemented
one, and the current typed semantics are already preserved per sample. Every
reserved field above is therefore zero and any nonzero value is touched
corruption. New uniform series semantics that cannot be recovered from the
integrity-checked chunks require a future series/blob/segment version rather than
unversioned bytes in schema 7.

## Indexed chunk-prefix integrity checks

The schema-7 writer stores one external `indexed_prefix_crc32c` in every
inline record or overflow entry. With no scalar lane it covers the exact final
40-byte `ChunkHeader`. With a scalar lane it covers the concatenation of the
40-byte `ChunkHeader` and exact 16-byte `TypedScalarLaneHeaderV1`; it does not
cover the scalar body. No field is zeroed for this calculation. The locator's
integrity-checked scalar-lane length determines whether the indexed prefix is 40
or 56 bytes, so the reader need not trust an unchecked chunk field to choose
the CRC span.

For every inline and overflow locator, scalar-lane shape is canonical:

- `scalar_lane_offset == 0` and `scalar_lane_len == 0`, or
  `scalar_lane_offset == 40` and `scalar_lane_len >= 16`;
- a nonzero lane requires chunk kind Histogram, ExponentialHistogram, or
  Summary and encoding `SCHEMA_VARLEN`;
- `TypedScalarLaneHeaderV1.body_len + 16 == scalar_lane_len` with checked
  arithmetic; and
- the integrity-checked chunk header has `num_points >= 1`.

An invalid existing chunk or locator shape is corruption/replay rejection; it
is not an inline-width failure that the schema-7 writer may hide by selecting
overflow. The writer rejects it before layout classification.

Before interpreting encoding, flags, series reference, counts, lengths,
scalar body length/CRC, or the stored native-payload CRC, the reader verifies
the indexed prefix as raw `[u8; 40]` or `[u8; 56]` bytes. This verification
must run before the existing semantic chunk decoder; a decoder that first maps
kind/encoding or discards header flags is not a substitute. Likewise the raw
16-byte scalar header validator checks magic, version, zero flags, body length,
and bounds before allocating the scalar body or calling a scalar-body decoder.
It then cross-checks:

- header `series_ref`, kind, min/max time, and file range against the locator;
- `length == header_len + payload_len`;
- `scalar_lane_len == header_len - 40` and its canonical offset; and
- header flags against the chunk-kind/version invariants.

The locator range is exact: trailing bytes after
`header_len + payload_len` are corruption, even if an older payload decoder
would otherwise ignore them.

The integrity-checked chunk header contains the native-payload CRC; the
integrity-checked scalar header contains the scalar-body CRC. The indexed prefix
plus those existing body CRCs forms a complete touched-subrecord chain without
changing chunk-frame bytes.

## Reader validation and corruption

Ordinary open first performs a lightweight footer preflight. It requires the
canonical tracked-file inventory: exactly one entry for each `SegmentFile`
tracked by the footer, no duplicate, missing, or unknown entry, canonical
order (`meta.json`, `symbols.bin`, `series.bin`, `chunks.bin`,
`ooo_chunks.bin`, `chunk_index.bin`, `indexes.puffin`), and zero
footer/header/entry reserved fields. Each tracked path is opened without
following a non-regular replacement; `fstat` length must equal
the footer length, and the platform identity of that opened object is captured
before any root or locator is trusted. Manifest-opened segments additionally
match their manifest identity and time range. A later reopen must match this
captured identity.

Ordinary open then validates the series, chunk-index, and index-container fixed
roots, their CRCs, file lengths, canonical section arithmetic, segment bounds,
hot/cold/exact page counts, descriptor ranges, zero root padding, and the
root-to-root count bindings. It does not read every hot page, cold page, exact
page, payload, blob, or full-file footer checksum.

A touched hot page, cold page, overflow blob, exact page, exact-postings
payload, FST, or label-value time-range payload is fully CRC-checked and
structurally validated before it may enter a cache. Any touched parse,
checksum, bounds, ordering, count, locator, substitution, symbol-resolution,
or header-agreement failure returns `InvalidData` or a structural short-read
error. It must never become a cache miss, missing matcher, pruning result,
skipped series, partial result, or empty query result.

Explicit full validation reads every hot page, cold page, exact page,
integrity-checked index payload, and overflow blob, validates every cold label row
and chunk header, proves canonical global blob coverage, and performs normal
footer size/checksum validation. It also derives routing
from the validated exact postings, symbols, and label-value time ranges and
requires byte-equivalent logical entries; then it proves each metric-range
group equals the exact `(__name__, value)` posting and the aggregate
series/chunk kind and time facts. Only that complete same-generation proof may
mint routing or metric-range authority. A structurally valid substitution,
narrowed time range, changed kind mask, missing group, or whole-blob swap is a
validation error even when all local lengths and CRCs remain valid. Full
validation remains outside timed query benchmarks. Footer checksum creation
and validation hash tracked files incrementally using exactly one 1 MiB buffer
per active hash; neither path may use a whole-file read for multi-gigabyte
artifacts. The buffer is charged to in-flight metadata (or the equivalent
replay-writer working-set counter), and its charge appears in the resource
report and replay RSS evidence.

## Deterministic writer

The writer retains metric-query series-ref ordering. It writes chunk frames
first, computes final per-chunk indexed-prefix CRCs, classifies every series
using the canonical inline rules, writes overflow blobs in series-ref order,
then writes the series root/pages/cold sections. Hot-page boundaries depend on
series-ref ordinal; cold-page boundaries depend only on fixed 16 KiB physical
position in the unchanged cold byte stream. All padding is zero. Hot-page,
cold-page, root, and blob CRCs are computed only after every covered byte is
final.

Sealing reads each final 40- or 56-byte indexed prefix exactly once. A bounded
structural prepass selects only series that cannot fit the inline widths, fully
integrity-checks their final prefixes, and retains their overflow blobs. The
series-page output pass fully integrity-checks the remaining single-chunk inline
candidates while retaining at most one hot page. Structural preclassification
is not validation: malformed locators or prefixes must still fail the canonical
classifier, and no malformed candidate may be converted into overflow or an
empty result. Cold bytes are emitted through one canonical 16 KiB page buffer;
this changes write-call granularity but not section boundaries, descriptors,
CRCs, or bytes.

The same seal builds exact postings, FSTs, and label-value time ranges from the
complete authoritative series/symbol inventory. It writes v8 payloads in
canonical key order, computes every exact/auxiliary payload CRC over final
bytes, places the expected count and CRC in its protected directory record,
then stores the final exact/auxiliary directory CRCs and authoritative series
and symbol counts in the v8 trailer. Missing ranges, unresolved values,
inconsistent FST/range inventories, out-of-range refs, or disagreement between
derived and supplied indexes are writer errors. The root-unbound encoder is
test-only.

Repeated replay with identical capture order, writer configuration, and
deterministic ID seed must produce identical segment IDs, manifests, relative
files, and bytes. A width exception deterministically selects overflow; it does
not change series identity or chunk ordering.

## Store-wide metadata governor

Schema 7 replaces per-segment permanent metadata caches with one store-owned
`MetadataGovernor`. The schema-6 read-only A/B adapter uses the same governor;
it may not retain its legacy roots, decoded metadata, or descriptors outside
the aggregate accounting. The initial public configuration is:

```text
retained_max_bytes      // default 64 MiB; zero disables retention
in_flight_max_bytes     // default 256 MiB; must be non-zero
max_open_files          // default 128; hard active cap, must be non-zero
max_cached_open_files   // default 64; idle subset, may be zero, <= hard cap
```

Benchmark and API store-open paths expose all four values before any segment is
opened. A zero retained or cached-file budget changes only retention, never
query semantics.

### Cache and accounting contract

One aggregate LRU covers symbol roots/pages, index roots/directories/pages,
metric ranges, series roots/hot pages/cold metadata, and overflow roots/blobs.
Cache values are typed `MetadataPin<T>` handles backed by an allocation-owned
RAII charge. The charge is attached to the allocation, not the LRU entry.
Keyed resident-hit promotion, oldest eviction, and keyed removal are expected
O(1); no resident hit scans the resident population to recover its recency
position.
Evicting a cache entry therefore does not subtract bytes while a caller still
pins the value.

A keyed live registry retains weak allocation identity while a value is
resident, pinned, or loading. LRU eviction removes only the resident pin and
resident-entry bookkeeping; an externally pinned allocation remains keyed and
is reused rather than loaded and charged twice. Final allocation drop removes
its live key. Live-registry and single-flight bookkeeping are charged and
bounded by live/resident loads, not by every key ever touched.

Accounting uses separate RAII charges for load scratch/working memory, the
final metadata allocation, and resident LRU/single-flight/live-registry
bookkeeping. Before each allocation or growth, the loader reserves a checked
declared upper bound or growth amount against `in_flight_max_bytes`; variable
records are header-bounded before body allocation and unbounded geometric
growth is forbidden. After allocation, owned boxed lengths, `Vec::capacity()`,
strings, decoded arrays, and fixed bookkeeping are measured and reconciled
before publication. If an upward reconciliation is refused, the allocation is
dropped and the query receives an explicit resource error. Reports exclude
allocator slack and state that charged bytes are a logical memory bound, not
RSS.

After validation, one transaction under the governor lock marks scratch
reservations for release and either transfers the final allocation charge from
in-flight to retained after evictions that actually free charges, or leaves it
transient in-flight. Detached charge handles and eviction victims are destroyed
only after unlocking as specified below. There is no uncharged or
double-counted interval. Evicting a pinned value releases only resident-entry
bookkeeping; its allocation remains
retained-charged until the final pin drops. A transient value remains
in-flight-charged until its final pin drops. Pinned allocations are not free
capacity. If retention is zero, full, or the value is individually too large,
the validated value remains usable transiently only when its final charge fits
the in-flight budget. Cache admission failure never means absence; a hard
in-flight refusal is an explicit resource error. Every error path releases all
reservations.

No RAII charge, metadata pin, file permit, file descriptor, waiter, or
single-flight completion is destroyed, closed, or woken while holding the
governor or file-manager mutex that its destructor or callback can re-enter.
Eviction and completion detach victims and waiters to deferred lists while
locked, unlock, perform drops/closes/wakes, then relock and recheck accounting
before transferring a charge. This applies to admission, reconciliation,
rollback, shutdown, and error paths, not only the steady-state LRU.

The metadata governor, sticky-corruption ledger, and file manager have no
nested lock order: code holds at most one of their mutexes. A structural file
error is recorded in the ledger only after all descriptor reservations and
leases for that operation have been cleaned up and no file-manager mutex is
held. Resource/stat snapshots copy each component independently and never
acquire those mutexes together.

Concurrent identical misses are single-flight. No governor mutex is held while
performing I/O or validation. Waiters receive the same validated value or same
load result. Transient operating-system errors are shared only by current
waiters and may be retried later.

### Sticky corruption ledger

The governor owns a non-evictable corruption ledger keyed by
`(stable segment identity, tracked SegmentFile)`. It records the first
structural `InvalidData` or touched `UnexpectedEof` error. Every cache hit and
load checks the ledger
first. Evicting roots, pages, blobs, or file descriptors cannot forget
corruption. Transient OS errors are never promoted to structural corruption.
An entry remains until that identity has left the canonical manifest/inventory
and its final handle, pin, waiter, and load has completed; cache/FD eviction
alone never removes it. Ledger bytes are charged mandatory semantic state and
are therefore bounded by the active plus retiring segment inventory.

### File descriptor governance

Roots and pages retain a `SegmentFileHandle`, never a `File`. A governed file
lease contains the open permit. `max_open_files` caps every distinct live
kernel file descriptor owned by the store, including leased and idle cached
descriptors. `max_cached_open_files` caps only the idle zero-lease subset and is
not additive. Each `GovernedOpenFile` owns exactly one hard-cap permit for its
lifetime; cloning a lease reuses that object. A keyed live table remains
authoritative while any lease exists, independent of the idle LRU. Only idle
zero-lease entries may be evicted. Before opening a new descriptor, the manager
reuses a matching live/idle object or evicts idle objects until capacity is
available. If every slot is leased it waits or returns the defined resource
error while holding no partial lease. A zero cached-file budget opens
transiently and closes after the last lease.

On every reopen, the manager verifies the footer-recorded length and the
platform file identity captured at store open. Replacement after eviction is
sticky corruption. Reads remain immutable positional reads; no shared seek
cursor is introduced.

For a multi-file operation, handles are deduplicated, stably sorted, and
partitioned only when their distinct count exceeds `max_open_files`.
`acquire_many` atomically reserves every additional descriptor slot needed by
one partition before returning any lease. It may evict idle entries first. If
other callers hold the needed capacity, it waits/retries or fails explicitly
with zero partial leases. Opens and identity checks occur outside the manager
mutex against reserved keyed `OPENING` states; any failure closes successful
opens and releases every reservation. A caller may not acquire a second set
while retaining leases outside its first set. This all-or-none reservation,
not partitioning alone, is the deadlock proof. Bound series/chunk-index roots
may be loaded sequentially and compared from governed metadata pins, so a
valid `max_open_files = 1` configuration remains usable.

When idle descriptors are selected as victims for `acquire_many`, their hard
slots remain reserved for that acquisition before the mutex is released.
Victim descriptors are closed outside the mutex, and only then are new files
opened into those reservations. No concurrent acquirer may steal a close-in-
progress slot, so the hard cap is never exceeded and close-before-open is
observable even at `max_open_files = 1`.

Snapshots report current/peak retained and in-flight bytes, cache hits/misses,
single-flight waits, evictions, admission refusals, active/cached/open peaks,
sticky artifacts, and charges by metadata class. Existing symbol counters stay
as a compatible subsection rather than the whole resource picture.

## Query integration

Schema-7 queries obtain hot records by grouping series refs by 16 KiB page.
Inline records immediately produce one planned chunk. Overflow records group
blob reads by physical offset. Label strings remain delayed and are decoded
from integrity-checked cold pages containing the unchanged keyset/value-code byte
stream only when selection, grouping, or result construction requires them.

All equality paths initially use governed exact postings. The facade owns an
optional `MetricRangeAuthority` and `RoutingAuthority`; with neither present,
`__name__` does not read metric ranges and early routing does not prune. The
choice is internal to the schema-neutral facade so evaluator code cannot turn
a summary without local integrity checks into candidates. A matcher is evaluated against the
empty string when its label is absent: equality to `""` and regexes that match
`""` retain absent-label candidates and never use routing/postings absence as
a complete negative proof.

The integration boundary is one `SegmentMetadataReader` containing governed
symbols, one footer-selected index backend, and exactly one series-layout
backend. Its query-local `SegmentMetadataSession` acquires and binds the symbol,
index, and layout roots for one registered generation. The schema-6 benchmark
backend binds its completely footer-validated v7 index, series v2, and
chunk-index v1; the production backend binds v8 index counts and directory
CRCs, series v3, and overflow-index v2. Root-derived capabilities, rather than
caller-supplied counts, authorize cross-file validation. The facade exposes
only schema-neutral operations for symbol lookup/resolution, integrity-checked
exact-postings selection, bounded integrity-checked FST/range visits, series
routing, verified label materialization, and chunk-locator visits. It does not
expose raw metric ranges or layout-specific records to evaluator code.

`route_series` may return only `series_ref`, kind facts, and integrity-checked chunk
locators. `materialize_verified` is the sole operation that may expose a stored
`series_id` with canonical labels; it integrity-checks every intersected cold page,
decodes the complete v2 label row, resolves the required symbols, and verifies
the existing label-byte fingerprint/collision contract. No per-series entry,
label, or chunk-locator map survives the query session; retained state consists
only of governed roots/pages/blobs and bounded facade bookkeeping.

The store must not retain millions of individual series entries or chunk
locators. Page/blob pins and query-local result memory replace the unbounded
per-segment maps. Segment-local symbol and value codes never cross segment
boundaries without canonical-byte verification or query-global remapping.
Segment-local routing may proceed with `series_ref`, kind, and a validated
locator, but a stored `series_id` does not become stable identity until the
referenced canonical label row and required symbols have been materialized and
its label-byte fingerprint has been verified.
Metadata planning is processed in bounded batches: a query releases hot,
cold, symbol, index, and blob pins before requesting a batch whose declared
working set would exceed the remaining in-flight budget. A resource refusal is
never translated into an absent series or partial result.

Every payload request is keyed by `(segment identity, file_id, offset,
length)`. The scheduler batches independently for `chunks.bin` and
`ooo_chunks.bin`; it never drops `file_id` or substitutes the in-order file for
an OOO locator. Cross-segment submission may combine requests only after this
file identity is retained through completion and result routing.

Public semantic `QueryStats`, result series/samples, and exact/portable
fingerprints must match schema 6. Physical metadata-read and resource-profile
fields are expected to differ and must be named in the A/B gate. Because
physical corpus fingerprints necessarily change with the layout, the A/B
harness records them separately and also computes a layout-neutral logical
replay identity; that logical identity must match.

The identity is `chronoxide-logical-replay-v1`, an incremental SHA-256 stream
beginning with the ASCII domain `chronoxide-logical-replay-v1\0`. All integers
are little-endian and every variable byte string has a `u32` byte-length
prefix. The stream has `u32 segment_count`, then segments in manifest order (or
segment-ID order for an explicitly manifestless corpus): segment-ID bytes,
`u64 start_ms`, `u64 end_ms`, and `u32 series_count`. Each series follows in
`series_ref` order as `u32 series_ref`, `u64 series_id`, `u8 kind_mask`,
`u32 label_count`, canonical key-byte/value-byte pairs ordered by key bytes,
and `u32 chunk_count`. Chunks are sorted by `(file_id, min_time_ms,
max_time_ms, kind, SHA-256(exact indexed chunk bytes))`; each contributes
`u8 file_id`, `u8 kind`, `u64 min_time_ms`, `u64 max_time_ms`, `u32 length`,
and the 32-byte chunk digest. Physical metadata offsets, page boundaries,
footer bytes, and metadata-file checksums are excluded. This identity proves
label, lane, routing, and exact unchanged chunk-byte equivalence while
remaining independent of the series/chunk-index layout version. Independent
decoded readbacks remain a separate semantic oracle.

## Version and A/B boundary

- The exact schema-7 reader accepts only homogeneous schema 7; schema 6,
  schema 8, and every other schema are rejected during footer preflight.
- Writers emit footer schema 7, symbols v3, series v3, chunk-index v2, and
  index-container v8 only when `storage_schema = "schema7"` is selected.
  Schema 8 is the default writer and reader contract.
- `chronoxide-query --storage-layout schema6-ab` may read one homogeneous
  schema-6 corpus with paged symbols v3, series v2, and chunk-index v1 only
  when its v7 index and every other tracked artifact have passed complete
  footer checksum validation outside timed queries and remain bound to the
  same opened file identities.
- That schema-6 path binds every selected series span to its exact
  authoritative 16-byte chunk-index directory pair. Routing-only output omits
  `series_id` until canonical labels and the stored fingerprint agree. Because
  v7 exact/FST/range payloads lack local expected CRCs, it retains complete
  final label-predicate verification and never removes candidates from a v7
  range summary alone.
- The benchmark option never enables a production fallback, never accepts a
  mixed manifest, and never changes writer output.
- A schema-7 footer with index-container v7 is invalid. Old corpora migrate
  only through deterministic replay to a new output root; no in-place index
  rewrite or mixed v7/v8 manifest is supported.

## Required coverage

Before the prefix replay, tests must include:

- deterministic series/layout golden bytes for empty, one-record, 409-record,
  410-record, inline, overflow, multi-chunk, mixed-kind, OOO, and cold records
  crossing a 16 KiB integrity-check boundary;
- deterministic v8 index golden bytes for empty, one-entry, 341-entry, and
  342-entry exact directories plus FST-only, range-only, and paired auxiliary
  records;
- exact `u32` time/offset and 21-bit scalar-length boundaries plus one-over
  overflow selection;
- round trips for every OTLP metric kind, including per-sample start time,
  flags, temporality, and reset hints through native and typed scalar lanes;
- root, hot/cold descriptor, hot/cold page, blob, entry, indexed-prefix,
  scalar-body, and native-payload CRC corruption;
- truncation at every fixed and variable structural boundary;
- malformed counts, offsets, lengths, ordering, padding, reserved fields,
  tags, kind masks, scalar lanes, nonzero reserved metadata words, and
  root-to-root substitutions;
- an ordered in-range exact-ref mutation preserving body count and length, and
  an equal-length exact-payload swap between label keys, both rejected by the
  touched CRC instead of changing candidates;
- exact body/directory count disagreement, valid-CRC out-of-range refs,
  trailer-to-series/symbol count mismatch before payload I/O, exact/auxiliary
  directory CRC disagreement, and truncation at each new v8 boundary;
- a structurally valid same-length FST replacement or swap, a time-range
  mutation preserving length/order/count/aggregate summary, FST/range
  item-count disagreement, and an unresolved visited FST value, all producing
  sticky corruption rather than an empty regex or time prune;
- cache hits and opaque pins rejecting substituted v8 record, root, and segment
  generation context, including after metadata and FD eviction;
- aligned schema-6 chunk-span substitution and valid schema-6 label-row
  substitution;
- structurally valid routing bucket/key/time substitutions, incomplete routing
  derivation, metric-name ownership substitution, metric-range boundary and
  kind/time substitutions, and whole routing/metric blob swaps; ordinary
  queries must fall back to exact postings, while full semantic validation must
  reject every disagreement;
- equality and regex matchers that do and do not match the empty string, with
  explicit-empty and absent labels and mixed selective matchers;
- equality, regex, discovery, and time-pruning queries proving touched v8
  payload corruption returns an error rather than a wrong, empty, skipped,
  pruned, or partial result;
- strict schema-7 rejection and benchmark-only homogeneous schema-6 open;
- footer validation and deterministic repeated replay bytes;
- zero/tiny aggregate budgets, global cross-class eviction, pinned-after-
  eviction accounting and same-key reuse, oversize transient values, and lease
  cleanup on error;
- concurrent same-key single-flight, transient retry, and corruption surviving
  metadata/FD eviction;
- active/cached FD limits, concurrent all-or-none multi-file acquisition,
  low-`RLIMIT_NOFILE`, and cross-segment batch splitting;
- OOO-only and mixed-lane queries proving payload reads preserve `file_id`;
- raw-prefix-before-decode, zero scalar-header flags, exact chunk length, and
  trailing-byte rejection;
- exact and portable fingerprints, semantic `QueryStats`, and independent
  readback coverage for scalar, Histogram, ExponentialHistogram, Summary,
  multi-chunk, and OOO data.

## Experiment and acceptance gate

The pre-implementation materiality prerequisite is complete in model v4. It
uses the observed 3,563,222 exact entries, 8,722 v7 pages, 10,458 projected v8
pages, and 33,322 auxiliary entries. The only format deltas applied without an
encoded replay are: exact and auxiliary records grow from 40 to 48 bytes,
exact-page density changes from 409 to 341, the trailer remains 256 bytes, and
exact/FST/range payload bodies retain their existing lengths. The result is a
28,764,752-byte v8 structural charge and a 2,257,877,360-byte net format saving.
These remain modeled values; do not report them or CRC CPU cost as measured
encoded/replay evidence.

Replay only the same two-million-message capture prefix used for the preserved
schema-6 baseline. Run schema 7 twice and require identical segment IDs,
manifests, relative files, and bytes. Full footer validation and independent
readbacks run outside timed queries. Promotion is a no-go if any mandatory
scalar, Histogram, ExponentialHistogram, Summary, multi-chunk, or OOO shape is
covered only by a skipped corpus readback; each shape requires an executed,
isolation-safe independent-oracle equivalent.

Use one identical schema-7 release query binary for schema-6/schema-7 A/B.
Hold the input capture and exact two-million-message prefix fingerprint, query
schedule, limits, projection configuration, range-cache budget, metadata
budgets, FD caps, and cache-eviction procedure constant. Record schema-6 and
schema-7 physical output fingerprints separately and require
`chronoxide-logical-replay-v1` to match. Compare aggregation, raw selector,
equality/regex, no-result, high-cardinality materialization, scalar, native
Histogram, native ExponentialHistogram, Summary, and warm long-lived API
shapes.

Report exact bytes by artifact, v7-to-v8 exact/auxiliary record bytes and exact
page counts, inline/overflow counts, replay/seal time including index CRC work,
cold and warm latency, RSS, retained/in-flight charges, open-file peaks,
page/blob reads split by hot/cold/exact/auxiliary class, logical metadata-used
bytes, issued metadata bytes and metadata read/used amplification, payload
used/read amplification, result shape, fingerprints, and named stats
differences. The host is noisy: correctness, deterministic bytes, capacity,
bounded memory, and FD enforcement are authoritative; latency is exploratory
unless an isolated run is available.

The prefix evidence selects schema 7 as the vNext read-layout candidate: its
measured space win survives encoded replay, its paired PromQL semantics and
complete `QueryStats` match schema 6, and the optimized reader shows no query-
shape regression in the current gate. Final writer promotion still requires
the remaining replay/corruption gates, measured v8 checksum/count overhead,
bounded memory and FDs, and broader isolated query evidence. If any gate fails,
record the no-go before changing the writer default. Do not start a
full-capture replay without explicit user authorization.
