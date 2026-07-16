# Storage vNext Paged Symbols Design

- **Date:** 2026-07-13
- **Status:** Completed schema-6 paged-symbol prototype and deterministic-prefix
  A/B baseline; retained for comparison, not promoted as a final vNext format.
- **Baseline revision:** `ccd7adec97c784de946537f87b567d4dd2b93445`
- **Normative format:** [storage.md](storage.md)

## Decision

The first storage-vNext experiment changes only the segment symbol dictionary:

- segment footer schema advances from `5` to `6`;
- `symbols.bin` advances from version `2` to version `3`;
- symbol strings are split into deterministic, independently checksummed pages;
- the immutable root contains page descriptors and complete first/last-string
  fences;
- readers use positional reads and retain only validated pages in an explicitly
  byte-bounded cache; and
- prior experimental segments are rejected rather than decoded through a
  compatibility path.

`series.bin v2`, `chunk_index.bin v1`, `indexes.puffin v7`, chunk payloads, and
PromQL semantics do not change in this experiment. Symbol IDs remain the
segment-local ordinal positions of the complete dictionary sorted by raw UTF-8
bytes.

This is an alpha format. Migration means deterministically replaying the
capture into a new output directory, not teaching the reader to accept both
formats.

## Experiment disposition

The implementation, focused corruption suite, deterministic two-million-message
prefix replay, full-footer validation, and same-binary query equivalence gate
are complete. The preserved result is documented in
[the prefix report](../../experiments/storage_vnext/2026-07-13-prefix-results.md).
Schema 6 is now the stable A/B baseline for subsequent isolated format
experiments.

The prefix established a useful access-pattern result: the selected query read
7,742,560 symbol bytes with paged v3 instead of 51,891,711 bytes with eager v2,
while retained symbol charge fell from 51,891,823 to 1,350,530 bytes. Exact and
portable semantic fingerprints, complete `QueryStats`, payload accounting, and
logical symbol work matched. Because the host was noisy, the recorded latency
samples remain exploratory.

Schema 6 is not being promoted through a four-run full-capture replay. Its
remaining production-promotion gates—especially aggregate metadata and
open-file governance—are carried into the next independently versioned format
experiment. This disposition does not weaken the schema-6 byte contract below
or authorize a production reader fallback.

## Why this is the first experiment

The measured real corpus contains 235,189,457 bytes of `symbols.bin` data
across 18 segments. A fresh query process currently reads and validates the
complete surviving dictionaries before planning. Across nine repetitions, the
selective scalar query spent a median 99.205 ms in symbol loading while its
selected chunk-payload stage took a median 20.954 ms.

Paging directly targets that demonstrated whole-file cost while leaving
series, postings, chunks, and typed OTLP semantics unchanged. It therefore
produces an attributable A/B result. The experiment is not gated on first
completing every possible current-format optimization; it will be compared
against the pinned existing corpus using identical replay input, queries, and
cache budgets.

## Goals

- Resolve `string -> symbol_id` after opening a small immutable root and
  touching at most one symbol page.
- Resolve `symbol_id -> string` after touching at most one symbol page.
- Detect corruption in every page used to answer a query without validating
  the complete symbol file on the timed hot path.
- Preserve deterministic symbol IDs and byte-for-byte replay output for an
  identical input and writer configuration.
- Bound retained page memory explicitly and keep reads positional and safe to
  share across concurrent query sessions.
- Keep an explicit full-validation operation for smoke, readback, recovery,
  and offline integrity checks.

## Non-goals

- Prefix-compressing strings inside pages.
- Adding a symbol FST or hash index.
- Changing symbol ordering, identity, normalization, or segment scope.
- Changing `series.bin`, postings codecs, metric-series ranges, chunk indexes,
  chunk frames, or typed scalar/native payloads.
- Solving cross-segment label identity. Segment-local IDs still require
  query-global remapping or canonical-byte comparison where labels cross a
  segment boundary.
- Treating page-cache or application-cache warmth as semantic state.
- Fixing replay peak RSS. Replay memory is measured in the A/B but is a
  separate optimization program.

## Binary conventions

All fixed-width integers are little-endian. All offsets in the root and page
descriptors are absolute file offsets unless explicitly described as relative.
All additions, multiplications, and host-size conversions use checked
arithmetic.

The physical order is exact:

```text
SymbolsHeaderV3                    // 80 bytes
SymbolPageDescriptorV3[page_count] // 48 bytes each
FenceBytes                         // complete first/last strings
SymbolPageV1[page_count]           // variable length, no padding
EOF
```

There are no unaccounted gaps, alignment padding, or trailing bytes.

## `SymbolsHeaderV3`

The header is exactly 80 bytes:

| Offset | Type | Field | Required value or meaning |
|---:|---|---|---|
| 0 | `u32` | `magic` | `SYMB` |
| 4 | `u16` | `version` | `3` |
| 6 | `u16` | `flags` | `0` |
| 8 | `u32` | `header_len` | `80` |
| 12 | `u32` | `descriptor_len` | `48` |
| 16 | `u32` | `symbol_count` | Total number of symbols |
| 20 | `u32` | `page_count` | Number of page descriptors and pages |
| 24 | `u64` | `directory_offset` | `80` |
| 32 | `u64` | `directory_len` | `page_count * 48` |
| 40 | `u64` | `fence_offset` | First byte after the directory |
| 48 | `u64` | `fence_len` | Exact length of `FenceBytes` |
| 56 | `u64` | `pages_offset` | First page byte |
| 64 | `u64` | `file_len` | Exact physical file length |
| 72 | `u32` | `root_crc32c` | CRC32C of `[0, pages_offset)` with this field zero |
| 76 | `u32` | `reserved0` | `0` |

The root is the header, descriptor directory, and fence region. CRC
verification treats bytes `[72, 76)` as four zero bytes. The stored page CRCs
therefore become authenticated members of the root.

The section boundaries are canonical:

```text
directory_offset = 80
directory_len    = page_count * 48
fence_offset     = directory_offset + directory_len
pages_offset     = fence_offset + fence_len
file_len         = pages_offset + sum(descriptor.page_len)
```

Schema 6 additionally caps `pages_offset`, and therefore the complete root,
at 67,108,864 bytes. Writers reject a larger root and readers enforce the
limit from the fixed header before allocating or reading the variable root.

For an empty dictionary:

```text
symbol_count = 0
page_count = 0
directory_offset = 80
directory_len = 0
fence_offset = 80
fence_len = 0
pages_offset = 80
file_len = 80
```

No non-empty dictionary may have zero pages, and no empty dictionary may have
pages or fence bytes.

## `SymbolPageDescriptorV3`

Each descriptor is exactly 48 bytes. Descriptor ordinal is the page index.

| Offset | Type | Field | Meaning |
|---:|---|---|---|
| 0 | `u32` | `first_symbol_id` | First segment-local symbol ID in the page |
| 4 | `u32` | `symbol_count` | Number of symbols in the page; non-zero |
| 8 | `u64` | `page_offset` | Absolute file offset of the page |
| 16 | `u32` | `page_len` | Exact encoded page length |
| 20 | `u32` | `page_crc32c` | CRC32C over the complete encoded page |
| 24 | `u32` | `first_fence_offset` | Relative to `fence_offset` |
| 28 | `u32` | `first_fence_len` | Complete first symbol byte length |
| 32 | `u32` | `last_fence_offset` | Relative to `fence_offset` |
| 36 | `u32` | `last_fence_len` | Complete last symbol byte length |
| 40 | `u32` | `string_bytes_len` | Sum of string bytes stored in the page |
| 44 | `u32` | `reserved0` | `0` |

Descriptors are stored in page order. Their symbol-ID ranges and physical page
ranges are contiguous:

```text
descriptor[0].first_symbol_id = 0
descriptor[i + 1].first_symbol_id =
    descriptor[i].first_symbol_id + descriptor[i].symbol_count

descriptor[0].page_offset = pages_offset
descriptor[i + 1].page_offset =
    descriptor[i].page_offset + descriptor[i].page_len
```

The final symbol range ends at `symbol_count`, and the final page ends exactly
at `file_len`.

### Fence bytes

Fences contain complete UTF-8 strings, not prefixes, hashes, or truncated
search keys. The writer appends fence bytes in descriptor order: the complete
first string followed by the complete last string for every page. A singleton
page therefore stores the same string twice. Fence locators must describe this
canonical concatenation exactly, without gaps, overlap, aliasing, or trailing
bytes.

For every page:

```text
symbol_count == 1  => first_fence == last_fence
symbol_count > 1   => first_fence < last_fence
```

using raw UTF-8 byte ordering. For adjacent pages:

```text
previous.last_fence < next.first_fence
```

A singleton page has equal first and last fences. Fence bytes must be valid
UTF-8. Agreement between a descriptor's fences and the actual first/last page
strings is checked whenever that page is validated.

Root-only count/length invariants are also mandatory. A singleton's
`string_bytes_len` equals one fence length. A two-symbol page's length equals
`first_fence_len + last_fence_len`. For larger pages it is at least that sum
plus `symbol_count - 2`, because every strictly ordered interior UTF-8 string
is non-empty. These checks prevent a checksum-valid root from proving a false
absence without touching its page in the cardinalities where the root has
enough information to reject the mismatch.

## `SymbolPageV1`

Each page begins with an exact 32-byte header:

| Offset | Type | Field | Required value or meaning |
|---:|---|---|---|
| 0 | `u32` | `magic` | `SYPG` |
| 4 | `u16` | `version` | `1` |
| 6 | `u16` | `flags` | `0` |
| 8 | `u32` | `page_index` | Descriptor ordinal |
| 12 | `u32` | `first_symbol_id` | Must equal the descriptor |
| 16 | `u32` | `symbol_count` | Must equal the descriptor and be non-zero |
| 20 | `u32` | `offsets_len` | `4 * (symbol_count + 1)` |
| 24 | `u32` | `strings_len` | Must equal descriptor `string_bytes_len` |
| 28 | `u32` | `reserved0` | `0` |

The header is followed immediately by:

```text
u32 local_offsets[symbol_count + 1]
u8  strings[strings_len]
```

Local offsets are relative to the beginning of `strings`. The first offset is
zero, the final offset equals `strings_len`, and offsets are non-decreasing.
The page length is exact:

```text
page_len = 32 + offsets_len + strings_len
```

Symbol `j` in a page is:

```text
strings[local_offsets[j] .. local_offsets[j + 1]]
```

All symbols are valid UTF-8, strictly increasing, and unique by raw UTF-8
bytes. An empty string is representable and, if present, sorts first. The first
and last page strings must exactly equal the descriptor fences.

Pages have no internal or trailing padding. A page CRC covers the page header,
offset table, and string bytes exactly as written.

## Deterministic page construction

The target page size is exactly 32 KiB (`32,768` bytes). It is a deterministic
packing target, not padding and not a hard maximum for an oversized singleton.

Starting from the first symbol not yet assigned:

1. Begin a new page with that symbol.
2. Consider consecutive symbols in sorted symbol-ID order.
3. Append the next symbol while the exact resulting page length remains at
   most 32,768 bytes.
4. Stop before the first symbol that would exceed 32,768 bytes.
5. If the first symbol alone produces a page longer than 32,768 bytes, emit it
   as one oversized singleton page, provided the encoded page is no larger
   than 16,777,216 bytes.
6. Continue until all symbols are assigned.

For `n` candidate symbols with a total of `s` UTF-8 bytes:

```text
encoded_page_len = 32 + 4 * (n + 1) + s
```

Every non-oversized page is greedily maximal. Except for the final page,
adding the next page's first symbol, including its additional four-byte offset,
must exceed 32,768 bytes. An oversized page must contain exactly one symbol.
Every page, including an oversized singleton, is at most 16,777,216 bytes;
writers and readers reject larger pages before allocating their encoded or
decoded page buffers.

The writer performs no content-dependent compression, alignment, or alternate
packing search. Given the same sorted dictionary, page boundaries and every
output byte are identical.

## Root opening and validation

A reader first obtains the physical file length, reads the 80-byte header, and
performs checked preliminary validation before allocating or following a
stored range. It then reads exactly `[0, pages_offset)`, verifies the root CRC,
and parses descriptors and fences.

Root validation rejects at least:

- wrong magic, version, flags, fixed lengths, or reserved values;
- stored `file_len` different from physical length;
- arithmetic overflow or a section boundary outside the file;
- non-canonical section offsets or lengths;
- `directory_len != page_count * 48`;
- inconsistent empty/non-empty counts;
- `page_count > symbol_count` or a root larger than 67,108,864 bytes, rejected
  before variable-root allocation;
- a descriptor page length larger than 16,777,216 bytes, rejected before page
  allocation;
- zero-count pages;
- page count greater than symbol count;
- non-contiguous, overflowing, or incomplete symbol-ID ranges;
- non-contiguous, overlapping, zero-length, or trailing physical page ranges;
- page-length arithmetic inconsistent with the descriptor's symbol and string
  counts;
- fence locators outside `FenceBytes` or non-canonical fence packing;
- invalid UTF-8 fences;
- invalid first/last or adjacent-page fence ordering;
- a non-maximal ordinary page or a multi-symbol oversized page; and
- a root CRC mismatch.

Root success proves that lookup can route to at most one page and that every
page read lies inside the exact file. It does not prove that an untouched page
matches its descriptor; that is the page-validation boundary.

## Touched-page validation

Before parsing a requested page, the reader reads exactly the descriptor's
`[page_offset, page_offset + page_len)` range and verifies `page_crc32c`. Only a
page whose CRC and complete structure validate may enter the page cache.

Touched-page validation rejects at least:

- a short read or CRC mismatch;
- wrong page magic, version, flags, index, IDs, counts, or reserved value;
- header/descriptor disagreement;
- arithmetic overflow or an exact-length mismatch;
- a malformed offset-table length;
- first offset not zero, final offset not `strings_len`, or an out-of-order or
  out-of-bounds offset;
- invalid UTF-8;
- duplicate or out-of-order strings;
- first/last strings different from the root fences; and
- any internal or trailing bytes not described by the page header.

Malformed touched metadata returns `InvalidData`. It must never be translated
to `None`, a cache miss, a segment prune, an empty posting set, or a partial
query result.

An invalid page that no operation touches may remain undiscovered during a
normal query. The explicit full validator catches it.

## Lookup and resolution APIs

The reader API is fallible:

```text
lookup(&str) -> io::Result<Option<u32>>
resolve(u32) -> io::Result<Option<SymbolRef>>
```

`lookup` binary-searches the validated descriptor fences by raw UTF-8 bytes.
If the target lies inside one page's inclusive fence range, the reader loads
and validates that page and binary-searches its strings. A target in a proven
gap between adjacent fences returns `None` without reading a page.

`resolve` locates the descriptor whose contiguous symbol-ID range contains the
ID, loads and validates that page, and slices the corresponding local offset
range. An ID greater than or equal to root `symbol_count` returns `None`.

`SymbolRef` owns or retains an `Arc` to its validated page plus the string byte
range. It must not borrow from a mutable cache guard or expose a reference that
becomes invalid when a page is evicted.

Batch APIs group requested strings or IDs by page, load each required page at
most once, and restore caller order. Full iteration visits pages in descriptor
order and rechecks strict ordering across page boundaries.

The PromQL series-label path uses a separate page visitor rather than retaining
one `SymbolRef` (and therefore one page `Arc`) per requested ID. Transient
request bookkeeping is capped at 65,536 key/value references and 1,024 series
entries; a single series above the reference cap is split into multiple
visitor calls. Those are count bounds, not a bound on final label bytes: the
query result must still own every materialized label string. The visitor stops
at the first missing ID after validating the preceding resolved prefix, so a
later page is not touched and malformed-input error precedence matches scalar
resolution. Successful logical-returned counters count every requested value,
including repeats, which keeps the v2 and v3 benchmark denominators comparable.

## Page cache and concurrency

- The validated immutable root, positional-read file handle, page cache, and
  sticky corruption state are shared across clones of one open segment.
- Each query/session clone retains independent read-stat deltas.
- The cache has an explicit byte budget. Owned decoded page allocations and
  fixed per-entry key/value bookkeeping are charged; allocator and hash-table
  capacity slack is excluded but remains indirectly bounded by the charged
  entry count. A budget of zero permits a read but retains no page afterward.
- Only completely validated pages enter the cache.
- Eviction is an implementation policy such as LRU and has no semantic effect.
- The first structural or checksum corruption detected for a page is sticky.
  Later touches return the same corruption result even if the page would have
  been evicted. Transient operating-system I/O errors are not reclassified as
  structural corruption.
- Concurrent cache misses for the same page should share one load or otherwise
  remain race-free and return identical validated bytes.
- Reads use immutable positional I/O. No clone shares or mutates a seek cursor.
- The symbol-page cache participates in the open-segment/session metadata
  budget and must not create an unbounded cache per query.

The experimental implementation uses a fixed 256 KiB cache for each touched
segment. That is sufficient to measure the format and access pattern, but it
is not the final production governance model: many simultaneously open
segments can still multiply that allowance. Promotion requires one explicit
aggregate open-segment/session metadata governor, including a runtime zero
budget for uncached measurements. Until then, the reported aggregate retained
charge and process RSS are acceptance gates rather than merely diagnostics.

The prototype reports retained resources from one store-level inventory, not
by summing query-session clones. Shared reader states are identity-deduplicated.
For each unique state the snapshot records the represented `symbols.bin` file
bytes, encoded v3 root bytes, decoded root charge, retained eager-v2 dictionary
charge for the benchmark-only comparison backend, validated-page charge and
capacity, and retained open-file count. Decoded root charge is the retained root
object plus its decoded descriptor and complete-fence allocations. A snapshot
error is explicit; it is not converted into a zero charge.

At minimum, query profiles record:

- root opens, physical bytes, and validation time;
- successful logical values and UTF-8 bytes returned to callers;
- logical page requests;
- page-cache hits and misses;
- physical page reads and bytes;
- page validation time and validated bytes;
- deduplicated root, eager-dictionary, and resident/charged page-cache bytes;
- retained symbol file descriptors; and
- touched corrupt pages.

Reporting the complete `symbols.bin` file length is useful inventory, but it
must not be reported as bytes physically read by a paged query.

## Full validation

`validate_all` first validates the root, then reads every page in descriptor
order and applies touched-page validation; an already cached immutable page may
serve as proof that the page was previously read and validated. The explicit
`SegmentReader::open_validated` path uses a zero-byte page cache, so that pass
physically reads and validates every page. Complete validation additionally
proves actual cross-page string ordering and exact first/last fence agreement
across the dictionary through page/fence agreement.

Full symbol validation is part of explicit footer/readback validation and is
outside timed query benchmarks. Footer schema 6 continues to carry the strong
whole-file size/checksum inventory; root and page CRC32C values provide the
local integrity boundary needed by lazy reads.

## Query integration requirements

- Early routing in `indexes.puffin` remains available before symbol-root open.
- A surviving segment opens and validates only the symbol root, not every
  page.
- Selector lowering propagates symbol lookup errors. A corrupt required page
  cannot become an absent matcher.
- Exact and metric-series planning continue using the unchanged segment-local
  symbol IDs.
- Regex/FST enumeration may produce symbol IDs without opening symbol pages;
  pages are resolved only when canonical string bytes are required.
- PromQL series label materialization uses the bounded page visitor described
  above and writes decoded strings directly into their final label vectors.
- Aggregations and cross-segment operations must not compare segment-local IDs
  directly. They resolve canonical bytes or remap to a verified query-global
  identity.
- Index-backed metadata discovery batches symbol resolution and preserves
  sorted unique output semantics. The no-label-value-index fallback and the
  independent smoke/readback oracle resolve one series incrementally, retaining
  at most the current key/value references rather than a whole-series set of
  page owners.
- Smoke/readback paths use the same paged reader and expose executed/skipped
  diagnostics. They must not restore eager whole-file loading as a shortcut.

## Writer requirements

The sealing writer may retain its current in-memory interner and sorted symbol
remap. After final symbol IDs are assigned, it writes v3 pages in symbol-ID
order using the deterministic greedy algorithm.

The writer must:

- reject more than `u32::MAX` symbols;
- reject a root larger than the schema-6 67,108,864-byte operational limit;
- reject an encoded page larger than the 16,777,216-byte operational limit
  before allocating its page buffer;
- reject page, directory, fence, or file fields that exceed their encoded
  widths;
- compute every page CRC over final page bytes;
- build canonical descriptors and fence bytes;
- compute the root CRC only after all root fields, descriptors, fences, page
  offsets, page lengths, and page CRCs are final; and
- emit no padding or trailing data.

Segment publication, deterministic segment IDs, footer creation, and manifest
ordering remain unchanged except for schema version 6.

## Version and rejection boundary

- Schema-6 writers emit only `symbols.bin v3`.
- Schema-6 readers accept only symbol version 3.
- Readers preflight the footer schema at segment open even when complete footer
  checksum validation is disabled. Schema 5 and every other schema version are
  rejected before query planning.
- A v3 symbol reader rejects v2 rather than falling back to eager decoding.
- One explicit read-only benchmark path,
  `chronoxide-query --experimental-storage-layout-ab`, may accept a homogeneous
  schema-5 corpus and eagerly validate/materialize `symbols.bin v2`. It is not
  enabled by production/API readers or writers and never acts as a schema-6
  fallback. This makes strict layout A/B possible with one identical release
  binary while preserving the production rejection boundary.
- Existing v2 readers reject the v3 version field when they touch
  `symbols.bin`; experimental corpora must live in separate output roots so an
  older process is never expected to interpret a schema-6 manifest.
- No mixed v5/v6 manifest is accepted by the vNext experiment.
- Regeneration is deterministic replay from the preserved capture with the
  identical writer configuration and deterministic segment-ID seed.

## Completed implementation slices

1. Extract symbol storage into a focused module while preserving the writer's
   in-memory interner.
2. Implement deterministic v3 encoding, root parsing, page parsing, positional
   reads, and focused byte/corruption tests.
3. Add the shared bounded validated-page cache, sticky corruption state,
   cloneable readers, and per-clone read statistics.
4. Change the segment writer to emit v3 and schema 6.
5. Replace eager `Arc<SegmentSymbols>` query state with the paged reader and
   make lookup/resolution failures propagate through lowering, matcher
   planning, projection, label materialization, discovery, smoke, and reports.
6. Add cheap schema preflight to ordinary segment open and retain complete
   footer/full-page validation as a separate explicit operation.
7. Run the deterministic prefix replay gate and same-binary prefix A/B.

The expected Rust touchpoints are concentrated in:

- `chronoxide-core/src/storage/series.rs` for extraction and re-export;
- a new focused `chronoxide-core/src/storage/symbols.rs`;
- `chronoxide-core/src/storage/segment/writer.rs`;
- `chronoxide-core/src/storage/segment/layout.rs` and `footer.rs`;
- `chronoxide-core/src/storage/segment/query_types.rs`, `query_reader.rs`, and
  `query_context.rs`;
- query lowering/helpers/projection and metadata-discovery paths; and
- `chronoxide-query` smoke/readback and benchmark reporting.

## Required tests

### Deterministic bytes and round trips

- Golden bytes for empty, singleton, ordinary multipage, exact-32-KiB-boundary,
  one-byte-over-boundary, and oversized-singleton dictionaries.
- Page boundaries remain identical across repeated encodes.
- Round trip all symbol IDs and lookups, including the empty string, multibyte
  UTF-8, and page-boundary strings.
- Batched lookups/resolutions match scalar operations and preserve caller
  order.
- v2 input and non-schema-6 segments are explicitly rejected.

### Root corruption

- Every magic/version/flag/fixed-length/reserved field.
- Root CRC and stored/physical file-length mismatch.
- Overflowing or non-canonical section arithmetic.
- Count/directory mismatches.
- Missing, overlapping, gapped, reordered, or out-of-file page ranges.
- Missing, overlapping, aliased, trailing, invalid-UTF-8, or incorrectly
  ordered fences.
- Non-contiguous symbol IDs, total-count mismatch, non-maximal pages, and
  invalid oversized pages.

### Page corruption

- Page CRC, magic, version, flags, reserved field, index, first ID, and count.
- Offset-table length, first/final offset, ordering, bounds, and exact page
  length.
- Invalid UTF-8, duplicate/out-of-order strings, fence disagreement, and
  trailing bytes.
- Swapped pages and a valid page referenced by the wrong descriptor.
- A corrupt untouched page does not fail an unrelated lookup, but
  `validate_all` finds it.
- A lookup routed to a corrupt page returns `InvalidData`, never `None`.
- Detected corruption remains sticky after valid-page cache eviction.

### Integration and semantics

- Segment writer/read/query round trips for every OTLP metric kind.
- Exact, regex, negative, missing-label, and metadata-discovery queries across
  page boundaries.
- Cross-segment grouping where equal strings have different segment-local IDs.
- Independent readback fingerprints and complete footer validation.
- Repeated deterministic replay produces identical schema-6 segment IDs,
  manifests, relative file lists, and bytes.

## Replay and query A/B

The experiment uses the same capture, host, toolchain, release mode, writer
configuration, deterministic ID seed, query schedule, limits, and explicit
cache budgets for the schema-5 baseline and schema-6 candidate. Binary hashes
and both revisions are recorded because this is a code-version comparison.

Run a deterministic prefix replay twice per format before the full capture.
Within one format, require identical segment names, manifest bytes, relative
file hashes, corpus fingerprint, counters, and time range. Cross-format byte
and corpus fingerprints are expected to differ.

After full replay:

- run `chronoxide-query --verify-readbacks --validate-segment-footers` outside
  timed measurements and inspect executed/skipped diagnostics;
- inventory bytes by artifact and report root, descriptor, fence, page-header,
  offset, and string bytes separately;
- run fresh-process queries after evicting and verifying residency for every
  segment artifact, not only `chunks.bin`;
- run the same suite against a long-lived API process under an explicit symbol
  page-cache budget; and
- alternate baseline/candidate order across repetitions.

Report cold and warm latency, root/page physical reads, logical returned UTF-8
bytes, page hit rate, page-read/logical-used amplification, deduplicated root,
dictionary, and page-cache charges, retained symbol file descriptors, process
RSS, replay throughput, seal latency, bytes by artifact, payload I/O, result
counts, semantic fingerprints, and explained `QueryStats` differences.

The primary acceptance signal is a material reduction in selective-query
symbol bytes and latency without unbounded retained memory or semantic drift.
File-size savings are useful but secondary. A regression in broad scans may be
acceptable only when quantified and outweighed by production query shapes.
