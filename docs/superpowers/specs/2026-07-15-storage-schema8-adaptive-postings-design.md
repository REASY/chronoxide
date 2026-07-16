# Storage schema 8: adaptive exact postings

- **Date:** 2026-07-15
- **Status:** Accepted as the default production sealed-store format. Alternating
  four-million-message schema-7/schema-8 replays were same-schema byte
  deterministic, and a replay with no schema option was byte-identical to an
  explicit schema-8 replay. Exhaustive postings, footer, independent-readback,
  direct-PromQL, and HTTP equivalence gates passed. Schema 7 remains available
  only as an explicit prior-format comparator.
- **Baseline:** storage schema 7 at `b29f2a2`
- **Normative storage contract:** [storage.md](storage.md)
- **Materiality evidence:**
  [storage read-layout review](2026-07-13-storage-read-layout-review.md)

## Decision

Storage schema 8 changes only the exact-postings encoding and its explicit
version boundary:

- `symbols.bin` remains version 3;
- `series.bin` remains version 3;
- `chunk_index.bin` remains version 2;
- chunk frames and payloads are byte-identical to schema 7;
- `indexes.puffin` advances from version 8 to version 9; and
- each exact-postings list deterministically selects raw little-endian `u32`
  references or delta unsigned LEB128, whichever is strictly smaller.

The query facade continues to receive a governed decoded `u32` reference set.
Encoded-domain intersection, union, difference, and iteration are separate
future experiments. This keeps the first result attributable to the storage
format rather than combining it with a selector-engine rewrite.

The footer advances from schema 7 to schema 8. A changed postings payload is
never published as index-container v8 or under footer schema 7. Old corpora are
regenerated through deterministic replay; no compatibility or in-place rewrite
is required.

## Materiality and intended claim

The full 18-segment reference corpus contains 3,922,789,036 exact-postings
bytes across 3,563,222 lists. A historical deterministic sample estimated
delta unsigned LEB128 at 26.8% of the raw bytes, but its machine-readable
output and reproducer were not retained. That estimate is prioritization
evidence only. Schema 8 must produce a saved full-corpus inventory and measured
encoded output before claiming a compression ratio.

The fresh four-million-message schema-7 corpus contains 1,412,247,800 postings
bytes in a 6,598,909,818-byte corpus. If the historical ratio transfers, the
arithmetic projection is approximately 1.03 GB saved, or 15.7% of the complete
corpus. This is a capacity and process-issued-I/O hypothesis. Existing common
metric-range queries read no exact postings, and the prior sparse-regex case
read only about 2.05 MB, so schema 8 does not claim an end-to-end latency win.

The initial implementation and measured evidence are recorded in the
[Schema-8 adaptive-postings result](../../experiments/storage_vnext/2026-07-15-schema8-adaptive-postings-results.md).
On the complete eight-segment, four-million-message corpus, exact postings are
72.90% smaller, `indexes.puffin` is 57.71% smaller, and the complete corpus is
15.60% smaller. An exhaustive integrity-checked walk matched all 1,290,200 decoded
posting lists and 351,771,750 references across the actual v8/v9 corpora. Warm
cached postings-only latency changed ordering across the noisy runs and is
classified as neutral; there is no end-to-end latency claim.

## Preserved semantics

Schema 8 does not change:

- event-time placement, capture-time policy, replay ordering, or deterministic
  segment IDs;
- symbol IDs, series-ref assignment, series identity, label encoding, routing
  keys, FSTs, label-value time ranges, or metric ranges;
- native/scalar chunks, typed OTLP metadata, PromQL projections, or query
  results;
- exact-postings logical membership, ordering, uniqueness, or root bounds;
- the 48-byte exact-directory record, 16 KiB exact pages, directory CRC chain,
  payload CRC, aggregate governor, sticky-corruption ledger, or immutable
  positional-read policy; or
- the decoded postings ownership and query-facade API.

The routing payload's `exact_postings_blob_len` does change to the exact schema-8
encoded length. The same-seal validator must derive and compare that value with
the selected codec; retaining the schema-7 raw-length formula is invalid.

## Explicit non-goals

The first experiment does not add Roaring, StreamVByte, SIMD-BP128, run
containers, inline singleton postings, encoded-domain set operations, or
postings block indexes. It does not reduce the writer's in-memory logical
postings inventory. It does not claim that fewer encoded bytes are operating-
system cache misses or storage-device reads.

## `indexes.puffin` v9

Container v9 retains the v8 canonical gap-free physical region order, fixed
16-byte header, fixed 256-byte trailer, locators, root counts, directory CRCs,
48-byte exact records, 48-byte auxiliary records, and auxiliary payload bytes.
The explicit differences are:

- header and trailer `version == 9`;
- terminal magic is `S9ND`;
- the exact directory uses magic `EXD9`, version 3;
- exact pages use magic `XPG9`, version 3; and
- exact-postings payload v3 is defined below.

All other header, directory, record, page, locator, and trailer fields retain
their v8 offsets and sizes. Auxiliary directory v2 (`AUX8`) is byte-identical
because no auxiliary semantics change.

### Exact-postings payload v3

The protected `ExactDirectoryRecordV2.ref_count` remains the authoritative
non-zero decoded count. Its `postings_len` is the exact complete payload length,
and `payload_crc32c` covers every payload byte.

```text
ExactPostingsPayloadV3:
  u8 codec       // 0 = RAW32, 1 = DELTA_ULEB128
  u8 flags       // 0
  u16 reserved   // 0
  u8 body[]
```

`RAW32` body:

```text
u32 series_ref[ref_count]
```

Every integer is little-endian. The complete RAW32 payload length is therefore
`4 + 4 * ref_count`, exactly matching the schema-7 v2 payload length because
the new four-byte header replaces the old four-byte count.

`DELTA_ULEB128` body:

```text
uLEB128 first_series_ref
uLEB128 positive_gap[ref_count - 1]
```

The first value is absolute. Every later value is the checked sum of the
previous reference and one strictly positive gap. Unsigned LEB128 is canonical:
the shortest representation is required; truncated, overlong, overflowing, or
trailing bytes are corruption.

For each list, the writer computes the complete RAW32 and DELTA_ULEB128 lengths,
including the shared four-byte header. It selects DELTA_ULEB128 only when its
length is strictly smaller; RAW32 wins ties. The reader enforces the same rule
after decoding, so a valid-but-noncanonical codec choice is corruption rather
than an alternate encoding.

Before reading a payload, an exact record must satisfy checked bounds
`4 + ref_count <= postings_len <= 4 + 4 * ref_count`. After payload CRC
verification and before publication, the reader requires:

- known codec, zero flags, and zero reserved bytes;
- exactly `ref_count` references and complete body consumption;
- canonical codec selection;
- strictly increasing and unique references; and
- every reference less than the same-generation root `series_count`.

CRC verification precedes parsing and allocation. The aggregate governor
continues to reserve decoded `u32` capacity before allocation, while the
positional payload read is charged by its encoded length. Cache values retain
the complete root, protected record, codec-validated decoded values, and
segment-generation provenance.

## Deterministic writer and routing

Codec selection depends only on the ordered reference values. It is independent
of allocation, hash iteration, host architecture, thread schedule, and buffer
size. Exact payloads remain in canonical key order.

The writer derives routing metadata with the same codec policy before encoding
the routing payload. Same-seal validation recomputes the selected length for
every exact key and rejects missing, extra, or raw-length routing metadata.
The final exact record, routing entry, payload bytes, and CRC must all agree.

Repeated replay with identical input, ordering, writer configuration, and
deterministic ID seed must produce identical segment IDs, manifests, relative
paths, and bytes.

## Required coverage

Before a replay gate, focused tests cover:

- golden bytes and round trips for singleton, RAW32, DELTA_ULEB128, tie, and
  the unsigned-LEB128 boundaries 127/128, 16,383/16,384, and `u32::MAX`;
- deterministic selection and repeated encoding;
- unknown codec, non-zero flags/reserved, CRC mismatch, truncation, trailing
  bytes, overlong/noncanonical varints, varint overflow, zero gap, checked-add
  overflow, duplicate/decreasing/out-of-range references, and noncanonical
  RAW/delta selection;
- locator lower/upper bounds derived from `ref_count`;
- protected-record, root, and generation substitution, including cache and FD
  eviction; and
- same-seal routing-length disagreement.

Existing v8 golden and corruption tests remain unchanged and continue proving
schema-7 rejection of v9 bytes.

## Promotion evidence and retained acceptance gates

The promotion run used one frozen release build and the same four-million-message
capture in schema-7/schema-8/schema-8/schema-7 order. Both same-schema pairs
were byte-identical, the segment IDs and replay counters matched across schemas,
and the no-schema-option replay was byte-identical to the explicit schema-8
output. Independent readbacks executed 38 of 38 checks with zero mismatches or
skips for both formats. The controlled direct-query A/B passed 88 canonical
comparisons over 11 query shapes, and the 11-shape HTTP gate matched the direct
query fingerprints. An additional no-schema-option native
ExponentialHistogram range query and HTTP query selected schema 8 and matched
their explicit-schema fingerprints and logical statistics.

The workspace test suite, formatting, focused Python model tests, and shell
syntax checks passed. Strict clippy was run and reported the pre-existing
workspace lint backlog; dependency advisory checking was unavailable because
neither `cargo-deny` nor `cargo-audit` was installed. `promtool` was unavailable.
These limitations are recorded rather than treated as successful checks.

Retain the following as the regression gates for future changes to this format:

1. Save a machine-readable complete postings inventory for the selected corpus:
   list count and length distribution, raw bytes, candidate delta bytes, codec
   counts, and corpus fingerprint.
2. Run focused codec/runtime tests, formatting, clippy, and the relevant
   workspace tests; report any unavailable tool or pre-existing failure.
3. Replay the same prefix of at least four million messages in alternating
   schema-7/schema-8 order with one code revision and fixed configuration.
   Repeat both schemas and require same-schema byte-identical output. Also
   require a no-schema-option replay to be byte-identical to explicit schema 8.
4. Require identical logical replay identity, independent readbacks with no
   skips, footer validation, result shapes, logical query statistics, and
   semantic fingerprints.
5. Run cold and warm direct and HTTP queries for long-list equality, multi-equality
   intersection, sparse and broad regex unions, negative matchers, no-result,
   scalar and typed histogram paths, and a metric-range control which reads
   zero postings.

Reports separate exact-postings, `indexes.puffin`, metadata, and total bytes;
codec distribution; replay/seal wall and CPU time; peak RSS; encoded postings
bytes read; decoded logical refs; cold/warm latency; and governor charges.
`index_postings_bytes_read` is expected to decrease. Other semantic
`QueryStats`, result shapes, and fingerprints must match unless a named physical
encoded-byte field is explicitly classified before the run.

The initial capacity gate is at least 65% fewer exact-postings bytes with no
total-size regression. The exploratory latency guard is no greater than a 2%
warm median regression on postings-heavy queries and the zero-postings control
when host noise permits that resolution. A capacity-only win may still be
accepted when any latency result is explicitly neutral and decode CPU remains
operationally bounded.
