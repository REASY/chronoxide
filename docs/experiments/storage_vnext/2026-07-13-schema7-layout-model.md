# Schema-7 inline-series layout materiality model

## Scope

This is the required read-only materiality gate before implementing storage
schema 7. The model scanned the existing schema-5 real-data corpus at:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/
  segments-replay-20260711-141105
```

The previously recorded corpus fingerprint is:

```text
b9c1470b99726c3f6a53591bf5ec7fb8f96b0691f474e6935a27fce6de145891
```

Model version 4 opens `meta.json`, `series.bin`, `chunk_index.bin`,
`indexes.puffin`, `chunks.bin`, and `ooo_chunks.bin` read-only. It validates the
series/chunk-index fixed headers and section bounds, every chunk-index directory
delta and locator, every proposed inline-width condition, and each v7 index
header, trailer, exact-directory root, and auxiliary-directory root needed for
exact v8 capacity accounting. It verifies chunk-file sizes and locator bounds
but does not parse each referenced `ChunkHeader`, read every exact-directory
page or index payload, or validate `footer.bin` file checksums; those are
implementation/replay gates, not claims of this capacity model. It does not
write below the corpus root.

Raw machine output is preserved at:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/
  storage-series-layout-model-v8-20260714-113324/model.json
```

Its SHA-256 is:

```text
f5bd76efde6ba3f36ae5a4c0aae8ed73b642fa153fb50fb2a1c3d84df52cd5f0
```

The model was run from commit
`8118f0d98abc76eb796ea2f1f87a862d4ec0a6fb`; the exact script SHA-256 was
`b40793149f797fe0a5595fbc7b6e08bca15d1d1164aad30e8c33d6f027d51fe5`.

## Observed shape

| Measure | Exact result |
| --- | ---: |
| Segments | 18 |
| Series | 47,766,209 |
| Chunks | 47,766,209 |
| Zero-chunk series | 0 |
| One-chunk series | 47,766,209 |
| Multi-chunk series | 0 |
| Maximum chunks per series | 1 |
| Series with metadata sidecar bytes | 0 |
| Selected-layout inline-eligible series | 47,766,209 |
| Selected-layout overflow series | 0 |

The zero sidecar count is canonical for the measured writer: `series.bin` v2
never implemented nonzero `meta_len`. Typed Histogram, ExponentialHistogram,
and Summary samples instead carry start time, OTLP flags, temporality, and
reset hints inside both native values and scalar lanes. The schema-7 design
keeps those authoritative bytes unchanged and does not force their redundant
duplication into a series-level sidecar.

Every selected-layout fit check passed. There were no segment-relative `u32`
time failures, `u32` file-offset failures, one-bit file-ID failures, scalar-lane
shape failures, 21-bit scalar-lane-length failures, chunk lengths shorter than
the 40-byte chunk header, or exact single-kind-mask mismatches. The observed
maxima were:

| Field | Maximum |
| --- | ---: |
| Chunk min/max delta from segment start | 899,999 ms |
| Chunk file offset | 1,449,297,167 bytes |
| Chunk length | 659,719 bytes |
| Scalar-lane length | 35,715 bytes |

The existing index flags use their full `u16` representation, but schema 7
does not narrow or duplicate them. The independently authenticated chunk header
remains authoritative for flags.

## Size model

The conservative screen keeps a 32-byte series record plus a 24-byte inline
chunk descriptor. It projects an isolated 1,528,518,832-byte reduction in
`series.bin + chunk_index.bin` (27.83%), but it is not the selected schema-7
layout. The selected design combines label location and the inline descriptor
into one 40-byte hot record, stored in fixed authenticated pages. It also
authenticates the unchanged cold keyset/dictionary/block byte stream with one
16-byte CRC descriptor per exact 16 KiB physical range and includes the exact
v7-to-v8 index-container structural delta.

| Measure | Reference corpus | Schema-7/v8 model | Net change | Effect |
| --- | ---: | ---: | ---: | ---: |
| `series.bin + chunk_index.bin` | 5,490,383,885 | 3,203,741,773 | -2,286,642,112 | 41.64% smaller |
| `indexes.puffin` | 4,918,064,629 | 4,946,829,381 | +28,764,752 | 0.58% larger |
| Metadata artifacts | 10,643,653,467 | 8,385,776,107 | -2,257,877,360 | 21.21% smaller |
| All standard segment artifacts | 21,545,107,020 | 19,287,229,660 | -2,257,877,360 | 10.48% smaller |

The selected exact projection is composed of:

| Selected component | Bytes |
| --- | ---: |
| Existing cold keyset/dictionary/block bytes | 1,286,955,981 |
| 116,798 fixed 16 KiB hot pages | 1,913,618,432 |
| Hot-page descriptors | 1,868,768 |
| 78,558 exact cold-page descriptors | 1,256,928 |
| Authenticated 4 KiB root-alignment padding | 37,344 |
| Schema-7 series roots | 3,168 |
| Overflow-only chunk-index roots | 1,152 |
| Overflow blobs and entries | 0 |
| **Projected series + chunk index** | **3,203,741,773** |

The v8 index-container correction is:

| Index measure | Observed v7 | Projected v8 | Change |
| --- | ---: | ---: | ---: |
| Exact entries | 3,563,222 | 3,563,222 | 0 |
| Exact records per page | 409 | 341 | -68 |
| Exact pages | 8,722 | 10,458 | +1,736 |
| Exact-page physical bytes | 142,901,248 | 171,343,872 | +28,442,624 |
| Exact-page descriptors | 279,104 | 334,656 | +55,552 |
| Auxiliary entries | 33,322 | 33,322 | 0 |
| Auxiliary-record bytes | 1,332,880 | 1,599,456 | +266,576 |
| Complete `indexes.puffin` bytes | 4,918,064,629 | 4,946,829,381 | +28,764,752 |

The exact-page physical row already contains its record bytes and page padding;
it is not added a second time. All unchanged index headers, payload bodies,
routing, metric ranges, directory headers, and trailer bytes account for the
remaining 4,773,551,397 bytes.

The fixed series pages contain 166,920 bytes of final-page zero padding across
all segments; that padding is already included in the page total. The absolute
all-artifact totals describe this schema-5 corpus and hold its `symbols.bin` v2
bytes constant; they are not measured schema-6 or schema-7 corpus totals.
Schema 6 and schema 7 both use `symbols.bin` v3, so the modeled byte delta
transfers to their A/B even though the absolute symbol total does not. The
`series.bin` v2, `chunk_index.bin` v1, and index-v7 baseline bytes transfer
directly to schema 6. The all-artifact total is 15,496 bytes larger than the
earlier five-large-file inventory because this tool also counts `meta.json`,
`footer.bin`, and any empty optional standard artifacts. The per-artifact map in
the raw JSON makes the scope explicit.

## Decision

The v8-aware materiality gate passes. The selected layout removes a projected
2,286,642,112 bytes from `series.bin + chunk_index.bin`; after the exact
28,764,752-byte v8 structural charge, the net projected saving is
2,257,877,360 bytes. That is 21.21% of modeled metadata and 10.48% of all
modeled standard artifacts, with every observed series on the inline path and
no dependence on a lossy narrowing assumption. This is large enough to justify
the isolated schema-7 implementation.

This is capacity and layout evidence, not a read-latency claim. Roots,
descriptors, alignment, page padding, overflow roots, and general overflow
encoding and v8 directory growth are included. CRC computation CPU, encoded
schema-7 bytes, actual writer cost, query latency, RSS, cache behavior, and
file-descriptor behavior remain unmodeled and require the deterministic prefix
replay and same-binary schema-6/schema-7 A/B. The host is noisy, so future
elapsed-time results remain exploratory unless the host is isolated.
