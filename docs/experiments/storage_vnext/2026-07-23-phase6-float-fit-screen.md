# Phase 6 Float codec fit screen

**Status:** accepted capacity screen; not a runtime codec promotion gate.

This screen measures exact RawF64, Gorilla, and per-chunk adaptive encoded
sizes over one fresh current-format four-million-message corpus. It does not
compare replay or query performance between physical RawF64 and Gorilla
corpora. Gorilla remains the production default.

## Result authority

- Result root:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase6-float-fit-current-4m-20260723T044808Z`
- Source commit: `f0f8a5c4c10d880c81aafc443cc1d5b2f8c1834f`
- Storage verifier SHA-256:
  `5e7f12aeb95f0dc7e4a27b4abbbe8122e7d329d73678a4a10f37fb1bfe6f4adc`
- Corpus: 5,569,245,002 bytes, 66 files, eight segments
- Stored samples: 154,901,989 across 17,286,074 chunks
- Float population: 141,374,001 points across 14,878,867 chunks
- Raw result: `storage-verify.json`
- Derived decision: `float-fit-summary.json`
- Result seal: `metadata/result-artifacts.sha256` plus the read-only
  `FLOAT_FIT_COMPLETE` marker

The old Phase 1 four-million-message corpus was not reused. The current reader
correctly rejected that pre-canonical experimental corpus with
`histogram bucket total overflows u64`. A fresh replay with the current writer
rejected 625 malformed typed Histogram datapoints at ingest and produced a
corpus that passes current storage semantics. Experimental-format backward
compatibility is not inferred from this result.

## Exact Float fit

| Candidate | Indexed bytes | Native payload bytes | Complete-corpus effect vs Gorilla |
| --- | ---: | ---: | ---: |
| Gorilla | 1,391,047,673 | 795,892,993 | baseline |
| RawF64 | 2,236,954,635 | 1,641,799,955 | +845,906,962 bytes, +15.1889% |
| Per-chunk adaptive minimum | 1,390,686,234 | 795,531,554 | -361,439 bytes, -0.00649% |

RawF64 is 106.284% larger than Gorilla at the Float-payload level and 60.811%
larger after the common indexed chunk header is included. Its possible decode
CPU advantage therefore costs 806.72 MiB, or 15.19% of the complete corpus.

The adaptive policy saves only 352.97 KiB over all-Gorilla. That is 0.0454% of
the Float payload, 0.0260% of Float indexed bytes, and 0.00649% of the complete
corpus. This is an upper-bound capacity result; it does not charge any new
policy/version complexity or provide the exhaustive streamed per-block
sidecar required by the Phase 6 promotion contract.

| Outcome | Chunks | Chunk share | Points | Point share |
| --- | ---: | ---: | ---: | ---: |
| Gorilla smaller | 12,643,753 | 84.978% | 137,761,095 | 97.444% |
| RawF64 smaller | 169,146 | 1.137% | 1,272,494 | 0.900% |
| Equal bytes | 2,065,968 | 13.885% | 2,340,412 | 1.655% |

Equal-byte chunks select RawF64 under the frozen evidence rule. They do not
contribute capacity savings. The existing one-byte chunk encoding field already
distinguishes RawF64 and Gorilla inside the 40-byte common header, so a Float
selector does not require another byte; the measured adaptive benefit is still
too small to justify a policy change.

The corpus contains 68,771,233 positive-zero Float points and 91,036,067
repeated-XOR transitions. It has 71,515,878 finite non-zero values, 1,083,412
ordinary NaNs, 3,478 positive infinities, and no negative zero, negative
infinity, or exact stale-NaN points. This distribution explains why Gorilla's
advantage is much stronger at four million messages than in the earlier 250k
allocator corpus.

## Correctness

- Exhaustive decode, segment-footer validation, and exact-postings validation:
  pass.
- Exact postings: 1,290,200 lists and 351,771,712 decoded references.
- Ordered decoded semantic fingerprint:
  `acf6b1299174feb76e75f0007413f5427cbef819907858a6dfff71e0e95cb8f7`.
- Topology-independent decoded semantic fingerprint:
  `e3e8176514b3ebce64f02bb4f7964caac804146166272aa12122f7c1c67159e7`.
- Independent readback oracle: 32 expected, 32 executed, zero skipped, zero
  isolation skips, and zero mismatches.

The replay took 8m29.13s with 11,371,468 KiB maximum RSS. The exhaustive fit
and correctness scan took 5m00.52s with 198,152 KiB maximum RSS while pinned to
CPU 31. These are single-policy operational observations, not Raw/Gorilla A/B
performance evidence.

## Decision

1. Retain Gorilla as the default sealed Float codec.
2. Defer adaptive RawF64/Gorilla selection. A 0.00649% complete-corpus saving
   cannot justify additional policy, audit, and testing complexity.
3. Treat an all-Raw runtime A/B as low priority. Run it only if a measured
   Float decode bottleneck can plausibly justify a 15.19% corpus-size increase;
   this fit alone cannot accept or reject that CPU/space trade.
4. Make no storage-version or reader/writer change from this capacity screen.
5. Evaluate timestamp candidates separately. Their estimates were collected
   by the same streaming verifier pass, but they are not part of this Float
   decision.
