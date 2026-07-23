# Phase 6 timestamp codec fit screen

**Status:** accepted capacity-model screen; no timestamp format change is
authorized.

This result evaluates four byte-exact candidate models over every native
timestamp stream in the fresh current-format four-million-message corpus from
the [Float fit screen](2026-07-23-phase6-float-fit-screen.md). The verifier
calculated both inventories in one streaming pass; this report makes a separate
timestamp decision.

## Scope and authority

- Result root:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase6-float-fit-current-4m-20260723T044808Z`
- Raw evidence: `storage-verify.json`
- Derived evidence: `timestamp-fit-summary.json`
- Completion marker: `TIMESTAMP_FIT_COMPLETE`
- Population: 154,901,989 points across 17,286,074 chunks
- Complete corpus: 5,569,245,002 bytes
- Scope: native-payload timestamp streams only
- Tie priority: current offset, adjacent delta, delta-of-delta, fixed-step

Each candidate includes the eight-byte first timestamp. The verifier excludes
a new codec selector, alignment, checksums, migration metadata, common chunk
headers, and the duplicate timestamps in typed scalar lanes. Candidate bytes
are therefore fit evidence, not a complete proposed file layout.

## Exact aggregate fit

| Candidate | Native timestamp bytes | Saving vs current | Complete-corpus saving |
| --- | ---: | ---: | ---: |
| Current offset ULEB | 565,806,666 | baseline | baseline |
| Adjacent-delta ULEB | 529,317,330 | 36,489,336 | 0.6552% |
| Delta-of-delta ZigZag ULEB128 | 352,959,130 | 212,847,536 | 3.8218% |
| Fixed-step residual bitpack | 346,941,335 | 218,865,331 | 3.9299% |
| Per-chunk adaptive minimum, selector excluded | 311,637,112 | 254,169,554 | 4.5638% |
| Adaptive plus a dense two-bit selector | 315,958,631 | 249,848,035 | 4.4862% |

The current encoding is never the unique winner and is selected for zero
chunks by the frozen adaptive rule. Fixed-step residual bitpacking is the best
single global candidate, but it beats global delta-of-delta by only 6,017,795
bytes, or 0.1081% of the complete corpus. Decode cost can easily dominate that
small difference, so both must be implemented in a non-production prototype
before choosing a physical format.

A dense four-way selector costs `ceil(17,286,074 * 2 / 8) = 4,321,519` bytes.
After charging that illustrative sidecar, adaptive selection still saves
30,982,704 bytes over global fixed-step and 249,848,035 bytes over the current
format. A versioned composite encoding in the existing chunk header could
avoid a separate sidecar, but no such encoding-ID or migration design has been
specified. Zero selector bytes must not be assumed.

## Why the candidates win

| Timestamp shape | Chunks | Current | Adjacent | Delta-of-delta | Fixed-step | Adaptive minimum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single point | 2,904,212 | 26,137,908 | 23,233,696 | 23,233,696 | 23,233,696 | 23,233,696 |
| Constant zero step | 77,018 | 969,473 | 892,455 | 892,455 | 770,180 | 728,388 |
| Constant positive step | 4,658,132 | 146,548,103 | 141,597,284 | 81,441,849 | 55,875,914 | 55,054,470 |
| Variable step | 9,646,712 | 392,151,182 | 363,593,895 | 247,391,130 | 267,061,545 | 232,620,558 |

Fixed-step wins the regular long streams; delta-of-delta wins much of the
variable-step population. The adaptive selections are:

| Selected codec | Chunks | Points | Unique-win chunks | Unique-win points |
| --- | ---: | ---: | ---: | ---: |
| Adjacent delta | 4,624,180 | 17,233,173 | 253,505 | 10,346,250 |
| Delta-of-delta | 5,425,152 | 50,186,840 | 4,242,983 | 40,707,572 |
| Fixed-step | 7,236,742 | 87,481,976 | 7,236,742 | 87,481,976 |

There are 5,552,844 tied-minimum chunks. Aggregate winner totals are not enough
to audit a literal adaptive policy or to evaluate a cheaper two-codec policy.
The next evidence tool must stream one row per physical chunk with candidate
sizes, timestamp shape, selected codec, tie outcome, fixed-step value, residual
bit width, kind, value encoding, and point count.

## Typed scalar-lane exclusion

Typed scalar lanes occupy 361,401,536 bytes in this corpus. Their duplicate
current-format timestamp streams account for exactly 54,998,719 bytes, but
those bytes remain unchanged under every candidate above. A future scalar-lane
redesign could remove or recode that duplication, but its possible savings are
outside this screen and must not be added to the table.

## Correctness boundary

The shared verifier pass completed exhaustive decoding, footer validation,
exact-postings validation, and both decoded semantic fingerprints. The
independent query oracle executed 32 of 32 expected readbacks with zero skips,
isolation skips, or mismatches. Those checks validate the current corpus and
the deterministic size calculations; they do not test a candidate timestamp
reader or writer, because none exists yet.

## Decision and next experiment

1. Prototype global fixed-step residual bitpacking first. It is the best single
   capacity candidate and has a plausible SIMD-friendly decode path.
2. Implement delta-of-delta ZigZag ULEB128 in the same benchmark as the
   mandatory comparator. Its 0.1081% corpus disadvantage may be worthwhile if
   it has lower encode/decode cost or simpler range handling.
3. Keep adjacent-delta as a model/control, not the first implementation target.
4. Defer adaptive selection until the per-block sidecar can evaluate fixed-only,
   delta-of-delta-only, two-codec, shape-rule, and fully adaptive policies after
   real selector costs.
5. Measure encode and full/scalar decode cycles, branch/cache misses, range
   startup, cold/warm queries, and replay/seal CPU and RSS. Include corruption,
   round-trip, deterministic-byte, replay-equivalence, and readback tests.
6. Before any persisted candidate is emitted, specify a versioned byte layout
   and encoding tag in `docs/superpowers/specs/storage.md`. This fit does not
   authorize an on-disk change.
