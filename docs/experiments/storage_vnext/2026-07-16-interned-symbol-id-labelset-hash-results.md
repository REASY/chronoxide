# Interned-symbol-ID label-set fingerprint result

- **Date:** 2026-07-16
- **Status:** Promoted to the normal `flat_interned` path. The legacy
  canonical-string fingerprint remains available as
  `experimental_flat_interned_canonical_string_hash` for controlled
  comparisons.
- **Raw same-binary A/B:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/labelset-hash-guarded-20260716-172014`
- **Post-promotion validation:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/labelset-hash-final-default-20260716-180851`

## Change

The flat label-set store already interns every normalized label name and value
before its final series lookup. The old path then hashed the normalized UTF-8
strings a second time. The new path fingerprints the ordered store-local
`(key_symbol_id, value_symbol_id)` pairs instead.

The fingerprint is still only a lookup hint. A hit is accepted only after an
exact comparison of the complete ordered symbol-pair row. Collision-chain
members are checked the same way. Prepared OTLP symbol caches are scoped by a
unique store ID, so symbol IDs from one store cannot be reused in another.
Nothing from this fingerprint is persisted or exposed as a stable identity.

The implementation retains normalization before first symbol insertion,
clears scratch state on failures, and records a successful fingerprint only
after an existing row is found or a new row is appended. Focused coverage
includes empty and wide rows, first/middle/last differences, forced collision
chains, raw-versus-normalized overlength UTF-8, generic/prepared cross-path
deduplication, cross-store symbol-ID differences, partial prepared-cache
failure and retry, and a deterministic randomized differential trace against
the canonical-string store.

## Workload and method

The same-binary comparison replayed the first one million messages from the
real `kafka-capture-001/partition-1.capture` corpus. It used Schema 8,
900-second segments, deterministic segment seed 42, and the C-I-I-C schedule:

1. canonical-string control;
2. interned-ID candidate;
3. interned-ID candidate;
4. canonical-string control.

Each accepted attempt started after `POSIX_FADV_DONTNEED`; `fincore` reported
zero resident capture pages. One frozen release binary selected the policy at
runtime. Its SHA-256 was
`4cf5778575be2b8318de81645d19560e482d5585a8495ec5da9cfb478765a005`
and its ELF build ID was `657b5a85a30b89a6b6ee40d800c27dda55f14b3c`.
The Git head was `cbc65acb8aaf23e1182eb3ccd7dc22cb178fce9c` and the
measured working-diff SHA-256 was
`66c1ca7ff59f47f186a6a8d7f7590613dbbc75e89d4dc33202d967f35c4758a5`.

The host was shared and noisy. Initial strict attempts were excluded when
unrelated Cargo jobs appeared. The accepted block recorded host activity but
disabled builder/idle rejection because a clean two-minute interval was not
available. Runtime builders appeared in both candidate runs and the final
control. Mean observed host idle was nevertheless balanced at about 94.4% for
both policies, and the two candidate instruction counts differed by only
0.0065%. Exact outputs and counters are authoritative; timing is strong
directional evidence from one balanced block, not a confidence-qualified
latency estimate.

## Result

Balanced means compare the two middle candidate positions with the two outer
controls and cancel linear order drift.

| Metric | Canonical strings | Interned IDs | Change |
| --- | ---: | ---: | ---: |
| Wall time | 154.850 s | 144.950 s | -6.393% |
| Process task-clock | 154.983 s | 145.037 s | -6.418% |
| CPU cycles | 863.315 B | 808.443 B | -6.356% |
| Instructions | 1.953578 T | 1.734516 T | -11.213% |
| Branches | 342.049 B | 310.821 B | -9.130% |
| Branch misses | 1.738 B | 1.537 B | -11.561% |
| Branch-miss rate | 0.5080% | 0.4944% | -0.0136 pp |
| Peak RSS | 8,670,136 KiB | 8,670,874 KiB | +0.0085% |
| Reported processing time | 60.275 s | 50.324 s | -16.510% |
| Reported label interning time | 33.416 s | 23.739 s | -28.960% |
| Derived non-intern build time | 26.860 s | 26.585 s | -1.023% |

Both candidate positions beat their linearly interpolated controls by
6.28-6.55% task-clock and 11.21% instructions. The 9.952-second reduction in
reported processing time nearly equals the 9.947-second task-clock reduction,
which localizes the gain to the intended ingest path. RSS did not materially
change.

The measured trace explains the size of the win:

- 38,747,141 successful label-set lookups;
- 781,408,899 fingerprinted label pairs, about 20.17 pairs per lookup;
- 33,532,270 exact existing-series hits, or 86.54% of lookups;
- 5,214,871 new series;
- zero fingerprint collisions and zero equality mismatches.

## Correctness gates

All four A/B runs had identical ingest totals and buffer accounting. Each
produced four segments, 34 files, and 1,584,337,371 bytes. The complete sorted
per-file SHA-256 manifests were byte-identical, with manifest digest
`c57bd2970b615958820edced252694180bede6d57ab898d4e864cefff5b70bfd`.

The standalone Schema 8 verification enabled full footer validation and ran
all 38 independent readback cases: 38 executed, zero skipped, zero isolation
skips, and zero mismatches.

After promotion and review hardening, a fresh release build used plain
`labelset_store = "flat_interned"`. It logged
`labelset_hash=interned_ids`, reproduced the exact reference manifest, and
again passed all 38 footer/readback checks. Its task-clock was 145.047 seconds
with 1.734944 trillion instructions, consistent with the measured candidates.
That final binary's SHA-256 is
`c43ecf7b756d66e625195ae1f5f6f8d1d4878eb227c6a30634dea8964793d531`
and its ELF build ID is `ec1c0cb9d0d2047a8cb3fa80c678549ad3a4c8eb`.

## Decision

Use interned-symbol-ID fingerprinting as the default flat-store strategy. It
removes repeated UTF-8 hashing from the dominant repeated-series path, has a
large and mechanistically consistent CPU benefit, preserves exact collision
verification, produces byte-identical storage, and has no material RSS cost.
Keep the canonical-string policy only as an explicit experimental regression
control.
