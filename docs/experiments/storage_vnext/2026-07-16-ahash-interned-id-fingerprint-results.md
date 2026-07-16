# AHash interned-ID fingerprint results

Date: 2026-07-16

## Decision

Keep the change. A per-store keyed AHash fingerprint over already-interned
`(key_id, value_id)` pairs reduces real-corpus replay CPU and elapsed time with
no material RSS cost. Exact ordered row equality remains authoritative, so the
fingerprint only selects collision-chain candidates and cannot change label-set
identity.

The previous SipHash implementation remains available as
`experimental_flat_interned_siphash` for diagnostic comparisons. The normal
`flat_interned` and experimental paged layouts now use AHash.

## Change

`FlatInternedLabelSetStore` now owns one randomly keyed `ahash::RandomState`.
The generic and prepared-OTLP paths stream the interned symbol-ID pairs into a
hasher built from that state. No hash value is persisted, included in segment
ordering, or exposed as a stable identifier.

This preserves the existing collision policy:

- a fingerprint hit always performs full ordered `(key_id, value_id)` equality;
- unequal rows sharing a fingerprint remain in the collision chain;
- forced-collision tests cover insertion and lookup;
- differential tests compare canonical-string hashing, SipHash, and two AHash
  stores with different fixed seeds over deterministic and randomized traces.

Per-store random keys retain collision-flood resistance appropriate for this
in-memory lookup hint. A fixed non-keyed fast hash was deliberately not used.

## Method

- Capture:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001/partition-1.capture`
- Raw result root:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/labelset-ahash-ab-20260716-201548`
- One identical release binary selected both variants through configuration:
  SHA-256 `74e169c36cebf161352808fb3ad8dfa50ba2b04fbd30aee0709b9535845d1cc3`,
  build ID `b9beb2f151d7dfc7ec7e1dc2eeab75c76eb2fff2`.
- Both variants used schema 8, the same capture, deterministic segment-ID seed,
  explicit capture cache eviction, and the same `perf stat` event set.
- The short 250k schedule was SipHash, AHash, AHash, SipHash.
- The confirmation schedule was one adjacent SipHash/AHash pair at one million
  messages.

The host was noisy. Both short AHash runs overlapped an Android build, including
`clang++` processes consuming 100-300% CPU; one later SipHash run also
overlapped that build. Their wall time and cycle ordering is therefore not a
valid latency comparison. Retired instructions were stable enough to motivate
the longer adjacent confirmation pair, which overlapped the same single-thread
Android build for both variants.

## Results

### Focused 24-label repeated-hit benchmark

The latest Criterion sample measured median lookup latency of 662.9 ns for
AHash and 667.4 ns for SipHash, a 0.67% reduction. An earlier sample favored
AHash by about 2.9%; the microbenchmark is supporting evidence only.

### 250k messages, mean of two runs per variant

Despite the asymmetric host load, AHash retired 1.36% fewer instructions and
1.21% fewer branches than SipHash. AHash wall/task time and cycles were worse in
this block because only its two runs overlapped the heaviest Android compiler
work, so those elapsed-time counters are rejected rather than interpreted as a
regression.

### One million messages

| Metric | SipHash control | AHash candidate | Difference |
| --- | ---: | ---: | ---: |
| Wall time | 153.69 s | 150.20 s | **-2.271%** |
| Task clock | 153,787.53 ms | 150,064.56 ms | **-2.421%** |
| Cycles | 845,131,830,282 | 827,903,606,068 | **-2.039%** |
| Instructions | 1,739,526,647,649 | 1,693,581,663,263 | **-2.641%** |
| Branches | 311,676,233,800 | 304,291,135,517 | **-2.370%** |
| Branch misses | 1,546,181,952 | 1,535,294,150 | **-0.704%** |
| Page faults | 2,867,769 | 2,867,763 | -0.0002% |
| Peak RSS | 8,572,980 KiB | 8,573,428 KiB | +448 KiB (+0.005%) |

The million-message workload computed 38,747,141 fingerprints over 781,408,899
label pairs. It performed 33,532,270 exact equality matches, with zero equality
mismatches and zero collision inserts. The reduction is useful to both replay
and live Kafka ingestion because the shared OTLP label interner owns this work.

## Correctness gates

- Every short-run manifest was byte-identical within the 250k schedule.
- The one-million SipHash and AHash manifests were byte-identical, with digest
  `c57bd2970b615958820edced252694180bede6d57ab898d4e864cefff5b70bfd`.
- Both variants recorded 38,680,023 samples and 5,214,871 series with identical
  symbols, typed datapoint counts, and event-time rejection counts.
- Candidate footer validation was requested and effective for all four
  segments.
- Independent readback verification executed 38 queries with zero skips and
  zero mismatches.
- Focused core interner tests and ingester configuration/processor tests pass.
