# AHash symbol-fingerprint results

Date: 2026-07-16

## Decision

Keep the change. A per-symbol-table keyed AHash fingerprint reduces real-corpus
ingest CPU and elapsed time without changing segment bytes or peak RSS in a
material way. This path is shared by replay and live Kafka ingestion; it is not
part of capture-file Zstd decoding.

The previous SipHash implementation remains available as
`experimental_flat_interned_siphash_symbols` for controlled diagnostics. The
normal `flat_interned` store uses AHash for both symbol and label-set lookup
fingerprints.

## Correctness model

The symbol fingerprint is an in-memory lookup hint only. It is neither
persisted nor used as a symbol or series identifier. A fingerprint hit still
requires complete string equality, and unequal strings with the same
fingerprint remain in the existing collision chain. Therefore changing the
fingerprint implementation cannot merge distinct symbols.

Each `ArenaSymbolTable` owns a randomly keyed `ahash::RandomState`. Cloning a
table preserves its keys and lookup behavior. Focused tests cover:

- deterministic symbol-ID assignment across SipHash and AHash tables with
  different fixed AHash keys;
- clone behavior;
- forced collisions, including primary and collision-chain lookup;
- the existing size and arena-capacity errors.

## Method

- Capture:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001/partition-1.capture`
- Raw result root:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/symbol-ahash-ab-20260716-213143`
- One identical release binary selected both variants through
  `ingestion.labelset_store`:
  - SHA-256
    `9b29f18d7f4154f769519381d7bc321f97963856da8e97321d2bbc00d604747e`;
  - build ID `c1423f2071bba240022d6b38e2a5eba5083b252d`.
- Both variants used schema 8, the same capture, deterministic segment-ID seed
  42, identical encodings, and explicit `POSIX_FADV_DONTNEED` capture-cache
  eviction before each run.
- The short 250k schedule was SipHash, AHash, AHash, SipHash.
- The confirmation schedule was one adjacent SipHash/AHash pair at one million
  messages.
- Every run recorded `perf stat`, `/usr/bin/time -v`, load, processes, cache
  residency, and byte-level segment manifests.

The host was comparatively quiet: pre-run one-minute load averages ranged from
0.64 to 1.35, with `btop` around 8% CPU and the IDE around 6%. No build or other
large measured workload overlapped these runs.

## Results

### 250k messages, mean of two runs per variant

| Metric | SipHash control | AHash candidate | Difference |
| --- | ---: | ---: | ---: |
| Wall time | 57.91 s | 57.16 s | **-1.295%** |
| Task clock | 57,885.45 ms | 57,097.12 ms | **-1.362%** |
| Cycles | 322,635,344,956 | 318,161,652,857 | **-1.387%** |
| Instructions | 790,622,926,742 | 773,269,413,570 | **-2.195%** |
| Branches | 142,704,442,556 | 141,110,553,878 | **-1.117%** |
| Branch misses | 591,707,567 | 573,549,565 | **-3.069%** |
| Peak RSS | 5,480,546 KiB | 5,480,282 KiB | -264 KiB (-0.005%) |

### One million messages

| Metric | SipHash control | AHash candidate | Difference |
| --- | ---: | ---: | ---: |
| Wall time | 134.73 s | 131.37 s | **-2.494%** |
| Task clock | 134,789.35 ms | 131,421.18 ms | **-2.499%** |
| Cycles | 752,660,791,604 | 733,739,312,390 | **-2.514%** |
| Instructions | 1,696,647,129,711 | 1,626,459,737,663 | **-4.137%** |
| Branches | 305,119,361,260 | 298,654,800,647 | **-2.119%** |
| Branch misses | 1,515,456,248 | 1,429,188,147 | **-5.693%** |
| Page faults | 2,867,898 | 2,867,798 | -0.003% |
| Peak RSS | 8,576,436 KiB | 8,574,412 KiB | -2,024 KiB (-0.024%) |

Both one-million-message runs accepted 38,747,141 datapoints, recorded
38,680,023 samples, assigned 5,214,871 label sets and 359,520 symbols, and
reported identical label-set equality/collision counters. No symbol or
label-set fingerprint collision occurred in the corpus.

## Correctness gates

- All four short-run segment manifests were byte-identical.
- The one-million SipHash and AHash segment manifests were byte-identical, with
  digest
  `c57bd2970b615958820edced252694180bede6d57ab898d4e864cefff5b70bfd`.
- Candidate footer-integrity validation was requested and effective for all
  four segments.
- Independent readback verification executed 38 queries with zero skips and
  zero mismatches, covering scalar, classic Histogram, ExponentialHistogram,
  and Summary projections.
- Focused core symbol-table, ingester configuration, and processor comparator
  tests pass.
