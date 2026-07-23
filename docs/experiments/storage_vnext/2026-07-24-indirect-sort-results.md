# In-memory indirect metric-order sort result

**Status:** promoted. The flat-interned seal path now sorts compact series
indices and shared projection metadata instead of cloning a complete owned
PromQL ordering key for every series. The accepted real replay reduced peak
RSS by about 957 MiB and wall time by 8.89% while reproducing every storage
byte and semantic fingerprint.

## Decision

Promote the indirect sorter for `FlatInternedLabelSetStore`.

On the accepted 250,000-message replay prefix:

- GNU maximum RSS fell by 979,948 KiB (956.98 MiB, 19.0385%);
- Heaptrack requested-live maximum fell by 886,429,743 bytes
  (845.365 MiB, 16.486%);
- whole-process allocation calls fell by 11,832,558 (4.535%);
- retired instructions fell by 7.8216%;
- replay wall time fell by 8.8866%;
- the complete 4,407,610-series head-window write fell by 15.7722%;
- its `seal_decode_ms` fell by 73.4896%; and
- every replay counter, storage byte, decoded semantic fingerprint, exact
  posting, independent readback, and PromQL result matched.

The downstream `writer_flush_ms` timer increased by 3.0536% in the two
official adjacent pairs. That local movement is real evidence and must not be
hidden. It does not reverse the aggregate result: the complete window became
4.276 seconds faster and the whole replay became 4.290 seconds faster.

## Change under test

The former flat-store fast path built an owned ordering key per series. It
cloned projected label arrays and `Arc` strings before sorting 4.4 million
rows. The replacement keeps:

- four compact structure-of-arrays columns: normalized metric rank, kind mask,
  shared projection-plan ID, and source `SeriesRef`;
- one `u32` argsort vector;
- one shared canonical projection plan for each exact source keyset;
- lexical rank tables for normalized metric names, normalized label names,
  and source label values; and
- borrowed immutable access to the original flat-interned label rows.

The per-series sort state is approximately 13 bytes of columns plus a 4-byte
index, apart from shared plans and rank tables. Shared tables are dropped
before the move-based final reorder.

The comparator retains the exact existing order:

1. normalized metric name;
2. persisted sample-kind mask;
3. canonical projected labelset;
4. source `SeriesRef`; and
5. original input index.

The original index makes the comparator a total order, so
`sort_unstable_by` is valid. The final reorder moves the original
`SeriesSamples`; it does not clone payloads.

The old owned-key implementation remains test-only as an independent
differential oracle.

This is an in-memory representation change. It changes neither the Schema 8
byte layout nor its semantics, so no storage-format version or `storage.md`
update is required.

## Measurement contract

- Control source:
  `4e1a779b39a7d5dc63816c2ff125366192358ab8`
  (`perf(storage): borrow canonical cold-series rows`)
- Control binary SHA-256:
  `4d7e06acf7f9de1bb67c4857d5c91e4c15f5c9953b39ec85f69ff84bb44ce093`
- Candidate binary SHA-256:
  `66b4af46b2fd33f08dbe2fffcabba4546ed04db9458f4ca774691c087000eaea`
- Candidate source delta: sealed as `metadata/candidate.patch`
- Workload: the exact accepted 250,000-message capture prefix
- Official schedule:
  `control A, candidate A, candidate B, control B`
- Observations: two per version
- CPU set: all 32 logical CPUs
- Capture residency before every timed arm: exactly zero bytes
- Writeback: explicitly quiescent before and after every timed arm
- QEMU: absent and forbidden by the harness
- Unrelated build, replay, profiler, and footer-validation work: absent from
  the measured process snapshots

This is a controlled same-host regression screen, not a claim about every
possible corpus or machine.

The complete evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/indirect-sort-ab-20260724-TnLvyb`

## Official ABBA result

Means use two observations per version. Negative deltas favor the candidate.

| Metric | Control mean | Candidate mean | Candidate versus control |
| --- | ---: | ---: | ---: |
| Retired instructions | 759,471,346,528 | 700,068,783,843 | -7.8216% |
| Perf task clock | 48,243.370 ms | 43,907.735 ms | -8.9870% |
| Replay wall | 48.275 s | 43.985 s | -4.290 s (-8.8866%) |
| Cycles | 268,004,871,142 | 244,105,044,632 | -8.9177% |
| Branches | 138,427,619,022 | 130,811,905,869 | -5.5016% |
| Branch misses | 490,840,278 | 484,093,240 | -1.3746% |
| Cache references | 10,090,444,476 | 8,697,952,128 | -13.8001% |
| Cache misses | 1,361,705,126 | 1,301,545,903 | -4.4179% |
| Minor faults | 1,487,388 | 1,283,194 | -204,195 (-13.7284%) |
| GNU maximum RSS | 5,147,186 KiB | 4,167,238 KiB | -979,948 KiB (-19.0385%) |
| Monitored tree RSS | 5,196,318 KiB | 4,233,230 KiB | -963,088 KiB (-18.5340%) |
| Kernel HWM | 5,147,186 KiB | 4,168,596 KiB | -978,590 KiB (-19.0121%) |
| Full head-window elapsed | 27,111.0 ms | 22,835.0 ms | -4,276.0 ms (-15.7722%) |
| `seal_decode_ms` | 6,463.5 ms | 1,713.5 ms | -4,750.0 ms (-73.4896%) |
| `writer_flush_ms` | 11,969.5 ms | 12,335.0 ms | +365.5 ms (+3.0536%) |

Absolute branch and cache misses fell, although their rates per branch and
per cache reference rose because the candidate retired substantially less
work.

Both adjacent pairs reproduced the primary improvements:

| Adjacent pair | Instructions | Task clock | Wall | Max RSS | `seal_decode_ms` | `writer_flush_ms` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Candidate A / control A | -7.8089% | -9.3956% | -9.2394% | -978,780 KiB | -73.1549% | +2.9333% |
| Candidate B / control B | -7.8342% | -8.5766% | -8.5323% | -981,116 KiB | -73.8243% | +3.1745% |

A reverse BAAB extension was started because of the local flush-timer
movement. Candidate C and control C completed with exact bytes, but an
unrelated workspace build appeared during the post-arm checks and the harness
stopped before control D/candidate D. The incomplete extension is preserved
but excluded from the official means.

## Heaptrack evidence

Heaptrack ran outside all timed observations. The control trace is the frozen
trace for the exact control binary.

| Heap measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Requested-live maximum | 5,376,967,439 B | 4,490,537,696 B | -886,429,743 B (-16.486%) |
| Whole-process allocation calls | 260,899,230 | 249,066,672 | -11,832,558 (-4.535%) |
| Dominant metric-order allocation stack | 8,956,447 calls | 35,430 calls | -99.6045% |
| Final leaked bytes | 414,748 B | 414,748 B | unchanged |

The filtered control report attributes 3.22 GB at the old process peak to the
owned metric-order allocation stack. No allocations from the replacement
sorter remained live at the candidate's process-wide peak. This statement is
about attribution at each global peak, not a claim that the replacement
sorter's own phase maximum is zero.

Candidate Heaptrack trace SHA-256:

`303ca93e4263eec36220893172d52d5c06df4b7fb4e781466cb8a577938d5a57`

Frozen control Heaptrack trace SHA-256:

`c99313b7529b699a646cb295b62468a499f573a7b7678b61f58413ccddf57811`

## Correctness and storage equivalence

Every official timed arm and the profiled replay accepted 250,000 messages and
stored 9,634,809 samples. Every corpus matched exactly:

- 34 files and 972,969,365 bytes;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
- storage-selection fingerprint
  `797b04acdce65589fbe81116a7623ff586bf8f3b8ebd5aa1af9e42ea03dce5a0`;
- decoded-semantic fingerprint
  `871776d4a17106af13cbdaf69c4680dc40b1a5c9af82e7992615c50074cfcb49`;
- 313,963 exact postings lists and 89,285,049 decoded references, with
  fingerprint
  `00da9eb2c8b3660d9a23cc9d1ce1a265ae81ffe654ac20e27aa43a23cb78977c`;
- footer and exhaustive storage validation passed;
- independent readbacks executed 40/40 with zero skips, isolation skips, or
  mismatches; and
- all 14 PromQL rows matched fingerprint
  `a75234c7dfc296bc69899bdec2d9a3c6cccdb23060b2d5a78484fe7bc478345f`.

The focused tests compare the indirect result against the complete owned
reference order. They cover:

- missing, empty, invalid, and normalization-colliding metric names;
- an entirely empty labelset;
- normalized label-name collisions with last-source-row-wins semantics;
- every sample kind and the persisted kind-mask order;
- source-reference and old-index tie breakers;
- deterministic generated inputs for contiguous and paged stores; and
- complete Schema 8 output-tree byte identity between indirect and reference
  ordering.

## Code verification

The final candidate passed:

- focused indirect/reference differential and byte-identity tests;
- borrowed-row tests for contiguous and paged flat stores;
- `cargo test -p chronoxide-core -p chronoxide-ingester --lib --all-features`;
- `cargo test --workspace --all-targets --all-features`;
- strict library, binary, and test Clippy gates for the changed crates;
- `cargo fmt --all -- --check`; and
- `git diff --check`.

## Artifact cleanup

After validation, the six timed-arm segment trees and the profiled segment
tree were removed, reclaiming 6.4 GiB. The evidence root retains the frozen
binaries, source patch, configs, exact manifests, inventories, perf/RSS data,
logs, validation summaries, Heaptrack trace, and filtered allocation reports.
