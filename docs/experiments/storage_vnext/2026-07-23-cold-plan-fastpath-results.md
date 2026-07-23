# Canonical cold-series plan fast-path result

**Status:** promoted. The fused canonical path removes the seal-time
normalization copy and its sorting work while preserving exact corpus bytes.
The accepted noisy-host screen found no large end-to-end regression. This is a
memory and allocation improvement, not an unconditional latency improvement.

## Decision

Promote the fused canonical fast path used by the Schema 7 and Schema 8 segment
writers.

On the 250,000-message real replay:

- whole-process requested-live bytes at the selected large-window seal peak
  fell by 886,340,430 bytes (845.28 MiB, 16.4843%);
- whole-process allocation calls fell by 13,347,200 (4.8669%);
- retired instructions fell by 0.3537% in the eight-arm replay screen;
- whole-replay wall and task clock were neutral at -0.0157% and -0.0115%;
- `writer_flush_ms` increased by 0.9070% on average; and
- every storage byte, replay counter, decoded semantic fingerprint, exact
  postings result, independent readback, and PromQL result matched.

The whole-process requested-live heap maximum did not move materially because
the earlier decode phase remained the process-wide ceiling. The optimization
removes a distinct later seal peak; it must not be advertised as a 16.5%
whole-process peak reduction.

## Change under test

Finalized writer rows already have canonical, strictly increasing label key
IDs. The production seal path now builds the cold-series plan directly from
those borrowed rows instead of constructing an outer normalized vector,
cloning every row's labels, and sorting every clone.

The final implementation also repairs the locality cost found in the first
borrowed-only version:

- keyset validation, unique-keyset discovery, and value-dictionary discovery
  share one source-label traversal;
- scratch keyset vectors are reused within the discovery and row-construction
  passes;
- a keyset is cloned only when a new shape is first encountered; and
- lookups borrow key slices rather than allocating temporary lookup keys.

The generic crate-internal builder remains compatible with unsorted input by
normalizing a copy. Both paths reject duplicate label keys. The canonical
builder validates all rows before either output file is mutated. Empty corpora
and empty-label rows remain valid.

This changes neither the Schema 7/8 byte layout nor its semantics. No storage
format version or `storage.md` update is required.

## Superseded first candidate

The first candidate removed the normalization allocation but retained four
walks over the fragmented source rows and allocated temporary keysets in two
per-series passes. Its official screen and separate stage diagnostic showed:

| Metric | First candidate versus control |
| --- | ---: |
| Retired instructions | -0.2909% |
| `series_ms` diagnostic | +5.0545% |
| `writer_flush_ms` | +1.2923% |
| Cache misses | +2.2329% |

All four adjacent pairs made `writer_flush_ms` slower. The likely mechanism is
loss of the compact, freshly touched normalization copy without eliminating
the repeated fragmented-row traversals. That implementation is rejected and
superseded; do not reintroduce the borrowed-only four-pass shape collector.

Its preserved evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/cold-plan-canonical-ab-20260723-EBhsno`

## Final measurement contract

- Control source:
  `b6ac93bf8a72fe1d6186ea5eeace7a987a281b64`
  (`perf(storage): elide trusted identity permutations`)
- Control binary SHA-256:
  `e0cba209e3771c0a161d3f4b5aa015e0eeeb3d52430bd72843f04a0d0f83520b`
- Candidate binary SHA-256:
  `4d7e06acf7f9de1bb67c4857d5c91e4c15f5c9953b39ec85f69ff84bb44ce093`
- Candidate source delta: sealed as `metadata/candidate.patch`
- Workload: the exact accepted 250,000-message capture prefix
- Schedule:
  `control A, candidate A, candidate B, control B, candidate C, control C, control D, candidate D`
- Observations: four per version
- CPU set: all 32 logical CPUs
- Capture residency before every timed arm: exactly zero bytes
- Writeback: quiescence was recorded after every arm and, by chaining,
  therefore before arms two through eight; there is no preserved pre-arm
  writeback gate for arm one

QEMU was permitted and explicitly accepted by the user, but no QEMU process
appears in the preserved process snapshots. Before/after snapshots found no
Cargo/Rust compiler or linker work, unrelated perf/Heaptrack, or other
Chronoxide process. This ad-hoc harness did not continuously monitor each
interval or explicitly filter arbitrary databases. The snapshots show no
obvious competing database, while Codex and browser activity remained. This
result is therefore a noisy-host regression screen, not a quiet-host baseline.
The symmetric ABBA plus reverse BAAB schedule and per-pair results are retained
to expose drift.

The complete final evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/cold-plan-fused-ab-20260723-AcVBFW`

## Official eight-arm result

Means use four observations per version. Negative deltas favor the candidate
except for RSS/fault counts, where they still mean less work or memory.

| Metric | Control mean | Candidate mean | Candidate versus control |
| --- | ---: | ---: | ---: |
| Retired instructions | 762,179,680,991 | 759,483,763,140 | -0.3537% |
| Perf task clock | 47,849.510 ms | 47,843.993 ms | -0.0115% |
| Replay wall | 47.920 s | 47.913 s | -0.0157% |
| `writer_flush_ms` | 11,769.25 ms | 11,876.00 ms | +0.9070% |
| Cycles | — | — | -0.0135% |
| Branches | — | — | -0.6465% |
| Branch misses | — | — | +0.0385% |
| Cache references | — | — | -0.4998% |
| Cache misses | — | — | +0.1189% |
| Minor faults | — | — | -62,525.5 (-4.0183%) |
| GNU maximum RSS | — | — | -30,921 KiB (-0.5970%) |
| Monitored process-tree peak RSS | — | — | -47,564 KiB (-0.9074%) |
| Kernel HWM | — | — | -32,005 KiB (-0.6178%) |
| Full head-window elapsed | — | — | -0.3105% |

The instruction reduction reproduced in every adjacent pair. Timing moved in
both directions, as expected on the accepted host:

| Adjacent pair | Instructions | Task clock | Wall | `writer_flush_ms` | Cache misses |
| --- | ---: | ---: | ---: | ---: | ---: |
| Candidate A / control A | -0.3120% | +0.4577% | +0.5254% | +0.8821% | +0.1761% |
| Candidate B / control B | -0.3465% | -1.4628% | -1.4624% | +0.4415% | +0.0379% |
| Candidate C / control C | -0.3929% | +0.3731% | +0.2519% | +0.8866% | +0.5049% |
| Candidate D / control D | -0.3635% | +0.6103% | +0.6469% | +1.4177% | -0.2397% |

One pair exceeded 1% for `writer_flush_ms`; three were below 0.9%, and the
mean was below 1%. The user explicitly authorized this qualitative noisy-host
screen and asked to reconsider only a large regression; no numerical threshold
was predeclared. The percentages are descriptive, not a post-hoc selection
rule, and do not prove that the writer stage is faster.

## Stage diagnostic

A separate candidate-control-control-candidate diagnostic enabled the
stage-level seal timers. Those timers are observer-bearing and are not the
authoritative replay latency baseline.

| Diagnostic | Control mean | Candidate mean | Candidate versus control |
| --- | ---: | ---: | ---: |
| `series_ms` | 4,107.0 ms | 4,153.5 ms | +46.5 ms (+1.1322%) |
| Complete segment seal | — | — | +1.1667% |
| Metadata before the target stage | — | — | +1.1058% |
| Indexes | — | — | +1.8359% |
| Writer flush | — | — | +1.1290% |
| Retired instructions | — | — | -0.3473% |
| Cache misses | — | — | -0.5977% |

The fused implementation reduced the first candidate's `series_ms` regression
from 5.05% to 1.13% and repaired its cache-miss increase. The remaining stage
movement is small, correlated with neighboring unchanged stages, and was not
reproduced in the authoritative official eight-arm mean, which was neutral.
The diagnostic runs themselves had +0.6844% wall and +0.6748% task clock.
This remains evidence against describing the change as a writer-latency win.

## Heaptrack evidence

Heaptrack was run outside all timed observations.

| Heap measure | Control | Final candidate | Change |
| --- | ---: | ---: | ---: |
| Whole-process requested-live maximum | 5,376,967,487 B | 5,376,967,439 B | -48 B |
| Whole-process requested-live bytes at selected large-window seal peak | 5,376,878,162 B | 4,490,537,732 B | -886,340,430 B (-16.4843%) |
| Whole-process allocation calls | 274,246,430 | 260,899,230 | -13,347,200 (-4.8669%) |
| Final leaked bytes | 414,748 B | 414,748 B | unchanged |

The final candidate also made 8,896,810 fewer allocation calls than the first
borrowed candidate. Allocation stacks attributable to the normalization label
clone/sort were absent.

The control column reuses the immediately preceding identity-fast-path
candidate, which is the exact control binary for this experiment
(`e0cba209...`). Its trace is under
`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/identity-order-fastpath-ab-20260723-9bf5jx/heaptrack-candidate/output`
and has SHA-256
`5181c889208f0a165bb29f0b9a169f7c0509a03451199be4ad4fcdfd3e0b1041`.
The final-candidate trace has SHA-256
`c99313b7529b699a646cb295b62468a499f573a7b7678b61f58413ccddf57811`.
The selected seal observations are the maximum requested-live Massif
snapshots associated with the 4,407,610-series window: 5,376,878,162 bytes at
102.167 seconds in the control trace and 4,490,537,732 bytes at 87.874 seconds
in the candidate trace. These are whole-process observations during that
phase, not allocations owned exclusively by the seal planner. Profiler runtime
and profiler-inflated RSS are not latency evidence.

## Correctness and storage equivalence

Every measured and profiled replay accepted 250,000 messages and stored
9,634,809 samples. Every corpus matched exactly:

- 34 files and 972,969,365 bytes;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
- storage-selection fingerprint
  `797b04acdce65589fbe81116a7623ff586bf8f3b8ebd5aa1af9e42ea03dce5a0`;
- decoded-semantic fingerprint
  `871776d4a17106af13cbdaf69c4680dc40b1a5c9af82e7992615c50074cfcb49`;
- 313,963 exact postings lists, 89,285,049 decoded references, and
  97,070,555 encoded bytes, with fingerprint
  `00da9eb2c8b3660d9a23cc9d1ce1a265ae81ffe654ac20e27aa43a23cb78977c`;
- footer and exhaustive storage validation passed;
- independent readbacks executed 40/40 with zero skips, isolation skips, or
  mismatches; and
- all 14 PromQL rows matched fingerprint
  `a75234c7dfc296bc69899bdec2d9a3c6cccdb23060b2d5a78484fe7bc478345f`.

The independent verifier and query binaries had SHA-256 digests
`5e7f12aeb95f0dc7e4a27b4abbbe8122e7d329d73678a4a10f37fb1bfe6f4adc`
and
`da7dd185821e45b3acdb4c52622ae7cf224788261da6eb63e36100f1c38814db`.

## Code verification

The implementation passed:

- focused cold-plan and v3 writer tests;
- the complete `chronoxide-core` library test set;
- exact Schema 7 and default Schema 8 seal tests;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed strict workspace Clippy gates;
- `cargo fmt --all -- --check`; and
- `git diff --check`.

Focused tests cover generic/canonical plan and byte equality, complete
Schema 7 writer byte/root/stat equality, descending and duplicate key
rejection, validation before output mutation, empty corpus, and empty-label
rows. The commands were rerun against the final patch, but their console logs
are not retained in the experiment root.
