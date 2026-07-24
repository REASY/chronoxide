# Page-aware payload-read planning result

**Status:** rejected after a correctness-passing real-corpus gate. Retain the
unaligned `4096`-byte maximum-gap payload planner. The page-aware candidate was
reverted.

## Decision

Do not promote 4 KiB or 8 KiB page-aligned buffered payload reads.

The candidate substantially reduced process-issued payload spans, and one
device-counter screen showed a directional partition-counter improvement.
Those structural changes did not become a robust end-to-end query-latency win
in the complete matrix. The `P8` expansion also exceeded the predeclared
physical-byte budget for `equality_last` and native ExponentialHistogram
queries.

Keep:

- the cursor and binary-search payload-span lookup from commit `7402096`;
- the current unaligned planner; and
- the bounded `4096`-byte maximum coalescing gap as the default.

This experiment changed neither stored bytes nor query semantics. It requires
no storage-format or PromQL-coverage update.

## Hypothesis and approach

The [Lance structural-encoding review](../../reviews/2026-07-23-lance-structural-encoding-review.md)
proposed separating exact logical chunk selection from page-aware physical I/O.
The experiment followed the accepted
[Phase 3 payload-coalescing result](2026-07-21-phase3-payload-coalescing.md)
and ran after the linear payload-span lookup confounder had been removed.

The candidate:

1. constructed the exact gap-zero payload union, retaining locator validation
   and error precedence;
2. expanded selected ranges outward to 4 KiB or 8 KiB boundaries; for an
   in-bounds exact range, only alignment-added overhang was clipped at the
   immutable payload-file length, while an out-of-bounds exact locator
   retained the existing short-read error precedence;
3. unioned overlapping and adjacent page ranges;
4. optionally applied the existing bounded gap merge; and
5. exposed only exact selected locator slices to decoders.

Page and gap bytes were physical over-read. They were never selected, decoded,
or made authoritative. Regular and out-of-order payload files remained
separate plans. This was a buffered `pread` experiment, not an `O_DIRECT`,
device-sector-alignment, or on-disk chunk-alignment experiment.

One release binary selected every policy at runtime:

| Arm | Physical alignment | Maximum merge gap |
| --- | --- | ---: |
| `E` | unaligned | 0 B |
| `G4` | unaligned | 4,096 B |
| `P4` | 4 KiB page union | 0 B |
| `P8` | 8 KiB page union | 0 B |
| `P4G4` | 4 KiB page union | 4,096 B |

`G4` is the current default. Its name refers to the maximum coalescing gap,
not 4 KiB page alignment.

This was a default-point recheck, not a repeat of Phase 3's complete
`0/256/1024/4096` fixed-gap matrix or its `io_uring` backend.

## Measurement contract

- Base commit:
  `74020960619d808d5f43b6d29a95ffbab6727a6b`
- Frozen candidate binary SHA-256:
  `84ff588fdb9421c3a57612746546058db2c1f0d7576e98a09ca8699ff9da8e74`
- Candidate patch SHA-256:
  `0e6497fcd32a39b3d3b85ee94d33b2e86b3505bb9fb4f5bc7741361f60bae8fb`
- Corpus: 66 files and 5,569,314,896 bytes across eight Schema 8 segments
- Corpus inventory SHA-256:
  `28547c0fc2b738eb58948400602640c017844cd57bd49917bffdf100a6e14a0b`
- Query-corpus fingerprint:
  `7e5cf252e5df9bdb786e1b9deb9248f09667962ac559f339ba47312c5c0e3ca3`
- Reader: forced buffered `pread`; the configured cross-backend queue-depth
  field was 128, while `pread` submissions remained synchronous at depth one
- Query policy: Schema 8, demand-driven labels, compact query-label IDs, a
  512 MiB query-label arena, repeated range execution, and a disabled range
  scalar cache
- Raw schema: v15, with identical finite query limits and complete effective
  configuration preserved for every arm
- Per process: one CLI-cold and two application-warm evaluations

Every fresh process started only after `POSIX_FADV_DONTNEED` and
`fincore == 0` for the complete corpus. This establishes zero observed Linux
page-cache residency; it does not establish a cold SSD or controller cache.
The runners rejected concurrent Cargo/Rust builds, profilers, unrelated
Chronoxide, GreptimeDB, Prometheus, and QEMU processes and captured pressure
and process snapshots. Footer and independent-readback validation ran outside
the timed matrices.

The initial screen used ten balanced Williams blocks:

- three queries;
- five arms; and
- 150 fresh processes / 450 evaluations.

The complete `G4` versus `P8` matrix used four alternating ABBA/BAAB blocks:

- all eleven entries in the sealed query manifest;
- eight fresh processes per arm and query; and
- 176 fresh processes / 528 evaluations.

One scheduled full-matrix process attempt, process 127, stopped before
measurement when `fincore` still found one 4 KiB `symbols.bin` page after the
first eviction pass. Its premeasurement-only directory was quarantined; the
preceding 126 valid observations were preserved. A second eviction cleared the
page, and the runner thereafter allowed bounded retries while still requiring
exactly zero resident bytes before accepting a measurement.

## Initial five-arm screen

The table reports arm-wide medians. Positive latency means the named page arm
was faster than `G4`. Physical spans and bytes are process-issued payload
ranges before operating-system caching, not device traffic; a negative span
percentage means fewer issued ranges, while a positive byte percentage means
more issued bytes.

| Query | Arm | Cold | Warm | Physical spans | Physical bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| Broad selector | `P4` | -0.32% | -0.53% | -46.47% | +1.89% |
| Broad selector | `P8` | +0.28% | +0.32% | -71.37% | +2.87% |
| Broad selector | `P4G4` | +1.84% | +1.73% | -68.05% | +2.29% |
| Scalar instant | `P4` | +0.42% | +1.28% | 0.00% | +0.12% |
| Scalar instant | `P8` | +0.53% | +1.31% | 0.00% | +0.16% |
| Scalar instant | `P4G4` | +0.09% | +0.85% | 0.00% | +0.12% |
| Scalar range | `P4` | -4.54% | -4.88% | -32.69% | +1.86% |
| Scalar range | `P8` | -1.59% | -1.94% | -59.62% | +3.10% |
| Scalar range | `P4G4` | -2.83% | -1.65% | -56.73% | +2.32% |

No page arm reached the screen's material-latency gate. `P8` and `P4G4`
reduced enough spans without a large aggregate regression to justify a
device-level confirmation, but neither was eligible for promotion from this
screen.

## Device-counter confirmation

Linux block tracepoints were unavailable without root. The experiment
therefore used isolated before/after counters from the corpus partition,
`/sys/class/block/nvme0n1p5/stat`; `/home` was on a different device. These
values distinguish partition activity from process-issued span accounting, but
they are partition-wide deltas rather than per-request block traces.

The balanced 36-process confirmation found the following scalar-range medians:

| Arm | Read I/Os | Sectors | Read time | Read time / I/O |
| --- | ---: | ---: | ---: | ---: |
| `G4` | 1,538 | 115,736 | 248.0 ms | 0.1612 ms |
| `P8` | 1,494 | 115,736 | 223.0 ms | 0.1492 ms |
| `P4G4` | 1,503 | 115,736 | 224.5 ms | 0.1494 ms |

For scalar range, `P8` reduced partition read I/Os by 2.86% and the derived
`read_time_ms / read_ios` ratio by 7.46%, with unchanged sectors and 6/6
directional ratio wins. Process wall time was 8.22 seconds for `P8` and 8.20
seconds for `G4` at the median, so this was a device-counter signal rather than
an end-to-end latency win. The broad-query device result was neutral. The
scalar-range signal was sufficient to advance a page-union representative.

`P4G4` also passed the scalar-range device gate, with 2.28% fewer read I/Os and
a 7.37% lower derived ratio. The full follow-up selected `P8` as an engineering
scope choice; among the two passing arms it had the larger read-I/O reduction
and isolated page union without a second gap merge. This was not a predeclared
tiebreaker. `P4G4` also had a 2.68% better device-run wall median, so the
subsequent `P8` matrix is not direct end-to-end evidence about `P4G4`.
`P4G4` nevertheless remained rejected by its initial screen: it missed the
material-latency gate and regressed scalar range.

## Complete query matrix

The table compares unpaired arm-wide `P8` and `G4` medians. Positive latency
means `P8` was faster. The win counts use eight adjacent, order-balanced
fresh-process pairs, two per ABBA/BAAB block, to expose local temporal drift
between processes. They were therefore required for a robust promotion.

| Query | Cold | Warm | Cold / warm wins | Spans | Physical bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Broad selector | +1.42% | +1.96% | 3/8 / 4/8 | -71.37% | +2.87% |
| Equality last | +4.30% | +5.20% | 8/8 / 5/8 | -30.51% | +110.80% |
| Sparse regex last | -0.26% | -0.25% | 2/8 / 3/8 | -50.00% | +2.54% |
| Negative matcher last | +1.05% | +0.88% | 6/8 / 3/8 | 0.00% | +0.77% |
| No result | +0.84% | +0.17% | 8/8 / 3/8 | 0.00% | 0.00% |
| Scalar instant | +1.71% | +1.47% | 6/8 / 6/8 | 0.00% | +0.16% |
| Scalar range | -1.04% | -0.34% | 6/8 / 3/8 | -59.62% | +3.10% |
| Histogram count range | +0.97% | +0.80% | 5/8 / 5/8 | -13.79% | +1.58% |
| Histogram p95 range | -0.24% | -0.18% | 3/8 / 2/8 | -13.79% | +1.58% |
| ExponentialHistogram count range | -0.95% | -0.60% | 2/8 / 2/8 | -42.86% | +16.73% |
| ExponentialHistogram p95 range | -0.29% | -0.21% | 3/8 / 5/8 | -42.86% | +16.73% |

The apparent broad-query arm-wide improvement did not survive the paired view:
the median paired difference moved 1.20% slower cold and 0.71% slower warm.
The earlier scalar-range device signal also did not become an end-to-end gain.

`equality_last` was the closest latency result, but it missed both robustness
requirements: cold improved only 4.30%, and warm won only 5/8 pairs. Its
physical bytes also grew from 411,936 to 868,352, or 110.80%.

The largest `P8` median-RSS increase was 3.12 MiB, or 2.98%, for scalar range;
scalar instant instead fell by 3.36 MiB. RSS was not a rejection reason.

## Predeclared promotion gate

Before the complete matrix, promotion required all of:

- exact correctness and accounting equivalence;
- no query worse by more than 3% in median cold latency, per-process warm
  median latency, or process wall time;
- no RSS increase that was both greater than 3% and greater than 16 MiB;
- no query above `1.05x` `G4` physical bytes or `8x` read/used amplification;
  and
- at least one query with a 5% or larger cold and warm improvement and at
  least 7/8 paired wins in both states.

The result was:

| Gate | Result |
| --- | --- |
| Correctness | pass |
| Maximum latency regression | pass |
| RSS | pass |
| Per-query physical-byte budget | **fail** |
| Material, repeatable latency gain | **fail** |

The physical-byte failures were `equality_last` at +110.80% and both native
ExponentialHistogram queries at +16.73%. No query met the material-latency
rule.

## Correctness

Footer validation passed for eight Schema 8 segments containing 154,902,724
datapoints and 17,286,077 chunks. Each of the five arms independently
executed 32/32 expected readback queries, including both expected multi-step
range cases, with zero skips and zero mismatches.

Across the 176-process complete matrix:

- semantic and portable fingerprints were exact;
- result counts, values, and ordering were exact;
- complete `QueryStats` and logical payload bytes were exact;
- cache, metadata, query-label, and nonphysical scheduler accounting matched;
- intended physical signatures were deterministic within every arm; and
- the corpus inventory remained byte-identical at 66 files and
  5,569,314,896 bytes.

Before measurement, the working session recorded successful focused payload
planner, decoder, segment-routing, CLI, raw-schema, and independent-oracle
tests, both warning-denied workspace Clippy gates, formatting, and
`git diff --check`. Those build/test logs were not copied into the evidence
root; its independent runtime correctness evidence is preserved separately
above.

## Interpretation

On this corpus and query matrix, the experiment demonstrated that page unions
can reduce process-issued span counts. It did not show that the existing
buffered reader pays a material penalty for unaligned spans. Large span
reductions repeatedly failed to improve latency:

- broad spans fell 71.37% while paired latency moved slightly slower;
- scalar-range spans fell 59.62% while cold and warm latency regressed; and
- ExponentialHistogram spans fell 42.86% while bytes grew 16.73% and latency
  regressed.

This is consistent with Linux buffered I/O already amortizing many application
spans and with outward page rounding adding unnecessary bytes. It is not a
universal claim about every device or I/O backend.

Do not repeat this comparator merely with another buffered-I/O page size.
Reconsider physical page alignment only after a material change such as
`O_DIRECT`, fixed registered buffers, a different payload-packing layout, or a
new workload/device profile that shows unaligned device I/O dominating.
Continue to keep physical over-read separate from logical selection and
decoder authority.

## Retained evidence and cleanup

The complete evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/lance-page-read-screen-20260724T141559Z`

It contains the frozen binary, exact candidate patch, runners, raw JSON,
reports, timing and RSS data, pressure and process snapshots, corpus
inventories, validation output, and analyzers. The primary summaries are:

- `analysis/screen.md`
- `analysis/device-counters.md`
- `analysis/full-matrix.md`
- `metadata/full-matrix-gate.md`
- `metadata/full-matrix-eviction-retry-note.md`
- `metadata/device-trace-permissions.txt`

The recorded result digests are:

| Artifact | SHA-256 |
| --- | --- |
| `inventory/before.json` and both after snapshots | `b64d87c95ff4aca51833540f915787421aded2acbb74fee628a7ef8222dac083` |
| `analysis/screen.json` | `5015ccbfa310533932b9b112150ec87b088308bd71fb32a5bfaf454ff38837a5` |
| `analysis/device-counters.json` | `f0ceee3767c52c0cccd25a8ee769364483f41a6e17fb2278b2c5f6bc3512d186` |
| `analysis/full-matrix.json` | `72935c912b6f889b49a1f5b17a193b40c6a8d101282b768af66b678b0b915a48` |

After the rejection, all candidate source, CLI, raw-schema, reporting, and
normative-storage-documentation changes were reverted. The release query
binary was rebuilt from the retained unaligned implementation. No
experimental code or storage-format change was committed.
