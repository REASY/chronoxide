# Phase 1 current-source four-million-message replay baseline

- **Measured:** 2026-07-21 13:16–13:42 `+08:00`
- **Code base:** `a8bd6d44d6c06375a09104a4a9c58ecbe6268021` plus the
  frozen tracked working-tree patch recorded with the run
- **Tracked patch SHA-256:**
  `5953acab6771907b64bb303206d4a3fe471328ac517c7617bc3537bc87ec358d`
- **Ingester binary SHA-256:**
  `de3ad84f277efb97fd4247281ee6e3a1c6ad8b663f8bd5c4d98ec06c73dd8354`
- **Allocator:** system allocator; no jemalloc feature was linked
- **Raw result:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase1-4m-20260721T051609Z`
- **Status:** complete; no coverage-gap rows

This is a baseline, not an optimization A/B. It records current behavior after
the already-promoted Schema 8, ingest, and head changes. Historical speedups
must not be added to these values or treated as a comparison against this run.

## Workload and controls

The runner replayed the first 4,000,000 messages from the pinned 13,000,000-
message capture. `partition-1.capture` is 20,589,025,986 bytes with SHA-256
`1ecebab16fc68b984949810f32c2778857940530336554872d775215fdd28dc4`;
the capture manifest SHA-256 is
`84181ec8e9959166bc01224cb031c90980286f9945ba6dca5368942490db070d`.
Every measured replay began with zero resident capture bytes according to
`fincore` after `POSIX_FADV_DONTNEED`. The capture had 4,197,187,584 resident
bytes after replay 1; this is evidence about the Linux page cache, not the
NVMe controller cache.

The effective configuration was Schema 8, a 900-second segment duration,
deterministic ID seed 42, compact numeric series enabled, adaptive head-series
tables enabled, Gorilla floats, delta-zig-zag integers, raw variable-length
values, one input partition, and the system allocator. The configured
3,600-second normal head duration was not effective because the enabled
segment writer rotates on the 900-second segment duration.

The host was an AMD Ryzen 9 9950X (16 cores/32 threads, one NUMA node, 64 MiB
L3) with 60,681 MiB RAM, Ubuntu 26.04, Linux 7.0.0-27, ext4 on NVMe,
rustc/cargo 1.97.0, LLVM 22.1.6, and perf 7.0.12. The harness recorded process
and pressure snapshots and rejected overlapping builds, replays, profilers,
footer scans, and query processes.

## Three measured replays

Hardware counters were multiplexed at 83% for cycles, instructions, branches,
and cache events; perf's scaled estimates are reported below. Task clock and
software events ran at 100%.

| Run | Wall | User | System | Task clock | Cycles | Instructions | IPC | Branch misses | Cache misses | Page faults | `/usr/bin/time` max RSS | Process-tree peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| replay-01 | 508.07 s | 502.82 s | 5.92 s | 508.544 s | 2,839,048,478,760 | 5,720,919,302,062 | 2.015 | 4,852,727,456 | 18,330,692,466 | 3,342,145 | 11,377,252 KiB | 11,445,404 KiB |
| replay-02 | 510.52 s | 504.99 s | 6.20 s | 510.980 s | 2,846,993,191,352 | 5,721,593,630,070 | 2.010 | 4,816,762,011 | 18,316,703,977 | 3,320,630 | 11,290,984 KiB | 11,359,500 KiB |
| replay-03 | 510.64 s | 505.32 s | 5.96 s | 511.079 s | 2,846,834,718,359 | 5,720,618,208,382 | 2.009 | 4,835,101,185 | 18,298,133,018 | 3,320,631 | 11,290,832 KiB | 11,360,312 KiB |
| **median** | **510.52 s** | **504.99 s** | **5.96 s** | **510.980 s** | **2,846,834,718,359** | **5,720,919,302,062** | **2.010** | **4,835,101,185** | **18,316,703,977** | **3,320,631** | **11,290,984 KiB** | **11,360,312 KiB** |

The median corresponds to 7,835.15 messages/s, 711,709 scaled cycles/message,
1,430,230 scaled instructions/message, 10.768 GiB single-process peak RSS, and
10.834 GiB process-tree peak RSS. Median branch-miss rate was 0.4555%; median
cache misses were 20.3452% of cache references. Wall-time max-minus-min spread
was 0.503%, task-clock spread was 0.496%, and process-tree RSS spread was
0.756%. There were three major faults across all measured runs and no swap in
the process-tree RSS samples.

The median application-reported label-processing totals were:

| Phase | Median |
| --- | ---: |
| Total processing | 241.606 s |
| Symbol/label interning | 73.730 s |
| Remaining build work | 167.876 s |

Interning was 30.52% of the reported processing interval. These application
timers do not cover replay decode, sealing, reporting, or shutdown and
therefore must not be compared directly with wall time.

Across the eight sealed windows, median summed writer timings were 127,279 ms
elapsed, 28,274 ms seal decode, 44,552 ms `record_samples`, 43,160 ms record
wall, 23,997 ms record metadata, 14,394 ms chunk append, and 53,063 ms writer
flush. These fields are partly inclusive and are not additive. All three runs
sealed exactly 154,902,724 samples in 17,286,077 chunks.

## Correctness and deterministic output

All three measured corpora and the separate profiled corpus were byte-for-byte
identical:

- 66 regular files;
- 5,569,314,896 bytes;
- manifest SHA-256
  `8b0789e2f6c404a144e0d2e87f152a83e9f0bedb9c5ab2c6512608056cae3289`;
- identical replay correctness documents; and
- eight deterministic segment IDs.

Ingest outcomes were identical in every run:

| Counter | Value |
| --- | ---: |
| OTLP messages | 4,000,000 |
| Metric records | 28,129,055 |
| Observed datapoints | 155,197,127 |
| Time-policy accepted | 155,073,601 |
| Recorded samples | 154,902,724 |
| Missing number values / accepted not recorded | 170,877 |
| Dropped too old | 66,243 |
| Dropped too future | 57,283 |
| Missing timestamps | 0 |
| Unique label sets | 6,607,139 |
| Unique metric names | 26,198 |

The untimed exhaustive verifier checked all 8 segments, 17,286,077 series,
17,286,077 chunks, and 154,902,724 samples. It decoded 1,290,200 exact
postings lists containing 351,771,750 refs and produced selection fingerprint
`ab7a25338c801b16548bae637566c9fa9af929522227f1ab15122a9d4b934e37`
and postings fingerprint
`6c62a5ba70f87eb672c67101b09a96b270cf82c509c284ec5db27ab01fac9de7`.
The independent readback oracle executed 38/38 cases with zero skips,
isolation skips, or mismatches; its PromQL-row fingerprint was
`fb41bdd76f8a4d4b9bc97d90c4397405717c49e15d9e2ecc557d7c4ae0a4741c`.

## Corpus composition

| Component | Bytes | Corpus share |
| --- | ---: | ---: |
| `chunks.bin` | 3,578,303,589 | 64.250% |
| `series.bin` | 1,154,153,445 | 20.723% |
| `indexes.puffin` | 754,231,284 | 13.543% |
| `symbols.bin` | 82,618,420 | 1.483% |
| `chunk_index.bin` | 512 | effectively zero |
| Footers, metadata, and manifest files | 7,646 | effectively zero |

This composition keeps chunk codecs relevant for capacity, but it does not by
itself prove that chunk bytes dominate query latency. That decision remains
conditional on the query profiles.

## Separate CPU profile

The fourth replay used `perf record` and did not contribute to latency
medians. It completed in 508.00 seconds with the same corpus and correctness
fingerprints. Perf captured approximately 24,000 `cpu-clock` samples with zero
lost samples. The self profile's largest symbols were:

| Self CPU | Symbol |
| ---: | --- |
| 19.94% | glibc `_int_malloc` |
| 6.55% | `FlatInternedLabelSetStore::intern_encoded` |
| 5.04% | glibc `__memcmp_evex_movbe` |
| 4.60% | `ArenaSymbolTable::intern` |
| 4.21% | glibc `_int_free_merge_chunk` |
| 3.12% | glibc `_int_free_chunk` |
| 2.38% | glibc `__libc_malloc2` |
| 2.02% | glibc `cfree` |
| 1.69% | `merge_prepared_labels` |
| 1.58% | glibc `memmove` |

Allocator entry/free symbols account for well over 30% of sampled self CPU
before counting downstream allocation callers. This is strong evidence for
the bounded allocator matrix in Phase 5 and against speculative protobuf
pooling: protobuf varint decode was 1.31%, repeated-message merge was 0.79%,
and byte-buffer replacement was 0.82% individually. The profile also retains
5.04% in `memcmp` plus material label/symbol hashing and equality work, so the
compact-ID and head-allocation hypotheses remain credible.

The system allocator exposes no jemalloc-style active/retained/purge telemetry.
This run therefore provides sampled allocator CPU stacks and process RSS, not
an allocator-internal retained-byte profile. Phase 5 must close that gap with
equivalent system/jemalloc builds and allocator-specific telemetry.

## Baseline decision

Phase 1 ingest baseline is accepted. It is deterministic, semantically
verified, tightly clustered, and has an uncontaminated separate profile. The
principal current ingest signal is allocation and label/symbol ownership work,
not event-skew collection, capture-buffer reuse, or another postings codec.
No production default changes as a result of this baseline alone.
