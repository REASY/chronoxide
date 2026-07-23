# Direct routing-index encoding result

**Status:** promoted as a bounded segment-seal allocation and RSS
improvement. The Schema 6/7/8 routing encoder now writes bucket records and
key bytes directly into one pre-sized final buffer instead of retaining
separate bucket and aggregate-key staging buffers.

## Decision

Promote the direct encoder, but classify it as a memory improvement rather
than a speedup.

On the accepted 250,000-message replay prefix, the candidate:

- reduced mean unprofiled ingester high-water RSS by 40,432 KiB
  (39.484 MiB, 1.358%) and monitored aggregate process-tree peak RSS by
  15,928 KiB (15.555 MiB, 0.530%);
- put every candidate RSS observation below every control observation, with
  controls at 2,976,572..2,977,520 KiB and candidates at
  2,936,084..2,937,032 KiB;
- removed the temporary routing bucket table and aggregate key buffer from
  all four segment encodes;
- changed mean instructions by -0.013%, task clock by -0.221%, cycles by
  -0.176%, and wall time by -0.205%;
- increased the largest-window writer-flush timer by a repeatable 50.25 ms
  (0.489%), while the complete largest-window elapsed timer changed by only
  +9.75 ms (0.048%);
- preserved all 34 storage files and 972,969,365 corpus bytes exactly;
- passed complete footer validation; and
- passed 40/40 independent readbacks with zero skips, isolation skips, or
  mismatches.

The flush movement is a measured tradeoff, not a claimed speedup and not
dismissed as noise. It is less than 0.5% of the approximately 10.3-second
writer flush and approximately 0.12% of the full replay. Aggregate
complete-process CPU and wall means were neutral. The stable 39.5 MiB RSS
reduction and the removal of two redundant live buffers justify the narrow
code-only change.

## Change under test

`SegmentRoutingIndex::encode` must preserve a deterministic open-addressed
table shared by the Schema 6, Schema 7, and Schema 8 writers. The former
implementation:

1. built and encoded one routing key for every exact-postings key;
2. sorted those encoded keys;
3. allocated a `Vec<RoutingBucketRecord>` and populated it by linear probing;
4. grew a separate `Vec<u8>` containing all encoded keys;
5. grew the final output `Vec<u8>`;
6. serialized the bucket table into that output; and
7. copied the aggregate key buffer after it.

The promoted implementation keeps routing-key construction, encoded-key sort
order, hashing, load factor, and probing unchanged. It:

1. validates the same per-key `u32` offset and length bounds while planning
   the aggregate key length;
2. computes the exact header, bucket-table, and key-region lengths with
   checked arithmetic;
3. fallibly reserves one final output allocation;
4. emits the unchanged 40-byte header;
5. zero-initializes the final bucket region, preserving canonical empty
   records;
6. probes that region using the complete four-byte `key_len` field as the
   occupancy marker;
7. appends each key directly in the established sorted order; and
8. writes one stack-encoded 40-byte record into the selected final bucket.

The format's unusual upper boundary is preserved. Each key length and each
key's starting offset must fit `u32`, but the final key is allowed to make the
aggregate key region exceed `u32::MAX` because the header stores its complete
length as `u64`. The sizing pass checks every starting offset before adding
the corresponding key, just as the former insertion loop did.

Allocation failure for the final routing blob is now returned as
`io::ErrorKind::OutOfMemory`. This replaces infallible `Vec` growth which
could abort the process; it does not change any accepted input, persisted
byte, or reader behavior.

## Exact-byte proof

A test-only reference encoder preserves the complete former two-buffer
algorithm and independently serializes every little-endian header and bucket
field. The new encoder is compared byte-for-byte with that reference for:

- an empty index and a one-entry index;
- entry counts 2, 3, 4, 7, 8, 15, and 16, covering bucket-count boundaries;
- keys whose normal `BTreeMap<(name, value)>` order differs from encoded-key
  order;
- an empty value, embedded NUL, multibyte UTF-8, and a value above 255 bytes;
  and
- naturally colliding generated cases.

Each result is also decoded and compared with the source
`SegmentRoutingIndex`. The pre-existing runtime collision oracle separately
forces two keys into bucket 7 of an eight-bucket table and proves wraparound
to bucket 0, exact touched spans, and cache reuse.

The production encoder still sorts by complete encoded key bytes. It does not
substitute map order: routing keys begin with the little-endian label-name
length, so those orders are not generally equivalent.

## Allocation-profile evidence

The control is the exact promoted postings-derived-FST binary and trace. The
candidate uses the same Rust system allocator, Schema 8 configuration,
accepted capture prefix, and deterministic segment seed 42.

The allocation sites establish the intended causal change:

| Routing allocation family | Control | Candidate |
| --- | ---: | ---: |
| Temporary bucket table | 41.94 MB over 4 calls | absent |
| Growing aggregate key buffer | 29.36 MB over 51 calls | absent |
| Final output buffer | 67.11 MB over 12 growth calls | 65.01 MB over 4 exact allocations |
| Sorted entry/key vector | 25.17 MB over 38 calls | 25.17 MB over 38 calls |

The final-output reduction is allocator rounding avoided by exact reservation;
the substantive lifetime win is removal of the bucket and aggregate-key
buffers. Whole-process allocation calls moved from 240,150,999 to
240,150,870 (-129), analyzed temporary allocations moved from 40,534,947 to
40,534,767 (-180), and reported leaked bytes remained 414,748.

The profiled GNU-time maximum RSS moved from 2,990,020 KiB to 2,951,592 KiB,
a reduction of 38,428 KiB (37.527 MiB, 1.285%). Heaptrack's rounded summary
also moved from 3.30 GB to 3.28 GB peak heap and from 3.05 GB to 3.02 GB peak
RSS.

One Heaptrack measure did not improve and is recorded explicitly. The Massif
export's sampled requested-live maximum moved from 3,271,002,194 bytes at
78.632 seconds to 3,279,384,768 bytes at 78.062 seconds: +8,382,574 bytes
(7.994 MiB, 0.256%). These sampled maxima occurred at different trace
instants, and the export does not provide useful allocation attribution for
the candidate maximum. The movement is unresolved and is not claimed as a
routing or requested-live peak win. The promotion claim rests on the removed
allocation sites and the counterbalanced unprofiled RSS result, which
reproduced the profiled RSS direction with very low dispersion.

Heaptrack itself changed replay runtime from 92.64 to 92.09 seconds. Profiler
runtime is diagnostic only and is not substituted for the unprofiled gate.

## Formal ABBA runtime evidence

Two counterbalanced blocks each used control, candidate, candidate, control
order. All eight arms used all CPUs, a freshly advised-away capture cache,
the same binary per side, the same configuration except output path, and the
same replay-counter and byte-manifest gates. Means are arithmetic means of
four observations per binary.

| Measure | Control mean | Candidate mean | Change |
| --- | ---: | ---: | ---: |
| Wall time | 41.370 s | 41.285 s | -0.085 s (-0.205%) |
| Task clock | 41,369.105 ms | 41,277.685 ms | -91.420 ms (-0.221%) |
| Cycles | 229,840,628,667 | 229,435,838,076 | -0.176% |
| Instructions | 669,019,052,468 | 668,928,930,914 | -0.013% |
| Branches | 124,318,982,187 | 124,323,027,581 | +0.003% |
| Branch misses | 487,903,505 | 477,064,430 | -2.222% |
| Cache references | 7,979,214,714 | 8,011,904,717 | +0.410% |
| Cache misses | 1,195,250,502 | 1,196,083,135 | +0.070% |
| Ingester high-water RSS | 2,977,037 KiB | 2,936,605 KiB | -40,432 KiB (-39.484 MiB, -1.358%) |
| Aggregate process-tree peak RSS | 3,003,159 KiB | 2,987,231 KiB | -15,928 KiB (-15.555 MiB, -0.530%) |
| Largest-window elapsed | 20,433.75 ms | 20,443.50 ms | +9.75 ms (+0.048%) |
| Largest-window writer flush | 10,269.50 ms | 10,319.75 ms | +50.25 ms (+0.489%) |

The ingester high-water RSS ranges do not overlap. The smallest control minus
the largest candidate is still 39,540 KiB (38.613 MiB). The two
counterbalanced block effects were -41,064 KiB and -39,800 KiB, so the result
is not carried by one run. Aggregate process-tree peaks also do not overlap;
their smaller delta includes the measurement wrappers and shared mappings
counted by the process-tree monitor.

The writer-flush ranges also do not overlap: controls were
10,259..10,279 ms and candidates were 10,314..10,323 ms. That small local
cost is therefore treated as real even though aggregate mean
complete-process instructions, cycles, task clock, wall time, and
largest-window elapsed remain neutral.

Raw machine tables are retained as:

- `metadata/abba-8run-summary.tsv`; and
- `metadata/abba-8run-means.tsv`.

## Correctness evidence

The profiled candidate and every formal A/B arm reproduced:

- 250,000 messages, 9,659,074 observed datapoints, 9,655,365
  time-policy-accepted datapoints, and 9,634,809 recorded samples;
- every event-time and storage acceptance/rejection counter;
- 4 deterministic segments, 34 files, and 972,969,365 bytes;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
- replay-correctness JSON, corpus summary, complete inventory, and every
  segment SHA-256 byte-for-byte.

The profiled candidate corpus separately passed complete segment-footer
validation and all 40 independent readback-oracle cases with zero skips,
isolation skips, or mismatches. Every formal arm had the same complete byte
manifest, so those storage-byte checks cover the identical persisted output;
they were not redundantly rerun inside each timed process.

The byte identity includes the routing blob inside `indexes.puffin`. The
change therefore affects neither routing-table contents nor any later index
or payload offset.

## Measurement contract

- Base source:
  `b73b9d333ca10fe733fe981aa8ff0d2247cef048`
- Frozen source-only patch SHA-256:
  `214c2f7133eb544ae5d313c8de4f4cf4164f6f556837457d86d1ff6dbe412d3f`
- Control ingester SHA-256:
  `57c83e747c73435bf218dde23f19313f3c6f09cf4a11eb052026e518e7e5cacb`
- Candidate ingester SHA-256:
  `2213193a75a81cf3e4d380aaa97ceebfc082e11c3aaf8f723f79f6bed7e42b53`
- Candidate query SHA-256:
  `5bd5c882fc80cdfc2afe255d3e4c501640faff4957a1a478f9bf5423e6f3990d`
- Control Heaptrack trace SHA-256:
  `d4e4cd63f4800e3f4d94f32b4568ad82443af66a1d0c61c19749d8e289703fe6`
- Candidate Heaptrack trace SHA-256:
  `713191d9ff95a1f96ba4a76614c956d831f362cf20afd220ff3c59c4bfd65a8e`
- Workload: exact accepted 250,000-message capture prefix
- Writer configuration: identical except for run-specific output paths;
  deterministic segment seed 42
- Storage schema: Schema 8
- Allocator: Rust system allocator

The evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/routing-direct-encode-memory-20260724T064026-hTADrG`

## Artifact cleanup

After the profile, eight formal runs, byte comparison, footer validation,
readback verification, and independent audit completed, every regenerated
segment tree was rehashed and compared with its retained manifest immediately
before deletion.

Cleanup reclaimed:

- 9 segment trees containing 306 files and 8,756,724,285 logical bytes;
- 1 redundant query binary containing 512,025,816 bytes; and
- 307 files and 9,268,750,101 logical bytes in total (8.632 GiB).

Frozen ingester binaries, source patch, hashes, Heaptrack trace and Massif
export, perf/RSS data, logs, manifests, reports, analysis, and explicit cleanup
records remain. No capture, accepted corpus, unrelated experiment, or
user-owned artifact was removed.

## Verification

The exact measured candidate source passed:

- the new staged-reference differential encoder test;
- the existing deterministic builder and wraparound-collision/read-span
  oracles;
- all 213 `storage::index` library tests;
- strict `chronoxide-core` all-feature library Clippy;
- complete segment-footer validation;
- `chronoxide-query --verify-readbacks` with 40/40 executed and zero skipped;
- `cargo fmt --all -- --check`;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed workspace-wide all-feature Clippy gates for libraries,
  binaries, tests, and benches with warnings denied; and
- `git diff --check`.

Because persisted bytes, versions, and semantics are unchanged, this candidate
requires no storage-version or `storage.md` update.
