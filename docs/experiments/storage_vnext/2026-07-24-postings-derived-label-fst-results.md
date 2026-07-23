# Postings-derived label-value FST result

**Status:** promoted as a segment-seal memory and CPU improvement. The Schema
7/8 writer now derives the label-value FST inventory from the exact-postings
keys already built during finalization instead of scanning every label
membership a second time. Schema 6 retains its independent series-derived
path.

## Decision

Promote the postings-derived builder.

On the accepted 250,000-message replay prefix, the candidate:

- reduced the official Heaptrack process requested-live maximum by
  358,308,232 bytes (341.709 MiB, 9.873%);
- reduced the numeric label-value collection site's peak live memory by
  535,548,752 bytes (510.739 MiB, 99.635%);
- reduced mean formal-ABBA instructions by 3.047%, task clock by 2.189%, and
  wall time by 2.182%;
- reduced mean maximum RSS by 262,122 KiB (255.979 MiB, 8.086%);
- reduced the largest-window writer-flush time by 9.828%;
- preserved all 34 storage files and 972,969,365 corpus bytes exactly;
- passed complete footer validation; and
- passed 40/40 independent readbacks with zero skips, isolation skips, or
  mismatches.

This is a code-only reuse of an already authoritative in-memory inventory. It
does not change the FST contents, index encoding, checksums, or any on-disk
version.

## Change under test

Final segment metadata already contains an exact-postings map keyed by each
unique `(label_name_symbol, label_value_symbol)` pair. Before this change,
`LabelValueFstIndex::from_series` traversed every finalized series and every
label membership into:

```text
BTreeMap<label_name_symbol, Vec<label_value_symbol>>
```

It then sorted and deduplicated each vector before resolving strings and
building the FSTs. The accepted corpus contains 89,285,049 label memberships
but only 313,963 unique exact-postings keys. The old pass therefore retained
hundreds of megabytes of duplicate numeric values merely to recover an
inventory which exact postings had already computed.

`LabelValueFstIndex::from_exact_postings` now visits the unique keys and feeds
them into the same private sorting, symbol-resolution, and FST-construction
helper as `from_series`. It deliberately ignores each postings list: only key
presence defines the label-value inventory. The old constructor remains
available for independent fixtures and comparisons.

The shared helper still:

- groups values by numeric label-name symbol;
- sorts and deduplicates numeric value symbols;
- sorts values by their resolved strings as required by `fst::SetBuilder`;
- rejects missing value symbols as `InvalidData`; and
- propagates FST builder/finalization failures.

Using the same helper is important: it makes the new source inventory an
implementation detail while retaining the established output ordering and
error behavior.

## Correctness and authority boundary

The production exact postings and FST are not independent user inputs. The
private `finalize_segment_symbol_ids` operation:

1. synthesizes any required metric-name label;
2. remaps all series labels through the final byte-sorted symbol dictionary;
3. sorts each series row by remapped label-name symbol;
4. inserts every finalized label membership into `CompactPostingsBuilder`; and
5. returns the finalized series, symbols, exact postings, and time-range
   inventory together.

For Schema 6, the production serializer performs root-bound validation and
receives both indexes from that single private construction path. The existing
`finalized_postings_match_legacy_after_symbol_remap` test independently
rebuilds postings from the finalized series and proves complete equality after
a deliberately non-canonical symbol remap.

Schema 7 and Schema 8 add stronger same-seal authorization before encoding.
Their authenticated writer exhaustively validates that exact postings contain
every finalized series-label membership and no foreign membership, then
validates the complete FST name/value inventory against those postings.
Malformed symbols, missing memberships, foreign memberships, invalid FSTs,
extra values, and omitted values remain hard errors.

The new focused test uses unsorted symbol insertion, repeated memberships,
multiple names, and multiple values. It independently builds the FST inventory
from the finalized-series shape and exact postings and requires complete
`LabelValueFstIndex` equality plus the expected lexicographic values.

## Heaptrack memory evidence

The control is the frozen promoted bounded-postings binary and trace. Both
profiles use the Rust system allocator, Schema 8, the same accepted capture
prefix, and deterministic segment seed 42.

| Requested-live measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Official process maximum | 3,629,310,426 B | 3,271,002,194 B | -358,308,232 B (-341.709 MiB, -9.873%) |
| Numeric label-value collection peak | 537,509,136 B | 1,960,384 B | -535,548,752 B (-510.739 MiB, -99.635%) |
| Process live at that collection peak | 3,626,545,890 B | 3,090,997,058 B | -535,548,832 B (-510.739 MiB, -14.767%) |
| Collection cumulative requested bytes | 1,080,419,392 B | 3,841,376 B | -1,076,578,016 B (-99.644%) |
| Collection allocation calls | 13,617 | 4,297 | -9,320 (-68.444%) |

The official process maximum is from the final exact promotion binary. The
allocation-site rows are from the initial measured Schema 8 algorithm trace,
whose patch digest was
`f973b74c0ed7c033ee0df5daccd2fd8a9998307c81cea76e495d008f911b6b2a`.
Review then found two assurance issues outside the optimized Schema 8
inventory traversal: the shared helper retained already processed vectors, and
Schema 6 lost its independently series-derived FST path. The final patch
consumes those vectors and keeps Schema 6 on `from_series`; the Schema 7/8
collection loop is otherwise unchanged. A fresh final-binary Heaptrack run
reproduced the algorithm trace's official 3,271,002,078-byte maximum within
116 bytes. The site trace is retained as causal attribution, while all
promotion-level whole-process and runtime values below come from the final
source.

At the collection peak, the complete process reduction differs from the
target-site reduction by only 80 bytes. This makes the causal attribution
unusually strong: the removed duplicate value inventory accounts for
effectively the entire stage-local improvement.

The final whole replay preserves essentially the same allocation-count
reduction:

| Allocation measure | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| Whole-process allocation calls | 240,160,312 | 240,150,999 | -9,313 (-0.003878%) |
| Temporary allocations | 40,535,727 | 40,534,947 | -780 (-0.001925%) |
| Heaptrack runtime | 92.42 s | 92.64 s | +0.22 s (directional only) |
| Reported leaked bytes | 414,748 B | 414,748 B | unchanged |

The candidate's peak moved later, after the FST collection was freed. In the
allocation-site trace, the raw event parser observes 3,304,556,510 bytes at
77.523 seconds, while the official Massif export reports 3,271,002,078 bytes.
The approximately 32 MiB difference comes from Heaptrack's
suppression/accounting treatment at the shifted peak. Official Massif values
are therefore used for whole-process comparison; raw event accounting is used
only for exact allocation-site lifetime attribution. The control raw and
official peaks differed by only 2,328 bytes because its maximum occurred at
the earlier FST crest.

GNU `time` observed the profiled run's maximum RSS move from 3,243,476 KiB to
2,990,020 KiB (-253,456 KiB, approximately -247.516 MiB). That allocator/OS
measure agrees directionally but is not substituted for requested-live
accounting.

## Formal ABBA runtime evidence

Two accepted counterbalanced blocks each used control, candidate, candidate,
control order. All eight runs reproduced the accepted replay counters and
exact corpus. Means are arithmetic means of four observations per binary.

| Measure | Control mean | Candidate mean | Change |
| --- | ---: | ---: | ---: |
| Wall time | 42.280 s | 41.3575 s | -0.9225 s (-2.182%) |
| Task clock | 42,286.890 ms | 41,361.325 ms | -925.565 ms (-2.189%) |
| Cycles | 234,964,113,697 | 229,801,243,210.25 | -2.197% |
| Instructions | 690,206,899,264.75 | 669,174,844,774.75 | -3.047% |
| Branches | 128,036,637,902.75 | 124,270,466,064.75 | -2.941% |
| Branch misses | 481,709,054.5 | 476,829,924 | -1.013% |
| Cache references | 8,141,316,986.25 | 7,960,026,754.5 | -2.227% |
| Cache misses | 1,194,122,187 | 1,191,678,105.75 | -0.205% |
| Maximum RSS | 3,241,563 KiB | 2,979,441 KiB | -262,122 KiB (-255.979 MiB, -8.086%) |
| Largest-window elapsed | 21,458 ms | 20,583.25 ms | -874.75 ms (-4.077%) |
| Largest-window writer flush | 11,327.5 ms | 10,214.25 ms | -1,113.25 ms (-9.828%) |

Each candidate arm was faster than each control arm for wall time, task clock,
cycles, instructions, largest-window elapsed time, and writer flush. The
instruction reduction is the most stable CPU evidence and follows directly
from replacing 89.285 million membership visits with 313,963 key visits.
Control wall time ranged from 42.03 to 42.97 seconds and candidate wall time
from 41.18 to 41.67 seconds; the slowest candidate still beat the fastest
control. Control writer-flush dispersion was wider because one arm reached
12,000 ms, but the candidate's 10,196..=10,240 ms range remained completely
separated from the control's 11,091..=12,000 ms range.

This is an isolated 250,000-message code screen with four runs per arm, the
ordinary all-CPU scheduler policy, and turbo enabled. The counterbalanced
separation supports promoting the local code change; it is not a substitute
for the still-open four-million-message allocator/topology gate or a general
long-run throughput claim.

## Measurement contract

- Base source:
  `ebb5e920b8b92cb61eb335f0779c15851abad46f`
- Candidate patch SHA-256:
  `9feb460ac58e04d0ff15578981811d5cb2128a57d6d7a75c960c313c5c1c167c`
- Control ingester SHA-256:
  `6c953b91f25e926b6237c7e294312d08a7745a8b19d3b9d218c199eee2532d33`
- Control query SHA-256:
  `76ca6106e318829159b26770eeabf7c69a7a26f501797d2240e8b53ff7367a2c`
- Candidate ingester SHA-256:
  `57c83e747c73435bf218dde23f19313f3c6f09cf4a11eb052026e518e7e5cacb`
- Candidate query SHA-256:
  `344ab0f2685a16a30e860c1349ab14502daa77ac931e328b0151e18d6780ed58`
- Frozen control Heaptrack trace SHA-256:
  `2ab55181616264452b1fdfbf3033d583c26180233ba69a28b6680346a60e817b`
- Candidate Heaptrack trace SHA-256:
  `d4e4cd63f4800e3f4d94f32b4568ad82443af66a1d0c61c19749d8e289703fe6`
- Workload: exact accepted 250,000-message capture prefix
- Writer configuration: identical except for run-specific output paths;
  deterministic segment seed 42
- Storage schema: Schema 8
- Allocator: Rust system allocator

The evidence root is:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/postings-derived-fst-memory-final-20260724T220136Z-cvEWLf`

It retains the measured source patch, binary hashes, frozen ingester binaries,
counterbalanced perf-stat/RSS runs, Heaptrack trace and Massif export,
allocation-site analyses, rendered configurations, replay logs, correctness
summaries, complete file manifests, footer validation, independent readbacks,
and cleanup records.

## Correctness evidence

Every formal and profiled replay reproduced:

- 250,000 messages and 9,634,809 recorded samples;
- all event-time and storage acceptance/rejection counters;
- 4 deterministic segments, 34 files, and 972,969,365 bytes;
- manifest SHA-256
  `09d4d8b5143e714468bd1358ab929153c233264e215bcbbd6036234b7d1c045e`;
- replay-correctness JSON, corpus summary, complete file inventory, and every
  segment SHA-256 byte-for-byte;
- complete segment-footer validation; and
- all 40 independent readback-oracle cases with zero skips, isolation skips,
  or mismatches.

The byte identity includes `indexes.puffin`, which contains the label-value
FSTs. The optimization therefore changed neither FST membership nor encoded
bytes.

## Residual profile

Removing the old FST crest moved the requested-live maximum past FST
construction. The allocation-site trace placed its new raw peak at 77.523
seconds; the final exact binary independently reproduced the same official
maximum within 116 bytes. At the analyzed raw peak:

- the finalized `Vec<SeriesEntry>` outer allocation retains 246,826,160 bytes;
- the inline-one chunk-entry store's outer allocation retains another
  246,826,160 bytes; and
- the postings-derived numeric FST collection retains zero bytes.

Those two equal outer arrays are now visible residual families, but their
presence alone does not authorize another representation change. Any follow-up
must include complete allocation-site accounting, lifetime overlap, a
correctness-preserving design, and a fresh real-corpus A/B.

The experimental `postings-total-window.json` selector omits the bounded
large-growth allocation site and is not complete postings attribution. It must
not be compared with the prior 467,075,328-byte complete postings result.
Exact postings code and persisted postings bytes did not change in this
candidate.

## Artifact cleanup

After all A/B, profile, byte-equivalence, footer, readback, analysis, and
workspace verification gates completed, the cleanup plan revalidated every
segment manifest, file count, and logical byte count before removal. Across
the preliminary attribution root and final promotion root, it reclaimed:

- 14 regenerated segment trees containing 476 files and 13,621,571,110
  logical bytes;
- 4 redundant query binaries containing 2,018,836,664 bytes; and
- 480 files and 15,640,407,774 logical bytes in total (14.566 GiB).

Frozen ingester binaries, patches, hashes, Heaptrack traces and Massif exports,
perf/RSS data, logs, manifests, reports, allocation analyses, and explicit
cleanup records remain. No capture, accepted corpus, unrelated experiment, or
user-owned artifact was removed.

## Verification

The exact measured candidate source passed:

- the focused FST inventory equivalence test;
- all 212 `storage::index` library tests;
- strict `chronoxide-core` all-feature library Clippy;
- complete segment-footer validation;
- `chronoxide-query --verify-readbacks` with 40/40 executed and zero skipped;
- `cargo fmt --all -- --check`;
- `cargo test --workspace --all-targets --all-features`;
- both prescribed workspace-wide all-feature Clippy gates for libraries,
  binaries, tests, and benches with warnings denied; and
- `git diff --check`.

Because persisted bytes and semantics are unchanged, this candidate requires no
storage-version or `storage.md` change.
