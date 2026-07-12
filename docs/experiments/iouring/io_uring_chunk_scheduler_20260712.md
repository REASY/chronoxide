# General chunk-payload scheduler experiment

## Status

The backend-independent scheduler and all payload-consumer migrations are
implemented, correctness-tested, and validated with the complete nine-repeat
real-corpus matrix. The evidence supports controlled opt-in use of auto mode.
It does not support changing the production default because the warm sparse
payload subphase has a consistent regression, despite neutral total latency.

## Implementation under test

The query payload path now has an explicit plan, fetch, and decode split:

- all per-segment Float, Int64, Histogram, ExponentialHistogram, Summary, and
  compact scalar-lane reads execute through one scheduler;
- native Histogram and ExponentialHistogram share one cross-file fetch path;
- the generic selector path can batch Float, Int64, Summary, virtual bucket,
  and typed `_count` / `_sum` payloads across segment files;
- per-file coalescing remains unchanged;
- scheduler groups remain bounded to 32 segment items, 256 physical spans,
  and 256 MiB;
- forced pread, forced io_uring, and conservative auto modes use the same
  physical plans and ordered results;
- the production CLI default remains pread and cross-segment planning remains
  behind `--experimental-cross-segment-chunk-reads`;
- auto requires at least eight time-overlapping candidate segments before
  using the cross-segment flow, then requires at least eight coalesced physical
  spans and queue depth of at least eight before selecting io_uring. Shallow
  work stays on per-segment pread.

The query session owns one persistent ring. No registered buffers, registered
files, index-reader changes, shared seek cursor, or on-disk change is included.

## Correctness evidence

Focused coverage includes:

- exact multi-segment equivalence for every payload kind and virtual scalar /
  bucket projection;
- exact semantic fingerprint and every `QueryStats` field;
- logical bytes, physical spans, and physical bytes;
- matched-series, projected-series, chunk, byte, and decoded-sample limits;
- cache disabled and enabled behavior;
- earlier payload corruption taking precedence over a later segment-planning
  error;
- empty, one-span, queue-depth-plus-one, and group-boundary plans;
- identical offsets in different files;
- overlapping ranges and gaps exactly at and beyond the coalescing threshold;
- missing and short backend results;
- synthetic out-of-order io_uring completion restoration plus missing,
  duplicate, and out-of-range completion errors;
- forced io_uring refusing to silently fall back when unavailable;
- auto-policy boundaries at seven, eight, and nine physical spans;
- profile accumulation, delta, and CLI reporting, including peak bytes in one
  concurrent backend submission rather than the whole scheduler group.

Completed gates:

```text
cargo test -p chronoxide-core                                      PASS
cargo test -p chronoxide-ingester --bin chronoxide-query           PASS
cargo test -p chronoxide-ingester --test source_level_e2e          PASS
cargo test -p chronoxide-core --features io_uring <focused tests>  PASS
cargo fmt --all -- --check                                         PASS
git diff --check                                                   PASS
```

The full core run included the independent range scalar cache error, lifecycle,
semantic-oracle, and pre-change oracle suites. Scheduler telemetry is
profile-only; the cache oracle normalizes it with the existing physical-read
fields when comparing logical profiles.

## Benchmark runner

`promql_cross_segment_bench_run.sh` now:

- defaults to nine repetitions;
- interleaves forced pread, forced io_uring, and auto cases;
- records payload-page-evicted and warm runs separately;
- calls `POSIX_FADV_DONTNEED` before every evicted run;
- records `fincore` page/byte residency before execution and rejects remaining
  pages above the configured threshold;
- records scheduler backend decisions, submissions, SQEs, maximum depth, and
  peak in-flight bytes in `summary.tsv`;
- checks exact semantic fingerprints, serialized `QueryStats`, logical payload
  bytes, physical spans, and physical payload bytes;
- checks for competing benchmark/build processes immediately before and after
  every measured process;
- includes the native histogram family, a selective aggregated scalar-lane
  query, the prior shallow range schedule, and the existing sparse scalar
  workload;
- preserves raw data, logs, residency snapshots, environment metadata, binary
  hash, commit, dirty patch, and human reports in one external run directory.

`POSIX_FADV_DONTNEED` and `fincore` cover Linux page-cache residency only. They
do not flush or measure the NVMe/controller cache.

The acceptance command, from a shell that reports `ulimit -l` as `65536`, is:

```sh
REPEATS=9 docs/experiments/iouring/promql_cross_segment_bench_run.sh
```

For a shorter runner validation without the expensive sparse selector:

```sh
REPEATS=1 INCLUDE_SPARSE=0 \
  docs/experiments/iouring/promql_cross_segment_bench_run.sh
```

## Ordinary-buffer allocation profile

A supporting warm-cache `perf record` profile used the real replay corpus and
the selective aggregated scalar-lane query with forced cross-segment io_uring.
It ran 20 repetitions in one process using the same persistent QD8 ring. All
20 semantic fingerprints, result counts, and `QueryStats` were identical. The
profile reported 1,620 physical spans/SQEs, 384,315,260 physical bytes, and a
2,754,428-byte maximum concurrent submission.

The report retained 285 cycle samples and lost eight (about 2.7%), so its
percentages are directional rather than acceptance measurements. Stacks that
ended in the io_uring `read_many` buffer `alloc_zeroed` path accounted for
approximately 1% of sampled cycles, including 0.32% directly attributed to
`__memset_avx512_unaligned_erms`. General malloc/free symbols were materially
larger, but their resolved stacks were dominated by query result-label and
evaluation allocations rather than payload buffers. This does not justify a
payload buffer pool, registered buffers, or registered files in the first
milestone.

Raw evidence is under the external directory
`io-uring-scheduler-perf-20260712-082957`, including `perf-repeat20-small.data`,
the text report, raw query JSON, markdown, logs, and binary SHA-256
`df353c385c8ee72800e0b1641b467838cc3c9295c3f6be7940b5fb86f701ca50`.
This profile is not part of the cold/warm latency acceptance matrix. With an
8 MiB memlock ceiling, perf buffers larger than eight pages caused the child
query's `io_uring_setup` to fail with `ENOMEM`, adding another reason to run
the final matrix under the documented finite 64 MiB limit.

## Environment blocker observed

The release binary built successfully with the io_uring feature. A diagnostic
runner attempt wrote only environment/build metadata under the external
run-specific directory ending in `io-uring-promql-shapes-20260712-081410`, then
aborted before measurement because the host-idle guard detected a concurrent
`codehop-server` / `codehop-index-worker` workload. No timing from that
directory is valid.

A second targeted runner validation used the selective scalar-lane query and
completed four single-run cases before the guard detected a newly started
unrelated Cargo build and aborted the remaining auto cases. Artifacts are under
`io-uring-promql-shapes-20260712-081808`. This validates the tooling, not
performance:

- every after-evict `fincore` snapshot reported zero resident payload pages;
- the following warm snapshots reported about 21 MiB resident;
- pread and io_uring produced the same semantic fingerprint, result counts,
  `QueryStats`, 5,022,492 logical bytes, 81 physical spans, and 19,215,763
  physical bytes;
- scheduler telemetry reported one pread execution with 81 positional
  submissions, and one io_uring execution with 81 SQEs over 11 submissions at
  maximum depth eight;
- the guard stopped before concurrent work could be knowingly included in
  later cases.

Do not compare the four timings: they are one sample each, their ordering did
not rotate, metadata/index residency differed, the memlock limit was 8 MiB,
and an unrelated build began during the schedule.

A final two-process runner validation completed after the host became idle,
under `io-uring-promql-shapes-20260712-082055`. For the same selective scalar
query, `cross-auto` selected io_uring for both evicted and warm runs and
reported the expected 81 SQEs, 11 submissions, and maximum depth eight. Its
fingerprint, `QueryStats`, results, logical bytes, physical spans, and physical
bytes match the forced cases. This proves the auto decision and runner fields,
but two cache states from one repetition are not acceptance evidence.

A shallow-schedule diagnostic then completed its payload-page-evicted case
under `io-uring-promql-shapes-20260712-082115` before the runner detected a new
unrelated build and aborted the warm case. Across 309 scheduler executions,
auto selected pread 308 times and io_uring once; the sole io_uring decision had
nine SQEs and maximum depth eight. This is evidence that the conservative auto
policy avoids queue overhead for almost all of this one-hour `[15m]` schedule,
but the unpaired, single-repetition latency is not acceptance evidence.

The Codex shell reports:

```text
MEMLOCK soft=8388608 hard=8388608 bytes
```

It cannot raise itself to the required 64 MiB hard limit. The final run must be
launched from the already-raised interactive shell or another process with the
documented finite 64 MiB limit.

The same 8 MiB shell reproduced the reason for that requirement during tests:
five rapidly created feature-test rings succeeded, then the immediately
following `io_uring_setup` returned `ENOMEM`. The request-order and generic
equivalence feature tests both passed when run in isolation. Production query
sessions reuse one persistent ring. The final 64 MiB benchmark completed 756
measured processes without `ENOMEM`.

## Nine-repeat results

The corrected final run is under
`io-uring-promql-shapes-20260712-155258`. It contains 756 measured processes:
nine repetitions, two payload-cache states, and six backend/flow cases for
each of seven workloads. It used QD8, binary SHA-256
`df353c385c8ee72800e0b1641b467838cc3c9295c3f6be7940b5fb86f701ca50`,
and a finite 64 MiB memlock limit.

For every workload, all 108 executions have one corpus fingerprint, one
semantic fingerprint/result shape/serialized `QueryStats` tuple, and one
logical-bytes/physical-spans/physical-bytes tuple. All 378
payload-page-evicted cases had zero resident payload bytes immediately before
execution. No benchmark, query, io_uring, or allocation failure occurred.

Payload-page-evicted medians for forced cross-segment backends are:

| Query | pread payload | io_uring payload | Payload improvement | pread e2e | io_uring e2e | E2e improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `count` | 23.191 ms | 14.795 ms | 36.2% | 255.568 ms | 247.129 ms | 3.3% |
| `sum` | 23.295 ms | 15.102 ms | 35.2% | 255.770 ms | 247.501 ms | 3.2% |
| `fraction` | 23.170 ms | 15.037 ms | 35.1% | 255.586 ms | 247.411 ms | 3.2% |
| `quantile` | 23.155 ms | 15.207 ms | 34.3% | 260.026 ms | 249.926 ms | 3.9% |

Paired-by-repetition median improvements are 33.1-36.2% for the payload phase
and 3.2-3.6% end to end. All four deep workloads therefore clear the 20%
payload and 2% end-to-end gates. Auto consistently selected io_uring for both
deep scheduler executions (24 SQEs over four QD8/QD8/QD7/QD1 submissions) and
produced comparable medians.

For the production candidate (`cross-auto` versus `default-pread`), no warm
end-to-end median regressed by more than 0.7% across any workload. The four
deep native queries improved by 0.5-1.0%. Warm payload timings are only a few
milliseconds and noisier in percentage terms.

The selective scalar-lane query filled QD8 (81 SQEs over 11 submissions). Its
payload-page-evicted medians improved from 21.225 ms to 19.679 ms (7.3%), while
end to end improved from 167.172 ms to 164.576 ms (1.6%). This confirms generic
batching works, but payload I/O is too small a fraction of total latency for a
large end-to-end gain.

For the shallow schedule, cross-auto chose pread 308 times and io_uring once
in every process. Against default pread, cross-auto improved the evicted
end-to-end median by 0.59% and regressed warm by 0.66%. Paired medians were a
0.28% improvement and a 0.31% regression, respectively. It clears the
less-than-1% shallow gate. Forced cross-segment modes remained worse,
confirming the need for the adaptive policy.

The corrected sparse scalar query reproduced the intended 254,219 series,
983,583 samples, 254,222 logical reads, 2,289 physical spans, and historical
fingerprint. Forced cross-io_uring improved payload-page-evicted medians from
88.164 ms to 72.615 ms (17.6%) and end to end from 2503.975 ms to 2486.122 ms
(0.7%). Auto used pread for one small execution and io_uring for the deep one:
2,287 SQEs over 287 io_uring submissions plus one pread submission. Against
default pread, cross-auto improved evicted payload by 15.7% and end to end by
0.9%.

The warm sparse payload subphase is the one unresolved production-default
concern: cross-auto took 18.035 ms versus 16.882 ms for default pread, a
consistent 6.8% regression (about 1.15 ms). Total warm query time improved by
0.22%, because payload I/O is less than 1% of the 2.4-second query. Therefore
the warm end-to-end gate passes, but a strict interpretation that applies the
1-2% threshold independently to the payload subphase does not.

Maximum observed RSS was 2,957,000 KiB on the result-heavy sparse query.
Maximum concurrently submitted payload bytes were 36,008,803 (34.34 MiB),
well below the 256 MiB group bound. The scheduler remained bounded throughout
the matrix.

### Acceptance audit

| Gate | Result | Evidence |
| --- | --- | --- |
| Semantic fingerprints | Pass | One fingerprint/result shape per workload across 108 cases |
| `QueryStats` | Pass | One complete serialized stats tuple per workload |
| Logical/physical accounting | Pass | One logical-byte/span/physical-byte tuple per workload |
| Deep payload improvement >=20% | Pass | 34.3-36.2% using case medians |
| Deep e2e improvement >=2% | Pass | 3.2-3.9% using case medians |
| Shallow regression <1% | Pass | 0.66% worst median regression |
| Warm regression | Qualified | E2e passes (<0.7% worst regression); sparse payload alone regresses 6.8% |
| Auto backend decisions | Pass | Deep work uses io_uring; small shallow/sparse executions use pread |
| Bounded memory | Pass | 34.34 MiB peak submitted bytes; 256 MiB group bound |
| No `ENOMEM` at 64 MiB memlock | Pass | All 756 processes completed |
| Correctness suites | Pass | Broad, cache-oracle, source-level, and focused io_uring suites listed above |

## Recommendation

Auto mode with cross-segment planning is ready for controlled opt-in use. It
meets every correctness gate, the deep cold payload/end-to-end gates, the
shallow gate, the warm end-to-end gate, memory bounds, and the finite-memlock
gate. It is not ready to become the production default: warm sparse payload
latency consistently regresses even though end-to-end latency does not, and
the feature remains behind the explicit experimental cross-segment flag. The
production default remains pread while feedback-based policy or another
warm-deep discriminator is investigated.
