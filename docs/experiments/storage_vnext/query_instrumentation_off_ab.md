# Query-instrumentation Off observer-cost A/B

`query_instrumentation_off_ab_run.sh` is a focused code-version gate for one
question: did adding query observability make the production
`QueryInstrumentationMode::Off` path materially slower or larger?

This is not a storage-layout comparison. Both binaries read the same immutable
Schema 8 corpus with the same query, limits, label policy, queue depth, cache
budget, and release host. The reference is the immediately pre-instrumentation
raw-v9 binary. The candidate is raw v10 and is explicitly invoked with
`--query-instrumentation off`.

## Accepted result

The clean run completed at:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/query-instrumentation-off-ab-20260721T050544Z
```

The reference binary SHA-256 was
`1cb7773cd6e1884af9c28fea8faad1685ba751cf891baa5500f4e0a0c95c9b94`;
the candidate was
`52360c7b51253bd5bacbd6e1d94251c505e837a6c262a39b7aea0a0548819eb7`.
Both read the corpus with query fingerprint
`7e5cf252e5df9bdb786e1b9deb9248f09667962ac559f339ba47312c5c0e3ca3`.

| Metric | Reference median | Candidate Off median | Candidate change | Gate |
| --- | ---: | ---: | ---: | --- |
| CLI-cold query wall | 8,524.34 ms | 8,598.85 ms | +0.874% | pass, at most +3% |
| Warm query wall | 7,587.82 ms | 7,686.81 ms | +1.305% | pass, at most +3% |
| Process peak RSS | 4,531,114 KiB | 4,530,892 KiB | -0.0049% | pass, at most +5% |

All exact and portable fingerprints, result cardinality, public `QueryStats`,
label counters, range-cache counters, and logical/physical payload counters
matched. Every candidate Off stage leaf was zero. All process-start corpus
residency observations were zero, the corpus remained immutable, and separate
footer/readback validation passed. The observer-cost gate is therefore
accepted: the small latency movement is below the predeclared materiality
threshold and is not evidence of a production-path regression.

An earlier result at
`query-instrumentation-off-ab-20260721T031408Z` overlapped an unrelated Python
test. It remains preserved but is excluded from the accepted evidence.

## Fixed schedule and gates

The focused manifest contains the raw high-cardinality instant selector:

```promql
{__name__=~"^http_.*_count$"}
```

Each block runs fresh processes in strict reference-candidate-candidate-
reference order. The default two blocks therefore provide four cold process
observations per binary. Each process performs one cold and two warm query
evaluations. Before every process the runner issues `POSIX_FADV_DONTNEED` for
every inventoried corpus file and requires `fincore` residency to be at or below
the configured bound. This establishes a Linux page-cache eviction boundary;
it does not claim to flush device or controller caches.

The gate requires:

- identical semantic and portable fingerprints, result cardinality, public
  `QueryStats`, logical/physical payload-read counters, label counters, and
  range-cache counters across every A/B observation;
- raw v9 from the reference and raw v10 from the candidate;
- `query_instrumentation=off` and zero exclusive stage time in every candidate
  run;
- candidate/reference median cold and warm latency no greater than `1.03` for
  the named broad query;
- candidate/reference median process peak RSS no greater than `1.05` for that
  query.

The general-query latency threshold is `1.05` if a larger fixed manifest is
substituted. Thresholds are explicit runner inputs and are copied into both
`metadata/settings.txt` and `comparisons/comparison.json`; changing them creates
a different experiment contract.

Footer validation and independent readback verification run once per binary
before measurement. They are excluded from timed query processes. The corpus is
fully inventoried before and after the A/B, and any byte or path-set change
fails the run.

## Provenance

Both executables are copied into the result root and SHA-256 hashed. Each source
root records its Git commit, Git tree, worktree status, tracked binary patch,
and a derived source-state digest. A clean `git archive` source root is accepted
when its commit and tree are supplied explicitly; the runner hashes its complete
file inventory. Untracked Rust or Cargo build inputs in a Git worktree are
rejected.

Historical raw-v9 binary
`2df1ab5f69bb22047f1d484d77091ee0b3f114e50cda5fc19624ab5f552ecef0`
has a preserved patch and status under the 2026-07-16 query-label experiment,
but it was built from commit `e5d642d1...` plus a large dirty query stack. It is
not a valid observer-only reference for current commit `a8bd6d44...`; using it
would confound instrumentation overhead with intervening query changes. Build
the reference from a clean archive of the exact pre-instrumentation commit.

## Focused invocation

With the reference and candidate binaries already built, run on a quiet host
with no concurrent build, replay, validation, profiler, or query process:

```sh
CORPUS_DIR=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/compact-head-4m-ab-20260716-002041/candidate/segments \
REFERENCE_QUERY_BIN=/tmp/chronoxide-instrumentation-ab/reference-chronoxide-query \
CANDIDATE_QUERY_BIN=/tmp/chronoxide-instrumentation-ab/current-chronoxide-query \
REFERENCE_SOURCE_ROOT=/tmp/chronoxide-preinst-src \
REFERENCE_SOURCE_COMMIT=a8bd6d44d6c06375a09104a4a9c58ecbe6268021 \
REFERENCE_SOURCE_TREE=d0ea6c9c587c34894d5eb9fcefc9c5024529e2f6 \
CANDIDATE_SOURCE_ROOT=/home/user/github/REASY/chronoxide \
QUERY_MANIFEST=/home/user/github/REASY/chronoxide/docs/experiments/storage_vnext/query_instrumentation_off_ab_queries.json \
BROAD_QUERY_NAME=broad-full-label-selector \
RESULT_DIR=/absolute/new/query-instrumentation-off-ab-result \
RUN_NOTE='controlled quiet-host pre-instrumentation versus current Off ABBA' \
BLOCKS=2 \
BENCHMARK_REPEATS=3 \
docs/experiments/storage_vnext/query_instrumentation_off_ab_run.sh
```

The reference binary must emit raw schema v9 and not expose the instrumentation
flag. The candidate must emit raw schema v10 and expose `off|detailed`. If the
candidate source changes after its binary is built, rebuild and preserve a new
binary before measuring so the recorded source state describes the executable
under test.

## Evidence layout

- `metadata/binaries.tsv`: source/preserved paths and binary SHA-256 digests.
- `metadata/sources.tsv`: commit, tree, source-state, patch, and status digests.
- `metadata/settings.txt`: complete fixed configuration and thresholds.
- `inventory/`: byte-level corpus inventories before and after measurement.
- `validation/`: untimed footer and independent-readback evidence by binary.
- `runs/`: raw v9/v10 JSON, human report, process timing/RSS, pressure,
  conflict, and residency evidence for every ABBA process.
- `summary.tsv`: one row per cold/warm query run with fingerprints,
  `QueryStats`, payload/read amplification, process time, and RSS.
- `comparisons/comparison.json`: raw observation arrays, medians, ratios,
  thresholds, and the final correctness/performance dispositions.

The runner creates `COMPLETE` only after all gates pass and the post-run corpus
inventory matches. A failed gate deliberately preserves partial raw evidence
without that marker.
