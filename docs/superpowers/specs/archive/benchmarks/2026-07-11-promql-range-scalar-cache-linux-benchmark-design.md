# PromQL Range Scalar-Cache Linux Benchmark Design

> **Archived historical record:** This document is retained for provenance and is not current authority. Consult the current contracts and code before relying on it.

## Goal

Provide a reproducible Linux amd64 procedure for measuring the decoded
scalar-lane cache on the real V7 replay corpus. The deliverable must support a
quick cache-off/cache-on confidence check and the complete historical
acceptance protocol, while proving that both modes return identical query
semantics.

The benchmark compares execution modes within one Linux host. Absolute Linux
latency and RSS are not compared with the original arm64 macOS values.

## Deliverables

Add four files under `docs/experiments`:

- `promql_range_scalar_cache_benchmark.md`: operator guide;
- `promql_range_scalar_cache/run.sh`: Linux runner;
- `promql_range_scalar_cache/analyze.py`: result validator and analyzer; and
- `promql_range_scalar_cache/test_analyze.py`: standard-library unit tests.

The implementation does not modify Rust production code, query semantics,
segment formats, the replay corpus, or existing runtime reports.

## Scope and Non-Goals

The scripts exercise sealed-segment PromQL range queries with the decoded
scalar-lane cache disabled and enabled. They do not benchmark:

- ingestion, replay, compaction, or segment writing;
- instant-query execution;
- a V7 index-format change;
- cold-device I/O; or
- footer validation cost.

The CLI's `cold` run means the first run for an expression in a fresh query
session. The runner never drops or bypasses the operating-system page cache.

## Historical Reproduction Inputs

Exact historical reproduction uses:

- cache implementation commit
  `cb89579790b2bb9d5e322cb97ade06dc1ec76f1d` or a descendant containing it;
- the existing `chronoxide-core/src/storage/segment/query_promql.rs` working
  tree diff with SHA-256
  `a2b5aea77bc55f35cafdc9cd8433e6bb2b87358a596968fe67ea3fe33a0fb8cd`;
- Rust and Cargo 1.95.0;
- the complete
  `data/perf/segment-index-v7/segments-replay-v7-no-record-index` directory,
  including its manifest; and
- corpus fingerprint
  `b9c1470b99726c3f6a53591bf5ec7fb8f96b0691f474e6935a27fce6de145891`.

The original Mac binary SHA-256 is historical evidence only. A Linux amd64
binary is expected to have a different digest. The runner records the Linux
binary digest and uses that single binary for every process in one invocation.

The guide explains how to export, transfer, apply, and verify the uncommitted
`query_promql.rs` patch. It also explains comparison mode: an explicit source
mismatch override permits an internally valid off/on experiment, but the
analyzer then treats historical semantic fingerprints as advisory and requires
off/on semantic equivalence instead.

## Fixed Workload

Every measured process runs this configuration:

- range: `1782982800000..1782986400000`, inclusive;
- step: `60000` ms, producing 61 evaluations;
- repetitions: five per query, with run zero cold and runs one through four
  warm;
- scalar query 1:
  `rate(go_gc_duration_seconds_count[15m])`;
- scalar query 2:
  `sum by (service_name_x55e50a58f9befba7)(rate(go_gc_duration_seconds_count[15m]))`;
- unaffected control:
  `histogram_quantile(0.95, sum by (service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257[15m])))`;
- cache off: `--range-scalar-cache-max-bytes 0`; and
- historical cache on: `--range-scalar-cache-max-bytes 8388608`.

The flag is always explicit because omitting it currently selects the 16 MiB
default rather than the historically selected 8 MiB candidate.

Production query-limit defaults, `--prewarm-query-contexts=false`,
`--prefetch-query-data=false`, and `--validate-segment-footers=false` remain
unchanged. Footer validation is run separately, if requested, so its complete
file reads do not contaminate timed RSS or latency.

## Runner Interface

The Bash runner is invoked from any directory and resolves the repository root
from its own location:

```text
run.sh quick --segments-dir DIR [options]
run.sh full  --segments-dir DIR [options]
```

Supported options are:

- `--out-dir DIR`: new output directory; the default includes a UTC timestamp;
- `--cpu-list LIST`: optional `taskset -c` CPU list applied identically to all
  measured processes;
- `--no-build`: reuse the selected release binary after recording its digest;
- `--binary PATH`: select the binary used with `--no-build`;
- `--allow-source-mismatch`: run comparison mode instead of enforcing the
  historical source state; and
- `--dry-run`: validate arguments and print the commands without building,
  reading the corpus, or writing reports.

Normal execution validates Linux, `x86_64`, GNU `/usr/bin/time`, Python 3,
Cargo/Rust, Git, SHA-256 tooling, optional `taskset`, source provenance, the
corpus path, and output safety. It refuses an existing non-empty output
directory and never deletes output or corpus data.

Unless `--no-build` is supplied, the runner executes exactly one locked release
build with Rust 1.95.0:

```text
cargo +1.95.0 build --locked --release \
  -p chronoxide-ingester --bin chronoxide-query
```

The runner records Git identity/status, relevant source diff hashes, Rust and
Cargo versions, Linux/kernel/CPU/memory/filesystem information, corpus size,
binary SHA-256, CPU affinity, and execution order before timing.

GNU Time writes a small key/value file containing elapsed seconds, user/system
seconds, and maximum RSS in KiB. The analyzer converts RSS to bytes before
comparing it with cache charges.

## Quick Mode

Quick mode runs two fresh processes in fixed order:

1. cache off with a zero-byte budget;
2. cache on with an 8 MiB budget.

It retains each process's Markdown, raw JSON, standard output/error log, and
GNU Time report. It is a functional and directional confidence check, not a
statistically sufficient acceptance run.

After the first process, the runner checks the raw report schema, corpus
fingerprint, fixed configuration, and run count before continuing. After both
processes, it invokes the analyzer.

## Full Mode

Full mode builds once, then performs the original two stages without unrelated
work between them.

First it runs one fresh process for each candidate budget:

- 4 MiB (`4194304` bytes);
- 8 MiB (`8388608` bytes);
- 16 MiB (`16777216` bytes); and
- 32 MiB (`33554432` bytes).

The analyzer computes the median of the four warm durations for each scalar
query. It selects the smallest cap whose median is no more than 2% slower than
the best cap for both scalar queries. The chosen byte count is persisted and
passed to the paired stage. For the historical corpus, 8 MiB is expected.

The runner then executes nine pairs of fresh processes. Odd pairs run off then
on; even pairs run on then off. Each process still contains 15 query runs, so
the complete sweep and paired protocol contains 330 query runs. Builds,
replays, profilers, and pairs are never overlapped by the runner.

## Output Layout

One invocation creates a self-contained directory:

```text
OUT/
  environment.txt
  source-mode.txt
  binary.sha256
  selected-cap-bytes.txt       # full mode
  quick/pair-01-{off,on}.{md,json,time,log}
  sweep/cap-{4,8,16,32}m.{md,json,time,log}
  pairs/pair-01-off.{md,json,time,log}
  pairs/pair-01-on.{md,json,time,log}
  ...
  pairs/pair-09-on.{md,json,time,log}
  analysis.json
  analysis.md
```

Only paths applicable to the selected mode are created. Output remains under
untracked benchmark storage and is never staged automatically.

## Analyzer Interface and Input Validation

The Python analyzer uses only the standard library:

```text
analyze.py OUT_DIR
analyze.py select-cap SWEEP_DIR
```

`select-cap` prints only the selected byte count on standard output so the
runner can consume it. Normal analysis writes `analysis.json` and
`analysis.md`, and prints a concise summary.

For every process, the analyzer requires:

- raw schema `chronoxide.query-benchmark.raw/v2`;
- the fixed corpus, bounds, step, queries, limits, and five repetitions;
- exactly 15 runs with one cold and four warm runs per query;
- one consistent binary/source run context;
- expected cache budget for its filename/mode; and
- parseable GNU Time and Markdown read-profile data.

It compares normalized off/on rows after removing only duration and cache
summary fields. Effective bounds, result series/sample counts, semantic
fingerprints, and every public `QueryStats` field must match exactly.

In exact-source mode, these historical semantic fingerprints are additionally
required:

- scalar rate:
  `5eb2038224f4280e3f45806f14d3585db0de494e94d59c44f3ff3168917343a2`;
- grouped scalar rate:
  `65215f26762abdea2af50219305207bf3682732329287fafe4f1b4ba9cb08f78`;
- native control:
  `61362a460f33920a99b28795230354eac99500b6e38080f668bdcc169add695b`.

Exact-source analysis also compares result counts and every `QueryStats` field
with the committed pre-cache oracle at
`docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-results-v1.json`.
That artifact is the canonical semantic baseline; timing fields are ignored.
Comparison mode reports differences from the historical oracle but fails only
when the measured off/on modes differ from each other.

The analyzer parses physical payload reads and bytes from the named Markdown
read-profile table because raw schema v2 does not contain those fields. Parsing
uses column names rather than fixed column positions.

## Calculations and Gates

Latency calculations exclude run zero. A process/query latency is the median
of warm indices one through four. A pair's gain is:

```text
1 - on_warm_median / off_warm_median
```

The full analysis computes:

- per-pair and median paired gain for each query;
- count of pairs in which cache-on is faster;
- population coefficient of variation across the nine process medians for
  each query and mode;
- paired maximum-RSS deltas in bytes;
- cache hit, miss, admission, bypass, refusal, peak, and final charges;
- logical versus physical payload reads and bytes; and
- cap-sweep warm medians and selection.

The complete acceptance gate requires:

- median paired gain of at least 10% for both scalar queries;
- cache-on faster in at least eight of nine pairs for both scalar queries;
- native-control median regression no greater than 3%;
- process-median population CV no greater than 3% for every query and mode;
- exact semantic/result/stat equivalence;
- median paired RSS delta no greater than median cache peak plus 4 MiB;
- decreased physical reads and decoded misses for scalar queries;
- no governor, allocation, unsupported-layout, or selected-cap budget bypass;
  and
- every retained and process-current charge finalized at zero.

Quick mode reports correctness, cache activity, I/O direction, warm medians,
and RSS without claiming the full statistical gate.

Malformed or semantically inconsistent evidence exits with status 1. A valid
full dataset that misses a performance gate writes complete analysis and exits
with status 2. A passing dataset, or a valid quick dataset, exits with status
0.

## Safety and Measurement Discipline

The guide and runner require an otherwise idle host and recommend a local
SSD/NVMe filesystem, at least 50 GiB free for source/build/corpus/results, and
enough RAM to avoid unrelated pressure. The user records CPU governor, turbo,
SMT, NUMA, affinity, and filesystem state but the runner does not change system
configuration or require root.

The runner does not invoke `cargo run`, `perf`, replay, cache dropping, sudo,
or footer validation during measured processes. It never compares Linux RSS
directly with the Mac `/usr/bin/time -l` byte value; GNU Time's KiB value is
converted first.

## Verification

Implementation verification includes:

- `bash -n` for the runner;
- runner `--dry-run` coverage for quick/full order and argument failures;
- Python unit tests over synthetic quick, sweep, and nine-pair fixtures;
- unit tests for median, population CV, cap selection, Markdown table parsing,
  GNU Time parsing, normalization, exit classification, and malformed inputs;
- analysis of the existing historical artifacts, confirming the recorded cap,
  gains, I/O values, RSS calculation, and expected CV gate failure; and
- `git diff --check` with an explicit staged-file audit before the focused
  documentation/tooling commit.

Existing user changes and runtime artifacts remain unstaged and unmodified.
