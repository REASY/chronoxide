# PromQL Read Benchmark Range Design

## Goal

Extend `chronoxide-query` so the existing real-segment PromQL benchmark can
measure both instant queries and Prometheus-style range queries. Make payload
read amplification prominent in the generated report.

This changes benchmark tooling only. It does not change PromQL evaluation or
the segment format.

## Command-Line Interface

The existing repeatable `--query` option remains the workload input.

- With no `--step-ms`, every expression uses the existing instant-query path.
- With `--step-ms`, every expression uses `query_range` with the same start,
  end, and step. Instant and range workloads are run as separate invocations
  and produce separate reports.
- Range mode requires explicit `--start-ms` and `--end-ms` arguments.
- `--step-ms` must be greater than zero, and `--end-ms` must be greater than or
  equal to `--start-ms`.
- A range may schedule at most 1,000,000 evaluations. This is checked before
  opening the store so scalar expressions cannot bypass sample and I/O limits
  with an effectively unbounded step count.
- Range mode rejects `--prewarm-query-contexts` and
  `--prefetch-query-data`. Those helpers currently prepare a single instant,
  so accepting them would make range measurements misleading.
- Instant mode preserves all current defaults and behavior.

The report records `instant` or `query_range`. A range report also records the
exact start, end, step, and scheduled evaluations per run. Evaluations are
inclusive and therefore total `floor((end - start) / step) + 1`.

## Execution and Measurement

The benchmark continues to open one store, then one fresh query session per
expression. The first measured execution in that session is labelled cold;
later repetitions are labelled warm. Cold is session-local: sessions share the
store and its caches, so later expressions can benefit from earlier queries.
The benchmark does not flush or bypass the operating-system page cache.

Instant mode calls `query_promql_with_limits`. Range mode calls the existing
`query_promql_range_with_limits`. Result-series and result-sample counts,
query limits, query statistics, and session-profile deltas use the same report
pipeline in both modes. Range result samples count all matrix points returned
across evaluation steps.

## Payload Byte Efficiency

The report surfaces the following measured values for the total benchmark and
for each query run:

- **Used bytes:** `chunk_payload_bytes`, the sum of exact encoded chunk payload
  ranges selected by the query.
- **Read bytes:** `chunk_payload_physical_bytes`, the sum of coalesced payload
  spans requested from `chunks.bin` by the query reader.
- **Read / used:** read bytes divided by used bytes. A run that selects no
  payload reports an em dash instead of a numeric ratio.

These counters measure query-reader payload I/O amplification before operating
system caching. They do not claim to measure storage-device traffic. Existing
logical-read, positional-read, and locality tables remain available for the
detailed breakdown.

## Real-Replay Workload

After the focused tests pass, the benchmark will run against
`data/perf/segment-index-v7/segments-replay-v7-no-record-index` in two reports.
The instant report will cover broad and selective equality, a missing metric,
label regex matching, aggregation, `rate`, `increase`, one-to-one and
many-to-one joins, summary projection, classic histogram quantiles, native
explicit-histogram quantiles, and native exponential-histogram quantiles.

The range report will cover the final hour of the corpus at a 60-second step
with a range-vector rate, an aggregation over that rate, and a native explicit
histogram quantile. The workload will use the production query limits. A
metric-name regex known to exceed the production regex-expansion limit is not
part of the successful suite; limit behavior remains covered separately.

## Errors and Reporting

Invalid range configuration fails before the store is opened or an output file
is written. Query evaluation and query-limit failures retain the expression in
their error message. A failed invocation returns non-zero and does not present
partial results as a successful benchmark.

## Verification

Tests in the existing `chronoxide-query` binary test module will prove:

- existing instant CLI defaults and benchmark behavior are unchanged;
- range CLI arguments select range mode;
- missing bounds, zero step, reversed bounds, excessive evaluation counts, and
  range prewarm/prefetch are rejected;
- an inclusive three-step `time() + 1` range returns three result samples;
- range metadata and scheduled evaluation count appear in Markdown;
- payload used bytes, read bytes, and their ratio appear in totals and per-run
  output, including the zero-used-bytes case.

Focused binary tests, `git diff --check`, and both real-replay benchmark
invocations are required before completion is reported.
