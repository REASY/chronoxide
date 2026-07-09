# PromQL Coverage

This document tracks Chronoxide PromQL compatibility against Prometheus target
semantics and the in-house Whitefalcon PromQL interface. Whitefalcon is a useful
comparison point, but Chronoxide should follow Prometheus where Whitefalcon
intentionally diverges.

Status legend:

- Supported: implemented with focused tests.
- Partial: useful support exists, but known semantics or coverage gaps remain.
- Unsupported: parser, lowering, or evaluator rejects the feature.

## Compatibility Matrix

| Area | Chronoxide status | Whitefalcon status | Prometheus target / notes |
| --- | --- | --- | --- |
| Parser and lowering | Partial | Partial | Chronoxide uses `promql-parser` and lowers supported expressions into an internal AST. OTLP-style dotted metric and label names are accepted and normalized. Unsupported parser forms fail during lowering rather than storage execution. Whitefalcon uses an ANTLR grammar with a narrower visitor; some tokens are lexed but rejected as unknown functions. |
| Instant query API | Supported | Supported | Chronoxide exposes core/store instant query methods and the `chronoxide-query` tool. Results are PromQL-shaped vectors/scalars represented as segment query results. Whitefalcon exposes HTTP instant query endpoints. |
| Range query API | Partial | Supported with WF-specific timing | Chronoxide `query_promql_range` evaluates the instant expression independently at each step and merges by labelset. More parity tests are needed for stored samples, head-aware execution, offsets, range functions, labels, scalars, and histogram projections. Whitefalcon shifts range results by granularity because its storage is look-ahead. |
| Vector selectors | Supported | Partial | Chronoxide supports metric shorthand, brace-only selectors, equality, inequality, positive regex, negative regex, missing-label semantics, metric-name regex, and OTLP name normalization. Whitefalcon selectors are tied to its label/grouping model and warn when grouping is implicit. |
| Instant vector lookback | Partial | Partial | Chronoxide uses a fixed 5 minute instant lookback and skips Prometheus stale markers. Further stale sample parity testing is still needed. Whitefalcon's range bucketing includes look-ahead/shift behavior. |
| `offset` modifier | Supported | Supported | Chronoxide supports `offset` on instant selectors and range selectors. `@` is not part of this support. |
| `@` modifier | Unsupported | Unsupported / not relied on | Prometheus supports explicit evaluation timestamp modifiers. Chronoxide does not lower this yet. |
| Subqueries | Unsupported | Unsupported for percentile/subquery combinations | Prometheus supports `[range:resolution]` subqueries. Chronoxide currently supports selector range arguments only for range functions. |
| Binary arithmetic | Supported | Supported | Chronoxide supports scalar-scalar, vector-scalar, scalar-vector, and vector-vector arithmetic for `+`, `-`, `*`, `/`, `%`, `^`. |
| Binary comparisons | Partial | Partial | Chronoxide supports comparison operators and `bool`, including vector/scalar forms. More edge-case parity is needed for vector matching cardinality and histogram operands. Whitefalcon rejects scalar comparisons without `bool`. |
| Set operators | Partial | Partial | Chronoxide supports `and`, `or`, `unless` for vectors. Scalar set operations are rejected. More vector-matching edge cases should be tested. |
| Vector matching | Partial | Partial | Chronoxide lowers `on`, `ignoring`, `group_left`, and `group_right`. Full Prometheus cardinality error parity is not yet proven. Whitefalcon supports a narrower join context and rejects some grouping contexts. |
| Aggregations | Partial | Partial | Chronoxide supports `sum`, `count`, `avg`, `min`, `max`, `stddev`, `stdvar`, `group`, `topk`, `bottomk`, `quantile`, and `count_values` over float samples. Histogram-aware aggregation parity is partial. Whitefalcon maps many selector aggregations into native extractors and has WF-specific grouping behavior. |
| Common range functions | Partial | Partial | Chronoxide supports `rate`, `increase`, `delta`, `irate`, `idelta`, `changes`, `resets`, `last_over_time`, `count_over_time`, `present_over_time`, `sum_over_time`, `avg_over_time`, `stddev_over_time`, `stdvar_over_time`, `min_over_time`, and `max_over_time`. Missing before the current compatibility work: `deriv`, `predict_linear`, `quantile_over_time`, and smoothing functions. Whitefalcon implements a subset through bucketed aggregation; `deriv`, `predict_linear`, and `holt_winters` are lexer tokens but not implemented by the visitor. |
| `absent` | Supported | Unsupported | Chronoxide supports Prometheus-style `absent()` for instant vectors. Whitefalcon's lexer/visitor do not expose `absent()`. |
| `absent_over_time` | Supported | Partial / divergent | Chronoxide returns a Prometheus-style single sample when no non-stale sample exists in the range. Whitefalcon implements per-series count inversion and fills every range step with `0` or `1`, which intentionally differs from Prometheus sparse absence output. |
| Scalar literals and `time()` | Supported | Supported | Chronoxide evaluates scalar literals and `time()` at the evaluation timestamp. Whitefalcon has scalar nodes and `time()` nodes. |
| `vector()` | Supported | Supported | Chronoxide converts scalar expressions to a single-sample vector. Whitefalcon supports `vector()` for scalar nodes. |
| `scalar()` | Unsupported | Unsupported | Prometheus returns the single vector sample as a scalar, or `NaN` when the input vector has zero or more than one element. Whitefalcon lexes `scalar` but the visitor rejects it as unknown. |
| Sort functions | Supported | Unsupported / not visible in evaluator | Chronoxide supports `sort` and `sort_desc` for instant vectors. Whitefalcon lexer lists sort functions, but the inspected visitor does not implement them. |
| Math functions | Partial | Partial | Chronoxide supports `abs`, `ceil`, `floor`, `round`, `clamp`, `clamp_min`, `clamp_max`, `ln`, `log2`, and `log10`. Missing before current compatibility work: `sgn` and trigonometric functions. Whitefalcon supports abs/ceil/floor/round/clamp/log functions, plus `sqrt` token without inspected evaluator support. |
| Calendar functions | Supported | Supported | Chronoxide supports `minute`, `hour`, `day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`, `month`, and `year`, with optional input defaulting to `vector(time())`. Whitefalcon supports equivalent time extraction nodes. |
| Label functions | Supported | Partial | Chronoxide supports `label_replace` and `label_join`, preserving source labels and adding/replacing destination labels. Whitefalcon supports both; docs/history describe behavior that should not be copied blindly where it differs from Prometheus. |
| Classic histogram projections | Partial | Divergent | Chronoxide projects OTLP classic histograms into Prometheus-shaped `_count`, `_sum`, and cumulative `_bucket{le=...}` series with synthetic `le="+Inf"`. Whitefalcon stores histograms as T-Digest/percentile data and cannot filter by Prometheus `le` bucket labels. |
| Native histogram functions | Partial | Unsupported / different model | Chronoxide supports first-pass native histogram and exponential histogram storage/projection, plus `histogram_quantile`, `histogram_fraction`, `histogram_count`, `histogram_sum`, and `histogram_avg` for supported shapes. Full Prometheus native histogram operator parity remains incomplete. |
| Summary projections | Partial | Divergent | Chronoxide projects OTLP summaries to `_count`, `_sum`, and `{quantile=...}` series. Whitefalcon percentile behavior is native to its percentile model. |
| Staleness | Partial | Divergent | Chronoxide persists and skips Prometheus stale markers in instant/range functions where implemented. More stale marker parity tests are needed across binary operators, aggregations, and query_range. Whitefalcon filters NaNs from output, which differs from Prometheus. |
| Counter resets | Partial | Partial | Chronoxide handles counter decreases and OTLP reset hints for scalar and typed histogram rate/increase paths. More temporality boundary tests remain. Whitefalcon has simpler cumulative/delta handling in its rate evaluator. |
| OTLP temporality | Partial | Not applicable | Chronoxide preserves OTLP temporality and projects delta histograms/exponential histograms to cumulative PromQL-shaped series. Prometheus target semantics are shaped by cumulative counters and native histograms; OTLP temporality remains a Chronoxide-specific correctness surface. |

## Near-Term Gaps

The current compatibility goal is limited to:

- Implement missing common functions: `deriv`, `predict_linear`,
  `quantile_over_time`, `holt_winters` / `double_exponential_smoothing`,
  `scalar`, `sgn`, and trigonometric functions.
- Keep `absent()` unchanged unless the audit finds a concrete Prometheus
  incompatibility.
- Add query_range parity tests over stored samples and head-aware paths.

Explicitly out of scope for this goal:

- `@` modifier.
- Subqueries.
- WAL/recovery work.
- Performance tuning.
