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
| Range query API | Partial | Supported with WF-specific timing | Chronoxide `query_promql_range` evaluates the instant expression independently at each step and merges by labelset. Golden coverage now includes stored samples, scalar/rate steps, offsets, label functions, binary/scalar and nested vector-vector composition, classic/OTLP/native histogram projections, native custom/exponential histogram fraction/avg, native histogram scalar float-drop behavior, `changes`/`resets`, binary/scalar, and stale-latest absence composition, and sealed-plus-head float and typed-histogram range cases. More parity tests are still needed for subquery and deeper native-histogram range composition. Whitefalcon shifts range results by granularity because its storage is look-ahead. |
| Vector selectors | Supported | Partial | Chronoxide supports metric shorthand, brace-only selectors, equality, inequality, positive regex, negative regex, missing-label semantics, metric-name regex, and OTLP name normalization. Whitefalcon selectors are tied to its label/grouping model and warn when grouping is implicit. |
| Instant vector lookback | Partial | Partial | Chronoxide uses a fixed 5 minute instant lookback and skips Prometheus stale markers. Golden coverage includes stale latest samples, stale-only absence, stale markers inside range functions, binary/vector matching with stale operands, and query_range aggregation steps over stale markers. More stale parity testing is still needed across deeper composition shapes. Whitefalcon's range bucketing includes look-ahead/shift behavior. |
| `offset` modifier | Supported | Supported | Chronoxide supports `offset` on instant selectors and range selectors. `@` is not part of this support. |
| `@` modifier | Unsupported | Unsupported / not relied on | Prometheus supports explicit evaluation timestamp modifiers. Chronoxide does not lower this yet. |
| Subqueries | Unsupported | Unsupported for percentile/subquery combinations | Prometheus supports `[range:resolution]` subqueries. Chronoxide currently supports selector range arguments only for range functions. |
| Binary arithmetic | Supported | Supported | Chronoxide supports scalar-scalar, vector-scalar, scalar-vector, and vector-vector arithmetic for `+`, `-`, `*`, `/`, `%`, `^`. |
| Binary comparisons | Partial | Partial | Chronoxide supports comparison operators and `bool`, including vector/scalar forms. More edge-case parity is needed for vector matching cardinality and histogram operands. Whitefalcon rejects scalar comparisons without `bool`. |
| Set operators | Partial | Partial | Chronoxide supports `and`, `or`, `unless` for vectors. Scalar set operations are rejected. More vector-matching edge cases should be tested. |
| Vector matching | Partial | Partial | Chronoxide lowers `on`, `ignoring`, `group_left`, and `group_right`. Golden coverage includes successful matching and representative Prometheus cardinality errors for duplicate one-to-one sides, duplicate group one-sides, and duplicate grouped result series. Whitefalcon supports a narrower join context and rejects some grouping contexts. |
| Aggregations | Partial | Partial | Chronoxide supports `sum`, `count`, `avg`, `min`, `max`, `stddev`, `stdvar`, `group`, `topk`, `bottomk`, `quantile`, and `count_values` over float samples. Histogram-aware aggregation parity is partial. Whitefalcon maps many selector aggregations into native extractors and has WF-specific grouping behavior. |
| Common range functions | Partial | Partial | Chronoxide supports `rate`, `increase`, `delta`, `irate`, `idelta`, `changes`, `resets`, `last_over_time`, `count_over_time`, `present_over_time`, `sum_over_time`, `avg_over_time`, `stddev_over_time`, `stdvar_over_time`, `min_over_time`, `max_over_time`, `deriv`, `predict_linear`, `quantile_over_time`, and `double_exponential_smoothing` / `holt_winters`. Direct native Histogram / ExponentialHistogram `changes()` and `resets()` are covered for observable component changes/decreases. Remaining gaps are subquery arguments, full histogram operand parity, and deeper edge-case coverage for non-finite values. Whitefalcon implements a subset through bucketed aggregation; `deriv`, `predict_linear`, and `holt_winters` are lexer tokens but not implemented by the visitor. |
| `absent` | Supported | Unsupported | Chronoxide supports Prometheus-style `absent()` for instant vectors. Whitefalcon's lexer/visitor do not expose `absent()`. |
| `absent_over_time` | Supported | Partial / divergent | Chronoxide returns a Prometheus-style single sample when no non-stale sample exists in the range. Whitefalcon implements per-series count inversion and fills every range step with `0` or `1`, which intentionally differs from Prometheus sparse absence output. |
| Scalar literals and `time()` | Supported | Supported | Chronoxide evaluates scalar literals and `time()` at the evaluation timestamp. Whitefalcon has scalar nodes and `time()` nodes. |
| `vector()` | Supported | Supported | Chronoxide converts scalar expressions to a single-sample vector. Whitefalcon supports `vector()` for scalar nodes. |
| `scalar()` | Supported | Unsupported | Chronoxide returns the single non-stale vector sample as a scalar, or `NaN` when the input vector has zero or more than one element. Whitefalcon lexes `scalar` but the visitor rejects it as unknown. |
| Sort functions | Supported | Unsupported / not visible in evaluator | Chronoxide supports `sort` and `sort_desc` for instant vectors. Golden coverage checks result membership through promtool and result ordering through a separate Prometheus HTTP API oracle because promtool rule tests canonicalize vector order. Whitefalcon lexer lists sort functions, but the inspected visitor does not implement them. |
| Math functions | Partial | Partial | Chronoxide supports `abs`, `ceil`, `floor`, `round`, `clamp`, `clamp_min`, `clamp_max`, `ln`, `log2`, `log10`, `sgn`, `pi`, and the Prometheus trigonometric function family (`sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh`, `deg`, `rad`). Other Prometheus math helpers such as `exp` and `sqrt` are still unsupported. Whitefalcon supports abs/ceil/floor/round/clamp/log functions, plus `sqrt` token without inspected evaluator support. |
| Calendar functions | Supported | Supported | Chronoxide supports `minute`, `hour`, `day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`, `month`, and `year`, with optional input defaulting to `vector(time())`. Whitefalcon supports equivalent time extraction nodes. |
| Label functions | Supported | Partial | Chronoxide supports `label_replace` and `label_join`, preserving source labels and adding/replacing destination labels. Whitefalcon supports both; docs/history describe behavior that should not be copied blindly where it differs from Prometheus. |
| Classic histogram projections | Partial | Divergent | Chronoxide projects OTLP classic histograms into Prometheus-shaped `_count`, `_sum`, and cumulative `_bucket{le=...}` series with synthetic `le="+Inf"`. Whitefalcon stores histograms as T-Digest/percentile data and cannot filter by Prometheus `le` bucket labels. |
| Native histogram functions | Partial | Unsupported / different model | Chronoxide supports first-pass native histogram and exponential histogram storage/projection, plus `histogram_quantile`, `histogram_fraction`, `histogram_count`, `histogram_sum`, and `histogram_avg` for supported shapes. Golden coverage includes native sum aggregation, custom-bucket coarsening for changed and aggregated explicit-bound layouts, native histogram vector-scalar `*` and histogram/scalar `/` arithmetic for custom and exponential histograms, native histogram vector-vector `+`/`-` arithmetic, non-bool and `bool` `==`/`!=` comparisons, `group_left` / `group_right` binary modifiers for custom and exponential native histogram arithmetic, same-kind and mixed custom/exponential native histogram `and` / `or` / `unless` set operators, mixed custom/exponential equality comparison, vector matching, group modifier, non-finite sum/scalar arithmetic, finite-plus-infinite and same-infinite subtraction edges, and scaled aggregation for custom and exponential native histograms, and invalid arithmetic/ordering drop semantics including same-kind ordering `bool` drops, invalid scalar/histogram and histogram/histogram drop shapes, stale latest custom/exponential native histogram absence, stale custom/exponential native histogram arithmetic and set vector matching, infinite-bound `histogram_fraction`, and float-drop behavior for `histogram_count`, `histogram_sum`, and `histogram_avg` on float-only and mixed float/native input. Full Prometheus native histogram operator parity remains incomplete. |
| Summary projections | Partial | Divergent | Chronoxide projects OTLP summaries to `_count`, `_sum`, and `{quantile=...}` series, with Prometheus golden coverage for each projected shape. Whitefalcon percentile behavior is native to its percentile model. |
| Staleness | Partial | Divergent | Chronoxide persists and skips Prometheus stale markers in instant/range functions where implemented. Golden coverage includes binary arithmetic, `or`, and `unless` vector matching with stale operands, query_range aggregation over a stale step, OTLP delta Histogram and ExponentialHistogram stale-fragment projection, stale latest custom/exponential native histogram absence in instant and query_range output, and stale custom/exponential native histogram vector matching. More stale marker parity tests are needed across deeper compositions and additional native histogram operand combinations. Whitefalcon filters NaNs from output, which differs from Prometheus. |
| Counter resets | Partial | Partial | Chronoxide handles counter decreases and OTLP reset hints for scalar and typed histogram rate/increase paths, and direct native Histogram / ExponentialHistogram `resets()` now counts observable component decreases. More temporality boundary tests remain. Whitefalcon has simpler cumulative/delta handling in its rate evaluator. |
| OTLP temporality | Partial | Not applicable | Chronoxide preserves OTLP temporality and projects delta histograms/exponential histograms to cumulative PromQL-shaped series. Golden coverage compares delta histogram and exponential histogram projections against equivalent cumulative Prometheus series, including direct native-function rate over delta Histogram / ExponentialHistogram samples, delta Histogram / ExponentialHistogram stale-fragment boundaries for `_count`, `_sum`, and `_bucket`, delta Histogram / ExponentialHistogram reset-boundary projections for `_count`, `_sum`, and `_bucket`, query_range reset-boundary and stale-fragment projection steps, and direct native-function stale-fragment coverage for delta Histogram / ExponentialHistogram. Deeper reset and staleness compositions remain Chronoxide-specific correctness work. |

## Prometheus Golden Suite

`chronoxide-core/tests/prometheus_golden.rs` is an ignored integration test that
uses the real Prometheus `promtool test rules` evaluator as the oracle. Each
case writes synthetic Chronoxide segment data, runs the Chronoxide PromQL query,
emits those results as `exp_samples` in a generated promtool YAML file, and
passes only when Prometheus evaluates the equivalent fixture to the same
samples.

Run it with:

```sh
CHRONOXIDE_PROMTOOL=/path/to/promtool \
  cargo test -p chronoxide-core --test prometheus_golden -- --ignored --nocapture
```

The current golden cases cover:

- float counters and gauges: `rate`, `increase`, `irate`, `delta`, `idelta`,
  `changes`, `resets`, `last_over_time`, `count_over_time`,
  `present_over_time`, `sum_over_time`, `avg_over_time`, `stddev_over_time`,
  `stdvar_over_time`, `min_over_time`, `max_over_time`,
  `quantile_over_time`, `deriv`, `predict_linear`, and
  `double_exponential_smoothing`;
- instant/vector composition: `sum by`, `count by`, `avg by`, `min by`,
  `max by`, `stddev by`, `stdvar by`, `group by`, `topk`, `bottomk`,
  `quantile by`, `count_values by`, scalar extraction, timestamp extraction,
  filter and `bool` comparisons, `and`, `or`, `unless`, `ignoring(...)`,
  `group_left`, and `group_right`;
- binary vector matching error paths: duplicate one-to-one match groups on
  either side, duplicate one-side series for `group_left` / `group_right`, and
  grouped-result label collisions;
- label, scalar, math, and calendar functions: `label_join`,
  `label_replace`, `scalar`, `timestamp`, `sgn`, `pi`, trigonometric and
  hyperbolic functions, `abs`, `ceil`, `floor`, `round`, `clamp`,
  `clamp_min`, `clamp_max`, `ln`, `log2`, `log10`, `deg`, `rad`, `minute`,
  `hour`, `day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`,
  `month`, and `year`;
- `absent` and `absent_over_time`, including a stale-only range;
- stale and non-finite samples in selected aggregation, range, binary,
  vector-matching, and `count_values` paths, including positive infinity
  aggregation/range propagation, mixed `+Inf`/`-Inf` aggregate NaN behavior,
  and Prometheus label spelling for both `+Inf` and `-Inf`;
- `sort` and `sort_desc` result sets through promtool, plus explicit
  `sort` / `sort_desc` ordering against a Prometheus HTTP API oracle because
  Prometheus' rule-test comparator sorts expected and actual vectors before
  comparison;
- `double_exponential_smoothing` against a Prometheus HTTP API oracle with
  `promql-experimental-functions` enabled because Prometheus' rule-test path
  rejects the function as disabled;
- classic histogram bucket queries and `histogram_quantile`;
- OTLP typed Histogram projection to `_count`, `_sum`, and `_bucket`, including
  cumulative projection from delta temporality, plus native typed Histogram
  `histogram_quantile`, `histogram_count`, `histogram_avg`, and
  `histogram_fraction` compared against Prometheus custom-bucket native
  histograms, including direct native-function rate, stale-fragment handling,
  and reset-boundary projection handling over delta Histogram samples;
- OTLP typed ExponentialHistogram `_bucket` projection, including cumulative
  projection from delta temporality, plus native `histogram_quantile` and
  `histogram_fraction` compared against Prometheus native exponential
  histograms, including direct native-function rate over delta
  ExponentialHistogram samples, stale-fragment handling, and reset-boundary
  projection handling;
- native histogram aggregation and bound edge cases, including `sum by (...)`
  over native exponential histogram rates, custom-bucket coarsening for changed
  and aggregated explicit-bound layouts, native histogram vector-scalar `*`
  and histogram/scalar `/` arithmetic for both classic/custom and exponential
  native histograms, native histogram vector-vector `+`/`-` arithmetic and
  non-bool plus `bool` `==`/`!=` comparisons for both classic/custom and
  exponential native histograms, non-finite native histogram sum/scalar
  arithmetic, finite-plus-infinite and same-infinite subtraction edges, and
  scaled aggregation for both custom and exponential histograms,
  `group_left` /
  `group_right` binary modifiers for custom and exponential histograms,
  same-kind and mixed
  custom/exponential `and` / `or` / `unless` set operators, mixed
  custom/exponential equality comparison, vector matching, group modifier, and
  invalid arithmetic/ordering drop semantics including same-kind ordering
  `bool` drops, invalid scalar/histogram and histogram/histogram drop shapes,
  direct `changes()` over custom and exponential native histograms with changed
  and unchanged samples, direct `resets()` over custom and exponential native
  histograms with observable component decreases,
  stale latest custom/exponential native histogram absence for
  `histogram_count`,
  stale custom/exponential native histogram arithmetic and set vector matching,
  query_range `changes()` / `resets()` steps for custom and exponential native
  histograms,
  `histogram_fraction(-Inf, Inf, ...)`, plus `histogram_count`,
  `histogram_sum`, and `histogram_avg` dropping float-only input and preserving
  only the native histogram output from mixed float/native input;
- OTLP Summary quantile, `_count`, and `_sum` projection;
- query_range step output for stored selectors, offsets, label functions,
  scalar counters/rates, stale aggregation steps, binary/scalar rate
  composition, nested vector-vector rate/aggregation composition, classic
  histogram quantiles, OTLP Histogram projection quantiles, OTLP delta
  Histogram and ExponentialHistogram reset-boundary and stale-fragment
  projection, and native custom/exponential histogram rate/aggregation
  quantiles, fraction/avg composition, scalar float-drop behavior,
  `changes`/`resets`, binary/scalar composition, and stale-latest absence;
- head-aware query_range output for a sealed-plus-active-head counter rate and
  a sealed-plus-active-head typed Histogram projection quantile.

This is now a real Prometheus-backed proof harness, but not yet a complete
proof for every supported expression form. Remaining expansion needed for a
full proof includes deeper stale compositions and remaining non-finite edge
cases, subquery and deeper native-histogram query_range composition beyond the
currently covered rate/fraction/avg/changes/resets paths, remaining native
histogram binary operator edge cases such as additional non-finite operand
combinations, native histogram error/drop cases beyond custom bucket coarsening,
and deeper OTLP delta reset/staleness compositions beyond the covered
projection and native-function paths.

## Near-Term Gaps

The current compatibility goal implemented:

- Common functions: `deriv`, `predict_linear`, `quantile_over_time`,
  `holt_winters` / `double_exponential_smoothing`, `scalar`, `sgn`, `pi`, and
  trigonometric functions.
- `absent()` remained unchanged because the audit did not find a concrete
  Prometheus incompatibility in the existing implementation.
- Query_range parity tests over stored samples, query sessions, head-aware
  paths, label functions, scalar/vector functions, offsets, range functions,
  and histogram projections.

Explicitly out of scope for this goal:

- `@` modifier.
- Subqueries.
- WAL/recovery work.
- Performance tuning.
