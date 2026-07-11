# Prometheus Rate/Increase Staleness Hardening Plan

> **For agentic workers:** This plan records the implementation and verification
> actually performed on the existing uncommitted staleness work. The user asked
> that no commit be created before primary-agent review.

**Goal:** Make scalar, virtual OTLP projection, and direct native Histogram and
ExponentialHistogram `rate()` / `increase()` semantics agree with the tested
Prometheus contract without regressing native single-delta-interval support.

**Architecture:** Exact stale NaN is removed on a rare aligned scalar filtering
path; wide native samples are scanned through borrowed filtered references.
Delta projection conversion resets its internal accumulator at stale, omits
stale converter output, and gives the first restarted sample unknown-reset
detection. Scalar and multi-sample native counter evaluation use the original
range bounds, including logical pre-epoch duration. A single selected native
OTLP delta interval retains the direct interval path.

**Tech Stack:** Rust, Chronoxide segment PromQL evaluator, typed OTLP
Histogram/ExponentialHistogram storage, Prometheus 3.13.0 `promtool test rules`.

---

## Owned Files

- `chronoxide-core/src/storage/segment/query_promql.rs`
- `chronoxide-core/tests/promql_query.rs`
- `chronoxide-core/tests/prometheus_golden.rs`
- `chronoxide-core/tests/promql_range_prechange_oracle.rs`
- `chronoxide-core/tests/promql_range_scalar_cache_oracle.rs`
- `chronoxide-ingester/src/bin/chronoxide-query.rs`
- `chronoxide-ingester/src/bin/chronoxide_query/tests.rs`
- `docs/superpowers/specs/storage.md`
- `docs/promql-coverage.md`
- `docs/superpowers/plans/2026-07-10-prometheus-rate-staleness.md`

No on-disk format, public query limits/stats, smoke configuration, runtime
artifact, or unrelated benchmark design is in scope.

## Completed Implementation

- [x] Preserve the approved exact-stale omission rule for scalar and native
  cumulative `rate()` / `increase()` while keeping the original logical range.
- [x] Fix decreasing cross-stale delta fragments for Histogram and
  ExponentialHistogram. The first generated post-stale cumulative sample uses
  `CounterResetHint::Unknown`; stale itself is not a reset. Focused coverage
  also proves that an equal post-stale delta fragment is not overcounted as a
  reset, while explicit cumulative reset hints remain authoritative.
- [x] Cover direct native count/sum and virtual `_count`, `_sum`, and `_bucket`
  `rate()` / `increase()` for both typed delta families in default CI,
  explicitly separating multi-sample native count/bucket counter extrapolation
  from virtual interval aggregation while requiring signed sum parity.
- [x] Include timestamp zero and retain negative logical left-boundary duration
  when a range begins before epoch zero, for scalar, virtual projection, and
  direct native counter paths.
- [x] Revert the unrelated timestamp mutation in
  `promql_query_range_projects_histogram_series` and add explicit pre-epoch
  rate/increase coverage instead.
- [x] Restore direct native Histogram and ExponentialHistogram evaluation for
  exactly one valid selected OTLP delta interval. Multi-sample native delta
  ranges continue through cumulative-shaped Prometheus counter math.
- [x] Distinguish exact stale NaN from ordinary `NaN`, `+Inf`, and `-Inf`.
  Scalar float paths, including virtual typed `_sum` projections, use
  Prometheus endpoint/reset arithmetic; direct native cumulative sums use the
  same shape while count/bucket components remain reset-aware.
- [x] Preserve signed finite-negative and ordinary non-finite optional sums for
  delta Histogram and ExponentialHistogram single-interval and multi-sample
  evaluation. Sum IEEE arithmetic is isolated from finite count/bucket/layout
  validation, so native and virtual count/bucket results remain available.
- [x] Remove unconditional no-stale allocation. Scalar samples/reset hints
  borrow their ordinary slices, native cumulative scans use filtered
  references, and delta converters do not clone stale payloads into output.
- [x] Add structural pointer-identity coverage proving the scalar no-stale path
  borrows its input slices.
- [x] Add genuine `promtool` golden cases for scalar stale and pre-epoch
  rate/increase, ordinary non-finite sample presence, and delta Histogram /
  ExponentialHistogram virtual `_count`, `_sum`, and `_bucket` stale
  rate/increase.
- [x] Update storage and coverage documentation for the demonstrated behavior.

## Observed RED/GREEN Evidence

All production changes below followed an observed focused failure.

1. Delta Histogram stale fragment

   - RED: `cargo test -p chronoxide-core --test promql_query promql_query_delta_histogram_rate_and_increase_bridge_decreasing_stale_fragment -- --exact --nocapture`
   - Outcome: exit 101; direct `histogram_count(rate(...))` returned zero
     results at a retained `20 -> 5` cross-stale decrease.
   - GREEN: the same command exited 0; 1 passed.

2. Delta ExponentialHistogram stale fragment

   - RED: `cargo test -p chronoxide-core --test promql_query promql_query_delta_exponential_histogram_rate_and_increase_bridge_decreasing_stale_fragment -- --exact --nocapture`
   - Outcome: exit 101; direct `histogram_count(rate(...))` returned zero
     results at the same decreasing fragment boundary.
   - GREEN: the same command exited 0; 1 passed.

3. Pre-epoch scalar and native/projection parity

   - RED scalar: `cargo test -p chronoxide-core --test promql_query promql_query_rate_and_increase_include_epoch_zero_for_pre_epoch_range -- --exact --nocapture`
     exited 101 because `increase(...[3s])` at 1s returned no result.
   - RED Histogram parity: `cargo test -p chronoxide-core --test promql_query promql_query_pre_epoch_native_histogram_rate_and_increase_match_virtual_projections -- --exact --nocapture`
     exited 101 because the virtual `_count` result was missing while direct
     native evaluation was present.
   - RED ExponentialHistogram parity: `cargo test -p chronoxide-core --test promql_query promql_query_pre_epoch_native_exponential_histogram_matches_virtual_projections -- --exact --nocapture`
     exited 101 for the same virtual `_count` gap.
   - GREEN: all three exact commands exited 0; 1 passed each.
   - Independent oracle: Prometheus 3.13.0 returned increase `7.5` and rate
     `2.5` for values `5 10` at 0s/1s over `[3s]` evaluated at 1s.

4. Native single delta interval

   - RED Histogram: `cargo test -p chronoxide-core --test promql_query promql_query_native_delta_histogram_rate_uses_single_interval -- --exact --nocapture`
     exited 101; native output was empty while the virtual projection existed.
   - RED ExponentialHistogram: `cargo test -p chronoxide-core --test promql_query promql_query_native_delta_exponential_histogram_rate_uses_single_interval -- --exact --nocapture`
     exited 101; native output length was zero instead of one.
   - GREEN: both exact commands exited 0; 1 passed each.

5. Ordinary non-finite values

   - RED scalar: `cargo test -p chronoxide-core --test promql_query promql_query_rate_and_increase_distinguish_stale_from_ordinary_non_finite_values -- --exact --nocapture`
     exited 101 because ordinary NaN produced no result.
   - RED native Histogram: `cargo test -p chronoxide-core --test promql_query promql_query_native_histogram_rate_and_increase_preserve_ordinary_non_finite_sums -- --exact --nocapture`
     exited 101 because an interior non-finite sum produced no result.
   - RED native ExponentialHistogram: `cargo test -p chronoxide-core --test promql_query promql_query_native_exponential_histogram_preserves_ordinary_non_finite_sums -- --exact --nocapture`
     exited 101 for the same reason.
   - Extending the native tests to require virtual `_sum` parity produced a
     second observed RED: the Histogram virtual projection returned zero
     results for the interior non-finite case while direct native evaluation
     was present. Endpoint/reset arithmetic was then shared by hinted scalar
     projections.
   - GREEN: all three exact commands exited 0; 1 passed each.
   - Independent scalar oracle established finite results for interior ordinary
     NaN / negative infinity, positive infinity propagation, ordinary NaN at
     an endpoint, and exact-stale omission. Native Prometheus oracle established
     endpoint/reset sum behavior for interior and endpoint non-finite sums.

6. No-stale allocation

   - RED: `cargo test -p chronoxide-core --lib storage::segment::query_promql::tests::rate_increase_scalar_samples_borrow_no_stale_input -- --exact --nocapture`
   - Outcome: exit 101; copied and input slice pointers differed.
   - GREEN: the same command exited 0; 1 passed with pointer identity for both
     samples and reset hints.

7. Golden integration

   - The first new virtual-delta golden fixture exposed a deliberate OTLP
     interval-versus-Prometheus extrapolation difference (`84` vs `86.1`), so
     the fixture was narrowed with a zero seed to test stale omission itself.
   - A subsequent ExponentialHistogram fixture used the wrong classic boundary
     sequence (`96` vs `112`) and was corrected to the actual `le="2"`
     projection.
   - Restoring the old all-interval native delta fast path then failed an
     existing range oracle (`3.025` vs `2.983333...` at 30s). The implementation
     was narrowed to the required single-interval case; multi-sample native
     delta evaluation remains cumulative-shaped.
   - GREEN: `CHRONOXIDE_PROMTOOL=/opt/homebrew/bin/promtool cargo test -p chronoxide-core --test prometheus_golden prometheus_golden_suite_matches_current_promql_surface -- --ignored --exact --nocapture`
     exited 0; 1 passed, 0 failed, 2 filtered out in 49.26s.
   - Final post-review rerun: the same command exited 0; 1 passed, 0 failed,
     2 filtered out in 42.61s.
   - Delta non-finite follow-up rerun: the same command exited 0; 1 passed,
     0 failed, 2 filtered out in 41.45s.
   - Final predecessor/oracle rerun: the same command exited 0; 1 passed,
     0 failed, 2 filtered out in 41.92s.

8. Reset-hint preservation across stale omission

   - RED cumulative: strengthening
     `promql_query_increase_uses_histogram_reset_hints_after_stale_marker` with
     an explicit reset from `10` to `20` produced `18.672890963654552` instead
     of `32.0106702234078` because filtering erased `CounterReset`.
   - RED delta: `promql_query_delta_histogram_equal_cross_stale_fragment_is_not_a_reset`
     produced virtual rate `0.641025641025641` instead of
     `0.5128205128205128` because the delta projection's synthetic fragment
     reset was preserved at an equal boundary.
   - GREEN: cumulative/unknown temporality now preserves stored hints; only the
     delta fallback normalizes the first post-stale fragment hint to `Unknown`.
     Both exact tests exited 0, as did the decreasing Histogram and
     ExponentialHistogram fragment tests.

9. Delta ordinary non-finite optional sums

   - RED single interval, Histogram and ExponentialHistogram: direct native
     sum/count/bucket-shaped results and virtual `_sum` were absent, while
     virtual `_count` and `_bucket` remained present. Observed result-length
     vectors were `[0, 0, 0, 1, 1, 0]` instead of `[1; 6]`.
   - RED multi-sample, both typed families: native sum/count/bucket-shaped
     results were absent while virtual `_sum`, `_count`, and `_bucket` were
     present. Observed vectors were `[0, 1, 0, 1, 1, 0]`.
   - GREEN: all four exact focused tests exited 0 after removing optional-sum
     finiteness from native shape validation and permitting ordinary
     non-finite IEEE results in the scalar delta interval path. Finite
     count/bucket/reset validation remains.
   - Oracle: Prometheus 3.13.0 confirms NaN result presence and `+Inf`/`-Inf`
     propagation for the equivalent two-point cumulative projection. A single
     complete OTLP delta interval has no two-point Prometheus representation;
   its sum result follows the already-supported direct-interval policy and
   IEEE arithmetic.

10. Signed sums and interval metadata

   - RED signed H/ExH single and multi cases produced missing native/virtual
     sum shapes for finite-negative intervals; the multi native shape also
     disappeared when cumulative sum logic interpreted the decrease as an
     invalid counter transition.
   - RED invalid-start H/ExH cases showed multi-sample native results surviving
     missing starts with count `5`; single-sample invalid starts were already
     absent.
   - GREEN: signed/non-finite single+multi tests pass for native/virtual
     `rate`/`increase`, and missing/equal/future starts reject the full H/ExH
     result in single+multi direct and virtual paths.

11. Rare-stale typed conversion

   - RED converter tests retained a cloned stale output sample between typed
     fragments.
   - GREEN: both converters omit stale output, reset accumulators, and mark the
     equal-valued first post-gap sample `Unknown`; cumulative native evaluation
     filters wide inputs through references rather than cloning payloads.

12. Independent CLI readback oracle

   - RED: ordinary NaN/Inf emitted no range readback, a pre-epoch timestamp-zero
     pair returned no expected increase, and exact stale omission rebased the
     extrapolation window.
   - GREEN: three focused verifier tests pass with exact-bit stale filtering,
     aligned hints, ordinary IEEE arithmetic, original range bounds, and
     logical pre-epoch duration.

13. Discriminating native coverage

   - Cumulative H/ExH first/interior/final ordinary non-finite cases now assert
     direct/virtual sum outcomes and count/bucket survival.
   - Equal-then-increasing post-stale ExponentialHistogram coverage exercises
     `Unknown` fragment handling, and explicit cumulative `CounterReset` after
     stale is covered through direct native count/sum for both typed families.

14. Pre-range delta projection seed

   - Independent review found that slicing away the cumulative predecessor
     made the first selected virtual interval look like a raw delta.
   - RED: signed H and ExH raw sums `-1,-2,-3` at 10s/20s/30s returned `-6`
     for virtual `increase(_sum[15s])`, while direct native returned the
     selected `-5`.
   - GREEN: both focused tests pass after retaining one aligned predecessor
     solely for subtraction; it is not selected, validated, or aggregated.
     Positive epoch-zero golden seeds were moved to timestamp 1ms, while
     timestamp-zero stale gaps remain exempt.
   - Broad-suite RED: the tracked start/reset range oracle still expected the
     old overcounted increases `5,12,16`. The interval fixture is `2,3,7,4`,
     so left-open 20-second windows select `5,10,11` and rates
     `0.25,0.5,0.55`. Corrected exact rows and semantic fingerprints pass in
     both the direct oracle and cached/uncached scalar-cache semantic matrix.

## Consolidated Quality Remediation

The quality-review acceptance criteria define the approved follow-up design.
No commit or new design document is created during this working-tree review.

**Interval policy:** Every selected non-stale OTLP delta Histogram or
ExponentialHistogram datapoint must carry `start_time_ms < timestamp_ms`.
Missing, zero-width, or reversed intervals invalidate the complete
`rate()`/`increase()` result for both virtual and direct native paths. Stale
no-recorded-value datapoints are gaps rather than intervals and remain exempt.

**Component policy:** Delta count and bucket components remain non-negative,
reset-aware Prometheus counters. Optional delta sums are signed additive IEEE
values. Single- and multi-interval native sums and virtual `_sum` projections
therefore aggregate interval sums directly, while an optional signed or
non-finite sum never invalidates an otherwise valid count/bucket shape.

**Allocation policy:** A stale rare path may allocate a vector of references,
but it must not clone every retained Histogram/ExponentialHistogram bucket
payload. Delta converters omit stale output samples, reset their accumulators,
and retain the first post-stale `Unknown` reset hint.

- [x] Add observed RED and GREEN coverage for finite-negative delta Histogram
  and ExponentialHistogram sums across single/multi, native/virtual, and
  `rate`/`increase` paths.
- [x] Add observed RED and GREEN H/ExH multi-sample regressions for missing,
  zero-width, and reversed starts; update every positive multi-sample fixture
  to use valid interval starts.
- [x] Replace wide-sample clone filtering with borrowed scans, add converter
  RED/GREEN coverage, and remove stale output cloning from both delta
  converters.
- [x] Update the independent `chronoxide-query` readback oracle and add focused
  verifier tests for exact stale omission, ordinary NaN/Inf, unchanged logical
  range extrapolation, and pre-epoch timestamp-zero inclusion.
- [x] Split cumulative native non-finite cases so first/interior values are
  outcome-discriminating and prove count/bucket survival.
- [x] Add equal/increasing post-stale ExponentialHistogram coverage and direct
  native cumulative reset-after-stale coverage for both typed families.
- [x] Reconcile storage and PromQL coverage documentation, including removal
  of the obsolete finite-negative interval rejection claim.
- [x] Run all required focused, core, CLI, promtool, formatting, diff, and
  workspace/index checks; obtain a fresh independent review.

## Final Verification

- [x] `cargo fmt --all -- --check`
- [x] `cargo test -p chronoxide-core --test promql_query -- --nocapture`
  (`265 passed`)
- [x] `cargo test -p chronoxide-core --test prometheus_golden -- --nocapture`
  (`3 ignored` as designed)
- [x] ignored exact `promtool` golden suite (`1 passed` with Prometheus 3.13.0)
- [x] relevant `chronoxide-query` verifier tests (`60 passed` for the full bin)
- [x] `cargo test -p chronoxide-core` (all library, integration, and doc targets
  passed; `394` library tests and `265` PromQL integration tests)
- [x] `git diff --check`
- [x] status/diff audit: only owned files changed by this task; nothing
  staged; pre-existing smoke/runtime artifacts untouched
- [x] independent code review and resolution of all important findings
