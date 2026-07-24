# Prometheus Rate/Increase Staleness Semantics

> **Archived historical record:** This document is retained for provenance and is not current authority. Consult the current contracts and code before relying on it.

## Goal

Make Chronoxide's current scalar, projected OTLP, and native Histogram and
ExponentialHistogram `rate()` / `increase()` evaluation agree with Prometheus
when a selected range contains the exact Prometheus stale-NaN marker.

## Scope

This change applies only to `rate()` and `increase()` paths:

- scalar cumulative counters and scalar OTLP delta projections;
- virtual `_count`, `_sum`, and `_bucket` projections from OTLP Histogram and
  ExponentialHistogram data; and
- direct native Histogram and ExponentialHistogram `rate()` / `increase()`.

It does not change the current semantics of `irate`, `delta`, `idelta`,
`changes`, `resets`, or other range functions. It does not change storage or
on-disk typed chunk encoding.

## Reference Behavior

Prometheus removes exact stale markers while constructing range-selector
matrices. `extrapolatedRate` then receives only the remaining float or native
histogram samples and extrapolates against the original query range. A stale
marker is therefore neither a range boundary nor a counter reset by itself.

The local vendored Prometheus reference implements this in
`promql/engine.go` by skipping `value.IsStaleNaN` while building the range
matrix, and in `promql/functions.go` by applying `extrapolatedRate` to the
resulting retained samples.

For example, for an evaluation range of 40 seconds, the selected values
`5 stale 5 15 25` are evaluated from the retained sequence `5 5 15 25` over
the original 40-second range. Chronoxide must not retain only `5 15 25` and
must not move the extrapolation start to the stale timestamp.

## Design

### Scalar counters and projections

Introduce a rate/increase-specific aligned filtering step that removes only
the exact Prometheus stale marker. It keeps timestamps, counter-reset hints,
and OTLP start-time metadata aligned for retained samples. Ordinary IEEE
`NaN`, `+Inf`, and `-Inf` do not become stale markers through this change.

Apply the filter before cumulative counter extrapolation and before delta
projection interval evaluation. Retained samples always use the original
left-open/right-closed range bounds for extrapolation.

### OTLP delta fragments

Delta Histograms and ExponentialHistograms need a distinct internal rule:
their conversion to a cumulative-shaped evaluation sequence resets its
accumulator after a stale datapoint so that post-stale deltas begin a new
projection fragment. The conversion preserves the stale marker until the
rate/increase filter removes it. This produces Prometheus-shaped values such
as `5 stale 5 15 25` without allowing the stale marker to cut the evaluation
range in two.

### Native histograms

Native cumulative Histogram and ExponentialHistogram counter increase
calculation iterates over non-stale selected samples, retaining its original
range bounds for the shared extrapolation calculation. Native delta samples
first use the fragment-preserving cumulative conversion above and then take
the same cumulative calculation. No direct delta interval fast path is
reintroduced for native Histogram or ExponentialHistogram evaluation.

### Documentation

Update the storage specification and PromQL coverage document to replace the
incorrect statement that stale/non-finite samples form rate/increase
extrapolation boundaries. The specification will distinguish exact stale
markers from ordinary non-finite IEEE values and describe the retained-sample
evaluation rule.

## Tests

Tests will be written before production edits and run red first. They will
cover:

- scalar `rate` and `increase` across an interior stale marker;
- native Histogram and ExponentialHistogram rate/increase across an interior
  stale marker, including original-range extrapolation;
- delta Histogram and ExponentialHistogram virtual projection and direct
  native-function cases that preserve pre-stale contribution and restart the
  delta projection accumulator; and
- the Prometheus/promtool golden instant and range cases already added in the
  working tree.

Focused query tests, the Prometheus golden suite with a supplied `promtool`,
formatting, and `git diff --check` will verify the result.

## Non-goals

- Changing persisted stale markers or typed payload layouts.
- Extending functionality beyond the current PromQL surface.
- Defining new semantics for `irate`, `delta`, `idelta`, `changes`, or
  `resets` in this change.
