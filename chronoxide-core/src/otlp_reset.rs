use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::io;

use crate::labels::SeriesRef;
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OtlpAggregationTemporality, downscale_exponential_histogram_buckets_to_map,
};

/// Stateful OTLP histogram reset detection shared by live ingestion and WAL replay.
///
/// State is keyed by the canonical series identity. Stale cumulative samples do
/// not replace the last observed non-stale state.
#[derive(Debug, Default)]
pub struct OtlpResetTracker {
    histogram: HashMap<SeriesRef, HistogramResetState>,
    exponential_histogram: HashMap<SeriesRef, ExponentialHistogramResetState>,
}

/// A histogram reset-tracker update whose hint has been computed but whose
/// semantic state is not visible to later samples yet.
///
/// The caller must commit this only after the corresponding sample has been
/// stored successfully. Dropping it leaves the tracker unchanged.
#[derive(Debug)]
pub struct PreparedHistogramReset {
    series: SeriesRef,
    next: Option<HistogramResetState>,
}

/// An exponential-histogram reset-tracker update whose hint has been computed
/// but whose semantic state is not visible to later samples yet.
///
/// The caller must commit this only after the corresponding sample has been
/// stored successfully. Dropping it leaves the tracker unchanged.
#[derive(Debug)]
pub struct PreparedExponentialHistogramReset {
    series: SeriesRef,
    next: Option<ExponentialHistogramResetState>,
}

impl OtlpResetTracker {
    pub fn stamp_histogram(&mut self, series: SeriesRef, value: &mut HistogramValue) {
        value.metadata.reset_hint = match value.metadata.temporality {
            OtlpAggregationTemporality::Cumulative => {
                if value.metadata.is_stale() {
                    CounterResetHint::Unknown
                } else {
                    let current = HistogramResetState::from_value(value);
                    let hint = self
                        .histogram
                        .get(&series)
                        .map(|previous| histogram_reset_hint(previous, &current))
                        .unwrap_or(CounterResetHint::Unknown);
                    self.histogram.insert(series, current);
                    hint
                }
            }
            OtlpAggregationTemporality::Delta => CounterResetHint::NotCounterReset,
            OtlpAggregationTemporality::Unspecified => CounterResetHint::Unknown,
        };
    }

    pub fn stamp_exponential_histogram(
        &mut self,
        series: SeriesRef,
        value: &mut ExponentialHistogramValue,
    ) {
        value.metadata.reset_hint = match value.metadata.temporality {
            OtlpAggregationTemporality::Cumulative => {
                if value.metadata.is_stale() {
                    CounterResetHint::Unknown
                } else {
                    let current = ExponentialHistogramResetState::from_value(value);
                    let hint = self
                        .exponential_histogram
                        .get(&series)
                        .map(|previous| exponential_histogram_reset_hint(previous, &current))
                        .unwrap_or(CounterResetHint::Unknown);
                    self.exponential_histogram.insert(series, current);
                    hint
                }
            }
            OtlpAggregationTemporality::Delta => CounterResetHint::NotCounterReset,
            OtlpAggregationTemporality::Unspecified => CounterResetHint::Unknown,
        };
    }

    /// Computes the stored reset hint and reserves any new map entry without
    /// changing the tracker's semantic history.
    ///
    /// All fallible work precedes the returned token. Committing the token is
    /// allocation-free as long as callers preserve the tracker's single-writer
    /// prepare-then-commit ordering.
    pub fn prepare_histogram(
        &mut self,
        series: SeriesRef,
        value: &mut HistogramValue,
    ) -> io::Result<PreparedHistogramReset> {
        let (hint, next) = match value.metadata.temporality {
            OtlpAggregationTemporality::Cumulative if !value.metadata.is_stale() => {
                let current = HistogramResetState::try_from_value(value)?;
                let hint = self
                    .histogram
                    .get(&series)
                    .map(|previous| histogram_reset_hint(previous, &current))
                    .unwrap_or(CounterResetHint::Unknown);
                if !self.histogram.contains_key(&series) {
                    try_reserve_tracker_entry(&mut self.histogram, "histogram")?;
                }
                (hint, Some(current))
            }
            OtlpAggregationTemporality::Cumulative => (CounterResetHint::Unknown, None),
            OtlpAggregationTemporality::Delta => (CounterResetHint::NotCounterReset, None),
            OtlpAggregationTemporality::Unspecified => (CounterResetHint::Unknown, None),
        };
        value.metadata.reset_hint = hint;
        Ok(PreparedHistogramReset { series, next })
    }

    /// Commits a prepared histogram state after its sample has been stored.
    pub fn commit_histogram(&mut self, prepared: PreparedHistogramReset) {
        if let Some(next) = prepared.next {
            self.histogram.insert(prepared.series, next);
        }
    }

    /// Computes the stored reset hint and reserves any new map entry without
    /// changing the tracker's semantic history.
    ///
    /// All fallible work precedes the returned token. Committing the token is
    /// allocation-free as long as callers preserve the tracker's single-writer
    /// prepare-then-commit ordering.
    pub fn prepare_exponential_histogram(
        &mut self,
        series: SeriesRef,
        value: &mut ExponentialHistogramValue,
    ) -> io::Result<PreparedExponentialHistogramReset> {
        let (hint, next) = match value.metadata.temporality {
            OtlpAggregationTemporality::Cumulative if !value.metadata.is_stale() => {
                let current = ExponentialHistogramResetState::try_from_value(value)?;
                let hint = self
                    .exponential_histogram
                    .get(&series)
                    .map(|previous| exponential_histogram_reset_hint(previous, &current))
                    .unwrap_or(CounterResetHint::Unknown);
                if !self.exponential_histogram.contains_key(&series) {
                    try_reserve_tracker_entry(
                        &mut self.exponential_histogram,
                        "exponential histogram",
                    )?;
                }
                (hint, Some(current))
            }
            OtlpAggregationTemporality::Cumulative => (CounterResetHint::Unknown, None),
            OtlpAggregationTemporality::Delta => (CounterResetHint::NotCounterReset, None),
            OtlpAggregationTemporality::Unspecified => (CounterResetHint::Unknown, None),
        };
        value.metadata.reset_hint = hint;
        Ok(PreparedExponentialHistogramReset { series, next })
    }

    /// Commits a prepared exponential-histogram state after its sample has
    /// been stored.
    pub fn commit_exponential_histogram(&mut self, prepared: PreparedExponentialHistogramReset) {
        if let Some(next) = prepared.next {
            self.exponential_histogram.insert(prepared.series, next);
        }
    }
}

#[derive(Debug, Clone)]
struct HistogramResetState {
    start_time_ms: Option<u64>,
    count: u64,
    sum: Option<f64>,
    explicit_bounds: Vec<f64>,
    bucket_counts: Vec<u64>,
}

impl HistogramResetState {
    fn from_value(value: &HistogramValue) -> Self {
        Self {
            start_time_ms: value.metadata.start_time_ms,
            count: value.count,
            sum: value.sum,
            explicit_bounds: value.explicit_bounds.clone(),
            bucket_counts: value.bucket_counts.clone(),
        }
    }

    fn try_from_value(value: &HistogramValue) -> io::Result<Self> {
        Ok(Self {
            start_time_ms: value.metadata.start_time_ms,
            count: value.count,
            sum: value.sum,
            explicit_bounds: try_clone_slice(
                &value.explicit_bounds,
                "histogram reset explicit bounds",
            )?,
            bucket_counts: try_clone_slice(&value.bucket_counts, "histogram reset bucket counts")?,
        })
    }
}

#[derive(Debug, Clone)]
struct ExponentialHistogramResetState {
    start_time_ms: Option<u64>,
    count: u64,
    sum: Option<f64>,
    scale: i32,
    zero_threshold_bits: u64,
    zero_count: u64,
    positive: ExponentialHistogramBuckets,
    negative: ExponentialHistogramBuckets,
}

impl ExponentialHistogramResetState {
    fn from_value(value: &ExponentialHistogramValue) -> Self {
        Self {
            start_time_ms: value.metadata.start_time_ms,
            count: value.count,
            sum: value.sum,
            scale: value.scale,
            zero_threshold_bits: value.zero_threshold.to_bits(),
            zero_count: value.zero_count,
            positive: value.positive.clone(),
            negative: value.negative.clone(),
        }
    }

    fn try_from_value(value: &ExponentialHistogramValue) -> io::Result<Self> {
        Ok(Self {
            start_time_ms: value.metadata.start_time_ms,
            count: value.count,
            sum: value.sum,
            scale: value.scale,
            zero_threshold_bits: value.zero_threshold.to_bits(),
            zero_count: value.zero_count,
            positive: ExponentialHistogramBuckets {
                offset: value.positive.offset,
                counts: try_clone_slice(
                    &value.positive.counts,
                    "positive exponential histogram reset buckets",
                )?,
            },
            negative: ExponentialHistogramBuckets {
                offset: value.negative.offset,
                counts: try_clone_slice(
                    &value.negative.counts,
                    "negative exponential histogram reset buckets",
                )?,
            },
        })
    }
}

fn try_reserve_tracker_entry<K: Eq + Hash, V>(
    map: &mut HashMap<K, V>,
    kind: &'static str,
) -> io::Result<()> {
    map.try_reserve(1).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("failed to reserve {kind} reset-tracker state: {error}"),
        )
    })
}

fn try_clone_slice<T: Copy>(source: &[T], description: &'static str) -> io::Result<Vec<T>> {
    let mut cloned = Vec::new();
    cloned.try_reserve_exact(source.len()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("failed to reserve {description}: {error}"),
        )
    })?;
    cloned.extend_from_slice(source);
    Ok(cloned)
}

fn histogram_reset_hint(
    previous: &HistogramResetState,
    current: &HistogramResetState,
) -> CounterResetHint {
    if start_time_advanced(previous.start_time_ms, current.start_time_ms) {
        return CounterResetHint::CounterReset;
    }
    if previous.explicit_bounds != current.explicit_bounds {
        return CounterResetHint::Unknown;
    }
    if current.count < previous.count || optional_f64_decreased(previous.sum, current.sum) {
        return CounterResetHint::CounterReset;
    }
    if previous.bucket_counts.len() != current.bucket_counts.len() {
        return CounterResetHint::Unknown;
    }
    if previous
        .bucket_counts
        .iter()
        .zip(&current.bucket_counts)
        .any(|(previous, current)| current < previous)
    {
        return CounterResetHint::CounterReset;
    }
    CounterResetHint::NotCounterReset
}

fn exponential_histogram_reset_hint(
    previous: &ExponentialHistogramResetState,
    current: &ExponentialHistogramResetState,
) -> CounterResetHint {
    if start_time_advanced(previous.start_time_ms, current.start_time_ms) {
        return CounterResetHint::CounterReset;
    }
    if previous.zero_threshold_bits != current.zero_threshold_bits {
        return CounterResetHint::Unknown;
    }
    if current.count < previous.count
        || current.zero_count < previous.zero_count
        || optional_f64_decreased(previous.sum, current.sum)
    {
        return CounterResetHint::CounterReset;
    }

    let target_scale = previous.scale.min(current.scale);
    let Ok(previous_positive) = downscale_exponential_histogram_buckets_to_map(
        &previous.positive,
        previous.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };
    let Ok(current_positive) = downscale_exponential_histogram_buckets_to_map(
        &current.positive,
        current.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };
    let Ok(previous_negative) = downscale_exponential_histogram_buckets_to_map(
        &previous.negative,
        previous.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };
    let Ok(current_negative) = downscale_exponential_histogram_buckets_to_map(
        &current.negative,
        current.scale,
        target_scale,
    ) else {
        return CounterResetHint::Unknown;
    };

    if bucket_map_decreased(&previous_positive, &current_positive)
        || bucket_map_decreased(&previous_negative, &current_negative)
    {
        CounterResetHint::CounterReset
    } else {
        CounterResetHint::NotCounterReset
    }
}

fn start_time_advanced(previous: Option<u64>, current: Option<u64>) -> bool {
    matches!((previous, current), (Some(previous), Some(current)) if current > previous)
}

fn optional_f64_decreased(previous: Option<f64>, current: Option<f64>) -> bool {
    matches!((previous, current), (Some(previous), Some(current)) if current < previous)
}

fn bucket_map_decreased(previous: &BTreeMap<i32, u64>, current: &BTreeMap<i32, u64>) -> bool {
    previous
        .iter()
        .any(|(index, previous_count)| current.get(index).copied().unwrap_or(0) < *previous_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::head::TypedSampleMetadata;

    fn metadata() -> TypedSampleMetadata {
        TypedSampleMetadata {
            start_time_ms: Some(1),
            flags: 0,
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::Unknown,
        }
    }

    fn histogram(count: u64) -> HistogramValue {
        HistogramValue {
            count,
            sum: Some(count as f64),
            min: None,
            max: None,
            metadata: metadata(),
            explicit_bounds: vec![1.0],
            bucket_counts: vec![count, 0],
        }
    }

    fn exponential_histogram(count: u64) -> ExponentialHistogramValue {
        ExponentialHistogramValue {
            count,
            sum: Some(count as f64),
            min: None,
            max: None,
            scale: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            metadata: metadata(),
            positive: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![count],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: Vec::new(),
            },
        }
    }

    #[test]
    fn dropped_histogram_prepare_does_not_change_the_next_committed_hint() {
        let series = SeriesRef::new(7);
        let mut tracker = OtlpResetTracker::default();
        let mut baseline = histogram(100);
        let baseline_update = tracker.prepare_histogram(series, &mut baseline).unwrap();
        assert_eq!(baseline.metadata.reset_hint, CounterResetHint::Unknown);
        tracker.commit_histogram(baseline_update);

        let mut rejected = histogram(200);
        let rejected_update = tracker.prepare_histogram(series, &mut rejected).unwrap();
        assert_eq!(
            rejected.metadata.reset_hint,
            CounterResetHint::NotCounterReset
        );
        drop(rejected_update);

        let mut later = histogram(150);
        let later_update = tracker.prepare_histogram(series, &mut later).unwrap();
        assert_eq!(
            later.metadata.reset_hint,
            CounterResetHint::NotCounterReset,
            "the rejected count=200 sample must not make the stored count=150 sample look reset"
        );
        tracker.commit_histogram(later_update);
    }

    #[test]
    fn dropped_exponential_prepare_does_not_change_the_next_committed_hint() {
        let series = SeriesRef::new(9);
        let mut tracker = OtlpResetTracker::default();
        let mut baseline = exponential_histogram(100);
        let baseline_update = tracker
            .prepare_exponential_histogram(series, &mut baseline)
            .unwrap();
        assert_eq!(baseline.metadata.reset_hint, CounterResetHint::Unknown);
        tracker.commit_exponential_histogram(baseline_update);

        let mut rejected = exponential_histogram(200);
        let rejected_update = tracker
            .prepare_exponential_histogram(series, &mut rejected)
            .unwrap();
        assert_eq!(
            rejected.metadata.reset_hint,
            CounterResetHint::NotCounterReset
        );
        drop(rejected_update);

        let mut later = exponential_histogram(150);
        let later_update = tracker
            .prepare_exponential_histogram(series, &mut later)
            .unwrap();
        assert_eq!(
            later.metadata.reset_hint,
            CounterResetHint::NotCounterReset,
            "the rejected count=200 sample must not make the stored count=150 sample look reset"
        );
        tracker.commit_exponential_histogram(later_update);
    }

    #[test]
    fn prepared_commit_matches_immediate_stamping_for_both_native_kinds() {
        let series = SeriesRef::new(11);
        let mut immediate = OtlpResetTracker::default();
        let mut transactional = OtlpResetTracker::default();

        for count in [100, 150, 75, 80] {
            let mut immediate_histogram = histogram(count);
            immediate.stamp_histogram(series, &mut immediate_histogram);
            let mut transactional_histogram = histogram(count);
            let prepared = transactional
                .prepare_histogram(series, &mut transactional_histogram)
                .unwrap();
            transactional.commit_histogram(prepared);
            assert_eq!(
                transactional_histogram.metadata.reset_hint,
                immediate_histogram.metadata.reset_hint
            );

            let mut immediate_exponential = exponential_histogram(count);
            immediate.stamp_exponential_histogram(series, &mut immediate_exponential);
            let mut transactional_exponential = exponential_histogram(count);
            let prepared = transactional
                .prepare_exponential_histogram(series, &mut transactional_exponential)
                .unwrap();
            transactional.commit_exponential_histogram(prepared);
            assert_eq!(
                transactional_exponential.metadata.reset_hint,
                immediate_exponential.metadata.reset_hint
            );
        }
    }
}
