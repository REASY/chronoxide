use super::*;

use std::sync::Arc;

use chronoxide_core::storage::head::{
    FrozenHeadFragment, FrozenHeadLane, SampleKind, SummaryQuantileValue,
};
use chronoxide_core::storage::segment::{SegmentFlushOutcome, SegmentId, SegmentWriterConfig};

use super::pipeline::record_series_samples;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LogicalSealKind {
    Scalar,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl LogicalSealKind {
    const fn from_sample_kind(kind: SampleKind) -> Self {
        match kind {
            SampleKind::Float | SampleKind::Int64 => Self::Scalar,
            SampleKind::Histogram => Self::Histogram,
            SampleKind::ExponentialHistogram => Self::ExponentialHistogram,
            SampleKind::Summary => Self::Summary,
        }
    }

    const fn ordering_sample_kind(self) -> SampleKind {
        match self {
            Self::Scalar => SampleKind::Float,
            Self::Histogram => SampleKind::Histogram,
            Self::ExponentialHistogram => SampleKind::ExponentialHistogram,
            Self::Summary => SampleKind::Summary,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiveSealScratchLimits {
    max_vector_bytes: usize,
}

impl LiveSealScratchLimits {
    const UNBOUNDED: Self = Self {
        max_vector_bytes: usize::MAX,
    };

    #[cfg(test)]
    const fn with_max_vector_bytes(max_vector_bytes: usize) -> Self {
        Self { max_vector_bytes }
    }

    fn checked_target_len<T>(
        self,
        current: usize,
        additional: usize,
        context: &'static str,
    ) -> io::Result<usize> {
        let target = current.checked_add(additional).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{context} sample count overflows usize"),
            )
        })?;
        let bytes = checked_vector_bytes::<T>(target, context)?;
        if bytes > self.max_vector_bytes {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "{context} requires {bytes} vector bytes, exceeding the live-seal scratch limit of {} bytes",
                    self.max_vector_bytes
                ),
            ));
        }
        Ok(target)
    }

    fn validate_samples(self, samples: &SeriesSamples) -> io::Result<()> {
        if self == Self::UNBOUNDED {
            return Ok(());
        }
        match samples {
            SeriesSamples::Float { samples, .. } => {
                self.checked_target_len::<(u64, f64)>(0, samples.len(), "live-seal float scratch")?;
            }
            SeriesSamples::Int64 { samples, .. } => {
                self.checked_target_len::<(u64, i64)>(0, samples.len(), "live-seal int scratch")?;
            }
            SeriesSamples::Histogram { samples } => {
                self.checked_target_len::<(u64, HistogramValue)>(
                    0,
                    samples.len(),
                    "live-seal histogram scratch",
                )?;
                for (_, value) in samples {
                    self.checked_target_len::<f64>(
                        0,
                        value.explicit_bounds.len(),
                        "live-seal histogram bounds scratch",
                    )?;
                    self.checked_target_len::<u64>(
                        0,
                        value.bucket_counts.len(),
                        "live-seal histogram buckets scratch",
                    )?;
                }
            }
            SeriesSamples::ExponentialHistogram { samples } => {
                self.checked_target_len::<(u64, ExponentialHistogramValue)>(
                    0,
                    samples.len(),
                    "live-seal exponential-histogram scratch",
                )?;
                for (_, value) in samples {
                    self.checked_target_len::<u64>(
                        0,
                        value.positive.counts.len(),
                        "live-seal positive exponential buckets scratch",
                    )?;
                    self.checked_target_len::<u64>(
                        0,
                        value.negative.counts.len(),
                        "live-seal negative exponential buckets scratch",
                    )?;
                }
            }
            SeriesSamples::Summary { samples } => {
                self.checked_target_len::<(u64, SummaryValue)>(
                    0,
                    samples.len(),
                    "live-seal summary scratch",
                )?;
                for (_, value) in samples {
                    self.checked_target_len::<SummaryQuantileValue>(
                        0,
                        value.quantiles.len(),
                        "live-seal summary quantiles scratch",
                    )?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LiveSealScratchProfile {
    peak_owned_sample_slots: usize,
    peak_owned_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SeriesScratchFootprint {
    owned_sample_slots: usize,
    owned_bytes: usize,
}

struct OrderedFrozenFragment {
    input_order: usize,
    fragment: Arc<FrozenHeadFragment>,
}

impl SeriesScratchFootprint {
    fn checked_add(self, other: Self) -> io::Result<Self> {
        Ok(Self {
            owned_sample_slots: self
                .owned_sample_slots
                .checked_add(other.owned_sample_slots)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "live-seal scratch sample-slot accounting overflows usize",
                    )
                })?,
            owned_bytes: self
                .owned_bytes
                .checked_add(other.owned_bytes)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "live-seal scratch byte accounting overflows usize",
                    )
                })?,
        })
    }
}

impl LiveSealScratchProfile {
    /// Records the stable-point owned allocation for one logical series.
    ///
    /// This includes outer sample buffers and the decoded typed values' nested
    /// vectors. Allocator-internal transient bytes during `realloc` are not
    /// observable here, so the field is deliberately reported as owned bytes
    /// rather than process RSS.
    fn observe(&mut self, footprint: SeriesScratchFootprint) {
        self.peak_owned_sample_slots = self
            .peak_owned_sample_slots
            .max(footprint.owned_sample_slots);
        self.peak_owned_bytes = self.peak_owned_bytes.max(footprint.owned_bytes);
    }

    fn observe_with_staged_float(
        &mut self,
        earlier: SeriesScratchFootprint,
        later: SeriesScratchFootprint,
        staged_capacity: usize,
    ) -> io::Result<()> {
        let staged = SeriesScratchFootprint {
            owned_sample_slots: staged_capacity,
            owned_bytes: checked_vector_bytes::<(u64, f64)>(
                staged_capacity,
                "live-seal staged scalar scratch",
            )?,
        };
        self.observe(earlier.checked_add(later)?.checked_add(staged)?);
        Ok(())
    }
}

/// Builds one retryable segment writer by borrowing immutable frozen input.
///
/// Pass one walks only run directories and canonical ordering metadata. Pass
/// two decodes, merges, writes, and drops one logical series at a time.
pub(super) fn build_frozen_segment_writer(
    config: SegmentWriterConfig,
    segment_id: SegmentId,
    labelsets: &LabelSetInterner,
    start_ms: u64,
    end_ms: u64,
    payload_lane: SegmentPayloadLane,
    fragments: &[Arc<FrozenHeadFragment>],
) -> Result<SegmentWriter> {
    let (writer, scratch) = build_frozen_segment_writer_with_limits(
        config,
        segment_id,
        labelsets,
        start_ms,
        end_ms,
        payload_lane,
        fragments,
        LiveSealScratchLimits::UNBOUNDED,
    )?;
    info!(
        start_ms,
        end_ms,
        peak_owned_sample_slots = scratch.peak_owned_sample_slots,
        peak_owned_bytes = scratch.peak_owned_bytes,
        "Built frozen live-seal writer"
    );
    Ok(writer)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the test-only scratch limit is part of the same frozen-seal operation"
)]
fn build_frozen_segment_writer_with_limits(
    config: SegmentWriterConfig,
    segment_id: SegmentId,
    labelsets: &LabelSetInterner,
    start_ms: u64,
    end_ms: u64,
    payload_lane: SegmentPayloadLane,
    fragments: &[Arc<FrozenHeadFragment>],
    scratch_limits: LiveSealScratchLimits,
) -> Result<(SegmentWriter, LiveSealScratchProfile)> {
    if start_ms >= end_ms {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "live seal range must be non-empty",
        )
        .into());
    }

    let mut ordered_fragments = Vec::new();
    ordered_fragments
        .try_reserve_exact(fragments.len())
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to reserve live-seal fragment order: {error}"),
            )
        })?;
    ordered_fragments.extend(
        fragments
            .iter()
            .enumerate()
            .filter(|(_, fragment)| fragment.start_ms() == start_ms && fragment.end_ms() == end_ms)
            .map(|(input_order, fragment)| OrderedFrozenFragment {
                input_order,
                fragment: Arc::clone(fragment),
            }),
    );
    ordered_fragments.sort_unstable_by_key(|ordered| {
        (
            ordered.fragment.lane(),
            ordered.fragment.publication_sequence(),
            ordered.input_order,
        )
    });
    match payload_lane {
        SegmentPayloadLane::InOrder => {
            if !ordered_fragments
                .iter()
                .any(|ordered| ordered.fragment.lane() == FrozenHeadLane::InOrder)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "normal live seal requires at least one in-order fragment",
                )
                .into());
            }
        }
        SegmentPayloadLane::OutOfOrder => {
            if ordered_fragments
                .iter()
                .any(|ordered| ordered.fragment.lane() != FrozenHeadLane::OutOfOrder)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "OOO live seal cannot include an in-order fragment",
                )
                .into());
            }
        }
    }

    let run_key_count = ordered_fragments
        .iter()
        .try_fold(0usize, |count, fragment| {
            count
                .checked_add(fragment.fragment.series_keys().len())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "live-seal run-key count overflows usize",
                    )
                })
        })?;
    let mut unique = Vec::new();
    unique.try_reserve_exact(run_key_count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("failed to reserve live-seal run keys: {error}"),
        )
    })?;
    for ordered in &ordered_fragments {
        for key in ordered.fragment.series_keys() {
            unique.push((key.series, LogicalSealKind::from_sample_kind(key.kind)));
        }
    }
    unique.sort_unstable();
    unique.dedup();
    let mut series_kinds = Vec::new();
    series_kinds
        .try_reserve_exact(unique.len())
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to reserve live-seal series descriptors: {error}"),
            )
        })?;
    series_kinds.extend(
        unique
            .iter()
            .map(|(series, kind)| (*series, kind.ordering_sample_kind())),
    );
    if series_kinds.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "live seal cannot build an empty segment",
        )
        .into());
    }
    let canonical_label_counts = order_series_kinds_for_metric_query(&mut series_kinds, labelsets)?;

    let mut writer = SegmentWriter::new(config)?;
    writer.set_next_segment_id_for_retry(segment_id)?;
    writer.set_next_segment_payload_lane(payload_lane)?;
    if !series_kinds.is_empty() {
        writer.reserve_metric_query_ordered_window_series_with_label_counts(
            start_ms,
            end_ms,
            series_kinds
                .iter()
                .zip(canonical_label_counts.iter().copied())
                .map(|((series, _kind), label_count)| (*series, label_count)),
        )?;
    }

    let mut profile = HeadWindowWriteProfile {
        start_ms,
        end_ms,
        series: u64::try_from(series_kinds.len()).unwrap_or(u64::MAX),
        ..HeadWindowWriteProfile::default()
    };
    let mut scratch = LiveSealScratchProfile::default();
    for (series, ordering_kind) in series_kinds {
        let logical_kind = LogicalSealKind::from_sample_kind(ordering_kind);
        let mut merged = None;
        for ordered in &ordered_fragments {
            for physical_kind in physical_kinds(logical_kind) {
                let Some(samples) = ordered.fragment.series_kind_samples_in_range(
                    series,
                    *physical_kind,
                    start_ms,
                    end_ms,
                )?
                else {
                    continue;
                };
                scratch_limits.validate_samples(&samples)?;
                let decoded_footprint = series_samples_owned_scratch(&samples)?;
                match &mut merged {
                    Some((earlier, earlier_footprint)) => merge_live_series_samples(
                        earlier,
                        earlier_footprint,
                        samples,
                        decoded_footprint,
                        series,
                        scratch_limits,
                        &mut scratch,
                    )?,
                    None => {
                        scratch.observe(decoded_footprint);
                        merged = Some((samples, decoded_footprint));
                    }
                }
            }
        }
        let Some((mut samples, merged_footprint)) = merged else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "live seal directory advertised series {} kind {logical_kind:?} without samples",
                    series.get()
                ),
            )
            .into());
        };
        sort_and_dedupe_live_series_samples(
            &mut samples,
            merged_footprint,
            scratch_limits,
            &mut scratch,
        )?;
        let mut samples_footprint = series_samples_owned_scratch(&samples)?;
        normalize_live_scalar_for_writer(
            &mut samples,
            &mut samples_footprint,
            scratch_limits,
            &mut scratch,
        )?;
        scratch.observe(samples_footprint);
        let mut one_series = Vec::new();
        one_series.try_reserve_exact(1).map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to reserve the live-seal writer input row: {error}"),
            )
        })?;
        one_series.push((series, samples));
        record_series_samples(labelsets, &mut writer, one_series, &mut profile)?;
    }
    Ok((writer, scratch))
}

pub(super) fn finish_frozen_segment_writer(
    writer: &mut SegmentWriter,
) -> Result<Option<SegmentFlushOutcome>> {
    writer.flush_with_outcome().map_err(Into::into)
}

fn physical_kinds(kind: LogicalSealKind) -> &'static [SampleKind] {
    match kind {
        LogicalSealKind::Scalar => &[SampleKind::Float, SampleKind::Int64],
        LogicalSealKind::Histogram => &[SampleKind::Histogram],
        LogicalSealKind::ExponentialHistogram => &[SampleKind::ExponentialHistogram],
        LogicalSealKind::Summary => &[SampleKind::Summary],
    }
}

fn merge_live_series_samples(
    earlier: &mut SeriesSamples,
    earlier_footprint: &mut SeriesScratchFootprint,
    later: SeriesSamples,
    later_footprint: SeriesScratchFootprint,
    series: SeriesRef,
    scratch_limits: LiveSealScratchLimits,
    scratch: &mut LiveSealScratchProfile,
) -> Result<()> {
    scratch_limits.validate_samples(&later)?;
    scratch.observe(earlier_footprint.checked_add(later_footprint)?);
    match (&mut *earlier, later) {
        (
            SeriesSamples::Float { encoding, samples },
            SeriesSamples::Float {
                encoding: later_encoding,
                samples: later_samples,
            },
        ) if *encoding == later_encoding => {
            *earlier_footprint = checked_extend(
                samples,
                later_samples,
                *earlier_footprint,
                later_footprint,
                scratch_limits,
                scratch,
                "live-seal float merge",
            )?;
        }
        (
            SeriesSamples::Int64 { encoding, samples },
            SeriesSamples::Int64 {
                encoding: later_encoding,
                samples: later_samples,
            },
        ) if *encoding == later_encoding => {
            *earlier_footprint = checked_extend(
                samples,
                later_samples,
                *earlier_footprint,
                later_footprint,
                scratch_limits,
                scratch,
                "live-seal int merge",
            )?;
        }
        (
            earlier_variant @ SeriesSamples::Float { .. },
            later_variant @ SeriesSamples::Int64 { .. },
        ) => {
            let SeriesSamples::Float {
                encoding,
                samples: earlier_samples,
            } = &*earlier_variant
            else {
                unreachable!("the match arm established a Float series");
            };
            let SeriesSamples::Int64 {
                samples: later_samples,
                ..
            } = &later_variant
            else {
                unreachable!("the match arm established an Int64 series");
            };
            let encoding = *encoding;
            let target = scratch_limits.checked_target_len::<(u64, f64)>(
                earlier_samples.len(),
                later_samples.len(),
                "live-seal mixed scalar merge",
            )?;
            let mut staged = try_vec_for_len::<(u64, f64)>(target, "live-seal mixed scalar merge")?;
            staged.extend(earlier_samples.iter().copied());
            staged.extend(
                later_samples
                    .iter()
                    .map(|(timestamp_ms, value)| (*timestamp_ms, *value as f64)),
            );
            scratch.observe_with_staged_float(
                *earlier_footprint,
                later_footprint,
                staged.capacity(),
            )?;
            *earlier_footprint = SeriesScratchFootprint {
                owned_sample_slots: staged.capacity(),
                owned_bytes: checked_vector_bytes::<(u64, f64)>(
                    staged.capacity(),
                    "live-seal mixed scalar scratch",
                )?,
            };
            *earlier_variant = SeriesSamples::Float {
                encoding,
                samples: staged,
            };
        }
        (
            earlier_variant @ SeriesSamples::Int64 { .. },
            later_variant @ SeriesSamples::Float { .. },
        ) => {
            let SeriesSamples::Int64 {
                samples: earlier_samples,
                ..
            } = &*earlier_variant
            else {
                unreachable!("the match arm established an Int64 series");
            };
            let SeriesSamples::Float {
                encoding,
                samples: later_samples,
            } = &later_variant
            else {
                unreachable!("the match arm established a Float series");
            };
            let encoding = *encoding;
            let target = scratch_limits.checked_target_len::<(u64, f64)>(
                earlier_samples.len(),
                later_samples.len(),
                "live-seal mixed scalar merge",
            )?;
            let mut staged = try_vec_for_len::<(u64, f64)>(target, "live-seal mixed scalar merge")?;
            staged.extend(
                earlier_samples
                    .iter()
                    .map(|(timestamp_ms, value)| (*timestamp_ms, *value as f64)),
            );
            staged.extend(later_samples.iter().copied());
            scratch.observe_with_staged_float(
                *earlier_footprint,
                later_footprint,
                staged.capacity(),
            )?;
            *earlier_footprint = SeriesScratchFootprint {
                owned_sample_slots: staged.capacity(),
                owned_bytes: checked_vector_bytes::<(u64, f64)>(
                    staged.capacity(),
                    "live-seal mixed scalar scratch",
                )?,
            };
            *earlier_variant = SeriesSamples::Float {
                encoding,
                samples: staged,
            };
        }
        (
            SeriesSamples::Histogram { samples },
            SeriesSamples::Histogram {
                samples: later_samples,
            },
        ) => {
            *earlier_footprint = checked_extend(
                samples,
                later_samples,
                *earlier_footprint,
                later_footprint,
                scratch_limits,
                scratch,
                "live-seal histogram merge",
            )?;
        }
        (
            SeriesSamples::ExponentialHistogram { samples },
            SeriesSamples::ExponentialHistogram {
                samples: later_samples,
            },
        ) => {
            *earlier_footprint = checked_extend(
                samples,
                later_samples,
                *earlier_footprint,
                later_footprint,
                scratch_limits,
                scratch,
                "live-seal exponential-histogram merge",
            )?;
        }
        (
            SeriesSamples::Summary { samples },
            SeriesSamples::Summary {
                samples: later_samples,
            },
        ) => {
            *earlier_footprint = checked_extend(
                samples,
                later_samples,
                *earlier_footprint,
                later_footprint,
                scratch_limits,
                scratch,
                "live-seal summary merge",
            )?;
        }
        (
            SeriesSamples::Float { encoding, .. },
            SeriesSamples::Float {
                encoding: later_encoding,
                ..
            },
        ) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "live seal float encoding mismatch for series {}: {encoding:?} versus {later_encoding:?}",
                    series.get()
                ),
            )
            .into());
        }
        (
            SeriesSamples::Int64 { encoding, .. },
            SeriesSamples::Int64 {
                encoding: later_encoding,
                ..
            },
        ) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "live seal int encoding mismatch for series {}: {encoding:?} versus {later_encoding:?}",
                    series.get()
                ),
            )
            .into());
        }
        (_, _) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("live seal sample kind changed for series {}", series.get()),
            )
            .into());
        }
    }
    scratch.observe(*earlier_footprint);
    Ok(())
}

fn normalize_live_scalar_for_writer(
    samples: &mut SeriesSamples,
    samples_footprint: &mut SeriesScratchFootprint,
    scratch_limits: LiveSealScratchLimits,
    scratch: &mut LiveSealScratchProfile,
) -> Result<()> {
    let SeriesSamples::Int64 {
        samples: int_samples,
        ..
    } = &*samples
    else {
        return Ok(());
    };
    let target = scratch_limits.checked_target_len::<(u64, f64)>(
        0,
        int_samples.len(),
        "live-seal int writer conversion",
    )?;
    let mut staged = try_vec_for_len::<(u64, f64)>(target, "live-seal int writer conversion")?;
    staged.extend(
        int_samples
            .iter()
            .map(|(timestamp_ms, value)| (*timestamp_ms, *value as f64)),
    );
    let staged_variant = SeriesSamples::Float {
        encoding: FloatEncoding::Gorilla,
        samples: staged,
    };
    let staged_footprint = series_samples_owned_scratch(&staged_variant)?;
    scratch.observe(samples_footprint.checked_add(staged_footprint)?);
    *samples = staged_variant;
    *samples_footprint = staged_footprint;
    scratch.observe(*samples_footprint);
    Ok(())
}

fn checked_extend<T>(
    destination: &mut Vec<T>,
    source: Vec<T>,
    destination_footprint: SeriesScratchFootprint,
    source_footprint: SeriesScratchFootprint,
    scratch_limits: LiveSealScratchLimits,
    scratch: &mut LiveSealScratchProfile,
    context: &'static str,
) -> io::Result<SeriesScratchFootprint> {
    scratch_limits.checked_target_len::<T>(destination.len(), source.len(), context)?;
    let destination_outer_before = checked_vector_bytes::<T>(destination.capacity(), context)?;
    let source_outer = checked_vector_bytes::<T>(source.capacity(), context)?;
    let destination_nested = destination_footprint
        .owned_bytes
        .checked_sub(destination_outer_before)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{context} destination scratch footprint is inconsistent"),
            )
        })?;
    let source_nested = source_footprint
        .owned_bytes
        .checked_sub(source_outer)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{context} source scratch footprint is inconsistent"),
            )
        })?;
    destination
        .try_reserve_exact(source.len())
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("{context} allocation failed before merge: {error}"),
            )
        })?;
    let destination_outer_after = checked_vector_bytes::<T>(destination.capacity(), context)?;
    let merged = SeriesScratchFootprint {
        owned_sample_slots: destination.capacity(),
        owned_bytes: destination_outer_after
            .checked_add(destination_nested)
            .and_then(|bytes| bytes.checked_add(source_nested))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{context} merged scratch footprint overflows usize"),
                )
            })?,
    };
    scratch.observe(merged.checked_add(SeriesScratchFootprint {
        owned_sample_slots: source.capacity(),
        owned_bytes: source_outer,
    })?);
    destination.extend(source);
    Ok(merged)
}

fn try_vec_for_len<T>(len: usize, context: &'static str) -> io::Result<Vec<T>> {
    checked_vector_bytes::<T>(len, context)?;
    let mut values = Vec::new();
    values.try_reserve_exact(len).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("{context} allocation failed before mutation: {error}"),
        )
    })?;
    Ok(values)
}

fn checked_vector_bytes<T>(len: usize, context: &'static str) -> io::Result<usize> {
    let element_size = std::mem::size_of::<T>();
    let bytes = len.checked_mul(element_size).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{context} vector byte count overflows usize"),
        )
    })?;
    if element_size != 0 && bytes > isize::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{context} requires {bytes} bytes, exceeding the maximum addressable Vec capacity"
            ),
        ));
    }
    Ok(bytes)
}

fn series_samples_owned_scratch(samples: &SeriesSamples) -> io::Result<SeriesScratchFootprint> {
    fn add_bytes(total: &mut usize, bytes: usize) -> io::Result<()> {
        *total = total.checked_add(bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "live-seal owned scratch byte count overflows usize",
            )
        })?;
        Ok(())
    }

    let (sample_slots, bytes) = match samples {
        SeriesSamples::Float { samples, .. } => (
            samples.capacity(),
            checked_vector_bytes::<(u64, f64)>(
                samples.capacity(),
                "live-seal float owned scratch",
            )?,
        ),
        SeriesSamples::Int64 { samples, .. } => (
            samples.capacity(),
            checked_vector_bytes::<(u64, i64)>(samples.capacity(), "live-seal int owned scratch")?,
        ),
        SeriesSamples::Histogram { samples } => {
            let mut bytes = checked_vector_bytes::<(u64, HistogramValue)>(
                samples.capacity(),
                "live-seal histogram owned scratch",
            )?;
            for (_, value) in samples {
                add_bytes(
                    &mut bytes,
                    checked_vector_bytes::<f64>(
                        value.explicit_bounds.capacity(),
                        "live-seal histogram bounds owned scratch",
                    )?,
                )?;
                add_bytes(
                    &mut bytes,
                    checked_vector_bytes::<u64>(
                        value.bucket_counts.capacity(),
                        "live-seal histogram buckets owned scratch",
                    )?,
                )?;
            }
            (samples.capacity(), bytes)
        }
        SeriesSamples::ExponentialHistogram { samples } => {
            let mut bytes = checked_vector_bytes::<(u64, ExponentialHistogramValue)>(
                samples.capacity(),
                "live-seal exponential-histogram owned scratch",
            )?;
            for (_, value) in samples {
                add_bytes(
                    &mut bytes,
                    checked_vector_bytes::<u64>(
                        value.positive.counts.capacity(),
                        "live-seal positive exponential buckets owned scratch",
                    )?,
                )?;
                add_bytes(
                    &mut bytes,
                    checked_vector_bytes::<u64>(
                        value.negative.counts.capacity(),
                        "live-seal negative exponential buckets owned scratch",
                    )?,
                )?;
            }
            (samples.capacity(), bytes)
        }
        SeriesSamples::Summary { samples } => {
            let mut bytes = checked_vector_bytes::<(u64, SummaryValue)>(
                samples.capacity(),
                "live-seal summary owned scratch",
            )?;
            for (_, value) in samples {
                add_bytes(
                    &mut bytes,
                    checked_vector_bytes::<SummaryQuantileValue>(
                        value.quantiles.capacity(),
                        "live-seal summary quantiles owned scratch",
                    )?,
                )?;
            }
            (samples.capacity(), bytes)
        }
    };
    Ok(SeriesScratchFootprint {
        owned_sample_slots: sample_slots,
        owned_bytes: bytes,
    })
}

fn sort_and_dedupe_live_series_samples(
    samples: &mut SeriesSamples,
    footprint: SeriesScratchFootprint,
    scratch_limits: LiveSealScratchLimits,
    scratch: &mut LiveSealScratchProfile,
) -> Result<()> {
    match samples {
        SeriesSamples::Float { samples, .. } => {
            sort_and_dedupe_keep_last(samples, footprint, scratch_limits, scratch)?
        }
        SeriesSamples::Int64 { samples, .. } => {
            sort_and_dedupe_keep_last(samples, footprint, scratch_limits, scratch)?
        }
        SeriesSamples::Histogram { samples } => {
            sort_and_dedupe_keep_last(samples, footprint, scratch_limits, scratch)?
        }
        SeriesSamples::ExponentialHistogram { samples } => {
            sort_and_dedupe_keep_last(samples, footprint, scratch_limits, scratch)?
        }
        SeriesSamples::Summary { samples } => {
            sort_and_dedupe_keep_last(samples, footprint, scratch_limits, scratch)?
        }
    }
    Ok(())
}

fn sort_and_dedupe_keep_last<T>(
    samples: &mut Vec<(u64, T)>,
    footprint: SeriesScratchFootprint,
    scratch_limits: LiveSealScratchLimits,
    scratch: &mut LiveSealScratchProfile,
) -> io::Result<()> {
    if samples.len() < 2 || samples.windows(2).all(|pair| pair[0].0 < pair[1].0) {
        return Ok(());
    }
    if samples.windows(2).any(|pair| pair[0].0 > pair[1].0) {
        // Tagging arrival order allows an allocation-free unstable sort while
        // preserving the stable equal-timestamp order required for LWW. The
        // only allocation is reserved fallibly before `samples` is drained.
        scratch_limits.checked_target_len::<(u64, usize, T)>(
            0,
            samples.len(),
            "live-seal stable-sort scratch",
        )?;
        let mut tagged =
            try_vec_for_len::<(u64, usize, T)>(samples.len(), "live-seal stable-sort scratch")?;
        let tagged_outer = SeriesScratchFootprint {
            owned_sample_slots: tagged.capacity(),
            owned_bytes: checked_vector_bytes::<(u64, usize, T)>(
                tagged.capacity(),
                "live-seal stable-sort owned scratch",
            )?,
        };
        scratch.observe(footprint.checked_add(tagged_outer)?);
        tagged.extend(
            samples
                .drain(..)
                .enumerate()
                .map(|(arrival_order, (timestamp_ms, value))| (timestamp_ms, arrival_order, value)),
        );
        tagged.sort_unstable_by_key(|(timestamp_ms, arrival_order, _)| {
            (*timestamp_ms, *arrival_order)
        });
        samples.extend(
            tagged
                .into_iter()
                .map(|(timestamp_ms, _arrival_order, value)| (timestamp_ms, value)),
        );
    }
    samples.reverse();
    samples.dedup_by_key(|(timestamp_ms, _)| *timestamp_ms);
    samples.reverse();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::ErrorKind;
    use chronoxide_core::labels::METRIC_NAME_LABEL;
    use chronoxide_core::storage::head::{
        ExponentialHistogramBuckets, FloatEncoding, HeadConfig, IntEncoding, SampleValue,
        TypedSampleMetadata,
    };
    use chronoxide_core::storage::segment::SegmentStoreReader;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    fn labelsets() -> (LabelSetInterner, SeriesRef) {
        let mut labelsets = LabelSetInterner::new_versioned_flat();
        let mut stats = OtlpMetricsIngestionStats::new();
        let series = labelsets
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "live_seal_metric")),
                    KeyValueRef::from(("host", "a")),
                ],
                &mut stats,
            )
            .unwrap();
        (labelsets, series)
    }

    fn head() -> HeadBuffer {
        HeadBuffer::new(HeadConfig::with_block_size(
            Duration::from_secs(10),
            2,
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ))
        .unwrap()
    }

    fn relative_files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        fn visit(root: &Path, dir: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
            let mut entries = fs::read_dir(dir)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            entries.sort_by_key(fs::DirEntry::file_name);
            for entry in entries {
                let path = entry.path();
                if entry.file_type().unwrap().is_dir() {
                    visit(root, &path, out);
                } else {
                    out.push((
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        fs::read(path).unwrap(),
                    ));
                }
            }
        }
        let mut files = Vec::new();
        visit(root, root, &mut files);
        files
    }

    fn chronoxide_io_error_kind(error: &crate::error::ChronoxideError) -> io::ErrorKind {
        match error.kind() {
            ErrorKind::IoError(error) => error.kind(),
            other => panic!("expected I/O error, got {other}"),
        }
    }

    fn sample_len(samples: &SeriesSamples) -> usize {
        match samples {
            SeriesSamples::Float { samples, .. } => samples.len(),
            SeriesSamples::Int64 { samples, .. } => samples.len(),
            SeriesSamples::Histogram { samples } => samples.len(),
            SeriesSamples::ExponentialHistogram { samples } => samples.len(),
            SeriesSamples::Summary { samples } => samples.len(),
        }
    }

    fn outer_element_bytes(samples: &SeriesSamples) -> usize {
        match samples {
            SeriesSamples::Float { .. } => std::mem::size_of::<(u64, f64)>(),
            SeriesSamples::Int64 { .. } => std::mem::size_of::<(u64, i64)>(),
            SeriesSamples::Histogram { .. } => std::mem::size_of::<(u64, HistogramValue)>(),
            SeriesSamples::ExponentialHistogram { .. } => {
                std::mem::size_of::<(u64, ExponentialHistogramValue)>()
            }
            SeriesSamples::Summary { .. } => std::mem::size_of::<(u64, SummaryValue)>(),
        }
    }

    fn histogram_value(count: u64) -> HistogramValue {
        HistogramValue {
            count,
            sum: Some(count as f64),
            min: Some(count as f64),
            max: Some(count as f64),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: Vec::new(),
            bucket_counts: vec![count],
        }
    }

    fn exponential_histogram_value(count: u64) -> ExponentialHistogramValue {
        ExponentialHistogramValue {
            count,
            sum: Some(count as f64),
            min: Some(count as f64),
            max: Some(count as f64),
            scale: 0,
            zero_threshold: 0.0,
            zero_count: count,
            metadata: TypedSampleMetadata::default(),
            positive: ExponentialHistogramBuckets {
                offset: 0,
                counts: Vec::new(),
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: Vec::new(),
            },
        }
    }

    fn summary_value(count: u64) -> SummaryValue {
        SummaryValue {
            count,
            sum: count as f64,
            metadata: TypedSampleMetadata::default(),
            quantiles: Vec::new(),
        }
    }

    #[test]
    fn live_seal_count_capacity_and_injected_allocation_boundaries_are_distinct() {
        let limits = LiveSealScratchLimits::UNBOUNDED;
        let count_error = limits
            .checked_target_len::<u8>(usize::MAX, 1, "test count")
            .unwrap_err();
        assert_eq!(count_error.kind(), io::ErrorKind::InvalidData);
        assert!(count_error.to_string().contains("sample count overflows"));

        let element_size = std::mem::size_of::<(u64, f64)>();
        let maximum_elements = (isize::MAX as usize) / element_size;
        assert_eq!(
            checked_vector_bytes::<(u64, f64)>(maximum_elements, "test capacity").unwrap(),
            maximum_elements * element_size
        );
        let capacity_error =
            checked_vector_bytes::<(u64, f64)>(maximum_elements + 1, "test capacity").unwrap_err();
        assert_eq!(capacity_error.kind(), io::ErrorKind::InvalidData);
        assert!(
            capacity_error
                .to_string()
                .contains("maximum addressable Vec capacity")
        );

        let exact_bytes = 2 * element_size;
        assert_eq!(
            LiveSealScratchLimits::with_max_vector_bytes(exact_bytes)
                .checked_target_len::<(u64, f64)>(1, 1, "test limit")
                .unwrap(),
            2
        );
        let allocation_error = LiveSealScratchLimits::with_max_vector_bytes(exact_bytes - 1)
            .checked_target_len::<(u64, f64)>(1, 1, "test limit")
            .unwrap_err();
        assert_eq!(allocation_error.kind(), io::ErrorKind::OutOfMemory);
    }

    #[test]
    fn every_same_kind_merge_reserves_before_mutating_and_can_retry() {
        let cases = vec![
            (
                SeriesSamples::Float {
                    encoding: FloatEncoding::Gorilla,
                    samples: vec![(1, 1.0)],
                },
                SeriesSamples::Float {
                    encoding: FloatEncoding::Gorilla,
                    samples: vec![(2, 2.0)],
                },
            ),
            (
                SeriesSamples::Int64 {
                    encoding: IntEncoding::DeltaZigZag,
                    samples: vec![(1, 1)],
                },
                SeriesSamples::Int64 {
                    encoding: IntEncoding::DeltaZigZag,
                    samples: vec![(2, 2)],
                },
            ),
            (
                SeriesSamples::Histogram {
                    samples: vec![(1, histogram_value(1))],
                },
                SeriesSamples::Histogram {
                    samples: vec![(2, histogram_value(2))],
                },
            ),
            (
                SeriesSamples::ExponentialHistogram {
                    samples: vec![(1, exponential_histogram_value(1))],
                },
                SeriesSamples::ExponentialHistogram {
                    samples: vec![(2, exponential_histogram_value(2))],
                },
            ),
            (
                SeriesSamples::Summary {
                    samples: vec![(1, summary_value(1))],
                },
                SeriesSamples::Summary {
                    samples: vec![(2, summary_value(2))],
                },
            ),
        ];
        let series = SeriesRef::new(0);

        for (mut earlier, later) in cases {
            let before = earlier.clone();
            let retry_later = later.clone();
            let limit = LiveSealScratchLimits::with_max_vector_bytes(outer_element_bytes(&earlier));
            let mut scratch = LiveSealScratchProfile::default();
            let mut earlier_footprint = series_samples_owned_scratch(&earlier).unwrap();
            let later_footprint = series_samples_owned_scratch(&later).unwrap();
            let error = merge_live_series_samples(
                &mut earlier,
                &mut earlier_footprint,
                later,
                later_footprint,
                series,
                limit,
                &mut scratch,
            )
            .unwrap_err();
            assert_eq!(chronoxide_io_error_kind(&error), io::ErrorKind::OutOfMemory);
            assert_eq!(earlier, before);
            assert_eq!(
                earlier_footprint,
                series_samples_owned_scratch(&earlier).unwrap()
            );

            let retry_footprint = series_samples_owned_scratch(&retry_later).unwrap();
            merge_live_series_samples(
                &mut earlier,
                &mut earlier_footprint,
                retry_later,
                retry_footprint,
                series,
                LiveSealScratchLimits::UNBOUNDED,
                &mut scratch,
            )
            .unwrap();
            assert_eq!(sample_len(&earlier), 2);
        }
    }

    #[test]
    fn both_mixed_scalar_directions_stage_conversion_before_replacement() {
        let cases = [
            (
                SeriesSamples::Float {
                    encoding: FloatEncoding::Raw,
                    samples: vec![(1, 1.25)],
                },
                SeriesSamples::Int64 {
                    encoding: IntEncoding::DeltaZigZag,
                    samples: vec![(2, 2)],
                },
            ),
            (
                SeriesSamples::Int64 {
                    encoding: IntEncoding::DeltaZigZag,
                    samples: vec![(1, 1)],
                },
                SeriesSamples::Float {
                    encoding: FloatEncoding::Raw,
                    samples: vec![(2, 2.25)],
                },
            ),
        ];
        let series = SeriesRef::new(0);

        for (mut earlier, later) in cases {
            let before = earlier.clone();
            let retry_later = later.clone();
            let mut scratch = LiveSealScratchProfile::default();
            let mut earlier_footprint = series_samples_owned_scratch(&earlier).unwrap();
            let later_footprint = series_samples_owned_scratch(&later).unwrap();
            let error = merge_live_series_samples(
                &mut earlier,
                &mut earlier_footprint,
                later,
                later_footprint,
                series,
                LiveSealScratchLimits::with_max_vector_bytes(std::mem::size_of::<(u64, f64)>()),
                &mut scratch,
            )
            .unwrap_err();
            assert_eq!(chronoxide_io_error_kind(&error), io::ErrorKind::OutOfMemory);
            assert_eq!(earlier, before);

            let retry_footprint = series_samples_owned_scratch(&retry_later).unwrap();
            merge_live_series_samples(
                &mut earlier,
                &mut earlier_footprint,
                retry_later,
                retry_footprint,
                series,
                LiveSealScratchLimits::UNBOUNDED,
                &mut scratch,
            )
            .unwrap();
            let SeriesSamples::Float { encoding, samples } = earlier else {
                panic!("mixed scalars must normalize to Float");
            };
            assert_eq!(encoding, FloatEncoding::Raw);
            assert_eq!(samples.len(), 2);
            assert!(scratch.peak_owned_sample_slots >= 4);
        }
    }

    #[test]
    fn pure_int_writer_conversion_is_transactional() {
        let mut samples = SeriesSamples::Int64 {
            encoding: IntEncoding::DeltaZigZag,
            samples: vec![(1, 1), (2, 2)],
        };
        let before = samples.clone();
        let mut scratch = LiveSealScratchProfile::default();
        let mut footprint = series_samples_owned_scratch(&samples).unwrap();
        let error = normalize_live_scalar_for_writer(
            &mut samples,
            &mut footprint,
            LiveSealScratchLimits::with_max_vector_bytes(std::mem::size_of::<(u64, f64)>()),
            &mut scratch,
        )
        .unwrap_err();
        assert_eq!(chronoxide_io_error_kind(&error), io::ErrorKind::OutOfMemory);
        assert_eq!(samples, before);

        normalize_live_scalar_for_writer(
            &mut samples,
            &mut footprint,
            LiveSealScratchLimits::UNBOUNDED,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(
            samples,
            SeriesSamples::Float {
                encoding: FloatEncoding::Gorilla,
                samples: vec![(1, 1.0), (2, 2.0)],
            }
        );
    }

    #[test]
    fn stable_sort_scratch_failure_is_non_mutating_and_retry_keeps_last() {
        let mut samples = SeriesSamples::Float {
            encoding: FloatEncoding::Gorilla,
            samples: vec![(2, 2.0), (1, 1.0), (1, 3.0)],
        };
        let before = samples.clone();
        let footprint = series_samples_owned_scratch(&samples).unwrap();
        let mut scratch = LiveSealScratchProfile::default();
        let error = sort_and_dedupe_live_series_samples(
            &mut samples,
            footprint,
            LiveSealScratchLimits::with_max_vector_bytes(3 * std::mem::size_of::<(u64, f64)>()),
            &mut scratch,
        )
        .unwrap_err();
        assert_eq!(chronoxide_io_error_kind(&error), io::ErrorKind::OutOfMemory);
        assert_eq!(samples, before);

        sort_and_dedupe_live_series_samples(
            &mut samples,
            footprint,
            LiveSealScratchLimits::UNBOUNDED,
            &mut scratch,
        )
        .unwrap();
        assert_eq!(
            samples,
            SeriesSamples::Float {
                encoding: FloatEncoding::Gorilla,
                samples: vec![(1, 3.0), (2, 2.0)],
            }
        );
    }

    #[test]
    fn failed_scratch_attempt_retains_fragments_and_retry_matches_bytes_and_lww_result() {
        let (labelsets, series) = labelsets();
        let mut head = head();
        head.record_sample(series, 1_000, SampleValue::Float(1.0))
            .unwrap();
        let mut fragments = head.try_freeze_for_publication().unwrap();
        head.record_sample(series, 1_000, SampleValue::Float(2.0))
            .unwrap();
        fragments.extend(head.try_freeze_for_publication().unwrap());
        let fragments = fragments.into_iter().map(Arc::new).collect::<Vec<_>>();

        let live_root = tempfile::tempdir().unwrap();
        let live_config = SegmentWriterConfig::new(live_root.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(91);
        let live_id = live_config.allocate_segment_id(0, 10_000).unwrap();
        let failure = match build_frozen_segment_writer_with_limits(
            live_config.clone(),
            live_id,
            &labelsets,
            0,
            10_000,
            SegmentPayloadLane::InOrder,
            &fragments,
            LiveSealScratchLimits::with_max_vector_bytes(std::mem::size_of::<(u64, f64)>()),
        ) {
            Ok(_) => panic!("the deterministic scratch limit must fail the first seal attempt"),
            Err(error) => error,
        };
        assert_eq!(
            chronoxide_io_error_kind(&failure),
            io::ErrorKind::OutOfMemory
        );
        let retained = fragments
            .iter()
            .map(|fragment| {
                fragment
                    .series_kind_samples_in_range(series, SampleKind::Float, 0, 10_000)
                    .unwrap()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            retained,
            vec![
                SeriesSamples::Float {
                    encoding: FloatEncoding::Gorilla,
                    samples: vec![(1_000, 1.0)],
                },
                SeriesSamples::Float {
                    encoding: FloatEncoding::Gorilla,
                    samples: vec![(1_000, 2.0)],
                },
            ]
        );

        let (mut live_writer, scratch) = build_frozen_segment_writer_with_limits(
            live_config,
            live_id,
            &labelsets,
            0,
            10_000,
            SegmentPayloadLane::InOrder,
            &fragments,
            LiveSealScratchLimits::UNBOUNDED,
        )
        .unwrap();
        assert!(scratch.peak_owned_sample_slots >= 2);
        assert!(scratch.peak_owned_bytes >= 2 * std::mem::size_of::<(u64, f64)>());
        finish_frozen_segment_writer(&mut live_writer)
            .unwrap()
            .expect("live segment");

        let baseline_root = tempfile::tempdir().unwrap();
        let baseline_config =
            SegmentWriterConfig::new(baseline_root.path(), Duration::from_secs(10))
                .with_deterministic_segment_ids(91);
        let baseline_id = baseline_config.allocate_segment_id(0, 10_000).unwrap();
        assert_eq!(live_id, baseline_id);
        let mut baseline = SegmentWriter::new(baseline_config).unwrap();
        baseline.set_next_segment_id_for_retry(baseline_id).unwrap();
        baseline
            .reserve_metric_query_ordered_window_series_with_label_counts(0, 10_000, [(series, 2)])
            .unwrap();
        record_segment_float_samples(&labelsets, &mut baseline, series, &[(1_000, 2.0)], false)
            .unwrap();
        baseline.flush().unwrap();

        assert_eq!(
            relative_files(live_root.path()),
            relative_files(baseline_root.path())
        );
        let result = SegmentStoreReader::open(live_root.path())
            .unwrap()
            .query_exact(
                &[(METRIC_NAME_LABEL, "live_seal_metric"), ("host", "a")],
                0,
                10_000,
            )
            .unwrap();
        assert_eq!(result[0].samples, vec![(1_000, 2.0)]);
    }

    #[test]
    fn pure_int_frozen_seal_preserves_existing_float_segment_bytes() {
        let (labelsets, series) = labelsets();
        let mut head = head();
        head.record_sample(series, 1_000, SampleValue::Int64(7))
            .unwrap();
        let fragments = head
            .try_freeze_for_publication()
            .unwrap()
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();

        let live_root = tempfile::tempdir().unwrap();
        let live_config = SegmentWriterConfig::new(live_root.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(93);
        let live_id = live_config.allocate_segment_id(0, 10_000).unwrap();
        let mut live_writer = build_frozen_segment_writer(
            live_config,
            live_id,
            &labelsets,
            0,
            10_000,
            SegmentPayloadLane::InOrder,
            &fragments,
        )
        .unwrap();
        finish_frozen_segment_writer(&mut live_writer)
            .unwrap()
            .expect("live segment");

        let baseline_root = tempfile::tempdir().unwrap();
        let baseline_config =
            SegmentWriterConfig::new(baseline_root.path(), Duration::from_secs(10))
                .with_deterministic_segment_ids(93);
        let baseline_id = baseline_config.allocate_segment_id(0, 10_000).unwrap();
        assert_eq!(live_id, baseline_id);
        let mut baseline = SegmentWriter::new(baseline_config).unwrap();
        baseline.set_next_segment_id_for_retry(baseline_id).unwrap();
        baseline
            .reserve_metric_query_ordered_window_series_with_label_counts(0, 10_000, [(series, 2)])
            .unwrap();
        record_segment_float_samples(&labelsets, &mut baseline, series, &[(1_000, 7.0)], false)
            .unwrap();
        baseline.flush().unwrap();

        assert_eq!(
            relative_files(live_root.path()),
            relative_files(baseline_root.path())
        );
        let result = SegmentStoreReader::open(live_root.path())
            .unwrap()
            .query_exact(
                &[(METRIC_NAME_LABEL, "live_seal_metric"), ("host", "a")],
                0,
                10_000,
            )
            .unwrap();
        assert_eq!(result[0].samples, vec![(1_000, 7.0)]);
    }

    #[test]
    fn preseal_ooo_is_merged_after_in_order_and_postseal_ooo_uses_ooo_payload() {
        let (labelsets, series) = labelsets();
        let mut head = HeadBuffer::new(
            HeadConfig::with_block_size(
                Duration::from_secs(10),
                2,
                FloatEncoding::Gorilla,
                IntEncoding::DeltaZigZag,
            )
            .with_out_of_order_time_window(Duration::from_secs(10)),
        )
        .unwrap();
        head.record_sample(series, 6_000, SampleValue::Float(6.0))
            .unwrap();
        let mut fragments = head.try_freeze_for_publication().unwrap();
        head.record_sample(series, 5_000, SampleValue::Float(5.0))
            .unwrap();
        fragments.extend(head.try_freeze_for_publication().unwrap());
        assert_eq!(
            fragments
                .iter()
                .map(FrozenHeadFragment::lane)
                .collect::<Vec<_>>(),
            vec![FrozenHeadLane::InOrder, FrozenHeadLane::OutOfOrder]
        );

        let root = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(root.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(92);
        let id = config.allocate_segment_id(0, 10_000).unwrap();
        let fragments = fragments.into_iter().map(Arc::new).collect::<Vec<_>>();
        let mut writer = build_frozen_segment_writer(
            config,
            id,
            &labelsets,
            0,
            10_000,
            SegmentPayloadLane::InOrder,
            &fragments,
        )
        .unwrap();
        finish_frozen_segment_writer(&mut writer).unwrap();

        let segment_dir = root.path().join(id.dir_name());
        assert!(fs::metadata(segment_dir.join("chunks.bin")).unwrap().len() > 0);
        assert_eq!(
            fs::metadata(segment_dir.join("ooo_chunks.bin"))
                .unwrap()
                .len(),
            0
        );
        let result = SegmentStoreReader::open(root.path())
            .unwrap()
            .query_exact(
                &[(METRIC_NAME_LABEL, "live_seal_metric"), ("host", "a")],
                0,
                10_000,
            )
            .unwrap();
        assert_eq!(result[0].samples, vec![(5_000, 5.0), (6_000, 6.0)]);
    }
}
