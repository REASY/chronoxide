use std::fmt;

use crate::storage::chunk::{ChunkKind, IndexedChunkAuthentication, IndexedChunkLocator};
use crate::storage::series::v3::{ChunkLocatorSource, SERIES_HOT_RECORDS_PER_PAGE_V1};

use super::*;

/// Caller-directed control for a fallible metadata visitor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentMetadataVisitControl {
    Continue,
    Stop,
}

/// Distinguishes complete traversal from an intentional early stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentMetadataVisitOutcome {
    Complete,
    Stopped,
}

/// Keeps caller failures distinct from storage/runtime failures and from an
/// intentional early stop. Visitor failures are never recorded as sticky
/// artifact corruption.
#[derive(Debug)]
pub(crate) enum SegmentMetadataVisitError<E> {
    Metadata(SegmentMetadataFacadeError),
    Visitor(E),
}

impl<E> From<SegmentMetadataFacadeError> for SegmentMetadataVisitError<E> {
    fn from(error: SegmentMetadataFacadeError) -> Self {
        Self::Metadata(error)
    }
}

impl<E> From<Schema6SeriesReaderError> for SegmentMetadataVisitError<E> {
    fn from(error: Schema6SeriesReaderError) -> Self {
        Self::Metadata(error.into())
    }
}

impl<E> From<Schema6ChunkIndexReaderError> for SegmentMetadataVisitError<E> {
    fn from(error: Schema6ChunkIndexReaderError) -> Self {
        Self::Metadata(error.into())
    }
}

impl<E> From<Schema7MetadataReaderError> for SegmentMetadataVisitError<E> {
    fn from(error: Schema7MetadataReaderError) -> Self {
        Self::Metadata(error.into())
    }
}

impl<E: fmt::Display> fmt::Display for SegmentMetadataVisitError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metadata(error) => write!(formatter, "segment metadata visit failed: {error}"),
            Self::Visitor(error) => write!(formatter, "segment metadata visitor failed: {error}"),
        }
    }
}

impl<E> std::error::Error for SegmentMetadataVisitError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Metadata(error) => Some(error),
            Self::Visitor(error) => Some(error),
        }
    }
}

/// Schema-neutral authentication carried by one exact chunk locator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentChunkAuthentication {
    Schema6Legacy,
    Schema7IndexedPrefix { crc32c: u32 },
}

/// Ephemeral view of one checked chunk locator. Raw layout directories and
/// metadata offsets never cross the facade.
#[derive(Clone, Copy)]
pub(crate) struct SegmentChunkLocator<'a> {
    locator: &'a IndexedChunkLocator,
}

impl SegmentChunkLocator<'_> {
    pub(crate) fn series_ref(&self) -> u32 {
        self.locator.series_ref()
    }

    pub(crate) fn file_id(&self) -> u8 {
        self.locator.entry().file_id
    }

    pub(crate) fn kind(&self) -> ChunkKind {
        self.locator.entry().kind
    }

    pub(crate) fn flags(&self) -> u16 {
        self.locator.entry().flags
    }

    pub(crate) fn min_time_ms(&self) -> u64 {
        self.locator.entry().min_time_ms
    }

    pub(crate) fn max_time_ms(&self) -> u64 {
        self.locator.entry().max_time_ms
    }

    pub(crate) fn file_offset(&self) -> u64 {
        self.locator.entry().offset
    }

    pub(crate) fn chunk_len(&self) -> u32 {
        self.locator.entry().length
    }

    pub(crate) fn scalar_lane_offset(&self) -> u32 {
        self.locator.entry().scalar_lane_offset
    }

    pub(crate) fn scalar_lane_len(&self) -> u32 {
        self.locator.entry().scalar_lane_len
    }

    pub(crate) fn indexed_prefix_len(&self) -> usize {
        self.locator.indexed_prefix_len()
    }

    pub(crate) fn authentication(&self) -> SegmentChunkAuthentication {
        match self.locator.authentication() {
            IndexedChunkAuthentication::Schema6V1Legacy => {
                SegmentChunkAuthentication::Schema6Legacy
            }
            IndexedChunkAuthentication::Schema7 {
                indexed_prefix_crc32c,
            } => SegmentChunkAuthentication::Schema7IndexedPrefix {
                crc32c: indexed_prefix_crc32c,
            },
        }
    }

    /// Clones the checked schema-neutral locator for deferred payload
    /// scheduling after this metadata callback returns. In particular, this
    /// retains schema-7 indexed-prefix authentication instead of degrading it
    /// to an unauthenticated physical range.
    pub(crate) fn to_owned_indexed_locator(self) -> IndexedChunkLocator {
        self.locator.clone()
    }
}

/// Opaque, borrowed locator batch whose owner retains every cache pin and
/// scratch charge for the complete visit.
pub(crate) struct SegmentChunkLocatorBatch<'a> {
    series_ref: u32,
    locators: &'a [IndexedChunkLocator],
}

impl SegmentChunkLocatorBatch<'_> {
    pub(crate) fn len(&self) -> usize {
        self.locators.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.locators.is_empty()
    }

    pub(crate) fn visit<E>(
        &self,
        mut visitor: impl FnMut(SegmentChunkLocator<'_>) -> Result<SegmentMetadataVisitControl, E>,
    ) -> Result<SegmentMetadataVisitOutcome, E> {
        for locator in self.locators {
            debug_assert_eq!(locator.series_ref(), self.series_ref);
            match visitor(SegmentChunkLocator { locator })? {
                SegmentMetadataVisitControl::Continue => {}
                SegmentMetadataVisitControl::Stop => {
                    return Ok(SegmentMetadataVisitOutcome::Stopped);
                }
            }
        }
        Ok(SegmentMetadataVisitOutcome::Complete)
    }
}

/// One fully verified series and its exact routed chunk locators. Stable
/// identity and canonical labels become observable only through this view.
pub(crate) struct SegmentVerifiedSeries<'a> {
    series_ref: u32,
    series_id: u64,
    metric_name_dropped_series_id: Option<u64>,
    kind_mask: u8,
    labels_complete: bool,
    integrity_checked_label_count: usize,
    labels: &'a [(String, String)],
    chunks: SegmentChunkLocatorBatch<'a>,
}

impl SegmentVerifiedSeries<'_> {
    pub(crate) fn series_ref(&self) -> u32 {
        self.series_ref
    }

    pub(crate) fn series_id(&self) -> u64 {
        self.series_id
    }

    pub(crate) fn metric_name_dropped_series_id(&self) -> Option<u64> {
        self.metric_name_dropped_series_id
    }

    pub(crate) fn kind_mask(&self) -> u8 {
        self.kind_mask
    }

    /// Whether [`Self::labels`] contains the complete canonical label set.
    pub(crate) fn labels_complete(&self) -> bool {
        self.labels_complete
    }

    pub(crate) fn integrity_checked_label_count(&self) -> usize {
        self.integrity_checked_label_count
    }

    pub(crate) fn labels(&self) -> &[(String, String)] {
        self.labels
    }

    pub(crate) fn chunks(&self) -> &SegmentChunkLocatorBatch<'_> {
        &self.chunks
    }
}

#[derive(Clone, Copy)]
enum SegmentLabelSelection<'a> {
    All,
    Requested {
        label_names: &'a [String],
        selective_kind_mask: u8,
        derive_metric_name_dropped_identity: bool,
    },
}

impl SegmentMetadataSession {
    /// Routes a governed candidate set and visits fully verified series in
    /// ascending `series_ref` order. The facade itself allocates nothing:
    /// candidate refs, backend planning, labels, and locator vectors retain
    /// their exact governor charges for the duration of each callback.
    pub(crate) fn visit_verified_series<E>(
        &self,
        root: &SegmentMetadataRoot,
        candidates: &GovernedSeriesRefSet,
        visitor: impl FnMut(SegmentVerifiedSeries<'_>) -> Result<SegmentMetadataVisitControl, E>,
    ) -> Result<SegmentMetadataVisitOutcome, SegmentMetadataVisitError<E>> {
        self.visit_verified_series_with_selection(
            root,
            candidates,
            SegmentLabelSelection::All,
            visitor,
        )
    }

    /// Visits fully integrity-checked series while exposing only the requested
    /// canonical label names. Omitted labels still participate in complete
    /// row decoding, symbol integrity checks, and stable-identity verification.
    pub(crate) fn visit_verified_series_selected<E>(
        &self,
        root: &SegmentMetadataRoot,
        candidates: &GovernedSeriesRefSet,
        selected_label_names: &[String],
        selective_kind_mask: u8,
        derive_metric_name_dropped_identity: bool,
        visitor: impl FnMut(SegmentVerifiedSeries<'_>) -> Result<SegmentMetadataVisitControl, E>,
    ) -> Result<SegmentMetadataVisitOutcome, SegmentMetadataVisitError<E>> {
        self.visit_verified_series_with_selection(
            root,
            candidates,
            SegmentLabelSelection::Requested {
                label_names: selected_label_names,
                selective_kind_mask,
                derive_metric_name_dropped_identity,
            },
            visitor,
        )
    }

    fn visit_verified_series_with_selection<E>(
        &self,
        root: &SegmentMetadataRoot,
        candidates: &GovernedSeriesRefSet,
        label_selection: SegmentLabelSelection<'_>,
        mut visitor: impl FnMut(SegmentVerifiedSeries<'_>) -> Result<SegmentMetadataVisitControl, E>,
    ) -> Result<SegmentMetadataVisitOutcome, SegmentMetadataVisitError<E>> {
        self.ensure_set(root, candidates)?;
        match (&self.backend, &root.backend) {
            (
                SegmentMetadataSessionBackend::Schema6 {
                    series,
                    chunk_index,
                    ..
                },
                SegmentMetadataRootBackend::Schema6 {
                    series: series_root,
                    chunk_index: chunk_index_root,
                    ..
                },
            ) => {
                // Schema 6 keeps its established batch verifier unchanged.
                // Selective ownership is a schema-7/8 read-layout capability.
                let verified = series.materialize_verified(
                    series_root,
                    chunk_index,
                    chunk_index_root,
                    &self.symbols,
                    candidates.values(),
                )?;
                for value in series.verified_series(&verified)? {
                    let locators = chunk_index.read_series_entries(
                        chunk_index_root,
                        value.series_ref(),
                        value.chunk_index(),
                    )?;
                    let locators = chunk_index.locators(&locators)?;
                    let view = SegmentVerifiedSeries {
                        series_ref: value.series_ref(),
                        series_id: value.series_id(),
                        metric_name_dropped_series_id: None,
                        kind_mask: value.kind_mask(),
                        labels_complete: true,
                        integrity_checked_label_count: value.labels().len(),
                        labels: value.labels(),
                        chunks: SegmentChunkLocatorBatch {
                            series_ref: value.series_ref(),
                            locators,
                        },
                    };
                    if visitor(view).map_err(SegmentMetadataVisitError::Visitor)?
                        == SegmentMetadataVisitControl::Stop
                    {
                        return Ok(SegmentMetadataVisitOutcome::Stopped);
                    }
                }
            }
            (
                SegmentMetadataSessionBackend::Schema7 { series, .. },
                SegmentMetadataRootBackend::Schema7 {
                    series: series_root,
                    ..
                },
            ) => {
                let refs = candidates.values();
                let mut page_start = 0usize;
                while page_start < refs.len() {
                    let page_index = refs[page_start] / SERIES_HOT_RECORDS_PER_PAGE_V1;
                    let page_end = refs[page_start..]
                        .partition_point(|series_ref| {
                            *series_ref / SERIES_HOT_RECORDS_PER_PAGE_V1 == page_index
                        })
                        .checked_add(page_start)
                        .ok_or(SegmentMetadataFacadeError::RefSetSizeOverflow)?;
                    let planned = series.plan_hot_page(
                        series_root,
                        page_index,
                        &refs[page_start..page_end],
                    )?;
                    let mut materialization =
                        series.materialization_context(series_root, planned.len())?;
                    for planned_index in 0..planned.len() {
                        let planned_value = planned.get(planned_index).ok_or(
                            SegmentMetadataFacadeError::InvalidSeriesRef {
                                series_ref: refs[page_start],
                                series_count: root.series_count,
                            },
                        )?;
                        let verified = match label_selection {
                            SegmentLabelSelection::Requested {
                                label_names,
                                selective_kind_mask,
                                derive_metric_name_dropped_identity,
                            } if planned_value.kind_mask & !selective_kind_mask == 0 => series
                                .materialize_verified_selected_cached(
                                    series_root,
                                    &self.symbols,
                                    &mut materialization,
                                    planned_value,
                                    label_names,
                                    derive_metric_name_dropped_identity,
                                )?,
                            SegmentLabelSelection::All
                            | SegmentLabelSelection::Requested { .. } => series
                                .materialize_verified_cached(
                                    series_root,
                                    &self.symbols,
                                    &mut materialization,
                                    planned_value,
                                )?,
                        };
                        match &planned_value.chunks {
                            ChunkLocatorSource::Inline(locator) => {
                                let locators = std::slice::from_ref(locator);
                                let view = SegmentVerifiedSeries {
                                    series_ref: verified.series_ref(),
                                    series_id: verified.series_id(),
                                    metric_name_dropped_series_id: verified
                                        .metric_name_dropped_series_id(),
                                    kind_mask: verified.kind_mask(),
                                    labels_complete: verified.labels_complete(),
                                    integrity_checked_label_count: verified
                                        .integrity_checked_label_count(),
                                    labels: verified.labels(),
                                    chunks: SegmentChunkLocatorBatch {
                                        series_ref: verified.series_ref(),
                                        locators,
                                    },
                                };
                                if visitor(view).map_err(SegmentMetadataVisitError::Visitor)?
                                    == SegmentMetadataVisitControl::Stop
                                {
                                    return Ok(SegmentMetadataVisitOutcome::Stopped);
                                }
                            }
                            ChunkLocatorSource::Overflow { .. } => {
                                let locator_batch =
                                    series.plan_overflow_blob(series_root, planned_value)?;
                                let view = SegmentVerifiedSeries {
                                    series_ref: verified.series_ref(),
                                    series_id: verified.series_id(),
                                    metric_name_dropped_series_id: verified
                                        .metric_name_dropped_series_id(),
                                    kind_mask: verified.kind_mask(),
                                    labels_complete: verified.labels_complete(),
                                    integrity_checked_label_count: verified
                                        .integrity_checked_label_count(),
                                    labels: verified.labels(),
                                    chunks: SegmentChunkLocatorBatch {
                                        series_ref: verified.series_ref(),
                                        locators: locator_batch.locators(),
                                    },
                                };
                                if visitor(view).map_err(SegmentMetadataVisitError::Visitor)?
                                    == SegmentMetadataVisitControl::Stop
                                {
                                    return Ok(SegmentMetadataVisitOutcome::Stopped);
                                }
                            }
                        }
                    }
                    page_start = page_end;
                }
            }
            _ => {
                return Err(SegmentMetadataFacadeError::ForeignLayoutBackend.into());
            }
        }
        Ok(SegmentMetadataVisitOutcome::Complete)
    }
}
