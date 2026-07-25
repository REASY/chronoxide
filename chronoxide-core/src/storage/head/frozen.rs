use std::error;
use std::fmt;
use std::sync::Arc;

use crate::storage::arena::FrozenBlockArena;
#[cfg(any(test, feature = "test-hooks"))]
use crate::storage::arena::{ArenaRead, BufferRef};

use super::*;

mod persistent;

#[cfg(test)]
mod tests;

pub use persistent::*;

/// The physical head lane retained by one immutable fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FrozenHeadLane {
    InOrder,
    OutOfOrder,
}

impl FrozenHeadLane {
    pub(crate) const fn is_out_of_order(self) -> bool {
        matches!(self, Self::OutOfOrder)
    }
}

/// One sealed encoded series in a compact, sorted frozen-fragment directory.
///
/// This first implementation deliberately owns the already encoded
/// `EncodedSeries`. It drops the mutable head table and all of its hash/direct
/// lookup capacity, but it does not yet erase the codec enum into a smaller
/// fixed-size descriptor. That further footprint reduction needs a codec-wide
/// immutable descriptor design; retaining the sealed encoder here keeps the
/// initial correctness boundary narrow and preserves the original bytes.
#[derive(Debug)]
struct FrozenSeriesRun {
    series: SeriesRef,
    kind: SampleKind,
    encoded: EncodedSeries,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrozenSeriesKey {
    pub series: SeriesRef,
    pub kind: SampleKind,
    pub codec: &'static str,
    pub samples: u64,
    pub blocks: usize,
}

/// Immutable encoded samples extracted from one head window.
///
/// The series directory is sorted by `(SeriesRef, SampleKind, codec name)`.
/// The mutable `HeadSeriesTable` and mutable arena capacity are not retained.
#[derive(Debug)]
pub struct FrozenHeadFragment {
    start_ms: u64,
    end_ms: u64,
    lane: FrozenHeadLane,
    datapoints: u64,
    coverage_tracking: bool,
    coverage: CoverageLedger,
    recorded_order_range: Option<RecordedSampleOrderRange>,
    recorded_orders: RecordedSampleOrderSet,
    publication_sequence: u64,
    runs: Box<[FrozenSeriesRun]>,
    arena: FrozenBlockArena,
}

impl FrozenHeadFragment {
    pub fn start_ms(&self) -> u64 {
        self.start_ms
    }

    pub fn end_ms(&self) -> u64 {
        self.end_ms
    }

    pub fn lane(&self) -> FrozenHeadLane {
        self.lane
    }

    pub fn datapoints(&self) -> u64 {
        self.datapoints
    }

    pub fn coverage_tracking_enabled(&self) -> bool {
        self.coverage_tracking
    }

    pub fn coverage(&self) -> CoverageLedger {
        self.coverage
    }

    pub fn recorded_order_range(&self) -> Option<RecordedSampleOrderRange> {
        self.recorded_order_range
    }

    pub fn recorded_orders(&self) -> &RecordedSampleOrderSet {
        &self.recorded_orders
    }

    pub fn publication_sequence(&self) -> u64 {
        self.publication_sequence
    }

    pub fn series_len(&self) -> usize {
        self.runs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub fn arena_page_count(&self) -> usize {
        self.arena.page_count()
    }

    pub fn arena_used_bytes(&self) -> usize {
        self.arena.total_used_bytes()
    }

    pub fn arena_allocated_bytes(&self) -> usize {
        self.arena.total_allocated_bytes()
    }

    pub fn estimated_run_bytes(&self) -> usize {
        self.runs
            .iter()
            .fold(self.recorded_orders.estimated_heap_bytes(), |bytes, run| {
                bytes
                    .saturating_add(std::mem::size_of::<FrozenSeriesRun>())
                    .saturating_add(run.encoded.estimated_bytes())
            })
    }

    pub fn series_keys(&self) -> impl ExactSizeIterator<Item = FrozenSeriesKey> + '_ {
        self.runs.iter().map(|run| FrozenSeriesKey {
            series: run.series,
            kind: run.kind,
            codec: run.encoded.codec_name(),
            samples: run.encoded.sample_count(),
            blocks: run.encoded.block_count(),
        })
    }

    /// Decodes one exact `(SeriesRef, SampleKind)` run without consuming the
    /// fragment. This is the bounded per-series primitive used by the later
    /// streaming seal path.
    pub fn series_kind_samples_in_range(
        &self,
        series: SeriesRef,
        kind: SampleKind,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Option<SeriesSamples>> {
        if end_ms <= start_ms {
            return Ok(None);
        }
        let Some(run) = self.run_exact(series, kind) else {
            return Ok(None);
        };
        let samples = run
            .encoded
            .samples_in_range(&self.arena, start_ms, end_ms)?;
        Ok((!samples.is_empty()).then_some(samples))
    }

    pub fn series_samples_in_range(
        &self,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<(SeriesRef, SeriesSamples)>> {
        if end_ms <= start_ms {
            return Ok(Vec::new());
        }

        let mut samples = Vec::new();
        samples
            .try_reserve_exact(self.runs.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        for run in &self.runs {
            let decoded = run
                .encoded
                .samples_in_range(&self.arena, start_ms, end_ms)?;
            if !decoded.is_empty() {
                samples.push((run.series, decoded));
            }
        }
        Ok(samples)
    }

    pub(crate) fn set_publication_sequence(&mut self, sequence: u64) {
        self.publication_sequence = sequence;
    }

    fn run_exact(&self, series: SeriesRef, kind: SampleKind) -> Option<&FrozenSeriesRun> {
        self.runs
            .binary_search_by_key(&(series, kind), |run| (run.series, run.kind))
            .ok()
            .map(|index| &self.runs[index])
    }

    #[cfg(test)]
    fn run_keys(&self) -> impl Iterator<Item = (SeriesRef, SampleKind, &'static str)> + '_ {
        self.runs
            .iter()
            .map(|run| (run.series, run.kind, run.encoded.codec_name()))
    }
}

/// A failed head-window freeze that still owns the complete recoverable window.
pub struct FreezeHeadWindowError {
    source: io::Error,
    window: Box<HeadWindow>,
}

impl FreezeHeadWindowError {
    pub(super) fn new(source: io::Error, window: HeadWindow) -> Self {
        Self {
            source,
            window: Box::new(window),
        }
    }

    pub fn error(&self) -> &io::Error {
        &self.source
    }

    pub fn into_parts(self) -> (io::Error, HeadWindow) {
        (self.source, *self.window)
    }
}

impl fmt::Debug for FreezeHeadWindowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FreezeHeadWindowError")
            .field("source", &self.source)
            .field("start_ms", &self.window.start_ms)
            .field("end_ms", &self.window.end_ms)
            .field("out_of_order", &self.window.out_of_order)
            .field("datapoints", &self.window.datapoints)
            .finish()
    }
}

impl fmt::Display for FreezeHeadWindowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl error::Error for FreezeHeadWindowError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.source)
    }
}

/// A partially completed buffer extraction.
///
/// The buffer has restored the window that failed (and retains every window
/// not yet attempted). Fragments completed before the failure remain owned by
/// this error so no encoded sample is silently discarded.
pub struct FreezeHeadBufferError {
    source: io::Error,
    fragments: Vec<FrozenHeadFragment>,
}

impl FreezeHeadBufferError {
    pub(crate) fn new(source: io::Error, fragments: Vec<FrozenHeadFragment>) -> Self {
        Self { source, fragments }
    }

    pub fn error(&self) -> &io::Error {
        &self.source
    }

    pub fn into_parts(self) -> (io::Error, Vec<FrozenHeadFragment>) {
        (self.source, self.fragments)
    }
}

impl fmt::Debug for FreezeHeadBufferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FreezeHeadBufferError")
            .field("source", &self.source)
            .field("completed_fragments", &self.fragments.len())
            .finish()
    }
}

impl fmt::Display for FreezeHeadBufferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl error::Error for FreezeHeadBufferError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.source)
    }
}

impl HeadWindow {
    /// Consumes this mutable window into exact-used immutable encoded storage.
    ///
    /// Every builder is sealed and every arena reference is checked before the
    /// arena is frozen. Any ordinary validation/allocation failure returns a
    /// complete window that can be restored or retried.
    pub fn try_freeze(mut self) -> Result<FrozenHeadFragment, FreezeHeadWindowError> {
        let mut runs = Vec::new();
        if let Err(error) = runs.try_reserve_exact(self.series.len()) {
            return Err(FreezeHeadWindowError::new(
                io::Error::new(io::ErrorKind::OutOfMemory, error),
                self,
            ));
        }

        if let Err(error) = self.try_seal_all_series() {
            return Err(FreezeHeadWindowError::new(error, self));
        }

        let mut sample_count = 0u64;
        for (_series, encoded) in self.series.iter() {
            if let Err(error) = encoded.validate_arena(&self.arena) {
                return Err(FreezeHeadWindowError::new(error, self));
            }
            sample_count = match sample_count.checked_add(encoded.sample_count()) {
                Some(count) => count,
                None => {
                    return Err(FreezeHeadWindowError::new(
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "frozen head sample count overflows u64",
                        ),
                        self,
                    ));
                }
            };
        }
        if sample_count != self.datapoints {
            return Err(FreezeHeadWindowError::new(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "head datapoint count {} disagrees with encoded series count {sample_count}",
                        self.datapoints
                    ),
                ),
                self,
            ));
        }
        if self.coverage_tracking {
            if self.coverage.sample_count() != self.datapoints
                || self.recorded_orders.sample_count() != self.datapoints
                || (self.datapoints == 0) != self.recorded_order_range.is_none()
                || (self.datapoints == 0) != self.recorded_orders.is_empty()
            {
                return Err(FreezeHeadWindowError::new(
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "tracked head coverage does not exactly match its recorded datapoints",
                    ),
                    self,
                ));
            }
            if let Err(error) = self.recorded_orders.validate() {
                return Err(FreezeHeadWindowError::new(error, self));
            }
            if let Some(range) = self.recorded_order_range {
                let exact_first = self.recorded_orders.runs().first().map(|run| run.first());
                let exact_last = self.recorded_orders.runs().last().map(|run| run.last());
                if exact_first != Some(range.first()) || exact_last != Some(range.last()) {
                    return Err(FreezeHeadWindowError::new(
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "tracked head coarse order range disagrees with exact ownership",
                        ),
                        self,
                    ));
                }
            }
        } else if self.coverage != CoverageLedger::empty()
            || self.recorded_order_range.is_some()
            || !self.recorded_orders.is_empty()
        {
            return Err(FreezeHeadWindowError::new(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "untracked head window unexpectedly carries live coverage",
                ),
                self,
            ));
        }

        let HeadWindow {
            start_ms,
            end_ms,
            series,
            datapoints,
            arena,
            out_of_order,
            coverage_tracking,
            coverage,
            recorded_order_range,
            recorded_orders,
        } = self;
        let arena = match arena.try_freeze() {
            Ok(arena) => arena,
            Err(error) => {
                let (source, arena) = error.into_parts();
                return Err(FreezeHeadWindowError::new(
                    source,
                    HeadWindow {
                        start_ms,
                        end_ms,
                        series,
                        datapoints,
                        arena,
                        out_of_order,
                        coverage_tracking,
                        coverage,
                        recorded_order_range,
                        recorded_orders,
                    },
                ));
            }
        };

        for (series, encoded) in series.into_entries() {
            runs.push(FrozenSeriesRun {
                series,
                kind: encoded.kind(),
                encoded,
            });
        }
        runs.sort_by(|left, right| {
            (left.series, left.kind, left.encoded.codec_name()).cmp(&(
                right.series,
                right.kind,
                right.encoded.codec_name(),
            ))
        });

        Ok(FrozenHeadFragment {
            start_ms,
            end_ms,
            lane: if out_of_order {
                FrozenHeadLane::OutOfOrder
            } else {
                FrozenHeadLane::InOrder
            },
            datapoints,
            coverage_tracking,
            coverage,
            recorded_order_range,
            recorded_orders,
            publication_sequence: 0,
            runs: runs.into_boxed_slice(),
            arena,
        })
    }

    pub(super) fn empty_tail_like(&self, adaptive_series_table: bool) -> Self {
        Self::new_with_lane_and_coverage(
            self.start_ms,
            self.end_ms,
            adaptive_series_table,
            self.out_of_order,
            self.coverage_tracking,
        )
    }
}

/// A set of immutable head fragments in deterministic read precedence.
///
/// The constructor orders in-range fragments before OOO fragments for an
/// aligned range, and older publication sequences before newer sequences.
/// Stable last-write-wins merging therefore retains later publications and,
/// for equal timestamps, the OOO lane.
#[derive(Debug, Default)]
pub struct FrozenHeadReadView {
    samples: LiveSampleStore,
    #[cfg(any(test, feature = "test-hooks"))]
    decode_hook: Option<TestDecodeHook>,
}

#[cfg(any(test, feature = "test-hooks"))]
struct TestDecodeHook {
    callback: Arc<dyn Fn() + Send + Sync>,
    fired: std::sync::atomic::AtomicBool,
}

#[cfg(any(test, feature = "test-hooks"))]
impl fmt::Debug for TestDecodeHook {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TestDecodeHook(..)")
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl TestDecodeHook {
    fn run_once(&self) {
        if !self.fired.swap(true, std::sync::atomic::Ordering::AcqRel) {
            (self.callback)();
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
struct TestDecodeArena<'a> {
    arena: &'a FrozenBlockArena,
    hook: &'a TestDecodeHook,
}

#[cfg(any(test, feature = "test-hooks"))]
impl ArenaRead for TestDecodeArena<'_> {
    fn slice(&self, buf_ref: BufferRef) -> io::Result<&[u8]> {
        self.hook.run_once();
        self.arena.slice(buf_ref)
    }
}

enum FrozenSelectorIndex<'a> {
    Legacy(HeadSelectorIndex),
    Live {
        catalog: &'a LiveSeriesCatalog,
        present: Vec<SeriesRef>,
    },
}

impl FrozenSelectorIndex<'_> {
    fn matching_series(
        &self,
        matchers: &[NormalizedMatcher],
        budget: &mut QueryBudget,
        match_promql_projection_names: bool,
    ) -> io::Result<Vec<SeriesRef>> {
        match self {
            Self::Legacy(index) => {
                index.matching_series(matchers, budget, match_promql_projection_names)
            }
            Self::Live { catalog, present } => {
                catalog.matching_series(present, matchers, budget, match_promql_projection_names)
            }
        }
    }

    fn series(&self, series: SeriesRef) -> io::Result<Option<HeadIndexedSeries>> {
        match self {
            Self::Legacy(index) => Ok(index.series(&series).cloned()),
            Self::Live { catalog, .. } => {
                let Some(series_id) = catalog.series_id(series)? else {
                    return Ok(None);
                };
                Ok(Some(HeadIndexedSeries {
                    series_id,
                    labels: catalog.materialize_labels(series)?,
                }))
            }
        }
    }
}

impl FrozenHeadReadView {
    pub fn new(fragments: Vec<Arc<FrozenHeadFragment>>) -> Self {
        Self::try_new(fragments)
            .expect("valid frozen fragments must build a persistent compatibility view")
    }

    pub fn try_new(fragments: Vec<Arc<FrozenHeadFragment>>) -> io::Result<Self> {
        Ok(Self {
            samples: LiveSampleStore::compatibility(fragments)?,
            #[cfg(any(test, feature = "test-hooks"))]
            decode_hook: None,
        })
    }

    pub fn from_owned(fragments: Vec<FrozenHeadFragment>) -> Self {
        Self::new(fragments.into_iter().map(Arc::new).collect())
    }

    pub fn from_sample_store(samples: LiveSampleStore) -> Self {
        Self {
            samples,
            #[cfg(any(test, feature = "test-hooks"))]
            decode_hook: None,
        }
    }

    pub fn sample_store(&self) -> &LiveSampleStore {
        &self.samples
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn fragment_count(&self) -> usize {
        usize::try_from(self.samples.fragment_count()).unwrap_or(usize::MAX)
    }

    pub(crate) fn required_catalog_revision(&self) -> u64 {
        self.samples.required_catalog_revision()
    }

    /// Installs a one-shot callback at the first encoded arena read.
    ///
    /// This exists only for deterministic cross-crate concurrency tests.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_decode_hook_for_test(&mut self, hook: impl Fn() + Send + Sync + 'static) {
        self.decode_hook = Some(TestDecodeHook {
            callback: Arc::new(hook),
            fired: std::sync::atomic::AtomicBool::new(false),
        });
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn has_decode_hook_for_test(&self) -> bool {
        self.decode_hook.is_some()
    }

    pub fn query_selector<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::unlimited();
        self.query_selector_with_budget(labels, selector, start_ms, end_ms, &mut budget)
    }

    pub(crate) fn query_selector_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        self.query_selector_with_optional_catalog(labels, None, selector, start_ms, end_ms, budget)
    }

    pub(crate) fn query_selector_with_live_catalog_budget(
        &self,
        catalog: &LiveSeriesCatalog,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        self.query_selector_with_optional_catalog(
            catalog.labels().as_ref(),
            Some(catalog),
            selector,
            start_ms,
            end_ms,
            budget,
        )
    }

    fn query_selector_with_optional_catalog<R>(
        &self,
        labels: &R,
        catalog: Option<&LiveSeriesCatalog>,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let runs = self.samples.ordered_runs(start_ms, end_ms)?;
        let range_end_ms = end_ms.saturating_add(1);
        let matchers = selector.normalized_matchers();
        let projection = selector.projection();
        let index =
            self.selector_index(labels, catalog, &runs, start_ms, range_end_ms, |kind| {
                sample_kind_matches_projection(projection, kind)
            })?;
        let candidate_series = index.matching_series(
            &matchers,
            budget,
            projection_matches_promql_metric_name_regex(projection),
        )?;
        let projected_label_filter = match projection {
            SegmentProjection::AllPromql { .. } => Some(compile_label_matchers(&matchers)?),
            SegmentProjection::None
            | SegmentProjection::Count
            | SegmentProjection::Sum
            | SegmentProjection::HistogramBucket { .. }
            | SegmentProjection::NativeHistogram
            | SegmentProjection::NativeExponentialHistogram
            | SegmentProjection::SummaryQuantile { .. } => None,
        };
        let mut results = Vec::new();

        for series in candidate_series {
            let Some(indexed) = index.series(series)? else {
                continue;
            };
            let mut observed = false;
            for run_ref in &runs {
                if run_ref.key().series() != series {
                    continue;
                }
                let run = run_ref.run()?;
                if !sample_kind_matches_projection(projection, run.kind) {
                    continue;
                }
                if !observed {
                    budget.observe_matched_series(indexed.series_id)?;
                    observed = true;
                }

                #[cfg(any(test, feature = "test-hooks"))]
                let hooked_arena = self.decode_hook.as_ref().map(|hook| TestDecodeArena {
                    arena: &run_ref.fragment().arena,
                    hook,
                });
                #[cfg(any(test, feature = "test-hooks"))]
                let arena: &dyn ArenaRead = match hooked_arena.as_ref() {
                    Some(arena) => arena,
                    None => &run_ref.fragment().arena,
                };
                #[cfg(not(any(test, feature = "test-hooks")))]
                let arena = &run_ref.fragment().arena;
                let samples = run
                    .encoded
                    .samples_in_range(arena, start_ms, range_end_ms)?;
                match (projection, samples) {
                    (
                        SegmentProjection::None | SegmentProjection::AllPromql { .. },
                        SeriesSamples::Float { samples, .. },
                    ) => {
                        budget.observe_samples_decoded(samples.len() as u64)?;
                        if samples.is_empty()
                            || projected_label_filter.as_ref().is_some_and(|filter| {
                                !labels_match_compiled(&indexed.labels, filter)
                            })
                        {
                            continue;
                        }
                        results.push(SegmentQueryResult::with_samples(
                            indexed.series_id,
                            indexed.labels.clone(),
                            samples,
                        ));
                    }
                    (
                        SegmentProjection::None | SegmentProjection::AllPromql { .. },
                        SeriesSamples::Int64 { samples, .. },
                    ) => {
                        budget.observe_samples_decoded(samples.len() as u64)?;
                        if samples.is_empty()
                            || projected_label_filter.as_ref().is_some_and(|filter| {
                                !labels_match_compiled(&indexed.labels, filter)
                            })
                        {
                            continue;
                        }
                        results.push(SegmentQueryResult::with_samples(
                            indexed.series_id,
                            indexed.labels.clone(),
                            samples
                                .into_iter()
                                .map(|(timestamp_ms, value)| (timestamp_ms, value as f64))
                                .collect(),
                        ));
                    }
                    (SegmentProjection::None, _) => {}
                    (projection, samples) => {
                        let decoded_count = series_samples_len(&samples);
                        let mut projected = project_head_series_samples(
                            projection,
                            &indexed.labels,
                            samples,
                            start_ms,
                            end_ms,
                        )?;
                        budget.observe_samples_decoded(decoded_count as u64)?;
                        if let Some(filter) = &projected_label_filter {
                            projected.retain(|result| {
                                query_labels_match_compiled(&result.labels, filter)
                            });
                        }
                        results.append(&mut projected);
                    }
                }
            }
        }

        let results = merge_head_query_results(results);
        budget.observe_projected_results(&results)?;
        Ok(results)
    }

    #[allow(
        dead_code,
        reason = "called by the production live-query session integration slice"
    )]
    pub(crate) fn query_native_histogram_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlHistogramSeries>>
    where
        R: SeriesLabelResolver,
    {
        self.query_native_histogram_with_optional_catalog(
            labels, None, selector, start_ms, end_ms, budget,
        )
    }

    pub(crate) fn query_native_histogram_with_live_catalog_budget(
        &self,
        catalog: &LiveSeriesCatalog,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
        self.query_native_histogram_with_optional_catalog(
            catalog.labels().as_ref(),
            Some(catalog),
            selector,
            start_ms,
            end_ms,
            budget,
        )
    }

    fn query_native_histogram_with_optional_catalog<R>(
        &self,
        labels: &R,
        catalog: Option<&LiveSeriesCatalog>,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlHistogramSeries>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let runs = self.samples.ordered_runs(start_ms, end_ms)?;
        let range_end_ms = end_ms.saturating_add(1);
        let index =
            self.selector_index(labels, catalog, &runs, start_ms, range_end_ms, |kind| {
                kind == SampleKind::Histogram
            })?;
        let candidate_series =
            index.matching_series(&selector.normalized_matchers(), budget, false)?;
        let mut results = Vec::new();

        for series in candidate_series {
            let Some(indexed) = index.series(series)? else {
                continue;
            };
            let mut observed = false;
            for run_ref in &runs {
                if run_ref.key().series() != series {
                    continue;
                }
                let run = run_ref.run()?;
                if run.kind != SampleKind::Histogram {
                    continue;
                }
                if !observed {
                    budget.observe_matched_series(indexed.series_id)?;
                    observed = true;
                }

                let SeriesSamples::Histogram { samples } = run.encoded.samples_in_range(
                    &run_ref.fragment().arena,
                    start_ms,
                    range_end_ms,
                )?
                else {
                    continue;
                };
                budget.observe_samples_decoded(samples.len() as u64)?;
                if samples.is_empty() {
                    continue;
                }

                let mut result = PromqlHistogramSeries::new(
                    indexed.series_id,
                    shared_query_labels(indexed.labels.clone()),
                );
                for (timestamp_ms, value) in samples {
                    if timestamp_ms >= start_ms && timestamp_ms <= end_ms {
                        result.push_sample(PromqlHistogramSample::from_histogram_value(
                            timestamp_ms,
                            value,
                        ));
                    }
                }
                if !result.samples.is_empty() {
                    budget.observe_projected_series(result.series_id)?;
                    results.push(result);
                }
            }
        }

        Ok(merge_histogram_query_results(results))
    }

    #[allow(
        dead_code,
        reason = "called by the production live-query session integration slice"
    )]
    pub(crate) fn query_native_exponential_histogram_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>>
    where
        R: SeriesLabelResolver,
    {
        self.query_native_exponential_histogram_with_optional_catalog(
            labels, None, selector, start_ms, end_ms, budget,
        )
    }

    pub(crate) fn query_native_exponential_histogram_with_live_catalog_budget(
        &self,
        catalog: &LiveSeriesCatalog,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
        self.query_native_exponential_histogram_with_optional_catalog(
            catalog.labels().as_ref(),
            Some(catalog),
            selector,
            start_ms,
            end_ms,
            budget,
        )
    }

    fn query_native_exponential_histogram_with_optional_catalog<R>(
        &self,
        labels: &R,
        catalog: Option<&LiveSeriesCatalog>,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let runs = self.samples.ordered_runs(start_ms, end_ms)?;
        let range_end_ms = end_ms.saturating_add(1);
        let index =
            self.selector_index(labels, catalog, &runs, start_ms, range_end_ms, |kind| {
                kind == SampleKind::ExponentialHistogram
            })?;
        let candidate_series =
            index.matching_series(&selector.normalized_matchers(), budget, false)?;
        let mut results = Vec::new();

        for series in candidate_series {
            let Some(indexed) = index.series(series)? else {
                continue;
            };
            let mut observed = false;
            for run_ref in &runs {
                if run_ref.key().series() != series {
                    continue;
                }
                let run = run_ref.run()?;
                if run.kind != SampleKind::ExponentialHistogram {
                    continue;
                }
                if !observed {
                    budget.observe_matched_series(indexed.series_id)?;
                    observed = true;
                }

                let SeriesSamples::ExponentialHistogram { samples } = run
                    .encoded
                    .samples_in_range(&run_ref.fragment().arena, start_ms, range_end_ms)?
                else {
                    continue;
                };
                budget.observe_samples_decoded(samples.len() as u64)?;
                if samples.is_empty() {
                    continue;
                }

                let mut result = PromqlExponentialHistogramSeries::new(
                    indexed.series_id,
                    shared_query_labels(indexed.labels.clone()),
                );
                for (timestamp_ms, value) in samples {
                    if timestamp_ms >= start_ms && timestamp_ms <= end_ms {
                        result.push_sample(
                            PromqlExponentialHistogramSample::from_exponential_histogram_value(
                                timestamp_ms,
                                value,
                            ),
                        );
                    }
                }
                if !result.samples.is_empty() {
                    budget.observe_projected_series(result.series_id)?;
                    results.push(result);
                }
            }
        }

        Ok(merge_exponential_histogram_query_results(results))
    }

    pub fn metric_names<R>(&self, labels: &R, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names<R>(&self, labels: &R, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values<R>(
        &self,
        labels: &R,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        let label_name = if label_name == METRIC_NAME_LABEL {
            METRIC_NAME_LABEL.to_string()
        } else {
            crate::promql::normalize_label_name(label_name)
        };
        Ok(metadata.label_values(&label_name))
    }

    pub(crate) fn collect_metadata<R>(
        &self,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()>
    where
        R: SeriesLabelResolver,
    {
        self.collect_metadata_with_optional_catalog(labels, None, start_ms, end_ms, metadata)
    }

    pub(crate) fn collect_metadata_with_live_catalog(
        &self,
        catalog: &LiveSeriesCatalog,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        self.collect_metadata_with_optional_catalog(
            catalog.labels().as_ref(),
            Some(catalog),
            start_ms,
            end_ms,
            metadata,
        )
    }

    fn collect_metadata_with_optional_catalog<R>(
        &self,
        labels: &R,
        catalog: Option<&LiveSeriesCatalog>,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(());
        }

        let range_end_ms = end_ms.saturating_add(1);
        if let Some(catalog) = catalog {
            let runs = self.samples.ordered_runs(start_ms, end_ms)?;
            for series in self.present_series(&runs, start_ms, range_end_ms, |_| true)? {
                metadata.add_labelset(&catalog.materialize_labels(series)?);
            }
            return Ok(());
        }

        let mut seen = BTreeSet::new();
        for run_ref in self.samples.ordered_runs(start_ms, end_ms)? {
            let series = run_ref.key().series();
            if seen.contains(&series) {
                continue;
            }
            let run = run_ref.run()?;
            let samples =
                run.encoded
                    .samples_in_range(&run_ref.fragment().arena, start_ms, range_end_ms)?;
            if samples.is_empty() {
                continue;
            }
            seen.insert(series);
            let Some((_, canonical_labels)) = canonical_head_labelset(labels, series) else {
                continue;
            };
            metadata.add_labelset(&canonical_labels);
        }
        Ok(())
    }

    fn selector_index<'catalog, R, F>(
        &self,
        labels: &R,
        catalog: Option<&'catalog LiveSeriesCatalog>,
        runs: &[FrozenRunRef],
        start_ms: u64,
        end_ms: u64,
        kind_matches: F,
    ) -> io::Result<FrozenSelectorIndex<'catalog>>
    where
        R: SeriesLabelResolver,
        F: Fn(SampleKind) -> bool,
    {
        let present = self.present_series(runs, start_ms, end_ms, kind_matches)?;
        if let Some(catalog) = catalog {
            return Ok(FrozenSelectorIndex::Live { catalog, present });
        }
        Ok(FrozenSelectorIndex::Legacy(
            HeadSelectorIndex::build_from_series_refs(present, labels)?,
        ))
    }

    fn present_series<F>(
        &self,
        runs: &[FrozenRunRef],
        start_ms: u64,
        end_ms: u64,
        kind_matches: F,
    ) -> io::Result<Vec<SeriesRef>>
    where
        F: Fn(SampleKind) -> bool,
    {
        let mut present = BTreeSet::new();
        for run_ref in runs {
            let run = run_ref.run()?;
            if kind_matches(run.kind)
                && !run
                    .encoded
                    .samples_in_range(&run_ref.fragment().arena, start_ms, end_ms)?
                    .is_empty()
            {
                present.insert(run.series);
            }
        }
        Ok(present.into_iter().collect())
    }
}
