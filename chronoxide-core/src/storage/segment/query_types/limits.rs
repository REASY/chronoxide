use super::super::{
    BTreeSet, ChunkKind, PromqlQueryError, SegmentChunkKindStats, SegmentChunkSummary, fmt, io,
};
use super::result::SegmentQueryResult;

#[derive(Debug, Clone, PartialEq)]
pub struct QueryExecution {
    pub results: Vec<SegmentQueryResult>,
    pub stats: QueryStats,
}

pub(in crate::storage::segment) fn ensure_query_result_labels_complete(
    results: &[SegmentQueryResult],
) -> Result<(), PromqlQueryError> {
    if results.iter().all(SegmentQueryResult::labels_are_complete) {
        Ok(())
    } else {
        Err(PromqlQueryError::Storage(
            "internal query invariant violated: incomplete labels escaped their terminal aggregation"
                .to_string(),
        ))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryDataPrefetchStats {
    pub query_stats: QueryStats,
    pub series_entries_read: u64,
    pub chunk_index_reads: u64,
    pub chunk_index_bytes_read: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryStats {
    pub segments_considered: u64,
    pub segments_skipped_by_time: u64,
    pub segments_skipped_by_missing_equality: u64,
    pub segments_skipped_by_matcher_time_range: u64,
    pub segments_queried: u64,
    pub matched_series: u64,
    pub projected_series: u64,
    pub chunk_reads: u64,
    pub bytes_read: u64,
    pub samples_decoded: u64,
    pub typed_scalar_chunks_decoded: u64,
    pub typed_full_chunks_decoded: u64,
    pub regex_values_examined: u64,
    pub index_postings_reads: u64,
    pub index_postings_bytes_read: u64,
}

impl QueryDataPrefetchStats {
    pub(crate) fn merge_from(&mut self, other: Self) {
        self.query_stats.merge_from(other.query_stats);
        self.series_entries_read = self
            .series_entries_read
            .saturating_add(other.series_entries_read);
        self.chunk_index_reads = self
            .chunk_index_reads
            .saturating_add(other.chunk_index_reads);
        self.chunk_index_bytes_read = self
            .chunk_index_bytes_read
            .saturating_add(other.chunk_index_bytes_read);
    }
}

impl QueryStats {
    pub(crate) fn merge_from(&mut self, other: Self) {
        self.segments_considered = self
            .segments_considered
            .saturating_add(other.segments_considered);
        self.segments_skipped_by_time = self
            .segments_skipped_by_time
            .saturating_add(other.segments_skipped_by_time);
        self.segments_skipped_by_missing_equality = self
            .segments_skipped_by_missing_equality
            .saturating_add(other.segments_skipped_by_missing_equality);
        self.segments_skipped_by_matcher_time_range = self
            .segments_skipped_by_matcher_time_range
            .saturating_add(other.segments_skipped_by_matcher_time_range);
        self.segments_queried = self.segments_queried.saturating_add(other.segments_queried);
        self.matched_series = self.matched_series.saturating_add(other.matched_series);
        self.projected_series = self.projected_series.saturating_add(other.projected_series);
        self.chunk_reads = self.chunk_reads.saturating_add(other.chunk_reads);
        self.bytes_read = self.bytes_read.saturating_add(other.bytes_read);
        self.samples_decoded = self.samples_decoded.saturating_add(other.samples_decoded);
        self.typed_scalar_chunks_decoded = self
            .typed_scalar_chunks_decoded
            .saturating_add(other.typed_scalar_chunks_decoded);
        self.typed_full_chunks_decoded = self
            .typed_full_chunks_decoded
            .saturating_add(other.typed_full_chunks_decoded);
        self.regex_values_examined = self
            .regex_values_examined
            .saturating_add(other.regex_values_examined);
        self.index_postings_reads = self
            .index_postings_reads
            .saturating_add(other.index_postings_reads);
        self.index_postings_bytes_read = self
            .index_postings_bytes_read
            .saturating_add(other.index_postings_bytes_read);
    }

    pub(crate) fn check_limits(self, limits: QueryLimits) -> Result<(), PromqlQueryError> {
        check_query_stat_limit(
            QueryLimit::MatchedSeries,
            self.matched_series,
            limits.max_matched_series,
        )?;
        check_query_stat_limit(
            QueryLimit::ProjectedSeries,
            self.projected_series,
            limits.max_projected_series,
        )?;
        check_query_stat_limit(
            QueryLimit::ChunkReads,
            self.chunk_reads,
            limits.max_chunk_reads,
        )?;
        check_query_stat_limit(
            QueryLimit::BytesRead,
            self.bytes_read,
            limits.max_bytes_read,
        )?;
        check_query_stat_limit(
            QueryLimit::SamplesDecoded,
            self.samples_decoded,
            limits.max_samples_decoded,
        )?;
        check_query_stat_limit(
            QueryLimit::RegexValuesExamined,
            self.regex_values_examined,
            limits.max_regex_values_examined,
        )
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryLimits {
    pub max_matched_series: Option<u64>,
    pub max_projected_series: Option<u64>,
    pub max_chunk_reads: Option<u64>,
    pub max_bytes_read: Option<u64>,
    pub max_samples_decoded: Option<u64>,
    pub max_regex_values_examined: Option<u64>,
}

pub const PRODUCTION_QUERY_MAX_SERIES_MATCHED: u64 = 1_000_000;
pub const PRODUCTION_QUERY_MAX_PROJECTED_SERIES: u64 = 2_000_000;
pub const PRODUCTION_QUERY_MAX_CHUNKS_READ: u64 = 5_000_000;
pub const PRODUCTION_QUERY_MAX_BYTES_READ: u64 = 2 * 1024 * 1024 * 1024;
pub const PRODUCTION_QUERY_MAX_SAMPLES: u64 = 50_000_000;
pub const PRODUCTION_REGEX_MAX_EXPANDED_VALUES: u64 = 100_000;

impl QueryLimits {
    pub const fn unlimited() -> Self {
        Self {
            max_matched_series: None,
            max_projected_series: None,
            max_chunk_reads: None,
            max_bytes_read: None,
            max_samples_decoded: None,
            max_regex_values_examined: None,
        }
    }

    pub const fn production_default() -> Self {
        Self {
            max_matched_series: Some(PRODUCTION_QUERY_MAX_SERIES_MATCHED),
            max_projected_series: Some(PRODUCTION_QUERY_MAX_PROJECTED_SERIES),
            max_chunk_reads: Some(PRODUCTION_QUERY_MAX_CHUNKS_READ),
            max_bytes_read: Some(PRODUCTION_QUERY_MAX_BYTES_READ),
            max_samples_decoded: Some(PRODUCTION_QUERY_MAX_SAMPLES),
            max_regex_values_examined: Some(PRODUCTION_REGEX_MAX_EXPANDED_VALUES),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeReport {
    pub totals: SegmentStoreSmokeTotals,
    pub sample_series: Vec<SegmentStoreSmokeSeries>,
    pub queries: Vec<SegmentStoreSmokeQuery>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeTotals {
    pub segments: u64,
    pub datapoints: u64,
    pub series: u64,
    pub chunks: u64,
    pub chunk_bytes: u64,
    pub by_kind: SegmentStoreSmokeKindTotals,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeKindTotals {
    pub float: SegmentStoreSmokeKindStats,
    pub int64: SegmentStoreSmokeKindStats,
    pub histogram: SegmentStoreSmokeKindStats,
    pub exponential_histogram: SegmentStoreSmokeKindStats,
    pub summary: SegmentStoreSmokeKindStats,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreSmokeKindStats {
    pub chunks: u64,
    pub chunk_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentStoreSmokeSeries {
    pub segment_id: String,
    pub series_ref: u32,
    pub series_id: u64,
    pub kind: ChunkKind,
    pub labels: Vec<(String, String)>,
    pub min_time_ms: u64,
    pub max_time_ms: u64,
    pub samples: u64,
    pub chunk_bytes: u64,
    pub bucket_le: Option<String>,
    pub quantile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentStoreSmokeQuery {
    pub kind: ChunkKind,
    pub query: String,
    pub result_series: u64,
    pub result_samples: u64,
    pub matched_series: u64,
    pub projected_series: u64,
    pub chunk_reads: u64,
    pub bytes_read: u64,
    pub samples_decoded: u64,
    pub typed_scalar_chunks_decoded: u64,
    pub typed_full_chunks_decoded: u64,
}

impl SegmentStoreSmokeKindTotals {
    pub(in crate::storage::segment) fn add_chunk(&mut self, kind: ChunkKind, bytes: u64) {
        let stats = self.stats_mut(kind);
        stats.chunks = stats.chunks.saturating_add(1);
        stats.chunk_bytes = stats.chunk_bytes.saturating_add(bytes);
    }

    pub(in crate::storage::segment) fn add_segment_stats(
        &mut self,
        kind: ChunkKind,
        stats: SegmentChunkKindStats,
    ) {
        let out = self.stats_mut(kind);
        out.chunks = out.chunks.saturating_add(stats.chunks);
        out.chunk_bytes = out.chunk_bytes.saturating_add(stats.chunk_bytes);
    }

    fn stats_mut(&mut self, kind: ChunkKind) -> &mut SegmentStoreSmokeKindStats {
        match kind {
            ChunkKind::Float => &mut self.float,
            ChunkKind::Int64 => &mut self.int64,
            ChunkKind::Histogram => &mut self.histogram,
            ChunkKind::ExponentialHistogram => &mut self.exponential_histogram,
            ChunkKind::Summary => &mut self.summary,
        }
    }
}

impl SegmentStoreSmokeTotals {
    pub(in crate::storage::segment) fn add_chunk_summary(&mut self, summary: &SegmentChunkSummary) {
        self.chunks = self.chunks.saturating_add(summary.chunks);
        self.chunk_bytes = self.chunk_bytes.saturating_add(summary.chunk_bytes);
        for kind in [
            ChunkKind::Float,
            ChunkKind::Int64,
            ChunkKind::Histogram,
            ChunkKind::ExponentialHistogram,
            ChunkKind::Summary,
        ] {
            self.by_kind
                .add_segment_stats(kind, summary.by_kind.stats(kind));
        }
    }
}

impl SegmentStoreSmokeReport {
    pub(in crate::storage::segment) fn sample_count_for_kind(&self, kind: ChunkKind) -> usize {
        self.sample_series
            .iter()
            .filter(|sample| sample.kind == kind)
            .count()
    }

    pub(in crate::storage::segment) fn sample_limits_reached_for_summary(
        &self,
        summary: &SegmentChunkSummary,
        sample_limit_per_kind: usize,
    ) -> bool {
        if sample_limit_per_kind == 0 {
            return true;
        }
        [
            ChunkKind::Float,
            ChunkKind::Int64,
            ChunkKind::Histogram,
            ChunkKind::ExponentialHistogram,
            ChunkKind::Summary,
        ]
        .into_iter()
        .all(|kind| {
            summary.by_kind.stats(kind).chunks == 0
                || self.sample_count_for_kind(kind) >= sample_limit_per_kind
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryProjectionConfig {
    exponential_histogram_bucket_boundaries: Vec<f64>,
}

impl QueryProjectionConfig {
    pub fn with_exponential_histogram_bucket_boundaries(
        mut self,
        mut boundaries: Vec<f64>,
    ) -> Self {
        assert!(
            boundaries.iter().all(|boundary| boundary.is_finite()),
            "exponential histogram projection boundaries must be finite"
        );
        boundaries.sort_by(f64::total_cmp);
        boundaries.dedup_by(|left, right| left.to_bits() == right.to_bits());
        self.exponential_histogram_bucket_boundaries = boundaries;
        self
    }

    pub(in crate::storage::segment) fn exponential_histogram_bucket_boundaries(&self) -> &[f64] {
        &self.exponential_histogram_bucket_boundaries
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryLimit {
    MatchedSeries,
    ProjectedSeries,
    ChunkReads,
    BytesRead,
    SamplesDecoded,
    RegexValuesExamined,
}

impl QueryLimit {
    fn as_str(self) -> &'static str {
        match self {
            Self::MatchedSeries => "matched_series",
            Self::ProjectedSeries => "projected_series",
            Self::ChunkReads => "chunk_reads",
            Self::BytesRead => "bytes_read",
            Self::SamplesDecoded => "samples_decoded",
            Self::RegexValuesExamined => "regex_values_examined",
        }
    }
}

fn check_query_stat_limit(
    limit: QueryLimit,
    value: u64,
    max: Option<u64>,
) -> Result<(), PromqlQueryError> {
    if let Some(max) = max
        && value > max
    {
        return Err(PromqlQueryError::LimitExceeded {
            limit: limit.as_str().to_string(),
            max,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimitExceeded {
    pub limit: QueryLimit,
    pub max: u64,
}

impl fmt::Display for QueryLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "query exceeded {} limit of {}",
            self.limit.as_str(),
            self.max
        )
    }
}

impl std::error::Error for QueryLimitExceeded {}

#[derive(Debug)]
pub(crate) struct QueryBudget {
    limits: QueryLimits,
    stats: QueryStats,
    seen_series: BTreeSet<u64>,
    seen_projected_series: BTreeSet<u64>,
}

impl QueryBudget {
    pub(crate) fn new(limits: QueryLimits) -> Self {
        Self {
            limits,
            stats: QueryStats::default(),
            seen_series: BTreeSet::new(),
            seen_projected_series: BTreeSet::new(),
        }
    }

    pub(crate) fn unlimited() -> Self {
        Self::new(QueryLimits::unlimited())
    }

    pub(crate) fn stats(&self) -> QueryStats {
        self.stats
    }

    pub(crate) fn observe_matched_series(&mut self, series_id: u64) -> io::Result<()> {
        if let Some(count) = observe_unique_series(
            &mut self.seen_series,
            series_id,
            self.stats.matched_series,
            QueryLimit::MatchedSeries,
            self.limits.max_matched_series,
        )? {
            self.stats.matched_series = count;
        }
        Ok(())
    }

    pub(crate) fn observe_projected_series(&mut self, series_id: u64) -> io::Result<()> {
        if let Some(count) = observe_unique_series(
            &mut self.seen_projected_series,
            series_id,
            self.stats.projected_series,
            QueryLimit::ProjectedSeries,
            self.limits.max_projected_series,
        )? {
            self.stats.projected_series = count;
        }
        Ok(())
    }

    pub(crate) fn observe_projected_results(
        &mut self,
        results: &[SegmentQueryResult],
    ) -> io::Result<()> {
        for result in results {
            self.observe_projected_series(result.series_id)?;
        }
        Ok(())
    }

    pub(crate) fn observe_candidate_series_refs(&mut self, count: u64) -> io::Result<()> {
        if let Some(max) = self.limits.max_matched_series
            && count > max
        {
            return Err(limit_exceeded_io(QueryLimitExceeded {
                limit: QueryLimit::MatchedSeries,
                max,
            }));
        }
        Ok(())
    }

    pub(crate) fn observe_chunk_read(&mut self, bytes: u64) -> io::Result<()> {
        self.stats.chunk_reads = self.checked_add(
            QueryLimit::ChunkReads,
            self.stats.chunk_reads,
            1,
            self.limits.max_chunk_reads,
        )?;
        self.stats.bytes_read = self.checked_add(
            QueryLimit::BytesRead,
            self.stats.bytes_read,
            bytes,
            self.limits.max_bytes_read,
        )?;
        Ok(())
    }

    pub(crate) fn observe_samples_decoded(&mut self, samples: u64) -> io::Result<()> {
        self.stats.samples_decoded = self.checked_add(
            QueryLimit::SamplesDecoded,
            self.stats.samples_decoded,
            samples,
            self.limits.max_samples_decoded,
        )?;
        Ok(())
    }

    pub(crate) fn observe_typed_scalar_chunk_decoded(&mut self) {
        self.stats.typed_scalar_chunks_decoded =
            self.stats.typed_scalar_chunks_decoded.saturating_add(1);
    }

    pub(crate) fn observe_typed_full_chunk_decoded(&mut self) {
        self.stats.typed_full_chunks_decoded =
            self.stats.typed_full_chunks_decoded.saturating_add(1);
    }

    pub(crate) fn observe_regex_value(&mut self) -> io::Result<()> {
        self.stats.regex_values_examined = self.checked_add(
            QueryLimit::RegexValuesExamined,
            self.stats.regex_values_examined,
            1,
            self.limits.max_regex_values_examined,
        )?;
        Ok(())
    }

    pub(crate) fn observe_index_postings_read(&mut self, bytes: u64) {
        self.stats.index_postings_reads = self.stats.index_postings_reads.saturating_add(1);
        self.stats.index_postings_bytes_read =
            self.stats.index_postings_bytes_read.saturating_add(bytes);
    }

    pub(crate) fn observe_segment_considered(&mut self) {
        self.stats.segments_considered = self.stats.segments_considered.saturating_add(1);
    }

    pub(crate) fn observe_segment_skipped_by_time(&mut self) {
        self.stats.segments_skipped_by_time = self.stats.segments_skipped_by_time.saturating_add(1);
    }

    pub(crate) fn observe_segment_skipped_by_missing_equality(&mut self) {
        self.stats.segments_skipped_by_missing_equality = self
            .stats
            .segments_skipped_by_missing_equality
            .saturating_add(1);
    }

    pub(crate) fn observe_segment_skipped_by_matcher_time_range(&mut self) {
        self.stats.segments_skipped_by_matcher_time_range = self
            .stats
            .segments_skipped_by_matcher_time_range
            .saturating_add(1);
    }

    pub(crate) fn observe_segment_queried(&mut self) {
        self.stats.segments_queried = self.stats.segments_queried.saturating_add(1);
    }

    fn checked_add(
        &self,
        limit: QueryLimit,
        current: u64,
        increment: u64,
        max: Option<u64>,
    ) -> io::Result<u64> {
        let next = current.saturating_add(increment);
        if let Some(max) = max
            && next > max
        {
            return Err(limit_exceeded_io(QueryLimitExceeded { limit, max }));
        }
        Ok(next)
    }
}

fn observe_unique_series(
    seen: &mut BTreeSet<u64>,
    series_id: u64,
    current: u64,
    limit: QueryLimit,
    max: Option<u64>,
) -> io::Result<Option<u64>> {
    if !seen.insert(series_id) {
        return Ok(None);
    }
    let next = current.saturating_add(1);
    if let Some(max) = max
        && next > max
    {
        return Err(limit_exceeded_io(QueryLimitExceeded { limit, max }));
    }
    Ok(Some(next))
}

pub(in crate::storage::segment) fn limit_exceeded_io(exceeded: QueryLimitExceeded) -> io::Error {
    io::Error::new(io::ErrorKind::QuotaExceeded, exceeded)
}

pub(in crate::storage::segment) fn query_limit_exceeded_from_io(
    err: &io::Error,
) -> Option<&QueryLimitExceeded> {
    err.get_ref()?.downcast_ref::<QueryLimitExceeded>()
}

pub(in crate::storage::segment) fn promql_error_from_query_io(err: io::Error) -> PromqlQueryError {
    if err.kind() == io::ErrorKind::QuotaExceeded
        && let Some(exceeded) = query_limit_exceeded_from_io(&err)
    {
        return PromqlQueryError::LimitExceeded {
            limit: exceeded.limit.as_str().to_string(),
            max: exceeded.max,
        };
    }

    if err.kind() == io::ErrorKind::InvalidData {
        let message = err.to_string();
        if message.contains("conflicting real and virtual PromQL series") {
            return PromqlQueryError::Invalid(message);
        }
    }

    PromqlQueryError::Storage(err.to_string())
}
