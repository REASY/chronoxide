mod labels;
mod limits;
mod profile;
mod result;
mod selector;
mod store;

pub(super) use labels::QueryLabelInterner;
pub use labels::{
    DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES, MAX_QUERY_LABEL_ARENA_BYTES, QueryLabelPairs,
    QueryLabelStoragePolicy, QueryLabelStorageStats, QueryLabels,
};
pub(crate) use labels::{query_labels_series_id, shared_query_labels};

pub(crate) use limits::QueryBudget;
pub use limits::{
    PRODUCTION_QUERY_MAX_BYTES_READ, PRODUCTION_QUERY_MAX_CHUNKS_READ,
    PRODUCTION_QUERY_MAX_PROJECTED_SERIES, PRODUCTION_QUERY_MAX_SAMPLES,
    PRODUCTION_QUERY_MAX_SERIES_MATCHED, PRODUCTION_REGEX_MAX_EXPANDED_VALUES,
    QueryDataPrefetchStats, QueryExecution, QueryLimit, QueryLimitExceeded, QueryLimits,
    QueryProjectionConfig, QueryStats, SegmentStoreSmokeKindStats, SegmentStoreSmokeKindTotals,
    SegmentStoreSmokeQuery, SegmentStoreSmokeReport, SegmentStoreSmokeSeries,
    SegmentStoreSmokeTotals,
};
pub(super) use limits::{ensure_query_result_labels_complete, promql_error_from_query_io};
#[allow(unused_imports)]
pub(super) use limits::{limit_exceeded_io, query_limit_exceeded_from_io};

pub use profile::{
    ChunkPayloadLocalityProfile, ChunkReadSchedulerProfile, QueryStageProfile,
    SegmentStoreQueryProfile, SegmentStoreQuerySessionStats, SegmentStoreSymbolResources,
};

#[allow(unused_imports)]
pub(crate) use result::PromqlExponentialHistogramBucketIter;
pub use result::SegmentQueryResult;
pub(crate) use result::{
    DeltaProjectionInterval, PromqlExponentialHistogramBuckets, PromqlExponentialHistogramSample,
    PromqlExponentialHistogramSeries, PromqlHistogramSample, PromqlHistogramSeries,
    QueryResultTemporality, merge_exponential_histogram_query_results,
    merge_histogram_query_results,
};

pub(crate) use selector::{
    BucketLeFilter, BucketLeMatcher, CompiledLabelMatcher, MetadataAccumulator, NormalizedMatcher,
    SegmentProjection, SegmentPruneReason,
};
pub use selector::{
    LabelMatcher, QueryInstrumentationMode, QueryLabelMaterializationPolicy,
    RangeExecutionFallbackReason, RangeExecutionMode, RangeExecutionSummary,
    RangeExecutionTerminalReason, SegmentSelector,
};
pub(super) use selector::{PROMQL_PROJECTION_SUFFIXES, QueryLabelDemand, ResolvedEqualityMatcher};

pub(super) use store::{
    CachedIndexReader, CachedSymbols, ProjectedLabelCache, ProjectedLabelCacheKey,
    ProjectedSeriesLabels, SegmentReaderQueryCache, SegmentSymbolFormat, SeriesLabelCache,
};
pub use store::{
    SegmentReader, SegmentStoreOpenOptions, SegmentStoreQuerySession, SegmentStoreReader,
    SegmentStoreSchemaPolicy,
};

#[cfg(test)]
#[path = "query_types/query_label_storage_tests.rs"]
mod query_label_storage_tests;

#[cfg(test)]
#[path = "query_types/index_read_profile_tests.rs"]
mod index_read_profile_tests;
