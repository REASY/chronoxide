use super::super::{
    Arc, ChunkIndexEntry, ChunkIndexRange, Duration, File, HashMap, MetadataGovernorConfig, Mutex,
    PathBuf, RangeScalarCacheSummary, RegisteredSegment, SegmentIndexReader, SegmentMeta,
    SegmentQuerySessionReader, SeriesEntry, SeriesEntryLocator, SeriesEntryMetadata,
    StoreMetadataRuntime,
};
use super::labels::{QueryLabelInterner, QueryLabels};
use super::limits::QueryProjectionConfig;
use super::profile::QueryStageProfile;
use super::selector::{
    QueryInstrumentationMode, QueryLabelMaterializationPolicy, RangeExecutionMode,
    RangeExecutionSummary,
};
use crate::storage::index::SegmentIndexReadStats;
use crate::storage::symbols::{SegmentSymbolReadStats, SegmentSymbolReader};

pub struct SegmentReader {
    pub(in crate::storage::segment) dir: PathBuf,
    pub(in crate::storage::segment) meta: SegmentMeta,
    pub(in crate::storage::segment) storage_schema_policy: SegmentStoreSchemaPolicy,
    pub(in crate::storage::segment) metadata_reader:
        super::super::metadata_facade::SegmentMetadataReader,
    pub(in crate::storage::segment) symbol_format: SegmentSymbolFormat,
    pub(in crate::storage::segment) query_cache: Arc<SegmentReaderQueryCache>,
    pub(in crate::storage::segment) registered_metadata: RegisteredSegment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::segment) enum SegmentSymbolFormat {
    PagedV3,
    #[allow(dead_code)] // Removed with the remaining schema-5 fingerprint adapter.
    LegacyV2ForLayoutAb,
}

#[derive(Default)]
pub(in crate::storage::segment) struct SegmentReaderQueryCache {
    pub(in crate::storage::segment) index_reader: Mutex<Option<SegmentIndexReader<File>>>,
    pub(in crate::storage::segment) symbols: Mutex<Option<SegmentSymbolReader<File>>>,
    pub(in crate::storage::segment) series_locators: Mutex<HashMap<u32, Arc<SeriesEntryLocator>>>,
    pub(in crate::storage::segment) series_metadata: Mutex<HashMap<u32, Arc<SeriesEntryMetadata>>>,
    pub(in crate::storage::segment) series_entries: Mutex<HashMap<u32, Arc<SeriesEntry>>>,
    pub(in crate::storage::segment) chunk_entries:
        Mutex<HashMap<ChunkIndexRange, Arc<Vec<ChunkIndexEntry>>>>,
}

pub(in crate::storage::segment) struct CachedIndexReader {
    pub(in crate::storage::segment) reader: SegmentIndexReader<File>,
    pub(in crate::storage::segment) cache_hit: bool,
    pub(in crate::storage::segment) file_bytes: u64,
    pub(in crate::storage::segment) open_elapsed: Duration,
    pub(in crate::storage::segment) open_read_stats: SegmentIndexReadStats,
}

pub(in crate::storage::segment) struct CachedSymbols {
    pub(in crate::storage::segment) symbols: Arc<SegmentSymbolReader<File>>,
    pub(in crate::storage::segment) cache_hit: bool,
    pub(in crate::storage::segment) file_bytes: u64,
    pub(in crate::storage::segment) open_elapsed: Duration,
    pub(in crate::storage::segment) open_read_stats: SegmentSymbolReadStats,
}

pub struct SegmentStoreReader {
    /// Readers remain time-ordered for segment discovery. Query precedence is
    /// a separate permutation so manifest-published stores can apply
    /// last-write-wins in authoritative manifest append order.
    pub(in crate::storage::segment) segments: Vec<SegmentReader>,
    pub(in crate::storage::segment) query_order: Vec<usize>,
    pub(in crate::storage::segment) query_projection_config: QueryProjectionConfig,
    pub(in crate::storage::segment) metadata_runtime: StoreMetadataRuntime,
}

/// Selects one exact sealed-segment schema for the complete store open.
///
/// This is an explicit whole-store policy. It never probes individual
/// segments to choose a reader, and a corpus containing any other schema is
/// rejected during footer preflight.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SegmentStoreSchemaPolicy {
    /// Prior-format schema-7 reader. Every segment must use footer schema 7.
    StrictSchema7,
    /// Production schema-8 reader using integrity-checked adaptive postings.
    #[default]
    StrictSchema8,
    /// Read-only schema-6 benchmark adapter with mandatory footer validation.
    ValidatedSchema6LayoutAb,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentStoreOpenOptions {
    pub validate_segment_footers: bool,
    /// Exact schema required for every segment in this store.
    pub storage_schema_policy: SegmentStoreSchemaPolicy,
    /// Aggregate metadata and file-descriptor limits, fixed before any segment opens.
    pub metadata_governor: MetadataGovernorConfig,
}

impl SegmentStoreOpenOptions {
    pub(in crate::storage::segment) fn requires_complete_footer_validation(
        self,
        policy: SegmentStoreSchemaPolicy,
    ) -> bool {
        self.validate_segment_footers
            || policy == SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb
    }
}

pub struct SegmentStoreQuerySession<'a> {
    pub(in crate::storage::segment) query_projection_config: QueryProjectionConfig,
    pub(in crate::storage::segment) segments: Vec<SegmentQuerySessionReader<'a>>,
    pub(in crate::storage::segment) label_cache: SeriesLabelCache,
    pub(in crate::storage::segment) projected_label_cache: ProjectedLabelCache,
    pub(in crate::storage::segment) range_scalar_cache_budget_bytes: u64,
    pub(in crate::storage::segment) range_scalar_cache_governor:
        Arc<super::super::range_scalar_cache::RangeScalarCacheGovernor>,
    pub(in crate::storage::segment) last_range_scalar_cache_summary:
        Option<RangeScalarCacheSummary>,
    pub(in crate::storage::segment) range_execution_mode: RangeExecutionMode,
    pub(in crate::storage::segment) last_range_execution_summary: Option<RangeExecutionSummary>,
    pub(in crate::storage::segment) experimental_cross_segment_chunk_reads: bool,
    pub(in crate::storage::segment) label_materialization_policy: QueryLabelMaterializationPolicy,
    pub(in crate::storage::segment) query_label_storage_policy_frozen: bool,
    pub(in crate::storage::segment) query_instrumentation_mode: QueryInstrumentationMode,
    pub(in crate::storage::segment) query_instrumentation_mode_frozen: bool,
    pub(in crate::storage::segment) label_interner: QueryLabelInterner,
    pub(in crate::storage::segment) query_stages: QueryStageProfile,
}

pub(in crate::storage::segment) type SeriesLabelCache = HashMap<u64, QueryLabels>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::storage::segment) struct ProjectedLabelCacheKey {
    pub(in crate::storage::segment) source_series_id: u64,
    pub(in crate::storage::segment) metric_suffix: &'static str,
}

#[derive(Debug)]
pub(in crate::storage::segment) struct ProjectedSeriesLabels {
    pub(in crate::storage::segment) series_id: u64,
    pub(in crate::storage::segment) labels: QueryLabels,
}

#[derive(Debug, Default)]
pub(in crate::storage::segment) struct ProjectedLabelCache {
    pub(in crate::storage::segment) entries:
        HashMap<ProjectedLabelCacheKey, Arc<ProjectedSeriesLabels>>,
    pub(in crate::storage::segment) hits: u64,
    pub(in crate::storage::segment) misses: u64,
}
