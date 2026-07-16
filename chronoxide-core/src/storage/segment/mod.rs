use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crc32c::{crc32c, crc32c_append};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::info;
use ulid::Ulid;

use crate::labels::{FlatInternedLabelSetStore, SeriesRef, SymbolId, SymbolTable};
use crate::promql::{
    METRIC_NAME_LABEL, PromqlAbsent, PromqlAbsentOverTime, PromqlAggregation,
    PromqlAggregationGrouping, PromqlAggregationOp, PromqlBinaryExpression, PromqlBinaryOp,
    PromqlDoubleExponentialSmoothing, PromqlHistogramFraction, PromqlHistogramQuantile,
    PromqlHistogramScalarFunction, PromqlHistogramScalarFunctionKind, PromqlMatcherOp,
    PromqlPredictLinear, PromqlQuantileOverTime, PromqlQuery, PromqlQueryError,
    PromqlRangeFunction, PromqlRangeFunctionKind, PromqlScalarFunction, PromqlSelector,
    PromqlVectorFunction, PromqlVectorMatching, PromqlVectorMatchingCardinality,
    PromqlVectorMatchingMode, format_promql_float_label, normalize_label_name,
    normalize_metric_name, parse_query,
};
#[cfg(test)]
use crate::storage::chunk::read_chunk_record_at;
use crate::storage::chunk::{
    CHUNK_HEADER_LEN as CHUNK_FILE_HEADER_LEN, ChunkIndexEntry, ChunkIndexRange, ChunkIndexReader,
    ChunkKind, ChunkPayloadBatch, ChunkPayloadBatchPlan, ChunkPayloadRead, ChunkRecord,
    ChunkSamples, ChunkScalarProjection, ChunkScalarSample, ChunkScalarValue, ChunkWriter,
    FRAME_HEADER_LEN as CHUNK_FRAME_HEADER_LEN, IndexedChunkLocator, chunk_index_ranges,
    plan_chunk_payload_batch, read_chunk_index, write_chunk_index,
};
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HeadBuffer,
    HistogramValue, OtlpAggregationTemporality, SeriesLabelResolver, SummaryValue,
    TypedCounterValue, TypedSampleMetadata, exponential_histogram_projected_bucket_count,
    prometheus_stale_nan,
};
#[cfg(test)]
use crate::storage::index::write_segment_indexes_unbound_for_test;
use crate::storage::index::{
    ExactPostingsIndex, ExactPostingsMetadata, LabelValueFstIndex, LabelValueTimeRangeIndex,
    MetricSeriesRangeIndex, SegmentIndexReader, SegmentIndexes, SegmentRoutingIndex,
    write_segment_indexes_for_roots, write_segment_indexes_v8_for_roots,
    write_segment_indexes_v9_for_roots,
};
use crate::storage::manifest::{
    ManifestInventory, ManifestRecord, ManifestSegment, ManifestWriter, read_current,
    read_manifest_inventory, write_current,
};
pub use crate::storage::metadata_governor::{
    MetadataGovernorConfig, MetadataGovernorConfigError, MetadataGovernorStats,
};
use crate::storage::metadata_runtime::{
    GovernedArtifactReader, RegisteredSegment, SegmentArtifactRegistration, StoreMetadataRuntime,
    StoreMetadataRuntimeError,
};
#[cfg(test)]
use crate::storage::series::read_symbols_bin;
use crate::storage::series::v3::{
    Schema7SeriesAssemblyInput, Schema7SeriesAssemblyStats, write_schema7_series_and_chunk_index,
};
use crate::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM, SERIES_KIND_INT64,
    SERIES_KIND_SUMMARY, SegmentSymbols, SeriesEntry, SeriesEntryLocator, SeriesEntryMetadata,
    SeriesReader, read_series_bin, write_series_bin, write_symbols_bin,
};
use crate::util::{XxHash64, xxhash64};

mod chunk_read_scheduler;
mod corpus_fingerprint;
mod footer;
mod full_validation;
mod id;
mod layout;
mod logical_replay_fingerprint;
#[allow(dead_code)] // Activated only after the schema-7 footer/query cutover.
pub(crate) mod metadata_facade;
mod promql_lowering;
mod query_context;
mod query_fingerprint;
mod query_helpers;
mod query_promql;
mod query_reader;
mod query_store;
mod query_types;
mod range_scalar_cache;
mod schema7_experiment;
mod writer;

#[cfg(test)]
mod range_scalar_cache_tests;

#[cfg(test)]
mod payload_routing_tests;

#[cfg(test)]
mod tests;

use chunk_read_scheduler::*;
pub use corpus_fingerprint::*;
use footer::*;
pub use id::*;
pub use layout::*;
pub use logical_replay_fingerprint::*;
use promql_lowering::*;
use query_context::*;
pub use query_fingerprint::*;
pub(crate) use query_helpers::*;
use query_promql::*;
use query_reader::open_metadata_runtime;
pub use query_types::*;
pub use range_scalar_cache::{
    DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES, DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES,
    MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES, RangeScalarCacheConfigError,
    RangeScalarCacheGovernorStats, RangeScalarCacheSummary, configure_range_scalar_cache_governor,
    range_scalar_cache_governor_stats, validate_range_scalar_cache_budget_bytes,
};
pub use schema7_experiment::*;
pub use writer::*;

#[cfg(test)]
fn schema6_test_open_options() -> SegmentStoreOpenOptions {
    SegmentStoreOpenOptions {
        storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
        ..SegmentStoreOpenOptions::default()
    }
}

#[cfg(test)]
fn open_schema6_segment_for_test(dir: impl AsRef<Path>) -> io::Result<SegmentReader> {
    let options = schema6_test_open_options();
    let metadata_runtime = open_metadata_runtime(options.metadata_governor)?;
    SegmentReader::open_footer_validated_with_options(dir, options, metadata_runtime, false)
}

#[cfg(test)]
fn open_schema6_store_for_test(dir: impl AsRef<Path>) -> io::Result<SegmentStoreReader> {
    SegmentStoreReader::open_with_options(dir, schema6_test_open_options())
}
