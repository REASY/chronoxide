use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
use crate::storage::chunk::{
    CHUNK_HEADER_LEN as CHUNK_FILE_HEADER_LEN, ChunkIndexEntry, ChunkIndexRange, ChunkIndexReader,
    ChunkKind, ChunkPayloadBatch, ChunkPayloadRead, ChunkRecord, ChunkSamples,
    ChunkScalarProjection, ChunkScalarSample, ChunkScalarValue, ChunkWriter,
    FRAME_HEADER_LEN as CHUNK_FRAME_HEADER_LEN, chunk_index_ranges, read_chunk_index,
    read_chunk_payload_batch_with_reader, read_chunk_record_at, write_chunk_index,
};
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HeadBuffer,
    HistogramValue, OtlpAggregationTemporality, SeriesLabelResolver, SummaryValue,
    TypedSampleMetadata, exponential_histogram_projected_bucket_count, prometheus_stale_nan,
};
use crate::storage::index::{
    ExactPostingsIndex, ExactPostingsMetadata, LabelValueFstIndex, LabelValueTimeRangeIndex,
    MetricSeriesRange, MetricSeriesRangeIndex, SegmentIndexReader, SegmentIndexes,
    SegmentRoutingIndex, write_segment_indexes,
};
use crate::storage::manifest::{
    ManifestInventory, ManifestRecord, ManifestSegment, ManifestWriter, read_current,
    read_manifest_inventory, write_current,
};
use crate::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM, SERIES_KIND_INT64,
    SERIES_KIND_SUMMARY, SegmentSymbols, SeriesEntry, SeriesEntryLocator, SeriesEntryMetadata,
    SeriesReader, read_series_bin, read_symbols_bin, write_series_bin, write_symbols_bin,
};

mod corpus_fingerprint;
mod footer;
mod id;
mod layout;
mod promql_lowering;
mod query_context;
mod query_fingerprint;
mod query_helpers;
mod query_promql;
mod query_reader;
mod query_store;
mod query_types;
mod range_scalar_cache;
mod writer;

#[cfg(test)]
mod range_scalar_cache_tests;

#[cfg(test)]
mod tests;

pub use corpus_fingerprint::*;
use footer::*;
pub use id::*;
pub use layout::*;
use promql_lowering::*;
use query_context::*;
pub use query_fingerprint::*;
pub(crate) use query_helpers::*;
use query_promql::*;
pub use query_types::*;
pub use range_scalar_cache::{
    DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES, DEFAULT_RANGE_SCALAR_CACHE_GOVERNOR_BYTES,
    MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES, RangeScalarCacheConfigError,
    RangeScalarCacheGovernorStats, RangeScalarCacheSummary, configure_range_scalar_cache_governor,
    range_scalar_cache_governor_stats, validate_range_scalar_cache_budget_bytes,
};
pub use writer::*;
