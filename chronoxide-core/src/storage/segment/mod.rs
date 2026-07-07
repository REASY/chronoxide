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
    METRIC_NAME_LABEL, PromqlHistogramQuantile, PromqlMatcherOp, PromqlQuery, PromqlQueryError,
    PromqlRangeFunction, PromqlRangeFunctionKind, PromqlSelector, normalize_label_name,
    normalize_metric_name, parse_query,
};
use crate::storage::chunk::{
    ChunkIndexEntry, ChunkIndexRange, ChunkIndexReader, ChunkKind, ChunkRecord, ChunkSamples,
    ChunkScalarProjection, ChunkScalarProjectionRecord, ChunkScalarSample, ChunkScalarValue,
    ChunkWriter, chunk_index_ranges, read_chunk_index, read_chunk_indexed_scalar_projection_at,
    read_chunk_record_at, write_chunk_index,
};
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramValue, HeadBuffer, HistogramValue,
    OtlpAggregationTemporality, SeriesLabelResolver, SummaryValue, TypedSampleMetadata,
    exponential_histogram_projected_bucket_count, prometheus_stale_nan,
};
use crate::storage::index::{
    ExactPostingsIndex, ExactPostingsMetadata, LabelValueFstIndex, LabelValueTimeRangeIndex,
    SegmentIndexReader, SegmentIndexes, SegmentRoutingIndex, write_segment_indexes,
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

mod footer;
mod id;
mod layout;
mod promql_lowering;
mod query_context;
mod query_helpers;
mod query_promql;
mod query_reader;
mod query_store;
mod query_types;
mod writer;

#[cfg(test)]
mod tests;

use footer::*;
pub use id::*;
pub use layout::*;
use promql_lowering::*;
use query_context::*;
pub(crate) use query_helpers::*;
use query_promql::*;
pub use query_types::*;
pub use writer::*;
