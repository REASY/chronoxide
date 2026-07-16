use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::{debug, warn};

use crate::labels::{LabelSetStore, METRIC_NAME_LABEL, SeriesRef, SeriesRefHashMap};
use crate::promql::{canonicalize_labelset, format_promql_float_label, series_id};
use crate::storage::arena::BlockArena;
use crate::storage::block::{
    Block, BlockBuilder, BlockCodec, FloatAlpCodec, FloatAlpRdCodec, FloatAlpRdSpiralCodec,
    FloatAlpSpiralCodec, FloatChimp128BaselineDeferredCodec, FloatChimp128DuckDBDeferredCodec,
    FloatElfCodec, FloatGorillaCodec, FloatRawCodec, IntDeltaCodec, IntRawCodec,
};
use crate::storage::encoding::{
    SchemaVarLenCodec, SchemaVarLenEncoding, VarLenCodec, VarLenEncoding, decode_varint,
    decode_zigzag_i64, encode_varint, encode_zigzag_i64,
};
use crate::storage::segment::{
    BucketLeFilter, CompiledBucketLeFilter, CompiledLabelMatcher, MetadataAccumulator,
    NormalizedMatcher, PromqlExponentialHistogramSample, PromqlExponentialHistogramSeries,
    PromqlHistogramSample, PromqlHistogramSeries, QueryBudget, SegmentProjection,
    SegmentQueryResult, SegmentSelector, compile_bucket_le_filter, compile_label_matchers,
    compile_promql_regex, labels_match_compiled, merge_exponential_histogram_query_results,
    merge_histogram_query_results, projection_matches_promql_metric_name_regex,
    promql_projection_metric_name_matches, segment_series_id, shared_query_labels,
};

mod buffer;
mod encoded;
mod last_timestamps;
mod projection;
mod series_table;
mod types;
mod window;

#[cfg(test)]
mod tests;

pub use buffer::*;
pub(crate) use encoded::*;
use last_timestamps::LastTimestampTable;
#[cfg(test)]
use last_timestamps::{DENSE_PAGE_THRESHOLD, PAGE_LEN};
pub use projection::*;
use series_table::HeadSeriesTable;
pub use types::*;
pub use window::*;
