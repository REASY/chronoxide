use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::io;
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::{debug, warn};

use crate::labels::{LabelSetStore, METRIC_NAME_LABEL, SeriesRef};
use crate::promql::{canonicalize_labelset, series_id};
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
    MetadataAccumulator, NormalizedMatcher, QueryBudget, SegmentProjection, SegmentQueryResult,
    SegmentSelector, compile_label_matchers, compile_promql_regex, labels_match_compiled,
    projection_matches_promql_metric_name_regex, promql_projection_metric_name_matches,
    segment_series_id,
};

mod buffer;
mod encoded;
mod projection;
mod types;
mod window;

#[cfg(test)]
mod tests;

pub use buffer::*;
pub(crate) use encoded::*;
pub use projection::*;
pub use types::*;
pub use window::*;
