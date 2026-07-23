use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use crc32c::crc32c;

use crate::storage::encoding::{
    SchemaVarLenCodec, SchemaVarLenEncoding, decode_gorilla_values, decode_varint,
    decode_zigzag_i64, encode_gorilla_values, encode_varint, encode_zigzag_i64,
    minimum_gorilla_encoded_len_bytes,
};
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramValue, HistogramValue, OtlpAggregationTemporality,
    SummaryValue, TypedCounterValue, TypedSampleMetadata, decode_opt_f64, decode_typed_metadata,
};

mod codec;
mod index;
#[allow(dead_code)] // Wired into the schema-neutral metadata backend after the governed adapter.
mod index_v1_runtime;
#[allow(dead_code)] // The pure schema-7 codec lands before its reader/writer integration.
mod index_v2;
#[allow(dead_code)] // The pure locator foundation lands before reader/query integration.
mod indexed_locator;
mod reader;
#[allow(dead_code)] // The pure schema-7 codec lands before its reader/writer integration.
mod schema7_prefix;
mod types;
mod writer;

#[cfg(test)]
mod tests;

pub use codec::*;
pub use index::*;
#[allow(unused_imports)]
pub(crate) use index_v1_runtime::*;
#[allow(unused_imports)]
pub(crate) use index_v2::*;
#[allow(unused_imports)]
pub(crate) use indexed_locator::*;
pub use reader::*;
#[allow(unused_imports)]
// Used by the schema-7 reader/writer integration added after the codec.
pub(crate) use schema7_prefix::*;
pub use types::*;
pub use writer::*;
