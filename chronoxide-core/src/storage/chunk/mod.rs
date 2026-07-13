use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use crc32c::crc32c;

use crate::storage::encoding::{
    SchemaVarLenCodec, SchemaVarLenEncoding, decode_gorilla_values, decode_varint,
    decode_zigzag_i64, encode_gorilla_values, encode_varint, encode_zigzag_i64,
};
use crate::storage::head::{
    CounterResetHint, ExponentialHistogramValue, HistogramValue, OtlpAggregationTemporality,
    SummaryValue, TypedCounterValue, TypedSampleMetadata, decode_opt_f64, decode_typed_metadata,
};

mod codec;
mod index;
mod reader;
mod types;
mod writer;

#[cfg(test)]
mod tests;

pub use codec::*;
pub use index::*;
pub use reader::*;
pub use types::*;
pub use writer::*;
