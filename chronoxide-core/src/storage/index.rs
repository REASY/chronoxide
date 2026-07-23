use std::collections::{BTreeMap, HashMap};
#[cfg(test)]
use std::io::SeekFrom;
use std::io::{self, Read, Seek, Write};

use fst::{IntoStreamer, Set, SetBuilder, Streamer};

use crate::labels::METRIC_NAME_LABEL;
use crate::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM, SERIES_KIND_INT64,
    SERIES_KIND_SUMMARY, SegmentSymbols, SeriesEntry,
};

mod read_at;
#[doc(hidden)]
pub use read_at::SegmentIndexReadAt;

mod model;
pub(in crate::storage) use model::ExactPostingsSelection;
pub(in crate::storage::index) use model::validate_metric_series_range_sequence;
pub use model::{
    ExactPostingsIndex, ExactPostingsMetadata, LabelValueFstIndex, LabelValueIndex,
    LabelValueTimeRange, LabelValueTimeRangeIndex, MetricSeriesRange, MetricSeriesRangeIndex,
    RoutingLookupResult, SegmentIndexReadCount, SegmentIndexReadStats, SegmentIndexes,
};

mod routing;
pub use routing::SegmentRoutingIndex;
pub(in crate::storage::index) use routing::{
    RoutingBucketKeyRange, RoutingBucketRecord, RoutingIndexHeader, routing_key_bytes,
    routing_key_hash, routing_key_hash_parts, routing_key_parts, validate_routing_bucket_key,
};

mod reader;
pub use reader::SegmentIndexReader;

mod standalone;
#[cfg(test)]
pub(in crate::storage::index) use standalone::write_exact_postings_blob;
pub use standalone::{
    read_exact_postings_index, read_label_value_fst_index, write_exact_postings_index,
    write_label_value_fst_index, write_label_value_time_range_index,
};

mod auxiliary;
#[cfg(test)]
pub(in crate::storage::index) use auxiliary::write_label_value_time_ranges_blob;
pub(in crate::storage::index) use auxiliary::{
    MetricSeriesRangeBlobBounds, MetricSeriesRangeBlobEvent, fst_io_error, read_fst_values,
    read_fst_values_with_prefix, read_label_value_fst_index_bytes,
    read_label_value_time_ranges_blob, read_metric_series_ranges_blob,
    walk_metric_series_ranges_blob, write_metric_series_ranges_blob,
};

mod v7;
pub(super) use v7::runtime::{
    GovernedSchema6BoundIndexRoot, GovernedSchema6ExactPostings,
    GovernedSchema6ExactPostingsSelection, GovernedSchema6IndexReader, GovernedSchema6IndexSession,
    Schema6IndexReaderError,
};
#[allow(dead_code)] // Made reachable only through the schema-7 same-seal validator.
mod v8;
pub(super) use v8::runtime::{
    GovernedSchema7BoundIndexRoot, GovernedSchema7ExactPostings,
    GovernedSchema7ExactPostingsSelection, GovernedSchema7IndexReader, GovernedSchema7IndexSession,
    Schema7IndexReaderError,
};

const EXACT_POSTINGS_MAGIC: u32 = u32::from_le_bytes(*b"PIDX");
const LABEL_VALUE_FST_MAGIC: u32 = u32::from_le_bytes(*b"LVIX");
const LABEL_VALUE_TIME_RANGE_MAGIC: u32 = u32::from_le_bytes(*b"LVTR");
const METRIC_SERIES_RANGES_MAGIC: u32 = u32::from_le_bytes(*b"MSRG");
const METRIC_SERIES_RANGES_VERSION: u16 = 1;
const METRIC_SERIES_RANGE_RECORD_LEN: usize = 28;
const VALID_METRIC_SERIES_KIND_MASK: u16 = (SERIES_KIND_FLOAT
    | SERIES_KIND_INT64
    | SERIES_KIND_HISTOGRAM
    | SERIES_KIND_EXPONENTIAL_HISTOGRAM
    | SERIES_KIND_SUMMARY) as u16;
const SEGMENT_INDEXES_MAGIC: u32 = u32::from_le_bytes(*b"SIDX");
#[cfg(test)]
const SEGMENT_INDEX_FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"SIDF");
const SEGMENT_INDEX_TRAILER_MAGIC: u32 = u32::from_le_bytes(*b"SIDT");
#[cfg(test)]
const SEGMENT_INDEX_VERSION: u16 = 6;
#[cfg(test)]
const SEGMENT_INDEX_HEADER_LEN: u64 = 8;
#[cfg(test)]
const SEGMENT_INDEX_TRAILER_LEN: u64 = 12;
const ROUTING_INDEX_MAGIC: u32 = u32::from_le_bytes(*b"RIDX");
const ROUTING_INDEX_VERSION: u16 = 2;
const ROUTING_INDEX_HEADER_LEN: usize = 40;
const ROUTING_INDEX_BUCKET_LEN: usize = 40;
#[cfg(test)]
const SEGMENT_INDEX_BLOB_EXACT_POSTINGS: u16 = 1;
const SEGMENT_INDEX_BLOB_LABEL_VALUE_FST: u16 = 2;
const SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES: u16 = 3;
#[cfg(test)]
const SEGMENT_INDEX_BLOB_ROUTING: u16 = 4;
#[cfg(test)]
const SEGMENT_INDEX_BLOB_METRIC_SERIES_RANGES: u16 = 5;
#[cfg(test)]
const NO_LABEL_VALUE_SYM: u32 = u32::MAX;

#[cfg(test)]
mod tests;

/// Test-fixture codec which deliberately omits cross-root validation.
///
/// Production segment writers must use [`write_segment_indexes_for_roots`].
#[cfg(test)]
pub(crate) fn write_segment_indexes_unbound_for_test(
    writer: impl Write,
    indexes: &SegmentIndexes,
) -> io::Result<()> {
    v7::write_segment_indexes_v7(writer, indexes)
}

/// Production writer entry point which proves every root-bound reference and
/// the derived routing map against the series and symbols emitted by the same
/// seal operation.
pub(crate) fn write_segment_indexes_for_roots(
    writer: impl Write,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
) -> io::Result<()> {
    indexes.validate_root_bounds(num_series, symbols)?;
    v7::write_segment_indexes_v7(writer, indexes)
}

#[cfg(test)]
pub(crate) fn write_segment_indexes_v8_for_roots_for_test(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
) -> io::Result<()> {
    v8::write_segment_indexes_v8_for_roots(writer, indexes, num_series, symbols, series)
}

pub(crate) fn write_segment_indexes_v8_for_roots(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
) -> io::Result<()> {
    v8::write_segment_indexes_v8_for_roots(writer, indexes, num_series, symbols, series)
}

pub(crate) fn write_segment_indexes_v9_for_roots(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    num_series: u32,
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
) -> io::Result<()> {
    v8::write_segment_indexes_v9_for_roots(writer, indexes, num_series, symbols, series)
}

#[cfg(test)]
pub(crate) fn write_segment_indexes_v8_unbound_for_test(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    series_count: u32,
    symbol_count: u32,
) -> io::Result<()> {
    v8::write_segment_indexes_v8_unbound_for_test(writer, indexes, series_count, symbol_count)
}

#[cfg(test)]
pub(crate) fn write_segment_indexes_v9_unbound_for_test(
    writer: impl Write + Seek,
    indexes: &SegmentIndexes,
    series_count: u32,
    symbol_count: u32,
) -> io::Result<()> {
    v8::write_segment_indexes_v9_unbound_for_test(writer, indexes, series_count, symbol_count)
}

#[cfg(test)]
pub(crate) fn corrupt_v8_exact_postings_payload_for_test(
    bytes: &mut [u8],
    label_name_sym: u32,
    label_value_sym: u32,
) -> io::Result<()> {
    v8::corrupt_exact_postings_payload_for_test(bytes, (label_name_sym, label_value_sym))
}

pub fn read_segment_indexes(mut reader: impl Read) -> io::Result<SegmentIndexes> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    SegmentIndexReader::open(std::io::Cursor::new(bytes))?.materialize()
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> io::Result<u16> {
    if cursor.saturating_add(2) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = u16::from_le_bytes(bytes[*cursor..*cursor + 2].try_into().unwrap());
    *cursor += 2;
    Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<u32> {
    if cursor.saturating_add(4) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    if cursor.saturating_add(8) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = u64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    if cursor.saturating_add(len) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let out = &bytes[*cursor..*cursor + len];
    *cursor += len;
    Ok(out)
}

fn read_bytes_at(bytes: &[u8], offset: u64, len: usize) -> io::Result<&[u8]> {
    let offset = usize::try_from(offset).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "byte slice offset exceeds platform usize",
        )
    })?;
    if offset.saturating_add(len) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    Ok(&bytes[offset..offset + len])
}
