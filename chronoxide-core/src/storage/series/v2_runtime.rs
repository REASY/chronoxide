//! Governed read-only runtime for schema-6 `series.bin` v2.
//!
//! The legacy byte layout is retained only for the schema-6/schema-7 A/B
//! baseline. This adapter gives it the same aggregate cache, scratch-memory,
//! descriptor, lifecycle, corruption, and positional-I/O rules as schema 7.

use std::io;
use std::ops::{Deref, Range};

use thiserror::Error;

use crate::hash::XxHash64;
use crate::storage::chunk::{
    GovernedSchema6ChunkIndexRoot, GovernedSchema6ChunkIndexSession, Schema6ChunkIndexReaderError,
};
use crate::storage::metadata_cache::{
    LoadedMetadata, MetadataCacheError, MetadataCacheKey, MetadataCacheKeyError, MetadataCachePin,
};
use crate::storage::metadata_governor::{MetadataCacheClass, MetadataCharge, MetadataUsageClass};
use crate::storage::metadata_runtime::{
    GovernedArtifactReader, RegisteredSegment, SegmentGenerationProvenance, SegmentReadGuard,
    StoreMetadataRuntimeError,
};
use crate::storage::segment::SegmentFile;
use crate::storage::symbols::{GovernedSymbolReaderError, GovernedSymbolSession};

use super::cold_v2::reader as cold_v2_reader;
use super::{
    GovernedSeriesCountBinding, SERIES_HEADER_LEN, SERIES_KIND_EXPONENTIAL_HISTOGRAM,
    SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM, SERIES_KIND_INT64, SERIES_KIND_SUMMARY,
    SERIES_TABLE_ENTRY_LEN, SeriesEntry, SeriesHeader, SeriesTableEntryV2, decode_series_header_v2,
    decode_series_table_entry,
};

mod verified;
#[allow(unused_imports)] // Re-exported for the schema-neutral facade checkpoint.
pub(crate) use verified::{GovernedSchema6VerifiedSeriesBatch, Schema6VerifiedSeries};

const CHUNK_INDEX_HEADER_LEN_V1: u64 = 12;
const CHUNK_INDEX_ENTRY_LEN_V1: usize = 40;
const OFFSET_PAIR_LEN: u64 = 16;
const VALUE_DICT_HEADER_LEN: u64 = 8;
const KEYSET_BLOCK_HEADER_LEN: u64 = 16;
const VALUE_SYM_LEN: u64 = 4;
const VALID_KIND_MASK: u8 = SERIES_KIND_FLOAT
    | SERIES_KIND_INT64
    | SERIES_KIND_HISTOGRAM
    | SERIES_KIND_EXPONENTIAL_HISTOGRAM
    | SERIES_KIND_SUMMARY;

/// Failures at the governed schema-6 series boundary.
#[derive(Debug, Error)]
pub(crate) enum Schema6SeriesReaderError {
    #[error(transparent)]
    Runtime(#[from] StoreMetadataRuntimeError),
    #[error(transparent)]
    Cache(#[from] MetadataCacheError),
    #[error(transparent)]
    CacheKey(#[from] MetadataCacheKeyError),
    #[error("schema-6 series planning failed: {0}")]
    Planning(#[from] io::Error),
    #[error("schema-6 series value belongs to another segment generation")]
    ForeignSegmentGeneration,
    #[error("schema-6 series ref {series_ref} exceeds declared count {num_series}")]
    InvalidSeriesRef { series_ref: u32, num_series: u32 },
    #[error(transparent)]
    Symbols(#[from] GovernedSymbolReaderError),
    #[error(transparent)]
    ChunkIndex(#[from] Schema6ChunkIndexReaderError),
}

/// Immutable facts from the exact schema-6 series root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema6SeriesRootV2 {
    header: SeriesHeader,
    series_file_len: u64,
    chunk_index_file_len: u64,
    chunk_index_data_start: u64,
}

impl Schema6SeriesRootV2 {
    fn charged_bytes(self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }

    pub(crate) fn num_series(&self) -> u32 {
        self.header.num_series
    }
}

/// Long-lived generation owner with no guard, descriptor, root, or page pin.
pub(crate) struct GovernedSchema6SeriesReader {
    registered: RegisteredSegment,
    expected_num_series: u32,
    chunk_index_file_len: u64,
}

/// Query-scoped authorization for schema-6 series metadata.
pub(crate) struct GovernedSchema6SeriesSession {
    guard: SegmentReadGuard,
    expected_num_series: u32,
    chunk_index_file_len: u64,
}

/// Query-local pin for the independently cached fixed root.
#[derive(Debug)]
pub(crate) struct GovernedSchema6SeriesRoot {
    provenance: SegmentGenerationProvenance,
    value: MetadataCachePin<Schema6SeriesRootV2>,
}

impl Deref for GovernedSchema6SeriesRoot {
    type Target = Schema6SeriesRootV2;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

/// Ordered metadata-only query output. Duplicate requested refs remain
/// duplicate output records. Stable series identity is deliberately absent:
/// schema 6 cannot authenticate `series_id` without resolving the referenced
/// label row, so only chunk routing may be consumed before materialization.
#[derive(Debug)]
pub(crate) struct GovernedSchema6SeriesMetadata {
    provenance: SegmentGenerationProvenance,
    values: Vec<(u32, Schema6SeriesRoutingMetadata)>,
    _charge: MetadataCharge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema6SeriesRoutingMetadata {
    pub(crate) kind_mask: u8,
    pub(crate) chunk_index: crate::storage::chunk::ChunkIndexRange,
}

impl From<SeriesTableEntryV2> for Schema6SeriesRoutingMetadata {
    fn from(entry: SeriesTableEntryV2) -> Self {
        Self {
            kind_mask: entry.kind_mask,
            chunk_index: entry.chunk_index,
        }
    }
}

impl GovernedSchema6SeriesMetadata {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self._charge.bytes()
    }
}

/// Ordered fully materialized schema-6 entries with governed nested label
/// allocations.
#[derive(Debug)]
pub(crate) struct GovernedSchema6SeriesEntries {
    provenance: SegmentGenerationProvenance,
    values: Vec<(u32, SeriesEntry)>,
    _charge: MetadataCharge,
}

impl GovernedSchema6SeriesEntries {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self._charge.bytes()
    }
}

#[derive(Debug)]
struct ValidatedSeriesTableSpan {
    first_ref: u32,
    entries: Vec<SeriesTableEntryV2>,
}

impl ValidatedSeriesTableSpan {
    fn charged_bytes(&self) -> io::Result<u64> {
        checked_struct_and_vec_bytes::<Self, SeriesTableEntryV2>(self.entries.capacity())
    }

    fn entry(&self, series_ref: u32) -> Option<SeriesTableEntryV2> {
        let index = series_ref.checked_sub(self.first_ref)? as usize;
        self.entries.get(index).copied()
    }
}

#[derive(Debug, Clone, Copy)]
struct TableSpanPlan {
    first_ref: u32,
    entry_count: usize,
    offset: u64,
    length: u64,
}

#[derive(Debug)]
struct LoadedTableSpan {
    first_ref: u32,
    entry_count: usize,
    value: MetadataCachePin<ValidatedSeriesTableSpan>,
}

#[derive(Debug)]
struct TableWork {
    valid_refs: Vec<u32>,
    plans: Vec<TableSpanPlan>,
    loaded: Vec<LoadedTableSpan>,
}

impl TableWork {
    fn temporary_bytes(&self) -> io::Result<u64> {
        checked_vec_bytes::<u32>(
            self.valid_refs.capacity(),
            "schema-6 valid-ref charge overflows",
        )?
        .checked_add(checked_vec_bytes::<TableSpanPlan>(
            self.plans.capacity(),
            "schema-6 table-plan charge overflows",
        )?)
        .and_then(|bytes| {
            checked_vec_bytes::<LoadedTableSpan>(
                self.loaded.capacity(),
                "schema-6 table-pin charge overflows",
            )
            .ok()
            .and_then(|pins| bytes.checked_add(pins))
        })
        .ok_or_else(|| invalid_input("schema-6 table working-set charge overflows"))
    }

    fn entry(&self, series_ref: u32) -> io::Result<SeriesTableEntryV2> {
        let span_index = self.loaded.partition_point(|span| {
            u64::from(span.first_ref) + span.entry_count as u64 <= u64::from(series_ref)
        });
        let span = self
            .loaded
            .get(span_index)
            .ok_or_else(|| invalid_data("schema-6 requested series table entry is missing"))?;
        span.value.entry(series_ref).ok_or_else(|| {
            invalid_data("schema-6 requested series table entry is outside its span")
        })
    }
}

#[derive(Debug)]
struct ValidatedColdEntryRange {
    start: u64,
    end: u64,
}

impl ValidatedColdEntryRange {
    fn as_range(&self) -> Range<u64> {
        self.start..self.end
    }
}

#[derive(Debug)]
struct ValidatedKeyset {
    keys: Vec<u32>,
}

impl ValidatedKeyset {
    fn charged_bytes(&self) -> io::Result<u64> {
        checked_struct_and_vec_bytes::<Self, u32>(self.keys.capacity())
    }
}

#[derive(Debug)]
struct ValidatedBlockFixed {
    bytes: [u8; KEYSET_BLOCK_HEADER_LEN as usize],
}

#[derive(Debug)]
struct ValidatedBlockMeta {
    value: cold_v2_reader::KeySetBlockMeta,
}

impl ValidatedBlockMeta {
    fn charged_bytes(&self) -> io::Result<u64> {
        checked_struct_and_vec_bytes::<Self, u8>(self.value.widths.capacity())
    }
}

#[derive(Debug)]
enum BlockMetaSource {
    EmptyWidths(cold_v2_reader::KeySetBlockMeta),
    Cached(MetadataCachePin<ValidatedBlockMeta>),
}

#[derive(Debug)]
struct LoadedBlockMeta {
    _fixed: MetadataCachePin<ValidatedBlockFixed>,
    source: BlockMetaSource,
}

impl LoadedBlockMeta {
    fn value(&self) -> &cold_v2_reader::KeySetBlockMeta {
        match &self.source {
            BlockMetaSource::EmptyWidths(value) => value,
            BlockMetaSource::Cached(value) => &value.value,
        }
    }
}

#[derive(Debug)]
struct ValidatedRow {
    bytes: Vec<u8>,
}

impl ValidatedRow {
    fn charged_bytes(&self) -> io::Result<u64> {
        checked_struct_and_vec_bytes::<Self, u8>(self.bytes.capacity())
    }
}

#[derive(Debug, Clone, Copy)]
struct ValidatedValueDictMeta {
    value: cold_v2_reader::ValueDictMeta,
}

#[derive(Debug, Clone, Copy)]
struct ValidatedValueSym(u32);

#[derive(Clone, Copy)]
struct ColdSection {
    offset: u64,
    end: u64,
    count: u32,
}

impl GovernedSchema6SeriesReader {
    /// Opens and validates the exact fixed root, then releases every
    /// query-scoped object before returning the long-lived owner.
    pub(crate) fn open(
        registered: &RegisteredSegment,
        expected_num_series: u32,
    ) -> Result<Self, Schema6SeriesReaderError> {
        let guard = registered.read_guard()?;
        let chunk_index_file_len = guard.reader(SegmentFile::ChunkIndex)?.len();
        let session = GovernedSchema6SeriesSession {
            guard,
            expected_num_series,
            chunk_index_file_len,
        };
        let root = session.load_root()?;
        drop(root);
        drop(session);
        Ok(Self {
            registered: registered.clone(),
            expected_num_series,
            chunk_index_file_len,
        })
    }

    pub(crate) fn query_session(
        &self,
    ) -> Result<GovernedSchema6SeriesSession, Schema6SeriesReaderError> {
        Ok(GovernedSchema6SeriesSession {
            guard: self.registered.read_guard()?,
            expected_num_series: self.expected_num_series,
            chunk_index_file_len: self.chunk_index_file_len,
        })
    }

    pub(crate) fn segment_identity(&self) -> &str {
        self.registered.segment_identity()
    }
}

impl GovernedSchema6SeriesSession {
    pub(crate) fn load_root(&self) -> Result<GovernedSchema6SeriesRoot, Schema6SeriesReaderError> {
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = metadata_key(
            &reader,
            0,
            SERIES_HEADER_LEN,
            MetadataCacheClass::SeriesRoot,
        )?;
        let series_file_len = reader.len();
        let chunk_index_file_len = self.chunk_index_file_len;
        let value = reader.get_or_load(
            key,
            std::mem::size_of::<Schema6SeriesRootV2>() as u64,
            move |bytes| {
                let root =
                    decode_schema6_series_root_v2(bytes, series_file_len, chunk_index_file_len)
                        .map_err(MetadataCacheError::from_io)?;
                Ok(LoadedMetadata::new(root, root.charged_bytes()))
            },
        )?;
        if value.header.num_series != self.expected_num_series {
            let actual = value.header.num_series;
            drop(value);
            return Err(reader
                .record_validation_error(invalid_data(format!(
                    "schema-6 series count mismatch: expected={} actual={actual}",
                    self.expected_num_series
                )))
                .into());
        }
        if value.chunk_index_file_len != self.chunk_index_file_len {
            drop(value);
            return Err(reader
                .record_validation_error(invalid_data(
                    "schema-6 cached series root has a foreign chunk-index length",
                ))
                .into());
        }
        Ok(GovernedSchema6SeriesRoot {
            provenance: self.guard.provenance(),
            value,
        })
    }

    /// Mints the only series-count capability accepted by shared governed
    /// indexes. The count cannot be detached from the validated root's segment
    /// generation or supplied as a bare caller-controlled integer.
    pub(crate) fn series_count_binding(
        &self,
        root: &GovernedSchema6SeriesRoot,
    ) -> Result<GovernedSeriesCountBinding, Schema6SeriesReaderError> {
        self.ensure_provenance(&root.provenance)?;
        Ok(GovernedSeriesCountBinding::new(
            self.guard.provenance(),
            root.num_series(),
        ))
    }

    pub(crate) fn read_metadata_entries(
        &self,
        root: &GovernedSchema6SeriesRoot,
        chunk_index: &GovernedSchema6ChunkIndexSession,
        chunk_index_root: &GovernedSchema6ChunkIndexRoot,
        series_refs: &[u32],
    ) -> Result<GovernedSchema6SeriesMetadata, Schema6SeriesReaderError> {
        self.ensure_provenance(&root.provenance)?;
        chunk_index.ensure_same_generation(&self.guard)?;
        chunk_index.bind_series_count(chunk_index_root, root.num_series())?;
        self.validate_series_refs(root, series_refs)?;
        let valid_count = series_refs.len();
        let declared =
            checked_table_work_upper::<(u32, Schema6SeriesRoutingMetadata)>(valid_count)?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let mut charge = reader
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(declared, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)?;
        let mut work = self.prepare_table_work(root, series_refs)?;
        let mut values = try_vec_with_capacity(valid_count, "schema-6 metadata output")?;
        let final_bytes = checked_vec_bytes::<(u32, Schema6SeriesRoutingMetadata)>(
            values.capacity(),
            "schema-6 metadata output charge overflows",
        )?;
        charge
            .reconcile(
                work.temporary_bytes()?
                    .checked_add(final_bytes)
                    .ok_or_else(|| {
                        invalid_input("schema-6 metadata working-set charge overflows")
                    })?,
            )
            .map_err(MetadataCacheError::from)?;
        self.load_table_spans(root, &mut work)?;
        for series_ref in series_refs.iter().copied() {
            let entry = self.record_series_result(work.entry(series_ref))?;
            chunk_index.validate_series_range(chunk_index_root, series_ref, entry.chunk_index)?;
            values.push((series_ref, Schema6SeriesRoutingMetadata::from(entry)));
        }
        drop(work);
        charge
            .reconcile(final_bytes)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedSchema6SeriesMetadata {
            provenance: self.guard.provenance(),
            values,
            _charge: charge,
        })
    }

    pub(crate) fn read_entries(
        &self,
        root: &GovernedSchema6SeriesRoot,
        chunk_index: &GovernedSchema6ChunkIndexSession,
        chunk_index_root: &GovernedSchema6ChunkIndexRoot,
        symbols: &GovernedSymbolSession,
        series_refs: &[u32],
    ) -> Result<GovernedSchema6SeriesEntries, Schema6SeriesReaderError> {
        self.ensure_provenance(&root.provenance)?;
        chunk_index.ensure_same_generation(&self.guard)?;
        chunk_index.bind_series_count(chunk_index_root, root.num_series())?;
        symbols.ensure_same_generation(&self.guard)?;
        self.validate_series_refs(root, series_refs)?;
        let valid_count = series_refs.len();
        let declared = checked_table_work_upper::<(u32, SeriesEntry)>(valid_count)?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let mut charge = reader
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(declared, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)?;
        let mut work = self.prepare_table_work(root, series_refs)?;
        let mut values = try_vec_with_capacity(valid_count, "schema-6 series output")?;
        let output_vec_bytes = checked_vec_bytes::<(u32, SeriesEntry)>(
            values.capacity(),
            "schema-6 series output charge overflows",
        )?;
        let temporary_bytes = work.temporary_bytes()?;
        let mut label_bytes = 0u64;
        charge
            .reconcile(
                temporary_bytes
                    .checked_add(output_vec_bytes)
                    .ok_or_else(|| invalid_input("schema-6 series working-set charge overflows"))?,
            )
            .map_err(MetadataCacheError::from)?;
        self.load_table_spans(root, &mut work)?;

        for series_ref in series_refs.iter().copied() {
            let table_entry = self.record_series_result(work.entry(series_ref))?;
            chunk_index.validate_series_range(
                chunk_index_root,
                series_ref,
                table_entry.chunk_index,
            )?;
            let entry = self.materialize_entry(
                root,
                symbols,
                table_entry,
                &mut charge,
                temporary_bytes
                    .checked_add(output_vec_bytes)
                    .and_then(|bytes| bytes.checked_add(label_bytes))
                    .ok_or_else(|| invalid_input("schema-6 series working-set charge overflows"))?,
            )?;
            label_bytes = label_bytes
                .checked_add(checked_vec_bytes::<(u32, u32)>(
                    entry.labels.capacity(),
                    "schema-6 materialized-label charge overflows",
                )?)
                .ok_or_else(|| invalid_input("schema-6 materialized-label charge overflows"))?;
            values.push((series_ref, entry));
        }

        let final_bytes = output_vec_bytes
            .checked_add(label_bytes)
            .ok_or_else(|| invalid_input("schema-6 series output charge overflows"))?;
        drop(work);
        charge
            .reconcile(final_bytes)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedSchema6SeriesEntries {
            provenance: self.guard.provenance(),
            values,
            _charge: charge,
        })
    }

    pub(crate) fn routing_entries<'a>(
        &'a self,
        values: &'a GovernedSchema6SeriesMetadata,
    ) -> Result<&'a [(u32, Schema6SeriesRoutingMetadata)], Schema6SeriesReaderError> {
        self.ensure_provenance(&values.provenance)?;
        Ok(&values.values)
    }

    pub(crate) fn entries<'a>(
        &'a self,
        values: &'a GovernedSchema6SeriesEntries,
    ) -> Result<&'a [(u32, SeriesEntry)], Schema6SeriesReaderError> {
        self.ensure_provenance(&values.provenance)?;
        Ok(&values.values)
    }

    fn prepare_table_work(
        &self,
        root: &GovernedSchema6SeriesRoot,
        series_refs: &[u32],
    ) -> Result<TableWork, Schema6SeriesReaderError> {
        let valid_count = series_refs.len();
        let mut valid_refs = try_vec_with_capacity(valid_count, "schema-6 valid refs")?;
        valid_refs.extend(series_refs.iter().copied());
        valid_refs.sort_unstable();
        valid_refs.dedup();

        let mut plans = try_vec_with_capacity(valid_refs.len(), "schema-6 table plans")?;
        let mut start = 0usize;
        while start < valid_refs.len() {
            let first_ref = valid_refs[start];
            let mut end = start + 1;
            while end < valid_refs.len() && valid_refs[end] == valid_refs[end - 1].saturating_add(1)
            {
                end += 1;
            }
            let entry_count = end - start;
            let length = checked_len_mul(
                entry_count,
                SERIES_TABLE_ENTRY_LEN as usize,
                "schema-6 table span length overflows",
            )?;
            let offset = root
                .header
                .series_table_offset
                .checked_add(
                    u64::from(first_ref)
                        .checked_mul(SERIES_TABLE_ENTRY_LEN)
                        .ok_or_else(|| invalid_data("schema-6 table span offset overflows"))?,
                )
                .ok_or_else(|| invalid_data("schema-6 table span offset overflows"))?;
            plans.push(TableSpanPlan {
                first_ref,
                entry_count,
                offset,
                length,
            });
            start = end;
        }
        let loaded = try_vec_with_capacity(plans.len(), "schema-6 table pins")?;
        Ok(TableWork {
            valid_refs,
            plans,
            loaded,
        })
    }

    fn load_table_spans(
        &self,
        root: &GovernedSchema6SeriesRoot,
        work: &mut TableWork,
    ) -> Result<(), Schema6SeriesReaderError> {
        let reader = self.guard.reader(SegmentFile::Series)?;
        for plan in &work.plans {
            let key = metadata_key(
                &reader,
                plan.offset,
                plan.length,
                MetadataCacheClass::SeriesHotPage,
            )?;
            let declared = checked_struct_and_vec_bytes::<
                ValidatedSeriesTableSpan,
                SeriesTableEntryV2,
            >(plan.entry_count)
            .map_err(MetadataCacheError::from_io)?;
            let header = root.header;
            let chunk_index_data_start = root.chunk_index_data_start;
            let chunk_index_file_len = root.chunk_index_file_len;
            let first_ref = plan.first_ref;
            let entry_count = plan.entry_count;
            let value = reader.get_or_load_owned(key, declared, move |bytes| {
                let value = decode_series_table_span(
                    bytes,
                    first_ref,
                    entry_count,
                    header,
                    chunk_index_data_start,
                    chunk_index_file_len,
                )
                .map_err(MetadataCacheError::from_io)?;
                let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
                Ok(LoadedMetadata::new(value, charged))
            })?;
            if value.first_ref != plan.first_ref || value.entries.len() != plan.entry_count {
                return Err(reader
                    .record_validation_error(invalid_data(
                        "schema-6 cached table span does not match its physical range",
                    ))
                    .into());
            }
            work.loaded.push(LoadedTableSpan {
                first_ref: plan.first_ref,
                entry_count: plan.entry_count,
                value,
            });
        }
        Ok(())
    }

    fn materialize_entry(
        &self,
        root: &GovernedSchema6SeriesRoot,
        symbols: &GovernedSymbolSession,
        table_entry: SeriesTableEntryV2,
        charge: &mut MetadataCharge,
        already_charged_bytes: u64,
    ) -> Result<SeriesEntry, Schema6SeriesReaderError> {
        let labels = self.decode_label_ids(root, table_entry, charge, already_charged_bytes)?;
        self.verify_series_identity(symbols, table_entry.series_id, &labels)?;
        Ok(SeriesEntry {
            series_id: table_entry.series_id,
            kind_mask: table_entry.kind_mask,
            chunk_index: table_entry.chunk_index,
            labels,
        })
    }

    fn decode_label_ids(
        &self,
        root: &GovernedSchema6SeriesRoot,
        table_entry: SeriesTableEntryV2,
        charge: &mut MetadataCharge,
        already_charged_bytes: u64,
    ) -> Result<Vec<(u32, u32)>, Schema6SeriesReaderError> {
        let keyset = self.load_keyset(root, table_entry.keyset_id)?;
        let declared_labels = checked_vec_bytes::<(u32, u32)>(
            keyset.keys.len(),
            "schema-6 label allocation charge overflows",
        )?;
        charge
            .reconcile(
                already_charged_bytes
                    .checked_add(declared_labels)
                    .ok_or_else(|| invalid_input("schema-6 label allocation charge overflows"))?,
            )
            .map_err(MetadataCacheError::from)?;
        let mut labels = try_vec_with_capacity(keyset.keys.len(), "schema-6 labels")?;
        let actual_labels = checked_vec_bytes::<(u32, u32)>(
            labels.capacity(),
            "schema-6 label allocation charge overflows",
        )?;
        charge
            .reconcile(
                already_charged_bytes
                    .checked_add(actual_labels)
                    .ok_or_else(|| invalid_input("schema-6 label allocation charge overflows"))?,
            )
            .map_err(MetadataCacheError::from)?;

        let block = self.load_keyset_block(root, table_entry.keyset_id)?;
        self.record_series_result(cold_v2_reader::validate_keyset_block_key_count(
            block.value(),
            keyset.keys.len(),
        ))?;
        let row = if block.value().row_len_bytes == 0 {
            if table_entry.row >= block.value().rows {
                return Err(
                    self.record_series_error(invalid_data("schema-6 series row is out of bounds"))
                );
            }
            None
        } else {
            Some(self.load_row(table_entry.row, block.value())?)
        };
        let row_bytes = row.as_ref().map_or(&[][..], |row| row.bytes.as_slice());
        let mut cursor = 0usize;
        for (index, key_sym) in keyset.keys.iter().copied().enumerate() {
            let dict = self.find_value_dict(root, key_sym)?;
            let width = *block.value().widths.get(index).ok_or_else(|| {
                self.record_series_error(invalid_data("schema-6 keyset block width is missing"))
            })?;
            self.record_series_result(cold_v2_reader::validate_value_code_width(
                width,
                dict.cardinality,
            ))?;
            let code = self.record_series_result(cold_v2_reader::read_value_code(
                row_bytes,
                &mut cursor,
                width,
            ))?;
            let value_sym = self.load_value_sym(dict, code)?.0;
            labels.push((key_sym, value_sym));
        }
        if cursor != row_bytes.len() {
            return Err(
                self.record_series_error(invalid_data("schema-6 series row has trailing bytes"))
            );
        }
        Ok(labels)
    }

    fn load_keyset(
        &self,
        root: &GovernedSchema6SeriesRoot,
        keyset_id: u32,
    ) -> Result<MetadataCachePin<ValidatedKeyset>, Schema6SeriesReaderError> {
        let section = ColdSection {
            offset: root.header.keysets_offset,
            end: root.header.value_dicts_offset,
            count: root.header.num_keysets,
        };
        let range = self.load_cold_entry_range(section, keyset_id)?;
        let range = range.as_range();
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = metadata_key(
            &reader,
            range.start,
            range.end - range.start,
            MetadataCacheClass::SeriesColdPage,
        )?;
        let declared =
            std::mem::size_of::<ValidatedKeyset>() as u64 + range.end.saturating_sub(range.start);
        Ok(reader.get_or_load_owned(key, declared, move |bytes| {
            let keys = cold_v2_reader::decode_keyset_entry(&bytes, range.start, range.end)
                .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedKeyset { keys };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?)
    }

    fn load_keyset_block(
        &self,
        root: &GovernedSchema6SeriesRoot,
        keyset_id: u32,
    ) -> Result<LoadedBlockMeta, Schema6SeriesReaderError> {
        let section = ColdSection {
            offset: root.header.keyset_blocks_offset,
            end: root.header.meta_offset,
            count: root.header.num_keysets,
        };
        let range = self.load_cold_entry_range(section, keyset_id)?;
        let range = range.as_range();
        let fixed_range = self.record_series_result(cold_v2_reader::keyset_block_header_range(
            range.start,
            range.end,
        ))?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let fixed_key = metadata_key(
            &reader,
            fixed_range.start,
            fixed_range.end - fixed_range.start,
            MetadataCacheClass::SeriesColdPage,
        )?;
        let fixed = reader.get_or_load(
            fixed_key,
            std::mem::size_of::<ValidatedBlockFixed>() as u64,
            |bytes| {
                let bytes: [u8; KEYSET_BLOCK_HEADER_LEN as usize] =
                    bytes.try_into().map_err(|_| {
                        MetadataCacheError::from_io(invalid_data(
                            "schema-6 keyset block header length is not exact",
                        ))
                    })?;
                Ok(LoadedMetadata::new(
                    ValidatedBlockFixed { bytes },
                    std::mem::size_of::<ValidatedBlockFixed>() as u64,
                ))
            },
        )?;
        let widths_range = self.record_series_result(cold_v2_reader::keyset_block_widths_range(
            &fixed.bytes,
            range.start,
            range.end,
        ))?;
        if widths_range.is_empty() {
            let value = self.record_series_result(cold_v2_reader::decode_keyset_block_meta(
                &fixed.bytes,
                &[],
                range.start,
                range.end,
            ))?;
            return Ok(LoadedBlockMeta {
                _fixed: fixed,
                source: BlockMetaSource::EmptyWidths(value),
            });
        }
        let key = metadata_key(
            &reader,
            widths_range.start,
            widths_range.end - widths_range.start,
            MetadataCacheClass::SeriesColdPage,
        )?;
        let declared = std::mem::size_of::<ValidatedBlockMeta>() as u64
            + widths_range.end.saturating_sub(widths_range.start);
        let fixed_bytes = fixed.bytes;
        let value = reader.get_or_load_owned(key, declared, move |widths| {
            let value = cold_v2_reader::decode_keyset_block_meta(
                &fixed_bytes,
                &widths,
                range.start,
                range.end,
            )
            .map_err(MetadataCacheError::from_io)?;
            let value = ValidatedBlockMeta { value };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?;
        Ok(LoadedBlockMeta {
            _fixed: fixed,
            source: BlockMetaSource::Cached(value),
        })
    }

    fn load_row(
        &self,
        row: u32,
        block: &cold_v2_reader::KeySetBlockMeta,
    ) -> Result<MetadataCachePin<ValidatedRow>, Schema6SeriesReaderError> {
        let range =
            self.record_series_result(cold_v2_reader::keyset_block_row_range(block, row))?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = metadata_key(
            &reader,
            range.start,
            range.end - range.start,
            MetadataCacheClass::SeriesColdPage,
        )?;
        let declared =
            std::mem::size_of::<ValidatedRow>() as u64 + range.end.saturating_sub(range.start);
        Ok(reader.get_or_load_owned(key, declared, move |bytes| {
            if bytes.len() as u64 != range.end - range.start {
                return Err(MetadataCacheError::from_io(invalid_data(
                    "schema-6 keyset row length is not exact",
                )));
            }
            let value = ValidatedRow { bytes };
            let charged = value.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(value, charged))
        })?)
    }

    fn find_value_dict(
        &self,
        root: &GovernedSchema6SeriesRoot,
        key_sym: u32,
    ) -> Result<cold_v2_reader::ValueDictMeta, Schema6SeriesReaderError> {
        let mut low = 0u32;
        let mut high = root.header.num_value_dicts;
        while low < high {
            let mid = low + (high - low) / 2;
            let meta = self.load_value_dict_meta(root, mid)?;
            match meta.value.key_sym.cmp(&key_sym) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return Ok(meta.value),
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        Err(self.record_series_error(invalid_data("schema-6 value dictionary is missing")))
    }

    fn load_value_dict_meta(
        &self,
        root: &GovernedSchema6SeriesRoot,
        dict_id: u32,
    ) -> Result<MetadataCachePin<ValidatedValueDictMeta>, Schema6SeriesReaderError> {
        let section = ColdSection {
            offset: root.header.value_dicts_offset,
            end: root.header.keyset_blocks_offset,
            count: root.header.num_value_dicts,
        };
        let range = self.load_cold_entry_range(section, dict_id)?;
        let range = range.as_range();
        let header_range = self.record_series_result(cold_v2_reader::value_dict_header_range(
            range.start,
            range.end,
        ))?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = metadata_key(
            &reader,
            header_range.start,
            VALUE_DICT_HEADER_LEN,
            MetadataCacheClass::SeriesColdPage,
        )?;
        Ok(reader.get_or_load(
            key,
            std::mem::size_of::<ValidatedValueDictMeta>() as u64,
            move |bytes| {
                let value = cold_v2_reader::decode_value_dict_meta(bytes, range.start, range.end)
                    .map_err(MetadataCacheError::from_io)?;
                Ok(LoadedMetadata::new(
                    ValidatedValueDictMeta { value },
                    std::mem::size_of::<ValidatedValueDictMeta>() as u64,
                ))
            },
        )?)
    }

    fn load_value_sym(
        &self,
        meta: cold_v2_reader::ValueDictMeta,
        code: u32,
    ) -> Result<MetadataCachePin<ValidatedValueSym>, Schema6SeriesReaderError> {
        let range =
            self.record_series_result(cold_v2_reader::value_dict_value_range(meta, code))?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = metadata_key(
            &reader,
            range.start,
            VALUE_SYM_LEN,
            MetadataCacheClass::SeriesColdPage,
        )?;
        Ok(reader.get_or_load(
            key,
            std::mem::size_of::<ValidatedValueSym>() as u64,
            move |bytes| {
                let value = cold_v2_reader::decode_value_dict_value(bytes, meta, code)
                    .map_err(MetadataCacheError::from_io)?;
                Ok(LoadedMetadata::new(
                    ValidatedValueSym(value),
                    std::mem::size_of::<ValidatedValueSym>() as u64,
                ))
            },
        )?)
    }

    fn load_cold_entry_range(
        &self,
        section: ColdSection,
        entry_index: u32,
    ) -> Result<MetadataCachePin<ValidatedColdEntryRange>, Schema6SeriesReaderError> {
        let pair_range = self.record_series_result(cold_v2_reader::offset_pair_range(
            section.offset,
            section.end,
            section.count,
            entry_index,
        ))?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = metadata_key(
            &reader,
            pair_range.start,
            OFFSET_PAIR_LEN,
            MetadataCacheClass::SeriesColdPage,
        )?;
        Ok(reader.get_or_load(
            key,
            std::mem::size_of::<ValidatedColdEntryRange>() as u64,
            move |bytes| {
                let range = cold_v2_reader::decode_entry_range(
                    bytes,
                    section.offset,
                    section.end,
                    section.count,
                    entry_index,
                )
                .map_err(MetadataCacheError::from_io)?;
                Ok(LoadedMetadata::new(
                    ValidatedColdEntryRange {
                        start: range.start,
                        end: range.end,
                    },
                    std::mem::size_of::<ValidatedColdEntryRange>() as u64,
                ))
            },
        )?)
    }

    fn ensure_provenance(
        &self,
        provenance: &SegmentGenerationProvenance,
    ) -> Result<(), Schema6SeriesReaderError> {
        if provenance.matches(&self.guard) {
            Ok(())
        } else {
            Err(Schema6SeriesReaderError::ForeignSegmentGeneration)
        }
    }

    fn validate_series_refs(
        &self,
        root: &GovernedSchema6SeriesRoot,
        series_refs: &[u32],
    ) -> Result<(), Schema6SeriesReaderError> {
        if let Some(&series_ref) = series_refs
            .iter()
            .find(|series_ref| **series_ref >= root.header.num_series)
        {
            return Err(Schema6SeriesReaderError::InvalidSeriesRef {
                series_ref,
                num_series: root.header.num_series,
            });
        }
        Ok(())
    }

    fn verify_series_identity(
        &self,
        symbols: &GovernedSymbolSession,
        expected_series_id: u64,
        labels: &[(u32, u32)],
    ) -> Result<(), Schema6SeriesReaderError> {
        let mut hash = XxHash64::default();
        for &(key_sym, value_sym) in labels {
            symbols.visit_required_resolved(key_sym, |key| {
                hash.update(key.as_bytes());
                hash.update(&[0]);
                Ok(())
            })?;
            symbols.visit_required_resolved(value_sym, |value| {
                hash.update(value.as_bytes());
                hash.update(&[0xff]);
                Ok(())
            })?;
        }
        let actual_series_id = hash.finish();
        if actual_series_id != expected_series_id {
            return Err(self.record_series_error(invalid_data(format!(
                "schema-6 series identity mismatch: expected={expected_series_id} actual={actual_series_id}"
            ))));
        }
        Ok(())
    }

    fn record_series_result<T>(
        &self,
        result: io::Result<T>,
    ) -> Result<T, Schema6SeriesReaderError> {
        result.map_err(|error| self.record_series_error(error))
    }

    fn record_series_error(&self, error: io::Error) -> Schema6SeriesReaderError {
        match self.guard.reader(SegmentFile::Series) {
            Ok(reader) => Schema6SeriesReaderError::Cache(reader.record_validation_error(error)),
            Err(error) => Schema6SeriesReaderError::Runtime(error),
        }
    }
}

fn decode_schema6_series_root_v2(
    bytes: &[u8],
    series_file_len: u64,
    chunk_index_file_len: u64,
) -> io::Result<Schema6SeriesRootV2> {
    let header = decode_series_header_v2(bytes)?;
    let series_table_len = u64::from(header.num_series)
        .checked_mul(SERIES_TABLE_ENTRY_LEN)
        .ok_or_else(|| invalid_data("schema-6 series table length overflows"))?;
    let canonical_keysets_offset = SERIES_HEADER_LEN
        .checked_add(series_table_len)
        .ok_or_else(|| invalid_data("schema-6 series table end overflows"))?;
    if header.keysets_offset != canonical_keysets_offset {
        return Err(invalid_data(
            "schema-6 keysets section does not immediately follow the series table",
        ));
    }
    if header.meta_offset != series_file_len {
        return Err(invalid_data(
            "schema-6 metadata offset must equal the registered series file length",
        ));
    }
    if header.num_keysets > header.num_series || (header.num_series == 0 && header.num_keysets != 0)
    {
        return Err(invalid_data("schema-6 keyset count is inconsistent"));
    }
    if header.num_series != 0 && header.num_keysets == 0 {
        return Err(invalid_data(
            "non-empty schema-6 series file has no keysets",
        ));
    }
    if header.num_series == 0 && header.num_value_dicts != 0 {
        return Err(invalid_data(
            "empty schema-6 series file has value dictionaries",
        ));
    }
    cold_v2_reader::validate_offset_table_minimum(
        header.value_dicts_offset - header.keysets_offset,
        header.num_keysets,
        "keysets",
    )?;
    cold_v2_reader::validate_offset_table_minimum(
        header.keyset_blocks_offset - header.value_dicts_offset,
        header.num_value_dicts,
        "value dictionaries",
    )?;
    cold_v2_reader::validate_offset_table_minimum(
        header.meta_offset - header.keyset_blocks_offset,
        header.num_keysets,
        "keyset blocks",
    )?;

    let chunk_index_data_start = CHUNK_INDEX_HEADER_LEN_V1
        .checked_add(
            u64::from(header.num_series)
                .checked_add(1)
                .and_then(|count| count.checked_mul(8))
                .ok_or_else(|| invalid_data("schema-6 chunk-index directory length overflows"))?,
        )
        .ok_or_else(|| invalid_data("schema-6 chunk-index data offset overflows"))?;
    if chunk_index_file_len < chunk_index_data_start {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "schema-6 chunk-index file is shorter than its offsets directory",
        ));
    }

    Ok(Schema6SeriesRootV2 {
        header,
        series_file_len,
        chunk_index_file_len,
        chunk_index_data_start,
    })
}

fn decode_series_table_span(
    bytes: Vec<u8>,
    first_ref: u32,
    entry_count: usize,
    header: SeriesHeader,
    chunk_index_data_start: u64,
    chunk_index_file_len: u64,
) -> io::Result<ValidatedSeriesTableSpan> {
    let expected_len = entry_count
        .checked_mul(SERIES_TABLE_ENTRY_LEN as usize)
        .ok_or_else(|| invalid_data("schema-6 series table span length overflows"))?;
    if bytes.len() != expected_len {
        return Err(if bytes.len() < expected_len {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "schema-6 series table span is truncated",
            )
        } else {
            invalid_data("schema-6 series table span has trailing bytes")
        });
    }
    let end_ref = u64::from(first_ref)
        .checked_add(entry_count as u64)
        .ok_or_else(|| invalid_data("schema-6 series table ref range overflows"))?;
    if end_ref > u64::from(header.num_series) {
        return Err(invalid_data(
            "schema-6 series table span exceeds the declared series count",
        ));
    }

    let mut entries = Vec::new();
    entries.try_reserve_exact(entry_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "schema-6 table allocation failed",
        )
    })?;
    for entry_bytes in bytes.chunks_exact(SERIES_TABLE_ENTRY_LEN as usize) {
        let entry = decode_series_table_entry(entry_bytes)?;
        validate_series_table_entry(entry, header, chunk_index_data_start, chunk_index_file_len)?;
        entries.push(entry);
    }
    Ok(ValidatedSeriesTableSpan { first_ref, entries })
}

fn validate_series_table_entry(
    entry: SeriesTableEntryV2,
    header: SeriesHeader,
    chunk_index_data_start: u64,
    chunk_index_file_len: u64,
) -> io::Result<()> {
    if entry.kind_mask == 0 || entry.kind_mask & !VALID_KIND_MASK != 0 {
        return Err(invalid_data("schema-6 series kind mask is invalid"));
    }
    if entry.keyset_id >= header.num_keysets {
        return Err(invalid_data("schema-6 series keyset id is out of bounds"));
    }
    if entry.meta_off != 0 || entry.meta_len != 0 {
        return Err(invalid_data(
            "schema-6 series metadata offset and length must be zero",
        ));
    }
    if entry.chunk_index.offset < chunk_index_data_start {
        return Err(invalid_data(
            "schema-6 series chunk-index span starts inside the offsets directory",
        ));
    }
    if !(entry.chunk_index.offset - chunk_index_data_start)
        .is_multiple_of(CHUNK_INDEX_ENTRY_LEN_V1 as u64)
    {
        return Err(invalid_data(
            "schema-6 series chunk-index span offset is not entry aligned",
        ));
    }
    if !(entry.chunk_index.len as usize).is_multiple_of(CHUNK_INDEX_ENTRY_LEN_V1) {
        return Err(invalid_data(
            "schema-6 series chunk-index span length is not entry aligned",
        ));
    }
    let end = entry
        .chunk_index
        .offset
        .checked_add(u64::from(entry.chunk_index.len))
        .ok_or_else(|| invalid_data("schema-6 series chunk-index span overflows"))?;
    if end > chunk_index_file_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "schema-6 series chunk-index span exceeds the registered file",
        ));
    }
    Ok(())
}

fn checked_table_work_upper<T>(valid_count: usize) -> io::Result<u64> {
    checked_vec_bytes::<u32>(valid_count, "schema-6 valid-ref charge overflows")?
        .checked_add(checked_vec_bytes::<TableSpanPlan>(
            valid_count,
            "schema-6 table-plan charge overflows",
        )?)
        .and_then(|bytes| {
            checked_vec_bytes::<LoadedTableSpan>(valid_count, "schema-6 table-pin charge overflows")
                .ok()
                .and_then(|pins| bytes.checked_add(pins))
        })
        .and_then(|bytes| {
            checked_vec_bytes::<T>(valid_count, "schema-6 output charge overflows")
                .ok()
                .and_then(|output| bytes.checked_add(output))
        })
        .ok_or_else(|| invalid_input("schema-6 table working-set charge overflows"))
}

fn checked_struct_and_vec_bytes<S, T>(capacity: usize) -> io::Result<u64> {
    (std::mem::size_of::<S>() as u64)
        .checked_add(checked_vec_bytes::<T>(
            capacity,
            "schema-6 cached allocation charge overflows",
        )?)
        .ok_or_else(|| invalid_data("schema-6 cached allocation charge overflows"))
}

fn checked_vec_bytes<T>(capacity: usize, message: &'static str) -> io::Result<u64> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| invalid_input(message))
}

fn checked_len_mul(value: usize, multiplier: usize, message: &'static str) -> io::Result<u64> {
    value
        .checked_mul(multiplier)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| invalid_input(message))
}

fn try_vec_with_capacity<T>(capacity: usize, what: &'static str) -> io::Result<Vec<T>> {
    let mut values = Vec::new();
    values.try_reserve_exact(capacity).map_err(|error| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!("failed to allocate {what}: {error}"),
        )
    })?;
    Ok(values)
}

fn metadata_key(
    reader: &GovernedArtifactReader,
    offset: u64,
    length: u64,
    class: MetadataCacheClass,
) -> Result<MetadataCacheKey, MetadataCacheKeyError> {
    reader.metadata_cache_key(offset, length, class)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
pub(super) mod tests;
