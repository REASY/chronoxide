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
pub(super) mod tests {
    use std::fs;
    use std::io::Cursor;

    use tempfile::TempDir;

    use crate::storage::chunk::{
        ChunkIndexEntry, ChunkIndexRange, ChunkKind, GovernedSchema6ChunkIndexReader,
        write_chunk_index,
    };
    use crate::storage::metadata_governor::MetadataGovernorConfig;
    use crate::storage::metadata_runtime::{
        MetadataIssuedReadCount, SegmentArtifactRegistration, StoreMetadataRuntime,
    };
    use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;
    use crate::storage::symbols::{GovernedSymbolReader, write_symbols_bin_v3};

    use super::super::{SeriesReader, build_series_bin_v2};
    use super::*;

    const CHUNKS_LEN: usize = 4096;
    const OOO_CHUNKS_LEN: usize = 2048;

    pub(super) struct Fixture {
        _directory: TempDir,
        pub(super) runtime: StoreMetadataRuntime,
        registered: Option<RegisteredSegment>,
        pub(super) entries: Vec<SeriesEntry>,
        series_bytes: Vec<u8>,
        series_path: std::path::PathBuf,
        pub(super) symbols_path: std::path::PathBuf,
    }

    pub(super) fn runtime(
        retained_max_bytes: u64,
        in_flight_max_bytes: u64,
    ) -> StoreMetadataRuntime {
        StoreMetadataRuntime::new(MetadataGovernorConfig {
            retained_max_bytes,
            in_flight_max_bytes,
            max_open_files: 1,
            max_cached_open_files: 0,
        })
        .expect("valid schema-6 series test runtime")
    }

    pub(super) fn default_entries() -> Vec<SeriesEntry> {
        // Four series imply a 52-byte v1 chunk-index root/directory. Each
        // pointer below is therefore exact, aligned, and within the fixture.
        let mut entries = vec![
            SeriesEntry {
                series_id: 0,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: ChunkIndexRange {
                    offset: 52,
                    len: 40,
                },
                labels: vec![(1, 10), (2, 20)],
            },
            SeriesEntry {
                series_id: 0,
                kind_mask: SERIES_KIND_HISTOGRAM,
                chunk_index: ChunkIndexRange {
                    offset: 92,
                    len: 40,
                },
                labels: vec![(1, 11), (2, 20)],
            },
            SeriesEntry {
                series_id: 0,
                kind_mask: SERIES_KIND_EXPONENTIAL_HISTOGRAM,
                chunk_index: ChunkIndexRange {
                    offset: 132,
                    len: 40,
                },
                labels: vec![(3, 30)],
            },
            SeriesEntry {
                series_id: 0,
                kind_mask: SERIES_KIND_SUMMARY,
                chunk_index: ChunkIndexRange {
                    offset: 172,
                    len: 40,
                },
                labels: Vec::new(),
            },
        ];
        for entry in &mut entries {
            entry.series_id = fixture_series_id(&entry.labels);
        }
        entries
    }

    fn fixture_symbols() -> Vec<String> {
        (0..=30)
            .map(|symbol_id| format!("s{symbol_id:02}"))
            .collect()
    }

    fn fixture_series_id(labels: &[(u32, u32)]) -> u64 {
        let symbols = fixture_symbols();
        let mut hash = XxHash64::default();
        for &(key_sym, value_sym) in labels {
            hash.update(symbols[key_sym as usize].as_bytes());
            hash.update(&[0]);
            hash.update(symbols[value_sym as usize].as_bytes());
            hash.update(&[0xff]);
        }
        hash.finish()
    }

    pub(super) fn fixture(
        identity: &str,
        runtime: StoreMetadataRuntime,
        entries: Vec<SeriesEntry>,
        mutate_series: impl FnOnce(&mut Vec<u8>),
    ) -> Fixture {
        let chunk_series_count = entries.len();
        fixture_with_chunk_series_count(
            identity,
            runtime,
            entries,
            chunk_series_count,
            mutate_series,
        )
    }

    fn fixture_with_chunk_series_count(
        identity: &str,
        runtime: StoreMetadataRuntime,
        entries: Vec<SeriesEntry>,
        chunk_series_count: usize,
        mutate_series: impl FnOnce(&mut Vec<u8>),
    ) -> Fixture {
        let mut series_bytes = build_series_bin_v2(&entries).expect("encode series fixture");
        mutate_series(&mut series_bytes);
        let mut chunk_index_bytes = Vec::new();
        let chunk_entries = (0..chunk_series_count)
            .map(|index| {
                vec![ChunkIndexEntry {
                    file_id: 0,
                    kind: ChunkKind::Float,
                    flags: 0,
                    min_time_ms: index as u64,
                    max_time_ms: index as u64,
                    offset: (index * 64) as u64,
                    length: 40,
                    scalar_lane_offset: 0,
                    scalar_lane_len: 0,
                }]
            })
            .collect::<Vec<_>>();
        write_chunk_index(&mut chunk_index_bytes, &chunk_entries)
            .expect("encode chunk-index fixture");
        let directory = TempDir::new().expect("create schema-6 series fixture directory");
        let mut series_path = None;
        let mut symbols_path = None;
        let symbols = fixture_symbols();
        let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
            let path = directory.path().join(file.filename());
            match file {
                SegmentFile::MetaJson => fs::write(&path, b"{}").expect("write meta fixture"),
                SegmentFile::Symbols => {
                    let mut encoded = Vec::new();
                    write_symbols_bin_v3(&mut encoded, symbols.iter())
                        .expect("encode symbols fixture");
                    fs::write(&path, encoded).expect("write symbols fixture");
                    symbols_path = Some(path.clone());
                }
                SegmentFile::Series => {
                    fs::write(&path, &series_bytes).expect("write series fixture");
                    series_path = Some(path.clone());
                }
                SegmentFile::Chunks => {
                    fs::write(&path, vec![0; CHUNKS_LEN]).expect("write chunks fixture")
                }
                SegmentFile::OooChunks => {
                    fs::write(&path, vec![0; OOO_CHUNKS_LEN]).expect("write OOO fixture")
                }
                SegmentFile::ChunkIndex => {
                    fs::write(&path, &chunk_index_bytes).expect("write chunk-index fixture")
                }
                SegmentFile::Indexes => {
                    fs::write(&path, b"indexes").expect("write indexes fixture")
                }
                SegmentFile::Footer => unreachable!("footer is not runtime-inventoried"),
            }
            let len = fs::metadata(&path).expect("stat fixture artifact").len();
            SegmentArtifactRegistration::new(file, path, len)
        });
        let registered = runtime
            .register_segment(identity, &artifacts)
            .expect("register schema-6 series fixture");
        Fixture {
            _directory: directory,
            runtime,
            registered: Some(registered),
            entries,
            series_bytes,
            series_path: series_path.expect("series path captured"),
            symbols_path: symbols_path.expect("symbols path captured"),
        }
    }

    pub(super) fn standard_fixture(
        identity: &str,
        retained_max_bytes: u64,
        in_flight_max_bytes: u64,
    ) -> Fixture {
        fixture(
            identity,
            runtime(retained_max_bytes, in_flight_max_bytes),
            default_entries(),
            |_| {},
        )
    }

    pub(super) fn open_reader(fixture: &Fixture) -> GovernedSchema6SeriesReader {
        GovernedSchema6SeriesReader::open(
            fixture.registered.as_ref().expect("fixture owner exists"),
            fixture.entries.len() as u32,
        )
        .expect("open governed schema-6 series reader")
    }

    pub(super) fn open_symbol_session(fixture: &Fixture) -> GovernedSymbolSession {
        GovernedSymbolReader::open(fixture.registered.as_ref().expect("fixture owner exists"))
            .expect("open governed symbol reader")
            .query_session()
            .expect("open governed symbol session")
    }

    pub(super) fn open_chunk_index_context(
        fixture: &Fixture,
    ) -> (
        GovernedSchema6ChunkIndexSession,
        GovernedSchema6ChunkIndexRoot,
    ) {
        let reader = GovernedSchema6ChunkIndexReader::open(
            fixture.registered.as_ref().expect("fixture owner exists"),
            fixture.entries.len() as u32,
        )
        .expect("open governed chunk-index reader");
        let session = reader
            .query_session()
            .expect("open governed chunk-index session");
        let root = session.load_root().expect("load governed chunk-index root");
        (session, root)
    }

    pub(super) fn read_metadata(
        fixture: &Fixture,
        session: &GovernedSchema6SeriesSession,
        root: &GovernedSchema6SeriesRoot,
        series_refs: &[u32],
    ) -> Result<GovernedSchema6SeriesMetadata, Schema6SeriesReaderError> {
        let (chunk_index, chunk_index_root) = open_chunk_index_context(fixture);
        session.read_metadata_entries(root, &chunk_index, &chunk_index_root, series_refs)
    }

    fn read_full_entries(
        fixture: &Fixture,
        session: &GovernedSchema6SeriesSession,
        root: &GovernedSchema6SeriesRoot,
        symbols: &GovernedSymbolSession,
        series_refs: &[u32],
    ) -> Result<GovernedSchema6SeriesEntries, Schema6SeriesReaderError> {
        let (chunk_index, chunk_index_root) = open_chunk_index_context(fixture);
        session.read_entries(root, &chunk_index, &chunk_index_root, symbols, series_refs)
    }

    pub(super) fn class_reads(
        runtime: &StoreMetadataRuntime,
        class: MetadataCacheClass,
    ) -> MetadataIssuedReadCount {
        runtime.snapshot().reads.classes[class.stable_index()].issued
    }

    pub(super) fn delta(
        after: MetadataIssuedReadCount,
        before: MetadataIssuedReadCount,
    ) -> MetadataIssuedReadCount {
        MetadataIssuedReadCount {
            calls: after.calls - before.calls,
            bytes: after.bytes - before.bytes,
        }
    }

    #[test]
    fn exact_root_and_coalesced_table_spans_are_cached_and_ordered() {
        let fixture = standard_fixture("schema6-series-cached", 1024 * 1024, 1024 * 1024);
        let before_root = class_reads(&fixture.runtime, MetadataCacheClass::SeriesRoot);
        let reader = open_reader(&fixture);
        assert_eq!(reader.segment_identity(), "schema6-series-cached");
        assert_eq!(
            delta(
                class_reads(&fixture.runtime, MetadataCacheClass::SeriesRoot),
                before_root
            ),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: SERIES_HEADER_LEN,
            }
        );

        let session = reader
            .query_session()
            .expect("open schema-6 series session");
        let root = session.load_root().expect("reuse cached series root");
        assert_eq!(root.num_series(), 4);
        let before_table = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
        let before_in_flight = fixture.runtime.snapshot().governor.in_flight_bytes;
        let metadata = read_metadata(&fixture, &session, &root, &[2, 0, 1, 1])
            .expect("read coalesced table span");
        let metadata_entries = session
            .routing_entries(&metadata)
            .expect("bind metadata to its session");
        assert_eq!(
            metadata_entries
                .iter()
                .map(|(series_ref, entry)| (*series_ref, entry.kind_mask, entry.chunk_index))
                .collect::<Vec<_>>(),
            vec![
                (
                    2,
                    fixture.entries[2].kind_mask,
                    fixture.entries[2].chunk_index,
                ),
                (
                    0,
                    fixture.entries[0].kind_mask,
                    fixture.entries[0].chunk_index,
                ),
                (
                    1,
                    fixture.entries[1].kind_mask,
                    fixture.entries[1].chunk_index,
                ),
                (
                    1,
                    fixture.entries[1].kind_mask,
                    fixture.entries[1].chunk_index,
                ),
            ]
        );
        assert!(metadata.charged_bytes() > 0);
        let charged_bytes = metadata.charged_bytes();
        let with_output = fixture.runtime.snapshot().governor;
        assert_eq!(
            with_output.in_flight_bytes,
            before_in_flight + charged_bytes
        );
        assert!(with_output.peak_in_flight_bytes >= with_output.in_flight_bytes);
        let after_table = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
        assert_eq!(
            delta(after_table, before_table),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: 3 * SERIES_TABLE_ENTRY_LEN,
            }
        );
        drop(metadata);
        assert_eq!(
            fixture.runtime.snapshot().governor.in_flight_bytes,
            before_in_flight
        );

        let second =
            read_metadata(&fixture, &session, &root, &[0, 1, 2]).expect("reuse exact table span");
        assert_eq!(session.routing_entries(&second).unwrap().len(), 3);
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
            after_table
        );
    }

    #[test]
    fn out_of_range_refs_fail_before_series_io_without_poisoning_the_series_artifact() {
        let fixture = standard_fixture("schema6-series-invalid-ref", 1024 * 1024, 1024 * 1024);
        let reader = open_reader(&fixture);
        let session = reader.query_session().expect("open invalid-ref session");
        let root = session.load_root().expect("load invalid-ref root");
        let before = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
        assert!(matches!(
            read_metadata(&fixture, &session, &root, &[0, 99]),
            Err(Schema6SeriesReaderError::InvalidSeriesRef {
                series_ref: 99,
                num_series: 4
            })
        ));
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
            before
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    }

    #[test]
    fn independently_valid_same_generation_roots_must_have_the_same_series_count() {
        let fixture = fixture_with_chunk_series_count(
            "schema6-series-root-count-binding",
            runtime(1024 * 1024, 1024 * 1024),
            default_entries(),
            3,
            |_| {},
        );
        let series_reader = open_reader(&fixture);
        let series_session = series_reader
            .query_session()
            .expect("open mismatched-count series session");
        let series_root = series_session
            .load_root()
            .expect("load independently valid series root");
        let chunk_reader = GovernedSchema6ChunkIndexReader::open(
            fixture.registered.as_ref().expect("fixture owner exists"),
            3,
        )
        .expect("open independently valid chunk-index root");
        let chunk_session = chunk_reader
            .query_session()
            .expect("open mismatched-count chunk-index session");
        let chunk_root = chunk_session
            .load_root()
            .expect("load mismatched-count chunk-index root");
        let before_series = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
        let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);

        let error = series_session
            .read_metadata_entries(&series_root, &chunk_session, &chunk_root, &[])
            .expect_err("same-generation roots with different counts must fail");
        assert!(matches!(
            error,
            Schema6SeriesReaderError::ChunkIndex(Schema6ChunkIndexReaderError::Cache(
                MetadataCacheError::Structural(_)
            ))
        ));
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
            before_series
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
            before_directory
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    }

    #[test]
    fn full_materialization_matches_legacy_reader_with_duplicates_and_empty_labels() {
        let fixture = standard_fixture("schema6-series-equivalence", 1024 * 1024, 1024 * 1024);
        let reader = open_reader(&fixture);
        let session = reader
            .query_session()
            .expect("open schema-6 series session");
        let symbols = open_symbol_session(&fixture);
        let root = session.load_root().expect("load series root");
        let refs = [3, 1, 1, 0, 2];
        let actual = read_full_entries(&fixture, &session, &root, &symbols, &refs)
            .expect("materialize governed schema-6 entries");
        let actual_entries = session
            .entries(&actual)
            .expect("bind series entries to their session");

        let mut legacy = SeriesReader::open(Cursor::new(fixture.series_bytes.clone()))
            .expect("open legacy series reader");
        let (expected, _) = legacy
            .read_entries_with_bytes(&refs)
            .expect("materialize legacy entries");
        assert_eq!(actual_entries, expected.as_slice());
        assert!(actual_entries[0].1.labels.is_empty());
        assert_eq!(actual_entries[1].1.labels, vec![(1, 11), (2, 20)]);
        assert_eq!(actual_entries[2], actual_entries[1]);
        assert!(actual.charged_bytes() > 0);
        assert!(
            class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage).calls > 0,
            "full materialization must use governed cold ranges"
        );
    }

    #[test]
    fn zero_retention_reissues_table_only_after_operation_pins_drop() {
        let fixture = standard_fixture("schema6-series-zero-retention", 0, 1024 * 1024);
        let reader = open_reader(&fixture);
        let session = reader
            .query_session()
            .expect("open schema-6 series session");
        let root = session.load_root().expect("load transient root");
        let before = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
        {
            let first = read_metadata(&fixture, &session, &root, &[0, 1])
                .expect("read transient table span");
            assert_eq!(session.routing_entries(&first).unwrap().len(), 2);
        }
        let middle = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
        assert_eq!(delta(middle, before).calls, 1);
        let second =
            read_metadata(&fixture, &session, &root, &[0, 1]).expect("reload released table span");
        assert_eq!(session.routing_entries(&second).unwrap().len(), 2);
        assert_eq!(
            delta(
                class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
                middle
            ),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: 2 * SERIES_TABLE_ENTRY_LEN,
            }
        );
    }

    #[test]
    fn tiny_budget_refuses_before_table_io_and_retry_succeeds() {
        let fixture = standard_fixture("schema6-series-budget", 1024 * 1024, 8192);
        let reader = open_reader(&fixture);
        let session = reader
            .query_session()
            .expect("open schema-6 series session");
        let root = session.load_root().expect("reuse cached root");
        let (chunk_index, chunk_index_root) = open_chunk_index_context(&fixture);
        let blocker = fixture
            .runtime
            .governor()
            .reserve_in_flight_for_usage(8100, MetadataUsageClass::Scratch)
            .expect("reserve competing scratch bytes");
        let before = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
        let error = session
            .read_metadata_entries(&root, &chunk_index, &chunk_index_root, &[0, 1, 2])
            .expect_err("tiny budget must refuse before table I/O");
        assert!(matches!(
            error,
            Schema6SeriesReaderError::Cache(MetadataCacheError::Budget(_))
        ));
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
            before
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

        drop(blocker);
        let retried = session
            .read_metadata_entries(&root, &chunk_index, &chunk_index_root, &[0, 1, 2])
            .expect("budget refusal must be retryable");
        assert_eq!(session.routing_entries(&retried).unwrap().len(), 3);
    }

    #[test]
    fn table_and_touched_cold_corruption_are_sticky_but_resource_errors_are_not() {
        let table_corruption = fixture(
            "schema6-series-table-corruption",
            runtime(0, 1024 * 1024),
            default_entries(),
            |bytes| {
                // First SeriesEntryV2.meta_len.
                bytes[SERIES_HEADER_LEN as usize + 36..SERIES_HEADER_LEN as usize + 40]
                    .copy_from_slice(&1u32.to_le_bytes());
            },
        );
        let reader = open_reader(&table_corruption);
        let session = reader.query_session().expect("open corruption session");
        let root = session.load_root().expect("load corruption root");
        let before = class_reads(&table_corruption.runtime, MetadataCacheClass::SeriesHotPage);
        assert!(read_metadata(&table_corruption, &session, &root, &[0]).is_err());
        let after = class_reads(&table_corruption.runtime, MetadataCacheClass::SeriesHotPage);
        assert_eq!(delta(after, before).calls, 1);
        table_corruption.runtime.evict_all_resident_metadata();
        assert!(read_metadata(&table_corruption, &session, &root, &[0]).is_err());
        assert_eq!(
            class_reads(&table_corruption.runtime, MetadataCacheClass::SeriesHotPage),
            after
        );
        assert_eq!(
            table_corruption.runtime.snapshot().cache.sticky_artifacts,
            1
        );

        let cold_corruption = fixture(
            "schema6-series-cold-corruption",
            runtime(0, 1024 * 1024),
            default_entries(),
            |bytes| {
                // First SeriesEntryV2.row points beyond its keyset block.
                bytes[SERIES_HEADER_LEN as usize + 28..SERIES_HEADER_LEN as usize + 32]
                    .copy_from_slice(&u32::MAX.to_le_bytes());
            },
        );
        let reader = open_reader(&cold_corruption);
        let session = reader
            .query_session()
            .expect("open cold corruption session");
        let symbols = open_symbol_session(&cold_corruption);
        let root = session.load_root().expect("load cold corruption root");
        assert!(read_full_entries(&cold_corruption, &session, &root, &symbols, &[0]).is_err());
        assert_eq!(cold_corruption.runtime.snapshot().cache.sticky_artifacts, 1);

        let row_substitution = fixture(
            "schema6-series-row-substitution",
            runtime(0, 1024 * 1024),
            default_entries(),
            |bytes| {
                // Series zero and one share a keyset. Point series zero at the
                // other valid row; only canonical identity verification can
                // distinguish this from a structurally valid row.
                bytes[SERIES_HEADER_LEN as usize + 28..SERIES_HEADER_LEN as usize + 32]
                    .copy_from_slice(&1u32.to_le_bytes());
            },
        );
        let reader = open_reader(&row_substitution);
        let session = reader
            .query_session()
            .expect("open row-substitution session");
        let symbols = open_symbol_session(&row_substitution);
        let root = session.load_root().expect("load row-substitution root");
        assert!(read_full_entries(&row_substitution, &session, &root, &symbols, &[0]).is_err());
        assert_eq!(
            row_substitution.runtime.snapshot().cache.sticky_artifacts,
            1
        );
    }

    #[test]
    fn reserved_fields_are_sticky_at_their_touched_boundaries() {
        let root_corruption = fixture(
            "schema6-series-root-reserved",
            runtime(0, 1024 * 1024),
            default_entries(),
            |bytes| {
                // The fixed header's reserved u32 follows the three counts.
                bytes[20..24].copy_from_slice(&1u32.to_le_bytes());
            },
        );
        let error = GovernedSchema6SeriesReader::open(
            root_corruption
                .registered
                .as_ref()
                .expect("root-corruption owner exists"),
            root_corruption.entries.len() as u32,
        )
        .err()
        .expect("reserved root field must fail");
        assert!(matches!(
            error,
            Schema6SeriesReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        assert_eq!(root_corruption.runtime.snapshot().cache.sticky_artifacts, 1);

        let table_corruption = fixture(
            "schema6-series-table-reserved",
            runtime(0, 1024 * 1024),
            default_entries(),
            |bytes| {
                // The first table entry's one-byte flags field follows kind_mask.
                bytes[SERIES_HEADER_LEN as usize + 9] = 1;
            },
        );
        let reader = open_reader(&table_corruption);
        let session = reader.query_session().expect("open table-reserved session");
        let root = session.load_root().expect("load table-reserved root");
        assert!(read_metadata(&table_corruption, &session, &root, &[0]).is_err());
        assert_eq!(
            table_corruption.runtime.snapshot().cache.sticky_artifacts,
            1
        );
    }

    #[test]
    fn lifecycle_and_generation_provenance_do_not_leak_guards_or_files() {
        let shared_runtime = runtime(0, 1024 * 1024);
        let mut first = fixture(
            "schema6-series-owner-first",
            shared_runtime.clone(),
            default_entries(),
            |_| {},
        );
        let second = fixture(
            "schema6-series-owner-second",
            shared_runtime,
            default_entries(),
            |_| {},
        );
        let first_reader = open_reader(&first);
        let second_reader = open_reader(&second);
        let first_symbols = open_symbol_session(&first);
        let second_symbols = open_symbol_session(&second);
        let (first_chunk_index, first_chunk_index_root) = open_chunk_index_context(&first);
        drop(first.registered.take());
        let first_session = first_reader.query_session().expect("open first session");
        let first_root = first_session.load_root().expect("load first root");
        let values = first_session
            .read_metadata_entries(
                &first_root,
                &first_chunk_index,
                &first_chunk_index_root,
                &[0],
            )
            .expect("load first metadata");
        first_session
            .routing_entries(&values)
            .expect("metadata matches first generation");
        let second_session = second_reader.query_session().expect("open second session");
        assert!(matches!(
            second_session.routing_entries(&values),
            Err(Schema6SeriesReaderError::ForeignSegmentGeneration)
        ));
        let (second_chunk_index, second_chunk_index_root) = open_chunk_index_context(&second);
        assert!(matches!(
            first_session.read_metadata_entries(
                &first_root,
                &second_chunk_index,
                &second_chunk_index_root,
                &[0]
            ),
            Err(Schema6SeriesReaderError::ChunkIndex(
                Schema6ChunkIndexReaderError::ForeignSegmentGeneration
            ))
        ));
        assert!(matches!(
            first_session.read_metadata_entries(
                &first_root,
                &first_chunk_index,
                &second_chunk_index_root,
                &[]
            ),
            Err(Schema6SeriesReaderError::ChunkIndex(
                Schema6ChunkIndexReaderError::ForeignSegmentGeneration
            ))
        ));
        assert!(matches!(
            first_session.read_entries(
                &first_root,
                &first_chunk_index,
                &first_chunk_index_root,
                &second_symbols,
                &[0]
            ),
            Err(Schema6SeriesReaderError::Symbols(
                GovernedSymbolReaderError::ForeignSegmentGeneration
            ))
        ));
        first_session
            .read_entries(
                &first_root,
                &first_chunk_index,
                &first_chunk_index_root,
                &first_symbols,
                &[0],
            )
            .expect("matching symbol generation is accepted");

        drop(first_reader);
        assert_eq!(first.runtime.snapshot().cache.registered_artifacts, 14);
        drop(first_root);
        drop(first_session);
        drop(first_symbols);
        drop(first_chunk_index_root);
        drop(first_chunk_index);
        // The provenance token and scratch charge do not own a read guard or
        // registered segment; only the second fixture remains registered.
        assert_eq!(first.runtime.snapshot().cache.registered_artifacts, 7);
        drop(values);
        drop(second_session);
        drop(second_symbols);
        drop(second_chunk_index_root);
        drop(second_chunk_index);
        drop(second_reader);
        drop(second);
        assert_eq!(first.runtime.snapshot().cache.registered_artifacts, 0);
        assert_eq!(first.runtime.snapshot().files.open_files, 0);
    }

    #[test]
    fn root_count_chunk_spans_and_truncation_are_strict() {
        let mismatch = standard_fixture("schema6-series-count-mismatch", 0, 1024 * 1024);
        let error = GovernedSchema6SeriesReader::open(
            mismatch.registered.as_ref().expect("fixture owner exists"),
            5,
        )
        .err()
        .expect("series count mismatch must fail");
        assert!(matches!(
            error,
            Schema6SeriesReaderError::Cache(MetadataCacheError::Structural(_))
        ));

        let invalid_span = fixture(
            "schema6-series-invalid-chunk-span",
            runtime(0, 1024 * 1024),
            default_entries(),
            |bytes| {
                // First SeriesEntryV2.chunk_index_offset is inside the v1
                // directory and is only touched with the table entry.
                bytes[SERIES_HEADER_LEN as usize + 12..SERIES_HEADER_LEN as usize + 20]
                    .copy_from_slice(&12u64.to_le_bytes());
            },
        );
        let reader = open_reader(&invalid_span);
        let session = reader.query_session().expect("open invalid-span session");
        let root = session.load_root().expect("load invalid-span root");
        assert!(read_metadata(&invalid_span, &session, &root, &[0]).is_err());
        assert_eq!(invalid_span.runtime.snapshot().cache.sticky_artifacts, 1);

        let aliased_span = fixture(
            "schema6-series-aliased-chunk-span",
            runtime(0, 1024 * 1024),
            default_entries(),
            |bytes| {
                // This is a valid aligned, in-bounds span, but it belongs to
                // series one rather than series zero.
                bytes[SERIES_HEADER_LEN as usize + 12..SERIES_HEADER_LEN as usize + 20]
                    .copy_from_slice(&92u64.to_le_bytes());
            },
        );
        let reader = open_reader(&aliased_span);
        let session = reader.query_session().expect("open aliased-span session");
        let root = session.load_root().expect("load aliased-span root");
        assert!(matches!(
            read_metadata(&aliased_span, &session, &root, &[0]),
            Err(Schema6SeriesReaderError::ChunkIndex(
                Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
            ))
        ));
        assert_eq!(aliased_span.runtime.snapshot().cache.sticky_artifacts, 1);

        let truncation = standard_fixture("schema6-series-truncation", 0, 1024 * 1024);
        let reader = open_reader(&truncation);
        let session = reader.query_session().expect("open truncation session");
        let symbols = open_symbol_session(&truncation);
        let root = session.load_root().expect("load truncation root");
        let len = fs::metadata(&truncation.series_path)
            .expect("stat series fixture")
            .len();
        fs::OpenOptions::new()
            .write(true)
            .open(&truncation.series_path)
            .expect("open series fixture for truncation")
            .set_len(len - 1)
            .expect("truncate series fixture");
        assert!(read_full_entries(&truncation, &session, &root, &symbols, &[0]).is_err());
        assert_eq!(truncation.runtime.snapshot().cache.sticky_artifacts, 1);
    }
}
