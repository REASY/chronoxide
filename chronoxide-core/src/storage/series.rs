use std::collections::{BTreeMap, HashMap};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};

use crate::storage::chunk::ChunkIndexRange;
use crate::storage::metadata_runtime::{SegmentGenerationProvenance, SegmentReadGuard};

pub(crate) mod cold_v2;
#[allow(dead_code)] // Wired into the schema-neutral metadata backend after the governed adapter.
pub(crate) mod v2_runtime;
#[allow(dead_code)]
// Wired into segment I/O after the isolated schema-7 codec lands.
pub(crate) mod v3;

use cold_v2::SeriesColdV2Plan;
use cold_v2::reader as cold_v2_reader;

const SERIES_MAGIC: u32 = u32::from_le_bytes(*b"SERI");
const SERIES_VERSION: u16 = 2;
const SERIES_HEADER_LEN: u64 = 64;
const SERIES_TABLE_ENTRY_LEN: u64 = 40;
const VALUE_DICT_FULL_CACHE_MAX_VALUES: u32 = 1024;

pub const SERIES_KIND_FLOAT: u8 = 0b0000_0001;
pub const SERIES_KIND_INT64: u8 = 0b0000_0010;
pub const SERIES_KIND_HISTOGRAM: u8 = 0b0000_0100;
pub const SERIES_KIND_EXPONENTIAL_HISTOGRAM: u8 = 0b0000_1000;
pub const SERIES_KIND_SUMMARY: u8 = 0b0001_0000;

/// Unforgeable query-local proof of the series count decoded from one
/// generation's validated series root. Shared index readers accept this
/// capability instead of an arbitrary caller-supplied integer.
#[derive(Debug)]
pub(crate) struct GovernedSeriesCountBinding {
    provenance: SegmentGenerationProvenance,
    num_series: u32,
}

impl GovernedSeriesCountBinding {
    fn new(provenance: SegmentGenerationProvenance, num_series: u32) -> Self {
        Self {
            provenance,
            num_series,
        }
    }

    pub(crate) fn matches(&self, guard: &SegmentReadGuard) -> bool {
        self.provenance.matches(guard)
    }

    pub(crate) fn num_series(&self) -> u32 {
        self.num_series
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SegmentSymbols {
    values: Vec<String>,
    bytes: Vec<u8>,
    offsets: Vec<usize>,
    by_value: HashMap<String, u32>,
    sorted: bool,
}

impl SegmentSymbols {
    pub fn intern(&mut self, value: &str) -> u32 {
        if self.sorted {
            if let Some(id) = self.lookup(value) {
                return id;
            }
            self.materialize_owned_values();
            self.rebuild_lookup_map();
            self.sorted = false;
        }
        if let Some(&id) = self.by_value.get(value) {
            return id;
        }

        let id = self.values.len() as u32;
        self.values.push(value.to_string());
        self.by_value.insert(value.to_string(), id);
        id
    }

    pub fn lookup(&self, value: &str) -> Option<u32> {
        if self.sorted {
            if self.has_packed_values() {
                self.lookup_packed(value)
            } else {
                self.values
                    .binary_search_by(|candidate| candidate.as_bytes().cmp(value.as_bytes()))
                    .ok()
                    .and_then(|id| u32::try_from(id).ok())
            }
        } else {
            self.by_value.get(value).copied()
        }
    }

    pub fn resolve(&self, id: u32) -> Option<&str> {
        if self.has_packed_values() {
            self.packed_symbol_bytes(id as usize)
                .and_then(|value| std::str::from_utf8(value).ok())
        } else {
            self.values.get(id as usize).map(String::as_str)
        }
    }

    pub fn len(&self) -> usize {
        if self.has_packed_values() {
            self.offsets.len().saturating_sub(1)
        } else {
            self.values.len()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn sorted_remap(&self) -> io::Result<(Self, Vec<u32>)> {
        let mut values = Vec::with_capacity(self.len());
        for old_id in 0..self.len() {
            let value = self
                .resolve(u32::try_from(old_id).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "symbol count exceeds u32")
                })?)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "symbol id is missing"))?
                .to_string();
            values.push((old_id, value));
        }
        values.sort_by(|left, right| left.1.as_bytes().cmp(right.1.as_bytes()));

        let mut remap = vec![0u32; values.len()];
        let mut sorted_values = Vec::with_capacity(values.len());
        let mut previous: Option<String> = None;
        for (new_id, (old_id, value)) in values.into_iter().enumerate() {
            if previous
                .as_deref()
                .is_some_and(|prev| prev.as_bytes() == value.as_bytes())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate symbol value",
                ));
            }
            remap[old_id] = u32::try_from(new_id).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "symbol count exceeds u32")
            })?;
            previous = Some(value.clone());
            sorted_values.push(value);
        }

        Ok((Self::from_sorted_values(sorted_values)?, remap))
    }

    fn from_sorted_values(values: Vec<String>) -> io::Result<Self> {
        validate_sorted_symbol_values(&values)?;
        Ok(Self {
            values,
            bytes: Vec::new(),
            offsets: Vec::new(),
            by_value: HashMap::new(),
            sorted: true,
        })
    }

    fn from_sorted_bytes(offsets: Vec<usize>, bytes: Vec<u8>) -> io::Result<Self> {
        validate_sorted_symbol_bytes(&offsets, &bytes)?;
        Ok(Self {
            values: Vec::new(),
            bytes,
            offsets,
            by_value: HashMap::new(),
            sorted: true,
        })
    }

    fn has_packed_values(&self) -> bool {
        !self.offsets.is_empty()
    }

    fn packed_symbol_bytes(&self, id: usize) -> Option<&[u8]> {
        let start = *self.offsets.get(id)?;
        let end = *self.offsets.get(id + 1)?;
        self.bytes.get(start..end)
    }

    fn lookup_packed(&self, value: &str) -> Option<u32> {
        let target = value.as_bytes();
        let mut low = 0usize;
        let mut high = self.len();
        while low < high {
            let mid = low + (high - low) / 2;
            let candidate = self.packed_symbol_bytes(mid)?;
            match candidate.cmp(target) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return u32::try_from(mid).ok(),
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        None
    }

    fn materialize_owned_values(&mut self) {
        if !self.has_packed_values() {
            return;
        }

        let mut values = Vec::with_capacity(self.len());
        for id in 0..self.len() {
            let value = self
                .resolve(id as u32)
                .expect("packed symbols are validated before construction");
            values.push(value.to_string());
        }
        self.values = values;
        self.bytes.clear();
        self.offsets.clear();
    }

    fn rebuild_lookup_map(&mut self) {
        self.materialize_owned_values();
        self.by_value = self
            .values
            .iter()
            .enumerate()
            .filter_map(|(idx, value)| u32::try_from(idx).ok().map(|id| (value.clone(), id)))
            .collect();
    }

    #[cfg(test)]
    fn has_packed_storage(&self) -> bool {
        self.has_packed_values()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesEntry {
    pub series_id: u64,
    pub kind_mask: u8,
    pub chunk_index: ChunkIndexRange,
    pub labels: Vec<(u32, u32)>,
}

pub(crate) trait SeriesEntryView {
    fn series_id(&self) -> u64;
    fn kind_mask(&self) -> u8;
    fn labels(&self) -> &[(u32, u32)];
}

impl SeriesEntryView for SeriesEntry {
    fn series_id(&self) -> u64 {
        self.series_id
    }

    fn kind_mask(&self) -> u8 {
        self.kind_mask
    }

    fn labels(&self) -> &[(u32, u32)] {
        &self.labels
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeriesEntryMetadata {
    pub series_id: u64,
    pub kind_mask: u8,
    pub chunk_index: ChunkIndexRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesEntryLocator {
    table_entry: SeriesTableEntryV2,
}

impl SeriesEntryLocator {
    pub(crate) fn metadata(self) -> SeriesEntryMetadata {
        SeriesEntryMetadata::from(self.table_entry)
    }
}

pub fn write_symbols_bin(writer: impl Write, symbols: &SegmentSymbols) -> io::Result<()> {
    validate_sorted_symbols(symbols)?;
    let symbol_count = u32::try_from(symbols.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "symbol count exceeds u32"))?;
    let mut values = Vec::with_capacity(symbols.len());
    for symbol_id in 0..symbol_count {
        values.push(
            symbols.resolve(symbol_id).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "symbol id is missing")
            })?,
        );
    }
    crate::storage::symbols::write_symbols_bin_v3(writer, values)
}

pub fn read_symbols_bin(reader: impl Read) -> io::Result<SegmentSymbols> {
    let values = crate::storage::symbols::read_symbols_bin_v3(reader)?;
    let string_bytes_len = values.iter().try_fold(0usize, |total, value| {
        total
            .checked_add(value.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "symbols bytes overflow"))
    })?;
    let mut offsets = Vec::with_capacity(values.len().saturating_add(1));
    let mut bytes = Vec::with_capacity(string_bytes_len);
    offsets.push(0);
    for value in values {
        bytes.extend_from_slice(value.as_bytes());
        offsets.push(bytes.len());
    }
    SegmentSymbols::from_sorted_bytes(offsets, bytes)
}

fn validate_sorted_symbol_values(values: &[String]) -> io::Result<()> {
    for pair in values.windows(2) {
        let left = pair[0].as_bytes();
        let right = pair[1].as_bytes();
        if left >= right {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "symbols must be sorted by unique UTF-8 bytes",
            ));
        }
    }
    Ok(())
}

fn validate_sorted_symbols(symbols: &SegmentSymbols) -> io::Result<()> {
    if symbols.has_packed_values() {
        validate_sorted_symbol_bytes(&symbols.offsets, &symbols.bytes)
    } else {
        validate_sorted_symbol_values(&symbols.values)
    }
}

fn validate_sorted_symbol_bytes(offsets: &[usize], bytes: &[u8]) -> io::Result<()> {
    if offsets.first().copied().unwrap_or_default() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "symbols first offset must be zero",
        ));
    }
    if offsets.last().copied().unwrap_or_default() != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "symbols final offset must match string bytes",
        ));
    }

    let mut previous: Option<&[u8]> = None;
    for pair in offsets.windows(2) {
        let start = pair[0];
        let end = pair[1];
        if end < start || end > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "symbols offsets out of order",
            ));
        }
        let value = &bytes[start..end];
        std::str::from_utf8(value).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "symbols string is not utf-8")
        })?;
        if previous.is_some_and(|prev| prev >= value) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "symbols must be sorted by unique UTF-8 bytes",
            ));
        }
        previous = Some(value);
    }

    Ok(())
}

pub fn write_series_bin(mut writer: impl Write, entries: &[SeriesEntry]) -> io::Result<()> {
    let encoded = build_series_bin_v2(entries)?;
    writer.write_all(&encoded)?;
    Ok(())
}

pub(crate) fn write_canonical_series_bin_rows<E: SeriesEntryView>(
    mut writer: impl Write,
    entries: &[E],
    chunk_ranges: &[ChunkIndexRange],
) -> io::Result<()> {
    if entries.len() != chunk_ranges.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "series and chunk-index range counts differ",
        ));
    }
    let cold = SeriesColdV2Plan::build_canonical_rows(entries)?;
    let encoded = build_series_bin_v2_from_plan(entries, cold, |index| chunk_ranges[index])?;
    writer.write_all(&encoded)
}

pub fn read_series_bin(mut reader: impl Read) -> io::Result<Vec<SeriesEntry>> {
    let bytes = read_all(&mut reader)?;
    let mut reader = SeriesReader::open(Cursor::new(bytes))?;
    let mut entries = Vec::with_capacity(reader.len());
    for series_ref in 0..reader.len() {
        let series_ref = u32::try_from(series_ref)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "series_ref exceeds u32"))?;
        let entry = reader
            .read_entry(series_ref)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "series entry missing"))?;
        entries.push(entry);
    }
    Ok(entries)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeriesHeader {
    num_series: u32,
    num_keysets: u32,
    num_value_dicts: u32,
    series_table_offset: u64,
    keysets_offset: u64,
    value_dicts_offset: u64,
    keyset_blocks_offset: u64,
    meta_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeriesTableEntryV2 {
    series_id: u64,
    kind_mask: u8,
    chunk_index: ChunkIndexRange,
    keyset_id: u32,
    row: u32,
    meta_off: u32,
    meta_len: u32,
}

impl From<SeriesTableEntryV2> for SeriesEntryMetadata {
    fn from(entry: SeriesTableEntryV2) -> Self {
        Self {
            series_id: entry.series_id,
            kind_mask: entry.kind_mask,
            chunk_index: entry.chunk_index,
        }
    }
}

type KeySetBlockMeta = cold_v2_reader::KeySetBlockMeta;
type ValueDictMeta = cold_v2_reader::ValueDictMeta;
type MaterializedSeriesRows = HashMap<(u32, u32), Vec<u8>>;

pub struct SeriesReader<R> {
    reader: R,
    header: SeriesHeader,
    keyset_offsets: Vec<u64>,
    keysets: Vec<Option<Vec<u32>>>,
    value_dict_offsets: Vec<u64>,
    value_dicts: BTreeMap<u32, ValueDictMeta>,
    value_dict_cache: BTreeMap<u32, Vec<u32>>,
    value_dict_value_cache: BTreeMap<(u32, u32), u32>,
    keyset_block_offsets: Vec<u64>,
    keyset_blocks: Vec<Option<KeySetBlockMeta>>,
}

impl<R> SeriesReader<R>
where
    R: Read + Seek,
{
    pub fn open(mut reader: R) -> io::Result<Self> {
        let header = read_series_header(&mut reader)?;
        let keyset_offsets = read_section_offsets(
            &mut reader,
            header.keysets_offset,
            header.value_dicts_offset,
            header.num_keysets,
        )?;
        let value_dict_offsets = read_section_offsets(
            &mut reader,
            header.value_dicts_offset,
            header.keyset_blocks_offset,
            header.num_value_dicts,
        )?;
        let keyset_block_offsets = read_section_offsets(
            &mut reader,
            header.keyset_blocks_offset,
            header.meta_offset,
            header.num_keysets,
        )?;
        Ok(Self {
            reader,
            header,
            keyset_offsets,
            keysets: vec![None; header.num_keysets as usize],
            value_dict_offsets,
            value_dicts: BTreeMap::new(),
            value_dict_cache: BTreeMap::new(),
            value_dict_value_cache: BTreeMap::new(),
            keyset_block_offsets,
            keyset_blocks: vec![None; header.num_keysets as usize],
        })
    }

    pub fn len(&self) -> usize {
        self.header.num_series as usize
    }

    pub fn is_empty(&self) -> bool {
        self.header.num_series == 0
    }

    pub fn read_entry(&mut self, series_ref: u32) -> io::Result<Option<SeriesEntry>> {
        self.read_entry_with_bytes(series_ref)
            .map(|(entry, _bytes_read)| entry)
    }

    pub fn read_entry_with_bytes(
        &mut self,
        series_ref: u32,
    ) -> io::Result<(Option<SeriesEntry>, u64)> {
        if series_ref >= self.header.num_series {
            return Ok((None, 0));
        }

        let table_entry = read_series_table_entry(
            &mut self.reader,
            self.header
                .series_table_offset
                .saturating_add(u64::from(series_ref) * SERIES_TABLE_ENTRY_LEN),
        )?;
        let (entry, materialized_bytes) = self.materialize_entry_with_bytes(table_entry)?;
        Ok((
            Some(entry),
            SERIES_TABLE_ENTRY_LEN.saturating_add(materialized_bytes),
        ))
    }

    pub fn read_entries_with_bytes(
        &mut self,
        series_refs: &[u32],
    ) -> io::Result<(Vec<(u32, SeriesEntry)>, u64)> {
        let (ordered_table_entries, mut bytes_read) =
            self.read_table_entries_with_bytes(series_refs)?;
        let unique_table_entries = ordered_table_entries
            .iter()
            .copied()
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect::<Vec<_>>();
        let (mut rows, row_bytes_read) = self.read_entry_rows_with_bytes(&unique_table_entries)?;
        bytes_read = bytes_read.saturating_add(row_bytes_read);

        let mut materialized = HashMap::with_capacity(unique_table_entries.len());
        for (series_ref, table_entry) in unique_table_entries {
            let (row, row_lookup_bytes) = self.row_bytes_for_table_entry(table_entry, &mut rows)?;
            bytes_read = bytes_read.saturating_add(row_lookup_bytes);
            let (entry, entry_bytes_read) =
                self.materialize_entry_from_row_with_bytes(table_entry, &row)?;
            bytes_read = bytes_read.saturating_add(entry_bytes_read);
            materialized.insert(series_ref, entry);
        }

        let entries = series_refs
            .iter()
            .filter_map(|series_ref| {
                materialized
                    .get(series_ref)
                    .cloned()
                    .map(|entry| (*series_ref, entry))
            })
            .collect();
        Ok((entries, bytes_read))
    }

    pub(crate) fn read_entry_locators_with_bytes(
        &mut self,
        series_refs: &[u32],
    ) -> io::Result<(Vec<(u32, SeriesEntryLocator)>, u64)> {
        let (table_entries, bytes_read) = self.read_table_entries_with_bytes(series_refs)?;
        Ok((
            table_entries
                .into_iter()
                .map(|(series_ref, table_entry)| (series_ref, SeriesEntryLocator { table_entry }))
                .collect(),
            bytes_read,
        ))
    }

    pub(crate) fn read_entries_from_locators_with_bytes(
        &mut self,
        locators: &[(u32, SeriesEntryLocator)],
    ) -> io::Result<(Vec<(u32, SeriesEntry)>, u64)> {
        let unique_table_entries = locators
            .iter()
            .copied()
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .map(|(series_ref, locator)| (series_ref, locator.table_entry))
            .collect::<Vec<_>>();
        let (mut rows, mut bytes_read) = self.read_entry_rows_with_bytes(&unique_table_entries)?;

        let mut materialized = HashMap::with_capacity(unique_table_entries.len());
        for (series_ref, table_entry) in unique_table_entries {
            let (row, row_lookup_bytes) = self.row_bytes_for_table_entry(table_entry, &mut rows)?;
            bytes_read = bytes_read.saturating_add(row_lookup_bytes);
            let (entry, entry_bytes_read) =
                self.materialize_entry_from_row_with_bytes(table_entry, &row)?;
            bytes_read = bytes_read.saturating_add(entry_bytes_read);
            materialized.insert(series_ref, entry);
        }

        let entries = locators
            .iter()
            .filter_map(|(series_ref, _)| {
                materialized
                    .get(series_ref)
                    .cloned()
                    .map(|entry| (*series_ref, entry))
            })
            .collect();
        Ok((entries, bytes_read))
    }

    pub fn read_metadata_entries_with_bytes(
        &mut self,
        series_refs: &[u32],
    ) -> io::Result<(Vec<(u32, SeriesEntryMetadata)>, u64)> {
        let (table_entries, bytes_read) = self.read_table_entries_with_bytes(series_refs)?;
        Ok((
            table_entries
                .into_iter()
                .map(|(series_ref, entry)| (series_ref, SeriesEntryMetadata::from(entry)))
                .collect(),
            bytes_read,
        ))
    }

    fn read_table_entries_with_bytes(
        &mut self,
        series_refs: &[u32],
    ) -> io::Result<(Vec<(u32, SeriesTableEntryV2)>, u64)> {
        let mut valid_refs = series_refs
            .iter()
            .copied()
            .filter(|series_ref| *series_ref < self.header.num_series)
            .collect::<Vec<_>>();
        if valid_refs.is_empty() {
            return Ok((Vec::new(), 0));
        }
        valid_refs.sort_unstable();
        valid_refs.dedup();

        let mut table_entries = HashMap::with_capacity(valid_refs.len());
        let mut bytes_read = 0u64;
        let mut span_start = 0usize;
        while span_start < valid_refs.len() {
            let start_ref = valid_refs[span_start];
            let mut span_end = span_start + 1;
            while span_end < valid_refs.len()
                && valid_refs[span_end] == valid_refs[span_end - 1].saturating_add(1)
            {
                span_end += 1;
            }

            let entry_count = span_end - span_start;
            let read_len = checked_usize(
                checked_mul_u64(
                    entry_count,
                    SERIES_TABLE_ENTRY_LEN as usize,
                    "series table span",
                )?,
                "series table span",
            )?;
            let offset = self
                .header
                .series_table_offset
                .checked_add(u64::from(start_ref) * SERIES_TABLE_ENTRY_LEN)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "series table offset overflow")
                })?;
            let mut bytes = vec![0u8; read_len];
            self.reader.seek(SeekFrom::Start(offset))?;
            self.reader.read_exact(&mut bytes)?;
            bytes_read = bytes_read.saturating_add(read_len as u64);

            for (idx, series_ref) in valid_refs[span_start..span_end].iter().copied().enumerate() {
                let entry_start = idx * SERIES_TABLE_ENTRY_LEN as usize;
                let entry_end = entry_start + SERIES_TABLE_ENTRY_LEN as usize;
                table_entries.insert(
                    series_ref,
                    decode_series_table_entry(&bytes[entry_start..entry_end])?,
                );
            }

            span_start = span_end;
        }

        let ordered_table_entries = series_refs
            .iter()
            .filter_map(|series_ref| {
                table_entries
                    .get(series_ref)
                    .copied()
                    .map(|entry| (*series_ref, entry))
            })
            .collect();
        Ok((ordered_table_entries, bytes_read))
    }

    fn read_entry_rows_with_bytes(
        &mut self,
        table_entries: &[(u32, SeriesTableEntryV2)],
    ) -> io::Result<(MaterializedSeriesRows, u64)> {
        let mut rows_by_keyset: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        let mut bytes_read = 0u64;
        for (_, table_entry) in table_entries {
            let (block, block_bytes_read) = self.keyset_block_with_bytes(table_entry.keyset_id)?;
            bytes_read = bytes_read.saturating_add(block_bytes_read);
            if table_entry.row >= block.rows {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "series row out of bounds",
                ));
            }
            if block.row_len_bytes > 0 {
                rows_by_keyset
                    .entry(table_entry.keyset_id)
                    .or_default()
                    .push(table_entry.row);
            }
        }

        let mut rows = HashMap::new();
        for (keyset_id, mut row_indexes) in rows_by_keyset {
            row_indexes.sort_unstable();
            row_indexes.dedup();
            let (block, block_bytes_read) = self.keyset_block_with_bytes(keyset_id)?;
            bytes_read = bytes_read.saturating_add(block_bytes_read);
            let row_len = block.row_len_bytes as usize;
            let mut span_start = 0usize;
            while span_start < row_indexes.len() {
                let start_row = row_indexes[span_start];
                let mut span_end = span_start + 1;
                while span_end < row_indexes.len()
                    && row_indexes[span_end] == row_indexes[span_end - 1].saturating_add(1)
                {
                    span_end += 1;
                }

                let row_count = span_end - span_start;
                let read_len = checked_usize(
                    checked_mul_u64(row_count, row_len, "series row span")?,
                    "series row span",
                )?;
                let row_offset = cold_v2_reader::keyset_block_row_range(&block, start_row)?.start;
                let mut bytes = vec![0u8; read_len];
                self.reader.seek(SeekFrom::Start(row_offset))?;
                self.reader.read_exact(&mut bytes)?;
                bytes_read = bytes_read.saturating_add(read_len as u64);

                for (idx, row_index) in row_indexes[span_start..span_end]
                    .iter()
                    .copied()
                    .enumerate()
                {
                    let row_start = idx * row_len;
                    let row_end = row_start + row_len;
                    rows.insert((keyset_id, row_index), bytes[row_start..row_end].to_vec());
                }

                span_start = span_end;
            }
        }

        Ok((rows, bytes_read))
    }

    fn row_bytes_for_table_entry(
        &mut self,
        table_entry: SeriesTableEntryV2,
        rows: &mut MaterializedSeriesRows,
    ) -> io::Result<(Vec<u8>, u64)> {
        let (block, bytes_read) = self.keyset_block_with_bytes(table_entry.keyset_id)?;
        if block.row_len_bytes == 0 {
            return Ok((Vec::new(), bytes_read));
        }
        let row = rows
            .remove(&(table_entry.keyset_id, table_entry.row))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "series row missing"))?;
        Ok((row, bytes_read))
    }

    fn materialize_entry_with_bytes(
        &mut self,
        table_entry: SeriesTableEntryV2,
    ) -> io::Result<(SeriesEntry, u64)> {
        if table_entry.meta_len != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series metadata payloads are not supported",
            ));
        }

        let (block, block_bytes_read) = self.keyset_block_with_bytes(table_entry.keyset_id)?;
        if table_entry.row >= block.rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series row out of bounds",
            ));
        }

        let row_len = block.row_len_bytes as usize;
        let mut row = vec![0u8; row_len];
        if row_len > 0 {
            let row_offset = cold_v2_reader::keyset_block_row_range(&block, table_entry.row)?.start;
            self.reader.seek(SeekFrom::Start(row_offset))?;
            self.reader.read_exact(&mut row)?;
        }
        let (entry, dict_bytes_read) =
            self.materialize_entry_from_row_with_bytes(table_entry, &row)?;
        Ok((
            entry,
            block_bytes_read
                .saturating_add(u64::from(block.row_len_bytes))
                .saturating_add(dict_bytes_read),
        ))
    }

    fn materialize_entry_from_row_with_bytes(
        &mut self,
        table_entry: SeriesTableEntryV2,
        row: &[u8],
    ) -> io::Result<(SeriesEntry, u64)> {
        if table_entry.meta_len != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series metadata payloads are not supported",
            ));
        }

        let (keyset, keyset_bytes_read) = self.keyset_with_bytes(table_entry.keyset_id)?;
        let (block, block_bytes_read) = self.keyset_block_with_bytes(table_entry.keyset_id)?;
        if table_entry.row >= block.rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series row out of bounds",
            ));
        }
        cold_v2_reader::validate_keyset_block_key_count(&block, keyset.len())?;
        if row.len() != block.row_len_bytes as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series row length mismatch",
            ));
        }

        let mut bytes_read = keyset_bytes_read.saturating_add(block_bytes_read);
        let mut cursor = 0usize;
        let mut labels = Vec::with_capacity(keyset.len());
        for (idx, key_sym) in keyset.iter().copied().enumerate() {
            let (dict, dict_meta_bytes_read) = self.value_dict_meta_with_bytes(key_sym)?;
            bytes_read = bytes_read.saturating_add(dict_meta_bytes_read);
            cold_v2_reader::validate_value_code_width(block.widths[idx], dict.cardinality)?;
            let code = cold_v2_reader::read_value_code(row, &mut cursor, block.widths[idx])?;
            let (value_sym, dict_value_bytes_read) =
                self.value_dict_value_with_bytes(key_sym, code, dict)?;
            bytes_read = bytes_read.saturating_add(dict_value_bytes_read);
            labels.push((key_sym, value_sym));
        }
        if cursor != row.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series row has trailing bytes",
            ));
        }

        Ok((
            SeriesEntry {
                series_id: table_entry.series_id,
                kind_mask: table_entry.kind_mask,
                chunk_index: table_entry.chunk_index,
                labels,
            },
            bytes_read,
        ))
    }

    fn keyset_with_bytes(&mut self, keyset_id: u32) -> io::Result<(Vec<u32>, u64)> {
        let idx = keyset_id as usize;
        if let Some(keyset) = self.keysets.get(idx).and_then(Option::as_ref) {
            return Ok((keyset.clone(), 0));
        }

        let (start, end) = section_entry_bounds(&self.keyset_offsets, idx, "keyset id")?;
        let read_len = checked_usize(
            end.checked_sub(start).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "keyset bounds invalid")
            })?,
            "keyset entry",
        )?;
        let bytes = read_exact_vec_at(&mut self.reader, start, read_len)?;
        let keyset = cold_v2_reader::decode_keyset_entry(&bytes, start, end)?;
        *self.keysets.get_mut(idx).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "keyset id out of bounds")
        })? = Some(keyset.clone());
        Ok((keyset, read_len as u64))
    }

    fn keyset_block_with_bytes(&mut self, keyset_id: u32) -> io::Result<(KeySetBlockMeta, u64)> {
        let idx = keyset_id as usize;
        if let Some(block) = self.keyset_blocks.get(idx).and_then(Option::as_ref) {
            return Ok((block.clone(), 0));
        }

        let (start, end) =
            section_entry_bounds(&self.keyset_block_offsets, idx, "keyset block id")?;
        let (block, bytes_read) = read_keyset_block_meta_at(&mut self.reader, start, end)?;
        *self.keyset_blocks.get_mut(idx).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "keyset block id out of bounds")
        })? = Some(block.clone());
        Ok((block, bytes_read))
    }

    fn value_dict_meta_with_bytes(&mut self, key_sym: u32) -> io::Result<(ValueDictMeta, u64)> {
        if let Some(meta) = self.value_dicts.get(&key_sym).copied() {
            return Ok((meta, 0));
        }

        let mut bytes_read = 0u64;
        let mut low = 0usize;
        let mut high = self.header.num_value_dicts as usize;
        while low < high {
            let mid = low + (high - low) / 2;
            let (start, end) =
                section_entry_bounds(&self.value_dict_offsets, mid, "value dictionary id")?;
            let meta = read_value_dict_meta_at(&mut self.reader, start, end)?;
            bytes_read = bytes_read.saturating_add(8);
            self.value_dicts.entry(meta.key_sym).or_insert(meta);
            match meta.key_sym.cmp(&key_sym) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return Ok((meta, bytes_read)),
                std::cmp::Ordering::Greater => high = mid,
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "value dictionary missing",
        ))
    }

    fn value_dict_value_with_bytes(
        &mut self,
        key_sym: u32,
        code: u32,
        meta: ValueDictMeta,
    ) -> io::Result<(u32, u64)> {
        let value_range = cold_v2_reader::value_dict_value_range(meta, code)?;
        if meta.cardinality <= VALUE_DICT_FULL_CACHE_MAX_VALUES {
            let (dict, bytes_read) = self.value_dict_with_bytes(key_sym, meta)?;
            let value = dict.get(code as usize).copied().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "value code out of bounds")
            })?;
            return Ok((value, bytes_read));
        }

        if let Some(value) = self.value_dict_value_cache.get(&(key_sym, code)).copied() {
            return Ok((value, 0));
        }

        let bytes = read_exact_vec_at(&mut self.reader, value_range.start, 4)?;
        let value = cold_v2_reader::decode_value_dict_value(&bytes, meta, code)?;
        self.value_dict_value_cache.insert((key_sym, code), value);
        Ok((value, 4))
    }

    fn value_dict_with_bytes(
        &mut self,
        key_sym: u32,
        meta: ValueDictMeta,
    ) -> io::Result<(&[u32], u64)> {
        let mut bytes_read = 0u64;
        if !self.value_dict_cache.contains_key(&key_sym) {
            self.reader.seek(SeekFrom::Start(meta.values_offset))?;
            let read_len = checked_usize(
                checked_mul_u64(meta.cardinality as usize, 4, "value dictionary")?,
                "value dictionary",
            )?;
            let mut bytes = vec![0u8; read_len];
            self.reader.read_exact(&mut bytes)?;
            let values = cold_v2_reader::decode_value_dict_values(&bytes, meta)?;
            bytes_read = bytes_read.saturating_add(u64::from(meta.cardinality).saturating_mul(4));
            self.value_dict_cache.insert(key_sym, values);
        }
        Ok((
            self.value_dict_cache
                .get(&key_sym)
                .map(Vec::as_slice)
                .unwrap(),
            bytes_read,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EncodedSeriesTableEntry {
    series_id: u64,
    kind_mask: u8,
    chunk_index: ChunkIndexRange,
    keyset_id: u32,
    row: u32,
}

fn build_series_bin_v2(entries: &[SeriesEntry]) -> io::Result<Vec<u8>> {
    let cold = SeriesColdV2Plan::build(entries)?;
    build_series_bin_v2_from_plan(entries, cold, |index| entries[index].chunk_index)
}

fn build_series_bin_v2_from_plan<E: SeriesEntryView>(
    entries: &[E],
    cold: SeriesColdV2Plan,
    chunk_index_at: impl Fn(usize) -> ChunkIndexRange,
) -> io::Result<Vec<u8>> {
    let num_series = cold.num_series();
    if cold.series_rows().len() != entries.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series cold plan row count mismatch",
        ));
    }
    let series_table_len = u64::from(num_series)
        .checked_mul(SERIES_TABLE_ENTRY_LEN)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series table too large"))?;
    let cold_lengths = cold.lengths();

    let series_table_offset = SERIES_HEADER_LEN;
    let keysets_offset = series_table_offset
        .checked_add(series_table_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let value_dicts_offset = keysets_offset
        .checked_add(cold_lengths.keysets)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let keyset_blocks_offset = value_dicts_offset
        .checked_add(cold_lengths.value_dicts)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let meta_offset = keyset_blocks_offset
        .checked_add(cold_lengths.keyset_blocks)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let cold_offsets = cold.section_offsets_at(keysets_offset)?;
    if cold_offsets.value_dicts != value_dicts_offset
        || cold_offsets.keyset_blocks != keyset_blocks_offset
        || cold_offsets.end != meta_offset
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series cold plan offsets disagree with v2 header",
        ));
    }

    let header = SeriesHeader {
        num_series,
        num_keysets: cold.num_keysets(),
        num_value_dicts: cold.num_value_dicts(),
        series_table_offset,
        keysets_offset,
        value_dicts_offset,
        keyset_blocks_offset,
        meta_offset,
    };

    let mut out = Vec::with_capacity(checked_usize(meta_offset, "series file size")?);
    write_series_header(&mut out, header)?;
    for (index, cold_row) in cold.series_rows().iter().enumerate() {
        write_series_table_entry(
            &mut out,
            EncodedSeriesTableEntry {
                series_id: cold_row.series_id,
                kind_mask: cold_row.kind_mask,
                chunk_index: chunk_index_at(index),
                keyset_id: cold_row.keyset_id,
                row: cold_row.row,
            },
        );
    }
    cold.append_sections_at(&mut out, cold_offsets)?;
    Ok(out)
}

fn write_series_header(writer: &mut Vec<u8>, header: SeriesHeader) -> io::Result<()> {
    writer.extend_from_slice(&SERIES_MAGIC.to_le_bytes());
    writer.extend_from_slice(&SERIES_VERSION.to_le_bytes());
    writer.extend_from_slice(&0u16.to_le_bytes());
    writer.extend_from_slice(&header.num_series.to_le_bytes());
    writer.extend_from_slice(&header.num_keysets.to_le_bytes());
    writer.extend_from_slice(&header.num_value_dicts.to_le_bytes());
    writer.extend_from_slice(&0u32.to_le_bytes());
    writer.extend_from_slice(&header.series_table_offset.to_le_bytes());
    writer.extend_from_slice(&header.keysets_offset.to_le_bytes());
    writer.extend_from_slice(&header.value_dicts_offset.to_le_bytes());
    writer.extend_from_slice(&header.keyset_blocks_offset.to_le_bytes());
    writer.extend_from_slice(&header.meta_offset.to_le_bytes());
    if writer.len() != SERIES_HEADER_LEN as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series header length mismatch",
        ));
    }
    Ok(())
}

fn write_series_table_entry(writer: &mut Vec<u8>, entry: EncodedSeriesTableEntry) {
    writer.extend_from_slice(&entry.series_id.to_le_bytes());
    writer.push(entry.kind_mask);
    writer.push(0);
    writer.extend_from_slice(&0u16.to_le_bytes());
    writer.extend_from_slice(&entry.chunk_index.offset.to_le_bytes());
    writer.extend_from_slice(&entry.chunk_index.len.to_le_bytes());
    writer.extend_from_slice(&entry.keyset_id.to_le_bytes());
    writer.extend_from_slice(&entry.row.to_le_bytes());
    writer.extend_from_slice(&0u32.to_le_bytes());
    writer.extend_from_slice(&0u32.to_le_bytes());
}

fn read_series_header(reader: &mut (impl Read + Seek)) -> io::Result<SeriesHeader> {
    let bytes = read_exact_vec_at(reader, 0, SERIES_HEADER_LEN as usize)?;
    decode_series_header_v2(&bytes)
}

/// Decodes the exact fixed schema-6 `series.bin` v2 header without performing
/// I/O. Runtime readers reuse this parser after governing the physical range.
fn decode_series_header_v2(bytes: &[u8]) -> io::Result<SeriesHeader> {
    if bytes.len() != SERIES_HEADER_LEN as usize {
        return Err(if bytes.len() < SERIES_HEADER_LEN as usize {
            io::Error::new(io::ErrorKind::UnexpectedEof, "series header is truncated")
        } else {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "series header has trailing bytes",
            )
        });
    }
    let mut cursor = 0usize;
    let magic = read_u32(bytes, &mut cursor)?;
    if magic != SERIES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != SERIES_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported series version",
        ));
    }
    let flags = read_u16(bytes, &mut cursor)?;
    if flags != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series header flags must be zero",
        ));
    }
    let num_series = read_u32(bytes, &mut cursor)?;
    let num_keysets = read_u32(bytes, &mut cursor)?;
    let num_value_dicts = read_u32(bytes, &mut cursor)?;
    let reserved0 = read_u32(bytes, &mut cursor)?;
    if reserved0 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series header reserved field must be zero",
        ));
    }
    let header = SeriesHeader {
        num_series,
        num_keysets,
        num_value_dicts,
        series_table_offset: read_u64(bytes, &mut cursor)?,
        keysets_offset: read_u64(bytes, &mut cursor)?,
        value_dicts_offset: read_u64(bytes, &mut cursor)?,
        keyset_blocks_offset: read_u64(bytes, &mut cursor)?,
        meta_offset: read_u64(bytes, &mut cursor)?,
    };
    if cursor != SERIES_HEADER_LEN as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series header length mismatch",
        ));
    }
    validate_series_header(header)?;
    Ok(header)
}

fn validate_series_header(header: SeriesHeader) -> io::Result<()> {
    if header.series_table_offset != SERIES_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series table offset invalid",
        ));
    }
    let expected_keysets_offset = header
        .series_table_offset
        .checked_add(u64::from(header.num_series) * SERIES_TABLE_ENTRY_LEN)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "series table too large"))?;
    if header.keysets_offset < expected_keysets_offset
        || header.value_dicts_offset < header.keysets_offset
        || header.keyset_blocks_offset < header.value_dicts_offset
        || header.meta_offset < header.keyset_blocks_offset
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series section offsets invalid",
        ));
    }
    Ok(())
}

fn read_series_table_entry(
    reader: &mut (impl Read + Seek),
    offset: u64,
) -> io::Result<SeriesTableEntryV2> {
    reader.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0u8; SERIES_TABLE_ENTRY_LEN as usize];
    reader.read_exact(&mut bytes)?;
    decode_series_table_entry(&bytes)
}

fn decode_series_table_entry(bytes: &[u8]) -> io::Result<SeriesTableEntryV2> {
    if bytes.len() != SERIES_TABLE_ENTRY_LEN as usize {
        return Err(if bytes.len() < SERIES_TABLE_ENTRY_LEN as usize {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "series table entry is truncated",
            )
        } else {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "series table entry has trailing bytes",
            )
        });
    }
    let mut cursor = 0usize;
    let series_id = read_u64(bytes, &mut cursor)?;
    let kind_mask = read_u8(bytes, &mut cursor)?;
    let flags = read_u8(bytes, &mut cursor)?;
    let reserved0 = read_u16(bytes, &mut cursor)?;
    if flags != 0 || reserved0 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series table entry flags and reserved field must be zero",
        ));
    }
    let chunk_index_offset = read_u64(bytes, &mut cursor)?;
    let chunk_index_len = read_u32(bytes, &mut cursor)?;
    let keyset_id = read_u32(bytes, &mut cursor)?;
    let row = read_u32(bytes, &mut cursor)?;
    let meta_off = read_u32(bytes, &mut cursor)?;
    let meta_len = read_u32(bytes, &mut cursor)?;
    if cursor != SERIES_TABLE_ENTRY_LEN as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series table entry length mismatch",
        ));
    }
    Ok(SeriesTableEntryV2 {
        series_id,
        kind_mask,
        chunk_index: ChunkIndexRange {
            offset: chunk_index_offset,
            len: chunk_index_len,
        },
        keyset_id,
        row,
        meta_off,
        meta_len,
    })
}

fn read_value_dict_meta_at(
    reader: &mut (impl Read + Seek),
    start: u64,
    end: u64,
) -> io::Result<ValueDictMeta> {
    let header_range = cold_v2_reader::value_dict_header_range(start, end)?;
    let header = read_exact_vec_at(reader, header_range.start, 8)?;
    cold_v2_reader::decode_value_dict_meta(&header, start, end)
}

fn read_keyset_block_meta_at(
    reader: &mut (impl Read + Seek),
    start: u64,
    end: u64,
) -> io::Result<(KeySetBlockMeta, u64)> {
    let fixed_range = cold_v2_reader::keyset_block_header_range(start, end)?;
    let fixed = read_exact_vec_at(reader, fixed_range.start, 16)?;
    let widths_range = cold_v2_reader::keyset_block_widths_range(&fixed, start, end)?;
    let widths_len_u64 = widths_range.end - widths_range.start;
    let widths_len = checked_usize(widths_len_u64, "keyset block widths")?;
    let widths = if widths_len == 0 {
        Vec::new()
    } else {
        read_exact_vec_at(reader, widths_range.start, widths_len)?
    };
    let block = cold_v2_reader::decode_keyset_block_meta(&fixed, &widths, start, end)?;
    Ok((block, 16u64.saturating_add(widths_len_u64)))
}

fn section_entry_bounds(offsets: &[u64], idx: usize, what: &str) -> io::Result<(u64, u64)> {
    if idx + 1 >= offsets.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{what} out of bounds"),
        ));
    }
    Ok((offsets[idx], offsets[idx + 1]))
}

fn read_section_offsets(
    reader: &mut (impl Read + Seek),
    section_offset: u64,
    section_end: u64,
    entry_count: u32,
) -> io::Result<Vec<u64>> {
    let table_range = cold_v2_reader::offset_table_range(section_offset, section_end, entry_count)?;
    let len = checked_usize(table_range.end - table_range.start, "section offset count")?;
    let bytes = read_exact_vec_at(reader, table_range.start, len)?;
    cold_v2_reader::decode_offset_table(&bytes, section_offset, section_end, entry_count)
}

fn read_exact_vec_at(
    reader: &mut (impl Read + Seek),
    offset: u64,
    len: usize,
) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0u8; len];
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn checked_mul_u64(value: usize, multiplier: usize, what: &str) -> io::Result<u64> {
    let value = checked_u64(value, what)?;
    value
        .checked_mul(u64::try_from(multiplier).unwrap())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("{what} too large")))
}

fn checked_u64(value: usize, what: &str) -> io::Result<u64> {
    u64::try_from(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("{what} exceeds u64")))
}

fn checked_usize(value: u64, what: &str) -> io::Result<usize> {
    usize::try_from(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("{what} exceeds usize")))
}

fn read_all(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> io::Result<u8> {
    if *cursor >= bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let value = bytes[*cursor];
    *cursor += 1;
    Ok(value)
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

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    #[derive(Debug)]
    struct CompactSeriesEntry {
        series_id: u64,
        kind_mask: u8,
        labels: Vec<(u32, u32)>,
    }

    impl SeriesEntryView for CompactSeriesEntry {
        fn series_id(&self) -> u64 {
            self.series_id
        }

        fn kind_mask(&self) -> u8 {
            self.kind_mask
        }

        fn labels(&self) -> &[(u32, u32)] {
            &self.labels
        }
    }

    struct CountingCursor {
        cursor: Cursor<Vec<u8>>,
        bytes_read: u64,
        read_calls: u64,
        seek_calls: u64,
    }

    impl CountingCursor {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                cursor: Cursor::new(bytes),
                bytes_read: 0,
                read_calls: 0,
                seek_calls: 0,
            }
        }

        fn bytes_read(&self) -> u64 {
            self.bytes_read
        }

        fn read_calls(&self) -> u64 {
            self.read_calls
        }

        fn seek_calls(&self) -> u64 {
            self.seek_calls
        }
    }

    impl Read for CountingCursor {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let read = self.cursor.read(buf)?;
            self.bytes_read = self.bytes_read.saturating_add(read as u64);
            self.read_calls = self.read_calls.saturating_add(1);
            Ok(read)
        }
    }

    impl Seek for CountingCursor {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            self.seek_calls = self.seek_calls.saturating_add(1);
            self.cursor.seek(pos)
        }
    }

    #[test]
    fn symbols_bin_v3_roundtrips_sorted_dictionary_and_lookup_ids() {
        let mut symbols = SegmentSymbols::default();
        let zeta = symbols.intern("zeta");
        let alpha = symbols.intern("alpha");
        let metric = symbols.intern("__name__");

        let (sorted, remap) = symbols.sorted_remap().unwrap();
        assert_eq!(remap[zeta as usize], 2);
        assert_eq!(remap[alpha as usize], 1);
        assert_eq!(remap[metric as usize], 0);

        let mut bytes = Vec::new();
        write_symbols_bin(&mut bytes, &sorted).unwrap();
        assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 3);

        let decoded = read_symbols_bin(Cursor::new(bytes)).unwrap();

        assert!(decoded.has_packed_storage());
        assert_eq!(decoded.resolve(0), Some("__name__"));
        assert_eq!(decoded.resolve(1), Some("alpha"));
        assert_eq!(decoded.resolve(2), Some("zeta"));
        assert_eq!(decoded.lookup("__name__"), Some(0));
        assert_eq!(decoded.lookup("alpha"), Some(1));
        assert_eq!(decoded.lookup("zeta"), Some(2));
        assert_eq!(decoded.lookup("missing"), None);
    }

    #[test]
    fn read_backed_symbols_materialize_only_when_mutated() {
        let mut symbols = SegmentSymbols::default();
        symbols.intern("alpha");
        symbols.intern("omega");
        let (sorted, _) = symbols.sorted_remap().unwrap();

        let mut bytes = Vec::new();
        write_symbols_bin(&mut bytes, &sorted).unwrap();
        let mut decoded = read_symbols_bin(Cursor::new(bytes)).unwrap();

        assert!(decoded.has_packed_storage());
        assert_eq!(decoded.intern("omega"), 1);
        assert!(decoded.has_packed_storage());

        assert_eq!(decoded.intern("zeta"), 2);
        assert!(!decoded.has_packed_storage());
        assert_eq!(decoded.lookup("alpha"), Some(0));
        assert_eq!(decoded.lookup("omega"), Some(1));
        assert_eq!(decoded.lookup("zeta"), Some(2));
        assert_eq!(decoded.resolve(2), Some("zeta"));
    }

    #[test]
    fn build_series_bin_v2_preallocates_exact_output_size() {
        let entries = vec![
            SeriesEntry {
                series_id: 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![(1, 10), (2, 20)],
            },
            SeriesEntry {
                series_id: 2,
                kind_mask: SERIES_KIND_HISTOGRAM,
                chunk_index: Default::default(),
                labels: vec![(1, 11), (2, 20)],
            },
        ];

        let encoded = build_series_bin_v2(&entries).unwrap();

        assert_eq!(encoded.capacity(), encoded.len());
    }

    #[test]
    fn build_series_bin_v2_matches_pre_refactor_golden_and_roundtrips() {
        let entries = vec![
            SeriesEntry {
                series_id: 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: ChunkIndexRange { offset: 7, len: 8 },
                labels: vec![(2, 20), (1, 10)],
            },
            SeriesEntry {
                series_id: 2,
                kind_mask: SERIES_KIND_HISTOGRAM,
                chunk_index: ChunkIndexRange { offset: 9, len: 10 },
                labels: vec![(1, 11), (2, 20)],
            },
        ];

        let encoded = build_series_bin_v2(&entries).unwrap();
        assert_eq!(encoded.len(), 264);
        assert_eq!(
            Sha256::digest(&encoded).as_slice(),
            &[
                0x2f, 0x82, 0x26, 0x63, 0xb0, 0x70, 0x0c, 0x73, 0xf7, 0xba, 0x95, 0x95, 0xc4, 0x1d,
                0xcc, 0xf5, 0x4f, 0xc9, 0x02, 0x7f, 0xbe, 0x96, 0x05, 0x61, 0x27, 0xb5, 0xaa, 0x58,
                0xad, 0x82, 0xde, 0x17,
            ]
        );

        let decoded = read_series_bin(Cursor::new(encoded)).unwrap();
        assert_eq!(decoded[0].series_id, entries[0].series_id);
        assert_eq!(decoded[0].kind_mask, entries[0].kind_mask);
        assert_eq!(decoded[0].chunk_index, entries[0].chunk_index);
        assert_eq!(decoded[0].labels, vec![(1, 10), (2, 20)]);
        assert_eq!(decoded[1], entries[1]);
    }

    #[test]
    fn compact_canonical_rows_preserve_schema6_bytes() {
        let entries = vec![
            SeriesEntry {
                series_id: 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: ChunkIndexRange { offset: 0, len: 0 },
                labels: Vec::new(),
            },
            SeriesEntry {
                series_id: 2,
                kind_mask: SERIES_KIND_HISTOGRAM,
                chunk_index: ChunkIndexRange {
                    offset: 17,
                    len: 40,
                },
                labels: vec![(1, 10), (2, 20)],
            },
            SeriesEntry {
                series_id: u64::MAX,
                kind_mask: SERIES_KIND_EXPONENTIAL_HISTOGRAM | SERIES_KIND_SUMMARY,
                chunk_index: ChunkIndexRange {
                    offset: u64::MAX,
                    len: u32::MAX,
                },
                labels: vec![(1, 11), (3, 30), (4, 40)],
            },
        ];
        let compact = entries
            .iter()
            .map(|entry| CompactSeriesEntry {
                series_id: entry.series_id,
                kind_mask: entry.kind_mask,
                labels: entry.labels.clone(),
            })
            .collect::<Vec<_>>();
        let ranges = entries
            .iter()
            .map(|entry| entry.chunk_index)
            .collect::<Vec<_>>();

        let mut embedded = Vec::new();
        write_series_bin(&mut embedded, &entries).unwrap();
        let mut positional = Vec::new();
        write_canonical_series_bin_rows(&mut positional, &compact, &ranges).unwrap();

        assert_eq!(positional, embedded);
    }

    #[test]
    fn compact_canonical_rows_reject_chunk_range_count_mismatch_without_writing() {
        let entries = [CompactSeriesEntry {
            series_id: 1,
            kind_mask: SERIES_KIND_FLOAT,
            labels: vec![(1, 10)],
        }];
        let mut output = Vec::new();

        let error = write_canonical_series_bin_rows(&mut output, &entries, &[]).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("range counts differ"));
        assert!(output.is_empty());
    }

    #[test]
    fn compact_canonical_rows_reject_noncanonical_labels_without_writing() {
        for labels in [vec![(2, 20), (1, 10)], vec![(1, 10), (1, 11)]] {
            let entries = [CompactSeriesEntry {
                series_id: 1,
                kind_mask: SERIES_KIND_FLOAT,
                labels,
            }];
            let ranges = [ChunkIndexRange {
                offset: 128,
                len: 40,
            }];
            let mut output = Vec::new();

            let error =
                write_canonical_series_bin_rows(&mut output, &entries, &ranges).unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(
                error
                    .to_string()
                    .contains("label keys are not strictly increasing")
            );
            assert!(output.is_empty());
        }
    }

    #[test]
    fn series_reader_open_does_not_eagerly_read_large_value_dictionaries() {
        let mut entries = Vec::new();
        for idx in 0..20_000u32 {
            entries.push(SeriesEntry {
                series_id: u64::from(idx) + 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![(1, idx), (2, 42)],
            });
        }
        let encoded = build_series_bin_v2(&entries).unwrap();
        let mut reader = SeriesReader::open(CountingCursor::new(encoded)).unwrap();

        assert!(
            reader.reader.bytes_read() < 32 * 1024,
            "SeriesReader::open should read metadata only, read {} bytes",
            reader.reader.bytes_read()
        );

        let entry = reader.read_entry(19_999).unwrap().unwrap();
        assert_eq!(entry.labels, vec![(1, 19_999), (2, 42)]);
    }

    #[test]
    fn series_reader_open_batches_metadata_reads() {
        let entries = (0..64u32)
            .map(|idx| SeriesEntry {
                series_id: u64::from(idx) + 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![(idx + 1, idx + 100), (1_000 + idx, idx + 200)],
            })
            .collect::<Vec<_>>();
        let encoded = build_series_bin_v2(&entries).unwrap();
        let mut reader = SeriesReader::open(CountingCursor::new(encoded)).unwrap();

        assert!(
            reader.reader.read_calls() <= 4,
            "SeriesReader::open should batch metadata reads, got {} read calls",
            reader.reader.read_calls()
        );

        let entry = reader.read_entry(63).unwrap().unwrap();
        assert_eq!(entry.series_id, 64);
    }

    #[test]
    fn series_reader_materializes_sparse_entry_without_full_value_dictionary_read() {
        let entries = (0..20_000u32)
            .map(|idx| SeriesEntry {
                series_id: u64::from(idx) + 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: Default::default(),
                labels: vec![(1, idx), (2, 42)],
            })
            .collect::<Vec<_>>();
        let encoded = build_series_bin_v2(&entries).unwrap();
        let mut reader = SeriesReader::open(CountingCursor::new(encoded)).unwrap();

        let (loaded, bytes_read) = reader.read_entries_with_bytes(&[19_999]).unwrap();

        assert_eq!(loaded[0].1.labels, vec![(1, 19_999), (2, 42)]);
        assert!(
            bytes_read < 1024,
            "sparse entry materialization should not read whole value dictionaries, read {bytes_read} bytes"
        );
    }

    #[test]
    fn series_reader_batch_reads_table_entries_in_coalesced_spans() {
        let entries = (0..8u32)
            .map(|idx| SeriesEntry {
                series_id: u64::from(idx) + 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: ChunkIndexRange {
                    offset: u64::from(idx) * 10,
                    len: idx + 1,
                },
                labels: vec![(1, idx), (2, 100 + idx)],
            })
            .collect::<Vec<_>>();
        let mut encoded = Vec::new();
        write_series_bin(&mut encoded, &entries).unwrap();
        let mut reader = SeriesReader::open(CountingCursor::new(encoded)).unwrap();
        let read_calls_before = reader.reader.read_calls();
        let seek_calls_before = reader.reader.seek_calls();

        let (loaded, bytes_read) = reader.read_entries_with_bytes(&[3, 1, 2]).unwrap();

        let loaded_refs = loaded
            .iter()
            .map(|(series_ref, _)| *series_ref)
            .collect::<Vec<_>>();
        assert_eq!(loaded_refs, vec![3, 1, 2]);
        assert_eq!(loaded[0].1.series_id, 4);
        assert_eq!(loaded[1].1.series_id, 2);
        assert_eq!(loaded[2].1.labels, vec![(1, 2), (2, 102)]);
        assert!(bytes_read >= 3 * SERIES_TABLE_ENTRY_LEN);

        let read_calls = reader.reader.read_calls() - read_calls_before;
        let seek_calls = reader.reader.seek_calls() - seek_calls_before;
        assert!(
            read_calls < 4 * 9,
            "batch reader should not decode each table entry through per-field reads, got {read_calls} read calls"
        );
        assert!(
            seek_calls <= 10,
            "batch reader should coalesce contiguous table and row spans while loading lazy metadata once, got {seek_calls} seeks"
        );
    }

    #[test]
    fn series_reader_materializes_entries_from_cached_locators_without_table_reread() {
        let entries = (0..8u32)
            .map(|idx| SeriesEntry {
                series_id: u64::from(idx) + 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: ChunkIndexRange {
                    offset: u64::from(idx) * 10,
                    len: idx + 1,
                },
                labels: vec![(1, idx), (2, 100 + idx)],
            })
            .collect::<Vec<_>>();
        let mut encoded = Vec::new();
        write_series_bin(&mut encoded, &entries).unwrap();

        let refs = [3, 1, 2];
        let mut locator_reader = SeriesReader::open(CountingCursor::new(encoded.clone())).unwrap();
        let (locators, locator_bytes) = locator_reader
            .read_entry_locators_with_bytes(&refs)
            .unwrap();
        let (loaded_from_locators, materialized_bytes) = locator_reader
            .read_entries_from_locators_with_bytes(&locators)
            .unwrap();

        let mut full_reader = SeriesReader::open(CountingCursor::new(encoded)).unwrap();
        let (loaded_from_full_read, full_bytes) =
            full_reader.read_entries_with_bytes(&refs).unwrap();

        assert_eq!(loaded_from_locators, loaded_from_full_read);
        assert_eq!(locator_bytes + materialized_bytes, full_bytes);
        assert!(
            materialized_bytes < full_bytes,
            "locator-based materialization should not reread fixed table entries"
        );
    }

    #[test]
    fn series_reader_batch_reads_keyset_rows_in_coalesced_spans() {
        let entries = (0..24u32)
            .map(|idx| SeriesEntry {
                series_id: u64::from(idx) + 1,
                kind_mask: SERIES_KIND_FLOAT,
                chunk_index: ChunkIndexRange {
                    offset: u64::from(idx) * 10,
                    len: idx + 1,
                },
                labels: vec![(1, idx), (2, 100 + idx)],
            })
            .collect::<Vec<_>>();
        let mut encoded = Vec::new();
        write_series_bin(&mut encoded, &entries).unwrap();
        let mut reader = SeriesReader::open(CountingCursor::new(encoded)).unwrap();
        let read_calls_before = reader.reader.read_calls();
        let seek_calls_before = reader.reader.seek_calls();

        let refs = (4..20).collect::<Vec<_>>();
        let (loaded, bytes_read) = reader.read_entries_with_bytes(&refs).unwrap();

        assert_eq!(loaded.len(), refs.len());
        assert_eq!(loaded[0].0, 4);
        assert_eq!(loaded[0].1.labels, vec![(1, 4), (2, 104)]);
        assert_eq!(loaded.last().unwrap().0, 19);
        assert_eq!(loaded.last().unwrap().1.labels, vec![(1, 19), (2, 119)]);
        assert!(bytes_read >= refs.len() as u64 * SERIES_TABLE_ENTRY_LEN);

        let read_calls = reader.reader.read_calls() - read_calls_before;
        let seek_calls = reader.reader.seek_calls() - seek_calls_before;
        assert!(
            read_calls <= 10,
            "batch reader should read adjacent keyset rows as spans while loading lazy metadata once, got {read_calls} read calls"
        );
        assert!(
            seek_calls <= 10,
            "batch reader should seek by row span while loading lazy metadata once, got {seek_calls} seeks"
        );
    }
}
