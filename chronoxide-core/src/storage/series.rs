use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};

use crate::storage::chunk::ChunkIndexRange;

const SYMBOLS_MAGIC: u32 = u32::from_le_bytes(*b"SYMB");
const SYMBOLS_VERSION: u16 = 2;
const SERIES_MAGIC: u32 = u32::from_le_bytes(*b"SERI");
const SERIES_VERSION: u16 = 2;
const SERIES_HEADER_LEN: u64 = 64;
const SERIES_TABLE_ENTRY_LEN: u64 = 40;

pub const SERIES_KIND_FLOAT: u8 = 0b0000_0001;
pub const SERIES_KIND_INT64: u8 = 0b0000_0010;
pub const SERIES_KIND_HISTOGRAM: u8 = 0b0000_0100;
pub const SERIES_KIND_EXPONENTIAL_HISTOGRAM: u8 = 0b0000_1000;
pub const SERIES_KIND_SUMMARY: u8 = 0b0001_0000;

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

pub fn write_symbols_bin(mut writer: impl Write, symbols: &SegmentSymbols) -> io::Result<()> {
    validate_sorted_symbols(symbols)?;
    let symbol_count = u32::try_from(symbols.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "symbol count exceeds u32"))?;
    let mut string_bytes = Vec::new();
    let mut offsets = Vec::with_capacity(symbols.len() + 1);
    offsets.push(0u64);
    for symbol_id in 0..symbols.len() {
        let value = symbols
            .resolve(u32::try_from(symbol_id).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "symbol count exceeds u32")
            })?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "symbol id is missing"))?;
        string_bytes.extend_from_slice(value.as_bytes());
        offsets.push(string_bytes.len() as u64);
    }

    writer.write_all(&SYMBOLS_MAGIC.to_le_bytes())?;
    writer.write_all(&SYMBOLS_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&symbol_count.to_le_bytes())?;
    for offset in offsets {
        writer.write_all(&offset.to_le_bytes())?;
    }
    writer.write_all(&string_bytes)?;
    Ok(())
}

pub fn read_symbols_bin(mut reader: impl Read) -> io::Result<SegmentSymbols> {
    let bytes = read_all(&mut reader)?;
    let mut cursor = 0usize;
    let magic = read_u32(&bytes, &mut cursor)?;
    if magic != SYMBOLS_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "symbols magic mismatch",
        ));
    }
    let version = read_u16(&bytes, &mut cursor)?;
    if version != SYMBOLS_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported symbols version",
        ));
    }
    let _flags = read_u16(&bytes, &mut cursor)?;
    let count = read_u32(&bytes, &mut cursor)? as usize;

    let mut offsets = Vec::with_capacity(count + 1);
    for _ in 0..=count {
        offsets.push(read_u64(&bytes, &mut cursor)? as usize);
    }

    let strings_start = cursor;
    let strings_len = offsets.last().copied().unwrap_or(0);
    if offsets.first().copied().unwrap_or_default() != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "symbols first offset must be zero",
        ));
    }
    if strings_start + strings_len > bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "symbols string section out of bounds",
        ));
    }
    if strings_start + strings_len != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "symbols file has trailing bytes",
        ));
    }

    let strings = &bytes[strings_start..strings_start + strings_len];
    for pair in offsets.windows(2) {
        let start = pair[0];
        let end = pair[1];
        if end < start || end > strings.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "symbols offsets out of order",
            ));
        }
        let value = std::str::from_utf8(&strings[start..end]).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "symbols string is not utf-8")
        })?;
        let _ = value;
    }
    SegmentSymbols::from_sorted_bytes(offsets, strings.to_vec())
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeySetBlockMeta {
    rows: u32,
    key_count: u32,
    row_len_bytes: u32,
    data_len: u32,
    widths: Vec<u8>,
    data_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ValueDictMeta {
    values_offset: u64,
    cardinality: u32,
}

pub struct SeriesReader<R> {
    reader: R,
    header: SeriesHeader,
    keysets: Vec<Vec<u32>>,
    value_dicts: BTreeMap<u32, ValueDictMeta>,
    value_dict_cache: BTreeMap<u32, Vec<u32>>,
    keyset_blocks: Vec<KeySetBlockMeta>,
}

impl<R> SeriesReader<R>
where
    R: Read + Seek,
{
    pub fn open(mut reader: R) -> io::Result<Self> {
        let header = read_series_header(&mut reader)?;
        let keysets = read_keysets_section(&mut reader, &header)?;
        let value_dicts = read_value_dicts_metadata(&mut reader, &header)?;
        let keyset_blocks = read_keyset_blocks_metadata(&mut reader, &header)?;
        Ok(Self {
            reader,
            header,
            keysets,
            value_dicts,
            value_dict_cache: BTreeMap::new(),
            keyset_blocks,
        })
    }

    pub fn len(&self) -> usize {
        self.header.num_series as usize
    }

    pub fn is_empty(&self) -> bool {
        self.header.num_series == 0
    }

    pub fn read_entry(&mut self, series_ref: u32) -> io::Result<Option<SeriesEntry>> {
        if series_ref >= self.header.num_series {
            return Ok(None);
        }

        let table_entry = read_series_table_entry(
            &mut self.reader,
            self.header
                .series_table_offset
                .saturating_add(u64::from(series_ref) * SERIES_TABLE_ENTRY_LEN),
        )?;
        self.materialize_entry(table_entry).map(Some)
    }

    fn materialize_entry(&mut self, table_entry: SeriesTableEntryV2) -> io::Result<SeriesEntry> {
        if table_entry.meta_len != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series metadata payloads are not supported",
            ));
        }

        let keyset = self
            .keysets
            .get(table_entry.keyset_id as usize)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "keyset id out of bounds"))?;
        let block = self
            .keyset_blocks
            .get(table_entry.keyset_id as usize)
            .cloned()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "keyset block id out of bounds")
            })?;
        if table_entry.row >= block.rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series row out of bounds",
            ));
        }
        if keyset.len() != block.key_count as usize || keyset.len() != block.widths.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "keyset block width count mismatch",
            ));
        }

        let row_len = block.row_len_bytes as usize;
        let mut row = vec![0u8; row_len];
        if row_len > 0 {
            let row_offset = block
                .data_offset
                .checked_add(u64::from(table_entry.row) * u64::from(block.row_len_bytes))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "series row offset overflow")
                })?;
            self.reader.seek(SeekFrom::Start(row_offset))?;
            self.reader.read_exact(&mut row)?;
        }

        let mut cursor = 0usize;
        let mut labels = Vec::with_capacity(keyset.len());
        for (idx, key_sym) in keyset.iter().copied().enumerate() {
            let code = read_value_code(&row, &mut cursor, block.widths[idx])?;
            let dict = self.value_dict(key_sym)?;
            let value_sym = dict.get(code as usize).copied().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "value code out of bounds")
            })?;
            labels.push((key_sym, value_sym));
        }
        if cursor != row.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series row has trailing bytes",
            ));
        }

        Ok(SeriesEntry {
            series_id: table_entry.series_id,
            kind_mask: table_entry.kind_mask,
            chunk_index: table_entry.chunk_index,
            labels,
        })
    }

    fn value_dict(&mut self, key_sym: u32) -> io::Result<&[u32]> {
        if !self.value_dict_cache.contains_key(&key_sym) {
            let meta = *self.value_dicts.get(&key_sym).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "value dictionary missing")
            })?;
            self.reader.seek(SeekFrom::Start(meta.values_offset))?;
            let mut values = Vec::with_capacity(meta.cardinality as usize);
            for _ in 0..meta.cardinality {
                values.push(read_exact_u32(&mut self.reader)?);
            }
            self.value_dict_cache.insert(key_sym, values);
        }
        Ok(self
            .value_dict_cache
            .get(&key_sym)
            .map(Vec::as_slice)
            .unwrap())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedSeriesEntry {
    series_id: u64,
    kind_mask: u8,
    chunk_index: ChunkIndexRange,
    labels: Vec<(u32, u32)>,
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
    let normalized = normalize_series_entries(entries);
    let keysets = collect_keysets(&normalized);
    let keyset_ids: BTreeMap<Vec<u32>, u32> = keysets
        .iter()
        .enumerate()
        .map(|(idx, keyset)| (keyset.clone(), idx as u32))
        .collect();
    let value_dicts = collect_value_dicts(&normalized);
    let value_codes = value_code_maps(&value_dicts);

    let mut rows_by_keyset: Vec<Vec<Vec<u32>>> = vec![Vec::new(); keysets.len()];
    let mut series_table = Vec::with_capacity(normalized.len());
    for entry in &normalized {
        let keyset: Vec<u32> = entry.labels.iter().map(|(key, _)| *key).collect();
        let keyset_id = *keyset_ids
            .get(&keyset)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "series keyset missing"))?;
        let row = u32::try_from(rows_by_keyset[keyset_id as usize].len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "keyset row count exceeds u32")
        })?;
        let mut codes = Vec::with_capacity(entry.labels.len());
        for (key, value) in &entry.labels {
            let code = value_codes
                .get(key)
                .and_then(|codes| codes.get(value))
                .copied()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "series value code missing")
                })?;
            codes.push(code);
        }
        rows_by_keyset[keyset_id as usize].push(codes);
        series_table.push(EncodedSeriesTableEntry {
            series_id: entry.series_id,
            kind_mask: entry.kind_mask,
            chunk_index: entry.chunk_index,
            keyset_id,
            row,
        });
    }

    let num_series = checked_u32(normalized.len(), "series count")?;
    let num_keysets = checked_u32(keysets.len(), "keyset count")?;
    let num_value_dicts = checked_u32(value_dicts.len(), "value dictionary count")?;

    let series_table_len = u64::from(num_series) * SERIES_TABLE_ENTRY_LEN;
    let keysets_len = keysets_section_len(&keysets)?;
    let value_dicts_len = value_dicts_section_len(&value_dicts)?;
    let keyset_blocks_len = keyset_blocks_section_len(&keysets, &rows_by_keyset, &value_dicts)?;

    let series_table_offset = SERIES_HEADER_LEN;
    let keysets_offset = series_table_offset
        .checked_add(series_table_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let value_dicts_offset = keysets_offset
        .checked_add(keysets_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let keyset_blocks_offset = value_dicts_offset
        .checked_add(value_dicts_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let meta_offset = keyset_blocks_offset
        .checked_add(keyset_blocks_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;

    let header = SeriesHeader {
        num_series,
        num_keysets,
        num_value_dicts,
        series_table_offset,
        keysets_offset,
        value_dicts_offset,
        keyset_blocks_offset,
        meta_offset,
    };

    let mut out = Vec::with_capacity(checked_usize(meta_offset, "series file size")?);
    write_series_header(&mut out, header)?;
    for entry in &series_table {
        write_series_table_entry(&mut out, *entry);
    }
    write_keysets_section(&mut out, keysets_offset, &keysets)?;
    write_value_dicts_section(&mut out, value_dicts_offset, &value_dicts)?;
    write_keyset_blocks_section(
        &mut out,
        keyset_blocks_offset,
        &keysets,
        &rows_by_keyset,
        &value_dicts,
    )?;
    Ok(out)
}

fn normalize_series_entries(entries: &[SeriesEntry]) -> Vec<NormalizedSeriesEntry> {
    entries
        .iter()
        .map(|entry| {
            let mut labels = entry.labels.clone();
            labels.sort_by_key(|(key, _)| *key);
            NormalizedSeriesEntry {
                series_id: entry.series_id,
                kind_mask: entry.kind_mask,
                chunk_index: entry.chunk_index,
                labels,
            }
        })
        .collect()
}

fn collect_keysets(entries: &[NormalizedSeriesEntry]) -> Vec<Vec<u32>> {
    let mut keysets = BTreeSet::new();
    for entry in entries {
        keysets.insert(entry.labels.iter().map(|(key, _)| *key).collect::<Vec<_>>());
    }
    keysets.into_iter().collect()
}

fn collect_value_dicts(entries: &[NormalizedSeriesEntry]) -> Vec<(u32, Vec<u32>)> {
    let mut values_by_key: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
    for entry in entries {
        for (key, value) in &entry.labels {
            values_by_key.entry(*key).or_default().insert(*value);
        }
    }
    values_by_key
        .into_iter()
        .map(|(key, values)| (key, values.into_iter().collect()))
        .collect()
}

fn value_code_maps(value_dicts: &[(u32, Vec<u32>)]) -> BTreeMap<u32, BTreeMap<u32, u32>> {
    value_dicts
        .iter()
        .map(|(key, values)| {
            let codes = values
                .iter()
                .enumerate()
                .map(|(idx, value)| (*value, idx as u32))
                .collect();
            (*key, codes)
        })
        .collect()
}

fn keysets_section_len(keysets: &[Vec<u32>]) -> io::Result<u64> {
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    keysets.iter().try_fold(offsets_len, |len, keyset| {
        len.checked_add(8 + checked_mul_u64(keyset.len(), 4, "keyset length")?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))
    })
}

fn value_dicts_section_len(value_dicts: &[(u32, Vec<u32>)]) -> io::Result<u64> {
    let offsets_len = checked_section_offsets_len(value_dicts.len())?;
    value_dicts
        .iter()
        .try_fold(offsets_len, |len, (_, values)| {
            len.checked_add(8 + checked_mul_u64(values.len(), 4, "value dictionary length")?)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))
        })
}

fn keyset_blocks_section_len(
    keysets: &[Vec<u32>],
    rows_by_keyset: &[Vec<Vec<u32>>],
    value_dicts: &[(u32, Vec<u32>)],
) -> io::Result<u64> {
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    let dict_by_key: BTreeMap<u32, &[u32]> = value_dicts
        .iter()
        .map(|(key, values)| (*key, values.as_slice()))
        .collect();
    keysets
        .iter()
        .enumerate()
        .try_fold(offsets_len, |len, (idx, keyset)| {
            let widths = widths_for_keyset(keyset, &dict_by_key);
            let row_len: u64 = widths.iter().map(|width| u64::from(*width)).sum();
            let rows = rows_by_keyset
                .get(idx)
                .map(Vec::len)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "keyset rows missing"))?;
            let data_len = row_len
                .checked_mul(u64::try_from(rows).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "row count exceeds u64")
                })?)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "series file too large")
                })?;
            len.checked_add(16 + checked_u64(widths.len(), "width count")? + data_len)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))
        })
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

fn write_keysets_section(
    writer: &mut Vec<u8>,
    section_offset: u64,
    keysets: &[Vec<u32>],
) -> io::Result<()> {
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    let mut cursor = section_offset
        .checked_add(offsets_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let mut entries = Vec::new();
    let mut offsets = Vec::with_capacity(keysets.len() + 1);
    for keyset in keysets {
        offsets.push(cursor);
        entries.extend_from_slice(&checked_u32(keyset.len(), "keyset length")?.to_le_bytes());
        entries.extend_from_slice(&0u32.to_le_bytes());
        for key in keyset {
            entries.extend_from_slice(&key.to_le_bytes());
        }
        cursor = cursor
            .checked_add(8 + checked_mul_u64(keyset.len(), 4, "keyset length")?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    }
    offsets.push(cursor);
    for offset in offsets {
        writer.extend_from_slice(&offset.to_le_bytes());
    }
    writer.extend_from_slice(&entries);
    Ok(())
}

fn write_value_dicts_section(
    writer: &mut Vec<u8>,
    section_offset: u64,
    value_dicts: &[(u32, Vec<u32>)],
) -> io::Result<()> {
    let offsets_len = checked_section_offsets_len(value_dicts.len())?;
    let mut cursor = section_offset
        .checked_add(offsets_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let mut entries = Vec::new();
    let mut offsets = Vec::with_capacity(value_dicts.len() + 1);
    for (key, values) in value_dicts {
        offsets.push(cursor);
        entries.extend_from_slice(&key.to_le_bytes());
        entries.extend_from_slice(
            &checked_u32(values.len(), "value dictionary length")?.to_le_bytes(),
        );
        for value in values {
            entries.extend_from_slice(&value.to_le_bytes());
        }
        cursor = cursor
            .checked_add(8 + checked_mul_u64(values.len(), 4, "value dictionary length")?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    }
    offsets.push(cursor);
    for offset in offsets {
        writer.extend_from_slice(&offset.to_le_bytes());
    }
    writer.extend_from_slice(&entries);
    Ok(())
}

fn write_keyset_blocks_section(
    writer: &mut Vec<u8>,
    section_offset: u64,
    keysets: &[Vec<u32>],
    rows_by_keyset: &[Vec<Vec<u32>>],
    value_dicts: &[(u32, Vec<u32>)],
) -> io::Result<()> {
    let dict_by_key: BTreeMap<u32, &[u32]> = value_dicts
        .iter()
        .map(|(key, values)| (*key, values.as_slice()))
        .collect();
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    let mut cursor = section_offset
        .checked_add(offsets_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    let mut entries = Vec::new();
    let mut offsets = Vec::with_capacity(keysets.len() + 1);

    for (idx, keyset) in keysets.iter().enumerate() {
        let rows = rows_by_keyset
            .get(idx)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "keyset rows missing"))?;
        let widths = widths_for_keyset(keyset, &dict_by_key);
        let row_len: usize = widths.iter().map(|width| usize::from(*width)).sum();
        let data_len = row_len.checked_mul(rows.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "series block data too large")
        })?;

        offsets.push(cursor);
        entries.extend_from_slice(&checked_u32(rows.len(), "keyset block rows")?.to_le_bytes());
        entries.extend_from_slice(&checked_u32(keyset.len(), "keyset length")?.to_le_bytes());
        entries.extend_from_slice(&checked_u32(row_len, "keyset row length")?.to_le_bytes());
        entries.extend_from_slice(&checked_u32(data_len, "keyset data length")?.to_le_bytes());
        entries.extend_from_slice(&widths);
        for row in rows {
            for (value_idx, code) in row.iter().copied().enumerate() {
                write_value_code(&mut entries, code, widths[value_idx])?;
            }
        }
        cursor = cursor
            .checked_add(
                16 + checked_u64(widths.len(), "width count")?
                    + checked_u64(data_len, "data length")?,
            )
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "series file too large"))?;
    }
    offsets.push(cursor);
    for offset in offsets {
        writer.extend_from_slice(&offset.to_le_bytes());
    }
    writer.extend_from_slice(&entries);
    Ok(())
}

fn widths_for_keyset(keyset: &[u32], dict_by_key: &BTreeMap<u32, &[u32]>) -> Vec<u8> {
    keyset
        .iter()
        .map(|key| value_code_width(dict_by_key.get(key).map(|values| values.len()).unwrap_or(0)))
        .collect()
}

fn value_code_width(cardinality: usize) -> u8 {
    if cardinality <= 1 {
        0
    } else if cardinality <= 256 {
        1
    } else if cardinality <= 65_536 {
        2
    } else {
        4
    }
}

fn write_value_code(writer: &mut Vec<u8>, code: u32, width: u8) -> io::Result<()> {
    match width {
        0 => Ok(()),
        1 => {
            let value = u8::try_from(code).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "value code exceeds u8")
            })?;
            writer.push(value);
            Ok(())
        }
        2 => {
            let value = u16::try_from(code).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "value code exceeds u16")
            })?;
            writer.extend_from_slice(&value.to_le_bytes());
            Ok(())
        }
        4 => {
            writer.extend_from_slice(&code.to_le_bytes());
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid value code width",
        )),
    }
}

fn read_value_code(row: &[u8], cursor: &mut usize, width: u8) -> io::Result<u32> {
    match width {
        0 => Ok(0),
        1 => Ok(u32::from(read_u8(row, cursor)?)),
        2 => Ok(u32::from(read_u16(row, cursor)?)),
        4 => read_u32(row, cursor),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid value code width",
        )),
    }
}

fn read_series_header(reader: &mut (impl Read + Seek)) -> io::Result<SeriesHeader> {
    reader.seek(SeekFrom::Start(0))?;
    let magic = read_exact_u32(reader)?;
    if magic != SERIES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series magic mismatch",
        ));
    }
    let version = read_exact_u16(reader)?;
    if version != SERIES_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported series version",
        ));
    }
    let _flags = read_exact_u16(reader)?;
    let header = SeriesHeader {
        num_series: read_exact_u32(reader)?,
        num_keysets: read_exact_u32(reader)?,
        num_value_dicts: read_exact_u32(reader)?,
        series_table_offset: {
            let _reserved0 = read_exact_u32(reader)?;
            read_exact_u64(reader)?
        },
        keysets_offset: read_exact_u64(reader)?,
        value_dicts_offset: read_exact_u64(reader)?,
        keyset_blocks_offset: read_exact_u64(reader)?,
        meta_offset: read_exact_u64(reader)?,
    };
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
    let series_id = read_exact_u64(reader)?;
    let kind_mask = read_exact_u8(reader)?;
    let _flags = read_exact_u8(reader)?;
    let _reserved0 = read_exact_u16(reader)?;
    let chunk_index_offset = read_exact_u64(reader)?;
    let chunk_index_len = read_exact_u32(reader)?;
    let keyset_id = read_exact_u32(reader)?;
    let row = read_exact_u32(reader)?;
    let meta_off = read_exact_u32(reader)?;
    let meta_len = read_exact_u32(reader)?;
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

fn read_keysets_section(
    reader: &mut (impl Read + Seek),
    header: &SeriesHeader,
) -> io::Result<Vec<Vec<u32>>> {
    let offsets = read_section_offsets(reader, header.keysets_offset, header.num_keysets as usize)?;
    let mut keysets = Vec::with_capacity(header.num_keysets as usize);
    for pair in offsets.windows(2) {
        let start = pair[0];
        let end = pair[1];
        reader.seek(SeekFrom::Start(start))?;
        let key_count = read_exact_u32(reader)? as usize;
        let _reserved0 = read_exact_u32(reader)?;
        let expected_end = start
            .checked_add(8 + checked_mul_u64(key_count, 4, "keyset length")?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "keyset too large"))?;
        if expected_end != end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "keyset entry length mismatch",
            ));
        }
        let mut keyset = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            keyset.push(read_exact_u32(reader)?);
        }
        keysets.push(keyset);
    }
    Ok(keysets)
}

fn read_value_dicts_metadata(
    reader: &mut (impl Read + Seek),
    header: &SeriesHeader,
) -> io::Result<BTreeMap<u32, ValueDictMeta>> {
    let offsets = read_section_offsets(
        reader,
        header.value_dicts_offset,
        header.num_value_dicts as usize,
    )?;
    let mut dicts = BTreeMap::new();
    for pair in offsets.windows(2) {
        let start = pair[0];
        let end = pair[1];
        reader.seek(SeekFrom::Start(start))?;
        let key = read_exact_u32(reader)?;
        let cardinality = read_exact_u32(reader)? as usize;
        let expected_end = start
            .checked_add(8 + checked_mul_u64(cardinality, 4, "value dictionary length")?)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "value dictionary too large")
            })?;
        if expected_end != end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "value dictionary entry length mismatch",
            ));
        }
        if dicts
            .insert(
                key,
                ValueDictMeta {
                    values_offset: start + 8,
                    cardinality: cardinality as u32,
                },
            )
            .is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate value dictionary",
            ));
        }
    }
    Ok(dicts)
}

fn read_keyset_blocks_metadata(
    reader: &mut (impl Read + Seek),
    header: &SeriesHeader,
) -> io::Result<Vec<KeySetBlockMeta>> {
    let offsets = read_section_offsets(
        reader,
        header.keyset_blocks_offset,
        header.num_keysets as usize,
    )?;
    let mut blocks = Vec::with_capacity(header.num_keysets as usize);
    for pair in offsets.windows(2) {
        let start = pair[0];
        let end = pair[1];
        reader.seek(SeekFrom::Start(start))?;
        let rows = read_exact_u32(reader)?;
        let key_count = read_exact_u32(reader)?;
        let row_len_bytes = read_exact_u32(reader)?;
        let data_len = read_exact_u32(reader)?;
        let mut widths = vec![0u8; key_count as usize];
        reader.read_exact(&mut widths)?;
        let data_offset = reader.seek(SeekFrom::Current(0))?;
        let expected_data_len = u64::from(rows)
            .checked_mul(u64::from(row_len_bytes))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "keyset block data too large")
            })?;
        if expected_data_len != u64::from(data_len)
            || data_offset
                .checked_add(u64::from(data_len))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "keyset block data too large")
                })?
                != end
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "keyset block entry length mismatch",
            ));
        }
        blocks.push(KeySetBlockMeta {
            rows,
            key_count,
            row_len_bytes,
            data_len,
            widths,
            data_offset,
        });
    }
    Ok(blocks)
}

fn read_section_offsets(
    reader: &mut (impl Read + Seek),
    section_offset: u64,
    entry_count: usize,
) -> io::Result<Vec<u64>> {
    reader.seek(SeekFrom::Start(section_offset))?;
    let mut offsets = Vec::with_capacity(entry_count + 1);
    for _ in 0..=entry_count {
        offsets.push(read_exact_u64(reader)?);
    }
    let expected_first = section_offset
        .checked_add(checked_section_offsets_len(entry_count)?)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "section offsets overflow"))?;
    if offsets.first().copied() != Some(expected_first) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series section offsets header invalid",
        ));
    }
    for pair in offsets.windows(2) {
        if pair[1] < pair[0] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series section offsets out of order",
            ));
        }
    }
    Ok(offsets)
}

fn checked_section_offsets_len(entry_count: usize) -> io::Result<u64> {
    checked_mul_u64(entry_count + 1, 8, "section offset count")
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

fn checked_u32(value: usize, what: &str) -> io::Result<u32> {
    u32::try_from(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("{what} exceeds u32")))
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

fn read_exact_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut bytes = [0u8; 1];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn read_exact_u16(reader: &mut impl Read) -> io::Result<u16> {
    let mut bytes = [0u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_exact_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_exact_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    struct CountingCursor {
        cursor: Cursor<Vec<u8>>,
        bytes_read: u64,
    }

    impl CountingCursor {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                cursor: Cursor::new(bytes),
                bytes_read: 0,
            }
        }

        fn bytes_read(&self) -> u64 {
            self.bytes_read
        }
    }

    impl Read for CountingCursor {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let read = self.cursor.read(buf)?;
            self.bytes_read = self.bytes_read.saturating_add(read as u64);
            Ok(read)
        }
    }

    impl Seek for CountingCursor {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            self.cursor.seek(pos)
        }
    }

    #[test]
    fn symbols_bin_v2_roundtrips_sorted_dictionary_and_lookup_ids() {
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
        assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 2);

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
}
