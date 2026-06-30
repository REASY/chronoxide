use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};

use fst::{Set, SetBuilder, Streamer};

use crate::storage::series::{SegmentSymbols, SeriesEntry};

const EXACT_POSTINGS_MAGIC: u32 = u32::from_le_bytes(*b"PIDX");
const LABEL_VALUE_FST_MAGIC: u32 = u32::from_le_bytes(*b"LVIX");
const LABEL_VALUE_TIME_RANGE_MAGIC: u32 = u32::from_le_bytes(*b"LVTR");
const SEGMENT_INDEXES_MAGIC: u32 = u32::from_le_bytes(*b"SIDX");

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExactPostingsIndex {
    postings: BTreeMap<(u32, u32), Vec<u32>>,
}

impl ExactPostingsIndex {
    pub fn insert(&mut self, label_name_sym: u32, label_value_sym: u32, series_ref: u32) {
        let refs = self
            .postings
            .entry((label_name_sym, label_value_sym))
            .or_default();
        match refs.binary_search(&series_ref) {
            Ok(_) => {}
            Err(idx) => refs.insert(idx, series_ref),
        }
    }

    pub fn get(&self, label_name_sym: u32, label_value_sym: u32) -> Option<&[u32]> {
        self.postings
            .get(&(label_name_sym, label_value_sym))
            .map(Vec::as_slice)
    }

    pub fn len(&self) -> usize {
        self.postings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LabelValueIndex {
    values: BTreeMap<u32, Vec<u32>>,
}

impl LabelValueIndex {
    pub fn insert(&mut self, label_name_sym: u32, label_value_sym: u32) {
        let values = self.values.entry(label_name_sym).or_default();
        match values.binary_search(&label_value_sym) {
            Ok(_) => {}
            Err(idx) => values.insert(idx, label_value_sym),
        }
    }

    pub fn values(&self, label_name_sym: u32) -> &[u32] {
        self.values
            .get(&label_name_sym)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn from_series(series: &[SeriesEntry]) -> Self {
        let mut index = Self::default();
        for entry in series {
            for (name, value) in &entry.labels {
                index.insert(*name, *value);
            }
        }
        index
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LabelValueFstIndex {
    fsts: BTreeMap<u32, Vec<u8>>,
}

impl LabelValueFstIndex {
    pub fn from_series(series: &[SeriesEntry], symbols: &SegmentSymbols) -> io::Result<Self> {
        let mut values: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
        for entry in series {
            for (name, value) in &entry.labels {
                let value = symbols.resolve(*value).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "value symbol missing")
                })?;
                values.entry(*name).or_default().insert(value.to_string());
            }
        }

        let mut fsts = BTreeMap::new();
        for (name, values) in values {
            let mut builder = SetBuilder::memory();
            for value in values {
                builder.insert(value).map_err(fst_io_error)?;
            }
            fsts.insert(name, builder.into_inner().map_err(fst_io_error)?);
        }

        Ok(Self { fsts })
    }

    pub fn insert_fst(&mut self, label_name_sym: u32, fst_bytes: Vec<u8>) {
        self.fsts.insert(label_name_sym, fst_bytes);
    }

    pub fn values(&self, label_name_sym: u32) -> io::Result<Vec<String>> {
        let Some(bytes) = self.fsts.get(&label_name_sym) else {
            return Ok(Vec::new());
        };
        let set = Set::new(bytes.as_slice()).map_err(fst_io_error)?;
        let mut stream = set.stream();
        let mut values = Vec::new();
        while let Some(value) = stream.next() {
            let value = std::str::from_utf8(value).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid utf8 fst value: {err}"),
                )
            })?;
            values.push(value.to_string());
        }
        Ok(values)
    }

    pub fn label_name_symbols(&self) -> Vec<u32> {
        self.fsts.keys().copied().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.fsts.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LabelValueTimeRange {
    pub min_time_ms: u64,
    pub max_time_ms: u64,
}

impl LabelValueTimeRange {
    pub fn overlaps(self, start_ms: u64, end_ms: u64) -> bool {
        self.max_time_ms >= start_ms && self.min_time_ms <= end_ms
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LabelValueTimeRangeIndex {
    ranges: BTreeMap<(u32, u32), LabelValueTimeRange>,
}

impl LabelValueTimeRangeIndex {
    pub fn insert(
        &mut self,
        label_name_sym: u32,
        label_value_sym: u32,
        min_time_ms: u64,
        max_time_ms: u64,
    ) {
        let range = LabelValueTimeRange {
            min_time_ms,
            max_time_ms,
        };
        self.ranges
            .entry((label_name_sym, label_value_sym))
            .and_modify(|existing| {
                existing.min_time_ms = existing.min_time_ms.min(range.min_time_ms);
                existing.max_time_ms = existing.max_time_ms.max(range.max_time_ms);
            })
            .or_insert(range);
    }

    pub fn get(&self, label_name_sym: u32, label_value_sym: u32) -> Option<LabelValueTimeRange> {
        self.ranges.get(&(label_name_sym, label_value_sym)).copied()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SegmentIndexes {
    pub exact_postings: ExactPostingsIndex,
    pub label_values: LabelValueFstIndex,
    pub label_value_time_ranges: LabelValueTimeRangeIndex,
}

pub fn write_exact_postings_index(
    mut writer: impl Write,
    index: &ExactPostingsIndex,
) -> io::Result<()> {
    writer.write_all(&EXACT_POSTINGS_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(index.postings.len() as u32).to_le_bytes())?;

    for ((name, value), refs) in &index.postings {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&value.to_le_bytes())?;
        writer.write_all(&(refs.len() as u32).to_le_bytes())?;
        for series_ref in refs {
            writer.write_all(&series_ref.to_le_bytes())?;
        }
    }

    Ok(())
}

pub fn read_exact_postings_index(mut reader: impl Read) -> io::Result<ExactPostingsIndex> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    let mut cursor = 0usize;

    let magic = read_u32(&bytes, &mut cursor)?;
    if magic != EXACT_POSTINGS_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "postings magic mismatch",
        ));
    }
    let version = read_u16(&bytes, &mut cursor)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported postings version",
        ));
    }
    let _flags = read_u16(&bytes, &mut cursor)?;
    let term_count = read_u32(&bytes, &mut cursor)? as usize;

    let mut index = ExactPostingsIndex::default();
    for _ in 0..term_count {
        let name = read_u32(&bytes, &mut cursor)?;
        let value = read_u32(&bytes, &mut cursor)?;
        let count = read_u32(&bytes, &mut cursor)? as usize;
        for _ in 0..count {
            let series_ref = read_u32(&bytes, &mut cursor)?;
            index.insert(name, value, series_ref);
        }
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "postings index has trailing bytes",
        ));
    }

    Ok(index)
}

pub fn write_label_value_fst_index(
    mut writer: impl Write,
    index: &LabelValueFstIndex,
) -> io::Result<()> {
    writer.write_all(&LABEL_VALUE_FST_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(index.fsts.len() as u32).to_le_bytes())?;

    for (name, bytes) in &index.fsts {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
        writer.write_all(bytes)?;
    }

    Ok(())
}

pub fn read_label_value_fst_index(mut reader: impl Read) -> io::Result<LabelValueFstIndex> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    read_label_value_fst_index_bytes(&bytes)
}

pub fn write_label_value_time_range_index(
    mut writer: impl Write,
    index: &LabelValueTimeRangeIndex,
) -> io::Result<()> {
    writer.write_all(&LABEL_VALUE_TIME_RANGE_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(index.ranges.len() as u32).to_le_bytes())?;

    for ((name, value), range) in &index.ranges {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&value.to_le_bytes())?;
        writer.write_all(&range.min_time_ms.to_le_bytes())?;
        writer.write_all(&range.max_time_ms.to_le_bytes())?;
    }

    Ok(())
}

pub fn write_segment_indexes(mut writer: impl Write, indexes: &SegmentIndexes) -> io::Result<()> {
    let mut postings_bytes = Vec::new();
    write_exact_postings_index(&mut postings_bytes, &indexes.exact_postings)?;

    let mut label_value_bytes = Vec::new();
    write_label_value_fst_index(&mut label_value_bytes, &indexes.label_values)?;

    let mut label_value_time_range_bytes = Vec::new();
    write_label_value_time_range_index(
        &mut label_value_time_range_bytes,
        &indexes.label_value_time_ranges,
    )?;

    writer.write_all(&SEGMENT_INDEXES_MAGIC.to_le_bytes())?;
    writer.write_all(&2u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(postings_bytes.len() as u32).to_le_bytes())?;
    writer.write_all(&(label_value_bytes.len() as u32).to_le_bytes())?;
    writer.write_all(&(label_value_time_range_bytes.len() as u32).to_le_bytes())?;
    writer.write_all(&postings_bytes)?;
    writer.write_all(&label_value_bytes)?;
    writer.write_all(&label_value_time_range_bytes)?;

    Ok(())
}

pub fn read_segment_indexes(mut reader: impl Read) -> io::Result<SegmentIndexes> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    let mut cursor = 0usize;

    let magic = read_u32(&bytes, &mut cursor)?;
    if magic == EXACT_POSTINGS_MAGIC {
        return Ok(SegmentIndexes {
            exact_postings: read_exact_postings_index(&bytes[..])?,
            label_values: LabelValueFstIndex::default(),
            label_value_time_ranges: LabelValueTimeRangeIndex::default(),
        });
    }
    if magic != SEGMENT_INDEXES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment indexes magic mismatch",
        ));
    }

    let version = read_u16(&bytes, &mut cursor)?;
    if !matches!(version, 1 | 2) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported segment indexes version",
        ));
    }
    let _flags = read_u16(&bytes, &mut cursor)?;
    let postings_len = read_u32(&bytes, &mut cursor)? as usize;
    let label_values_len = read_u32(&bytes, &mut cursor)? as usize;
    let label_value_time_ranges_len = if version >= 2 {
        read_u32(&bytes, &mut cursor)? as usize
    } else {
        0
    };
    let postings_bytes = read_bytes(&bytes, &mut cursor, postings_len)?;
    let label_value_bytes = read_bytes(&bytes, &mut cursor, label_values_len)?;
    let label_value_time_range_bytes =
        read_bytes(&bytes, &mut cursor, label_value_time_ranges_len)?;
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment indexes have trailing bytes",
        ));
    }

    Ok(SegmentIndexes {
        exact_postings: read_exact_postings_index(postings_bytes)?,
        label_values: read_label_value_fst_index_bytes(label_value_bytes)?,
        label_value_time_ranges: read_label_value_time_range_index_bytes(
            label_value_time_range_bytes,
        )?,
    })
}

fn read_label_value_fst_index_bytes(bytes: &[u8]) -> io::Result<LabelValueFstIndex> {
    let mut cursor = 0usize;

    let magic = read_u32(bytes, &mut cursor)?;
    if magic != LABEL_VALUE_FST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value index magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported label value index version",
        ));
    }
    let _flags = read_u16(bytes, &mut cursor)?;
    let label_count = read_u32(bytes, &mut cursor)? as usize;

    let mut index = LabelValueFstIndex::default();
    for _ in 0..label_count {
        let name = read_u32(bytes, &mut cursor)?;
        let fst_len = read_u32(bytes, &mut cursor)? as usize;
        index.insert_fst(name, read_bytes(bytes, &mut cursor, fst_len)?.to_vec());
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value index has trailing bytes",
        ));
    }

    Ok(index)
}

fn read_label_value_time_range_index_bytes(bytes: &[u8]) -> io::Result<LabelValueTimeRangeIndex> {
    if bytes.is_empty() {
        return Ok(LabelValueTimeRangeIndex::default());
    }

    let mut cursor = 0usize;

    let magic = read_u32(bytes, &mut cursor)?;
    if magic != LABEL_VALUE_TIME_RANGE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time range index magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported label value time range index version",
        ));
    }
    let _flags = read_u16(bytes, &mut cursor)?;
    let range_count = read_u32(bytes, &mut cursor)? as usize;

    let mut index = LabelValueTimeRangeIndex::default();
    for _ in 0..range_count {
        let name = read_u32(bytes, &mut cursor)?;
        let value = read_u32(bytes, &mut cursor)?;
        let min_time_ms = read_u64(bytes, &mut cursor)?;
        let max_time_ms = read_u64(bytes, &mut cursor)?;
        index.insert(name, value, min_time_ms, max_time_ms);
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time range index has trailing bytes",
        ));
    }

    Ok(index)
}

fn fst_io_error(err: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
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
