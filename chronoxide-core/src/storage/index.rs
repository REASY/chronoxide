use std::collections::{BTreeMap, HashMap};
use std::io::{self, Read, Seek, SeekFrom, Write};

use fst::{Set, SetBuilder, Streamer};

use crate::storage::series::{SegmentSymbols, SeriesEntry};

const EXACT_POSTINGS_MAGIC: u32 = u32::from_le_bytes(*b"PIDX");
const LABEL_VALUE_FST_MAGIC: u32 = u32::from_le_bytes(*b"LVIX");
const LABEL_VALUE_TIME_RANGE_MAGIC: u32 = u32::from_le_bytes(*b"LVTR");
const SEGMENT_INDEXES_MAGIC: u32 = u32::from_le_bytes(*b"SIDX");
const SEGMENT_INDEX_FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"SIDF");
const SEGMENT_INDEX_TRAILER_MAGIC: u32 = u32::from_le_bytes(*b"SIDT");
const SEGMENT_INDEX_VERSION: u16 = 3;
const SEGMENT_INDEX_HEADER_LEN: u64 = 8;
const SEGMENT_INDEX_TRAILER_LEN: u64 = 12;
const SEGMENT_INDEX_BLOB_EXACT_POSTINGS: u16 = 1;
const SEGMENT_INDEX_BLOB_LABEL_VALUE_FST: u16 = 2;
const SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES: u16 = 3;
const NO_LABEL_VALUE_SYM: u32 = u32::MAX;

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

    pub fn insert_monotonic(&mut self, label_name_sym: u32, label_value_sym: u32, series_ref: u32) {
        let refs = self
            .postings
            .entry((label_name_sym, label_value_sym))
            .or_default();
        match refs.last().copied() {
            Some(last) if last == series_ref => {}
            Some(last) if last < series_ref => refs.push(series_ref),
            _ => match refs.binary_search(&series_ref) {
                Ok(_) => {}
                Err(idx) => refs.insert(idx, series_ref),
            },
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
        let mut values: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for entry in series {
            for (name, value) in &entry.labels {
                values.entry(*name).or_default().push(*value);
            }
        }

        let mut fsts = BTreeMap::new();
        for (name, mut values) in values {
            values.sort_unstable();
            values.dedup();
            values.sort_by(|left, right| {
                let left = symbols.resolve(*left).unwrap_or("");
                let right = symbols.resolve(*right).unwrap_or("");
                left.cmp(right)
            });

            let mut builder = SetBuilder::memory();
            for value in values {
                let value = symbols.resolve(value).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "value symbol missing")
                })?;
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
        read_fst_values(bytes)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactPostingsMetadata {
    pub byte_len: u64,
    pub time_range: LabelValueTimeRange,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LabelValueTimeRangeIndex {
    ranges: HashMap<(u32, u32), LabelValueTimeRange>,
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

    pub fn insert_many(&mut self, labels: &[(u32, u32)], min_time_ms: u64, max_time_ms: u64) {
        self.ranges.reserve(labels.len());
        for (name, value) in labels {
            self.insert(*name, *value, min_time_ms, max_time_ms);
        }
    }

    pub fn get(&self, label_name_sym: u32, label_value_sym: u32) -> Option<LabelValueTimeRange> {
        self.ranges.get(&(label_name_sym, label_value_sym)).copied()
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    fn label_time_ranges(&self) -> BTreeMap<u32, LabelValueTimeRange> {
        let mut out = BTreeMap::new();
        for ((name, _value), range) in &self.ranges {
            out.entry(*name)
                .and_modify(|existing: &mut LabelValueTimeRange| {
                    existing.min_time_ms = existing.min_time_ms.min(range.min_time_ms);
                    existing.max_time_ms = existing.max_time_ms.max(range.max_time_ms);
                })
                .or_insert(*range);
        }
        out
    }

    fn ranges_by_label(&self) -> BTreeMap<u32, Vec<(u32, LabelValueTimeRange)>> {
        let mut out: BTreeMap<u32, Vec<(u32, LabelValueTimeRange)>> = BTreeMap::new();
        for ((name, value), range) in &self.ranges {
            out.entry(*name).or_default().push((*value, *range));
        }
        for ranges in out.values_mut() {
            ranges.sort_unstable_by_key(|(value, _range)| *value);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_value_time_range_index_bulk_insert_merges_ranges() {
        let mut index = LabelValueTimeRangeIndex::default();

        index.insert_many(&[(2, 20), (1, 10)], 1_000, 2_000);
        index.insert_many(&[(1, 10), (3, 30)], 500, 4_000);

        assert_eq!(index.len(), 3);
        assert_eq!(
            index.get(1, 10),
            Some(LabelValueTimeRange {
                min_time_ms: 500,
                max_time_ms: 4_000,
            })
        );
        assert_eq!(
            index.get(2, 20),
            Some(LabelValueTimeRange {
                min_time_ms: 1_000,
                max_time_ms: 2_000,
            })
        );
        assert_eq!(
            index.get(3, 30),
            Some(LabelValueTimeRange {
                min_time_ms: 500,
                max_time_ms: 4_000,
            })
        );
    }

    #[test]
    fn segment_index_serializes_label_value_time_ranges_deterministically() {
        let mut forward = LabelValueTimeRangeIndex::default();
        forward.insert_many(&[(1, 10), (1, 20), (2, 30)], 1_000, 2_000);

        let mut reverse = LabelValueTimeRangeIndex::default();
        reverse.insert_many(&[(2, 30), (1, 20), (1, 10)], 1_000, 2_000);

        let mut forward_bytes = Vec::new();
        write_segment_indexes(
            &mut forward_bytes,
            &SegmentIndexes {
                exact_postings: ExactPostingsIndex::default(),
                label_values: LabelValueFstIndex::default(),
                label_value_time_ranges: forward,
            },
        )
        .unwrap();

        let mut reverse_bytes = Vec::new();
        write_segment_indexes(
            &mut reverse_bytes,
            &SegmentIndexes {
                exact_postings: ExactPostingsIndex::default(),
                label_values: LabelValueFstIndex::default(),
                label_value_time_ranges: reverse,
            },
        )
        .unwrap();

        assert_eq!(forward_bytes, reverse_bytes);
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SegmentIndexes {
    pub exact_postings: ExactPostingsIndex,
    pub label_values: LabelValueFstIndex,
    pub label_value_time_ranges: LabelValueTimeRangeIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentIndexDirectoryEntry {
    kind: u16,
    label_name_sym: u32,
    label_value_sym: u32,
    offset: u64,
    len: u64,
    min_time_ms: u64,
    max_time_ms: u64,
}

pub struct SegmentIndexReader<R> {
    reader: R,
    exact_postings: BTreeMap<(u32, u32), SegmentIndexDirectoryEntry>,
    label_value_fsts: BTreeMap<u32, SegmentIndexDirectoryEntry>,
    label_value_time_ranges: BTreeMap<u32, SegmentIndexDirectoryEntry>,
}

impl<R> SegmentIndexReader<R>
where
    R: Read + Seek,
{
    pub fn open(mut reader: R) -> io::Result<Self> {
        let entries = read_segment_index_directory(&mut reader)?;
        let mut exact_postings = BTreeMap::new();
        let mut label_value_fsts = BTreeMap::new();
        let mut label_value_time_ranges = BTreeMap::new();

        for entry in entries {
            match entry.kind {
                SEGMENT_INDEX_BLOB_EXACT_POSTINGS => {
                    exact_postings.insert((entry.label_name_sym, entry.label_value_sym), entry);
                }
                SEGMENT_INDEX_BLOB_LABEL_VALUE_FST => {
                    label_value_fsts.insert(entry.label_name_sym, entry);
                }
                SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES => {
                    label_value_time_ranges.insert(entry.label_name_sym, entry);
                }
                _ => {}
            }
        }

        Ok(Self {
            reader,
            exact_postings,
            label_value_fsts,
            label_value_time_ranges,
        })
    }

    pub fn label_name_symbols(&self) -> Vec<u32> {
        self.label_value_fsts.keys().copied().collect()
    }

    pub fn has_label_values(&self) -> bool {
        !self.label_value_fsts.is_empty()
    }

    pub fn label_time_range(&self, label_name_sym: u32) -> Option<LabelValueTimeRange> {
        self.label_value_fsts
            .get(&label_name_sym)
            .map(|entry| LabelValueTimeRange {
                min_time_ms: entry.min_time_ms,
                max_time_ms: entry.max_time_ms,
            })
    }

    pub fn label_values(&mut self, label_name_sym: u32) -> io::Result<Vec<String>> {
        let Some(entry) = self.label_value_fsts.get(&label_name_sym).copied() else {
            return Ok(Vec::new());
        };
        let bytes = self.read_blob(entry)?;
        read_fst_values(&bytes)
    }

    pub fn exact_postings(
        &mut self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<Vec<u32>>> {
        let Some(entry) = self
            .exact_postings
            .get(&(label_name_sym, label_value_sym))
            .copied()
        else {
            return Ok(None);
        };
        let bytes = self.read_blob(entry)?;
        Ok(Some(read_exact_postings_blob(&bytes)?))
    }

    pub fn exact_postings_metadata(
        &self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> Option<ExactPostingsMetadata> {
        self.exact_postings
            .get(&(label_name_sym, label_value_sym))
            .map(|entry| ExactPostingsMetadata {
                byte_len: entry.len,
                time_range: LabelValueTimeRange {
                    min_time_ms: entry.min_time_ms,
                    max_time_ms: entry.max_time_ms,
                },
            })
    }

    pub fn label_value_time_range(
        &mut self,
        label_name_sym: u32,
        label_value_sym: u32,
    ) -> io::Result<Option<LabelValueTimeRange>> {
        let Some(ranges) = self.label_value_time_ranges(label_name_sym)? else {
            return Ok(None);
        };
        Ok(ranges
            .into_iter()
            .find_map(|(value_sym, range)| (value_sym == label_value_sym).then_some(range)))
    }

    pub fn label_value_time_ranges(
        &mut self,
        label_name_sym: u32,
    ) -> io::Result<Option<Vec<(u32, LabelValueTimeRange)>>> {
        let Some(entry) = self.label_value_time_ranges.get(&label_name_sym).copied() else {
            return Ok(None);
        };
        let bytes = self.read_blob(entry)?;
        Ok(Some(read_label_value_time_ranges_blob(&bytes)?))
    }

    fn read_blob(&mut self, entry: SegmentIndexDirectoryEntry) -> io::Result<Vec<u8>> {
        let len = usize::try_from(entry.len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment index blob length exceeds platform usize",
            )
        })?;
        let mut bytes = vec![0u8; len];
        self.reader.seek(SeekFrom::Start(entry.offset))?;
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes)
    }
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

    let mut ranges: Vec<_> = index.ranges.iter().collect();
    ranges.sort_unstable_by_key(|((name, value), _range)| (*name, *value));
    for ((name, value), range) in ranges {
        writer.write_all(&name.to_le_bytes())?;
        writer.write_all(&value.to_le_bytes())?;
        writer.write_all(&range.min_time_ms.to_le_bytes())?;
        writer.write_all(&range.max_time_ms.to_le_bytes())?;
    }

    Ok(())
}

pub fn write_segment_indexes(mut writer: impl Write, indexes: &SegmentIndexes) -> io::Result<()> {
    writer.write_all(&SEGMENT_INDEXES_MAGIC.to_le_bytes())?;
    writer.write_all(&SEGMENT_INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;

    let mut entries = Vec::new();
    let mut offset = SEGMENT_INDEX_HEADER_LEN;
    let label_time_ranges = indexes.label_value_time_ranges.label_time_ranges();

    for ((name, value), refs) in &indexes.exact_postings.postings {
        let payload = write_exact_postings_blob(refs)?;
        let range = indexes
            .label_value_time_ranges
            .get(*name, *value)
            .unwrap_or(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            });
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_EXACT_POSTINGS,
                label_name_sym: *name,
                label_value_sym: *value,
                offset: 0,
                len: 0,
                min_time_ms: range.min_time_ms,
                max_time_ms: range.max_time_ms,
            },
            &payload,
        )?;
    }

    for (name, fst_bytes) in &indexes.label_values.fsts {
        let range = label_time_ranges
            .get(name)
            .copied()
            .unwrap_or(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            });
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
                label_name_sym: *name,
                label_value_sym: NO_LABEL_VALUE_SYM,
                offset: 0,
                len: 0,
                min_time_ms: range.min_time_ms,
                max_time_ms: range.max_time_ms,
            },
            fst_bytes,
        )?;
    }

    for (name, ranges) in indexes.label_value_time_ranges.ranges_by_label() {
        let payload = write_label_value_time_ranges_blob(&ranges)?;
        let range = label_time_ranges
            .get(&name)
            .copied()
            .unwrap_or(LabelValueTimeRange {
                min_time_ms: 0,
                max_time_ms: u64::MAX,
            });
        write_segment_index_blob(
            &mut writer,
            &mut entries,
            &mut offset,
            SegmentIndexDirectoryEntry {
                kind: SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
                label_name_sym: name,
                label_value_sym: NO_LABEL_VALUE_SYM,
                offset: 0,
                len: 0,
                min_time_ms: range.min_time_ms,
                max_time_ms: range.max_time_ms,
            },
            &payload,
        )?;
    }

    let footer = encode_segment_index_footer(&entries)?;
    writer.write_all(&footer)?;
    writer.write_all(&(footer.len() as u64).to_le_bytes())?;
    writer.write_all(&SEGMENT_INDEX_TRAILER_MAGIC.to_le_bytes())?;

    Ok(())
}

pub fn read_segment_indexes(mut reader: impl Read) -> io::Result<SegmentIndexes> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    read_segment_indexes_v3_bytes(&bytes)
}

fn write_segment_index_blob(
    writer: &mut impl Write,
    entries: &mut Vec<SegmentIndexDirectoryEntry>,
    offset: &mut u64,
    mut entry: SegmentIndexDirectoryEntry,
    payload: &[u8],
) -> io::Result<()> {
    entry.offset = *offset;
    entry.len = u64::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment index blob length exceeds u64",
        )
    })?;
    writer.write_all(payload)?;
    *offset = offset
        .checked_add(entry.len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "segment index too large"))?;
    entries.push(entry);
    Ok(())
}

fn read_segment_indexes_v3_bytes(bytes: &[u8]) -> io::Result<SegmentIndexes> {
    let entries = parse_segment_index_directory(bytes)?;
    let mut exact_postings = ExactPostingsIndex::default();
    let mut label_values = LabelValueFstIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();

    for entry in entries {
        let payload = segment_index_blob_bytes(bytes, entry)?;
        match entry.kind {
            SEGMENT_INDEX_BLOB_EXACT_POSTINGS => {
                for series_ref in read_exact_postings_blob(payload)? {
                    exact_postings.insert(entry.label_name_sym, entry.label_value_sym, series_ref);
                }
            }
            SEGMENT_INDEX_BLOB_LABEL_VALUE_FST => {
                label_values.insert_fst(entry.label_name_sym, payload.to_vec());
            }
            SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES => {
                for (value_sym, range) in read_label_value_time_ranges_blob(payload)? {
                    label_value_time_ranges.insert(
                        entry.label_name_sym,
                        value_sym,
                        range.min_time_ms,
                        range.max_time_ms,
                    );
                }
            }
            _ => {}
        }
    }

    Ok(SegmentIndexes {
        exact_postings,
        label_values,
        label_value_time_ranges,
    })
}

fn read_segment_index_directory(
    reader: &mut (impl Read + Seek),
) -> io::Result<Vec<SegmentIndexDirectoryEntry>> {
    let len = reader.seek(SeekFrom::End(0))?;
    if len < SEGMENT_INDEX_HEADER_LEN + SEGMENT_INDEX_TRAILER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment index truncated",
        ));
    }

    reader.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; SEGMENT_INDEX_HEADER_LEN as usize];
    reader.read_exact(&mut header)?;
    validate_segment_index_header(&header)?;

    reader.seek(SeekFrom::End(-(SEGMENT_INDEX_TRAILER_LEN as i64)))?;
    let mut trailer = [0u8; SEGMENT_INDEX_TRAILER_LEN as usize];
    reader.read_exact(&mut trailer)?;
    let footer_len = u64::from_le_bytes(trailer[0..8].try_into().unwrap());
    let trailer_magic = u32::from_le_bytes(trailer[8..12].try_into().unwrap());
    if trailer_magic != SEGMENT_INDEX_TRAILER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index trailer magic mismatch",
        ));
    }
    if footer_len > len.saturating_sub(SEGMENT_INDEX_HEADER_LEN + SEGMENT_INDEX_TRAILER_LEN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length invalid",
        ));
    }

    let footer_start = len - SEGMENT_INDEX_TRAILER_LEN - footer_len;
    reader.seek(SeekFrom::Start(footer_start))?;
    let footer_len = usize::try_from(footer_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length exceeds platform usize",
        )
    })?;
    let mut footer = vec![0u8; footer_len];
    reader.read_exact(&mut footer)?;
    decode_segment_index_footer(&footer)
}

fn parse_segment_index_directory(bytes: &[u8]) -> io::Result<Vec<SegmentIndexDirectoryEntry>> {
    if bytes.len() < (SEGMENT_INDEX_HEADER_LEN + SEGMENT_INDEX_TRAILER_LEN) as usize {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment index truncated",
        ));
    }
    validate_segment_index_header(&bytes[..SEGMENT_INDEX_HEADER_LEN as usize])?;

    let trailer_start = bytes.len() - SEGMENT_INDEX_TRAILER_LEN as usize;
    let footer_len =
        u64::from_le_bytes(bytes[trailer_start..trailer_start + 8].try_into().unwrap());
    let trailer_magic = u32::from_le_bytes(
        bytes[trailer_start + 8..trailer_start + 12]
            .try_into()
            .unwrap(),
    );
    if trailer_magic != SEGMENT_INDEX_TRAILER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index trailer magic mismatch",
        ));
    }
    let footer_len = usize::try_from(footer_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length exceeds platform usize",
        )
    })?;
    if footer_len > trailer_start.saturating_sub(SEGMENT_INDEX_HEADER_LEN as usize) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer length invalid",
        ));
    }
    let footer_start = trailer_start - footer_len;
    decode_segment_index_footer(&bytes[footer_start..trailer_start])
}

fn validate_segment_index_header(header: &[u8]) -> io::Result<()> {
    let mut cursor = 0usize;
    let magic = read_u32(header, &mut cursor)?;
    if magic != SEGMENT_INDEXES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment indexes magic mismatch",
        ));
    }
    let version = read_u16(header, &mut cursor)?;
    if version != SEGMENT_INDEX_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported segment indexes version",
        ));
    }
    let _flags = read_u16(header, &mut cursor)?;
    Ok(())
}

fn encode_segment_index_footer(entries: &[SegmentIndexDirectoryEntry]) -> io::Result<Vec<u8>> {
    let mut footer = Vec::new();
    footer.extend_from_slice(&SEGMENT_INDEX_FOOTER_MAGIC.to_le_bytes());
    footer.extend_from_slice(&SEGMENT_INDEX_VERSION.to_le_bytes());
    footer.extend_from_slice(&0u16.to_le_bytes());
    footer.extend_from_slice(
        &(u32::try_from(entries.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment index directory entry count exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    footer.extend_from_slice(&0u32.to_le_bytes());

    for entry in entries {
        footer.extend_from_slice(&entry.kind.to_le_bytes());
        footer.extend_from_slice(&0u16.to_le_bytes());
        footer.extend_from_slice(&entry.label_name_sym.to_le_bytes());
        footer.extend_from_slice(&entry.label_value_sym.to_le_bytes());
        footer.extend_from_slice(&entry.offset.to_le_bytes());
        footer.extend_from_slice(&entry.len.to_le_bytes());
        footer.extend_from_slice(&entry.min_time_ms.to_le_bytes());
        footer.extend_from_slice(&entry.max_time_ms.to_le_bytes());
    }

    Ok(footer)
}

fn decode_segment_index_footer(bytes: &[u8]) -> io::Result<Vec<SegmentIndexDirectoryEntry>> {
    let mut cursor = 0usize;
    let magic = read_u32(bytes, &mut cursor)?;
    if magic != SEGMENT_INDEX_FOOTER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer magic mismatch",
        ));
    }
    let version = read_u16(bytes, &mut cursor)?;
    if version != SEGMENT_INDEX_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported segment index footer version",
        ));
    }
    let _flags = read_u16(bytes, &mut cursor)?;
    let entry_count = read_u32(bytes, &mut cursor)? as usize;
    let _reserved = read_u32(bytes, &mut cursor)?;

    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let kind = read_u16(bytes, &mut cursor)?;
        let _flags = read_u16(bytes, &mut cursor)?;
        let label_name_sym = read_u32(bytes, &mut cursor)?;
        let label_value_sym = read_u32(bytes, &mut cursor)?;
        let offset = read_u64(bytes, &mut cursor)?;
        let len = read_u64(bytes, &mut cursor)?;
        let min_time_ms = read_u64(bytes, &mut cursor)?;
        let max_time_ms = read_u64(bytes, &mut cursor)?;
        entries.push(SegmentIndexDirectoryEntry {
            kind,
            label_name_sym,
            label_value_sym,
            offset,
            len,
            min_time_ms,
            max_time_ms,
        });
    }

    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index footer has trailing bytes",
        ));
    }
    Ok(entries)
}

fn segment_index_blob_bytes(bytes: &[u8], entry: SegmentIndexDirectoryEntry) -> io::Result<&[u8]> {
    let mut cursor = usize::try_from(entry.offset).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index blob offset exceeds platform usize",
        )
    })?;
    let len = usize::try_from(entry.len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment index blob length exceeds platform usize",
        )
    })?;
    read_bytes(bytes, &mut cursor, len)
}

fn write_exact_postings_blob(refs: &[u32]) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &(u32::try_from(refs.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "postings list length exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    for series_ref in refs {
        bytes.extend_from_slice(&series_ref.to_le_bytes());
    }
    Ok(bytes)
}

fn read_exact_postings_blob(bytes: &[u8]) -> io::Result<Vec<u32>> {
    let mut cursor = 0usize;
    let count = read_u32(bytes, &mut cursor)? as usize;
    let mut refs = Vec::with_capacity(count);
    for _ in 0..count {
        refs.push(read_u32(bytes, &mut cursor)?);
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "exact postings blob has trailing bytes",
        ));
    }
    Ok(refs)
}

fn write_label_value_time_ranges_blob(
    ranges: &[(u32, LabelValueTimeRange)],
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &(u32::try_from(ranges.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "label value time range count exceeds u32",
            )
        })?)
        .to_le_bytes(),
    );
    for (value_sym, range) in ranges {
        bytes.extend_from_slice(&value_sym.to_le_bytes());
        bytes.extend_from_slice(&range.min_time_ms.to_le_bytes());
        bytes.extend_from_slice(&range.max_time_ms.to_le_bytes());
    }
    Ok(bytes)
}

fn read_label_value_time_ranges_blob(bytes: &[u8]) -> io::Result<Vec<(u32, LabelValueTimeRange)>> {
    let mut cursor = 0usize;
    let count = read_u32(bytes, &mut cursor)? as usize;
    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        let value_sym = read_u32(bytes, &mut cursor)?;
        let min_time_ms = read_u64(bytes, &mut cursor)?;
        let max_time_ms = read_u64(bytes, &mut cursor)?;
        ranges.push((
            value_sym,
            LabelValueTimeRange {
                min_time_ms,
                max_time_ms,
            },
        ));
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "label value time ranges blob has trailing bytes",
        ));
    }
    Ok(ranges)
}

fn read_fst_values(bytes: &[u8]) -> io::Result<Vec<String>> {
    let set = Set::new(bytes).map_err(fst_io_error)?;
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
