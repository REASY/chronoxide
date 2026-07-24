//! Deterministic writer-side plan for the `series.bin` v2 cold label stream.
//!
//! Schema 7 retains these three byte encodings unchanged, but rebases their
//! absolute offset tables after its paged hot-series region. Keeping the plan
//! independent of either hot-record version makes that sharing explicit.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};

use super::{SeriesEntry, SeriesEntryStore, SeriesEntryView};

pub(crate) mod reader;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesColdV2SeriesRow {
    pub(crate) series_id: u64,
    pub(crate) kind_mask: u8,
    pub(crate) keyset_id: u32,
    pub(crate) row: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesColdV2Lengths {
    pub(crate) keysets: u64,
    pub(crate) value_dicts: u64,
    pub(crate) keyset_blocks: u64,
}

impl SeriesColdV2Lengths {
    pub(crate) fn total(self) -> io::Result<u64> {
        self.keysets
            .checked_add(self.value_dicts)
            .and_then(|len| len.checked_add(self.keyset_blocks))
            .ok_or_else(|| invalid_input("series cold sections are too large"))
    }
}

/// Absolute locations for the three contiguous v2 cold sections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesColdV2SectionOffsets {
    pub(crate) keysets: u64,
    pub(crate) value_dicts: u64,
    pub(crate) keyset_blocks: u64,
    pub(crate) end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedSeriesEntry {
    series_id: u64,
    kind_mask: u8,
    labels: Vec<(u32, u32)>,
}

type ColdKeysets = Vec<Vec<u32>>;
type ColdValueDicts = Vec<(u32, Vec<u32>)>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColdKeysetRows {
    row_count: u32,
    expected_rows: u32,
    row_len: u32,
    widths: Box<[u8]>,
    data: Vec<u8>,
}

impl ColdKeysetRows {
    fn with_exact_capacity(
        keyset: &[u32],
        dict_by_key: &BTreeMap<u32, &[u32]>,
        expected_rows: u32,
    ) -> io::Result<Self> {
        checked_u32(keyset.len(), "keyset row width")?;
        let widths = keyset
            .iter()
            .map(|key| value_code_width_for_key(*key, dict_by_key))
            .collect::<io::Result<Vec<_>>>()?
            .into_boxed_slice();
        let row_len = widths.iter().try_fold(0usize, |sum, width| {
            sum.checked_add(usize::from(*width))
                .ok_or_else(|| invalid_input("series keyset row is too large"))
        })?;
        let packed_data_len = checked_packed_data_len(row_len, expected_rows)?;
        let mut data = Vec::new();
        data.try_reserve_exact(packed_data_len)
            .map_err(|_| invalid_input("series keyset data allocation failed"))?;
        Ok(Self {
            row_count: 0,
            expected_rows,
            row_len: checked_u32(row_len, "keyset row length")?,
            widths,
            data,
        })
    }

    fn next_row(&self) -> u32 {
        self.row_count
    }

    fn append_row(
        &mut self,
        labels: &[(u32, u32)],
        value_codes: &BTreeMap<u32, BTreeMap<u32, u32>>,
    ) -> io::Result<()> {
        if self.row_count >= self.expected_rows {
            return Err(invalid_data("keyset row count exceeds expected rows"));
        }
        if labels.len() != self.widths.len() {
            return Err(invalid_data("keyset row width count mismatch"));
        }

        let next_row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| invalid_input("keyset block rows exceed u32"))?;
        let row_start = self.data.len();
        for ((key, value), width) in labels.iter().zip(self.widths.iter().copied()) {
            let code = value_codes
                .get(key)
                .and_then(|codes| codes.get(value))
                .copied()
                .ok_or_else(|| invalid_data("series value code missing"));
            let result = code.and_then(|code| write_value_code(&mut self.data, code, width));
            if let Err(error) = result {
                self.data.truncate(row_start);
                return Err(error);
            }
        }

        let appended = self
            .data
            .len()
            .checked_sub(row_start)
            .ok_or_else(|| invalid_data("keyset row data length mismatch"))
            .and_then(|appended| checked_u32(appended, "keyset row length"));
        let appended = match appended {
            Ok(appended) => appended,
            Err(error) => {
                self.data.truncate(row_start);
                return Err(error);
            }
        };
        if appended != self.row_len {
            self.data.truncate(row_start);
            return Err(invalid_data("keyset row data length mismatch"));
        }
        self.row_count = next_row_count;
        Ok(())
    }

    fn validate(&self) -> io::Result<()> {
        if self.row_count != self.expected_rows {
            return Err(invalid_data("keyset row count mismatch"));
        }
        let row_len = self.widths.iter().try_fold(0usize, |sum, width| {
            if !matches!(width, 0 | 1 | 2 | 4) {
                return Err(invalid_data("invalid value code width"));
            }
            sum.checked_add(usize::from(*width))
                .ok_or_else(|| invalid_input("series keyset row is too large"))
        })?;
        if checked_u32(row_len, "keyset row length")? != self.row_len {
            return Err(invalid_data("keyset row length mismatch"));
        }
        let expected_data_len = checked_packed_data_len(row_len, self.row_count)?;
        if self.data.len() != expected_data_len {
            return Err(invalid_data("keyset row data length mismatch"));
        }
        Ok(())
    }

    fn validate_against(
        &self,
        keyset: &[u32],
        dict_by_key: &BTreeMap<u32, &[u32]>,
    ) -> io::Result<()> {
        self.validate()?;
        if keyset.len() != self.widths.len() {
            return Err(invalid_data("keyset row width count mismatch"));
        }
        for (key, actual) in keyset.iter().zip(self.widths.iter().copied()) {
            if actual != value_code_width_for_key(*key, dict_by_key)? {
                return Err(invalid_data("keyset value code width mismatch"));
            }
        }
        Ok(())
    }
}

impl SeriesEntryView for NormalizedSeriesEntry {
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

/// Canonical label rows and exact section sizes shared by series v2 and v3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeriesColdV2Plan {
    series_rows: Vec<SeriesColdV2SeriesRow>,
    num_series: u32,
    num_keysets: u32,
    num_value_dicts: u32,
    keysets: Vec<Vec<u32>>,
    value_dicts: Vec<(u32, Vec<u32>)>,
    rows_by_keyset: Vec<ColdKeysetRows>,
    lengths: SeriesColdV2Lengths,
}

impl SeriesColdV2Plan {
    pub(crate) fn build(entries: &[SeriesEntry]) -> io::Result<Self> {
        checked_u32(entries.len(), "series count")?;
        let normalized = normalize_series_entries(entries);
        Self::build_from_entries(&normalized)
    }

    /// Builds directly from rows whose label key IDs are already strictly increasing.
    ///
    /// The order is checked before plan construction so callers cannot publish
    /// a cold stream that its reader would reject.
    #[cfg(test)]
    pub(crate) fn build_canonical(entries: &[SeriesEntry]) -> io::Result<Self> {
        Self::build_from_entries(entries)
    }

    pub(crate) fn build_canonical_rows<S: SeriesEntryStore + ?Sized>(
        entries: &S,
    ) -> io::Result<Self> {
        Self::build_from_entries(entries)
    }

    fn build_from_entries<S: SeriesEntryStore + ?Sized>(entries: &S) -> io::Result<Self> {
        let num_series = checked_u32(entries.len(), "series count")?;
        let (keysets, expected_rows_by_keyset, value_dicts) = collect_cold_shapes(entries)?;
        let num_keysets = checked_u32(keysets.len(), "keyset count")?;
        let keyset_ids = keyset_id_map(&keysets)?;
        let num_value_dicts = checked_u32(value_dicts.len(), "value dictionary count")?;
        validate_cold_shapes(&keysets, &value_dicts)?;
        let mut rows_by_keyset = {
            let dict_by_key = dictionary_slices(&value_dicts);
            keysets
                .iter()
                .zip(&expected_rows_by_keyset)
                .map(|(keyset, expected_rows)| {
                    ColdKeysetRows::with_exact_capacity(keyset, &dict_by_key, *expected_rows)
                })
                .collect::<io::Result<Vec<_>>>()?
        };
        let value_codes = value_code_maps(&value_dicts)?;
        let mut series_rows = Vec::with_capacity(entries.len());
        let mut keyset_scratch = Vec::new();
        for entry in entries.entries() {
            let entry = entry?;
            let labels = entry.labels();
            keyset_scratch.clear();
            keyset_scratch.extend(labels.iter().map(|(key, _)| *key));
            let keyset_id = *keyset_ids
                .get(keyset_scratch.as_slice())
                .ok_or_else(|| invalid_data("series keyset missing"))?;
            let keyset_idx =
                usize::try_from(keyset_id).map_err(|_| invalid_input("keyset id exceeds usize"))?;
            let rows = rows_by_keyset
                .get_mut(keyset_idx)
                .ok_or_else(|| invalid_data("series keyset id out of bounds"))?;
            let row = rows.next_row();
            rows.append_row(labels, &value_codes)?;
            series_rows.push(SeriesColdV2SeriesRow {
                series_id: entry.series_id(),
                kind_mask: entry.kind_mask(),
                keyset_id,
                row,
            });
        }
        for rows in &rows_by_keyset {
            rows.validate()?;
        }

        let lengths = SeriesColdV2Lengths {
            keysets: keysets_section_len(&keysets)?,
            value_dicts: value_dicts_section_len(&value_dicts)?,
            keyset_blocks: keyset_blocks_section_len(&keysets, &rows_by_keyset, &value_dicts)?,
        };
        lengths.total()?;

        Ok(Self {
            series_rows,
            num_series,
            num_keysets,
            num_value_dicts,
            keysets,
            value_dicts,
            rows_by_keyset,
            lengths,
        })
    }

    pub(crate) fn series_rows(&self) -> &[SeriesColdV2SeriesRow] {
        &self.series_rows
    }

    pub(crate) fn num_series(&self) -> u32 {
        self.num_series
    }

    pub(crate) fn num_keysets(&self) -> u32 {
        self.num_keysets
    }

    pub(crate) fn num_value_dicts(&self) -> u32 {
        self.num_value_dicts
    }

    pub(crate) fn lengths(&self) -> SeriesColdV2Lengths {
        self.lengths
    }

    pub(crate) fn section_offsets_at(
        &self,
        keysets_offset: u64,
    ) -> io::Result<SeriesColdV2SectionOffsets> {
        let value_dicts = keysets_offset
            .checked_add(self.lengths.keysets)
            .ok_or_else(|| invalid_input("series cold keysets offset overflow"))?;
        let keyset_blocks = value_dicts
            .checked_add(self.lengths.value_dicts)
            .ok_or_else(|| invalid_input("series cold value dictionaries offset overflow"))?;
        let end = keyset_blocks
            .checked_add(self.lengths.keyset_blocks)
            .ok_or_else(|| invalid_input("series cold keyset blocks offset overflow"))?;
        Ok(SeriesColdV2SectionOffsets {
            keysets: keysets_offset,
            value_dicts,
            keyset_blocks,
            end,
        })
    }

    /// Streams the three sections to a writer positioned at `offsets.keysets`.
    ///
    /// The encoded offset tables use the supplied absolute locations. The
    /// caller owns positioning because a sequential `Write` need not implement
    /// `Seek`; the returned count is the exact number of emitted bytes.
    pub(crate) fn write_sections_at(
        &self,
        writer: &mut impl Write,
        offsets: SeriesColdV2SectionOffsets,
    ) -> io::Result<u64> {
        self.validate_section_offsets(offsets)?;
        write_keysets_section(writer, offsets.keysets, &self.keysets)?;
        write_value_dicts_section(writer, offsets.value_dicts, &self.value_dicts)?;
        write_keyset_blocks_section(
            writer,
            offsets.keyset_blocks,
            &self.keysets,
            &self.rows_by_keyset,
            &self.value_dicts,
        )?;
        self.lengths.total()
    }

    /// Appends the three sections to a complete file buffer at absolute offsets.
    ///
    /// Offsets must describe the canonical contiguous layout derived from this
    /// plan, and the existing file buffer must end exactly at `offsets.keysets`.
    pub(crate) fn append_sections_at(
        &self,
        writer: &mut Vec<u8>,
        offsets: SeriesColdV2SectionOffsets,
    ) -> io::Result<()> {
        self.validate_section_offsets(offsets)?;
        if checked_u64(writer.len(), "series cold output offset")? != offsets.keysets {
            return Err(invalid_input(
                "series cold output does not start at the keysets offset",
            ));
        }

        let additional = checked_usize(self.lengths.total()?, "series cold section bytes")?;
        writer
            .try_reserve_exact(additional)
            .map_err(|_| invalid_input("series cold output allocation failed"))?;
        let written = self.write_sections_at(writer, offsets)?;
        if written != self.lengths.total()? {
            return Err(invalid_data("series cold encoded length mismatch"));
        }
        require_writer_offset(writer, offsets.end, "cold section end")
    }

    fn validate_section_offsets(&self, offsets: SeriesColdV2SectionOffsets) -> io::Result<()> {
        let canonical = self.section_offsets_at(offsets.keysets)?;
        if offsets != canonical {
            return Err(invalid_input(
                "series cold section offsets are not canonical and contiguous",
            ));
        }
        Ok(())
    }
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
                labels,
            }
        })
        .collect()
}

fn collect_cold_shapes<S: SeriesEntryStore + ?Sized>(
    entries: &S,
) -> io::Result<(ColdKeysets, Vec<u32>, ColdValueDicts)> {
    let mut rows_by_keyset: BTreeMap<Vec<u32>, u32> = BTreeMap::new();
    let mut values_by_key: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
    let mut keyset_scratch = Vec::new();

    for (row, entry) in entries.entries().enumerate() {
        let entry = entry?;
        let labels = entry.labels();
        keyset_scratch.clear();
        keyset_scratch.reserve(labels.len());
        let mut previous_key = None;
        for &(key, value) in labels {
            if previous_key.is_some_and(|previous_key| previous_key >= key) {
                return Err(invalid_data(&format!(
                    "series label keys are not strictly increasing at row {row}"
                )));
            }
            previous_key = Some(key);
            keyset_scratch.push(key);
            values_by_key.entry(key).or_default().insert(value);
        }
        if let Some(rows) = rows_by_keyset.get_mut(keyset_scratch.as_slice()) {
            *rows = rows
                .checked_add(1)
                .ok_or_else(|| invalid_input("keyset block rows exceed u32"))?;
        } else {
            rows_by_keyset.insert(keyset_scratch.clone(), 1);
        }
    }

    let (keysets, expected_rows_by_keyset) = rows_by_keyset.into_iter().unzip();
    let value_dicts = values_by_key
        .into_iter()
        .map(|(key, values)| (key, values.into_iter().collect()))
        .collect();
    Ok((keysets, expected_rows_by_keyset, value_dicts))
}

fn keyset_id_map(keysets: &[Vec<u32>]) -> io::Result<BTreeMap<Vec<u32>, u32>> {
    let mut keyset_ids = BTreeMap::new();
    for (idx, keyset) in keysets.iter().enumerate() {
        let keyset_id = checked_u32(idx, "keyset id")?;
        if keyset_ids.insert(keyset.clone(), keyset_id).is_some() {
            return Err(invalid_data("duplicate canonical keyset"));
        }
    }
    Ok(keyset_ids)
}

fn validate_cold_shapes(keysets: &[Vec<u32>], value_dicts: &[(u32, Vec<u32>)]) -> io::Result<()> {
    for keyset in keysets {
        checked_u32(keyset.len(), "keyset length")?;
    }
    for (_, values) in value_dicts {
        checked_u32(values.len(), "value dictionary length")?;
    }
    Ok(())
}

fn value_code_maps(
    value_dicts: &[(u32, Vec<u32>)],
) -> io::Result<BTreeMap<u32, BTreeMap<u32, u32>>> {
    let mut by_key = BTreeMap::new();
    for (key, values) in value_dicts {
        let mut codes = BTreeMap::new();
        for (idx, value) in values.iter().copied().enumerate() {
            let code = checked_u32(idx, "value code")?;
            if codes.insert(value, code).is_some() {
                return Err(invalid_data("duplicate value dictionary symbol"));
            }
        }
        if by_key.insert(*key, codes).is_some() {
            return Err(invalid_data("duplicate value dictionary key"));
        }
    }
    Ok(by_key)
}

fn keysets_section_len(keysets: &[Vec<u32>]) -> io::Result<u64> {
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    keysets.iter().try_fold(offsets_len, |len, keyset| {
        len.checked_add(
            8u64.checked_add(checked_mul_u64(keyset.len(), 4, "keyset length")?)
                .ok_or_else(|| invalid_input("series keyset entry is too large"))?,
        )
        .ok_or_else(|| invalid_input("series keysets section is too large"))
    })
}

fn value_dicts_section_len(value_dicts: &[(u32, Vec<u32>)]) -> io::Result<u64> {
    let offsets_len = checked_section_offsets_len(value_dicts.len())?;
    value_dicts
        .iter()
        .try_fold(offsets_len, |len, (_, values)| {
            len.checked_add(
                8u64.checked_add(checked_mul_u64(values.len(), 4, "value dictionary length")?)
                    .ok_or_else(|| invalid_input("series value dictionary entry is too large"))?,
            )
            .ok_or_else(|| invalid_input("series value dictionaries section is too large"))
        })
}

fn keyset_blocks_section_len(
    keysets: &[Vec<u32>],
    rows_by_keyset: &[ColdKeysetRows],
    value_dicts: &[(u32, Vec<u32>)],
) -> io::Result<u64> {
    if rows_by_keyset.len() != keysets.len() {
        return Err(invalid_data("keyset row block count mismatch"));
    }
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    let dict_by_key = dictionary_slices(value_dicts);
    keysets
        .iter()
        .enumerate()
        .try_fold(offsets_len, |len, (idx, keyset)| {
            let rows = rows_by_keyset
                .get(idx)
                .ok_or_else(|| invalid_data("keyset rows missing"))?;
            rows.validate_against(keyset, &dict_by_key)?;
            let data_len = checked_u64(rows.data.len(), "keyset data length")?;
            let block_len = 16u64
                .checked_add(checked_u64(rows.widths.len(), "width count")?)
                .and_then(|value| value.checked_add(data_len))
                .ok_or_else(|| invalid_input("series keyset block is too large"))?;
            len.checked_add(block_len)
                .ok_or_else(|| invalid_input("series keyset blocks section is too large"))
        })
}

fn write_keysets_section(
    writer: &mut impl Write,
    section_offset: u64,
    keysets: &[Vec<u32>],
) -> io::Result<()> {
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    let mut cursor = section_offset
        .checked_add(offsets_len)
        .ok_or_else(|| invalid_input("series keysets offset overflow"))?;
    for keyset in keysets {
        writer.write_all(&cursor.to_le_bytes())?;
        cursor = cursor
            .checked_add(
                8u64.checked_add(checked_mul_u64(keyset.len(), 4, "keyset length")?)
                    .ok_or_else(|| invalid_input("series keyset entry is too large"))?,
            )
            .ok_or_else(|| invalid_input("series keysets section is too large"))?;
    }
    writer.write_all(&cursor.to_le_bytes())?;

    for keyset in keysets {
        writer.write_all(&checked_u32(keyset.len(), "keyset length")?.to_le_bytes())?;
        writer.write_all(&0u32.to_le_bytes())?;
        for key in keyset {
            writer.write_all(&key.to_le_bytes())?;
        }
    }
    Ok(())
}

fn write_value_dicts_section(
    writer: &mut impl Write,
    section_offset: u64,
    value_dicts: &[(u32, Vec<u32>)],
) -> io::Result<()> {
    let offsets_len = checked_section_offsets_len(value_dicts.len())?;
    let mut cursor = section_offset
        .checked_add(offsets_len)
        .ok_or_else(|| invalid_input("series value dictionaries offset overflow"))?;
    for (_, values) in value_dicts {
        writer.write_all(&cursor.to_le_bytes())?;
        cursor = cursor
            .checked_add(
                8u64.checked_add(checked_mul_u64(values.len(), 4, "value dictionary length")?)
                    .ok_or_else(|| invalid_input("series value dictionary entry is too large"))?,
            )
            .ok_or_else(|| invalid_input("series value dictionaries section is too large"))?;
    }
    writer.write_all(&cursor.to_le_bytes())?;

    for (key, values) in value_dicts {
        writer.write_all(&key.to_le_bytes())?;
        writer.write_all(&checked_u32(values.len(), "value dictionary length")?.to_le_bytes())?;
        for value in values {
            writer.write_all(&value.to_le_bytes())?;
        }
    }
    Ok(())
}

fn write_keyset_blocks_section(
    writer: &mut impl Write,
    section_offset: u64,
    keysets: &[Vec<u32>],
    rows_by_keyset: &[ColdKeysetRows],
    value_dicts: &[(u32, Vec<u32>)],
) -> io::Result<()> {
    if rows_by_keyset.len() != keysets.len() {
        return Err(invalid_data("keyset row block count mismatch"));
    }
    let dict_by_key = dictionary_slices(value_dicts);
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    let mut cursor = section_offset
        .checked_add(offsets_len)
        .ok_or_else(|| invalid_input("series keyset blocks offset overflow"))?;

    for (idx, keyset) in keysets.iter().enumerate() {
        let rows = rows_by_keyset
            .get(idx)
            .ok_or_else(|| invalid_data("keyset rows missing"))?;
        rows.validate_against(keyset, &dict_by_key)?;

        writer.write_all(&cursor.to_le_bytes())?;
        let data_len = checked_u64(rows.data.len(), "keyset data length")?;
        let block_len = 16u64
            .checked_add(checked_u64(rows.widths.len(), "width count")?)
            .and_then(|value| value.checked_add(data_len))
            .ok_or_else(|| invalid_input("series keyset block is too large"))?;
        cursor = cursor
            .checked_add(block_len)
            .ok_or_else(|| invalid_input("series keyset blocks section is too large"))?;
    }
    writer.write_all(&cursor.to_le_bytes())?;

    for (idx, keyset) in keysets.iter().enumerate() {
        let rows = rows_by_keyset
            .get(idx)
            .ok_or_else(|| invalid_data("keyset rows missing"))?;
        rows.validate_against(keyset, &dict_by_key)?;
        writer.write_all(&rows.row_count.to_le_bytes())?;
        writer.write_all(&checked_u32(keyset.len(), "keyset length")?.to_le_bytes())?;
        writer.write_all(&rows.row_len.to_le_bytes())?;
        writer.write_all(&checked_u32(rows.data.len(), "keyset data length")?.to_le_bytes())?;
        writer.write_all(rows.widths.as_ref())?;
        writer.write_all(&rows.data)?;
    }
    Ok(())
}

fn dictionary_slices(value_dicts: &[(u32, Vec<u32>)]) -> BTreeMap<u32, &[u32]> {
    value_dicts
        .iter()
        .map(|(key, values)| (*key, values.as_slice()))
        .collect()
}

fn value_code_width_for_key(key: u32, dict_by_key: &BTreeMap<u32, &[u32]>) -> io::Result<u8> {
    let values = dict_by_key
        .get(&key)
        .ok_or_else(|| invalid_data("keyset value dictionary missing"))?;
    if values.is_empty() {
        return Err(invalid_data("keyset value dictionary is empty"));
    }
    Ok(value_code_width(values.len()))
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

fn write_value_code(writer: &mut impl Write, code: u32, width: u8) -> io::Result<()> {
    match width {
        0 if code == 0 => Ok(()),
        0 => Err(invalid_data("implicit value code is not zero")),
        1 => {
            let value = u8::try_from(code).map_err(|_| invalid_input("value code exceeds u8"))?;
            writer.write_all(&[value])
        }
        2 => {
            let value = u16::try_from(code).map_err(|_| invalid_input("value code exceeds u16"))?;
            writer.write_all(&value.to_le_bytes())
        }
        4 => writer.write_all(&code.to_le_bytes()),
        _ => Err(invalid_data("invalid value code width")),
    }
}

fn checked_packed_data_len(row_len: usize, row_count: u32) -> io::Result<usize> {
    checked_u32(row_len, "keyset row length")?;
    let row_count =
        usize::try_from(row_count).map_err(|_| invalid_input("keyset block rows exceed usize"))?;
    let data_len = row_len
        .checked_mul(row_count)
        .ok_or_else(|| invalid_input("series keyset block data is too large"))?;
    checked_u32(data_len, "keyset data length")?;
    Ok(data_len)
}

fn checked_section_offsets_len(entry_count: usize) -> io::Result<u64> {
    let offset_count = entry_count
        .checked_add(1)
        .ok_or_else(|| invalid_input("section offset count is too large"))?;
    checked_mul_u64(offset_count, 8, "section offset count")
}

fn checked_mul_u64(value: usize, multiplier: usize, what: &str) -> io::Result<u64> {
    checked_u64(value, what)?
        .checked_mul(u64::try_from(multiplier).map_err(|_| invalid_input(what))?)
        .ok_or_else(|| invalid_input(&format!("{what} is too large")))
}

fn checked_u64(value: usize, what: &str) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| invalid_input(&format!("{what} exceeds u64")))
}

fn checked_u32(value: usize, what: &str) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| invalid_input(&format!("{what} exceeds u32")))
}

fn checked_usize(value: u64, what: &str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid_input(&format!("{what} exceeds usize")))
}

fn require_writer_offset(writer: &[u8], expected: u64, what: &str) -> io::Result<()> {
    if checked_u64(writer.len(), "series cold output offset")? != expected {
        return Err(invalid_data(&format!(
            "series cold {what} offset does not match encoded bytes"
        )));
    }
    Ok(())
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::chunk::ChunkIndexRange;

    #[test]
    fn empty_plan_encodes_schema7_canonical_rebased_sections() {
        let plan = SeriesColdV2Plan::build(&[]).unwrap();
        assert!(plan.series_rows().is_empty());
        assert_eq!(plan.num_series(), 0);
        assert_eq!(plan.num_keysets(), 0);
        assert_eq!(plan.num_value_dicts(), 0);
        assert_eq!(
            plan.lengths(),
            SeriesColdV2Lengths {
                keysets: 8,
                value_dicts: 8,
                keyset_blocks: 8,
            }
        );

        let offsets = plan.section_offsets_at(4_096).unwrap();
        assert_eq!(
            offsets,
            SeriesColdV2SectionOffsets {
                keysets: 4_096,
                value_dicts: 4_104,
                keyset_blocks: 4_112,
                end: 4_120,
            }
        );
        let mut bytes = vec![0; 4_096];
        plan.append_sections_at(&mut bytes, offsets).unwrap();
        assert_eq!(bytes.len(), 4_120);
        assert_eq!(
            u64::from_le_bytes(bytes[4_096..4_104].try_into().unwrap()),
            4_104
        );
        assert_eq!(
            u64::from_le_bytes(bytes[4_104..4_112].try_into().unwrap()),
            4_112
        );
        assert_eq!(
            u64::from_le_bytes(bytes[4_112..4_120].try_into().unwrap()),
            4_120
        );
    }

    #[test]
    fn nonempty_plan_rebases_every_section_terminal_offset() {
        let entries = sample_entries();
        let plan = SeriesColdV2Plan::build(&entries).unwrap();
        assert_eq!(
            plan.series_rows(),
            &[
                SeriesColdV2SeriesRow {
                    series_id: 1,
                    kind_mask: 1,
                    keyset_id: 0,
                    row: 0,
                },
                SeriesColdV2SeriesRow {
                    series_id: 2,
                    kind_mask: 4,
                    keyset_id: 0,
                    row: 1,
                },
            ]
        );
        assert_eq!(plan.num_series(), 2);
        assert_eq!(
            plan.lengths(),
            SeriesColdV2Lengths {
                keysets: 32,
                value_dicts: 52,
                keyset_blocks: 36,
            }
        );
        let offsets = plan.section_offsets_at(20_480).unwrap();
        assert_eq!(offsets.keysets, 20_480);
        assert_eq!(offsets.value_dicts, 20_512);
        assert_eq!(offsets.keyset_blocks, 20_564);
        assert_eq!(offsets.end, 20_600);
        let mut bytes = vec![0; 20_480];
        plan.append_sections_at(&mut bytes, offsets).unwrap();

        assert_eq!(bytes.len() as u64, offsets.end);
        assert_eq!(read_u64(&bytes, offsets.keysets + 8), offsets.value_dicts);
        assert_eq!(
            read_u64(&bytes, offsets.value_dicts + 16),
            offsets.keyset_blocks
        );
        assert_eq!(read_u64(&bytes, offsets.keyset_blocks + 8), offsets.end);
    }

    #[test]
    fn canonical_build_matches_generic_normalization_and_bytes_exactly() {
        let unsorted = sample_entries();
        let mut canonical = unsorted.clone();
        for entry in &mut canonical {
            entry.labels.sort_unstable_by_key(|(key, _)| *key);
        }

        let generic_plan = SeriesColdV2Plan::build(&unsorted).unwrap();
        let canonical_plan = SeriesColdV2Plan::build_canonical(&canonical).unwrap();
        assert_eq!(canonical_plan, generic_plan);

        let offsets = generic_plan.section_offsets_at(4_096).unwrap();
        let mut generic_bytes = vec![0; 4_096];
        generic_plan
            .append_sections_at(&mut generic_bytes, offsets)
            .unwrap();
        let mut canonical_bytes = vec![0; 4_096];
        canonical_plan
            .append_sections_at(&mut canonical_bytes, offsets)
            .unwrap();
        assert_eq!(canonical_bytes, generic_bytes);
    }

    #[test]
    fn canonical_build_rejects_descending_label_keys() {
        let error = SeriesColdV2Plan::build_canonical(&sample_entries()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "series label keys are not strictly increasing at row 0"
        );
    }

    #[test]
    fn duplicate_label_keys_are_rejected_by_both_build_paths() {
        let entries = vec![SeriesEntry {
            series_id: 1,
            kind_mask: 1,
            chunk_index: ChunkIndexRange { offset: 7, len: 8 },
            labels: vec![(2, 20), (1, 10), (1, 11)],
        }];

        let generic_error = SeriesColdV2Plan::build(&entries).unwrap_err();
        assert_eq!(generic_error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            generic_error.to_string(),
            "series label keys are not strictly increasing at row 0"
        );

        let mut canonical = entries;
        canonical[0].labels.sort_unstable_by_key(|(key, _)| *key);
        let canonical_error = SeriesColdV2Plan::build_canonical(&canonical).unwrap_err();
        assert_eq!(canonical_error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(canonical_error.to_string(), generic_error.to_string());
    }

    #[test]
    fn canonical_build_accepts_empty_corpus_and_empty_label_row() {
        let generic_empty = SeriesColdV2Plan::build(&[]).unwrap();
        let canonical_empty = SeriesColdV2Plan::build_canonical(&[]).unwrap();
        assert_eq!(canonical_empty, generic_empty);

        let entries = vec![SeriesEntry {
            series_id: 7,
            kind_mask: 2,
            chunk_index: ChunkIndexRange { offset: 9, len: 10 },
            labels: Vec::new(),
        }];
        let generic_row = SeriesColdV2Plan::build(&entries).unwrap();
        let canonical_row = SeriesColdV2Plan::build_canonical(&entries).unwrap();
        assert_eq!(canonical_row, generic_row);
        assert_eq!(
            canonical_row.series_rows(),
            &[SeriesColdV2SeriesRow {
                series_id: 7,
                kind_mask: 2,
                keyset_id: 0,
                row: 0,
            }]
        );
        assert_eq!(canonical_row.num_keysets(), 1);
        assert_eq!(canonical_row.num_value_dicts(), 0);
    }

    #[test]
    fn interleaved_keysets_keep_independent_rows_and_zero_width_bytes() {
        let entries = vec![
            series_entry(1, Vec::new()),
            series_entry(2, vec![(1, 10), (2, 20)]),
            series_entry(3, Vec::new()),
            series_entry(4, vec![(1, 11), (2, 20)]),
        ];
        let plan = SeriesColdV2Plan::build_canonical(&entries).unwrap();
        assert_eq!(
            plan.series_rows(),
            &[
                cold_row(1, 0, 0),
                cold_row(2, 1, 0),
                cold_row(3, 0, 1),
                cold_row(4, 1, 1),
            ]
        );

        let offsets = plan.section_offsets_at(0).unwrap();
        let mut bytes = Vec::new();
        plan.append_sections_at(&mut bytes, offsets).unwrap();
        let first_block = read_u64(&bytes, offsets.keyset_blocks);
        let second_block = read_u64(&bytes, offsets.keyset_blocks + 8);
        assert_eq!(read_u64(&bytes, offsets.keyset_blocks + 16), offsets.end);

        assert_eq!(read_u32(&bytes, first_block), 2);
        assert_eq!(read_u32(&bytes, first_block + 4), 0);
        assert_eq!(read_u32(&bytes, first_block + 8), 0);
        assert_eq!(read_u32(&bytes, first_block + 12), 0);
        assert_eq!(second_block, first_block + 16);

        assert_eq!(read_u32(&bytes, second_block), 2);
        assert_eq!(read_u32(&bytes, second_block + 4), 2);
        assert_eq!(read_u32(&bytes, second_block + 8), 1);
        assert_eq!(read_u32(&bytes, second_block + 12), 2);
        let widths = usize::try_from(second_block + 16).unwrap();
        assert_eq!(&bytes[widths..widths + 2], &[1, 0]);
        assert_eq!(&bytes[widths + 2..widths + 4], &[0, 1]);
    }

    #[test]
    fn multirow_singleton_keyset_retains_rows_without_encoded_data() {
        let entries = vec![
            series_entry(1, vec![(1, 10), (2, 20)]),
            series_entry(2, vec![(1, 10), (2, 20)]),
        ];
        let plan = SeriesColdV2Plan::build_canonical(&entries).unwrap();
        assert_eq!(plan.series_rows(), &[cold_row(1, 0, 0), cold_row(2, 0, 1)]);

        let offsets = plan.section_offsets_at(0).unwrap();
        let mut bytes = Vec::new();
        plan.append_sections_at(&mut bytes, offsets).unwrap();
        let block = read_u64(&bytes, offsets.keyset_blocks);
        assert_eq!(read_u32(&bytes, block), 2);
        assert_eq!(read_u32(&bytes, block + 4), 2);
        assert_eq!(read_u32(&bytes, block + 8), 0);
        assert_eq!(read_u32(&bytes, block + 12), 0);
        let widths = usize::try_from(block + 16).unwrap();
        assert_eq!(&bytes[widths..widths + 2], &[0, 0]);
        assert_eq!(offsets.end, block + 18);
    }

    #[test]
    fn packed_keyset_rows_reject_malformed_lengths_and_shapes() {
        let rows = ColdKeysetRows {
            row_count: 2,
            expected_rows: 2,
            row_len: 2,
            widths: vec![1, 1].into_boxed_slice(),
            data: vec![0, 1, 2],
        };
        let error = rows.validate().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "keyset row data length mismatch");

        let rows = ColdKeysetRows {
            row_count: 2,
            expected_rows: 2,
            row_len: 1,
            widths: vec![1].into_boxed_slice(),
            data: vec![0, 1, 2],
        };
        assert_eq!(
            rows.validate().unwrap_err().to_string(),
            "keyset row data length mismatch"
        );

        let rows = ColdKeysetRows {
            row_count: 1,
            expected_rows: 1,
            row_len: 0,
            widths: vec![0].into_boxed_slice(),
            data: vec![0],
        };
        assert_eq!(
            rows.validate().unwrap_err().to_string(),
            "keyset row data length mismatch"
        );

        let rows = ColdKeysetRows {
            row_count: 1,
            expected_rows: 1,
            row_len: 3,
            widths: vec![3].into_boxed_slice(),
            data: vec![0; 3],
        };
        assert_eq!(
            rows.validate().unwrap_err().to_string(),
            "invalid value code width"
        );

        let values = [10, 11];
        let dict_by_key = BTreeMap::from([(1, values.as_slice())]);
        let rows = packed_rows(1, &[0], &[]);
        assert_eq!(
            rows.validate_against(&[1], &dict_by_key)
                .unwrap_err()
                .to_string(),
            "keyset value code width mismatch"
        );
        assert_eq!(
            rows.validate_against(&[1, 2], &dict_by_key)
                .unwrap_err()
                .to_string(),
            "keyset row width count mismatch"
        );

        let extra = vec![packed_rows(1, &[1], &[0]), packed_rows(1, &[], &[])];
        assert_eq!(
            keyset_blocks_section_len(&[vec![1]], &extra, &[(1, vec![10, 11])])
                .unwrap_err()
                .to_string(),
            "keyset row block count mismatch"
        );
    }

    #[test]
    fn packed_keyset_rows_reject_overflow_and_incomplete_rows() {
        let row_overflow = checked_packed_data_len(usize::MAX, 2).unwrap_err();
        assert_eq!(row_overflow.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(row_overflow.to_string(), "keyset row length exceeds u32");
        let data_overflow = checked_packed_data_len(4, u32::MAX).unwrap_err();
        assert_eq!(data_overflow.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(data_overflow.to_string(), "keyset data length exceeds u32");

        let rows = ColdKeysetRows {
            row_count: 1,
            expected_rows: 2,
            row_len: 0,
            widths: Box::new([]),
            data: Vec::new(),
        };
        let mismatch = rows.validate().unwrap_err();
        assert_eq!(mismatch.kind(), io::ErrorKind::InvalidData);
        assert_eq!(mismatch.to_string(), "keyset row count mismatch");
    }

    #[test]
    fn packed_keyset_rows_roll_back_failures_and_reject_overfill() {
        let missing = ColdKeysetRows::with_exact_capacity(&[1], &BTreeMap::new(), 1).unwrap_err();
        assert_eq!(missing.to_string(), "keyset value dictionary missing");
        let empty_values = [];
        let empty_dict = BTreeMap::from([(1, empty_values.as_slice())]);
        let empty = ColdKeysetRows::with_exact_capacity(&[1], &empty_dict, 1).unwrap_err();
        assert_eq!(empty.to_string(), "keyset value dictionary is empty");

        let first_values = [10, 11];
        let second_values = [20, 21];
        let dict_by_key =
            BTreeMap::from([(1, first_values.as_slice()), (2, second_values.as_slice())]);
        let mut rows = ColdKeysetRows::with_exact_capacity(&[1, 2], &dict_by_key, 1).unwrap();
        let value_codes = BTreeMap::from([
            (1, BTreeMap::from([(10, 0), (11, 1)])),
            (2, BTreeMap::from([(20, 0), (21, 1)])),
        ]);

        let missing = rows
            .append_row(&[(1, 11), (2, 22)], &value_codes)
            .unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::InvalidData);
        assert_eq!(missing.to_string(), "series value code missing");
        assert_eq!(rows.row_count, 0);
        assert!(rows.data.is_empty());

        rows.append_row(&[(1, 11), (2, 21)], &value_codes).unwrap();
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.data, [1, 1]);
        let before = rows.clone();
        let overfill = rows
            .append_row(&[(1, 10), (2, 20)], &value_codes)
            .unwrap_err();
        assert_eq!(overfill.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            overfill.to_string(),
            "keyset row count exceeds expected rows"
        );
        assert_eq!(rows, before);
        rows.validate_against(&[1, 2], &dict_by_key).unwrap();
    }

    #[test]
    fn packed_keyset_blocks_match_frozen_mixed_width_golden_and_legacy_encoder() {
        let keysets = vec![vec![], vec![1, 2, 3, 4]];
        let value_dicts = vec![
            (1, vec![0]),
            (2, (0..256).collect()),
            (3, (0..257).collect()),
            (4, (0..65_537).collect()),
        ];
        let rows = vec![
            packed_rows(2, &[], &[]),
            packed_rows(
                2,
                &[0, 1, 2, 4],
                &[0, 0, 0, 0, 0, 0, 0, 255, 0, 1, 0, 0, 1, 0],
            ),
        ];
        let legacy_rows = vec![
            vec![vec![], vec![]],
            vec![vec![0, 0, 0, 0], vec![0, 255, 256, 65_536]],
        ];

        let section_offset = 4_096;
        let mut actual = Vec::new();
        write_keyset_blocks_section(&mut actual, section_offset, &keysets, &rows, &value_dicts)
            .unwrap();
        let mut legacy = Vec::new();
        write_legacy_keyset_blocks_section(
            &mut legacy,
            section_offset,
            &keysets,
            &legacy_rows,
            &value_dicts,
        )
        .unwrap();
        assert_eq!(actual, legacy);

        let mut expected = Vec::new();
        for offset in [4_120u64, 4_136, 4_170] {
            expected.extend_from_slice(&offset.to_le_bytes());
        }
        append_expected_block(&mut expected, 2, 0, 0, &[], &[]);
        append_expected_block(
            &mut expected,
            2,
            4,
            7,
            &[0, 1, 2, 4],
            &[0, 0, 0, 0, 0, 0, 0, 255, 0, 1, 0, 0, 1, 0],
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn packed_plan_build_matches_legacy_encoder_for_every_width() {
        let entries = (0..65_537u32)
            .map(|value| {
                series_entry(
                    u64::from(value),
                    vec![(1, 0), (2, value % 256), (3, value % 257), (4, value)],
                )
            })
            .collect::<Vec<_>>();
        let legacy_rows = vec![
            (0..65_537u32)
                .map(|value| vec![0, value % 256, value % 257, value])
                .collect::<Vec<_>>(),
        ];
        let plan = SeriesColdV2Plan::build_canonical(&entries).unwrap();
        assert_eq!(plan.keysets, [vec![1, 2, 3, 4]]);
        assert_eq!(plan.rows_by_keyset[0].widths.as_ref(), [0, 1, 2, 4]);
        assert_eq!(plan.rows_by_keyset[0].row_count, 65_537);
        assert_eq!(plan.series_rows()[0], cold_row(0, 0, 0));
        assert_eq!(plan.series_rows()[65_536], cold_row(65_536, 0, 65_536));

        for start in [0usize, 4_096] {
            let offsets = plan
                .section_offsets_at(u64::try_from(start).unwrap())
                .unwrap();
            let mut actual = vec![0; start];
            plan.append_sections_at(&mut actual, offsets).unwrap();

            let mut legacy = vec![0; start];
            write_keysets_section(&mut legacy, offsets.keysets, &plan.keysets).unwrap();
            write_value_dicts_section(&mut legacy, offsets.value_dicts, &plan.value_dicts).unwrap();
            write_legacy_keyset_blocks_section(
                &mut legacy,
                offsets.keyset_blocks,
                &plan.keysets,
                &legacy_rows,
                &plan.value_dicts,
            )
            .unwrap();
            assert_eq!(actual, legacy);
        }
    }

    #[test]
    fn value_code_width_and_encoding_boundaries_are_exact() {
        assert_eq!(value_code_width(0), 0);
        assert_eq!(value_code_width(1), 0);
        assert_eq!(value_code_width(2), 1);
        assert_eq!(value_code_width(256), 1);
        assert_eq!(value_code_width(257), 2);
        assert_eq!(value_code_width(65_536), 2);
        assert_eq!(value_code_width(65_537), 4);

        let mut bytes = Vec::new();
        write_value_code(&mut bytes, 0, 0).unwrap();
        write_value_code(&mut bytes, 255, 1).unwrap();
        write_value_code(&mut bytes, 65_535, 2).unwrap();
        write_value_code(&mut bytes, u32::MAX, 4).unwrap();
        assert_eq!(bytes, [255, 255, 255, 255, 255, 255, 255].as_slice());

        assert_eq!(
            write_value_code(&mut Vec::new(), 1, 0).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            write_value_code(&mut Vec::new(), 256, 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            write_value_code(&mut Vec::new(), 65_536, 2)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            write_value_code(&mut Vec::new(), 0, 3).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn plan_rejects_offset_overflow_and_noncanonical_layout() {
        let plan = SeriesColdV2Plan::build(&[]).unwrap();
        assert!(plan.section_offsets_at(u64::MAX - 7).is_err());

        let mut bytes = vec![0; 4_096];
        let mut offsets = plan.section_offsets_at(4_096).unwrap();
        offsets.value_dicts += 1;
        assert!(plan.append_sections_at(&mut bytes, offsets).is_err());
        assert_eq!(bytes.len(), 4_096);
    }

    #[test]
    fn streaming_encoder_matches_append_bytes_exactly() {
        let plan = SeriesColdV2Plan::build(&sample_entries()).unwrap();
        let offsets = plan.section_offsets_at(20_480).unwrap();

        let mut complete_file = vec![0; 20_480];
        plan.append_sections_at(&mut complete_file, offsets)
            .unwrap();
        let expected = &complete_file[20_480..];

        let mut streamed = Vec::new();
        let bytes_written = plan.write_sections_at(&mut streamed, offsets).unwrap();
        assert_eq!(bytes_written, plan.lengths().total().unwrap());
        assert_eq!(usize::try_from(bytes_written).unwrap(), streamed.len());
        assert_eq!(streamed, expected);
    }

    #[test]
    fn packed_streaming_encoder_propagates_sink_failure_and_plan_is_reusable() {
        let plan = SeriesColdV2Plan::build(&sample_entries()).unwrap();
        let offsets = plan.section_offsets_at(0).unwrap();
        let mut expected = Vec::new();
        plan.write_sections_at(&mut expected, offsets).unwrap();
        let fail_after = expected.len() - 1;
        let mut failing = FailAfterWriter {
            bytes: Vec::new(),
            fail_after,
        };

        let error = plan.write_sections_at(&mut failing, offsets).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "injected packed-row write failure");
        assert_eq!(failing.bytes, expected[..fail_after]);

        let mut retried = Vec::new();
        plan.write_sections_at(&mut retried, offsets).unwrap();
        assert_eq!(retried, expected);
    }

    fn sample_entries() -> Vec<SeriesEntry> {
        vec![
            SeriesEntry {
                series_id: 1,
                kind_mask: 1,
                chunk_index: ChunkIndexRange { offset: 7, len: 8 },
                labels: vec![(2, 20), (1, 10)],
            },
            SeriesEntry {
                series_id: 2,
                kind_mask: 4,
                chunk_index: ChunkIndexRange { offset: 9, len: 10 },
                labels: vec![(1, 11), (2, 20)],
            },
        ]
    }

    fn series_entry(series_id: u64, labels: Vec<(u32, u32)>) -> SeriesEntry {
        SeriesEntry {
            series_id,
            kind_mask: 1,
            chunk_index: ChunkIndexRange { offset: 0, len: 0 },
            labels,
        }
    }

    fn cold_row(series_id: u64, keyset_id: u32, row: u32) -> SeriesColdV2SeriesRow {
        SeriesColdV2SeriesRow {
            series_id,
            kind_mask: 1,
            keyset_id,
            row,
        }
    }

    fn packed_rows(row_count: u32, widths: &[u8], data: &[u8]) -> ColdKeysetRows {
        let row_len = widths.iter().map(|width| u32::from(*width)).sum::<u32>();
        assert_eq!(
            data.len(),
            usize::try_from(row_count)
                .unwrap()
                .checked_mul(usize::try_from(row_len).unwrap())
                .unwrap()
        );
        ColdKeysetRows {
            row_count,
            expected_rows: row_count,
            row_len,
            widths: widths.to_vec().into_boxed_slice(),
            data: data.to_vec(),
        }
    }

    fn write_legacy_keyset_blocks_section(
        writer: &mut impl Write,
        section_offset: u64,
        keysets: &[Vec<u32>],
        rows_by_keyset: &[Vec<Vec<u32>>],
        value_dicts: &[(u32, Vec<u32>)],
    ) -> io::Result<()> {
        let dict_by_key = value_dicts
            .iter()
            .map(|(key, values)| (*key, values.as_slice()))
            .collect::<BTreeMap<_, _>>();
        let offsets_len = u64::try_from(
            keysets
                .len()
                .checked_add(1)
                .ok_or_else(|| invalid_input("legacy offset count overflow"))?,
        )
        .map_err(|_| invalid_input("legacy offset count exceeds u64"))?
        .checked_mul(8)
        .ok_or_else(|| invalid_input("legacy offset bytes overflow"))?;
        let mut cursor = section_offset
            .checked_add(offsets_len)
            .ok_or_else(|| invalid_input("legacy section offset overflow"))?;
        let mut shapes = Vec::with_capacity(keysets.len());

        for (idx, keyset) in keysets.iter().enumerate() {
            let rows = rows_by_keyset
                .get(idx)
                .ok_or_else(|| invalid_data("legacy keyset rows missing"))?;
            let widths = keyset
                .iter()
                .map(|key| {
                    let cardinality = dict_by_key
                        .get(key)
                        .ok_or_else(|| invalid_data("legacy value dictionary missing"))?
                        .len();
                    Ok(legacy_value_code_width(cardinality))
                })
                .collect::<io::Result<Vec<_>>>()?;
            let row_len = widths.iter().try_fold(0usize, |sum, width| {
                sum.checked_add(usize::from(*width))
                    .ok_or_else(|| invalid_input("legacy row length overflow"))
            })?;
            let data_len = row_len
                .checked_mul(rows.len())
                .ok_or_else(|| invalid_input("legacy data length overflow"))?;
            writer.write_all(&cursor.to_le_bytes())?;
            cursor = cursor
                .checked_add(16)
                .and_then(|value| value.checked_add(u64::try_from(widths.len()).ok()?))
                .and_then(|value| value.checked_add(u64::try_from(data_len).ok()?))
                .ok_or_else(|| invalid_input("legacy block length overflow"))?;
            shapes.push((widths, row_len, data_len));
        }
        writer.write_all(&cursor.to_le_bytes())?;

        for (idx, keyset) in keysets.iter().enumerate() {
            let rows = rows_by_keyset
                .get(idx)
                .ok_or_else(|| invalid_data("legacy keyset rows missing"))?;
            let (widths, row_len, data_len) = shapes
                .get(idx)
                .ok_or_else(|| invalid_data("legacy block shape missing"))?;
            writer.write_all(&u32::try_from(rows.len()).unwrap().to_le_bytes())?;
            writer.write_all(&u32::try_from(keyset.len()).unwrap().to_le_bytes())?;
            writer.write_all(&u32::try_from(*row_len).unwrap().to_le_bytes())?;
            writer.write_all(&u32::try_from(*data_len).unwrap().to_le_bytes())?;
            writer.write_all(widths)?;
            for row in rows {
                if row.len() != widths.len() {
                    return Err(invalid_data("legacy row width mismatch"));
                }
                for (code, width) in row.iter().copied().zip(widths.iter().copied()) {
                    legacy_write_value_code(writer, code, width)?;
                }
            }
        }
        Ok(())
    }

    fn legacy_value_code_width(cardinality: usize) -> u8 {
        match cardinality {
            0 | 1 => 0,
            2..=256 => 1,
            257..=65_536 => 2,
            _ => 4,
        }
    }

    fn legacy_write_value_code(writer: &mut impl Write, code: u32, width: u8) -> io::Result<()> {
        match width {
            0 if code == 0 => Ok(()),
            0 => Err(invalid_data("legacy implicit code is not zero")),
            1 => writer.write_all(&[
                u8::try_from(code).map_err(|_| invalid_input("legacy code exceeds u8"))?
            ]),
            2 => writer.write_all(
                &u16::try_from(code)
                    .map_err(|_| invalid_input("legacy code exceeds u16"))?
                    .to_le_bytes(),
            ),
            4 => writer.write_all(&code.to_le_bytes()),
            _ => Err(invalid_data("legacy invalid value code width")),
        }
    }

    #[derive(Debug)]
    struct FailAfterWriter {
        bytes: Vec<u8>,
        fail_after: usize,
    }

    impl Write for FailAfterWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let remaining = self.fail_after.saturating_sub(self.bytes.len());
            if remaining == 0 {
                return Err(io::Error::other("injected packed-row write failure"));
            }
            let written = remaining.min(bytes.len());
            self.bytes.extend_from_slice(&bytes[..written]);
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn append_expected_block(
        bytes: &mut Vec<u8>,
        rows: u32,
        key_count: u32,
        row_len: u32,
        widths: &[u8],
        data: &[u8],
    ) {
        bytes.extend_from_slice(&rows.to_le_bytes());
        bytes.extend_from_slice(&key_count.to_le_bytes());
        bytes.extend_from_slice(&row_len.to_le_bytes());
        bytes.extend_from_slice(&u32::try_from(data.len()).unwrap().to_le_bytes());
        bytes.extend_from_slice(widths);
        bytes.extend_from_slice(data);
    }

    fn read_u64(bytes: &[u8], offset: u64) -> u64 {
        let offset = usize::try_from(offset).unwrap();
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
    }

    fn read_u32(bytes: &[u8], offset: u64) -> u32 {
        let offset = usize::try_from(offset).unwrap();
        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
    }
}
