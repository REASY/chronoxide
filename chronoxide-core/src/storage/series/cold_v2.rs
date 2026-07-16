//! Deterministic writer-side plan for the `series.bin` v2 cold label stream.
//!
//! Schema 7 retains these three byte encodings unchanged, but rebases their
//! absolute offset tables after its paged hot-series region. Keeping the plan
//! independent of either hot-record version makes that sharing explicit.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};

use super::SeriesEntry;

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

/// Canonical label rows and exact section sizes shared by series v2 and v3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeriesColdV2Plan {
    series_rows: Vec<SeriesColdV2SeriesRow>,
    num_series: u32,
    num_keysets: u32,
    num_value_dicts: u32,
    keysets: Vec<Vec<u32>>,
    value_dicts: Vec<(u32, Vec<u32>)>,
    rows_by_keyset: Vec<Vec<Vec<u32>>>,
    lengths: SeriesColdV2Lengths,
}

impl SeriesColdV2Plan {
    pub(crate) fn build(entries: &[SeriesEntry]) -> io::Result<Self> {
        let num_series = checked_u32(entries.len(), "series count")?;
        let normalized = normalize_series_entries(entries);
        let keysets = collect_keysets(&normalized);
        let num_keysets = checked_u32(keysets.len(), "keyset count")?;
        let keyset_ids = keyset_id_map(&keysets)?;
        let value_dicts = collect_value_dicts(&normalized);
        let num_value_dicts = checked_u32(value_dicts.len(), "value dictionary count")?;
        validate_cold_shapes(&keysets, &value_dicts)?;
        let value_codes = value_code_maps(&value_dicts)?;

        let mut rows_by_keyset = vec![Vec::new(); keysets.len()];
        let mut series_rows = Vec::with_capacity(normalized.len());
        for entry in &normalized {
            let keyset = entry.labels.iter().map(|(key, _)| *key).collect::<Vec<_>>();
            let keyset_id = *keyset_ids
                .get(&keyset)
                .ok_or_else(|| invalid_data("series keyset missing"))?;
            let keyset_idx =
                usize::try_from(keyset_id).map_err(|_| invalid_input("keyset id exceeds usize"))?;
            let rows = rows_by_keyset
                .get_mut(keyset_idx)
                .ok_or_else(|| invalid_data("series keyset id out of bounds"))?;
            let row = checked_u32(rows.len(), "keyset row count")?;

            let mut codes = Vec::with_capacity(entry.labels.len());
            for (key, value) in &entry.labels {
                let code = value_codes
                    .get(key)
                    .and_then(|codes| codes.get(value))
                    .copied()
                    .ok_or_else(|| invalid_data("series value code missing"))?;
                codes.push(code);
            }
            rows.push(codes);
            series_rows.push(SeriesColdV2SeriesRow {
                series_id: entry.series_id,
                kind_mask: entry.kind_mask,
                keyset_id,
                row,
            });
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

fn collect_keysets(entries: &[NormalizedSeriesEntry]) -> Vec<Vec<u32>> {
    let mut keysets = BTreeSet::new();
    for entry in entries {
        keysets.insert(entry.labels.iter().map(|(key, _)| *key).collect());
    }
    keysets.into_iter().collect()
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
    rows_by_keyset: &[Vec<Vec<u32>>],
    value_dicts: &[(u32, Vec<u32>)],
) -> io::Result<u64> {
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    let dict_by_key = dictionary_slices(value_dicts);
    keysets
        .iter()
        .enumerate()
        .try_fold(offsets_len, |len, (idx, keyset)| {
            let widths = widths_for_keyset(keyset, &dict_by_key);
            let row_len = widths.iter().try_fold(0u64, |sum, width| {
                sum.checked_add(u64::from(*width))
                    .ok_or_else(|| invalid_input("series keyset row is too large"))
            })?;
            let rows = rows_by_keyset
                .get(idx)
                .ok_or_else(|| invalid_data("keyset rows missing"))?;
            checked_u32(rows.len(), "keyset block rows")?;
            u32::try_from(row_len).map_err(|_| invalid_input("keyset row length exceeds u32"))?;
            let data_len = row_len
                .checked_mul(checked_u64(rows.len(), "keyset block rows")?)
                .ok_or_else(|| invalid_input("series keyset block data is too large"))?;
            u32::try_from(data_len)
                .map_err(|_| invalid_input("keyset block data length exceeds u32"))?;
            let block_len = 16u64
                .checked_add(checked_u64(widths.len(), "width count")?)
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
    rows_by_keyset: &[Vec<Vec<u32>>],
    value_dicts: &[(u32, Vec<u32>)],
) -> io::Result<()> {
    let dict_by_key = dictionary_slices(value_dicts);
    let offsets_len = checked_section_offsets_len(keysets.len())?;
    let mut cursor = section_offset
        .checked_add(offsets_len)
        .ok_or_else(|| invalid_input("series keyset blocks offset overflow"))?;

    let mut block_shapes = Vec::with_capacity(keysets.len());
    for (idx, keyset) in keysets.iter().enumerate() {
        let rows = rows_by_keyset
            .get(idx)
            .ok_or_else(|| invalid_data("keyset rows missing"))?;
        let widths = widths_for_keyset(keyset, &dict_by_key);
        let row_len = widths.iter().try_fold(0usize, |sum, width| {
            sum.checked_add(usize::from(*width))
                .ok_or_else(|| invalid_input("series keyset row is too large"))
        })?;
        let data_len = row_len
            .checked_mul(rows.len())
            .ok_or_else(|| invalid_input("series keyset block data is too large"))?;
        checked_u32(rows.len(), "keyset block rows")?;
        checked_u32(row_len, "keyset row length")?;
        checked_u32(data_len, "keyset data length")?;

        writer.write_all(&cursor.to_le_bytes())?;
        let data_len_u64 = checked_u64(data_len, "keyset data length")?;
        let block_len = 16u64
            .checked_add(checked_u64(widths.len(), "width count")?)
            .and_then(|value| value.checked_add(data_len_u64))
            .ok_or_else(|| invalid_input("series keyset block is too large"))?;
        cursor = cursor
            .checked_add(block_len)
            .ok_or_else(|| invalid_input("series keyset blocks section is too large"))?;
        block_shapes.push((widths, row_len, data_len));
    }
    writer.write_all(&cursor.to_le_bytes())?;

    for (idx, keyset) in keysets.iter().enumerate() {
        let rows = rows_by_keyset
            .get(idx)
            .ok_or_else(|| invalid_data("keyset rows missing"))?;
        let (widths, row_len, data_len) = block_shapes
            .get(idx)
            .ok_or_else(|| invalid_data("keyset block shape missing"))?;
        writer.write_all(&checked_u32(rows.len(), "keyset block rows")?.to_le_bytes())?;
        writer.write_all(&checked_u32(keyset.len(), "keyset length")?.to_le_bytes())?;
        writer.write_all(&checked_u32(*row_len, "keyset row length")?.to_le_bytes())?;
        writer.write_all(&checked_u32(*data_len, "keyset data length")?.to_le_bytes())?;
        writer.write_all(widths)?;
        for row in rows {
            if row.len() != widths.len() {
                return Err(invalid_data("keyset row width count mismatch"));
            }
            for (code, width) in row.iter().copied().zip(widths.iter().copied()) {
                write_value_code(writer, code, width)?;
            }
        }
    }
    Ok(())
}

fn dictionary_slices(value_dicts: &[(u32, Vec<u32>)]) -> BTreeMap<u32, &[u32]> {
    value_dicts
        .iter()
        .map(|(key, values)| (*key, values.as_slice()))
        .collect()
}

fn widths_for_keyset(keyset: &[u32], dict_by_key: &BTreeMap<u32, &[u32]>) -> Vec<u8> {
    keyset
        .iter()
        .map(|key| value_code_width(dict_by_key.get(key).map_or(0, |values| values.len())))
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
    fn streaming_encoder_matches_compatibility_vec_bytes_exactly() {
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

    fn read_u64(bytes: &[u8], offset: u64) -> u64 {
        let offset = usize::try_from(offset).unwrap();
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
    }
}
