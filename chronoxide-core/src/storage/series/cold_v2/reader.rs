//! Pure readers and range planners for the unchanged `series.bin` v2 cold stream.
//!
//! The schema-6 reader and schema-7 authenticated-page reader share these
//! structural rules. This module deliberately performs no I/O and owns no
//! cache state: callers must authenticate the exact supplied byte ranges.

use std::io;
use std::ops::Range;

const OFFSET_LEN: u64 = 8;
const OFFSET_PAIR_LEN: usize = 16;
const KEYSET_HEADER_LEN: u64 = 8;
const VALUE_DICT_HEADER_LEN: u64 = 8;
const KEYSET_BLOCK_HEADER_LEN: u64 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValueDictMeta {
    pub(crate) key_sym: u32,
    pub(crate) values_offset: u64,
    pub(crate) cardinality: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeySetBlockMeta {
    pub(crate) rows: u32,
    pub(crate) key_count: u32,
    pub(crate) row_len_bytes: u32,
    pub(crate) data_len: u32,
    pub(crate) widths: Vec<u8>,
    pub(crate) data_offset: u64,
}

/// Exact byte length of one absolute-offset table with its terminal offset.
pub(crate) fn checked_offset_table_len(entry_count: u32) -> io::Result<u64> {
    u64::from(entry_count)
        .checked_add(1)
        .and_then(|count| count.checked_mul(OFFSET_LEN))
        .ok_or_else(|| invalid_data("series cold offset table length overflows"))
}

/// Validates that a cold section can contain the offset table implied by its count.
pub(crate) fn validate_offset_table_minimum(
    section_len: u64,
    entry_count: u32,
    what: &'static str,
) -> io::Result<()> {
    let minimum = checked_offset_table_len(entry_count)?;
    if section_len < minimum {
        return Err(invalid_data(match what {
            "keysets" => "series cold keysets section is shorter than its offset table",
            "value dictionaries" => {
                "series cold value dictionaries section is shorter than its offset table"
            }
            "keyset blocks" => "series cold keyset blocks section is shorter than its offset table",
            _ => "series cold section is shorter than its offset table",
        }));
    }
    Ok(())
}

/// Returns the exact complete offset-table range for one cold section.
pub(crate) fn offset_table_range(
    section_offset: u64,
    section_end: u64,
    entry_count: u32,
) -> io::Result<Range<u64>> {
    let section_len = section_end
        .checked_sub(section_offset)
        .ok_or_else(|| invalid_data("series cold section bounds are reversed"))?;
    validate_offset_table_minimum(section_len, entry_count, "section")?;
    let end = section_offset
        .checked_add(checked_offset_table_len(entry_count)?)
        .ok_or_else(|| invalid_data("series cold offset table end overflows"))?;
    Ok(section_offset..end)
}

/// Returns the exact 16-byte offset-pair range needed to locate one entry.
pub(crate) fn offset_pair_range(
    section_offset: u64,
    section_end: u64,
    entry_count: u32,
    entry_index: u32,
) -> io::Result<Range<u64>> {
    let table_range = offset_table_range(section_offset, section_end, entry_count)?;
    if entry_index >= entry_count {
        return Err(invalid_data("series cold entry index is out of bounds"));
    }
    let start = section_offset
        .checked_add(
            u64::from(entry_index)
                .checked_mul(OFFSET_LEN)
                .ok_or_else(|| invalid_data("series cold offset-pair position overflows"))?,
        )
        .ok_or_else(|| invalid_data("series cold offset-pair position overflows"))?;
    let end = start
        .checked_add(OFFSET_PAIR_LEN as u64)
        .ok_or_else(|| invalid_data("series cold offset-pair range overflows"))?;
    if end > table_range.end {
        return Err(invalid_data(
            "series cold offset-pair range exceeds its offset table",
        ));
    }
    Ok(start..end)
}

/// Decodes one authenticated offset pair into an exact entry range.
pub(crate) fn decode_entry_range(
    bytes: &[u8],
    section_offset: u64,
    section_end: u64,
    entry_count: u32,
    entry_index: u32,
) -> io::Result<Range<u64>> {
    offset_pair_range(section_offset, section_end, entry_count, entry_index)?;
    require_len(bytes, OFFSET_PAIR_LEN, "series cold offset pair")?;
    let start = read_u64_at(bytes, 0)?;
    let end = read_u64_at(bytes, 8)?;
    let entries_offset = section_offset
        .checked_add(checked_offset_table_len(entry_count)?)
        .ok_or_else(|| invalid_data("series cold entries offset overflows"))?;
    if start >= end {
        return Err(invalid_data(
            "series cold entry offsets are not strictly increasing",
        ));
    }
    if start < entries_offset || end > section_end {
        return Err(invalid_data("series cold entry bounds are invalid"));
    }
    if entry_index == 0 && start != entries_offset {
        return Err(invalid_data(
            "series cold first entry does not follow its offset table",
        ));
    }
    if entry_index + 1 == entry_count && end != section_end {
        return Err(invalid_data(
            "series cold final entry does not end at its section boundary",
        ));
    }
    Ok(start..end)
}

/// Decodes and validates a complete absolute-offset table.
pub(crate) fn decode_offset_table(
    bytes: &[u8],
    section_offset: u64,
    section_end: u64,
    entry_count: u32,
) -> io::Result<Vec<u64>> {
    offset_table_range(section_offset, section_end, entry_count)?;
    let expected_len = usize_from_u64(checked_offset_table_len(entry_count)?, "offset table")?;
    require_len(bytes, expected_len, "series cold offset table")?;

    let offset_count = usize::try_from(u64::from(entry_count) + 1)
        .map_err(|_| resource_error("series cold offset count exceeds usize"))?;
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(offset_count)
        .map_err(|_| resource_error("series cold offset allocation failed"))?;

    let entries_offset = section_offset
        .checked_add(checked_offset_table_len(entry_count)?)
        .ok_or_else(|| invalid_data("series cold entries offset overflows"))?;
    if entry_count == 0 {
        let terminal = read_u64_at(bytes, 0)?;
        if terminal != entries_offset || terminal != section_end {
            return Err(invalid_data(
                "series cold empty offset table does not end at its section boundary",
            ));
        }
        offsets.push(terminal);
        return Ok(offsets);
    }

    for entry_index in 0..entry_count {
        let pair_start = usize::try_from(entry_index)
            .map_err(|_| resource_error("series cold entry index exceeds usize"))?
            .checked_mul(OFFSET_LEN as usize)
            .ok_or_else(|| resource_error("series cold offset-pair index overflows"))?;
        let pair_end = pair_start
            .checked_add(OFFSET_PAIR_LEN)
            .ok_or_else(|| resource_error("series cold offset-pair index overflows"))?;
        let pair = bytes
            .get(pair_start..pair_end)
            .ok_or_else(|| unexpected_eof("series cold offset pair is truncated"))?;
        let range =
            decode_entry_range(pair, section_offset, section_end, entry_count, entry_index)?;
        if entry_index == 0 {
            offsets.push(range.start);
        }
        offsets.push(range.end);
    }
    Ok(offsets)
}

/// Decodes one exact keyset entry and enforces canonical key order.
pub(crate) fn decode_keyset_entry(
    bytes: &[u8],
    entry_start: u64,
    entry_end: u64,
) -> io::Result<Vec<u32>> {
    require_range_len(bytes, entry_start, entry_end, "series cold keyset entry")?;
    if bytes.len() < KEYSET_HEADER_LEN as usize {
        return Err(unexpected_eof("series cold keyset header is truncated"));
    }
    let key_count = read_u32_at(bytes, 0)?;
    if read_u32_at(bytes, 4)? != 0 {
        return Err(invalid_data("series cold keyset reserved field is nonzero"));
    }
    let expected_len = KEYSET_HEADER_LEN
        .checked_add(
            u64::from(key_count)
                .checked_mul(4)
                .ok_or_else(|| invalid_data("series cold keyset length overflows"))?,
        )
        .ok_or_else(|| invalid_data("series cold keyset length overflows"))?;
    if expected_len != bytes.len() as u64 {
        return Err(invalid_data("series cold keyset entry length mismatch"));
    }

    let key_count = usize::try_from(key_count)
        .map_err(|_| resource_error("series cold key count exceeds usize"))?;
    let mut keys = Vec::new();
    keys.try_reserve_exact(key_count)
        .map_err(|_| resource_error("series cold keyset allocation failed"))?;
    let mut previous = None;
    for index in 0..key_count {
        let key = read_u32_at(bytes, KEYSET_HEADER_LEN as usize + index * 4)?;
        if previous.is_some_and(|previous| previous >= key) {
            return Err(invalid_data(
                "series cold keyset keys are not strictly increasing",
            ));
        }
        previous = Some(key);
        keys.push(key);
    }
    Ok(keys)
}

/// Returns the exact fixed header range for one value dictionary entry.
pub(crate) fn value_dict_header_range(entry_start: u64, entry_end: u64) -> io::Result<Range<u64>> {
    fixed_prefix_range(
        entry_start,
        entry_end,
        VALUE_DICT_HEADER_LEN,
        "series cold value dictionary header exceeds its entry",
    )
}

/// Decodes an exact value-dictionary header and validates its full entry range.
pub(crate) fn decode_value_dict_meta(
    header_bytes: &[u8],
    entry_start: u64,
    entry_end: u64,
) -> io::Result<ValueDictMeta> {
    require_len(
        header_bytes,
        VALUE_DICT_HEADER_LEN as usize,
        "series cold value dictionary header",
    )?;
    let key_sym = read_u32_at(header_bytes, 0)?;
    let cardinality = read_u32_at(header_bytes, 4)?;
    if cardinality == 0 {
        return Err(invalid_data(
            "series cold value dictionary cardinality is zero",
        ));
    }
    let values_len = u64::from(cardinality)
        .checked_mul(4)
        .ok_or_else(|| invalid_data("series cold value dictionary length overflows"))?;
    let expected_end = entry_start
        .checked_add(VALUE_DICT_HEADER_LEN)
        .and_then(|offset| offset.checked_add(values_len))
        .ok_or_else(|| invalid_data("series cold value dictionary range overflows"))?;
    if expected_end != entry_end {
        return Err(invalid_data(
            "series cold value dictionary entry length mismatch",
        ));
    }
    Ok(ValueDictMeta {
        key_sym,
        values_offset: entry_start + VALUE_DICT_HEADER_LEN,
        cardinality,
    })
}

/// Decodes one exact complete value dictionary.
pub(crate) fn decode_value_dict_values(bytes: &[u8], meta: ValueDictMeta) -> io::Result<Vec<u32>> {
    let expected_len = usize_from_u64(
        u64::from(meta.cardinality)
            .checked_mul(4)
            .ok_or_else(|| invalid_data("series cold value dictionary length overflows"))?,
        "value dictionary",
    )?;
    require_len(bytes, expected_len, "series cold value dictionary values")?;
    let count = usize::try_from(meta.cardinality)
        .map_err(|_| resource_error("series cold value dictionary count exceeds usize"))?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| resource_error("series cold value dictionary allocation failed"))?;
    for index in 0..count {
        let value = read_u32_at(bytes, index * 4)?;
        values.push(value);
    }
    Ok(values)
}

/// Decodes one exact sparse dictionary value after checking its code bound.
pub(crate) fn decode_value_dict_value(
    bytes: &[u8],
    meta: ValueDictMeta,
    code: u32,
) -> io::Result<u32> {
    value_dict_value_range(meta, code)?;
    require_len(bytes, 4, "series cold value dictionary value")?;
    read_u32_at(bytes, 0)
}

/// Returns the exact four-byte range for one sparse dictionary value.
pub(crate) fn value_dict_value_range(meta: ValueDictMeta, code: u32) -> io::Result<Range<u64>> {
    if code >= meta.cardinality {
        return Err(invalid_data("series cold value code is out of bounds"));
    }
    let start = meta
        .values_offset
        .checked_add(
            u64::from(code)
                .checked_mul(4)
                .ok_or_else(|| invalid_data("series cold value dictionary offset overflows"))?,
        )
        .ok_or_else(|| invalid_data("series cold value dictionary offset overflows"))?;
    let end = start
        .checked_add(4)
        .ok_or_else(|| invalid_data("series cold value dictionary range overflows"))?;
    Ok(start..end)
}

/// Returns the exact fixed header range for one keyset block.
pub(crate) fn keyset_block_header_range(
    entry_start: u64,
    entry_end: u64,
) -> io::Result<Range<u64>> {
    fixed_prefix_range(
        entry_start,
        entry_end,
        KEYSET_BLOCK_HEADER_LEN,
        "series cold keyset block header exceeds its entry",
    )
}

/// Returns the exact width-array range after decoding a fixed block header.
pub(crate) fn keyset_block_widths_range(
    fixed_header: &[u8],
    entry_start: u64,
    entry_end: u64,
) -> io::Result<Range<u64>> {
    require_len(
        fixed_header,
        KEYSET_BLOCK_HEADER_LEN as usize,
        "series cold keyset block header",
    )?;
    let key_count = read_u32_at(fixed_header, 4)?;
    let start = keyset_block_header_range(entry_start, entry_end)?.end;
    let end = start
        .checked_add(u64::from(key_count))
        .ok_or_else(|| invalid_data("series cold keyset block widths range overflows"))?;
    if end > entry_end {
        return Err(invalid_data(
            "series cold keyset block widths exceed its entry",
        ));
    }
    Ok(start..end)
}

/// Decodes one exact block header plus width array.
pub(crate) fn decode_keyset_block_meta(
    fixed_header: &[u8],
    widths: &[u8],
    entry_start: u64,
    entry_end: u64,
) -> io::Result<KeySetBlockMeta> {
    require_len(
        fixed_header,
        KEYSET_BLOCK_HEADER_LEN as usize,
        "series cold keyset block header",
    )?;
    let rows = read_u32_at(fixed_header, 0)?;
    let key_count = read_u32_at(fixed_header, 4)?;
    let row_len_bytes = read_u32_at(fixed_header, 8)?;
    let data_len = read_u32_at(fixed_header, 12)?;
    if rows == 0 {
        return Err(invalid_data("series cold keyset block has no rows"));
    }
    if widths.len()
        != usize::try_from(key_count)
            .map_err(|_| resource_error("series cold key count exceeds usize"))?
    {
        return Err(invalid_data(
            "series cold keyset block width count mismatch",
        ));
    }
    let mut width_sum = 0u32;
    for &width in widths {
        if !matches!(width, 0 | 1 | 2 | 4) {
            return Err(invalid_data("series cold value-code width is invalid"));
        }
        width_sum = width_sum
            .checked_add(u32::from(width))
            .ok_or_else(|| invalid_data("series cold keyset row width overflows"))?;
    }
    if width_sum != row_len_bytes {
        return Err(invalid_data(
            "series cold keyset row length does not match its widths",
        ));
    }
    let expected_data_len = rows
        .checked_mul(row_len_bytes)
        .ok_or_else(|| invalid_data("series cold keyset block data length overflows"))?;
    if expected_data_len != data_len {
        return Err(invalid_data(
            "series cold keyset block data length mismatch",
        ));
    }
    let data_offset = entry_start
        .checked_add(KEYSET_BLOCK_HEADER_LEN)
        .and_then(|offset| offset.checked_add(u64::from(key_count)))
        .ok_or_else(|| invalid_data("series cold keyset block data offset overflows"))?;
    let expected_end = data_offset
        .checked_add(u64::from(data_len))
        .ok_or_else(|| invalid_data("series cold keyset block range overflows"))?;
    if expected_end != entry_end {
        return Err(invalid_data(
            "series cold keyset block entry length mismatch",
        ));
    }
    let mut owned_widths = Vec::new();
    owned_widths
        .try_reserve_exact(widths.len())
        .map_err(|_| resource_error("series cold keyset widths allocation failed"))?;
    owned_widths.extend_from_slice(widths);
    Ok(KeySetBlockMeta {
        rows,
        key_count,
        row_len_bytes,
        data_len,
        widths: owned_widths,
        data_offset,
    })
}

/// Returns the exact packed byte range for one validated block row.
pub(crate) fn keyset_block_row_range(block: &KeySetBlockMeta, row: u32) -> io::Result<Range<u64>> {
    if row >= block.rows {
        return Err(invalid_data(
            "series cold keyset block row is out of bounds",
        ));
    }
    let start = block
        .data_offset
        .checked_add(
            u64::from(row)
                .checked_mul(u64::from(block.row_len_bytes))
                .ok_or_else(|| invalid_data("series cold keyset block row offset overflows"))?,
        )
        .ok_or_else(|| invalid_data("series cold keyset block row offset overflows"))?;
    let end = start
        .checked_add(u64::from(block.row_len_bytes))
        .ok_or_else(|| invalid_data("series cold keyset block row range overflows"))?;
    let data_end = block
        .data_offset
        .checked_add(u64::from(block.data_len))
        .ok_or_else(|| invalid_data("series cold keyset block data range overflows"))?;
    if end > data_end {
        return Err(invalid_data(
            "series cold keyset block row exceeds its data range",
        ));
    }
    Ok(start..end)
}

pub(crate) fn validate_keyset_block_key_count(
    block: &KeySetBlockMeta,
    key_count: usize,
) -> io::Result<()> {
    if usize::try_from(block.key_count).ok() != Some(key_count) {
        return Err(invalid_data(
            "series cold keyset and block key counts differ",
        ));
    }
    Ok(())
}

pub(crate) fn canonical_value_code_width(cardinality: u32) -> io::Result<u8> {
    match cardinality {
        0 => Err(invalid_data(
            "series cold value dictionary cardinality is zero",
        )),
        1 => Ok(0),
        2..=256 => Ok(1),
        257..=65_536 => Ok(2),
        _ => Ok(4),
    }
}

pub(crate) fn validate_value_code_width(width: u8, cardinality: u32) -> io::Result<()> {
    if width != canonical_value_code_width(cardinality)? {
        return Err(invalid_data(
            "series cold value-code width is noncanonical for its dictionary",
        ));
    }
    Ok(())
}

pub(crate) fn read_value_code(row: &[u8], cursor: &mut usize, width: u8) -> io::Result<u32> {
    let value = match width {
        0 => 0,
        1 => u32::from(read_exact_array::<1>(row, cursor)?[0]),
        2 => u32::from(u16::from_le_bytes(read_exact_array::<2>(row, cursor)?)),
        4 => u32::from_le_bytes(read_exact_array::<4>(row, cursor)?),
        _ => return Err(invalid_data("series cold value-code width is invalid")),
    };
    Ok(value)
}

fn fixed_prefix_range(
    entry_start: u64,
    entry_end: u64,
    prefix_len: u64,
    overflow_message: &'static str,
) -> io::Result<Range<u64>> {
    let end = entry_start
        .checked_add(prefix_len)
        .ok_or_else(|| invalid_data(overflow_message))?;
    if end > entry_end {
        return Err(invalid_data(overflow_message));
    }
    Ok(entry_start..end)
}

fn require_range_len(bytes: &[u8], start: u64, end: u64, what: &'static str) -> io::Result<()> {
    let range_len = end
        .checked_sub(start)
        .ok_or_else(|| invalid_data("series cold entry bounds are reversed"))?;
    let bytes_len = u64::try_from(bytes.len())
        .map_err(|_| resource_error("series cold entry length exceeds u64"))?;
    if range_len != bytes_len {
        return Err(invalid_data(match what {
            "series cold keyset entry" => "series cold keyset entry length is not exact",
            _ => "series cold entry length is not exact",
        }));
    }
    Ok(())
}

fn require_len(bytes: &[u8], expected: usize, what: &'static str) -> io::Result<()> {
    if bytes.len() != expected {
        return Err(invalid_data(match what {
            "series cold offset pair" => "series cold offset pair length is not exact",
            "series cold offset table" => "series cold offset table length is not exact",
            "series cold value dictionary header" => {
                "series cold value dictionary header length is not exact"
            }
            "series cold value dictionary values" => {
                "series cold value dictionary values length is not exact"
            }
            "series cold value dictionary value" => {
                "series cold value dictionary value length is not exact"
            }
            "series cold keyset block header" => {
                "series cold keyset block header length is not exact"
            }
            _ => "series cold range length is not exact",
        }));
    }
    Ok(())
}

fn read_u32_at(bytes: &[u8], offset: usize) -> io::Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| unexpected_eof("series cold u32 offset overflows"))?;
    let encoded = bytes
        .get(offset..end)
        .ok_or_else(|| unexpected_eof("series cold u32 is truncated"))?;
    Ok(u32::from_le_bytes(
        encoded.try_into().expect("checked fixed-width slice"),
    ))
}

fn read_u64_at(bytes: &[u8], offset: usize) -> io::Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| unexpected_eof("series cold u64 offset overflows"))?;
    let encoded = bytes
        .get(offset..end)
        .ok_or_else(|| unexpected_eof("series cold u64 is truncated"))?;
    Ok(u64::from_le_bytes(
        encoded.try_into().expect("checked fixed-width slice"),
    ))
}

fn read_exact_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> io::Result<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| unexpected_eof("series cold row cursor overflows"))?;
    let encoded = bytes
        .get(*cursor..end)
        .ok_or_else(|| unexpected_eof("series cold packed row is truncated"))?;
    *cursor = end;
    Ok(encoded.try_into().expect("checked fixed-width slice"))
}

fn usize_from_u64(value: u64, what: &'static str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| {
        resource_error(match what {
            "offset table" => "series cold offset table length exceeds usize",
            "value dictionary" => "series cold value dictionary length exceeds usize",
            _ => "series cold length exceeds usize",
        })
    })
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn unexpected_eof(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, message)
}

fn resource_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::OutOfMemory, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn assert_invalid(error: io::Error, expected: &str) {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn offset_tables_and_point_ranges_enforce_exact_section_bounds() {
        let mut table = vec![0; 24];
        put_u64(&mut table, 0, 124);
        put_u64(&mut table, 8, 132);
        put_u64(&mut table, 16, 144);

        assert_eq!(checked_offset_table_len(2).unwrap(), 24);
        assert_eq!(offset_table_range(100, 144, 2).unwrap(), 100..124);
        assert_eq!(offset_pair_range(100, 144, 2, 0).unwrap(), 100..116);
        assert_eq!(offset_pair_range(100, 144, 2, 1).unwrap(), 108..124);
        assert_eq!(
            decode_entry_range(&table[..16], 100, 144, 2, 0).unwrap(),
            124..132
        );
        assert_eq!(
            decode_entry_range(&table[8..], 100, 144, 2, 1).unwrap(),
            132..144
        );
        assert_eq!(
            decode_offset_table(&table, 100, 144, 2).unwrap(),
            vec![124, 132, 144]
        );
        assert_eq!(
            decode_offset_table(&108u64.to_le_bytes(), 100, 108, 0).unwrap(),
            vec![108]
        );

        assert_invalid(
            validate_offset_table_minimum(23, 2, "keysets").unwrap_err(),
            "shorter than its offset table",
        );
        let mut bad = table.clone();
        put_u64(&mut bad, 8, 124);
        assert_invalid(
            decode_offset_table(&bad, 100, 144, 2).unwrap_err(),
            "not strictly increasing",
        );
        assert_invalid(
            decode_entry_range(&table[..16], 100, 131, 2, 0).unwrap_err(),
            "bounds are invalid",
        );
        assert_invalid(
            decode_offset_table(&108u64.to_le_bytes(), 100, 109, 0).unwrap_err(),
            "empty offset table",
        );
    }

    #[test]
    fn keysets_require_zero_reserved_exact_length_and_strict_keys() {
        let mut bytes = vec![0; 20];
        put_u32(&mut bytes, 0, 3);
        put_u32(&mut bytes, 8, 1);
        put_u32(&mut bytes, 12, 4);
        put_u32(&mut bytes, 16, 9);
        assert_eq!(
            decode_keyset_entry(&bytes, 200, 220).unwrap(),
            vec![1, 4, 9]
        );

        let mut bad = bytes.clone();
        put_u32(&mut bad, 4, 1);
        assert_invalid(
            decode_keyset_entry(&bad, 200, 220).unwrap_err(),
            "reserved field",
        );
        let mut bad = bytes.clone();
        put_u32(&mut bad, 16, 4);
        assert_invalid(
            decode_keyset_entry(&bad, 200, 220).unwrap_err(),
            "not strictly increasing",
        );
        assert_invalid(
            decode_keyset_entry(&bytes, 200, 219).unwrap_err(),
            "not exact",
        );
    }

    #[test]
    fn dictionaries_enforce_shape_codes_and_width_boundaries() {
        let mut header = [0; 8];
        put_u32(&mut header, 0, 7);
        put_u32(&mut header, 4, 3);
        let meta = decode_value_dict_meta(&header, 300, 320).unwrap();
        assert_eq!(meta.key_sym, 7);
        assert_eq!(meta.values_offset, 308);
        assert_eq!(value_dict_header_range(300, 320).unwrap(), 300..308);
        assert_eq!(value_dict_value_range(meta, 2).unwrap(), 316..320);
        assert_eq!(
            decode_value_dict_values(&[1, 0, 0, 0, 5, 0, 0, 0, 9, 0, 0, 0], meta).unwrap(),
            vec![1, 5, 9]
        );
        assert_eq!(
            decode_value_dict_value(&9u32.to_le_bytes(), meta, 2).unwrap(),
            9
        );

        assert_invalid(
            decode_value_dict_value(&0u32.to_le_bytes(), meta, 3).unwrap_err(),
            "out of bounds",
        );
        assert_invalid(
            value_dict_header_range(300, 307).unwrap_err(),
            "exceeds its entry",
        );

        for (cardinality, width) in [
            (1, 0),
            (2, 1),
            (256, 1),
            (257, 2),
            (65_536, 2),
            (65_537, 4),
            (u32::MAX, 4),
        ] {
            assert_eq!(canonical_value_code_width(cardinality).unwrap(), width);
            validate_value_code_width(width, cardinality).unwrap();
        }
        assert_invalid(
            canonical_value_code_width(0).unwrap_err(),
            "cardinality is zero",
        );
        assert_invalid(
            validate_value_code_width(2, 256).unwrap_err(),
            "noncanonical",
        );
    }

    #[test]
    fn keyset_blocks_require_exact_canonical_widths_and_data_shape() {
        let mut fixed = [0; 16];
        put_u32(&mut fixed, 0, 2);
        put_u32(&mut fixed, 4, 3);
        put_u32(&mut fixed, 8, 3);
        put_u32(&mut fixed, 12, 6);
        let block = decode_keyset_block_meta(&fixed, &[0, 1, 2], 400, 425).unwrap();
        assert_eq!(block.rows, 2);
        assert_eq!(block.data_offset, 419);
        assert_eq!(keyset_block_header_range(400, 425).unwrap(), 400..416);
        assert_eq!(
            keyset_block_widths_range(&fixed, 400, 425).unwrap(),
            416..419
        );
        assert_eq!(keyset_block_row_range(&block, 0).unwrap(), 419..422);
        assert_eq!(keyset_block_row_range(&block, 1).unwrap(), 422..425);
        validate_keyset_block_key_count(&block, 3).unwrap();

        assert_invalid(
            decode_keyset_block_meta(&fixed, &[0, 3, 0], 400, 425).unwrap_err(),
            "width is invalid",
        );
        assert_invalid(
            decode_keyset_block_meta(&fixed, &[0, 1, 1], 400, 425).unwrap_err(),
            "row length does not match",
        );
        let mut zero_rows = fixed;
        put_u32(&mut zero_rows, 0, 0);
        assert_invalid(
            decode_keyset_block_meta(&zero_rows, &[0, 1, 2], 400, 425).unwrap_err(),
            "has no rows",
        );
        assert_invalid(
            keyset_block_header_range(400, 415).unwrap_err(),
            "exceeds its entry",
        );
        assert_invalid(
            keyset_block_widths_range(&fixed, 400, 418).unwrap_err(),
            "widths exceed",
        );
        assert_invalid(
            keyset_block_row_range(&block, 2).unwrap_err(),
            "out of bounds",
        );

        let row = [7, 0x34, 0x12, 0xef, 0xcd, 0xab, 0x89];
        let mut cursor = 0;
        assert_eq!(read_value_code(&row, &mut cursor, 0).unwrap(), 0);
        assert_eq!(read_value_code(&row, &mut cursor, 1).unwrap(), 7);
        assert_eq!(read_value_code(&row, &mut cursor, 2).unwrap(), 0x1234);
        assert_eq!(read_value_code(&row, &mut cursor, 4).unwrap(), 0x89ab_cdef);
        assert_eq!(cursor, row.len());
    }
}
