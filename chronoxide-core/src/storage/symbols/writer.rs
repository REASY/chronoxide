use std::io::{self, Write};

use crc32c::crc32c;

use super::format::{
    ROOT_CRC_OFFSET, SYMBOLS_V3_HEADER_LEN, SYMBOLS_V3_MAGIC, SYMBOLS_V3_MAX_PAGE_BYTES,
    SYMBOLS_V3_MAX_ROOT_BYTES, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN, SYMBOLS_V3_PAGE_HEADER_LEN,
    SYMBOLS_V3_PAGE_MAGIC, SYMBOLS_V3_PAGE_TARGET_BYTES, SYMBOLS_V3_PAGE_VERSION,
    SYMBOLS_V3_VERSION, invalid_symbols_input, put_u16, put_u32, put_u64, symbols_root_crc,
};

struct EncodedSymbolPage {
    first_symbol_id: u32,
    symbol_count: u32,
    string_bytes_len: u32,
    first_fence: Vec<u8>,
    last_fence: Vec<u8>,
    bytes: Vec<u8>,
    crc32c: u32,
}

#[derive(Clone, Copy)]
pub(super) struct SymbolWriterOperationalLimits {
    pub(super) max_page_bytes: usize,
    pub(super) max_root_bytes: usize,
}

impl SymbolWriterOperationalLimits {
    const PRODUCTION: Self = Self {
        max_page_bytes: SYMBOLS_V3_MAX_PAGE_BYTES,
        max_root_bytes: SYMBOLS_V3_MAX_ROOT_BYTES,
    };
}

pub fn write_symbols_bin_v3<W, I, S>(writer: W, symbols: I) -> io::Result<()>
where
    W: Write,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    write_symbols_bin_v3_with_operational_limits(
        writer,
        symbols,
        SymbolWriterOperationalLimits::PRODUCTION,
    )
}

pub(super) fn write_symbols_bin_v3_with_operational_limits<W, I, S>(
    mut writer: W,
    symbols: I,
    limits: SymbolWriterOperationalLimits,
) -> io::Result<()>
where
    W: Write,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let values = symbols
        .into_iter()
        .map(|value| value.as_ref().as_bytes().to_vec())
        .collect::<Vec<_>>();
    let symbol_count = u32::try_from(values.len())
        .map_err(|_| invalid_symbols_input("symbol count exceeds u32"))?;
    validate_sorted_values(&values)?;

    let pages = encode_pages(&values, limits.max_page_bytes)?;
    let page_count = u32::try_from(pages.len())
        .map_err(|_| invalid_symbols_input("symbols page count exceeds u32"))?;
    let directory_len = pages
        .len()
        .checked_mul(SYMBOLS_V3_PAGE_DESCRIPTOR_LEN)
        .ok_or_else(|| invalid_symbols_input("symbols directory length overflow"))?;
    let fence_offset = SYMBOLS_V3_HEADER_LEN
        .checked_add(directory_len)
        .ok_or_else(|| invalid_symbols_input("symbols fence offset overflow"))?;

    let mut fences = Vec::new();
    let mut fence_ranges = Vec::with_capacity(pages.len());
    for page in &pages {
        let first_offset = u32::try_from(fences.len())
            .map_err(|_| invalid_symbols_input("symbols fence region exceeds u32"))?;
        let first_len = u32::try_from(page.first_fence.len())
            .map_err(|_| invalid_symbols_input("symbols first fence exceeds u32"))?;
        fences.extend_from_slice(&page.first_fence);
        let last_offset = u32::try_from(fences.len())
            .map_err(|_| invalid_symbols_input("symbols fence region exceeds u32"))?;
        let last_len = u32::try_from(page.last_fence.len())
            .map_err(|_| invalid_symbols_input("symbols last fence exceeds u32"))?;
        fences.extend_from_slice(&page.last_fence);
        fence_ranges.push((first_offset, first_len, last_offset, last_len));
    }
    let pages_offset = fence_offset
        .checked_add(fences.len())
        .ok_or_else(|| invalid_symbols_input("symbols pages offset overflow"))?;
    if pages_offset > limits.max_root_bytes {
        return Err(invalid_symbols_input(
            "symbols root exceeds the operational size limit",
        ));
    }
    let mut file_len = u64::try_from(pages_offset)
        .map_err(|_| invalid_symbols_input("symbols pages offset exceeds u64"))?;
    for page in &pages {
        file_len = file_len
            .checked_add(
                u64::try_from(page.bytes.len())
                    .map_err(|_| invalid_symbols_input("symbols page length exceeds u64"))?,
            )
            .ok_or_else(|| invalid_symbols_input("symbols file length overflow"))?;
    }

    let mut root = vec![0u8; pages_offset];
    put_u32(&mut root, 0, SYMBOLS_V3_MAGIC);
    put_u16(&mut root, 4, SYMBOLS_V3_VERSION);
    put_u16(&mut root, 6, 0);
    put_u32(&mut root, 8, SYMBOLS_V3_HEADER_LEN as u32);
    put_u32(&mut root, 12, SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32);
    put_u32(&mut root, 16, symbol_count);
    put_u32(&mut root, 20, page_count);
    put_u64(&mut root, 24, SYMBOLS_V3_HEADER_LEN as u64);
    put_u64(
        &mut root,
        32,
        u64::try_from(directory_len)
            .map_err(|_| invalid_symbols_input("symbols directory length exceeds u64"))?,
    );
    put_u64(
        &mut root,
        40,
        u64::try_from(fence_offset)
            .map_err(|_| invalid_symbols_input("symbols fence offset exceeds u64"))?,
    );
    put_u64(
        &mut root,
        48,
        u64::try_from(fences.len())
            .map_err(|_| invalid_symbols_input("symbols fence length exceeds u64"))?,
    );
    put_u64(
        &mut root,
        56,
        u64::try_from(pages_offset)
            .map_err(|_| invalid_symbols_input("symbols pages offset exceeds u64"))?,
    );
    put_u64(&mut root, 64, file_len);
    put_u32(&mut root, ROOT_CRC_OFFSET, 0);
    put_u32(&mut root, 76, 0);

    let mut page_offset = u64::try_from(pages_offset)
        .map_err(|_| invalid_symbols_input("symbols pages offset exceeds u64"))?;
    for (page_index, page) in pages.iter().enumerate() {
        let descriptor_offset = SYMBOLS_V3_HEADER_LEN
            .checked_add(
                page_index
                    .checked_mul(SYMBOLS_V3_PAGE_DESCRIPTOR_LEN)
                    .ok_or_else(|| invalid_symbols_input("symbols descriptor offset overflow"))?,
            )
            .ok_or_else(|| invalid_symbols_input("symbols descriptor offset overflow"))?;
        let (first_offset, first_len, last_offset, last_len) = fence_ranges[page_index];
        put_u32(&mut root, descriptor_offset, page.first_symbol_id);
        put_u32(&mut root, descriptor_offset + 4, page.symbol_count);
        put_u64(&mut root, descriptor_offset + 8, page_offset);
        put_u32(
            &mut root,
            descriptor_offset + 16,
            u32::try_from(page.bytes.len())
                .map_err(|_| invalid_symbols_input("symbols page length exceeds u32"))?,
        );
        put_u32(&mut root, descriptor_offset + 20, page.crc32c);
        put_u32(&mut root, descriptor_offset + 24, first_offset);
        put_u32(&mut root, descriptor_offset + 28, first_len);
        put_u32(&mut root, descriptor_offset + 32, last_offset);
        put_u32(&mut root, descriptor_offset + 36, last_len);
        put_u32(&mut root, descriptor_offset + 40, page.string_bytes_len);
        put_u32(&mut root, descriptor_offset + 44, 0);
        page_offset = page_offset
            .checked_add(page.bytes.len() as u64)
            .ok_or_else(|| invalid_symbols_input("symbols page offset overflow"))?;
    }
    root[fence_offset..pages_offset].copy_from_slice(&fences);
    let root_crc = symbols_root_crc(&root);
    put_u32(&mut root, ROOT_CRC_OFFSET, root_crc);
    writer.write_all(&root)?;
    for page in pages {
        writer.write_all(&page.bytes)?;
    }
    Ok(())
}

fn encode_pages(values: &[Vec<u8>], max_page_bytes: usize) -> io::Result<Vec<EncodedSymbolPage>> {
    let mut pages = Vec::new();
    let mut start = 0usize;
    while start < values.len() {
        let mut end = start;
        let mut string_bytes_len = 0usize;
        while end < values.len() {
            let candidate_strings_len = string_bytes_len
                .checked_add(values[end].len())
                .ok_or_else(|| invalid_symbols_input("symbols page string length overflow"))?;
            let candidate_count = end - start + 1;
            let candidate_len = encoded_page_len(candidate_count, candidate_strings_len)?;
            if candidate_count > 1 && candidate_len > SYMBOLS_V3_PAGE_TARGET_BYTES {
                break;
            }
            string_bytes_len = candidate_strings_len;
            end += 1;
            if candidate_len > SYMBOLS_V3_PAGE_TARGET_BYTES {
                break;
            }
        }
        if end == start {
            return Err(invalid_symbols_input("symbols page made no progress"));
        }
        pages.push(encode_page(
            u32::try_from(pages.len())
                .map_err(|_| invalid_symbols_input("symbols page index exceeds u32"))?,
            start,
            &values[start..end],
            max_page_bytes,
        )?);
        start = end;
    }
    Ok(pages)
}

fn encode_page(
    page_index: u32,
    first_symbol_id: usize,
    values: &[Vec<u8>],
    max_page_bytes: usize,
) -> io::Result<EncodedSymbolPage> {
    let symbol_count = u32::try_from(values.len())
        .map_err(|_| invalid_symbols_input("symbols page count exceeds u32"))?;
    let first_symbol_id = u32::try_from(first_symbol_id)
        .map_err(|_| invalid_symbols_input("first symbol id exceeds u32"))?;
    let string_bytes_len = values.iter().try_fold(0usize, |total, value| {
        total
            .checked_add(value.len())
            .ok_or_else(|| invalid_symbols_input("symbols page string length overflow"))
    })?;
    let page_len = encoded_page_len(values.len(), string_bytes_len)?;
    if page_len > max_page_bytes {
        return Err(invalid_symbols_input(
            "symbols page exceeds the operational size limit",
        ));
    }
    let offsets_len = values
        .len()
        .checked_add(1)
        .and_then(|count| count.checked_mul(4))
        .ok_or_else(|| invalid_symbols_input("symbols page offsets length overflow"))?;
    let mut bytes = vec![0u8; page_len];
    put_u32(&mut bytes, 0, SYMBOLS_V3_PAGE_MAGIC);
    put_u16(&mut bytes, 4, SYMBOLS_V3_PAGE_VERSION);
    put_u16(&mut bytes, 6, 0);
    put_u32(&mut bytes, 8, page_index);
    put_u32(&mut bytes, 12, first_symbol_id);
    put_u32(&mut bytes, 16, symbol_count);
    put_u32(
        &mut bytes,
        20,
        u32::try_from(offsets_len)
            .map_err(|_| invalid_symbols_input("symbols page offsets exceed u32"))?,
    );
    put_u32(
        &mut bytes,
        24,
        u32::try_from(string_bytes_len)
            .map_err(|_| invalid_symbols_input("symbols page strings exceed u32"))?,
    );
    put_u32(&mut bytes, 28, 0);

    let strings_offset = SYMBOLS_V3_PAGE_HEADER_LEN + offsets_len;
    let mut string_cursor = 0usize;
    put_u32(&mut bytes, SYMBOLS_V3_PAGE_HEADER_LEN, 0);
    for (index, value) in values.iter().enumerate() {
        let destination_start = strings_offset + string_cursor;
        let destination_end = destination_start + value.len();
        bytes[destination_start..destination_end].copy_from_slice(value);
        string_cursor += value.len();
        put_u32(
            &mut bytes,
            SYMBOLS_V3_PAGE_HEADER_LEN + (index + 1) * 4,
            u32::try_from(string_cursor)
                .map_err(|_| invalid_symbols_input("symbols page offset exceeds u32"))?,
        );
    }
    let crc32c = crc32c(&bytes);
    Ok(EncodedSymbolPage {
        first_symbol_id,
        symbol_count,
        string_bytes_len: u32::try_from(string_bytes_len)
            .map_err(|_| invalid_symbols_input("symbols page strings exceed u32"))?,
        first_fence: values.first().cloned().unwrap_or_default(),
        last_fence: values.last().cloned().unwrap_or_default(),
        bytes,
        crc32c,
    })
}

fn encoded_page_len(symbol_count: usize, string_bytes_len: usize) -> io::Result<usize> {
    SYMBOLS_V3_PAGE_HEADER_LEN
        .checked_add(
            symbol_count
                .checked_add(1)
                .and_then(|count| count.checked_mul(4))
                .ok_or_else(|| invalid_symbols_input("symbols page offsets length overflow"))?,
        )
        .and_then(|length| length.checked_add(string_bytes_len))
        .ok_or_else(|| invalid_symbols_input("symbols page length overflow"))
}

fn validate_sorted_values(values: &[Vec<u8>]) -> io::Result<()> {
    for pair in values.windows(2) {
        if pair[0] >= pair[1] {
            return Err(invalid_symbols_input(
                "symbols must be sorted by unique UTF-8 bytes",
            ));
        }
    }
    Ok(())
}
