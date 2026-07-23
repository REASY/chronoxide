use std::io;

use crc32c::{crc32c, crc32c_append};

pub const SYMBOLS_V3_MAGIC: u32 = u32::from_le_bytes(*b"SYMB");
pub const SYMBOLS_V2_VERSION_FOR_LAYOUT_AB: u16 = 2;
pub const SYMBOLS_V3_VERSION: u16 = 3;
pub const SYMBOLS_V3_HEADER_LEN: usize = 80;
pub const SYMBOLS_V3_PAGE_DESCRIPTOR_LEN: usize = 48;
pub const SYMBOLS_V3_PAGE_TARGET_BYTES: usize = 32 * 1024;
pub const SYMBOLS_V3_PAGE_MAGIC: u32 = u32::from_le_bytes(*b"SYPG");
pub const SYMBOLS_V3_PAGE_VERSION: u16 = 1;
pub const SYMBOLS_V3_PAGE_HEADER_LEN: usize = 32;
pub const SYMBOLS_V3_MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_SYMBOL_PAGE_CACHE_MAX_BYTES: usize = 256 * 1024;
pub const SYMBOLS_V3_MAX_ROOT_BYTES: usize = 64 * 1024 * 1024;

pub(super) const ROOT_CRC_OFFSET: usize = 72;
const ROOT_CRC_LEN: usize = 4;
pub(super) const SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB: usize = 12;

#[derive(Debug, Clone)]
pub(super) struct SymbolPageDescriptor {
    pub(super) first_symbol_id: u32,
    pub(super) symbol_count: u32,
    pub(super) page_offset: u64,
    pub(super) page_len: u32,
    pub(super) page_crc32c: u32,
    pub(super) first_fence_offset: usize,
    pub(super) first_fence_len: usize,
    pub(super) last_fence_offset: usize,
    pub(super) last_fence_len: usize,
    pub(super) string_bytes_len: u32,
}

#[derive(Debug)]
pub(super) struct SymbolRoot {
    pub(super) symbol_count: u32,
    pub(super) source_file_bytes: u64,
    pub(super) encoded_bytes: usize,
    pub(super) descriptors: Box<[SymbolPageDescriptor]>,
    pub(super) fences: Box<[u8]>,
}

impl SymbolRoot {
    pub(super) fn first_fence(&self, descriptor: &SymbolPageDescriptor) -> &[u8] {
        &self.fences[descriptor.first_fence_offset
            ..descriptor.first_fence_offset + descriptor.first_fence_len]
    }

    pub(super) fn last_fence(&self, descriptor: &SymbolPageDescriptor) -> &[u8] {
        &self.fences
            [descriptor.last_fence_offset..descriptor.last_fence_offset + descriptor.last_fence_len]
    }

    pub(super) fn retained_charge_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.descriptors
                    .len()
                    .saturating_mul(std::mem::size_of::<SymbolPageDescriptor>()),
            )
            .saturating_add(self.fences.len())
    }
}

#[derive(Debug)]
pub(super) struct ValidatedSymbolPage {
    pub(super) first_symbol_id: u32,
    offsets: Box<[u32]>,
    strings: Box<str>,
}

impl ValidatedSymbolPage {
    pub(super) fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub(super) fn symbol(&self, local_id: usize) -> Option<&str> {
        let start = usize::try_from(*self.offsets.get(local_id)?).ok()?;
        let end = usize::try_from(*self.offsets.get(local_id + 1)?).ok()?;
        self.strings.get(start..end)
    }

    pub(super) fn charge_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.offsets
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(self.strings.len())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SymbolRootHeaderFacts {
    pub(super) file_len: u64,
    pub(super) symbol_count: u32,
    pub(super) page_count: u32,
    pub(super) fence_offset: u64,
    pub(super) pages_offset: u64,
    pub(super) root_len: usize,
}

pub(super) fn decode_symbol_root_header(
    header: &[u8],
    file_len: u64,
) -> io::Result<SymbolRootHeaderFacts> {
    if header.len() != SYMBOLS_V3_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "symbols v3 header length is not exact",
        ));
    }
    if read_u32_at(header, 0) != SYMBOLS_V3_MAGIC {
        return Err(invalid_symbols_data("symbols magic mismatch"));
    }
    if read_u16_at(header, 4) != SYMBOLS_V3_VERSION {
        return Err(invalid_symbols_data("unsupported symbols version"));
    }
    if read_u16_at(header, 6) != 0 {
        return Err(invalid_symbols_data("symbols flags are non-zero"));
    }
    if read_u32_at(header, 8) != SYMBOLS_V3_HEADER_LEN as u32 {
        return Err(invalid_symbols_data("symbols header length is invalid"));
    }
    if read_u32_at(header, 12) != SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u32 {
        return Err(invalid_symbols_data(
            "symbols page descriptor length is invalid",
        ));
    }
    if read_u32_at(header, 76) != 0 {
        return Err(invalid_symbols_data("symbols reserved field is non-zero"));
    }
    let symbol_count = read_u32_at(header, 16);
    let page_count = read_u32_at(header, 20);
    if (symbol_count == 0) != (page_count == 0) {
        return Err(invalid_symbols_data(
            "symbols and page counts disagree about emptiness",
        ));
    }
    if page_count > symbol_count {
        return Err(invalid_symbols_data(
            "symbols page count exceeds symbol count",
        ));
    }
    if read_u64_at(header, 24) != SYMBOLS_V3_HEADER_LEN as u64 {
        return Err(invalid_symbols_data("symbols directory offset is invalid"));
    }
    let expected_directory_len = u64::from(page_count)
        .checked_mul(SYMBOLS_V3_PAGE_DESCRIPTOR_LEN as u64)
        .ok_or_else(|| invalid_symbols_data("symbols directory length overflow"))?;
    if read_u64_at(header, 32) != expected_directory_len {
        return Err(invalid_symbols_data("symbols directory length is invalid"));
    }
    let expected_fence_offset = (SYMBOLS_V3_HEADER_LEN as u64)
        .checked_add(expected_directory_len)
        .ok_or_else(|| invalid_symbols_data("symbols fence offset overflow"))?;
    if read_u64_at(header, 40) != expected_fence_offset {
        return Err(invalid_symbols_data("symbols fence offset is invalid"));
    }
    let fence_len = read_u64_at(header, 48);
    let expected_pages_offset = expected_fence_offset
        .checked_add(fence_len)
        .ok_or_else(|| invalid_symbols_data("symbols pages offset overflow"))?;
    if read_u64_at(header, 56) != expected_pages_offset {
        return Err(invalid_symbols_data("symbols pages offset is invalid"));
    }
    if read_u64_at(header, 64) != file_len {
        return Err(invalid_symbols_data("symbols file length is invalid"));
    }
    if expected_pages_offset > file_len {
        return Err(invalid_symbols_data("symbols root exceeds the file"));
    }
    let root_len = usize::try_from(expected_pages_offset)
        .map_err(|_| invalid_symbols_data("symbols root length exceeds platform usize"))?;
    if root_len > SYMBOLS_V3_MAX_ROOT_BYTES {
        return Err(invalid_symbols_data(
            "symbols root exceeds the operational size limit",
        ));
    }
    Ok(SymbolRootHeaderFacts {
        file_len,
        symbol_count,
        page_count,
        fence_offset: expected_fence_offset,
        pages_offset: expected_pages_offset,
        root_len,
    })
}

pub(super) fn decode_symbol_root(
    root: &[u8],
    facts: SymbolRootHeaderFacts,
) -> io::Result<SymbolRoot> {
    if root.len() != facts.root_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "symbols root length is not exact",
        ));
    }
    if symbols_root_crc(root) != read_u32_at(root, ROOT_CRC_OFFSET) {
        return Err(invalid_symbols_data("symbols root CRC mismatch"));
    }

    let fence_start = usize::try_from(facts.fence_offset)
        .map_err(|_| invalid_symbols_data("symbols fence offset exceeds platform usize"))?;
    let fence_end = usize::try_from(facts.pages_offset)
        .map_err(|_| invalid_symbols_data("symbols pages offset exceeds platform usize"))?;
    let fences = root
        .get(fence_start..fence_end)
        .ok_or_else(|| invalid_symbols_data("symbols fence region is out of bounds"))?
        .to_vec()
        .into_boxed_slice();
    let page_count_usize = usize::try_from(facts.page_count)
        .map_err(|_| invalid_symbols_data("symbols page count exceeds platform usize"))?;
    let mut descriptors = Vec::new();
    descriptors
        .try_reserve_exact(page_count_usize)
        .map_err(|_| io::Error::other("symbols page descriptor allocation is too large"))?;
    let mut expected_symbol_id = 0u32;
    let mut expected_page_offset = facts.pages_offset;
    let mut expected_fence_offset = 0usize;
    let mut previous_last_fence: Option<Vec<u8>> = None;
    for page_index in 0..page_count_usize {
        let offset = SYMBOLS_V3_HEADER_LEN + page_index * SYMBOLS_V3_PAGE_DESCRIPTOR_LEN;
        let descriptor_bytes = root
            .get(offset..offset + SYMBOLS_V3_PAGE_DESCRIPTOR_LEN)
            .ok_or_else(|| invalid_symbols_data("symbols page descriptor is truncated"))?;
        let first_symbol_id = read_u32_at(descriptor_bytes, 0);
        let descriptor_symbol_count = read_u32_at(descriptor_bytes, 4);
        if descriptor_symbol_count == 0 {
            return Err(invalid_symbols_data(
                "symbols page descriptor has no symbols",
            ));
        }
        if first_symbol_id != expected_symbol_id {
            return Err(invalid_symbols_data(
                "symbols page symbol ids are not contiguous",
            ));
        }
        expected_symbol_id = expected_symbol_id
            .checked_add(descriptor_symbol_count)
            .ok_or_else(|| invalid_symbols_data("symbols page symbol count overflow"))?;
        let page_offset = read_u64_at(descriptor_bytes, 8);
        if page_offset != expected_page_offset {
            return Err(invalid_symbols_data(
                "symbols page byte ranges are not contiguous",
            ));
        }
        let page_len = read_u32_at(descriptor_bytes, 16);
        if u64::from(page_len) > SYMBOLS_V3_MAX_PAGE_BYTES as u64 {
            return Err(invalid_symbols_data(
                "symbols page exceeds the operational size limit",
            ));
        }
        let string_bytes_len = read_u32_at(descriptor_bytes, 40);
        let expected_page_len = u64::from(descriptor_symbol_count)
            .checked_add(1)
            .and_then(|count| count.checked_mul(4))
            .and_then(|offsets_len| offsets_len.checked_add(SYMBOLS_V3_PAGE_HEADER_LEN as u64))
            .and_then(|length| length.checked_add(u64::from(string_bytes_len)))
            .ok_or_else(|| invalid_symbols_data("symbols page length overflow"))?;
        if u64::from(page_len) != expected_page_len {
            return Err(invalid_symbols_data("symbols page length is inconsistent"));
        }
        if descriptor_symbol_count > 1
            && usize::try_from(page_len)
                .ok()
                .is_some_and(|length| length > SYMBOLS_V3_PAGE_TARGET_BYTES)
        {
            return Err(invalid_symbols_data(
                "multi-symbol page exceeds the v3 target",
            ));
        }
        expected_page_offset = expected_page_offset
            .checked_add(u64::from(page_len))
            .ok_or_else(|| invalid_symbols_data("symbols page end overflow"))?;
        if expected_page_offset > facts.file_len {
            return Err(invalid_symbols_data("symbols page exceeds the file"));
        }
        if read_u32_at(descriptor_bytes, 44) != 0 {
            return Err(invalid_symbols_data(
                "symbols page descriptor reserved field is non-zero",
            ));
        }
        let first_fence_offset = usize::try_from(read_u32_at(descriptor_bytes, 24))
            .map_err(|_| invalid_symbols_data("symbols first fence offset exceeds usize"))?;
        let first_fence_len = usize::try_from(read_u32_at(descriptor_bytes, 28))
            .map_err(|_| invalid_symbols_data("symbols first fence length exceeds usize"))?;
        let last_fence_offset = usize::try_from(read_u32_at(descriptor_bytes, 32))
            .map_err(|_| invalid_symbols_data("symbols last fence offset exceeds usize"))?;
        let last_fence_len = usize::try_from(read_u32_at(descriptor_bytes, 36))
            .map_err(|_| invalid_symbols_data("symbols last fence length exceeds usize"))?;
        if first_fence_offset != expected_fence_offset {
            return Err(invalid_symbols_data(
                "symbols first fence is not canonically positioned",
            ));
        }
        expected_fence_offset = expected_fence_offset
            .checked_add(first_fence_len)
            .ok_or_else(|| invalid_symbols_data("symbols first fence end overflow"))?;
        if last_fence_offset != expected_fence_offset {
            return Err(invalid_symbols_data(
                "symbols last fence is not canonically positioned",
            ));
        }
        expected_fence_offset = expected_fence_offset
            .checked_add(last_fence_len)
            .ok_or_else(|| invalid_symbols_data("symbols last fence end overflow"))?;
        let first_fence = checked_fence(&fences, first_fence_offset, first_fence_len)?;
        let last_fence = checked_fence(&fences, last_fence_offset, last_fence_len)?;
        if descriptor_symbol_count == 1 {
            if first_fence != last_fence {
                return Err(invalid_symbols_data("singleton symbols page fences differ"));
            }
            if u64::try_from(first_fence.len())
                .map_err(|_| invalid_symbols_data("symbols fence length exceeds u64"))?
                != u64::from(string_bytes_len)
            {
                return Err(invalid_symbols_data(
                    "singleton symbols page string length disagrees with its fence",
                ));
            }
        } else {
            if first_fence >= last_fence {
                return Err(invalid_symbols_data(
                    "multi-symbol page fences are not strictly ordered",
                ));
            }
            let fence_string_bytes = first_fence
                .len()
                .checked_add(last_fence.len())
                .ok_or_else(|| invalid_symbols_data("symbols fence byte length overflow"))?;
            let minimum_string_bytes = u64::try_from(fence_string_bytes)
                .map_err(|_| invalid_symbols_data("symbols fence byte length exceeds u64"))?
                .checked_add(u64::from(descriptor_symbol_count - 2))
                .ok_or_else(|| invalid_symbols_data("symbols minimum byte length overflow"))?;
            if minimum_string_bytes > u64::from(string_bytes_len) {
                return Err(invalid_symbols_data(
                    "symbols page count and fences exceed its string byte length",
                ));
            }
            if descriptor_symbol_count == 2 && minimum_string_bytes != u64::from(string_bytes_len) {
                return Err(invalid_symbols_data(
                    "two-symbol page string length disagrees with its fences",
                ));
            }
        }
        if previous_last_fence
            .as_deref()
            .is_some_and(|previous| previous >= first_fence)
        {
            return Err(invalid_symbols_data(
                "symbols page fences are not strictly ordered",
            ));
        }
        previous_last_fence = Some(last_fence.to_vec());
        descriptors.push(SymbolPageDescriptor {
            first_symbol_id,
            symbol_count: descriptor_symbol_count,
            page_offset,
            page_len,
            page_crc32c: read_u32_at(descriptor_bytes, 20),
            first_fence_offset,
            first_fence_len,
            last_fence_offset,
            last_fence_len,
            string_bytes_len,
        });
    }
    if expected_symbol_id != facts.symbol_count {
        return Err(invalid_symbols_data(
            "symbols descriptor counts do not match the header",
        ));
    }
    if expected_fence_offset != fences.len() {
        return Err(invalid_symbols_data(
            "symbols fence region has trailing bytes",
        ));
    }
    if expected_page_offset != facts.file_len {
        return Err(invalid_symbols_data("symbols file has trailing bytes"));
    }
    for pair in descriptors.windows(2) {
        let current = &pair[0];
        let next = &pair[1];
        if usize::try_from(current.page_len)
            .ok()
            .is_some_and(|length| length > SYMBOLS_V3_PAGE_TARGET_BYTES)
        {
            // The earlier size check proves an oversized page is a singleton.
            continue;
        }
        let candidate_count = u64::from(current.symbol_count)
            .checked_add(1)
            .ok_or_else(|| invalid_symbols_data("symbols greedy page count overflow"))?;
        let candidate_offsets_len = candidate_count
            .checked_add(1)
            .and_then(|count| count.checked_mul(4))
            .ok_or_else(|| invalid_symbols_data("symbols greedy offsets length overflow"))?;
        let candidate_strings_len = u64::from(current.string_bytes_len)
            .checked_add(
                u64::try_from(next.first_fence_len)
                    .map_err(|_| invalid_symbols_data("symbols next fence length exceeds u64"))?,
            )
            .ok_or_else(|| invalid_symbols_data("symbols greedy strings length overflow"))?;
        let candidate_page_len = (SYMBOLS_V3_PAGE_HEADER_LEN as u64)
            .checked_add(candidate_offsets_len)
            .and_then(|length| length.checked_add(candidate_strings_len))
            .ok_or_else(|| invalid_symbols_data("symbols greedy page length overflow"))?;
        if candidate_page_len <= SYMBOLS_V3_PAGE_TARGET_BYTES as u64 {
            return Err(invalid_symbols_data("symbols page is not greedily maximal"));
        }
    }
    Ok(SymbolRoot {
        symbol_count: facts.symbol_count,
        source_file_bytes: facts.file_len,
        encoded_bytes: facts.root_len,
        descriptors: descriptors.into_boxed_slice(),
        fences,
    })
}

fn checked_fence(fences: &[u8], offset: usize, len: usize) -> io::Result<&[u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid_symbols_data("symbols fence range overflow"))?;
    let fence = fences
        .get(offset..end)
        .ok_or_else(|| invalid_symbols_data("symbols fence is out of bounds"))?;
    std::str::from_utf8(fence)
        .map_err(|_| invalid_symbols_data("symbols fence is not valid UTF-8"))?;
    Ok(fence)
}

pub(super) fn validate_page(
    page_index: u32,
    descriptor: &SymbolPageDescriptor,
    first_fence: &[u8],
    last_fence: &[u8],
    bytes: Vec<u8>,
) -> io::Result<ValidatedSymbolPage> {
    if crc32c(&bytes) != descriptor.page_crc32c {
        return Err(invalid_symbols_data("symbols page CRC mismatch"));
    }
    if bytes.len() < SYMBOLS_V3_PAGE_HEADER_LEN {
        return Err(invalid_symbols_data("symbols page is truncated"));
    }
    if read_u32_at(&bytes, 0) != SYMBOLS_V3_PAGE_MAGIC {
        return Err(invalid_symbols_data("symbols page magic mismatch"));
    }
    if read_u16_at(&bytes, 4) != SYMBOLS_V3_PAGE_VERSION {
        return Err(invalid_symbols_data("symbols page version mismatch"));
    }
    if read_u16_at(&bytes, 6) != 0 {
        return Err(invalid_symbols_data("symbols page flags are non-zero"));
    }
    if read_u32_at(&bytes, 8) != page_index {
        return Err(invalid_symbols_data("symbols page index mismatch"));
    }
    if read_u32_at(&bytes, 12) != descriptor.first_symbol_id {
        return Err(invalid_symbols_data("symbols page first id mismatch"));
    }
    if read_u32_at(&bytes, 16) != descriptor.symbol_count {
        return Err(invalid_symbols_data("symbols page count mismatch"));
    }
    let expected_offsets_len = descriptor
        .symbol_count
        .checked_add(1)
        .and_then(|count| count.checked_mul(4))
        .ok_or_else(|| invalid_symbols_data("symbols page offsets length overflow"))?;
    if read_u32_at(&bytes, 20) != expected_offsets_len {
        return Err(invalid_symbols_data("symbols page offsets length mismatch"));
    }
    if read_u32_at(&bytes, 24) != descriptor.string_bytes_len {
        return Err(invalid_symbols_data("symbols page strings length mismatch"));
    }
    if read_u32_at(&bytes, 28) != 0 {
        return Err(invalid_symbols_data(
            "symbols page reserved field is non-zero",
        ));
    }
    let offsets_start = SYMBOLS_V3_PAGE_HEADER_LEN;
    let offsets_end = offsets_start
        .checked_add(
            usize::try_from(expected_offsets_len)
                .map_err(|_| invalid_symbols_data("symbols page offsets exceed usize"))?,
        )
        .ok_or_else(|| invalid_symbols_data("symbols page offsets end overflow"))?;
    let strings_len = usize::try_from(descriptor.string_bytes_len)
        .map_err(|_| invalid_symbols_data("symbols page strings exceed usize"))?;
    let expected_page_len = offsets_end
        .checked_add(strings_len)
        .ok_or_else(|| invalid_symbols_data("symbols page length overflow"))?;
    if expected_page_len != bytes.len() {
        return Err(invalid_symbols_data("symbols page length mismatch"));
    }
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(descriptor.symbol_count as usize + 1)
        .map_err(|_| io::Error::other("symbols page offsets allocation is too large"))?;
    for offset in (offsets_start..offsets_end).step_by(4) {
        offsets.push(read_u32_at(&bytes, offset));
    }
    if offsets.first().copied() != Some(0) {
        return Err(invalid_symbols_data(
            "symbols page first offset must be zero",
        ));
    }
    if offsets.last().copied() != Some(descriptor.string_bytes_len) {
        return Err(invalid_symbols_data(
            "symbols page final offset does not match strings",
        ));
    }
    let string_bytes = &bytes[offsets_end..];
    let mut previous: Option<&[u8]> = None;
    for pair in offsets.windows(2) {
        let start = usize::try_from(pair[0])
            .map_err(|_| invalid_symbols_data("symbols page offset exceeds usize"))?;
        let end = usize::try_from(pair[1])
            .map_err(|_| invalid_symbols_data("symbols page offset exceeds usize"))?;
        if end < start {
            return Err(invalid_symbols_data(
                "symbols page offsets are out of order",
            ));
        }
        let value = string_bytes
            .get(start..end)
            .ok_or_else(|| invalid_symbols_data("symbols page offset is out of bounds"))?;
        std::str::from_utf8(value)
            .map_err(|_| invalid_symbols_data("symbols page value is not valid UTF-8"))?;
        if previous.is_some_and(|previous| previous >= value) {
            return Err(invalid_symbols_data(
                "symbols page values are not strictly sorted and unique",
            ));
        }
        previous = Some(value);
    }
    let first_value = offsets
        .get(0..2)
        .and_then(|pair| string_bytes.get(pair[0] as usize..pair[1] as usize))
        .ok_or_else(|| invalid_symbols_data("symbols page first value is missing"))?;
    let last_pair = offsets
        .get(offsets.len().saturating_sub(2)..)
        .ok_or_else(|| invalid_symbols_data("symbols page last value is missing"))?;
    let last_value = string_bytes
        .get(last_pair[0] as usize..last_pair[1] as usize)
        .ok_or_else(|| invalid_symbols_data("symbols page last value is missing"))?;
    if first_value != first_fence || last_value != last_fence {
        return Err(invalid_symbols_data(
            "symbols page values do not match its fences",
        ));
    }
    let strings = String::from_utf8(string_bytes.to_vec())
        .map_err(|_| invalid_symbols_data("symbols page strings are not valid UTF-8"))?;
    Ok(ValidatedSymbolPage {
        first_symbol_id: descriptor.first_symbol_id,
        offsets: offsets.into_boxed_slice(),
        strings: strings.into_boxed_str(),
    })
}

pub(super) fn symbols_root_crc(root: &[u8]) -> u32 {
    let before = root.get(..ROOT_CRC_OFFSET).unwrap_or(root);
    let after = root
        .get(ROOT_CRC_OFFSET + ROOT_CRC_LEN..)
        .unwrap_or_default();
    crc32c_append(crc32c_append(crc32c(before), &[0; ROOT_CRC_LEN]), after)
}

pub(super) fn invalid_symbols_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(super) fn invalid_symbols_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

pub(super) fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

pub(super) fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

pub(super) fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
