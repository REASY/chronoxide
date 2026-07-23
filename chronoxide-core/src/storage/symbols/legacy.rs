use std::io;

use super::format::{
    SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB, SYMBOLS_V2_VERSION_FOR_LAYOUT_AB, SYMBOLS_V3_MAGIC,
    invalid_symbols_data, read_u16_at, read_u32_at, read_u64_at,
};
use super::reader::{SegmentSymbolReadAt, SegmentSymbolReadCounters, read_exact_at_counted};

#[derive(Debug)]
pub(super) struct LegacySymbolDictionary {
    pub(super) source_file_bytes: u64,
    offsets: Box<[usize]>,
    strings: Box<str>,
}

impl LegacySymbolDictionary {
    pub(super) fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub(super) fn symbol(&self, symbol_id: usize) -> Option<&str> {
        let start = *self.offsets.get(symbol_id)?;
        let end = *self.offsets.get(symbol_id.checked_add(1)?)?;
        self.strings.get(start..end)
    }

    pub(super) fn lookup(&self, target: &[u8]) -> Option<u32> {
        let mut low = 0usize;
        let mut high = self.len();
        while low < high {
            let mid = low + (high - low) / 2;
            match self.symbol(mid)?.as_bytes().cmp(target) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return u32::try_from(mid).ok(),
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        None
    }

    pub(super) fn retained_charge_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.offsets
                    .len()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add(self.strings.len())
    }
}

pub(super) fn read_legacy_v2_dictionary(
    source: &impl SegmentSymbolReadAt,
    counters: &SegmentSymbolReadCounters,
) -> io::Result<LegacySymbolDictionary> {
    let source_file_bytes = source.len()?;
    let file_len = usize::try_from(source_file_bytes)
        .map_err(|_| invalid_symbols_data("legacy v2 symbols length exceeds platform usize"))?;
    if file_len < SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "legacy v2 symbols file is shorter than its header",
        ));
    }

    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(file_len)
        .map_err(|_| io::Error::other("legacy v2 symbols allocation is too large"))?;
    bytes.resize(file_len, 0);
    read_exact_at_counted(source, &counters.legacy_eager, 0, &mut bytes)?;

    if read_u32_at(&bytes, 0) != SYMBOLS_V3_MAGIC {
        return Err(invalid_symbols_data("symbols magic mismatch"));
    }
    if read_u16_at(&bytes, 4) != SYMBOLS_V2_VERSION_FOR_LAYOUT_AB {
        return Err(invalid_symbols_data("unsupported symbols version"));
    }
    if read_u16_at(&bytes, 6) != 0 {
        return Err(invalid_symbols_data("legacy v2 symbols flags are non-zero"));
    }
    let symbol_count = usize::try_from(read_u32_at(&bytes, 8))
        .map_err(|_| invalid_symbols_data("legacy v2 symbol count exceeds platform usize"))?;
    let offset_count = symbol_count
        .checked_add(1)
        .ok_or_else(|| invalid_symbols_data("legacy v2 offset count overflow"))?;
    let offsets_bytes = offset_count
        .checked_mul(std::mem::size_of::<u64>())
        .ok_or_else(|| invalid_symbols_data("legacy v2 offset table length overflow"))?;
    let strings_start = SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB
        .checked_add(offsets_bytes)
        .ok_or_else(|| invalid_symbols_data("legacy v2 string section offset overflow"))?;
    if strings_start > bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "legacy v2 symbols offset table is truncated",
        ));
    }

    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(offset_count)
        .map_err(|_| io::Error::other("legacy v2 offset allocation is too large"))?;
    for offset_index in 0..offset_count {
        let byte_offset = SYMBOLS_V2_HEADER_LEN_FOR_LAYOUT_AB
            .checked_add(offset_index.saturating_mul(std::mem::size_of::<u64>()))
            .ok_or_else(|| invalid_symbols_data("legacy v2 offset position overflow"))?;
        offsets.push(
            usize::try_from(read_u64_at(&bytes, byte_offset)).map_err(|_| {
                invalid_symbols_data("legacy v2 symbol offset exceeds platform usize")
            })?,
        );
    }
    if offsets.first().copied() != Some(0) {
        return Err(invalid_symbols_data(
            "legacy v2 symbols first offset must be zero",
        ));
    }
    let strings_len = bytes.len() - strings_start;
    if offsets.last().copied() != Some(strings_len) {
        return Err(invalid_symbols_data(
            "legacy v2 symbols final offset must match file length",
        ));
    }

    bytes.drain(..strings_start);
    let strings = String::from_utf8(bytes)
        .map_err(|_| invalid_symbols_data("legacy v2 symbols are not valid UTF-8"))?;
    let mut previous: Option<&[u8]> = None;
    for pair in offsets.windows(2) {
        let value = strings.get(pair[0]..pair[1]).ok_or_else(|| {
            invalid_symbols_data("legacy v2 symbol offsets are out of order or out of bounds")
        })?;
        if previous.is_some_and(|previous| previous >= value.as_bytes()) {
            return Err(invalid_symbols_data(
                "legacy v2 symbols are not strictly sorted and unique",
            ));
        }
        previous = Some(value.as_bytes());
    }

    Ok(LegacySymbolDictionary {
        source_file_bytes,
        offsets: offsets.into_boxed_slice(),
        strings: strings.into_boxed_str(),
    })
}
