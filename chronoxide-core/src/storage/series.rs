use std::collections::HashMap;
use std::io::{self, Read, Write};

const SYMBOLS_MAGIC: u32 = u32::from_le_bytes(*b"SYMB");
const SERIES_MAGIC: u32 = u32::from_le_bytes(*b"SERI");

pub const SERIES_KIND_FLOAT: u8 = 0b0000_0001;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SegmentSymbols {
    values: Vec<String>,
    by_value: HashMap<String, u32>,
}

impl SegmentSymbols {
    pub fn intern(&mut self, value: &str) -> u32 {
        if let Some(&id) = self.by_value.get(value) {
            return id;
        }

        let id = self.values.len() as u32;
        self.values.push(value.to_string());
        self.by_value.insert(value.to_string(), id);
        id
    }

    pub fn lookup(&self, value: &str) -> Option<u32> {
        self.by_value.get(value).copied()
    }

    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.values.get(id as usize).map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesEntry {
    pub series_id: u64,
    pub kind_mask: u8,
    pub labels: Vec<(u32, u32)>,
}

pub fn write_symbols_bin(mut writer: impl Write, symbols: &SegmentSymbols) -> io::Result<()> {
    let mut string_bytes = Vec::new();
    let mut offsets = Vec::with_capacity(symbols.values.len() + 1);
    offsets.push(0u64);
    for value in &symbols.values {
        string_bytes.extend_from_slice(value.as_bytes());
        offsets.push(string_bytes.len() as u64);
    }

    writer.write_all(&SYMBOLS_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(symbols.values.len() as u32).to_le_bytes())?;
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
    if version != 1 {
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
    if strings_start + strings_len > bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "symbols string section out of bounds",
        ));
    }

    let mut symbols = SegmentSymbols::default();
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
        symbols.intern(value);
    }
    Ok(symbols)
}

pub fn write_series_bin_v1(mut writer: impl Write, entries: &[SeriesEntry]) -> io::Result<()> {
    let num_series = entries.len() as u32;
    let header_len = 4 + 2 + 2 + 4;
    let offsets_len = (entries.len() + 1) * 8;
    let mut entry_bytes = Vec::new();
    let mut offsets = Vec::with_capacity(entries.len() + 1);
    let mut cursor = (header_len + offsets_len) as u64;

    for entry in entries {
        offsets.push(cursor);
        let before = entry_bytes.len();
        write_series_entry(&mut entry_bytes, entry)?;
        cursor = cursor.saturating_add((entry_bytes.len() - before) as u64);
    }
    offsets.push(cursor);

    writer.write_all(&SERIES_MAGIC.to_le_bytes())?;
    writer.write_all(&1u16.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&num_series.to_le_bytes())?;
    for offset in offsets {
        writer.write_all(&offset.to_le_bytes())?;
    }
    writer.write_all(&entry_bytes)?;
    Ok(())
}

pub fn read_series_bin_v1(mut reader: impl Read) -> io::Result<Vec<SeriesEntry>> {
    let bytes = read_all(&mut reader)?;
    let mut cursor = 0usize;
    let magic = read_u32(&bytes, &mut cursor)?;
    if magic != SERIES_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series magic mismatch",
        ));
    }
    let version = read_u16(&bytes, &mut cursor)?;
    if version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported series version",
        ));
    }
    let _flags = read_u16(&bytes, &mut cursor)?;
    let num_series = read_u32(&bytes, &mut cursor)? as usize;

    let expected_entries_start = 4 + 2 + 2 + 4 + ((num_series + 1) * 8);
    let mut offsets = Vec::with_capacity(num_series + 1);
    for _ in 0..=num_series {
        offsets.push(read_u64(&bytes, &mut cursor)? as usize);
    }
    if offsets.first().copied() != Some(expected_entries_start) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series offsets header invalid",
        ));
    }

    let mut entries = Vec::with_capacity(num_series);
    for pair in offsets.windows(2) {
        let start = pair[0];
        let end = pair[1];
        if end < start || end > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series offsets out of bounds",
            ));
        }
        let mut entry_cursor = start;
        let entry = read_series_entry(&bytes[..end], &mut entry_cursor)?;
        if entry_cursor != end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series entry has trailing bytes",
            ));
        }
        entries.push(entry);
    }
    Ok(entries)
}

fn write_series_entry(mut writer: impl Write, entry: &SeriesEntry) -> io::Result<()> {
    writer.write_all(&entry.series_id.to_le_bytes())?;
    writer.write_all(&[entry.kind_mask])?;
    writer.write_all(&[0])?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&0u32.to_le_bytes())?;
    writer.write_all(&(entry.labels.len() as u32).to_le_bytes())?;
    for (key, value) in &entry.labels {
        writer.write_all(&key.to_le_bytes())?;
        writer.write_all(&value.to_le_bytes())?;
    }
    Ok(())
}

fn read_series_entry(bytes: &[u8], cursor: &mut usize) -> io::Result<SeriesEntry> {
    let series_id = read_u64(bytes, cursor)?;
    let kind_mask = read_u8(bytes, cursor)?;
    let _reserved0 = read_u8(bytes, cursor)?;
    let _reserved1 = read_u16(bytes, cursor)?;
    let meta_len = read_u32(bytes, cursor)? as usize;
    let num_labels = read_u32(bytes, cursor)? as usize;

    if meta_len > 0 {
        skip(bytes, cursor, meta_len)?;
    }

    let mut labels = Vec::with_capacity(num_labels);
    for _ in 0..num_labels {
        let key = read_u32(bytes, cursor)?;
        let value = read_u32(bytes, cursor)?;
        labels.push((key, value));
    }

    Ok(SeriesEntry {
        series_id,
        kind_mask,
        labels,
    })
}

fn read_all(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn skip(bytes: &[u8], cursor: &mut usize, len: usize) -> io::Result<()> {
    if cursor.saturating_add(len) > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    *cursor += len;
    Ok(())
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
