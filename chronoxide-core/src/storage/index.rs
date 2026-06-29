use std::collections::BTreeMap;
use std::io::{self, Read, Write};

const EXACT_POSTINGS_MAGIC: u32 = u32::from_le_bytes(*b"PIDX");

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
