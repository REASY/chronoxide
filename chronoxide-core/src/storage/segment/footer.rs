use super::*;
use crate::storage::file_manager::open_immutable;

pub(super) const SEGMENT_FOOTER_HASH_BUFFER_BYTES: usize = 1024 * 1024;

pub(super) fn encode_segment_footer(footer: &SegmentFooter) -> io::Result<Vec<u8>> {
    let file_count = u16::try_from(footer.files.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment footer file count exceeds u16",
        )
    })?;
    let mut payload = Vec::new();
    payload.extend_from_slice(&file_count.to_le_bytes());
    payload.extend_from_slice(&0u16.to_le_bytes());

    for file in &footer.files {
        let file_id = segment_footer_file_id(file.file)?;
        payload.extend_from_slice(&file_id.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&file.size.to_le_bytes());
        payload.extend_from_slice(&file.checksum_xxh64.to_le_bytes());
    }

    let payload_len = u64::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment footer payload length exceeds u64",
        )
    })?;
    let mut header = [0u8; SEGMENT_FOOTER_HEADER_LEN];
    header[0..4].copy_from_slice(&SEGMENT_FOOTER_MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&SEGMENT_FOOTER_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&footer.schema_version.to_le_bytes());
    header[8..16].copy_from_slice(&payload_len.to_le_bytes());

    let mut out =
        Vec::with_capacity(SEGMENT_FOOTER_HEADER_LEN + payload.len() + SEGMENT_FOOTER_TRAILER_LEN);
    out.extend_from_slice(&header);
    out.extend_from_slice(&payload);
    out.extend_from_slice(&segment_footer_crc(&header, &payload).to_le_bytes());
    Ok(out)
}

#[cfg(test)]
pub(super) fn write_segment_footer_for_schema6(segment_dir: impl AsRef<Path>) -> io::Result<()> {
    write_segment_footer_for_schema(segment_dir, SEGMENT_SCHEMA_VERSION_V6)
}

pub(super) fn write_segment_footer_for_schema(
    segment_dir: impl AsRef<Path>,
    schema_version: u16,
) -> io::Result<()> {
    let segment_dir = segment_dir.as_ref();
    let footer = build_segment_footer_for_schema(segment_dir, schema_version)?;
    fs::write(
        segment_dir.join(SegmentFile::Footer.filename()),
        encode_segment_footer(&footer)?,
    )
}

pub(super) fn read_segment_footer_for_schema6(
    segment_dir: impl AsRef<Path>,
) -> io::Result<SegmentFooter> {
    read_segment_footer_for_exact_schema(segment_dir.as_ref(), SEGMENT_SCHEMA_VERSION_V6)
}

pub(super) fn read_segment_footer_for_schema7(
    segment_dir: impl AsRef<Path>,
) -> io::Result<SegmentFooter> {
    read_segment_footer_for_exact_schema(segment_dir.as_ref(), SEGMENT_SCHEMA_VERSION_V7)
}

pub(super) fn read_segment_footer_for_schema8(
    segment_dir: impl AsRef<Path>,
) -> io::Result<SegmentFooter> {
    read_segment_footer_for_exact_schema(segment_dir.as_ref(), SEGMENT_SCHEMA_VERSION_V8)
}

pub(super) fn read_segment_footer_for_exact_schema(
    segment_dir: &Path,
    expected_schema_version: u16,
) -> io::Result<SegmentFooter> {
    let path = segment_dir.join(SegmentFile::Footer.filename());
    let mut source = open_immutable(&path)?;
    validate_exact_footer_metadata(&source.metadata()?)?;

    let mut bytes = [0u8; SEGMENT_FOOTER_ENCODED_LEN];
    source
        .read_exact(&mut bytes)
        .map_err(normalize_footer_short_read)?;
    let mut trailing = [0u8; 1];
    if source.read(&mut trailing)? != 0 {
        return Err(noncanonical_footer_length());
    }
    validate_exact_footer_metadata(&source.metadata()?)?;
    decode_segment_footer_for_exact_schema(&bytes, expected_schema_version)
}

fn validate_exact_footer_metadata(metadata: &fs::Metadata) -> io::Result<()> {
    if !metadata.file_type().is_file() {
        return Err(invalid_segment_data("segment footer is not a regular file"));
    }
    if metadata.len() != SEGMENT_FOOTER_ENCODED_LEN as u64 {
        return Err(noncanonical_footer_length());
    }
    Ok(())
}

fn normalize_footer_short_read(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        noncanonical_footer_length()
    } else {
        error
    }
}

fn noncanonical_footer_length() -> io::Error {
    invalid_segment_data("segment footer length is not canonical")
}

#[cfg(test)]
pub(super) fn validate_segment_footer_for_schema6(segment_dir: impl AsRef<Path>) -> io::Result<()> {
    validate_segment_footer_with_policy(segment_dir.as_ref(), false, SEGMENT_SCHEMA_VERSION_V6)
}

#[cfg(test)]
pub(super) fn validate_segment_footer_for_schema7(segment_dir: impl AsRef<Path>) -> io::Result<()> {
    validate_segment_footer_with_policy(segment_dir.as_ref(), false, SEGMENT_SCHEMA_VERSION_V7)
}

#[cfg(test)]
pub(super) fn validate_segment_footer_for_schema8(segment_dir: impl AsRef<Path>) -> io::Result<()> {
    validate_segment_footer_with_policy(segment_dir.as_ref(), false, SEGMENT_SCHEMA_VERSION_V8)
}

#[cfg(test)]
fn validate_segment_footer_with_policy(
    segment_dir: &Path,
    allow_legacy_schema5_for_layout_ab: bool,
    expected_schema_version: u16,
) -> io::Result<()> {
    let footer = if allow_legacy_schema5_for_layout_ab {
        let bytes = fs::read(segment_dir.join(SegmentFile::Footer.filename()))?;
        decode_segment_footer_with_policy(&bytes, true, expected_schema_version)?
    } else {
        read_segment_footer_for_exact_schema(segment_dir, expected_schema_version)?
    };
    let mut seen = Vec::with_capacity(footer.files.len());

    for expected in &footer.files {
        if seen.contains(&expected.file) {
            return Err(invalid_segment_data("duplicate segment footer file entry"));
        }
        seen.push(expected.file);

        let actual = segment_footer_file(segment_dir, expected.file)?;
        if actual.size != expected.size || actual.checksum_xxh64 != expected.checksum_xxh64 {
            return Err(invalid_segment_data(
                "segment footer file size or checksum mismatch",
            ));
        }
    }

    for expected in SEGMENT_FOOTER_TRACKED_FILES {
        if !seen.contains(&expected) {
            return Err(invalid_segment_data("segment footer missing tracked file"));
        }
    }

    Ok(())
}

#[cfg(test)]
pub(super) fn build_segment_footer_for_schema6(segment_dir: &Path) -> io::Result<SegmentFooter> {
    build_segment_footer_for_schema(segment_dir, SEGMENT_SCHEMA_VERSION_V6)
}

pub(super) fn build_segment_footer_for_schema(
    segment_dir: &Path,
    schema_version: u16,
) -> io::Result<SegmentFooter> {
    if !matches!(
        schema_version,
        SEGMENT_SCHEMA_VERSION_V6 | SEGMENT_SCHEMA_VERSION_V7 | SEGMENT_SCHEMA_VERSION_V8
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported segment footer schema version",
        ));
    }
    let mut files = Vec::with_capacity(SEGMENT_FOOTER_TRACKED_FILES.len());
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        files.push(segment_footer_file(segment_dir, file)?);
    }
    Ok(SegmentFooter {
        schema_version,
        files,
    })
}

pub(super) fn segment_footer_file(
    segment_dir: &Path,
    file: SegmentFile,
) -> io::Result<SegmentFooterFile> {
    let mut source = File::open(segment_dir.join(file.filename()))?;
    let metadata = source.metadata()?;
    if !metadata.is_file() {
        return Err(invalid_segment_data(
            "segment footer tracks a non-regular file",
        ));
    }
    let size = metadata.len();
    let mut read_size = 0u64;
    let mut hash = XxHash64::default();
    let mut buffer = vec![0u8; SEGMENT_FOOTER_HASH_BUFFER_BYTES];
    loop {
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        read_size =
            read_size
                .checked_add(u64::try_from(count).map_err(|_| {
                    invalid_segment_data("segment footer hash read length exceeds u64")
                })?)
                .ok_or_else(|| invalid_segment_data("segment footer hash length overflow"))?;
        hash.update(&buffer[..count]);
    }
    if read_size != size {
        return Err(invalid_segment_data(
            "segment file length changed while computing footer checksum",
        ));
    }
    Ok(SegmentFooterFile {
        file,
        size,
        checksum_xxh64: hash.finish(),
    })
}

pub(super) fn decode_segment_footer_for_schema6(bytes: &[u8]) -> io::Result<SegmentFooter> {
    decode_segment_footer_with_policy(bytes, false, SEGMENT_SCHEMA_VERSION_V6)
}

#[cfg(test)]
pub(super) fn decode_segment_footer_for_layout_ab(bytes: &[u8]) -> io::Result<SegmentFooter> {
    decode_segment_footer_with_policy(bytes, true, SEGMENT_SCHEMA_VERSION_V6)
}

pub(super) fn decode_segment_footer_for_schema7(bytes: &[u8]) -> io::Result<SegmentFooter> {
    decode_segment_footer_with_policy(bytes, false, SEGMENT_SCHEMA_VERSION_V7)
}

pub(super) fn decode_segment_footer_for_schema8(bytes: &[u8]) -> io::Result<SegmentFooter> {
    decode_segment_footer_with_policy(bytes, false, SEGMENT_SCHEMA_VERSION_V8)
}

pub(super) fn decode_segment_footer_for_exact_schema(
    bytes: &[u8],
    expected_schema_version: u16,
) -> io::Result<SegmentFooter> {
    match expected_schema_version {
        SEGMENT_SCHEMA_VERSION_V6 => decode_segment_footer_for_schema6(bytes),
        SEGMENT_SCHEMA_VERSION_V7 => decode_segment_footer_for_schema7(bytes),
        SEGMENT_SCHEMA_VERSION_V8 => decode_segment_footer_for_schema8(bytes),
        _ => Err(invalid_segment_data(
            "unsupported segment footer schema version",
        )),
    }
}

fn decode_segment_footer_with_policy(
    bytes: &[u8],
    allow_legacy_schema5_for_layout_ab: bool,
    expected_schema_version: u16,
) -> io::Result<SegmentFooter> {
    if bytes.len() < SEGMENT_FOOTER_HEADER_LEN + SEGMENT_FOOTER_TRAILER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment footer truncated",
        ));
    }
    let header: [u8; SEGMENT_FOOTER_HEADER_LEN] =
        bytes[0..SEGMENT_FOOTER_HEADER_LEN].try_into().unwrap();

    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != SEGMENT_FOOTER_MAGIC {
        return Err(invalid_segment_data("invalid segment footer magic"));
    }
    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != SEGMENT_FOOTER_VERSION {
        return Err(invalid_segment_data("unsupported segment footer version"));
    }
    let schema_version = u16::from_le_bytes(header[6..8].try_into().unwrap());
    if schema_version != expected_schema_version
        && !(allow_legacy_schema5_for_layout_ab
            && expected_schema_version == SEGMENT_SCHEMA_VERSION_V6
            && schema_version == LEGACY_SEGMENT_SCHEMA_VERSION_FOR_LAYOUT_AB)
    {
        return Err(invalid_segment_data(
            "unsupported segment footer schema version",
        ));
    }
    let payload_len = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let payload_len = usize::try_from(payload_len).map_err(|_| {
        invalid_segment_data("segment footer payload length exceeds platform usize")
    })?;
    let expected_len = SEGMENT_FOOTER_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(SEGMENT_FOOTER_TRAILER_LEN))
        .ok_or_else(|| invalid_segment_data("segment footer length overflow"))?;
    if bytes.len() < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment footer truncated",
        ));
    }
    if bytes.len() != expected_len {
        return Err(invalid_segment_data("segment footer has trailing bytes"));
    }

    let payload = &bytes[SEGMENT_FOOTER_HEADER_LEN..SEGMENT_FOOTER_HEADER_LEN + payload_len];
    let expected_crc = u32::from_le_bytes(
        bytes[SEGMENT_FOOTER_HEADER_LEN + payload_len..][..SEGMENT_FOOTER_TRAILER_LEN]
            .try_into()
            .unwrap(),
    );
    let actual_crc = segment_footer_crc(&header, payload);
    if expected_crc != actual_crc {
        return Err(invalid_segment_data("segment footer checksum mismatch"));
    }

    let mut cursor = 0usize;
    let file_count = footer_read_u16(payload, &mut cursor)? as usize;
    if file_count != SEGMENT_FOOTER_TRACKED_FILES.len() {
        return Err(invalid_segment_data(
            "segment footer tracked-file count is not canonical",
        ));
    }
    let reserved = footer_read_u16(payload, &mut cursor)?;
    if reserved != 0 {
        return Err(invalid_segment_data(
            "segment footer payload reserved field is non-zero",
        ));
    }
    let mut files = Vec::with_capacity(file_count);
    for expected_file in SEGMENT_FOOTER_TRACKED_FILES {
        let file_id = footer_read_u16(payload, &mut cursor)?;
        let reserved = footer_read_u16(payload, &mut cursor)?;
        if reserved != 0 {
            return Err(invalid_segment_data(
                "segment footer file-entry reserved field is non-zero",
            ));
        }
        let file = segment_file_from_footer_id(file_id)?;
        if file != expected_file {
            return Err(invalid_segment_data(
                "segment footer tracked-file inventory is not canonical",
            ));
        }
        let size = footer_read_u64(payload, &mut cursor)?;
        let checksum_xxh64 = footer_read_u64(payload, &mut cursor)?;
        files.push(SegmentFooterFile {
            file,
            size,
            checksum_xxh64,
        });
    }
    if cursor != payload.len() {
        return Err(invalid_segment_data(
            "segment footer payload has trailing bytes",
        ));
    }

    Ok(SegmentFooter {
        schema_version,
        files,
    })
}

pub(super) fn segment_footer_file_id(file: SegmentFile) -> io::Result<u16> {
    match file {
        SegmentFile::MetaJson => Ok(1),
        SegmentFile::Symbols => Ok(2),
        SegmentFile::Series => Ok(3),
        SegmentFile::Chunks => Ok(4),
        SegmentFile::OooChunks => Ok(5),
        SegmentFile::ChunkIndex => Ok(6),
        SegmentFile::Indexes => Ok(7),
        SegmentFile::Footer => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "segment footer cannot describe itself",
        )),
    }
}

pub(super) fn segment_file_from_footer_id(file_id: u16) -> io::Result<SegmentFile> {
    match file_id {
        1 => Ok(SegmentFile::MetaJson),
        2 => Ok(SegmentFile::Symbols),
        3 => Ok(SegmentFile::Series),
        4 => Ok(SegmentFile::Chunks),
        5 => Ok(SegmentFile::OooChunks),
        6 => Ok(SegmentFile::ChunkIndex),
        7 => Ok(SegmentFile::Indexes),
        _ => Err(invalid_segment_data("unknown segment footer file id")),
    }
}

pub(super) fn segment_footer_crc(header: &[u8; SEGMENT_FOOTER_HEADER_LEN], payload: &[u8]) -> u32 {
    crc32c_append(crc32c(header), payload)
}

pub(super) fn invalid_segment_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) fn footer_read_bytes<'a>(
    buf: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "segment footer truncated"))?;
    if end > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment footer truncated",
        ));
    }
    let bytes = &buf[*cursor..end];
    *cursor = end;
    Ok(bytes)
}

pub(super) fn footer_read_array<const N: usize>(
    buf: &[u8],
    cursor: &mut usize,
) -> io::Result<[u8; N]> {
    let bytes = footer_read_bytes(buf, cursor, N)?;
    Ok(bytes.try_into().unwrap())
}

pub(super) fn footer_read_u16(buf: &[u8], cursor: &mut usize) -> io::Result<u16> {
    Ok(u16::from_le_bytes(footer_read_array(buf, cursor)?))
}

pub(super) fn footer_read_u64(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(footer_read_array(buf, cursor)?))
}

pub(super) fn sort_segment_readers(segments: &mut [SegmentReader]) {
    segments.sort_by(|left, right| {
        left.meta
            .start_ms
            .cmp(&right.meta.start_ms)
            .then_with(|| left.meta.end_ms.cmp(&right.meta.end_ms))
            .then_with(|| left.meta.segment_id.cmp(&right.meta.segment_id))
    });
}

pub(super) fn validate_manifest_segment_meta(
    manifest_segment: &ManifestSegment,
    meta: &SegmentMeta,
) -> io::Result<()> {
    if meta.segment_id != manifest_segment.segment_id
        || meta.start_ms != manifest_segment.start_ms
        || meta.end_ms != manifest_segment.end_ms
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest segment does not match segment meta.json",
        ));
    }
    Ok(())
}

pub(super) fn append_segment_manifest_record(
    segments_dir: &Path,
    meta: &SegmentMeta,
) -> io::Result<()> {
    let manifest_dir = segments_dir.join("manifest");
    let current = read_current(&manifest_dir)?;
    let mut writer = match current {
        Some(file_name) => ManifestWriter::open_append(&manifest_dir, &file_name)?,
        None => ManifestWriter::create(&manifest_dir, 1)?,
    };
    writer.append(&ManifestRecord::SegmentSealed(ManifestSegment::new(
        meta.segment_id.clone(),
        meta.start_ms,
        meta.end_ms,
        None,
    )?))?;
    writer.sync_all()?;
    write_current(&manifest_dir, writer.file_name())
}
