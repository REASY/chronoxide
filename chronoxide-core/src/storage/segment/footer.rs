use super::*;

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

pub(super) fn write_segment_footer(segment_dir: impl AsRef<Path>) -> io::Result<()> {
    let segment_dir = segment_dir.as_ref();
    let footer = build_segment_footer(segment_dir)?;
    fs::write(
        segment_dir.join(SegmentFile::Footer.filename()),
        encode_segment_footer(&footer)?,
    )
}

pub(super) fn read_segment_footer(segment_dir: impl AsRef<Path>) -> io::Result<SegmentFooter> {
    let bytes = fs::read(segment_dir.as_ref().join(SegmentFile::Footer.filename()))?;
    decode_segment_footer(&bytes)
}

pub(super) fn validate_segment_footer(segment_dir: impl AsRef<Path>) -> io::Result<()> {
    let segment_dir = segment_dir.as_ref();
    let footer = read_segment_footer(segment_dir)?;
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

pub(super) fn build_segment_footer(segment_dir: &Path) -> io::Result<SegmentFooter> {
    let mut files = Vec::with_capacity(SEGMENT_FOOTER_TRACKED_FILES.len());
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        files.push(segment_footer_file(segment_dir, file)?);
    }
    Ok(SegmentFooter {
        schema_version: SEGMENT_SCHEMA_VERSION,
        files,
    })
}

pub(super) fn segment_footer_file(
    segment_dir: &Path,
    file: SegmentFile,
) -> io::Result<SegmentFooterFile> {
    let bytes = fs::read(segment_dir.join(file.filename()))?;
    let size = u64::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "segment file size exceeds u64"))?;
    Ok(SegmentFooterFile {
        file,
        size,
        checksum_xxh64: xxhash64(&bytes),
    })
}

pub(super) fn decode_segment_footer(bytes: &[u8]) -> io::Result<SegmentFooter> {
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
    if schema_version != SEGMENT_SCHEMA_VERSION {
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
    let _reserved = footer_read_u16(payload, &mut cursor)?;
    let mut files = Vec::with_capacity(file_count);
    for _ in 0..file_count {
        let file_id = footer_read_u16(payload, &mut cursor)?;
        let _reserved = footer_read_u16(payload, &mut cursor)?;
        let size = footer_read_u64(payload, &mut cursor)?;
        let checksum_xxh64 = footer_read_u64(payload, &mut cursor)?;
        files.push(SegmentFooterFile {
            file: segment_file_from_footer_id(file_id)?,
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

pub(super) fn xxhash64(input: &[u8]) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;
    const P3: u64 = 1_609_587_929_392_839_161;
    const P4: u64 = 9_650_029_242_287_828_579;
    const P5: u64 = 2_870_177_450_012_600_261;

    let mut cursor = 0usize;
    let mut h64;

    if input.len() >= 32 {
        let mut v1 = P1.wrapping_add(P2);
        let mut v2 = P2;
        let mut v3 = 0;
        let mut v4 = 0u64.wrapping_sub(P1);

        while cursor + 32 <= input.len() {
            v1 = xxh64_round(v1, xxh64_read_u64(input, cursor));
            cursor += 8;
            v2 = xxh64_round(v2, xxh64_read_u64(input, cursor));
            cursor += 8;
            v3 = xxh64_round(v3, xxh64_read_u64(input, cursor));
            cursor += 8;
            v4 = xxh64_round(v4, xxh64_read_u64(input, cursor));
            cursor += 8;
        }

        h64 = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h64 = xxh64_merge_round(h64, v1);
        h64 = xxh64_merge_round(h64, v2);
        h64 = xxh64_merge_round(h64, v3);
        h64 = xxh64_merge_round(h64, v4);
    } else {
        h64 = P5;
    }

    h64 = h64.wrapping_add(input.len() as u64);

    while cursor + 8 <= input.len() {
        let k1 = xxh64_round(0, xxh64_read_u64(input, cursor));
        h64 ^= k1;
        h64 = h64.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        cursor += 8;
    }

    if cursor + 4 <= input.len() {
        h64 ^= u64::from(xxh64_read_u32(input, cursor)).wrapping_mul(P1);
        h64 = h64.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        cursor += 4;
    }

    while cursor < input.len() {
        h64 ^= u64::from(input[cursor]).wrapping_mul(P5);
        h64 = h64.rotate_left(11).wrapping_mul(P1);
        cursor += 1;
    }

    h64 ^= h64 >> 33;
    h64 = h64.wrapping_mul(P2);
    h64 ^= h64 >> 29;
    h64 = h64.wrapping_mul(P3);
    h64 ^ (h64 >> 32)
}

pub(super) fn xxh64_round(acc: u64, input: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;

    acc.wrapping_add(input.wrapping_mul(P2))
        .rotate_left(31)
        .wrapping_mul(P1)
}

pub(super) fn xxh64_merge_round(acc: u64, value: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P4: u64 = 9_650_029_242_287_828_579;

    (acc ^ xxh64_round(0, value))
        .wrapping_mul(P1)
        .wrapping_add(P4)
}

pub(super) fn xxh64_read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + 8].try_into().unwrap())
}

pub(super) fn xxh64_read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + 4].try_into().unwrap())
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
