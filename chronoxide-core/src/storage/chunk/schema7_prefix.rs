use super::*;

const SCHEMA7_INDEXED_PREFIX_LEN: usize = CHUNK_HEADER_LEN;
const SCHEMA7_INDEXED_PREFIX_WITH_SCALAR_LEN: usize =
    CHUNK_HEADER_LEN + TYPED_SCALAR_LANE_HEADER_LEN;
const SCHEMA7_TYPED_CHUNK_FLAGS: u16 = CHUNK_FLAG_HAS_START_TIME
    | CHUNK_FLAG_HAS_PER_SAMPLE_FLAGS
    | CHUNK_FLAG_HAS_COUNTER_RESET_HINTS
    | CHUNK_FLAG_TEMPORALITY_DELTA;

/// Authenticated routing facts supplied by a schema-7 inline locator or overflow entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema7ChunkPrefixExpectation {
    pub(crate) series_ref: u32,
    pub(crate) kind: ChunkKind,
    pub(crate) min_time_ms: u64,
    pub(crate) max_time_ms: u64,
    pub(crate) length: u32,
    pub(crate) scalar_lane_offset: u32,
    pub(crate) scalar_lane_len: u32,
    pub(crate) indexed_prefix_crc32c: u32,
}

impl Schema7ChunkPrefixExpectation {
    /// Returns the exact raw prefix length that must be read before semantic decoding.
    pub(crate) fn indexed_prefix_len(&self) -> io::Result<usize> {
        let prefix_len = match (self.scalar_lane_offset, self.scalar_lane_len) {
            (0, 0) => Ok(SCHEMA7_INDEXED_PREFIX_LEN),
            (offset, len)
                if offset == CHUNK_HEADER_LEN as u32
                    && len >= TYPED_SCALAR_LANE_HEADER_LEN as u32 =>
            {
                Ok(SCHEMA7_INDEXED_PREFIX_WITH_SCALAR_LEN)
            }
            _ => Err(invalid_data(
                "schema-7 scalar lane locator is not canonical",
            )),
        }?;
        let minimum_length = (CHUNK_HEADER_LEN as u32)
            .checked_add(self.scalar_lane_len)
            .ok_or_else(|| invalid_data("schema-7 chunk header length overflows"))?;
        if self.length < minimum_length {
            return Err(invalid_data(
                "schema-7 locator is shorter than its indexed chunk header",
            ));
        }
        Ok(prefix_len)
    }
}

/// The authenticated scalar-lane header. The body is validated separately when touched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VerifiedSchema7ScalarLaneHeader {
    pub(crate) body_len: u32,
    pub(crate) body_crc32c: u32,
}

/// A semantically validated schema-7 indexed chunk prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VerifiedSchema7ChunkPrefix {
    pub(crate) kind: ChunkKind,
    pub(crate) encoding: ChunkEncoding,
    pub(crate) flags: u16,
    pub(crate) series_ref: u32,
    pub(crate) min_time_ms: u64,
    pub(crate) max_time_ms: u64,
    pub(crate) num_points: u32,
    pub(crate) header_len: u32,
    pub(crate) payload_len: u32,
    pub(crate) chunk_crc32c: u32,
    pub(crate) scalar_lane: Option<VerifiedSchema7ScalarLaneHeader>,
}

/// Verifies a schema-7 locator's exact raw indexed prefix before interpreting its headers.
pub(crate) fn verify_schema7_indexed_prefix(
    expectation: &Schema7ChunkPrefixExpectation,
    prefix: &[u8],
) -> io::Result<VerifiedSchema7ChunkPrefix> {
    // The locator determines the authenticated span. Validate only its canonical shape before
    // reading or interpreting any bytes from the chunk itself.
    let expected_prefix_len = expectation.indexed_prefix_len()?;
    if prefix.len() < expected_prefix_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "schema-7 indexed prefix short read",
        ));
    }
    if prefix.len() > expected_prefix_len {
        return Err(invalid_data("schema-7 indexed prefix has trailing bytes"));
    }
    if crc32c(prefix) != expectation.indexed_prefix_crc32c {
        return Err(invalid_data("schema-7 indexed prefix crc mismatch"));
    }

    // No header field is interpreted above this point. In particular, an unauthenticated unknown
    // kind or encoding cannot steer semantic decoding or select a different CRC span.
    let kind = chunk_kind_from_u8(prefix[0])?;
    let encoding = chunk_encoding_from_u8(prefix[1])?;
    validate_kind_encoding(kind, encoding)?;

    let flags = read_u16_at(prefix, 2);
    validate_chunk_flags(kind, flags)?;

    let series_ref = read_u32_at(prefix, 4);
    let min_time_ms = read_u64_at(prefix, 8);
    let max_time_ms = read_u64_at(prefix, 16);
    let num_points = read_u32_at(prefix, 24);
    let header_len = read_u32_at(prefix, 28);
    let payload_len = read_u32_at(prefix, 32);
    let chunk_crc32c = read_u32_at(prefix, 36);

    if num_points == 0 {
        return Err(invalid_data("schema-7 chunk has zero points"));
    }
    if min_time_ms > max_time_ms {
        return Err(invalid_data("schema-7 chunk time range is reversed"));
    }

    let expected_header_len = (CHUNK_HEADER_LEN as u32)
        .checked_add(expectation.scalar_lane_len)
        .ok_or_else(|| invalid_data("schema-7 chunk header length overflows"))?;
    if header_len != expected_header_len {
        return Err(invalid_data(
            "schema-7 scalar lane length does not match chunk header length",
        ));
    }
    let exact_length = header_len
        .checked_add(payload_len)
        .ok_or_else(|| invalid_data("schema-7 chunk length overflows"))?;
    if expectation.length != exact_length {
        return Err(invalid_data(
            "schema-7 locator length does not match exact chunk length",
        ));
    }

    if series_ref != expectation.series_ref {
        return Err(invalid_data(
            "schema-7 locator series does not match chunk header",
        ));
    }
    if kind != expectation.kind {
        return Err(invalid_data(
            "schema-7 locator kind does not match chunk header",
        ));
    }
    if min_time_ms != expectation.min_time_ms || max_time_ms != expectation.max_time_ms {
        return Err(invalid_data(
            "schema-7 locator time range does not match chunk header",
        ));
    }

    let scalar_lane = if expectation.scalar_lane_len == 0 {
        None
    } else {
        if !matches!(
            kind,
            ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
        ) || encoding != ChunkEncoding::SchemaVarLen
        {
            return Err(invalid_data(
                "schema-7 scalar lane requires a typed schema-varlen chunk",
            ));
        }

        let magic = read_u32_at(prefix, CHUNK_HEADER_LEN);
        if magic != TYPED_SCALAR_LANE_MAGIC {
            return Err(invalid_data("schema-7 typed scalar lane magic mismatch"));
        }
        let version = read_u16_at(prefix, CHUNK_HEADER_LEN + 4);
        if version != TYPED_SCALAR_LANE_VERSION {
            return Err(invalid_data(
                "schema-7 typed scalar lane version is unsupported",
            ));
        }
        let scalar_flags = read_u16_at(prefix, CHUNK_HEADER_LEN + 6);
        if scalar_flags != 0 {
            return Err(invalid_data(
                "schema-7 typed scalar lane flags must be zero",
            ));
        }
        let body_len = read_u32_at(prefix, CHUNK_HEADER_LEN + 8);
        let exact_scalar_lane_len = (TYPED_SCALAR_LANE_HEADER_LEN as u32)
            .checked_add(body_len)
            .ok_or_else(|| invalid_data("schema-7 typed scalar lane length overflows"))?;
        if expectation.scalar_lane_len != exact_scalar_lane_len {
            return Err(invalid_data(
                "schema-7 typed scalar lane body length does not match locator",
            ));
        }
        Some(VerifiedSchema7ScalarLaneHeader {
            body_len,
            body_crc32c: read_u32_at(prefix, CHUNK_HEADER_LEN + 12),
        })
    };

    Ok(VerifiedSchema7ChunkPrefix {
        kind,
        encoding,
        flags,
        series_ref,
        min_time_ms,
        max_time_ms,
        num_points,
        header_len,
        payload_len,
        chunk_crc32c,
        scalar_lane,
    })
}

fn validate_kind_encoding(kind: ChunkKind, encoding: ChunkEncoding) -> io::Result<()> {
    let valid = matches!(
        (kind, encoding),
        (
            ChunkKind::Float,
            ChunkEncoding::RawF64 | ChunkEncoding::Gorilla
        ) | (
            ChunkKind::Int64,
            ChunkEncoding::RawI64 | ChunkEncoding::IntDeltaZigZag
        ) | (
            ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary,
            ChunkEncoding::SchemaVarLen
        )
    );
    if !valid {
        return Err(invalid_data("schema-7 chunk kind/encoding pair is invalid"));
    }
    Ok(())
}

fn validate_chunk_flags(kind: ChunkKind, flags: u16) -> io::Result<()> {
    match kind {
        ChunkKind::Float | ChunkKind::Int64 if flags != 0 => {
            Err(invalid_data("schema-7 scalar chunk flags must be zero"))
        }
        ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
            if flags & !SCHEMA7_TYPED_CHUNK_FLAGS != 0 =>
        {
            Err(invalid_data(
                "schema-7 typed chunk flags contain reserved bits",
            ))
        }
        _ => Ok(()),
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn read_u16_at(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    const SERIES_REF: u32 = 0x1020_3040;
    const MIN_TIME_MS: u64 = 1_700_000_000_123;
    const MAX_TIME_MS: u64 = 1_700_000_004_567;
    const NATIVE_PAYLOAD_LEN: u32 = 29;
    const NATIVE_PAYLOAD_CRC: u32 = 0x89ab_cdef;
    const SCALAR_BODY_LEN: u32 = 37;
    const SCALAR_BODY_CRC: u32 = 0x7654_3210;

    fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
        bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn fixture(
        kind: ChunkKind,
        encoding: ChunkEncoding,
        scalar: bool,
    ) -> (Vec<u8>, Schema7ChunkPrefixExpectation) {
        let scalar_lane_len = if scalar {
            TYPED_SCALAR_LANE_HEADER_LEN as u32 + SCALAR_BODY_LEN
        } else {
            0
        };
        let mut prefix = vec![
            0;
            if scalar {
                SCHEMA7_INDEXED_PREFIX_WITH_SCALAR_LEN
            } else {
                SCHEMA7_INDEXED_PREFIX_LEN
            }
        ];
        prefix[0] = kind as u8;
        prefix[1] = encoding as u8;
        put_u16(
            &mut prefix,
            2,
            if matches!(kind, ChunkKind::Float | ChunkKind::Int64) {
                0
            } else {
                CHUNK_FLAG_HAS_START_TIME | CHUNK_FLAG_TEMPORALITY_DELTA
            },
        );
        put_u32(&mut prefix, 4, SERIES_REF);
        put_u64(&mut prefix, 8, MIN_TIME_MS);
        put_u64(&mut prefix, 16, MAX_TIME_MS);
        put_u32(&mut prefix, 24, 3);
        put_u32(&mut prefix, 28, CHUNK_HEADER_LEN as u32 + scalar_lane_len);
        put_u32(&mut prefix, 32, NATIVE_PAYLOAD_LEN);
        put_u32(&mut prefix, 36, NATIVE_PAYLOAD_CRC);
        if scalar {
            put_u32(&mut prefix, 40, TYPED_SCALAR_LANE_MAGIC);
            put_u16(&mut prefix, 44, TYPED_SCALAR_LANE_VERSION);
            put_u16(&mut prefix, 46, 0);
            put_u32(&mut prefix, 48, SCALAR_BODY_LEN);
            put_u32(&mut prefix, 52, SCALAR_BODY_CRC);
        }

        let expectation = Schema7ChunkPrefixExpectation {
            series_ref: SERIES_REF,
            kind,
            min_time_ms: MIN_TIME_MS,
            max_time_ms: MAX_TIME_MS,
            length: CHUNK_HEADER_LEN as u32 + scalar_lane_len + NATIVE_PAYLOAD_LEN,
            scalar_lane_offset: if scalar { CHUNK_HEADER_LEN as u32 } else { 0 },
            scalar_lane_len,
            indexed_prefix_crc32c: crc32c(&prefix),
        };
        (prefix, expectation)
    }

    fn assert_invalid_data(error: io::Error, message: &str) {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), message);
    }

    fn authenticate(prefix: &[u8], expectation: &mut Schema7ChunkPrefixExpectation) {
        expectation.indexed_prefix_crc32c = crc32c(prefix);
    }

    #[test]
    fn verifies_exact_non_scalar_golden_prefix() {
        let (prefix, expectation) = fixture(ChunkKind::Float, ChunkEncoding::Gorilla, false);
        assert_eq!(
            prefix,
            vec![
                0x00, 0x03, 0x00, 0x00, 0x40, 0x30, 0x20, 0x10, 0x7b, 0x68, 0xe5, 0xcf, 0x8b, 0x01,
                0x00, 0x00, 0xd7, 0x79, 0xe5, 0xcf, 0x8b, 0x01, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
                0x28, 0x00, 0x00, 0x00, 0x1d, 0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89,
            ]
        );
        assert_eq!(expectation.indexed_prefix_crc32c, 0xc0dd_f5fd);

        let verified = verify_schema7_indexed_prefix(&expectation, &prefix).unwrap();
        assert_eq!(verified.kind, ChunkKind::Float);
        assert_eq!(verified.encoding, ChunkEncoding::Gorilla);
        assert_eq!(verified.flags, 0);
        assert_eq!(verified.series_ref, SERIES_REF);
        assert_eq!(verified.min_time_ms, MIN_TIME_MS);
        assert_eq!(verified.max_time_ms, MAX_TIME_MS);
        assert_eq!(verified.num_points, 3);
        assert_eq!(verified.header_len, 40);
        assert_eq!(verified.payload_len, NATIVE_PAYLOAD_LEN);
        assert_eq!(verified.chunk_crc32c, NATIVE_PAYLOAD_CRC);
        assert_eq!(verified.scalar_lane, None);
    }

    #[test]
    fn verifies_exact_scalar_golden_prefix_and_retains_headers() {
        let (prefix, expectation) = fixture(
            ChunkKind::ExponentialHistogram,
            ChunkEncoding::SchemaVarLen,
            true,
        );
        assert_eq!(
            prefix,
            vec![
                0x03, 0x00, 0x12, 0x00, 0x40, 0x30, 0x20, 0x10, 0x7b, 0x68, 0xe5, 0xcf, 0x8b, 0x01,
                0x00, 0x00, 0xd7, 0x79, 0xe5, 0xcf, 0x8b, 0x01, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
                0x5d, 0x00, 0x00, 0x00, 0x1d, 0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x54, 0x53,
                0x43, 0x4c, 0x01, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76,
            ]
        );
        assert_eq!(expectation.indexed_prefix_crc32c, 0x2120_e2ff);

        let verified = verify_schema7_indexed_prefix(&expectation, &prefix).unwrap();
        assert_eq!(verified.kind, ChunkKind::ExponentialHistogram);
        assert_eq!(verified.encoding, ChunkEncoding::SchemaVarLen);
        assert_eq!(
            verified.flags,
            CHUNK_FLAG_HAS_START_TIME | CHUNK_FLAG_TEMPORALITY_DELTA
        );
        assert_eq!(
            verified.scalar_lane,
            Some(VerifiedSchema7ScalarLaneHeader {
                body_len: SCALAR_BODY_LEN,
                body_crc32c: SCALAR_BODY_CRC,
            })
        );
    }

    #[test]
    fn authenticates_unknown_kind_before_semantic_decode() {
        let (mut prefix, mut expectation) =
            fixture(ChunkKind::Float, ChunkEncoding::Gorilla, false);
        prefix[0] = 0xff;

        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 indexed prefix crc mismatch",
        );

        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "unknown chunk kind",
        );
    }

    #[test]
    fn rejects_unknown_or_kind_incompatible_encoding_after_authentication() {
        let (mut prefix, mut expectation) =
            fixture(ChunkKind::Float, ChunkEncoding::Gorilla, false);
        prefix[1] = 0xff;
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "unknown chunk encoding",
        );

        prefix[1] = ChunkEncoding::SchemaVarLen as u8;
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 chunk kind/encoding pair is invalid",
        );
    }

    #[test]
    fn rejects_reserved_chunk_flags_and_nonzero_scalar_flags() {
        let (mut prefix, mut expectation) =
            fixture(ChunkKind::Histogram, ChunkEncoding::SchemaVarLen, true);
        put_u16(&mut prefix, 2, 1 << 5);
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 typed chunk flags contain reserved bits",
        );

        put_u16(&mut prefix, 2, CHUNK_FLAG_HAS_START_TIME);
        put_u16(&mut prefix, 46, 1);
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 typed scalar lane flags must be zero",
        );

        let (mut prefix, mut expectation) = fixture(ChunkKind::Float, ChunkEncoding::RawF64, false);
        put_u16(&mut prefix, 2, CHUNK_FLAG_HAS_START_TIME);
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 scalar chunk flags must be zero",
        );
    }

    #[test]
    fn rejects_zero_points_and_reversed_time() {
        let (mut prefix, mut expectation) = fixture(ChunkKind::Int64, ChunkEncoding::RawI64, false);
        put_u32(&mut prefix, 24, 0);
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 chunk has zero points",
        );

        put_u32(&mut prefix, 24, 1);
        put_u64(&mut prefix, 8, MAX_TIME_MS + 1);
        expectation.min_time_ms = MAX_TIME_MS + 1;
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 chunk time range is reversed",
        );
    }

    #[test]
    fn rejects_noncanonical_scalar_locator_shapes() {
        let (prefix, expectation) =
            fixture(ChunkKind::Histogram, ChunkEncoding::SchemaVarLen, true);
        for (offset, len) in [(40, 0), (0, 16), (39, 16), (40, 15), (41, 16)] {
            let malformed = Schema7ChunkPrefixExpectation {
                scalar_lane_offset: offset,
                scalar_lane_len: len,
                ..expectation
            };
            assert_invalid_data(
                verify_schema7_indexed_prefix(&malformed, &prefix).unwrap_err(),
                "schema-7 scalar lane locator is not canonical",
            );
        }
    }

    #[test]
    fn rejects_scalar_lane_on_non_typed_chunk() {
        let (mut prefix, mut expectation) =
            fixture(ChunkKind::Histogram, ChunkEncoding::SchemaVarLen, true);
        prefix[0] = ChunkKind::Float as u8;
        prefix[1] = ChunkEncoding::RawF64 as u8;
        put_u16(&mut prefix, 2, 0);
        expectation.kind = ChunkKind::Float;
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 scalar lane requires a typed schema-varlen chunk",
        );
    }

    #[test]
    fn rejects_scalar_header_magic_version_and_body_length_corruption() {
        let (prefix, expectation) = fixture(ChunkKind::Summary, ChunkEncoding::SchemaVarLen, true);

        let mut bad_magic = prefix.clone();
        put_u32(&mut bad_magic, 40, TYPED_SCALAR_LANE_MAGIC ^ 1);
        let mut bad_magic_expectation = expectation;
        authenticate(&bad_magic, &mut bad_magic_expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&bad_magic_expectation, &bad_magic).unwrap_err(),
            "schema-7 typed scalar lane magic mismatch",
        );

        let mut bad_version = prefix.clone();
        put_u16(&mut bad_version, 44, TYPED_SCALAR_LANE_VERSION + 1);
        let mut bad_version_expectation = expectation;
        authenticate(&bad_version, &mut bad_version_expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&bad_version_expectation, &bad_version).unwrap_err(),
            "schema-7 typed scalar lane version is unsupported",
        );

        let mut bad_length = prefix.clone();
        put_u32(&mut bad_length, 48, SCALAR_BODY_LEN + 1);
        let mut bad_length_expectation = expectation;
        authenticate(&bad_length, &mut bad_length_expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&bad_length_expectation, &bad_length).unwrap_err(),
            "schema-7 typed scalar lane body length does not match locator",
        );

        let mut overflow = prefix;
        put_u32(&mut overflow, 48, u32::MAX);
        let mut overflow_expectation = expectation;
        authenticate(&overflow, &mut overflow_expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&overflow_expectation, &overflow).unwrap_err(),
            "schema-7 typed scalar lane length overflows",
        );
    }

    #[test]
    fn rejects_header_and_locator_length_disagreement_and_trailing_prefix() {
        let (mut prefix, mut expectation) = fixture(ChunkKind::Float, ChunkEncoding::RawF64, false);
        expectation.length += 1;
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 locator length does not match exact chunk length",
        );

        let too_short = Schema7ChunkPrefixExpectation {
            length: CHUNK_HEADER_LEN as u32 - 1,
            ..expectation
        };
        assert_invalid_data(
            verify_schema7_indexed_prefix(&too_short, &prefix).unwrap_err(),
            "schema-7 locator is shorter than its indexed chunk header",
        );

        expectation.length -= 1;
        put_u32(&mut prefix, 28, CHUNK_HEADER_LEN as u32 + 1);
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 scalar lane length does not match chunk header length",
        );

        let (mut prefix, expectation) = fixture(ChunkKind::Float, ChunkEncoding::RawF64, false);
        prefix.push(0);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 indexed prefix has trailing bytes",
        );
    }

    #[test]
    fn rejects_checked_header_and_exact_length_overflow() {
        let (prefix, expectation) =
            fixture(ChunkKind::Histogram, ChunkEncoding::SchemaVarLen, true);
        let oversized_header = Schema7ChunkPrefixExpectation {
            scalar_lane_len: u32::MAX,
            ..expectation
        };
        assert_invalid_data(
            verify_schema7_indexed_prefix(&oversized_header, &prefix).unwrap_err(),
            "schema-7 chunk header length overflows",
        );

        let (mut prefix, mut expectation) = fixture(ChunkKind::Float, ChunkEncoding::RawF64, false);
        put_u32(&mut prefix, 32, u32::MAX);
        authenticate(&prefix, &mut expectation);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&expectation, &prefix).unwrap_err(),
            "schema-7 chunk length overflows",
        );
    }

    #[test]
    fn reports_structural_short_reads_for_exact_prefix_spans() {
        let (prefix, expectation) = fixture(ChunkKind::Float, ChunkEncoding::RawF64, false);
        let error = verify_schema7_indexed_prefix(&expectation, &prefix[..39]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(error.to_string(), "schema-7 indexed prefix short read");

        let (prefix, expectation) =
            fixture(ChunkKind::Histogram, ChunkEncoding::SchemaVarLen, true);
        let error = verify_schema7_indexed_prefix(&expectation, &prefix[..55]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(error.to_string(), "schema-7 indexed prefix short read");
    }

    #[test]
    fn rejects_cross_record_prefix_substitution() {
        let (first_prefix, first_expectation) =
            fixture(ChunkKind::Histogram, ChunkEncoding::SchemaVarLen, true);
        let (mut second_prefix, _) =
            fixture(ChunkKind::Histogram, ChunkEncoding::SchemaVarLen, true);
        put_u32(&mut second_prefix, 4, SERIES_REF + 1);

        assert_invalid_data(
            verify_schema7_indexed_prefix(&first_expectation, &second_prefix).unwrap_err(),
            "schema-7 indexed prefix crc mismatch",
        );

        let mut substituted_locator = first_expectation;
        authenticate(&second_prefix, &mut substituted_locator);
        assert_invalid_data(
            verify_schema7_indexed_prefix(&substituted_locator, &second_prefix).unwrap_err(),
            "schema-7 locator series does not match chunk header",
        );

        assert!(verify_schema7_indexed_prefix(&first_expectation, &first_prefix).is_ok());
    }

    #[test]
    fn rejects_locator_kind_and_time_substitution_after_prefix_authentication() {
        let (prefix, expectation) = fixture(ChunkKind::Float, ChunkEncoding::Gorilla, false);
        let wrong_kind = Schema7ChunkPrefixExpectation {
            kind: ChunkKind::Int64,
            ..expectation
        };
        assert_invalid_data(
            verify_schema7_indexed_prefix(&wrong_kind, &prefix).unwrap_err(),
            "schema-7 locator kind does not match chunk header",
        );

        let wrong_time = Schema7ChunkPrefixExpectation {
            max_time_ms: MAX_TIME_MS + 1,
            ..expectation
        };
        assert_invalid_data(
            verify_schema7_indexed_prefix(&wrong_time, &prefix).unwrap_err(),
            "schema-7 locator time range does not match chunk header",
        );
    }
}
