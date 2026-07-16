//! Schema-neutral chunk locators for metadata planning.
//!
//! This module validates only facts available before payload I/O. Schema-7
//! indexed-prefix verification and legacy schema-6 chunk decoding remain
//! separate integration steps.

use std::cmp::Ordering;

use thiserror::Error;

use super::{CHUNK_HEADER_LEN, ChunkIndexEntry, ChunkKind, TYPED_SCALAR_LANE_HEADER_LEN};

const CHUNK_FILE_IN_ORDER: u8 = 0;
const CHUNK_FILE_OUT_OF_ORDER: u8 = 1;
const INDEXED_PREFIX_WITH_SCALAR_LEN: usize = CHUNK_HEADER_LEN + TYPED_SCALAR_LANE_HEADER_LEN;

/// Authentication carried by the metadata schema that produced a locator.
///
/// A zero schema-7 CRC is a valid value. The enum variant, rather than a
/// sentinel CRC, distinguishes authenticated schema 7 from legacy schema 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum IndexedChunkAuthentication {
    Schema6V1Legacy,
    Schema7 { indexed_prefix_crc32c: u32 },
}

/// One checked, schema-neutral locator for an exact indexed chunk range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexedChunkLocator {
    series_ref: u32,
    entry: ChunkIndexEntry,
    authentication: IndexedChunkAuthentication,
}

impl IndexedChunkLocator {
    /// Builds a locator decoded from the legacy schema-6 chunk index v1.
    pub(crate) fn try_schema6_v1(
        series_ref: u32,
        entry: ChunkIndexEntry,
    ) -> Result<Self, IndexedChunkLocatorError> {
        Self::try_new(
            series_ref,
            entry,
            IndexedChunkAuthentication::Schema6V1Legacy,
        )
    }

    /// Builds a locator decoded from schema 7.
    ///
    /// The optional input models metadata decode boundaries where an external
    /// CRC may be absent. Absence is rejected; `Some(0)` is retained exactly.
    /// Schema-7 metadata has no chunk-flags field, so `entry.flags` must be the
    /// zero placeholder. Authoritative flags come only from the subsequently
    /// authenticated indexed prefix.
    pub(crate) fn try_schema7(
        series_ref: u32,
        entry: ChunkIndexEntry,
        indexed_prefix_crc32c: Option<u32>,
    ) -> Result<Self, IndexedChunkLocatorError> {
        let indexed_prefix_crc32c =
            indexed_prefix_crc32c.ok_or(IndexedChunkLocatorError::MissingSchema7Authentication)?;
        if entry.flags != 0 {
            return Err(IndexedChunkLocatorError::Schema7FlagsMustBeZero { flags: entry.flags });
        }
        Self::try_new(
            series_ref,
            entry,
            IndexedChunkAuthentication::Schema7 {
                indexed_prefix_crc32c,
            },
        )
    }

    fn try_new(
        series_ref: u32,
        entry: ChunkIndexEntry,
        authentication: IndexedChunkAuthentication,
    ) -> Result<Self, IndexedChunkLocatorError> {
        validate_entry(&entry)?;
        Ok(Self {
            series_ref,
            entry,
            authentication,
        })
    }

    pub(crate) fn series_ref(&self) -> u32 {
        self.series_ref
    }

    pub(crate) fn entry(&self) -> &ChunkIndexEntry {
        &self.entry
    }

    pub(crate) fn authentication(&self) -> IndexedChunkAuthentication {
        self.authentication
    }

    /// Exact raw prefix length selected solely from the checked locator shape.
    pub(crate) fn indexed_prefix_len(&self) -> usize {
        if self.entry.scalar_lane_len == 0 {
            CHUNK_HEADER_LEN
        } else {
            INDEXED_PREFIX_WITH_SCALAR_LEN
        }
    }

    /// Physical payload identity retained through request planning and routing.
    pub(crate) fn payload_identity(&self) -> (u8, u64, u32) {
        (self.entry.file_id, self.entry.offset, self.entry.length)
    }
}

impl PartialOrd for IndexedChunkLocator {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexedChunkLocator {
    fn cmp(&self, other: &Self) -> Ordering {
        self.series_ref
            .cmp(&other.series_ref)
            .then_with(|| self.entry.file_id.cmp(&other.entry.file_id))
            .then_with(|| self.entry.kind.cmp(&other.entry.kind))
            .then_with(|| self.entry.flags.cmp(&other.entry.flags))
            .then_with(|| self.entry.min_time_ms.cmp(&other.entry.min_time_ms))
            .then_with(|| self.entry.max_time_ms.cmp(&other.entry.max_time_ms))
            .then_with(|| self.entry.offset.cmp(&other.entry.offset))
            .then_with(|| self.entry.length.cmp(&other.entry.length))
            .then_with(|| {
                self.entry
                    .scalar_lane_offset
                    .cmp(&other.entry.scalar_lane_offset)
            })
            .then_with(|| self.entry.scalar_lane_len.cmp(&other.entry.scalar_lane_len))
            .then_with(|| self.authentication.cmp(&other.authentication))
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexedChunkLocatorError {
    #[error("chunk locator file_id is invalid: {file_id}")]
    InvalidFileId { file_id: u8 },
    #[error("chunk locator time range is reversed: min={min_time_ms} max={max_time_ms}")]
    ReversedTimeRange { min_time_ms: u64, max_time_ms: u64 },
    #[error("chunk locator is shorter than the 40-byte chunk header: {length}")]
    ChunkTooShort { length: u32 },
    #[error("chunk locator file range overflows: offset={offset} length={length}")]
    FileRangeOverflow { offset: u64, length: u32 },
    #[error(
        "chunk scalar-lane locator is not canonical: offset={scalar_lane_offset} length={scalar_lane_len}"
    )]
    NonCanonicalScalarLane {
        scalar_lane_offset: u32,
        scalar_lane_len: u32,
    },
    #[error("chunk scalar lane is not valid for kind {kind:?}")]
    ScalarLaneKind { kind: ChunkKind },
    #[error(
        "chunk scalar lane exceeds the exact chunk range: scalar_end={scalar_end} chunk_length={chunk_length}"
    )]
    ScalarLaneOutOfBounds { scalar_end: u32, chunk_length: u32 },
    #[error("schema-7 chunk locator is missing indexed-prefix authentication")]
    MissingSchema7Authentication,
    #[error("schema-7 chunk locator has unauthenticated nonzero flags: {flags:#06x}")]
    Schema7FlagsMustBeZero { flags: u16 },
}

fn validate_entry(entry: &ChunkIndexEntry) -> Result<(), IndexedChunkLocatorError> {
    if !matches!(entry.file_id, CHUNK_FILE_IN_ORDER | CHUNK_FILE_OUT_OF_ORDER) {
        return Err(IndexedChunkLocatorError::InvalidFileId {
            file_id: entry.file_id,
        });
    }
    if entry.min_time_ms > entry.max_time_ms {
        return Err(IndexedChunkLocatorError::ReversedTimeRange {
            min_time_ms: entry.min_time_ms,
            max_time_ms: entry.max_time_ms,
        });
    }
    if entry.length < CHUNK_HEADER_LEN as u32 {
        return Err(IndexedChunkLocatorError::ChunkTooShort {
            length: entry.length,
        });
    }
    entry.offset.checked_add(u64::from(entry.length)).ok_or(
        IndexedChunkLocatorError::FileRangeOverflow {
            offset: entry.offset,
            length: entry.length,
        },
    )?;

    match (entry.scalar_lane_offset, entry.scalar_lane_len) {
        (0, 0) => Ok(()),
        (offset, length)
            if offset == CHUNK_HEADER_LEN as u32
                && length >= TYPED_SCALAR_LANE_HEADER_LEN as u32 =>
        {
            if !matches!(
                entry.kind,
                ChunkKind::Histogram | ChunkKind::ExponentialHistogram | ChunkKind::Summary
            ) {
                return Err(IndexedChunkLocatorError::ScalarLaneKind { kind: entry.kind });
            }
            let scalar_end = offset.checked_add(length).ok_or(
                IndexedChunkLocatorError::NonCanonicalScalarLane {
                    scalar_lane_offset: offset,
                    scalar_lane_len: length,
                },
            )?;
            if scalar_end > entry.length {
                return Err(IndexedChunkLocatorError::ScalarLaneOutOfBounds {
                    scalar_end,
                    chunk_length: entry.length,
                });
            }
            Ok(())
        }
        (scalar_lane_offset, scalar_lane_len) => {
            Err(IndexedChunkLocatorError::NonCanonicalScalarLane {
                scalar_lane_offset,
                scalar_lane_len,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const ALL_KINDS: [ChunkKind; 5] = [
        ChunkKind::Float,
        ChunkKind::Int64,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ];
    const SCALAR_KINDS: [ChunkKind; 3] = [
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ];

    fn entry(kind: ChunkKind, file_id: u8) -> ChunkIndexEntry {
        ChunkIndexEntry {
            file_id,
            kind,
            flags: 0,
            min_time_ms: 100,
            max_time_ms: 120,
            offset: 4096,
            length: 80,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
        }
    }

    #[test]
    fn accepts_both_files_and_all_kinds_without_scalar_lanes() {
        for file_id in [CHUNK_FILE_IN_ORDER, CHUNK_FILE_OUT_OF_ORDER] {
            for kind in ALL_KINDS {
                let legacy = IndexedChunkLocator::try_schema6_v1(7, entry(kind, file_id)).unwrap();
                assert_eq!(legacy.series_ref(), 7);
                assert_eq!(legacy.entry().kind, kind);
                assert_eq!(legacy.indexed_prefix_len(), 40);
                assert_eq!(legacy.payload_identity(), (file_id, 4096, 80));

                let schema7 =
                    IndexedChunkLocator::try_schema7(7, entry(kind, file_id), Some(0x1234))
                        .unwrap();
                assert_eq!(schema7.indexed_prefix_len(), 40);
                assert_eq!(schema7.payload_identity(), (file_id, 4096, 80));
            }
        }
    }

    #[test]
    fn accepts_canonical_scalar_lanes_for_every_typed_kind_and_file() {
        for file_id in [CHUNK_FILE_IN_ORDER, CHUNK_FILE_OUT_OF_ORDER] {
            for kind in SCALAR_KINDS {
                let mut scalar = entry(kind, file_id);
                scalar.scalar_lane_offset = 40;
                scalar.scalar_lane_len = 16;
                scalar.length = 56;

                let locator = IndexedChunkLocator::try_schema7(9, scalar, Some(1)).unwrap();
                assert_eq!(locator.indexed_prefix_len(), 56);
                assert_eq!(locator.payload_identity(), (file_id, 4096, 56));
            }
        }
    }

    #[test]
    fn rejects_invalid_file_time_and_file_ranges() {
        let mut invalid_file = entry(ChunkKind::Float, 2);
        assert!(matches!(
            IndexedChunkLocator::try_schema6_v1(0, invalid_file.clone()),
            Err(IndexedChunkLocatorError::InvalidFileId { file_id: 2 })
        ));

        invalid_file.file_id = 0;
        invalid_file.min_time_ms = 121;
        assert!(matches!(
            IndexedChunkLocator::try_schema6_v1(0, invalid_file),
            Err(IndexedChunkLocatorError::ReversedTimeRange { .. })
        ));

        for length in [0, 39] {
            let mut too_short = entry(ChunkKind::Float, 0);
            too_short.length = length;
            assert_eq!(
                IndexedChunkLocator::try_schema6_v1(0, too_short).unwrap_err(),
                IndexedChunkLocatorError::ChunkTooShort { length }
            );
        }

        let mut overflowing = entry(ChunkKind::Float, 0);
        overflowing.offset = u64::MAX - 39;
        overflowing.length = 40;
        assert!(matches!(
            IndexedChunkLocator::try_schema6_v1(0, overflowing),
            Err(IndexedChunkLocatorError::FileRangeOverflow { .. })
        ));
    }

    #[test]
    fn rejects_noncanonical_or_out_of_bounds_scalar_shapes() {
        for (scalar_lane_offset, scalar_lane_len) in [
            (0, 16),
            (40, 0),
            (40, 15),
            (41, 16),
            (u32::MAX, 16),
            (40, u32::MAX),
        ] {
            let mut malformed = entry(ChunkKind::Histogram, 0);
            malformed.scalar_lane_offset = scalar_lane_offset;
            malformed.scalar_lane_len = scalar_lane_len;
            assert!(matches!(
                IndexedChunkLocator::try_schema7(0, malformed, Some(1)),
                Err(IndexedChunkLocatorError::NonCanonicalScalarLane { .. })
            ));
        }

        let mut out_of_bounds = entry(ChunkKind::Histogram, 0);
        out_of_bounds.scalar_lane_offset = 40;
        out_of_bounds.scalar_lane_len = 41;
        assert!(matches!(
            IndexedChunkLocator::try_schema7(0, out_of_bounds, Some(1)),
            Err(IndexedChunkLocatorError::ScalarLaneOutOfBounds { .. })
        ));

        for kind in [ChunkKind::Float, ChunkKind::Int64] {
            let mut wrong_kind = entry(kind, 0);
            wrong_kind.scalar_lane_offset = 40;
            wrong_kind.scalar_lane_len = 16;
            assert_eq!(
                IndexedChunkLocator::try_schema7(0, wrong_kind, Some(1)).unwrap_err(),
                IndexedChunkLocatorError::ScalarLaneKind { kind }
            );
        }
    }

    #[test]
    fn schema_authentication_is_explicit_and_zero_is_not_a_sentinel() {
        let entry = entry(ChunkKind::Float, 0);
        let legacy = IndexedChunkLocator::try_schema6_v1(3, entry.clone()).unwrap();
        let schema7_zero = IndexedChunkLocator::try_schema7(3, entry.clone(), Some(0)).unwrap();

        assert_eq!(
            legacy.authentication(),
            IndexedChunkAuthentication::Schema6V1Legacy
        );
        assert_eq!(
            schema7_zero.authentication(),
            IndexedChunkAuthentication::Schema7 {
                indexed_prefix_crc32c: 0
            }
        );
        assert_ne!(legacy, schema7_zero);
        assert_ne!(legacy.cmp(&schema7_zero), Ordering::Equal);
        assert_eq!(
            IndexedChunkLocator::try_schema7(3, entry, None).unwrap_err(),
            IndexedChunkLocatorError::MissingSchema7Authentication
        );
    }

    #[test]
    fn schema7_rejects_unauthenticated_flags_while_legacy_preserves_them() {
        let mut flagged = entry(ChunkKind::Histogram, 0);
        flagged.flags = 0x0002;

        let legacy = IndexedChunkLocator::try_schema6_v1(3, flagged.clone()).unwrap();
        assert_eq!(legacy.entry().flags, 0x0002);
        assert_eq!(
            IndexedChunkLocator::try_schema7(3, flagged, Some(7)).unwrap_err(),
            IndexedChunkLocatorError::Schema7FlagsMustBeZero { flags: 0x0002 }
        );
    }

    #[test]
    fn equality_order_and_payload_identity_keep_file_id() {
        let file0 =
            IndexedChunkLocator::try_schema7(5, entry(ChunkKind::Float, 0), Some(7)).unwrap();
        let file1 =
            IndexedChunkLocator::try_schema7(5, entry(ChunkKind::Float, 1), Some(7)).unwrap();

        assert_ne!(file0, file1);
        assert_eq!(file0.cmp(&file1), Ordering::Less);
        assert_eq!(file0.payload_identity(), (0, 4096, 80));
        assert_eq!(file1.payload_identity(), (1, 4096, 80));

        let ordered = BTreeSet::from([file0, file1]);
        assert_eq!(ordered.len(), 2);
    }
}
