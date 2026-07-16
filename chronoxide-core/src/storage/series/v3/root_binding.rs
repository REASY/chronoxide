//! Pure cross-artifact validation for the schema-7 fixed roots.
//!
//! The individual codecs authenticate one exact `series.bin` v3 root and one
//! exact `chunk_index.bin` v2 root. This module binds those roots to each
//! other and to independently inventoried segment facts before a caller may
//! use any descriptor or locator. It deliberately owns no file, cache, or
//! runtime state.

use std::io;

use crate::storage::chunk::{
    CHUNK_OVERFLOW_ROOT_V2_LEN, ChunkOverflowRootV2, decode_chunk_overflow_root_v2,
};

use super::{SeriesRootV3, decode_series_root_v3};

/// Independently validated context required to bind schema-7 roots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema7RootBindingContext {
    pub(crate) series_file_len: u64,
    pub(crate) chunk_index_file_len: u64,
    pub(crate) segment_start_ms: u64,
    pub(crate) segment_end_ms: u64,
    pub(crate) series_count: u32,
}

/// Derived immutable page ranges from an authenticated `series.bin` v3 root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema7SeriesPageFacts {
    pub(crate) root_len: u64,
    pub(crate) hot_page_count: u32,
    pub(crate) hot_pages_offset: u64,
    pub(crate) hot_pages_len: u64,
    pub(crate) cold_page_count: u32,
    pub(crate) cold_pages_offset: u64,
    pub(crate) cold_pages_len: u64,
    pub(crate) file_len: u64,
}

/// Derived immutable blob ranges from an authenticated `chunk_index.bin` v2 root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Schema7OverflowBlobFacts {
    pub(crate) root_len: u64,
    pub(crate) blob_count: u32,
    pub(crate) blobs_offset: u64,
    pub(crate) blobs_len: u64,
    pub(crate) file_len: u64,
}

/// Two authenticated roots bound to the same independently inventoried segment.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Schema7RootBinding {
    series_root: SeriesRootV3,
    chunk_index_root: ChunkOverflowRootV2,
    series_pages: Schema7SeriesPageFacts,
    overflow_blobs: Schema7OverflowBlobFacts,
}

impl Schema7RootBinding {
    /// Decodes and binds two exact root byte ranges.
    ///
    /// `context` lengths normally come from the canonical footer inventory;
    /// its segment bounds and series count come from independently validated
    /// segment metadata/index context. The returned roots are safe to use for
    /// subsequent touched-range reads, but this does not authenticate any page
    /// or overflow blob body.
    pub(crate) fn decode(
        series_root_bytes: &[u8],
        chunk_index_root_bytes: &[u8],
        context: Schema7RootBindingContext,
    ) -> io::Result<Self> {
        // Keep context validation ahead of either decoder so malformed caller
        // context has the same error precedence as the original byte API.
        validate_expected_segment_bounds(context)?;

        let series_root = decode_series_root_v3(series_root_bytes)?;
        let chunk_index_root =
            decode_chunk_overflow_root_v2(chunk_index_root_bytes, context.chunk_index_file_len)?;
        let (series_pages, overflow_blobs) =
            Self::bind_decoded(&series_root, &chunk_index_root, context)?;

        Ok(Self {
            series_root,
            chunk_index_root,
            series_pages,
            overflow_blobs,
        })
    }

    /// Cross-validates separately decoded roots without cloning either root.
    ///
    /// The caller keeps the independently cached roots pinned while using the
    /// returned copy-only facts for stateless touched-range reads. The facts do
    /// not retain either root and therefore must not be used as a replacement
    /// for those cache pins.
    pub(crate) fn bind_decoded(
        series_root: &SeriesRootV3,
        chunk_index_root: &ChunkOverflowRootV2,
        context: Schema7RootBindingContext,
    ) -> io::Result<(Schema7SeriesPageFacts, Schema7OverflowBlobFacts)> {
        validate_expected_segment_bounds(context)?;
        let series = series_root.header;

        if series.num_series != chunk_index_root.series_count {
            return Err(invalid_data(
                "schema-7 series and chunk-index root counts are not bound",
            ));
        }
        if series.chunk_index_file_len != chunk_index_root.file_len {
            return Err(invalid_data(
                "schema-7 series and chunk-index root lengths are not bound",
            ));
        }
        if series.chunk_index_root_crc32c != chunk_index_root.root_crc32c {
            return Err(invalid_data(
                "schema-7 series and chunk-index root CRCs are not bound",
            ));
        }
        if series.file_len != context.series_file_len {
            return Err(invalid_data(
                "schema-7 series root file length does not match footer inventory",
            ));
        }
        if series.chunk_index_file_len != context.chunk_index_file_len {
            return Err(invalid_data(
                "schema-7 series root chunk-index length does not match footer inventory",
            ));
        }
        if series.segment_start_ms != context.segment_start_ms
            || series.segment_end_ms != context.segment_end_ms
        {
            return Err(invalid_data(
                "schema-7 series root segment bounds do not match expected segment",
            ));
        }
        if series.num_series != context.series_count {
            return Err(invalid_data(
                "schema-7 series root count does not match expected segment",
            ));
        }
        if chunk_index_root.series_count != context.series_count {
            return Err(invalid_data(
                "schema-7 chunk-index root count does not match expected segment",
            ));
        }

        let cold_pages_len = series.cold_bytes_len()?;
        let series_pages = Schema7SeriesPageFacts {
            root_len: series.hot_pages_offset,
            hot_page_count: series.page_count,
            hot_pages_offset: series.hot_pages_offset,
            hot_pages_len: series.hot_pages_len,
            cold_page_count: series.cold_page_count,
            cold_pages_offset: series.keysets_offset,
            cold_pages_len,
            file_len: series.file_len,
        };
        let overflow_blobs = Schema7OverflowBlobFacts {
            root_len: CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
            blob_count: chunk_index_root.blob_count,
            blobs_offset: CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
            blobs_len: chunk_index_root.blobs_len,
            file_len: chunk_index_root.file_len,
        };

        Ok((series_pages, overflow_blobs))
    }

    pub(crate) fn series_root(&self) -> &SeriesRootV3 {
        &self.series_root
    }

    pub(crate) fn chunk_index_root(&self) -> &ChunkOverflowRootV2 {
        &self.chunk_index_root
    }

    pub(crate) fn series_pages(&self) -> Schema7SeriesPageFacts {
        self.series_pages
    }

    pub(crate) fn overflow_blobs(&self) -> Schema7OverflowBlobFacts {
        self.overflow_blobs
    }

    /// Returns the logical bytes owned by this decoded cache value.
    ///
    /// The fixed charge includes the binding, both inline roots, the two `Vec`
    /// headers, and derived facts. Heap charges use the descriptor vectors'
    /// actual capacities. The encoded input roots are caller-owned scratch and
    /// are deliberately excluded.
    pub(crate) fn charged_bytes(&self) -> io::Result<u64> {
        let hot_descriptor_bytes = self
            .series_root
            .hot_descriptors
            .capacity()
            .checked_mul(std::mem::size_of::<super::SeriesHotPageDescriptorV1>())
            .ok_or_else(charge_overflow)?;
        let cold_descriptor_bytes = self
            .series_root
            .cold_descriptors
            .capacity()
            .checked_mul(std::mem::size_of::<super::SeriesColdPageDescriptorV1>())
            .ok_or_else(charge_overflow)?;
        let charged_bytes = std::mem::size_of::<Self>()
            .checked_add(hot_descriptor_bytes)
            .and_then(|bytes| bytes.checked_add(cold_descriptor_bytes))
            .ok_or_else(charge_overflow)?;
        u64::try_from(charged_bytes).map_err(|_| charge_overflow())
    }
}

fn validate_expected_segment_bounds(context: Schema7RootBindingContext) -> io::Result<()> {
    if context.segment_start_ms >= context.segment_end_ms {
        return Err(invalid_data("schema-7 expected segment bounds are invalid"));
    }
    Ok(())
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn charge_overflow() -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        "schema-7 root binding charge exceeds the platform counter",
    )
}

#[cfg(test)]
mod tests {
    use crate::storage::chunk::{
        ChunkKind, ChunkOverflowBlobV1, OverflowChunkEntryV1, encode_chunk_index_v2,
    };

    use super::super::{
        SERIES_HOT_RECORDS_PER_PAGE_V1, SeriesColdPageDescriptorV1, SeriesHeaderV3,
        SeriesHeaderV3Params, SeriesHotPageDescriptorV1, encode_series_root_v3,
    };
    use super::*;

    const SEGMENT_START_MS: u64 = 1_000;
    const SEGMENT_END_MS: u64 = 2_000;

    struct Fixture {
        series_root: Vec<u8>,
        chunk_index_root: Vec<u8>,
        context: Schema7RootBindingContext,
    }

    fn fixture(series_count: u32) -> Fixture {
        let chunk_index = encode_chunk_index_v2(series_count, &[]).unwrap();
        fixture_with_chunk_index(
            series_count,
            &chunk_index.bytes,
            chunk_index.root.root_crc32c,
            chunk_index.root.file_len,
        )
    }

    fn fixture_with_chunk_index(
        series_count: u32,
        chunk_index: &[u8],
        bound_chunk_index_crc32c: u32,
        bound_chunk_index_file_len: u64,
    ) -> Fixture {
        let empty = series_count == 0;
        let header = SeriesHeaderV3::new(SeriesHeaderV3Params {
            num_series: series_count,
            num_keysets: u32::from(!empty),
            num_value_dicts: u32::from(!empty),
            chunk_index_root_crc32c: bound_chunk_index_crc32c,
            keysets_len: if empty { 8 } else { 16 },
            value_dicts_len: if empty { 8 } else { 16 },
            keyset_blocks_len: if empty { 8 } else { 16 },
            segment_start_ms: SEGMENT_START_MS,
            segment_end_ms: SEGMENT_END_MS,
            chunk_index_file_len: bound_chunk_index_file_len,
        })
        .unwrap();

        let hot_descriptors = (0..header.page_count)
            .map(|page_index| {
                let first_series_ref = page_index * SERIES_HOT_RECORDS_PER_PAGE_V1;
                SeriesHotPageDescriptorV1 {
                    first_series_ref,
                    record_count: (series_count - first_series_ref)
                        .min(SERIES_HOT_RECORDS_PER_PAGE_V1),
                    page_crc32c: 0x1000_0000 | page_index,
                }
            })
            .collect::<Vec<_>>();
        let cold_descriptors = [SeriesColdPageDescriptorV1::new(header, 0, 0x2000_0000).unwrap()];
        let (header, series_root) =
            encode_series_root_v3(header, &hot_descriptors, &cold_descriptors).unwrap();
        let chunk_index_file_len = u64::try_from(chunk_index.len()).unwrap();

        Fixture {
            series_root,
            chunk_index_root: chunk_index[..CHUNK_OVERFLOW_ROOT_V2_LEN].to_vec(),
            context: Schema7RootBindingContext {
                series_file_len: header.file_len,
                chunk_index_file_len,
                segment_start_ms: SEGMENT_START_MS,
                segment_end_ms: SEGMENT_END_MS,
                series_count,
            },
        }
    }

    fn one_blob_chunk_index(series_count: u32) -> Vec<u8> {
        encode_chunk_index_v2(
            series_count,
            &[ChunkOverflowBlobV1 {
                series_ref: 0,
                entries: vec![OverflowChunkEntryV1 {
                    file_id: 0,
                    kind: ChunkKind::Float,
                    min_time_ms: SEGMENT_START_MS,
                    max_time_ms: SEGMENT_START_MS,
                    offset: 0,
                    length: 40,
                    scalar_lane_offset: 0,
                    scalar_lane_len: 0,
                    indexed_prefix_crc32c: 0x1234_5678,
                }],
            }],
        )
        .unwrap()
        .bytes
    }

    fn equal_length_substitution_indexes() -> (Vec<u8>, Vec<u8>) {
        let entry = |index: u32| OverflowChunkEntryV1 {
            file_id: 0,
            kind: ChunkKind::Float,
            min_time_ms: SEGMENT_START_MS + u64::from(index),
            max_time_ms: SEGMENT_START_MS + u64::from(index),
            offset: u64::from(index) * 40,
            length: 40,
            scalar_lane_offset: 0,
            scalar_lane_len: 0,
            indexed_prefix_crc32c: 0x3000_0000 | index,
        };

        // 1 * 32 + 20 * 44 == 12 * 32 + 12 * 44. These valid indexes have
        // the same series count and file length, but distinct roots and CRCs.
        let one_blob = encode_chunk_index_v2(
            12,
            &[ChunkOverflowBlobV1 {
                series_ref: 0,
                entries: (0..20).map(entry).collect(),
            }],
        )
        .unwrap()
        .bytes;
        let twelve_blobs = encode_chunk_index_v2(
            12,
            &(0..12)
                .map(|series_ref| ChunkOverflowBlobV1 {
                    series_ref,
                    entries: vec![entry(series_ref)],
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .bytes;
        assert_eq!(one_blob.len(), twelve_blobs.len());
        assert_ne!(
            &one_blob[..CHUNK_OVERFLOW_ROOT_V2_LEN],
            &twelve_blobs[..CHUNK_OVERFLOW_ROOT_V2_LEN]
        );
        (one_blob, twelve_blobs)
    }

    fn assert_invalid_data(error: io::Error, expected: &str) {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn canonical_roots_bind_and_expose_golden_page_and_blob_facts() {
        let fixture = fixture(410);
        let binding = Schema7RootBinding::decode(
            &fixture.series_root,
            &fixture.chunk_index_root,
            fixture.context,
        )
        .unwrap();

        assert_eq!(binding.series_root().header.num_series, 410);
        assert_eq!(
            binding.series_root().header.chunk_index_root_crc32c,
            binding.chunk_index_root().root_crc32c
        );
        assert_eq!(
            binding.series_pages(),
            Schema7SeriesPageFacts {
                root_len: 4_096,
                hot_page_count: 2,
                hot_pages_offset: 4_096,
                hot_pages_len: 32_768,
                cold_page_count: 1,
                cold_pages_offset: 36_864,
                cold_pages_len: 48,
                file_len: 36_912,
            }
        );
        assert_eq!(
            binding.overflow_blobs(),
            Schema7OverflowBlobFacts {
                root_len: 64,
                blob_count: 0,
                blobs_offset: 64,
                blobs_len: 0,
                file_len: 64,
            }
        );

        let expected_charge = std::mem::size_of::<Schema7RootBinding>()
            + binding.series_root().hot_descriptors.capacity()
                * std::mem::size_of::<SeriesHotPageDescriptorV1>()
            + binding.series_root().cold_descriptors.capacity()
                * std::mem::size_of::<SeriesColdPageDescriptorV1>();
        assert_eq!(binding.charged_bytes().unwrap(), expected_charge as u64);
        assert!(binding.charged_bytes().unwrap() < fixture.series_root.len() as u64);
    }

    #[test]
    fn decoded_roots_bind_without_cloning_or_retaining_descriptors() {
        let fixture = fixture(410);
        let (series_pages, overflow_blobs) = {
            let series_root = decode_series_root_v3(&fixture.series_root).unwrap();
            let chunk_index_root = decode_chunk_overflow_root_v2(
                &fixture.chunk_index_root,
                fixture.context.chunk_index_file_len,
            )
            .unwrap();
            let hot_descriptors = series_root.hot_descriptors.as_ptr();
            let cold_descriptors = series_root.cold_descriptors.as_ptr();

            let facts =
                Schema7RootBinding::bind_decoded(&series_root, &chunk_index_root, fixture.context)
                    .unwrap();

            assert_eq!(series_root.hot_descriptors.as_ptr(), hot_descriptors);
            assert_eq!(series_root.cold_descriptors.as_ptr(), cold_descriptors);
            facts
        };

        // The returned values are standalone copy-only range facts, not a
        // hidden cross-artifact owner or borrow of either decoded root.
        assert_eq!(series_pages.root_len, 4_096);
        assert_eq!(series_pages.hot_page_count, 2);
        assert_eq!(series_pages.file_len, 36_912);
        assert_eq!(overflow_blobs.root_len, 64);
        assert_eq!(overflow_blobs.blob_count, 0);
        assert_eq!(overflow_blobs.file_len, 64);
    }

    #[test]
    fn canonical_empty_roots_bind_without_inventing_a_hot_or_blob_page() {
        let fixture = fixture(0);
        let binding = Schema7RootBinding::decode(
            &fixture.series_root,
            &fixture.chunk_index_root,
            fixture.context,
        )
        .unwrap();

        assert_eq!(binding.series_root().hot_descriptors.len(), 0);
        assert_eq!(binding.series_root().cold_descriptors.len(), 1);
        assert_eq!(binding.series_pages().root_len, 4_096);
        assert_eq!(binding.series_pages().hot_page_count, 0);
        assert_eq!(binding.series_pages().hot_pages_len, 0);
        assert_eq!(binding.series_pages().cold_page_count, 1);
        assert_eq!(binding.series_pages().cold_pages_offset, 4_096);
        assert_eq!(binding.series_pages().cold_pages_len, 24);
        assert_eq!(binding.series_pages().file_len, 4_120);
        assert_eq!(binding.overflow_blobs().blob_count, 0);
        assert_eq!(binding.overflow_blobs().blobs_len, 0);
    }

    #[test]
    fn valid_root_substitution_is_rejected() {
        let (bound_chunk_index, substituted_chunk_index) = equal_length_substitution_indexes();
        let bound_crc32c = u32::from_le_bytes(bound_chunk_index[56..60].try_into().unwrap());
        let fixture = fixture_with_chunk_index(
            12,
            &bound_chunk_index,
            bound_crc32c,
            bound_chunk_index.len() as u64,
        );

        let error = Schema7RootBinding::decode(
            &fixture.series_root,
            &substituted_chunk_index[..CHUNK_OVERFLOW_ROOT_V2_LEN],
            fixture.context,
        )
        .unwrap_err();
        assert_invalid_data(error, "root CRCs are not bound");

        let series_root = decode_series_root_v3(&fixture.series_root).unwrap();
        let substituted_root = decode_chunk_overflow_root_v2(
            &substituted_chunk_index[..CHUNK_OVERFLOW_ROOT_V2_LEN],
            fixture.context.chunk_index_file_len,
        )
        .unwrap();
        let error =
            Schema7RootBinding::bind_decoded(&series_root, &substituted_root, fixture.context)
                .unwrap_err();
        assert_invalid_data(error, "root CRCs are not bound");
    }

    #[test]
    fn truncated_or_non_exact_root_ranges_are_rejected() {
        let fixture = fixture(2);

        assert!(
            Schema7RootBinding::decode(
                &fixture.series_root[..fixture.series_root.len() - 1],
                &fixture.chunk_index_root,
                fixture.context,
            )
            .is_err()
        );
        assert!(
            Schema7RootBinding::decode(
                &fixture.series_root,
                &fixture.chunk_index_root[..CHUNK_OVERFLOW_ROOT_V2_LEN - 1],
                fixture.context,
            )
            .is_err()
        );

        let mut series_with_trailing = fixture.series_root.clone();
        series_with_trailing.push(0);
        assert!(
            Schema7RootBinding::decode(
                &series_with_trailing,
                &fixture.chunk_index_root,
                fixture.context,
            )
            .is_err()
        );
        let mut chunk_index_with_trailing = fixture.chunk_index_root.clone();
        chunk_index_with_trailing.push(0);
        assert!(
            Schema7RootBinding::decode(
                &fixture.series_root,
                &chunk_index_with_trailing,
                fixture.context,
            )
            .is_err()
        );
    }

    #[test]
    fn footer_inventory_lengths_must_match_both_roots() {
        let fixture = fixture(2);

        let mut wrong_series_len = fixture.context;
        wrong_series_len.series_file_len += 1;
        assert_invalid_data(
            Schema7RootBinding::decode(
                &fixture.series_root,
                &fixture.chunk_index_root,
                wrong_series_len,
            )
            .unwrap_err(),
            "series root file length does not match footer inventory",
        );

        let mut wrong_chunk_index_len = fixture.context;
        wrong_chunk_index_len.chunk_index_file_len += 1;
        assert_invalid_data(
            Schema7RootBinding::decode(
                &fixture.series_root,
                &fixture.chunk_index_root,
                wrong_chunk_index_len,
            )
            .unwrap_err(),
            "chunk index file length does not match root",
        );

        let chunk_index = encode_chunk_index_v2(2, &[]).unwrap();
        let wrong_embedded_len = fixture_with_chunk_index(
            2,
            &chunk_index.bytes,
            chunk_index.root.root_crc32c,
            chunk_index.root.file_len + 1,
        );
        assert_invalid_data(
            Schema7RootBinding::decode(
                &wrong_embedded_len.series_root,
                &wrong_embedded_len.chunk_index_root,
                wrong_embedded_len.context,
            )
            .unwrap_err(),
            "root lengths are not bound",
        );
    }

    #[test]
    fn expected_segment_bounds_are_required_and_must_match() {
        let fixture = fixture(2);

        let mut invalid_bounds = fixture.context;
        invalid_bounds.segment_start_ms = invalid_bounds.segment_end_ms;
        assert_invalid_data(
            Schema7RootBinding::decode(
                &fixture.series_root,
                &fixture.chunk_index_root,
                invalid_bounds,
            )
            .unwrap_err(),
            "expected segment bounds are invalid",
        );

        assert_invalid_data(
            Schema7RootBinding::decode(&[], &[], invalid_bounds).unwrap_err(),
            "expected segment bounds are invalid",
        );

        let mut wrong_start = fixture.context;
        wrong_start.segment_start_ms += 1;
        assert_invalid_data(
            Schema7RootBinding::decode(
                &fixture.series_root,
                &fixture.chunk_index_root,
                wrong_start,
            )
            .unwrap_err(),
            "segment bounds do not match expected segment",
        );

        let mut wrong_end = fixture.context;
        wrong_end.segment_end_ms += 1;
        assert_invalid_data(
            Schema7RootBinding::decode(&fixture.series_root, &fixture.chunk_index_root, wrong_end)
                .unwrap_err(),
            "segment bounds do not match expected segment",
        );
    }

    #[test]
    fn expected_and_cross_root_series_counts_must_match() {
        let fixture = fixture(2);

        let mut wrong_expected = fixture.context;
        wrong_expected.series_count += 1;
        assert_invalid_data(
            Schema7RootBinding::decode(
                &fixture.series_root,
                &fixture.chunk_index_root,
                wrong_expected,
            )
            .unwrap_err(),
            "series root count does not match expected segment",
        );

        let other_chunk_index = encode_chunk_index_v2(3, &[]).unwrap();
        assert_invalid_data(
            Schema7RootBinding::decode(
                &fixture.series_root,
                &other_chunk_index.bytes[..CHUNK_OVERFLOW_ROOT_V2_LEN],
                fixture.context,
            )
            .unwrap_err(),
            "root counts are not bound",
        );
    }

    #[test]
    fn both_root_crcs_and_the_cross_root_crc_binding_are_required() {
        let fixture = fixture(2);

        let mut corrupt_series = fixture.series_root.clone();
        corrupt_series[200] ^= 1;
        assert_invalid_data(
            Schema7RootBinding::decode(&corrupt_series, &fixture.chunk_index_root, fixture.context)
                .unwrap_err(),
            "root CRC mismatch",
        );

        let mut corrupt_chunk_index = fixture.chunk_index_root.clone();
        corrupt_chunk_index[8] ^= 1;
        assert_invalid_data(
            Schema7RootBinding::decode(&fixture.series_root, &corrupt_chunk_index, fixture.context)
                .unwrap_err(),
            "root crc mismatch",
        );

        let chunk_index = encode_chunk_index_v2(2, &[]).unwrap();
        let wrong_binding = fixture_with_chunk_index(
            2,
            &chunk_index.bytes,
            chunk_index.root.root_crc32c ^ 1,
            chunk_index.root.file_len,
        );
        assert_invalid_data(
            Schema7RootBinding::decode(
                &wrong_binding.series_root,
                &wrong_binding.chunk_index_root,
                wrong_binding.context,
            )
            .unwrap_err(),
            "root CRCs are not bound",
        );
    }

    #[test]
    fn derived_ranges_end_at_the_authenticated_file_lengths() {
        let chunk_index = one_blob_chunk_index(2);
        let root_crc32c = u32::from_le_bytes(chunk_index[56..60].try_into().unwrap());
        let fixture =
            fixture_with_chunk_index(2, &chunk_index, root_crc32c, chunk_index.len() as u64);
        let binding = Schema7RootBinding::decode(
            &fixture.series_root,
            &fixture.chunk_index_root,
            fixture.context,
        )
        .unwrap();

        let pages = binding.series_pages();
        assert_eq!(
            pages.hot_pages_offset + pages.hot_pages_len,
            pages.cold_pages_offset
        );
        assert_eq!(
            pages.cold_pages_offset + pages.cold_pages_len,
            pages.file_len
        );
        let blobs = binding.overflow_blobs();
        assert_eq!(blobs.blobs_offset + blobs.blobs_len, blobs.file_len);
        assert_eq!(blobs.blob_count, 1);
    }
}
