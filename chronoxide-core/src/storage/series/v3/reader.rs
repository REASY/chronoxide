//! Pure schema-7 metadata planning after touched-range authentication.
//!
//! These helpers deliberately own no file, cache, or runtime state. They use
//! the existing hot-page and overflow-blob codecs as the authentication and
//! structural-validation boundary, then translate the selected metadata into
//! schema-neutral series and chunk plans.

use std::io;

use crate::storage::chunk::{
    ChunkIndexEntry, ChunkKind, ChunkOverflowBlobLocatorV1, ChunkOverflowRootV2,
    IndexedChunkLocator, OverflowChunkEntryV1, visit_physical_chunk_overflow_blob_v1,
};

use super::{
    InlineChunkV3, SERIES_HOT_PAGE_HEADER_LEN_V1, SERIES_HOT_PAGE_LEN_V1, SERIES_HOT_RECORD_LEN_V3,
    Schema7OverflowBlobFacts, Schema7SeriesPageFacts, SeriesHeaderV3, SeriesHotLocationV3,
    SeriesHotPageDescriptorV1, SeriesHotV3, SeriesHotV3Context,
    classifier::chunk_entry_fits_inline, visit_series_hot_page_v1,
};

/// The unchanged v2 cold-label row addressed by one schema-neutral series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColdLabelRowLocator {
    pub(crate) keyset_id: u32,
    pub(crate) row: u32,
}

/// Immediate or deferred source of exact chunk locators for one series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChunkLocatorSource {
    Inline(IndexedChunkLocator),
    Overflow {
        locator: ChunkOverflowBlobLocatorV1,
        expected_kind_mask: u8,
    },
}

/// Compact schema-neutral metadata retained while a query plans one series.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PlannedSeries {
    pub(crate) series_ref: u32,
    pub(crate) kind_mask: u8,
    pub(crate) cold_labels: ColdLabelRowLocator,
    pub(crate) chunks: ChunkLocatorSource,
    // This is an expected fingerprint, not an authenticated stable identity.
    // Only the governed cold-label materializer may consume it and expose the
    // verified `series_id` in its result.
    pub(super) expected_label_identity: u64,
}

impl std::fmt::Debug for PlannedSeries {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlannedSeries")
            .field("series_ref", &self.series_ref)
            .field("kind_mask", &self.kind_mask)
            .field("cold_labels", &self.cold_labels)
            .field("chunks", &self.chunks)
            .finish_non_exhaustive()
    }
}

/// The contiguous locator range belonging to one series in a flat batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SeriesChunkSpan {
    pub(crate) series_ref: u32,
    pub(crate) start: u32,
    pub(crate) len: u32,
}

/// Chunk locators without a `Vec` allocation per selected series.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FlatChunkLocatorBatch {
    locators: Vec<IndexedChunkLocator>,
    series_spans: Vec<SeriesChunkSpan>,
}

impl FlatChunkLocatorBatch {
    pub(crate) fn locators(&self) -> &[IndexedChunkLocator] {
        &self.locators
    }

    pub(crate) fn series_spans(&self) -> &[SeriesChunkSpan] {
        &self.series_spans
    }

    pub(super) fn capacities(&self) -> (usize, usize) {
        (self.locators.capacity(), self.series_spans.capacity())
    }
}

/// Cacheable authenticated hot page with the exact root, page, descriptor,
/// and footer chunk-file lengths used to validate its bytes.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ValidatedSeriesHotPage {
    header: SeriesHeaderV3,
    page_index: u32,
    descriptor: SeriesHotPageDescriptorV1,
    chunk_file_lens: [u64; 2],
    bytes: Box<[u8]>,
}

impl ValidatedSeriesHotPage {
    /// Returns the exact final-allocation charge for one retained raw hot page.
    pub(crate) fn declared_max_bytes(_descriptor: SeriesHotPageDescriptorV1) -> io::Result<u64> {
        charged_raw_bytes::<Self>(
            SERIES_HOT_PAGE_LEN_V1,
            "schema-7 hot-page declared charge overflows",
        )
    }

    /// Validates borrowed bytes and copies them into an exact raw cache value.
    pub(crate) fn decode(
        header: SeriesHeaderV3,
        page_index: u32,
        descriptor: SeriesHotPageDescriptorV1,
        page_bytes: &[u8],
        chunk_file_lens: [u64; 2],
    ) -> io::Result<Self> {
        validate_hot_page_for_cache(header, page_index, descriptor, page_bytes, chunk_file_lens)?;
        let bytes = copy_to_boxed_slice(page_bytes, "schema-7 hot-page allocation failed")?;
        Ok(Self {
            header,
            page_index,
            descriptor,
            chunk_file_lens,
            bytes,
        })
    }

    /// Validates and consumes the governed raw read allocation without keeping
    /// a second decoded record vector in the cache value.
    pub(crate) fn decode_owned(
        header: SeriesHeaderV3,
        page_index: u32,
        descriptor: SeriesHotPageDescriptorV1,
        page_bytes: Vec<u8>,
        chunk_file_lens: [u64; 2],
    ) -> io::Result<Self> {
        validate_hot_page_for_cache(header, page_index, descriptor, &page_bytes, chunk_file_lens)?;
        Ok(Self {
            header,
            page_index,
            descriptor,
            chunk_file_lens,
            bytes: page_bytes.into_boxed_slice(),
        })
    }

    /// Returns the measured logical bytes owned by this cache value.
    pub(crate) fn charged_bytes(&self) -> io::Result<u64> {
        charged_raw_bytes::<Self>(self.bytes.len(), "schema-7 hot-page charge overflows")
    }

    fn record(&self, series_ref: u32) -> io::Result<SeriesHotV3> {
        let record_index = series_ref
            .checked_sub(self.descriptor.first_series_ref)
            .ok_or_else(|| invalid_data("schema-7 selected series ref precedes its hot page"))?;
        if record_index >= self.descriptor.record_count {
            return Err(invalid_data(
                "schema-7 selected series ref exceeds its hot page",
            ));
        }
        let record_offset = usize_from_u32(record_index, "hot record index")?
            .checked_mul(SERIES_HOT_RECORD_LEN_V3)
            .and_then(|offset| offset.checked_add(SERIES_HOT_PAGE_HEADER_LEN_V1))
            .ok_or_else(|| invalid_data("schema-7 hot record offset overflows"))?;
        let record_end = record_offset
            .checked_add(SERIES_HOT_RECORD_LEN_V3)
            .ok_or_else(|| invalid_data("schema-7 hot record end overflows"))?;
        let encoded = self
            .bytes
            .get(record_offset..record_end)
            .ok_or_else(|| invalid_data("schema-7 cached hot record is truncated"))?;
        SeriesHotV3::decode(
            encoded,
            SeriesHotV3Context::from_header(self.header, self.chunk_file_lens)?,
        )
    }
}

/// Cacheable intrinsically authenticated physical overflow blob.
///
/// Hot-record identity is intentionally not part of admission. Every hit and
/// miss is rebound to its authenticated hot record through
/// [`Self::validate_bound_context`] so cache flight order cannot change
/// corruption semantics.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ValidatedOverflowBlob {
    header: SeriesHeaderV3,
    root: ChunkOverflowRootV2,
    blob_facts: Schema7OverflowBlobFacts,
    blob_offset: u64,
    series_ref: u32,
    chunk_count: u32,
    kind_mask: u8,
    chunk_file_lens: [u64; 2],
    bytes: Box<[u8]>,
}

impl ValidatedOverflowBlob {
    /// Returns the exact final-allocation charge for one retained raw blob.
    pub(crate) fn declared_max_bytes(locator: ChunkOverflowBlobLocatorV1) -> io::Result<u64> {
        charged_raw_bytes::<Self>(
            usize_from_u32(locator.blob_len, "overflow blob length")?,
            "schema-7 overflow-blob declared charge overflows",
        )
    }

    /// Validates borrowed bytes and copies them into an exact raw cache value.
    pub(crate) fn decode_bound(
        blob_bytes: &[u8],
        header: SeriesHeaderV3,
        root: &ChunkOverflowRootV2,
        blob_facts: Schema7OverflowBlobFacts,
        planned: &PlannedSeries,
        chunk_file_lens: [u64; 2],
    ) -> io::Result<Self> {
        let locator = validate_expected_overflow_source(header, root, blob_facts, planned)?;
        let decoded = Self::decode_physical(
            blob_bytes,
            header,
            root,
            blob_facts,
            locator.blob_offset,
            chunk_file_lens,
        )?;
        decoded.validate_bound_context(header, root, blob_facts, planned, chunk_file_lens)?;
        Ok(decoded)
    }

    /// Validates borrowed physical bytes before allocating their exact raw
    /// cache representation.
    pub(crate) fn decode_physical(
        blob_bytes: &[u8],
        header: SeriesHeaderV3,
        root: &ChunkOverflowRootV2,
        blob_facts: Schema7OverflowBlobFacts,
        blob_offset: u64,
        chunk_file_lens: [u64; 2],
    ) -> io::Result<Self> {
        let identity = validate_physical_overflow_blob(
            header,
            root,
            blob_facts,
            blob_offset,
            blob_bytes,
            chunk_file_lens,
            |_| Ok(()),
        )?;
        let bytes = copy_to_boxed_slice(blob_bytes, "schema-7 overflow-blob allocation failed")?;
        Ok(Self {
            header,
            root: *root,
            blob_facts,
            blob_offset,
            series_ref: identity.series_ref,
            chunk_count: identity.chunk_count,
            kind_mask: identity.kind_mask,
            chunk_file_lens,
            bytes,
        })
    }

    /// Validates and consumes the governed raw read allocation without keeping
    /// a second decoded entry vector in the cache value.
    pub(crate) fn decode_bound_owned(
        blob_bytes: Vec<u8>,
        header: SeriesHeaderV3,
        root: &ChunkOverflowRootV2,
        blob_facts: Schema7OverflowBlobFacts,
        planned: &PlannedSeries,
        chunk_file_lens: [u64; 2],
    ) -> io::Result<Self> {
        let locator = validate_expected_overflow_source(header, root, blob_facts, planned)?;
        let decoded = Self::decode_physical_owned(
            blob_bytes,
            header,
            root,
            blob_facts,
            locator.blob_offset,
            chunk_file_lens,
        )?;
        decoded.validate_bound_context(header, root, blob_facts, planned, chunk_file_lens)?;
        Ok(decoded)
    }

    /// Validates and consumes one governed physical-range read without
    /// trusting identity copied from a hot record.
    pub(crate) fn decode_physical_owned(
        blob_bytes: Vec<u8>,
        header: SeriesHeaderV3,
        root: &ChunkOverflowRootV2,
        blob_facts: Schema7OverflowBlobFacts,
        blob_offset: u64,
        chunk_file_lens: [u64; 2],
    ) -> io::Result<Self> {
        let identity = validate_physical_overflow_blob(
            header,
            root,
            blob_facts,
            blob_offset,
            &blob_bytes,
            chunk_file_lens,
            |_| Ok(()),
        )?;
        Ok(Self {
            header,
            root: *root,
            blob_facts,
            blob_offset,
            series_ref: identity.series_ref,
            chunk_count: identity.chunk_count,
            kind_mask: identity.kind_mask,
            chunk_file_lens,
            bytes: blob_bytes.into_boxed_slice(),
        })
    }

    /// Returns the measured logical bytes owned by this cache value.
    pub(crate) fn charged_bytes(&self) -> io::Result<u64> {
        charged_raw_bytes::<Self>(self.bytes.len(), "schema-7 overflow-blob charge overflows")
    }

    /// Rebinds a cache hit to the authenticated hot record and roots expected
    /// by the current query. A mismatch means two schema-7 records alias the
    /// same physical cache range with incompatible identities.
    pub(crate) fn validate_bound_context(
        &self,
        header: SeriesHeaderV3,
        root: &ChunkOverflowRootV2,
        blob_facts: Schema7OverflowBlobFacts,
        planned: &PlannedSeries,
        chunk_file_lens: [u64; 2],
    ) -> io::Result<()> {
        let locator = validate_expected_overflow_source(header, root, blob_facts, planned)?;
        if self.header != header
            || self.root != *root
            || self.blob_facts != blob_facts
            || self.blob_offset != locator.blob_offset
            || self.bytes.len() != usize_from_u32(locator.blob_len, "overflow blob length")?
            || self.chunk_file_lens != chunk_file_lens
        {
            return Err(invalid_data(
                "schema-7 cached overflow blob decode context does not match its bound root",
            ));
        }
        if self.series_ref != locator.series_ref {
            return Err(invalid_data(
                "schema-7 overflow blob series_ref does not match hot record",
            ));
        }
        if self.chunk_count != locator.chunk_count {
            return Err(invalid_data(
                "schema-7 overflow blob chunk count does not match hot record",
            ));
        }
        if self.kind_mask != planned.kind_mask {
            return Err(invalid_data(
                "schema-7 overflow kind mask does not match its hot record",
            ));
        }
        Ok(())
    }
}

fn validate_hot_page_for_cache(
    header: SeriesHeaderV3,
    page_index: u32,
    descriptor: SeriesHotPageDescriptorV1,
    page_bytes: &[u8],
    chunk_file_lens: [u64; 2],
) -> io::Result<()> {
    visit_series_hot_page_v1(
        header,
        page_index,
        descriptor,
        page_bytes,
        chunk_file_lens,
        |_, record| {
            // Row bounds require authenticated cold keyset bytes and remain a
            // delayed-label check. The root count is available at admission.
            if record.keyset_id >= header.num_keysets {
                return Err(invalid_data(
                    "schema-7 hot record keyset ID is out of range",
                ));
            }
            Ok(())
        },
    )
}

/// Authenticates one complete hot page and plans a sorted-unique subset of its
/// series refs.
pub(crate) fn plan_schema7_hot_page(
    header: SeriesHeaderV3,
    page_facts: Schema7SeriesPageFacts,
    page_index: u32,
    descriptor: SeriesHotPageDescriptorV1,
    page_bytes: &[u8],
    chunk_file_lens: [u64; 2],
    selected_series_refs: &[u32],
) -> io::Result<Vec<PlannedSeries>> {
    let page = ValidatedSeriesHotPage::decode(
        header,
        page_index,
        descriptor,
        page_bytes,
        chunk_file_lens,
    )?;
    plan_schema7_decoded_hot_page(
        header,
        page_facts,
        page_index,
        descriptor,
        &page,
        chunk_file_lens,
        selected_series_refs,
    )
}

/// Plans selected series from an already authenticated physical hot-page cache
/// value, re-checking its root/descriptor identity on every use.
pub(crate) fn plan_schema7_decoded_hot_page(
    header: SeriesHeaderV3,
    page_facts: Schema7SeriesPageFacts,
    page_index: u32,
    descriptor: SeriesHotPageDescriptorV1,
    validated_page: &ValidatedSeriesHotPage,
    chunk_file_lens: [u64; 2],
    selected_series_refs: &[u32],
) -> io::Result<Vec<PlannedSeries>> {
    validate_series_page_facts(header, page_facts)?;
    if page_index >= page_facts.hot_page_count {
        return Err(invalid_data(
            "schema-7 planned hot page index is out of range",
        ));
    }
    validate_sorted_unique_page_refs(descriptor, selected_series_refs)?;
    if validated_page.header != header
        || validated_page.page_index != page_index
        || validated_page.descriptor != descriptor
        || validated_page.chunk_file_lens != chunk_file_lens
    {
        return Err(invalid_data(
            "schema-7 cached hot page decode context does not match the expected root",
        ));
    }
    if validated_page.bytes.len() != SERIES_HOT_PAGE_LEN_V1 {
        return Err(invalid_data(
            "schema-7 cached hot page does not have its exact physical length",
        ));
    }

    let mut planned = Vec::new();
    planned
        .try_reserve_exact(selected_series_refs.len())
        .map_err(|_| resource_error("schema-7 planned series allocation failed"))?;
    for &series_ref in selected_series_refs {
        let record = validated_page.record(series_ref)?;
        let chunks = match record.location {
            SeriesHotLocationV3::Inline(inline) => {
                ChunkLocatorSource::Inline(inline_locator(header, series_ref, inline)?)
            }
            SeriesHotLocationV3::Overflow(overflow) => ChunkLocatorSource::Overflow {
                locator: ChunkOverflowBlobLocatorV1 {
                    series_ref,
                    blob_offset: overflow.blob_offset,
                    blob_len: overflow.blob_len,
                    chunk_count: overflow.chunk_count,
                },
                expected_kind_mask: record.kind_mask,
            },
        };
        planned.push(PlannedSeries {
            series_ref,
            kind_mask: record.kind_mask,
            cold_labels: ColdLabelRowLocator {
                keyset_id: record.keyset_id,
                row: record.row,
            },
            chunks,
            expected_label_identity: record.series_id,
        });
    }
    Ok(planned)
}

/// Authenticates and maps the touched overflow blob referenced by one planned
/// series into a flat schema-neutral locator batch.
pub(crate) fn plan_schema7_overflow_blob(
    header: SeriesHeaderV3,
    root: &ChunkOverflowRootV2,
    blob_facts: Schema7OverflowBlobFacts,
    planned: &PlannedSeries,
    blob_bytes: &[u8],
    chunk_file_lens: [u64; 2],
) -> io::Result<FlatChunkLocatorBatch> {
    let blob = ValidatedOverflowBlob::decode_bound(
        blob_bytes,
        header,
        root,
        blob_facts,
        planned,
        chunk_file_lens,
    )?;
    plan_schema7_decoded_overflow_blob(header, root, blob_facts, planned, &blob, chunk_file_lens)
}

/// Maps an already authenticated physical overflow-blob cache value, re-
/// checking root, hot-record, and integration invariants on every use.
pub(crate) fn plan_schema7_decoded_overflow_blob(
    header: SeriesHeaderV3,
    root: &ChunkOverflowRootV2,
    blob_facts: Schema7OverflowBlobFacts,
    planned: &PlannedSeries,
    validated_blob: &ValidatedOverflowBlob,
    chunk_file_lens: [u64; 2],
) -> io::Result<FlatChunkLocatorBatch> {
    validated_blob.validate_bound_context(header, root, blob_facts, planned, chunk_file_lens)?;
    let locator = validate_expected_overflow_source(header, root, blob_facts, planned)?;

    let mut locators = Vec::new();
    locators
        .try_reserve_exact(usize_from_u32(locator.chunk_count, "overflow chunk count")?)
        .map_err(|_| resource_error("schema-7 overflow locator allocation failed"))?;
    let identity = validate_physical_overflow_blob(
        header,
        root,
        blob_facts,
        locator.blob_offset,
        &validated_blob.bytes,
        chunk_file_lens,
        |entry| {
            locators.push(indexed_locator(planned.series_ref, &entry)?);
            Ok(())
        },
    )?;
    if identity.series_ref != planned.series_ref
        || identity.chunk_count != locator.chunk_count
        || identity.kind_mask != planned.kind_mask
    {
        return Err(invalid_data(
            "schema-7 overflow blob identity changed after cache binding",
        ));
    }

    let locator_count = u32::try_from(locators.len())
        .map_err(|_| invalid_data("schema-7 overflow locator count exceeds u32"))?;
    let mut series_spans = Vec::new();
    series_spans
        .try_reserve_exact(1)
        .map_err(|_| resource_error("schema-7 overflow span allocation failed"))?;
    series_spans.push(SeriesChunkSpan {
        series_ref: planned.series_ref,
        start: 0,
        len: locator_count,
    });
    Ok(FlatChunkLocatorBatch {
        locators,
        series_spans,
    })
}

fn validate_expected_overflow_source(
    header: SeriesHeaderV3,
    root: &ChunkOverflowRootV2,
    blob_facts: Schema7OverflowBlobFacts,
    planned: &PlannedSeries,
) -> io::Result<ChunkOverflowBlobLocatorV1> {
    validate_overflow_facts(header, root, blob_facts)?;
    if planned.series_ref >= header.num_series {
        return Err(invalid_data(
            "schema-7 planned overflow series ref is out of range",
        ));
    }
    let ChunkLocatorSource::Overflow {
        locator,
        expected_kind_mask,
    } = &planned.chunks
    else {
        return Err(invalid_data(
            "schema-7 planned series does not reference an overflow blob",
        ));
    };
    if locator.series_ref != planned.series_ref || *expected_kind_mask != planned.kind_mask {
        return Err(invalid_data(
            "schema-7 planned overflow source does not match its series",
        ));
    }
    let blob_end = locator
        .blob_offset
        .checked_add(u64::from(locator.blob_len))
        .ok_or_else(|| invalid_data("schema-7 planned overflow blob range overflows"))?;
    let facts_end = blob_facts
        .blobs_offset
        .checked_add(blob_facts.blobs_len)
        .ok_or_else(|| invalid_data("schema-7 expected overflow blob region overflows"))?;
    if locator.blob_offset < blob_facts.blobs_offset || blob_end > facts_end {
        return Err(invalid_data(
            "schema-7 planned overflow blob exceeds expected blob facts",
        ));
    }
    Ok(*locator)
}

#[derive(Clone, Copy)]
struct PhysicalOverflowIdentity {
    series_ref: u32,
    chunk_count: u32,
    kind_mask: u8,
}

fn validate_physical_overflow_blob<F>(
    header: SeriesHeaderV3,
    root: &ChunkOverflowRootV2,
    blob_facts: Schema7OverflowBlobFacts,
    blob_offset: u64,
    blob_bytes: &[u8],
    chunk_file_lens: [u64; 2],
    mut visit: F,
) -> io::Result<PhysicalOverflowIdentity>
where
    F: FnMut(OverflowChunkEntryV1) -> io::Result<()>,
{
    validate_overflow_facts(header, root, blob_facts)?;
    let blob_len = u64::try_from(blob_bytes.len())
        .map_err(|_| invalid_data("schema-7 overflow blob length exceeds u64"))?;
    let blob_end = blob_offset
        .checked_add(blob_len)
        .ok_or_else(|| invalid_data("schema-7 overflow blob range overflows"))?;
    let facts_end = blob_facts
        .blobs_offset
        .checked_add(blob_facts.blobs_len)
        .ok_or_else(|| invalid_data("schema-7 expected overflow blob region overflows"))?;
    if blob_offset < blob_facts.blobs_offset || blob_end > facts_end {
        return Err(invalid_data(
            "schema-7 physical overflow blob exceeds expected blob facts",
        ));
    }

    let mut decoded_kind_mask = 0u8;
    let mut sole_locator = None;
    let mut entry_count = 0u32;
    let facts =
        visit_physical_chunk_overflow_blob_v1(blob_bytes, root, blob_offset, |facts, entry| {
            validate_overflow_entry(header, &entry, chunk_file_lens)?;
            decoded_kind_mask |= kind_bit(entry.kind);
            let decoded = indexed_locator(facts.series_ref, &entry)?;
            entry_count = entry_count
                .checked_add(1)
                .ok_or_else(|| invalid_data("schema-7 decoded overflow entry count overflows"))?;
            if facts.chunk_count == 1 {
                sole_locator = Some(decoded);
            }
            visit(entry)
        })?;
    if entry_count != facts.chunk_count {
        return Err(invalid_data(
            "schema-7 decoded overflow entry count changed during validation",
        ));
    }
    if sole_locator
        .is_some_and(|locator| chunk_entry_fits_inline(header.segment_start_ms, locator.entry()))
    {
        return Err(invalid_data(
            "schema-7 one-chunk overflow blob is noncanonical",
        ));
    }
    Ok(PhysicalOverflowIdentity {
        series_ref: facts.series_ref,
        chunk_count: facts.chunk_count,
        kind_mask: decoded_kind_mask,
    })
}

fn inline_locator(
    header: SeriesHeaderV3,
    series_ref: u32,
    inline: InlineChunkV3,
) -> io::Result<IndexedChunkLocator> {
    let kind = chunk_kind(inline.chunk_kind)?;
    let min_time_ms = header
        .segment_start_ms
        .checked_add(u64::from(inline.min_time_delta_ms))
        .ok_or_else(|| invalid_data("schema-7 inline minimum time overflows"))?;
    let max_time_ms = header
        .segment_start_ms
        .checked_add(u64::from(inline.max_time_delta_ms))
        .ok_or_else(|| invalid_data("schema-7 inline maximum time overflows"))?;
    IndexedChunkLocator::try_schema7(
        series_ref,
        ChunkIndexEntry {
            file_id: inline.file_id,
            kind,
            flags: 0,
            min_time_ms,
            max_time_ms,
            offset: u64::from(inline.file_offset),
            length: inline.chunk_length,
            scalar_lane_offset: inline.scalar_lane_offset(),
            scalar_lane_len: inline.scalar_lane_len,
        },
        Some(inline.indexed_prefix_crc32c),
    )
    .map_err(locator_error)
}

fn indexed_locator(
    series_ref: u32,
    entry: &OverflowChunkEntryV1,
) -> io::Result<IndexedChunkLocator> {
    IndexedChunkLocator::try_schema7(
        series_ref,
        ChunkIndexEntry {
            file_id: entry.file_id,
            kind: entry.kind,
            flags: 0,
            min_time_ms: entry.min_time_ms,
            max_time_ms: entry.max_time_ms,
            offset: entry.offset,
            length: entry.length,
            scalar_lane_offset: entry.scalar_lane_offset,
            scalar_lane_len: entry.scalar_lane_len,
        },
        Some(entry.indexed_prefix_crc32c),
    )
    .map_err(locator_error)
}

fn validate_series_page_facts(
    header: SeriesHeaderV3,
    facts: Schema7SeriesPageFacts,
) -> io::Result<()> {
    if facts.root_len != header.hot_pages_offset
        || facts.hot_page_count != header.page_count
        || facts.hot_pages_offset != header.hot_pages_offset
        || facts.hot_pages_len != header.hot_pages_len
        || facts.cold_page_count != header.cold_page_count
        || facts.cold_pages_offset != header.keysets_offset
        || facts.cold_pages_len != header.cold_bytes_len()?
        || facts.file_len != header.file_len
    {
        return Err(invalid_data(
            "schema-7 expected series page facts do not match the root",
        ));
    }
    Ok(())
}

fn validate_overflow_facts(
    header: SeriesHeaderV3,
    root: &ChunkOverflowRootV2,
    facts: Schema7OverflowBlobFacts,
) -> io::Result<()> {
    if header.num_series != root.series_count
        || header.chunk_index_file_len != root.file_len
        || header.chunk_index_root_crc32c != root.root_crc32c
        || facts.root_len != 64
        || facts.blob_count != root.blob_count
        || facts.blobs_offset != 64
        || facts.blobs_len != root.blobs_len
        || facts.file_len != root.file_len
    {
        return Err(invalid_data(
            "schema-7 expected overflow blob facts do not match the bound roots",
        ));
    }
    Ok(())
}

fn validate_sorted_unique_page_refs(
    descriptor: SeriesHotPageDescriptorV1,
    refs: &[u32],
) -> io::Result<()> {
    let page_end = descriptor
        .first_series_ref
        .checked_add(descriptor.record_count)
        .ok_or_else(|| invalid_data("schema-7 expected hot page range overflows"))?;
    let mut previous = None;
    for &series_ref in refs {
        if series_ref < descriptor.first_series_ref || series_ref >= page_end {
            return Err(invalid_data(
                "schema-7 selected series ref does not belong to its hot page",
            ));
        }
        if previous.is_some_and(|previous| series_ref <= previous) {
            return Err(invalid_data(
                "schema-7 selected series refs are not sorted and unique",
            ));
        }
        previous = Some(series_ref);
    }
    Ok(())
}

fn validate_overflow_entry(
    header: SeriesHeaderV3,
    entry: &OverflowChunkEntryV1,
    chunk_file_lens: [u64; 2],
) -> io::Result<()> {
    if entry.min_time_ms < header.segment_start_ms
        || entry.min_time_ms > entry.max_time_ms
        || entry.max_time_ms >= header.segment_end_ms
    {
        return Err(invalid_data(
            "schema-7 overflow chunk time range is outside its segment",
        ));
    }
    let file_index = usize::from(entry.file_id);
    let file_len = chunk_file_lens
        .get(file_index)
        .copied()
        .ok_or_else(|| invalid_data("schema-7 overflow chunk file ID is invalid"))?;
    let chunk_end = entry
        .offset
        .checked_add(u64::from(entry.length))
        .ok_or_else(|| invalid_data("schema-7 overflow chunk file range overflows"))?;
    if chunk_end > file_len {
        return Err(invalid_data(
            "schema-7 overflow chunk file range is out of bounds",
        ));
    }
    Ok(())
}

fn chunk_kind(value: u8) -> io::Result<ChunkKind> {
    match value {
        value if value == ChunkKind::Float as u8 => Ok(ChunkKind::Float),
        value if value == ChunkKind::Int64 as u8 => Ok(ChunkKind::Int64),
        value if value == ChunkKind::Histogram as u8 => Ok(ChunkKind::Histogram),
        value if value == ChunkKind::ExponentialHistogram as u8 => {
            Ok(ChunkKind::ExponentialHistogram)
        }
        value if value == ChunkKind::Summary as u8 => Ok(ChunkKind::Summary),
        _ => Err(invalid_data(
            "schema-7 planned inline chunk kind is invalid",
        )),
    }
}

fn kind_bit(kind: ChunkKind) -> u8 {
    1u8 << kind as u8
}

fn usize_from_u32(value: u32, what: &'static str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("schema-7 {what} exceeds usize"),
        )
    })
}

fn charged_raw_bytes<Owner>(length: usize, message: &'static str) -> io::Result<u64> {
    let bytes = std::mem::size_of::<Owner>()
        .checked_add(length)
        .ok_or_else(|| resource_error(message))?;
    u64::try_from(bytes).map_err(|_| resource_error(message))
}

fn copy_to_boxed_slice(bytes: &[u8], message: &'static str) -> io::Result<Box<[u8]>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| resource_error(message))?;
    owned.extend_from_slice(bytes);
    Ok(owned.into_boxed_slice())
}

fn locator_error(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn resource_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::OutOfMemory, message)
}

#[cfg(test)]
mod tests {
    use crc32c::crc32c;

    use crate::storage::chunk::{
        CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN, CHUNK_OVERFLOW_ROOT_V2_LEN, ChunkOverflowBlobV1,
        EncodedChunkIndexV2, IndexedChunkAuthentication, encode_chunk_index_v2,
    };

    use super::super::{
        InlineChunkV3, SERIES_HOT_SCALAR_LANE_LEN_MAX, SeriesHeaderV3Params, SeriesHotV3,
        encode_series_hot_page_v1,
    };
    use super::*;

    const SEGMENT_START_MS: u64 = 1_000;
    const SEGMENT_END_MS: u64 = SEGMENT_START_MS + u32::MAX as u64 + 100;
    const CHUNK_FILE_LENS: [u64; 2] = [u32::MAX as u64 + 10_000_000, u32::MAX as u64 + 10_000_000];

    fn kind_mask(kinds: impl IntoIterator<Item = ChunkKind>) -> u8 {
        kinds
            .into_iter()
            .fold(0, |mask, kind| mask | kind_bit(kind))
    }

    fn header_for_index(series_count: u32, index: &EncodedChunkIndexV2) -> SeriesHeaderV3 {
        header_for_index_with_bounds(series_count, index, SEGMENT_START_MS, SEGMENT_END_MS)
    }

    fn header_for_index_with_bounds(
        series_count: u32,
        index: &EncodedChunkIndexV2,
        segment_start_ms: u64,
        segment_end_ms: u64,
    ) -> SeriesHeaderV3 {
        SeriesHeaderV3::new(SeriesHeaderV3Params {
            num_series: series_count,
            num_keysets: series_count.min(3),
            num_value_dicts: u32::from(series_count != 0),
            chunk_index_root_crc32c: index.root.root_crc32c,
            keysets_len: if series_count == 0 { 8 } else { 32 },
            value_dicts_len: if series_count == 0 { 8 } else { 16 },
            keyset_blocks_len: if series_count == 0 { 8 } else { 32 },
            segment_start_ms,
            segment_end_ms,
            chunk_index_file_len: index.root.file_len,
        })
        .unwrap()
    }

    fn series_page_facts(header: SeriesHeaderV3) -> Schema7SeriesPageFacts {
        Schema7SeriesPageFacts {
            root_len: header.hot_pages_offset,
            hot_page_count: header.page_count,
            hot_pages_offset: header.hot_pages_offset,
            hot_pages_len: header.hot_pages_len,
            cold_page_count: header.cold_page_count,
            cold_pages_offset: header.keysets_offset,
            cold_pages_len: header.cold_bytes_len().unwrap(),
            file_len: header.file_len,
        }
    }

    fn overflow_blob_facts(root: ChunkOverflowRootV2) -> Schema7OverflowBlobFacts {
        Schema7OverflowBlobFacts {
            root_len: CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
            blob_count: root.blob_count,
            blobs_offset: CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
            blobs_len: root.blobs_len,
            file_len: root.file_len,
        }
    }

    fn inline_record(
        series_ref: u32,
        kind: ChunkKind,
        file_id: u8,
        scalar_lane_len: u32,
        indexed_prefix_crc32c: u32,
    ) -> SeriesHotV3 {
        let delta = series_ref;
        SeriesHotV3 {
            series_id: 10_000 + u64::from(series_ref),
            keyset_id: series_ref % 3,
            row: 20_000 + series_ref,
            kind_mask: kind_bit(kind),
            location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                chunk_kind: kind as u8,
                file_id,
                scalar_lane_len,
                min_time_delta_ms: delta,
                max_time_delta_ms: delta + 1,
                file_offset: series_ref * 64,
                chunk_length: 48 + scalar_lane_len,
                indexed_prefix_crc32c,
            }),
        }
    }

    fn page_records(first_series_ref: u32, count: u32) -> Vec<SeriesHotV3> {
        (first_series_ref..first_series_ref + count)
            .map(|series_ref| {
                inline_record(series_ref, ChunkKind::Float, 0, 0, 0x8000_0000 | series_ref)
            })
            .collect()
    }

    fn assert_invalid(error: io::Error, expected: &str) {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn plans_boundary_refs_and_preserves_exact_inline_locator_facts() {
        let index = encode_chunk_index_v2(411, &[]).unwrap();
        let header = header_for_index(411, &index);
        let facts = series_page_facts(header);

        let mut page0_records = page_records(0, 409);
        page0_records[0] = inline_record(0, ChunkKind::Float, 0, 0, 0);
        page0_records[408] = inline_record(408, ChunkKind::Histogram, 1, 16, 0xaabb_ccdd);
        let (page0_descriptor, page0_bytes) =
            encode_series_hot_page_v1(header, 0, &page0_records, CHUNK_FILE_LENS).unwrap();
        let page0 = plan_schema7_hot_page(
            header,
            facts,
            0,
            page0_descriptor,
            &page0_bytes,
            CHUNK_FILE_LENS,
            &[0, 408],
        )
        .unwrap();
        assert_eq!(page0.len(), 2);
        assert_eq!(page0[0].series_ref, 0);
        assert_eq!(page0[0].cold_labels.keyset_id, 0);
        assert_eq!(page0[0].cold_labels.row, 20_000);
        let ChunkLocatorSource::Inline(first) = &page0[0].chunks else {
            panic!("expected inline locator");
        };
        assert_eq!(first.series_ref(), 0);
        assert_eq!(first.entry().file_id, 0);
        assert_eq!(first.entry().kind, ChunkKind::Float);
        assert_eq!(first.entry().min_time_ms, SEGMENT_START_MS);
        assert_eq!(first.entry().max_time_ms, SEGMENT_START_MS + 1);
        assert_eq!(first.entry().offset, 0);
        assert_eq!(first.entry().length, 48);
        assert_eq!(first.entry().scalar_lane_offset, 0);
        assert_eq!(first.entry().scalar_lane_len, 0);
        assert_eq!(
            first.authentication(),
            IndexedChunkAuthentication::Schema7 {
                indexed_prefix_crc32c: 0
            }
        );

        let ChunkLocatorSource::Inline(last) = &page0[1].chunks else {
            panic!("expected inline locator");
        };
        assert_eq!(last.series_ref(), 408);
        assert_eq!(last.entry().file_id, 1);
        assert_eq!(last.entry().kind, ChunkKind::Histogram);
        assert_eq!(last.entry().min_time_ms, SEGMENT_START_MS + 408);
        assert_eq!(last.entry().max_time_ms, SEGMENT_START_MS + 409);
        assert_eq!(last.entry().offset, 408 * 64);
        assert_eq!(last.entry().length, 64);
        assert_eq!(last.entry().scalar_lane_offset, 40);
        assert_eq!(last.entry().scalar_lane_len, 16);
        assert_eq!(
            last.authentication(),
            IndexedChunkAuthentication::Schema7 {
                indexed_prefix_crc32c: 0xaabb_ccdd
            }
        );

        let page1_records = vec![
            inline_record(409, ChunkKind::Int64, 0, 0, 9),
            inline_record(410, ChunkKind::ExponentialHistogram, 1, 24, 10),
        ];
        let (page1_descriptor, page1_bytes) =
            encode_series_hot_page_v1(header, 1, &page1_records, CHUNK_FILE_LENS).unwrap();
        let decoded_page1 = ValidatedSeriesHotPage::decode(
            header,
            1,
            page1_descriptor,
            &page1_bytes,
            CHUNK_FILE_LENS,
        )
        .unwrap();
        let page1 = plan_schema7_decoded_hot_page(
            header,
            facts,
            1,
            page1_descriptor,
            &decoded_page1,
            CHUNK_FILE_LENS,
            &[409, 410],
        )
        .unwrap();
        assert_eq!(
            page1
                .iter()
                .map(|series| series.series_ref)
                .collect::<Vec<_>>(),
            [409, 410]
        );
        let ChunkLocatorSource::Inline(last) = &page1[1].chunks else {
            panic!("expected inline locator");
        };
        assert_eq!(last.entry().file_id, 1);
        assert_eq!(last.entry().kind, ChunkKind::ExponentialHistogram);
        assert_eq!(last.entry().scalar_lane_offset, 40);
        assert_eq!(last.entry().scalar_lane_len, 24);
    }

    #[test]
    fn hot_page_selection_requires_sorted_unique_refs_from_exact_page() {
        let index = encode_chunk_index_v2(411, &[]).unwrap();
        let header = header_for_index(411, &index);
        let facts = series_page_facts(header);
        let records = page_records(0, 409);
        let (descriptor, bytes) =
            encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();
        let plan = |refs: &[u32]| {
            plan_schema7_hot_page(header, facts, 0, descriptor, &bytes, CHUNK_FILE_LENS, refs)
        };

        assert!(plan(&[]).unwrap().is_empty());
        assert_invalid(plan(&[408, 0]).unwrap_err(), "not sorted and unique");
        assert_invalid(plan(&[0, 0]).unwrap_err(), "not sorted and unique");
        assert_invalid(plan(&[409]).unwrap_err(), "does not belong");
    }

    #[test]
    fn hot_page_authentication_and_bound_facts_are_mandatory() {
        let index = encode_chunk_index_v2(1, &[]).unwrap();
        let header = header_for_index(1, &index);
        let facts = series_page_facts(header);
        let records = page_records(0, 1);
        let (descriptor, bytes) =
            encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();

        let mut corrupt = bytes.clone();
        corrupt[100] ^= 1;
        assert_invalid(
            plan_schema7_hot_page(
                header,
                facts,
                0,
                descriptor,
                &corrupt,
                CHUNK_FILE_LENS,
                &[0],
            )
            .unwrap_err(),
            "CRC mismatch",
        );

        let mut substituted_facts = facts;
        substituted_facts.hot_pages_offset += 1;
        assert_invalid(
            plan_schema7_hot_page(
                header,
                substituted_facts,
                0,
                descriptor,
                &bytes,
                CHUNK_FILE_LENS,
                &[0],
            )
            .unwrap_err(),
            "page facts do not match",
        );

        let cached_page =
            ValidatedSeriesHotPage::decode(header, 0, descriptor, &bytes, CHUNK_FILE_LENS).unwrap();
        let substituted_descriptor = SeriesHotPageDescriptorV1 {
            page_crc32c: descriptor.page_crc32c ^ 1,
            ..descriptor
        };
        assert_invalid(
            plan_schema7_decoded_hot_page(
                header,
                facts,
                0,
                substituted_descriptor,
                &cached_page,
                CHUNK_FILE_LENS,
                &[0],
            )
            .unwrap_err(),
            "decode context does not match",
        );
    }

    #[test]
    fn hot_page_cache_admission_rejects_impossible_keysets_and_measures_owned_bytes() {
        let index = encode_chunk_index_v2(1, &[]).unwrap();
        let header = header_for_index(1, &index);
        let records = page_records(0, 1);
        let (descriptor, bytes) =
            encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();
        let page =
            ValidatedSeriesHotPage::decode(header, 0, descriptor, &bytes, CHUNK_FILE_LENS).unwrap();
        let expected_charge =
            std::mem::size_of::<ValidatedSeriesHotPage>() + SERIES_HOT_PAGE_LEN_V1;
        assert_eq!(page.charged_bytes().unwrap(), expected_charge as u64);
        assert_eq!(
            page.charged_bytes().unwrap(),
            ValidatedSeriesHotPage::declared_max_bytes(descriptor).unwrap()
        );
        let owned_page = ValidatedSeriesHotPage::decode_owned(
            header,
            0,
            descriptor,
            bytes.clone(),
            CHUNK_FILE_LENS,
        )
        .unwrap();
        assert_eq!(owned_page, page);

        let mut bad_records = records;
        bad_records[0].keyset_id = header.num_keysets;
        let (bad_descriptor, bad_bytes) =
            encode_series_hot_page_v1(header, 0, &bad_records, CHUNK_FILE_LENS).unwrap();
        assert_invalid(
            ValidatedSeriesHotPage::decode(header, 0, bad_descriptor, &bad_bytes, CHUNK_FILE_LENS)
                .unwrap_err(),
            "keyset ID is out of range",
        );
    }

    #[test]
    fn hot_page_cache_hit_rejects_valid_header_and_file_length_substitution() {
        let index = encode_chunk_index_v2(1, &[]).unwrap();
        let header = header_for_index(1, &index);
        let facts = series_page_facts(header);
        let records = page_records(0, 1);
        let (descriptor, bytes) =
            encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS).unwrap();
        let cached_page =
            ValidatedSeriesHotPage::decode(header, 0, descriptor, &bytes, CHUNK_FILE_LENS).unwrap();

        let substituted_header =
            header_for_index_with_bounds(1, &index, SEGMENT_START_MS + 10, SEGMENT_END_MS + 10);
        let substituted_facts = series_page_facts(substituted_header);
        assert!(
            ValidatedSeriesHotPage::decode(
                substituted_header,
                0,
                descriptor,
                &bytes,
                CHUNK_FILE_LENS,
            )
            .is_ok(),
            "the same descriptor/page bytes are independently valid under the substituted header"
        );
        assert_invalid(
            plan_schema7_decoded_hot_page(
                substituted_header,
                substituted_facts,
                0,
                descriptor,
                &cached_page,
                CHUNK_FILE_LENS,
                &[0],
            )
            .unwrap_err(),
            "decode context does not match",
        );

        let substituted_chunk_file_lens = [CHUNK_FILE_LENS[0] - 1, CHUNK_FILE_LENS[1]];
        assert!(
            ValidatedSeriesHotPage::decode(
                header,
                0,
                descriptor,
                &bytes,
                substituted_chunk_file_lens,
            )
            .is_ok(),
            "the same descriptor/page bytes are independently valid under both file inventories"
        );
        assert_invalid(
            plan_schema7_decoded_hot_page(
                header,
                facts,
                0,
                descriptor,
                &cached_page,
                substituted_chunk_file_lens,
                &[0],
            )
            .unwrap_err(),
            "decode context does not match",
        );
    }

    fn overflow_entry(
        file_id: u8,
        kind: ChunkKind,
        min_time_ms: u64,
        offset: u64,
        scalar_lane_len: u32,
        indexed_prefix_crc32c: u32,
    ) -> OverflowChunkEntryV1 {
        OverflowChunkEntryV1 {
            file_id,
            kind,
            min_time_ms,
            max_time_ms: min_time_ms + 1,
            offset,
            length: 48 + scalar_lane_len,
            scalar_lane_offset: u32::from(scalar_lane_len != 0) * 40,
            scalar_lane_len,
            indexed_prefix_crc32c,
        }
    }

    struct OverflowFixture {
        header: SeriesHeaderV3,
        root: ChunkOverflowRootV2,
        facts: Schema7OverflowBlobFacts,
        planned: PlannedSeries,
        blob_bytes: Vec<u8>,
    }

    fn overflow_fixture(
        series_count: u32,
        series_ref: u32,
        entries: Vec<OverflowChunkEntryV1>,
    ) -> OverflowFixture {
        let mask = kind_mask(entries.iter().map(|entry| entry.kind));
        let encoded = encode_chunk_index_v2(
            series_count,
            &[ChunkOverflowBlobV1 {
                series_ref,
                entries,
            }],
        )
        .unwrap();
        let locator = encoded.blob_locators[0];
        let blob_start = usize::try_from(locator.blob_offset).unwrap();
        let blob_end = blob_start + usize::try_from(locator.blob_len).unwrap();
        let root = encoded.root;
        OverflowFixture {
            header: header_for_index(series_count, &encoded),
            root,
            facts: overflow_blob_facts(root),
            planned: PlannedSeries {
                series_ref,
                kind_mask: mask,
                cold_labels: ColdLabelRowLocator {
                    keyset_id: 3,
                    row: 5,
                },
                chunks: ChunkLocatorSource::Overflow {
                    locator,
                    expected_kind_mask: mask,
                },
                expected_label_identity: 55,
            },
            blob_bytes: encoded.bytes[blob_start..blob_end].to_vec(),
        }
    }

    #[test]
    fn overflow_mapping_preserves_stored_order_files_scalars_kinds_and_authentication() {
        let entries = vec![
            overflow_entry(0, ChunkKind::Float, 1_010, 64, 0, 0),
            overflow_entry(0, ChunkKind::Histogram, 1_020, 128, 16, 0x1111_2222),
            overflow_entry(
                1,
                ChunkKind::ExponentialHistogram,
                1_030,
                256,
                24,
                0x3333_4444,
            ),
        ];
        let fixture = overflow_fixture(16, 7, entries);
        let decoded_blob = ValidatedOverflowBlob::decode_bound(
            &fixture.blob_bytes,
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            CHUNK_FILE_LENS,
        )
        .unwrap();
        let expected_charge =
            std::mem::size_of::<ValidatedOverflowBlob>() + fixture.blob_bytes.len();
        assert_eq!(
            decoded_blob.charged_bytes().unwrap(),
            expected_charge as u64
        );
        let ChunkLocatorSource::Overflow { locator, .. } = &fixture.planned.chunks else {
            unreachable!()
        };
        assert_eq!(
            decoded_blob.charged_bytes().unwrap(),
            ValidatedOverflowBlob::declared_max_bytes(*locator).unwrap()
        );
        let owned_blob = ValidatedOverflowBlob::decode_bound_owned(
            fixture.blob_bytes.clone(),
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            CHUNK_FILE_LENS,
        )
        .unwrap();
        assert_eq!(owned_blob, decoded_blob);
        let batch = plan_schema7_decoded_overflow_blob(
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            &decoded_blob,
            CHUNK_FILE_LENS,
        )
        .unwrap();

        assert_eq!(
            batch.series_spans,
            [SeriesChunkSpan {
                series_ref: 7,
                start: 0,
                len: 3,
            }]
        );
        assert_eq!(batch.locators.len(), 3);
        assert_eq!(batch.locators[0].entry().file_id, 0);
        assert_eq!(batch.locators[0].entry().kind, ChunkKind::Float);
        assert_eq!(batch.locators[0].entry().min_time_ms, 1_010);
        assert_eq!(batch.locators[0].entry().offset, 64);
        assert_eq!(batch.locators[0].entry().scalar_lane_offset, 0);
        assert_eq!(batch.locators[0].entry().scalar_lane_len, 0);
        assert_eq!(
            batch.locators[0].authentication(),
            IndexedChunkAuthentication::Schema7 {
                indexed_prefix_crc32c: 0,
            }
        );
        assert_eq!(batch.locators[1].entry().kind, ChunkKind::Histogram);
        assert_eq!(batch.locators[1].entry().scalar_lane_offset, 40);
        assert_eq!(batch.locators[1].entry().scalar_lane_len, 16);
        assert_eq!(batch.locators[2].entry().file_id, 1);
        assert_eq!(
            batch.locators[2].entry().kind,
            ChunkKind::ExponentialHistogram
        );
        assert_eq!(batch.locators[2].entry().scalar_lane_len, 24);
    }

    #[test]
    fn overflow_rejects_segment_file_and_kind_mask_mismatches() {
        let before_segment = overflow_fixture(
            2,
            0,
            vec![
                overflow_entry(0, ChunkKind::Float, SEGMENT_START_MS - 1, 0, 0, 1),
                overflow_entry(0, ChunkKind::Float, SEGMENT_START_MS + 1, 64, 0, 2),
            ],
        );
        assert_invalid(
            ValidatedOverflowBlob::decode_bound(
                &before_segment.blob_bytes,
                before_segment.header,
                &before_segment.root,
                before_segment.facts,
                &before_segment.planned,
                CHUNK_FILE_LENS,
            )
            .unwrap_err(),
            "outside its segment",
        );

        let file_bounds = overflow_fixture(
            2,
            0,
            vec![
                overflow_entry(0, ChunkKind::Float, SEGMENT_START_MS, 0, 0, 1),
                overflow_entry(1, ChunkKind::Float, SEGMENT_START_MS + 1, 64, 0, 2),
            ],
        );
        assert_invalid(
            plan_schema7_overflow_blob(
                file_bounds.header,
                &file_bounds.root,
                file_bounds.facts,
                &file_bounds.planned,
                &file_bounds.blob_bytes,
                [1_000, 100],
            )
            .unwrap_err(),
            "file range is out of bounds",
        );

        let mut wrong_mask = overflow_fixture(
            2,
            0,
            vec![
                overflow_entry(0, ChunkKind::Float, SEGMENT_START_MS, 0, 0, 1),
                overflow_entry(0, ChunkKind::Int64, SEGMENT_START_MS + 1, 64, 0, 2),
            ],
        );
        wrong_mask.planned.kind_mask = kind_bit(ChunkKind::Float);
        let ChunkLocatorSource::Overflow {
            expected_kind_mask, ..
        } = &mut wrong_mask.planned.chunks
        else {
            unreachable!()
        };
        *expected_kind_mask = wrong_mask.planned.kind_mask;
        assert_invalid(
            ValidatedOverflowBlob::decode_bound(
                &wrong_mask.blob_bytes,
                wrong_mask.header,
                &wrong_mask.root,
                wrong_mask.facts,
                &wrong_mask.planned,
                CHUNK_FILE_LENS,
            )
            .unwrap_err(),
            "kind mask does not match",
        );
    }

    #[test]
    fn overflow_rejects_crc_order_identity_and_bound_fact_substitution() {
        let entries = vec![
            overflow_entry(0, ChunkKind::Float, 1_010, 64, 0, 1),
            overflow_entry(0, ChunkKind::Float, 1_020, 128, 0, 2),
        ];
        let fixture = overflow_fixture(4, 0, entries.clone());

        let mut corrupt = fixture.blob_bytes.clone();
        corrupt[CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN + 10] ^= 1;
        assert_invalid(
            plan_schema7_overflow_blob(
                fixture.header,
                &fixture.root,
                fixture.facts,
                &fixture.planned,
                &corrupt,
                CHUNK_FILE_LENS,
            )
            .unwrap_err(),
            "crc mismatch",
        );

        let mut reversed = fixture.blob_bytes.clone();
        let first = CHUNK_OVERFLOW_BLOB_V1_HEADER_LEN;
        let second = first + 44;
        for index in 0..44 {
            reversed.swap(first + index, second + index);
        }
        refresh_blob_crc(&mut reversed);
        assert_invalid(
            plan_schema7_overflow_blob(
                fixture.header,
                &fixture.root,
                fixture.facts,
                &fixture.planned,
                &reversed,
                CHUNK_FILE_LENS,
            )
            .unwrap_err(),
            "not strictly ordered",
        );

        let substituted = overflow_fixture(4, 1, entries);
        assert_eq!(fixture.blob_bytes.len(), substituted.blob_bytes.len());
        assert_invalid(
            plan_schema7_overflow_blob(
                fixture.header,
                &fixture.root,
                fixture.facts,
                &fixture.planned,
                &substituted.blob_bytes,
                CHUNK_FILE_LENS,
            )
            .unwrap_err(),
            "series_ref does not match",
        );

        let mut wrong_facts = fixture.facts;
        wrong_facts.blobs_len -= 1;
        assert_invalid(
            plan_schema7_overflow_blob(
                fixture.header,
                &fixture.root,
                wrong_facts,
                &fixture.planned,
                &fixture.blob_bytes,
                CHUNK_FILE_LENS,
            )
            .unwrap_err(),
            "blob facts do not match",
        );
    }

    #[test]
    fn one_entry_overflow_requires_an_actual_inline_width_exception() {
        let canonical = overflow_fixture(
            2,
            0,
            vec![overflow_entry(
                1,
                ChunkKind::Summary,
                SEGMENT_START_MS,
                u64::from(u32::MAX),
                SERIES_HOT_SCALAR_LANE_LEN_MAX,
                0,
            )],
        );
        assert_invalid(
            plan_schema7_overflow_blob(
                canonical.header,
                &canonical.root,
                canonical.facts,
                &canonical.planned,
                &canonical.blob_bytes,
                CHUNK_FILE_LENS,
            )
            .unwrap_err(),
            "one-chunk overflow blob is noncanonical",
        );

        let width_exceptions = [
            overflow_fixture(
                2,
                0,
                vec![overflow_entry(
                    0,
                    ChunkKind::Float,
                    SEGMENT_START_MS,
                    u64::from(u32::MAX) + 1,
                    0,
                    0,
                )],
            ),
            overflow_fixture(
                2,
                0,
                vec![overflow_entry(
                    0,
                    ChunkKind::Histogram,
                    SEGMENT_START_MS,
                    0,
                    SERIES_HOT_SCALAR_LANE_LEN_MAX + 1,
                    0,
                )],
            ),
            overflow_fixture(
                2,
                0,
                vec![overflow_entry(
                    0,
                    ChunkKind::Float,
                    SEGMENT_START_MS + u64::from(u32::MAX) + 1,
                    0,
                    0,
                    0,
                )],
            ),
        ];
        for fixture in width_exceptions {
            let batch = plan_schema7_overflow_blob(
                fixture.header,
                &fixture.root,
                fixture.facts,
                &fixture.planned,
                &fixture.blob_bytes,
                CHUNK_FILE_LENS,
            )
            .unwrap();
            assert_eq!(batch.locators.len(), 1);
            assert_eq!(batch.series_spans[0].len, 1);
        }
    }

    #[test]
    fn overflow_source_identity_is_rechecked_before_blob_decode() {
        let mut fixture = overflow_fixture(
            2,
            0,
            vec![
                overflow_entry(0, ChunkKind::Float, 1_010, 0, 0, 1),
                overflow_entry(0, ChunkKind::Float, 1_020, 64, 0, 2),
            ],
        );
        let ChunkLocatorSource::Overflow { locator, .. } = &mut fixture.planned.chunks else {
            unreachable!()
        };
        locator.series_ref = 1;
        assert_invalid(
            plan_schema7_overflow_blob(
                fixture.header,
                &fixture.root,
                fixture.facts,
                &fixture.planned,
                &fixture.blob_bytes,
                CHUNK_FILE_LENS,
            )
            .unwrap_err(),
            "source does not match",
        );
    }

    #[test]
    fn overflow_cache_hit_rejects_a_different_valid_root_for_the_same_blob() {
        let entries = vec![
            overflow_entry(0, ChunkKind::Float, 1_010, 0, 0, 1),
            overflow_entry(0, ChunkKind::Float, 1_020, 64, 0, 2),
        ];
        let fixture = overflow_fixture(4, 0, entries.clone());
        let ChunkLocatorSource::Overflow { locator, .. } = &fixture.planned.chunks else {
            unreachable!()
        };
        let locator = *locator;
        let cached_blob = ValidatedOverflowBlob::decode_bound(
            &fixture.blob_bytes,
            fixture.header,
            &fixture.root,
            fixture.facts,
            &fixture.planned,
            CHUNK_FILE_LENS,
        )
        .unwrap();

        let substituted_index = encode_chunk_index_v2(
            4,
            &[
                ChunkOverflowBlobV1 {
                    series_ref: 0,
                    entries,
                },
                ChunkOverflowBlobV1 {
                    series_ref: 1,
                    entries: vec![overflow_entry(1, ChunkKind::Summary, 1_030, 128, 16, 3)],
                },
            ],
        )
        .unwrap();
        let substituted_root = substituted_index.root;
        let substituted_locator = substituted_index.blob_locators[0];
        assert_eq!(substituted_locator, locator);
        assert_ne!(substituted_root, fixture.root);
        let blob_start = usize::try_from(substituted_locator.blob_offset).unwrap();
        let blob_end = blob_start + usize::try_from(substituted_locator.blob_len).unwrap();
        assert_eq!(
            &substituted_index.bytes[blob_start..blob_end],
            fixture.blob_bytes
        );

        assert_invalid(
            plan_schema7_decoded_overflow_blob(
                header_for_index(4, &substituted_index),
                &substituted_root,
                overflow_blob_facts(substituted_root),
                &fixture.planned,
                &cached_blob,
                CHUNK_FILE_LENS,
            )
            .unwrap_err(),
            "decode context does not match",
        );

        assert_invalid(
            plan_schema7_decoded_overflow_blob(
                fixture.header,
                &fixture.root,
                fixture.facts,
                &fixture.planned,
                &cached_blob,
                [CHUNK_FILE_LENS[0] - 1, CHUNK_FILE_LENS[1]],
            )
            .unwrap_err(),
            "decode context does not match",
        );
    }

    fn refresh_blob_crc(bytes: &mut [u8]) {
        bytes[28..32].fill(0);
        let checksum = crc32c(bytes);
        bytes[28..32].copy_from_slice(&checksum.to_le_bytes());
    }
}
