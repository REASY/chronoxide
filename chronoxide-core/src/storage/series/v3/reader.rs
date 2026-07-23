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
mod tests;
