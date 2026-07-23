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
mod tests;
