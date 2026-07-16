//! Authentication boundary for one physical schema-7 cold-label page.
//!
//! Cold pages have no page header or physical padding. Their descriptor in the
//! authenticated series root supplies the page index, exact physical length,
//! and CRC. This value retains the complete decode context so a cache hit
//! cannot substitute bytes validated under another root, descriptor, or page.

use std::io;

use crc32c::crc32c;

use super::{SeriesColdPageDescriptorV1, SeriesHeaderV3};

/// One owned, fully authenticated physical cold-label page.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ValidatedSeriesColdPage {
    header: SeriesHeaderV3,
    page_index: u32,
    descriptor: SeriesColdPageDescriptorV1,
    bytes: Box<[u8]>,
}

impl ValidatedSeriesColdPage {
    /// Returns the exact final-allocation bound for one decoded cold page.
    pub(crate) fn declared_max_bytes(descriptor: SeriesColdPageDescriptorV1) -> io::Result<u64> {
        let fixed = u64::try_from(std::mem::size_of::<Self>())
            .map_err(|_| resource_error("series v3 cold page charge exceeds u64"))?;
        fixed
            .checked_add(u64::from(descriptor.page_len))
            .ok_or_else(|| resource_error("series v3 cold page charge overflows"))
    }

    /// Validates and owns one exact physical cold-page range.
    ///
    /// The final page is not padded to 16 KiB. Passing a padded final page, a
    /// truncated page, or bytes for another descriptor is therefore a
    /// structural error even when the extra bytes are zero.
    pub(crate) fn decode(
        header: SeriesHeaderV3,
        page_index: u32,
        descriptor: SeriesColdPageDescriptorV1,
        page_bytes: &[u8],
    ) -> io::Result<Self> {
        let mut owned = Vec::new();
        let expected_len = validate_page_bytes(header, page_index, descriptor, page_bytes)?;
        owned
            .try_reserve_exact(expected_len)
            .map_err(|_| resource_error("series v3 cold page allocation failed"))?;
        owned.extend_from_slice(page_bytes);
        Self::decode_owned(header, page_index, descriptor, owned)
    }

    /// Validates and takes ownership of a governed exact-range allocation.
    pub(crate) fn decode_owned(
        header: SeriesHeaderV3,
        page_index: u32,
        descriptor: SeriesColdPageDescriptorV1,
        page_bytes: Vec<u8>,
    ) -> io::Result<Self> {
        validate_page_bytes(header, page_index, descriptor, &page_bytes)?;
        Ok(Self {
            header,
            page_index,
            descriptor,
            bytes: page_bytes.into_boxed_slice(),
        })
    }

    /// Returns the authenticated bytes after rebinding a cache hit to the
    /// exact root and descriptor expected by the caller.
    pub(crate) fn bytes_for(
        &self,
        header: SeriesHeaderV3,
        page_index: u32,
        descriptor: SeriesColdPageDescriptorV1,
    ) -> io::Result<&[u8]> {
        // `decode_owned` admitted this page only after validating the complete
        // immutable header/descriptor context. A hit must prove that the
        // caller presents those exact authenticated values, but re-deriving
        // the complete series layout here adds no further substitution
        // protection. Equality against the admitted context is sufficient and
        // keeps the hot cache-hit path proportional to the requested page.
        if self.header != header || self.page_index != page_index || self.descriptor != descriptor {
            return Err(invalid_data(
                "series v3 cached cold page decode context does not match the expected root",
            ));
        }
        Ok(&self.bytes)
    }

    /// Logical allocation bytes owned by this cache value.
    ///
    /// The caller-owned encoded scratch buffer and allocator slack are not
    /// included. The boxed page has exactly the authenticated physical length.
    pub(crate) fn charged_bytes(&self) -> io::Result<u64> {
        let fixed = u64::try_from(std::mem::size_of::<Self>())
            .map_err(|_| resource_error("series v3 cold page charge exceeds u64"))?;
        let bytes = u64::try_from(self.bytes.len())
            .map_err(|_| resource_error("series v3 cold page charge exceeds u64"))?;
        fixed
            .checked_add(bytes)
            .ok_or_else(|| resource_error("series v3 cold page charge overflows"))
    }
}

fn validate_page_bytes(
    header: SeriesHeaderV3,
    page_index: u32,
    descriptor: SeriesColdPageDescriptorV1,
    page_bytes: &[u8],
) -> io::Result<usize> {
    validate_context(header, page_index, descriptor)?;
    let expected_len = usize::try_from(descriptor.page_len)
        .map_err(|_| invalid_data("series v3 cold page length exceeds usize"))?;
    if page_bytes.len() != expected_len {
        return Err(invalid_data("series v3 cold page length is not exact"));
    }
    if crc32c(page_bytes) != descriptor.page_crc32c {
        return Err(invalid_data("series v3 cold page CRC mismatch"));
    }
    Ok(expected_len)
}

fn validate_context(
    header: SeriesHeaderV3,
    page_index: u32,
    descriptor: SeriesColdPageDescriptorV1,
) -> io::Result<()> {
    header.validate()?;
    descriptor.validate(header, page_index)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn resource_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::OutOfMemory, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::series::v3::SERIES_COLD_PAGE_LEN_V1;

    const SEGMENT_START_MS: u64 = 1_000;
    const SEGMENT_END_MS: u64 = 2_000;

    fn header_with_cold_lengths(
        keysets_len: u64,
        value_dicts_len: u64,
        keyset_blocks_len: u64,
    ) -> SeriesHeaderV3 {
        super::super::SeriesHeaderV3::new(super::super::SeriesHeaderV3Params {
            num_series: 1,
            num_keysets: 1,
            num_value_dicts: 1,
            chunk_index_root_crc32c: 0x1234_5678,
            keysets_len,
            value_dicts_len,
            keyset_blocks_len,
            segment_start_ms: SEGMENT_START_MS,
            segment_end_ms: SEGMENT_END_MS,
            chunk_index_file_len: 64,
        })
        .unwrap()
    }

    fn two_page_header() -> SeriesHeaderV3 {
        header_with_cold_lengths(SERIES_COLD_PAGE_LEN_V1, 16, 16)
    }

    fn page_bytes(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|index| seed.wrapping_add(index as u8))
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
    fn authenticates_one_exact_full_cold_page_and_reports_its_charge() {
        let header = two_page_header();
        assert_eq!(header.cold_page_count, 2);
        let bytes = page_bytes(SERIES_COLD_PAGE_LEN_V1 as usize, 7);
        let descriptor = SeriesColdPageDescriptorV1::new(header, 0, crc32c(&bytes)).unwrap();

        let page = ValidatedSeriesColdPage::decode(header, 0, descriptor, &bytes).unwrap();
        let owned =
            ValidatedSeriesColdPage::decode_owned(header, 0, descriptor, bytes.clone()).unwrap();

        assert_eq!(page.bytes_for(header, 0, descriptor).unwrap(), bytes);
        assert_eq!(owned, page);
        assert_eq!(
            page.charged_bytes().unwrap(),
            (std::mem::size_of::<ValidatedSeriesColdPage>() + bytes.len()) as u64
        );
        assert_eq!(
            page.charged_bytes().unwrap(),
            ValidatedSeriesColdPage::declared_max_bytes(descriptor).unwrap()
        );
    }

    #[test]
    fn final_page_has_its_exact_short_length_and_rejects_physical_padding() {
        let header = two_page_header();
        let bytes = page_bytes(32, 19);
        let descriptor = SeriesColdPageDescriptorV1::new(header, 1, crc32c(&bytes)).unwrap();
        assert_eq!(descriptor.page_len, 32);

        let page = ValidatedSeriesColdPage::decode(header, 1, descriptor, &bytes).unwrap();
        assert_eq!(page.bytes_for(header, 1, descriptor).unwrap(), bytes);

        assert_invalid(
            ValidatedSeriesColdPage::decode(header, 1, descriptor, &bytes[..31]).unwrap_err(),
            "length is not exact",
        );
        let padded = vec![0; SERIES_COLD_PAGE_LEN_V1 as usize];
        assert_invalid(
            ValidatedSeriesColdPage::decode(header, 1, descriptor, &padded).unwrap_err(),
            "length is not exact",
        );
    }

    #[test]
    fn cache_hit_rejects_header_page_and_descriptor_substitution() {
        let header = header_with_cold_lengths(SERIES_COLD_PAGE_LEN_V1 * 2, 16, 16);
        assert_eq!(header.cold_page_count, 3);
        let bytes = page_bytes(SERIES_COLD_PAGE_LEN_V1 as usize, 23);
        let descriptor0 = SeriesColdPageDescriptorV1::new(header, 0, crc32c(&bytes)).unwrap();
        let page = ValidatedSeriesColdPage::decode(header, 0, descriptor0, &bytes).unwrap();

        let substituted_header = SeriesHeaderV3::new(super::super::SeriesHeaderV3Params {
            num_series: 1,
            num_keysets: 1,
            num_value_dicts: 1,
            chunk_index_root_crc32c: 0x1234_5678,
            keysets_len: SERIES_COLD_PAGE_LEN_V1 * 2,
            value_dicts_len: 16,
            keyset_blocks_len: 16,
            segment_start_ms: SEGMENT_START_MS + 1,
            segment_end_ms: SEGMENT_END_MS + 1,
            chunk_index_file_len: 64,
        })
        .unwrap();
        assert_invalid(
            page.bytes_for(substituted_header, 0, descriptor0)
                .unwrap_err(),
            "decode context does not match",
        );

        let descriptor1 = SeriesColdPageDescriptorV1::new(header, 1, crc32c(&bytes)).unwrap();
        assert_invalid(
            page.bytes_for(header, 1, descriptor1).unwrap_err(),
            "decode context does not match",
        );

        let other_bytes = page_bytes(SERIES_COLD_PAGE_LEN_V1 as usize, 29);
        let substituted_descriptor =
            SeriesColdPageDescriptorV1::new(header, 0, crc32c(&other_bytes)).unwrap();
        assert_invalid(
            page.bytes_for(header, 0, substituted_descriptor)
                .unwrap_err(),
            "decode context does not match",
        );
    }

    #[test]
    fn corruption_and_noncanonical_descriptor_are_rejected_before_ownership() {
        let header = two_page_header();
        let bytes = page_bytes(SERIES_COLD_PAGE_LEN_V1 as usize, 31);
        let descriptor = SeriesColdPageDescriptorV1::new(header, 0, crc32c(&bytes)).unwrap();

        let mut corrupt = bytes.clone();
        corrupt[8_191] ^= 1;
        assert_invalid(
            ValidatedSeriesColdPage::decode(header, 0, descriptor, &corrupt).unwrap_err(),
            "CRC mismatch",
        );

        let wrong_index = SeriesColdPageDescriptorV1 {
            page_index: 1,
            ..descriptor
        };
        assert_invalid(
            ValidatedSeriesColdPage::decode(header, 0, wrong_index, &bytes).unwrap_err(),
            "descriptor is noncanonical",
        );

        let wrong_length = SeriesColdPageDescriptorV1 {
            page_len: descriptor.page_len - 1,
            ..descriptor
        };
        assert_invalid(
            ValidatedSeriesColdPage::decode(header, 0, wrong_length, &bytes).unwrap_err(),
            "descriptor is noncanonical",
        );
    }
}
