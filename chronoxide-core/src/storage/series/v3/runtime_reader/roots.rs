use super::*;

impl Schema7MetadataSession {
    /// Loads the two roots as separate pure cache values.
    pub(crate) fn load_roots(&self) -> Result<Schema7RootPins, Schema7MetadataReaderError> {
        self.load_roots_with_prefix(None)
    }

    pub(super) fn load_roots_with_prefix(
        &self,
        series_prefix: Option<&[u8]>,
    ) -> Result<Schema7RootPins, Schema7MetadataReaderError> {
        let series_reader = self.guard.reader(SegmentFile::Series)?;
        let series_key = cache_key(
            &series_reader,
            0,
            self.root_len,
            MetadataCacheClass::SeriesRoot,
        )?;
        let series_declared =
            SeriesRootV3::declared_max_bytes(self.root_len).map_err(MetadataCacheError::from_io)?;
        let load_series = |bytes: &[u8]| {
            let root = decode_series_root_v3(bytes).map_err(MetadataCacheError::from_io)?;
            let charged = root.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(root, charged))
        };
        let series = match series_prefix {
            Some(prefix) => series_reader.get_or_load_with_prefix(
                series_key,
                series_declared,
                prefix,
                load_series,
            )?,
            None => series_reader.get_or_load(series_key, series_declared, load_series)?,
        };

        let overflow_reader = self.guard.reader(SegmentFile::ChunkIndex)?;
        let overflow_key = cache_key(
            &overflow_reader,
            0,
            CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
            MetadataCacheClass::OverflowRoot,
        )?;
        let expected_file_len = self.context.chunk_index_file_len;
        let overflow = overflow_reader.get_or_load(
            overflow_key,
            std::mem::size_of::<ChunkOverflowRootV2>() as u64,
            move |bytes| {
                let root = decode_chunk_overflow_root_v2(bytes, expected_file_len)
                    .map_err(MetadataCacheError::from_io)?;
                let charged = root.charged_bytes();
                Ok(LoadedMetadata::new(root, charged))
            },
        )?;

        Ok(Schema7RootPins {
            provenance: self.guard.provenance(),
            series,
            overflow,
        })
    }

    /// Cross-validates separately cached roots while retaining their pins only
    /// in the returned query-local binding.
    pub(crate) fn bind(
        &self,
        roots: Schema7RootPins,
    ) -> Result<BoundSchema7Roots, Schema7MetadataReaderError> {
        self.ensure_provenance(&roots.provenance)?;
        match Schema7RootBinding::bind_decoded(&roots.series, &roots.overflow, self.context) {
            Ok((series_pages, overflow_blobs)) => Ok(BoundSchema7Roots {
                roots,
                series_pages,
                overflow_blobs,
            }),
            Err(error) => Err(self.record_cross_artifact_error(error)),
        }
    }

    /// Exposes the schema-neutral series-count capability only after both
    /// schema-7 roots have been cross-validated for this generation.
    pub(crate) fn series_count_binding(
        &self,
        roots: &BoundSchema7Roots,
    ) -> Result<GovernedSeriesCountBinding, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        Ok(GovernedSeriesCountBinding::new(
            self.guard.provenance(),
            roots.series_root().header.num_series,
        ))
    }

    /// Loads and authenticates one exact fixed-size hot page.
    pub(super) fn load_hot_page(
        &self,
        roots: &BoundSchema7Roots,
        page_index: u32,
    ) -> Result<MetadataCachePin<ValidatedSeriesHotPage>, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let descriptor = roots.hot_descriptor(page_index)?;
        let page_offset = roots
            .series_pages
            .hot_pages_offset
            .checked_add(
                u64::from(page_index)
                    .checked_mul(SERIES_HOT_PAGE_LEN_V1 as u64)
                    .ok_or_else(|| planning_error("schema-7 hot page offset overflows"))?,
            )
            .ok_or_else(|| planning_error("schema-7 hot page offset overflows"))?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = cache_key(
            &reader,
            page_offset,
            SERIES_HOT_PAGE_LEN_V1 as u64,
            MetadataCacheClass::SeriesHotPage,
        )?;
        let declared = ValidatedSeriesHotPage::declared_max_bytes(descriptor)
            .map_err(MetadataCacheError::from_io)?;
        let header = roots.series_root().header;
        let chunk_file_lens = self.chunk_file_lens;
        Ok(reader.get_or_load_owned(key, declared, move |bytes| {
            let page = ValidatedSeriesHotPage::decode_owned(
                header,
                page_index,
                descriptor,
                bytes,
                chunk_file_lens,
            )
            .map_err(MetadataCacheError::from_io)?;
            let charged = page.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(page, charged))
        })?)
    }

    /// Plans selected series from one authenticated page and retains a scratch
    /// charge for the resulting query-local vector.
    pub(crate) fn plan_hot_page(
        &self,
        roots: &BoundSchema7Roots,
        page_index: u32,
        selected_series_refs: &[u32],
    ) -> Result<GovernedPlannedSeries, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let declared = checked_vec_bytes::<PlannedSeries>(
            selected_series_refs.len(),
            "schema-7 planned-series allocation charge overflows",
        )?;
        let mut charge = self
            .guard
            .reader(SegmentFile::Series)?
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(declared, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)?;
        let descriptor = roots.hot_descriptor(page_index)?;
        let page = self.load_hot_page(roots, page_index)?;
        let values = plan_schema7_decoded_hot_page(
            roots.series_root().header,
            roots.series_pages,
            page_index,
            descriptor,
            &page,
            self.chunk_file_lens,
            selected_series_refs,
        )
        .map_err(Schema7MetadataReaderError::Planning)?;
        charge
            .reconcile(checked_vec_bytes::<PlannedSeries>(
                values.capacity(),
                "schema-7 planned-series allocation charge overflows",
            )?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedPlannedSeries {
            provenance: self.guard.provenance(),
            values,
            _charge: charge,
        })
    }

    /// Loads and authenticates one exact cold-label page. The returned pin
    /// keeps the page bytes governed until the caller finishes materializing
    /// labels from them.
    pub(super) fn load_cold_page(
        &self,
        roots: &BoundSchema7Roots,
        page_index: u32,
    ) -> Result<MetadataCachePin<ValidatedSeriesColdPage>, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        let descriptor = roots.cold_descriptor(page_index)?;
        let page_offset = roots
            .series_pages
            .cold_pages_offset
            .checked_add(
                u64::from(page_index)
                    .checked_mul(super::SERIES_COLD_PAGE_LEN_V1)
                    .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?,
            )
            .ok_or_else(|| planning_error("schema-7 cold page offset overflows"))?;
        let reader = self.guard.reader(SegmentFile::Series)?;
        let key = cache_key(
            &reader,
            page_offset,
            u64::from(descriptor.page_len),
            MetadataCacheClass::SeriesColdPage,
        )?;
        let declared = ValidatedSeriesColdPage::declared_max_bytes(descriptor)
            .map_err(MetadataCacheError::from_io)?;
        let header = roots.series_root().header;
        Ok(reader.get_or_load_owned(key, declared, move |bytes| {
            let page = ValidatedSeriesColdPage::decode_owned(header, page_index, descriptor, bytes)
                .map_err(MetadataCacheError::from_io)?;
            let charged = page.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(page, charged))
        })?)
    }
}
