use super::*;

impl Schema7MetadataSession {
    /// Loads and fully integration-validates one exact overflow blob.
    pub(super) fn load_overflow_blob(
        &self,
        roots: &BoundSchema7Roots,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<MetadataCachePin<ValidatedOverflowBlob>, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(planned.provenance)?;
        let ChunkLocatorSource::Overflow { locator, .. } = planned.chunks else {
            return Err(planning_error(
                "schema-7 inline series has no overflow blob",
            ));
        };
        let reader = self.guard.reader(SegmentFile::ChunkIndex)?;
        let key = cache_key(
            &reader,
            locator.blob_offset,
            u64::from(locator.blob_len),
            MetadataCacheClass::OverflowBlob,
        )?;
        let declared = ValidatedOverflowBlob::declared_max_bytes(locator)
            .map_err(MetadataCacheError::from_io)?;
        let header = roots.series_root().header;
        let overflow_root = *roots.overflow_root();
        let overflow_blobs = roots.overflow_blobs;
        let chunk_file_lens = self.chunk_file_lens;
        let blob = reader.get_or_load_owned(key, declared, move |bytes| {
            let blob = ValidatedOverflowBlob::decode_physical_owned(
                bytes,
                header,
                &overflow_root,
                overflow_blobs,
                locator.blob_offset,
                chunk_file_lens,
            )
            .map_err(MetadataCacheError::from_io)?;
            let charged = blob.charged_bytes().map_err(MetadataCacheError::from_io)?;
            Ok(LoadedMetadata::new(blob, charged))
        });
        let blob = blob?;
        if let Err(error) = blob.validate_bound_context(
            header,
            &overflow_root,
            overflow_blobs,
            planned.value,
            chunk_file_lens,
        ) {
            return Err(self.record_cross_artifact_error(error));
        }
        Ok(blob)
    }

    /// Resolves an overflow-backed series and retains scratch accounting for
    /// both flat vectors until the caller drops the result.
    pub(crate) fn plan_overflow_blob(
        &self,
        roots: &BoundSchema7Roots,
        planned: GovernedPlannedSeriesRef<'_>,
    ) -> Result<GovernedChunkLocatorBatch, Schema7MetadataReaderError> {
        self.ensure_bound_roots(roots)?;
        self.ensure_provenance(planned.provenance)?;
        let ChunkLocatorSource::Overflow { locator, .. } = planned.chunks else {
            return Err(planning_error(
                "schema-7 inline series has no overflow blob",
            ));
        };
        let declared = checked_batch_bytes(
            usize::try_from(locator.chunk_count)
                .map_err(|_| planning_error("schema-7 overflow chunk count exceeds usize"))?,
            1,
        )?;
        let mut charge = self
            .guard
            .reader(SegmentFile::ChunkIndex)?
            .runtime()
            .governor()
            .reserve_in_flight_for_usage(declared, MetadataUsageClass::Scratch)
            .map_err(MetadataCacheError::from)?;
        let blob = self.load_overflow_blob(roots, planned)?;
        let value = plan_schema7_decoded_overflow_blob(
            roots.series_root().header,
            roots.overflow_root(),
            roots.overflow_blobs,
            planned.value,
            &blob,
            self.chunk_file_lens,
        )
        .map_err(Schema7MetadataReaderError::Planning)?;
        let (locator_capacity, span_capacity) = value.capacities();
        charge
            .reconcile(checked_batch_bytes(locator_capacity, span_capacity)?)
            .map_err(MetadataCacheError::from)?;
        Ok(GovernedChunkLocatorBatch {
            value,
            _charge: charge,
        })
    }
}
