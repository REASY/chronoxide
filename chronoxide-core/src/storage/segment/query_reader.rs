use super::full_validation::{
    RegisteredSegmentValidationPolicy, preflight_registered_segment,
    registered_validation_error_to_io,
};
use super::metadata_facade::{
    Schema7MetadataOpenContext, SegmentMetadataFacadeError, SegmentMetadataLayout,
    SegmentMetadataReader, SegmentMetadataVisitControl, SegmentMetadataVisitError,
};
use super::range_scalar_cache::{
    RangeScalarCacheAdmission, RangeScalarCacheCall, RangeScalarCacheKey, RangeScalarCacheLookup,
};
use super::*;
use crate::storage::symbols::SegmentSymbolReader;

// Bounds transient page-request bookkeeping. Materialized result labels have
// their own unavoidable output-size cost, and an oversized single series is
// split into multiple reference batches.
pub(super) const SERIES_LABEL_BATCH_MAX_SYMBOL_REFERENCES: usize = 64 * 1024;
pub(super) const SERIES_LABEL_BATCH_MAX_ENTRIES: usize = 1024;

// Smoke verification is intentionally sample-limited by callers, but the
// limit is a CLI input and may be arbitrarily large. Keep transient metadata,
// labels, locators, and payload buffers bounded independently of that input.
// One individually oversized chunk is still admitted so the scan can make
// progress.
const SMOKE_SERIES_SCAN_BATCH_MAX_ENTRIES: u32 = 256;
const SMOKE_SAMPLE_BATCH_MAX_ENTRIES: usize = 64;
const SMOKE_SAMPLE_BATCH_MAX_BYTES: u64 = 8 * 1024 * 1024;

pub(super) struct NativeTypedCrossSegmentPlan {
    pub(super) series: Vec<NativeTypedCrossSegmentSeries>,
    pub(super) payload_requests: Vec<ChunkPayloadRead>,
    terminal_output_names: Option<Arc<[String]>>,
}

pub(super) struct NativeTypedCrossSegmentSeries {
    series_id: u64,
    metric_name_dropped_series_id: Option<u64>,
    labels: QueryLabels,
    labels_complete: bool,
    chunks: Vec<IndexedChunkLocator>,
}

pub(super) struct GenericCrossSegmentPlan {
    projection: SegmentProjection,
    projected_label_filter: Option<Vec<CompiledLabelMatcher>>,
    terminal_output_names: Option<Arc<[String]>>,
    series: Vec<GenericCrossSegmentSeries>,
    pub(super) payload_requests: Vec<ChunkPayloadRead>,
}

struct GenericCrossSegmentSeries {
    series_id: u64,
    metric_name_dropped_series_id: Option<u64>,
    labels: QueryLabels,
    labels_complete: bool,
    chunks: Arc<Vec<IndexedChunkLocator>>,
}

pub(super) struct GenericRangeScalarCache<'a> {
    pub(super) segment_ordinal: usize,
    pub(super) call: &'a mut RangeScalarCacheCall,
}

type CachedProjectedLabels = Option<Vec<(String, String)>>;
type CachedHistogramBucketSeries = Option<(String, u64, CachedProjectedLabels)>;
type CachedHistogramInfSeries = Option<(u64, CachedProjectedLabels)>;

fn range_scalar_cache_key(
    segment_ordinal: usize,
    entry: &ChunkIndexEntry,
    projection: ChunkScalarProjection,
) -> Option<RangeScalarCacheKey> {
    if entry.file_id != 0 || entry.scalar_lane_offset == 0 || entry.scalar_lane_len == 0 {
        return None;
    }
    Some(RangeScalarCacheKey {
        segment_ordinal,
        file_id: entry.file_id,
        chunk_offset: entry.offset,
        chunk_len: entry.length,
        scalar_lane_offset: entry.scalar_lane_offset,
        scalar_lane_len: entry.scalar_lane_len,
        projection,
        chunk_kind: entry.kind,
    })
}

impl SegmentReader {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let options = SegmentStoreOpenOptions::default();
        let metadata_runtime = open_metadata_runtime(options.metadata_governor)?;
        Self::open_with_options(dir, options, metadata_runtime)
    }

    pub(super) fn open_with_options(
        dir: impl AsRef<Path>,
        options: SegmentStoreOpenOptions,
        metadata_runtime: StoreMetadataRuntime,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        // Reading the small footer validates its own checksum and enforces the
        // segment schema boundary without hashing every tracked segment file.
        // Full per-file validation remains opt-in through `open_validated`.
        let policy = options.storage_schema_policy;
        let validation_policy = match policy {
            SegmentStoreSchemaPolicy::StrictSchema7 => RegisteredSegmentValidationPolicy::Schema7,
            SegmentStoreSchemaPolicy::StrictSchema8 => RegisteredSegmentValidationPolicy::Schema8,
            SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb => {
                RegisteredSegmentValidationPolicy::ValidatedSchema6
            }
        };
        let preflight = preflight_registered_segment(&metadata_runtime, &dir, validation_policy)
            .map_err(registered_validation_error_to_io)?;
        let (registered_metadata, footer, meta) = preflight
            .read_registered_meta()
            .map_err(registered_validation_error_to_io)?;
        Self::open_registered(dir, policy, footer, meta, registered_metadata)
    }

    fn open_registered(
        dir: PathBuf,
        policy: SegmentStoreSchemaPolicy,
        footer: SegmentFooter,
        meta: SegmentMeta,
        registered_metadata: RegisteredSegment,
    ) -> io::Result<Self> {
        let symbol_format = SegmentSymbolFormat::PagedV3;
        let series_count = u32::try_from(meta.series)
            .map_err(|_| invalid_segment_data("segment series count exceeds u32"))?;
        let metadata_layout = match policy {
            SegmentStoreSchemaPolicy::StrictSchema7 => {
                SegmentMetadataLayout::Schema7(Schema7MetadataOpenContext {
                    series_file_len: segment_footer_file_len(&footer, SegmentFile::Series)?,
                    chunk_index_file_len: segment_footer_file_len(
                        &footer,
                        SegmentFile::ChunkIndex,
                    )?,
                    segment_start_ms: meta.start_ms,
                    segment_end_ms: meta.end_ms,
                    series_count,
                })
            }
            SegmentStoreSchemaPolicy::StrictSchema8 => {
                SegmentMetadataLayout::Schema8(Schema7MetadataOpenContext {
                    series_file_len: segment_footer_file_len(&footer, SegmentFile::Series)?,
                    chunk_index_file_len: segment_footer_file_len(
                        &footer,
                        SegmentFile::ChunkIndex,
                    )?,
                    segment_start_ms: meta.start_ms,
                    segment_end_ms: meta.end_ms,
                    series_count,
                })
            }
            SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb => {
                SegmentMetadataLayout::Schema6 { series_count }
            }
        };
        let metadata_reader = SegmentMetadataReader::open(&registered_metadata, metadata_layout)
            .map_err(metadata_facade_io_error)?;
        Ok(Self {
            dir,
            meta,
            storage_schema_policy: policy,
            metadata_reader,
            symbol_format,
            query_cache: Arc::new(SegmentReaderQueryCache::default()),
            registered_metadata,
        })
    }

    pub fn open_validated(dir: impl AsRef<Path>) -> io::Result<Self> {
        let options = SegmentStoreOpenOptions::default();
        let metadata_runtime = open_metadata_runtime(options.metadata_governor)?;
        Self::open_validated_with_options(dir, options, metadata_runtime)
    }

    pub(super) fn open_validated_with_options(
        dir: impl AsRef<Path>,
        options: SegmentStoreOpenOptions,
        metadata_runtime: StoreMetadataRuntime,
    ) -> io::Result<Self> {
        Self::open_footer_validated_with_options(dir, options, metadata_runtime, true)
    }

    pub(super) fn open_footer_validated_with_options(
        dir: impl AsRef<Path>,
        options: SegmentStoreOpenOptions,
        metadata_runtime: StoreMetadataRuntime,
        validate_all_symbols: bool,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let policy = options.storage_schema_policy;
        let validation_policy = match policy {
            SegmentStoreSchemaPolicy::StrictSchema7 => RegisteredSegmentValidationPolicy::Schema7,
            SegmentStoreSchemaPolicy::StrictSchema8 => RegisteredSegmentValidationPolicy::Schema8,
            SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb => {
                RegisteredSegmentValidationPolicy::ValidatedSchema6
            }
        };
        let preflight = preflight_registered_segment(&metadata_runtime, &dir, validation_policy)
            .map_err(registered_validation_error_to_io)?;
        let validated = preflight
            .validate_footer_checksums()
            .map_err(registered_validation_error_to_io)?;
        let (registered_metadata, footer, meta) = validated.into_open_parts();
        let reader = Self::open_registered(dir, policy, footer, meta, registered_metadata)?;
        if validate_all_symbols {
            reader.validate_all_symbols()?;
        }
        Ok(reader)
    }

    pub(super) fn validate_all_symbols(&self) -> io::Result<()> {
        // The footer authenticates the complete file bytes, while the v3 page
        // validator proves the format's internal structure. Keep both checks
        // in the explicit offline validation path and outside timed queries.
        self.metadata_reader
            .validate_all_symbols()
            .map_err(metadata_facade_io_error)
    }

    pub fn meta(&self) -> &SegmentMeta {
        debug_assert_eq!(
            self.registered_metadata.segment_identity(),
            self.dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(""),
            "segment metadata registration must remain bound to its immutable directory identity"
        );
        &self.meta
    }

    pub fn file_path(&self, file: SegmentFile) -> PathBuf {
        self.dir.join(file.filename())
    }

    pub(super) fn cached_index_reader(&self) -> io::Result<CachedIndexReader> {
        let mut cached = self
            .query_cache
            .index_reader
            .lock()
            .map_err(|_| io::Error::other("segment index reader cache lock poisoned"))?;
        if let Some(reader) = cached.as_ref() {
            return Ok(CachedIndexReader {
                reader: reader.try_clone_reader()?,
                cache_hit: true,
                file_bytes: 0,
                open_elapsed: Duration::ZERO,
                open_read_stats: crate::storage::index::SegmentIndexReadStats::default(),
            });
        }

        let path = self.file_path(SegmentFile::Indexes);
        let file_bytes = file_len(&path)?;
        let start = Instant::now();
        let reader = SegmentIndexReader::open(File::open(path)?)?;
        let open_elapsed = start.elapsed();
        let open_read_stats = reader.read_stats();
        let cloned = reader.try_clone_reader()?;
        *cached = Some(reader);
        Ok(CachedIndexReader {
            reader: cloned,
            cache_hit: false,
            file_bytes,
            open_elapsed,
            open_read_stats,
        })
    }

    pub(super) fn cached_symbols(&self) -> io::Result<CachedSymbols> {
        let mut cached = self
            .query_cache
            .symbols
            .lock()
            .map_err(|_| io::Error::other("segment symbols cache lock poisoned"))?;
        if let Some(symbols) = cached.as_ref() {
            return Ok(CachedSymbols {
                symbols: Arc::new(symbols.try_clone_reader()?),
                cache_hit: true,
                file_bytes: 0,
                open_elapsed: Duration::ZERO,
                open_read_stats: Default::default(),
            });
        }

        let path = self.file_path(SegmentFile::Symbols);
        let file_bytes = file_len(&path)?;
        let start = Instant::now();
        let symbols = match self.symbol_format {
            SegmentSymbolFormat::PagedV3 => SegmentSymbolReader::open(File::open(path)?)?,
            SegmentSymbolFormat::LegacyV2ForLayoutAb => {
                SegmentSymbolReader::open_legacy_v2_for_layout_ab(File::open(path)?)?
            }
        };
        let open_elapsed = start.elapsed();
        let open_read_stats = symbols.read_stats();
        let cloned = symbols.try_clone_reader()?;
        *cached = Some(symbols);
        Ok(CachedSymbols {
            symbols: Arc::new(cloned),
            cache_hit: false,
            file_bytes,
            open_elapsed,
            open_read_stats,
        })
    }

    pub fn open_chunks(&self) -> io::Result<File> {
        File::open(self.file_path(SegmentFile::Chunks))
    }

    pub fn read_chunk_index(&self) -> io::Result<Vec<Vec<ChunkIndexEntry>>> {
        let mut file = File::open(self.file_path(SegmentFile::ChunkIndex))?;
        read_chunk_index(&mut file)
    }

    pub fn query_exact(
        &self,
        matchers: &[(&str, &str)],
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let matchers: Vec<NormalizedMatcher> = matchers
            .iter()
            .map(|(name, value)| NormalizedMatcher::Eq {
                name: (*name).to_string(),
                value: (*value).to_string(),
            })
            .collect();
        let mut budget = QueryBudget::unlimited();
        self.query_normalized(
            &matchers,
            &SegmentProjection::None,
            &QueryLabelDemand::Full,
            start_ms,
            end_ms,
            &mut budget,
        )
    }

    pub fn query_selector(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let mut budget = QueryBudget::unlimited();
        self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)
    }

    pub(super) fn query_selector_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let matchers = selector.normalized_matchers();
        self.query_normalized(
            &matchers,
            &selector.projection,
            selector.label_demand(),
            start_ms,
            end_ms,
            budget,
        )
    }

    pub fn metric_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metric_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_values(label_name, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_values(&normalize_discovery_label_name(label_name)))
    }

    pub(super) fn collect_smoke_report(
        &self,
        start_ms: u64,
        end_ms: u64,
        sample_limit_per_kind: usize,
        collect_totals: bool,
        report: &mut SegmentStoreSmokeReport,
    ) -> io::Result<()> {
        if !collect_totals
            && self.meta.chunk_summary.as_ref().is_some_and(|summary| {
                report.sample_limits_reached_for_summary(summary, sample_limit_per_kind)
            })
        {
            return Ok(());
        }

        struct PlannedSmokeSample {
            series_ref: u32,
            series_id: u64,
            labels: Vec<(String, String)>,
            locator: IndexedChunkLocator,
        }

        let chunk_reader = Arc::new(crate::storage::io::ChunkReader::new(
            crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Pread,
                queue_depth: 1,
            },
        )?);
        let mut context = FacadeSegmentQueryContext::open(&self.metadata_reader, chunk_reader)?;
        let mut sample_counts = [
            report.sample_count_for_kind(ChunkKind::Float),
            report.sample_count_for_kind(ChunkKind::Int64),
            report.sample_count_for_kind(ChunkKind::Histogram),
            report.sample_count_for_kind(ChunkKind::ExponentialHistogram),
            report.sample_count_for_kind(ChunkKind::Summary),
        ];
        let series_count = context.root.series_count();
        let mut next_series_ref = 0u32;
        let mut next_chunk_index = 0usize;
        while next_series_ref < series_count {
            if !collect_totals
                && self.meta.chunk_summary.as_ref().is_some_and(|summary| {
                    smoke_sample_limits_reached_for_counts(
                        summary,
                        sample_limit_per_kind,
                        &sample_counts,
                    )
                })
            {
                break;
            }

            let candidate_end = next_series_ref
                .saturating_add(SMOKE_SERIES_SCAN_BATCH_MAX_ENTRIES)
                .min(series_count);
            let refs = (next_series_ref..candidate_end).collect::<Vec<_>>();
            let candidates = context
                .metadata
                .series_ref_set(&context.root, &refs)
                .map_err(metadata_facade_io_error)?;
            let batch_first_series_ref = next_series_ref;
            let batch_first_chunk_index = next_chunk_index;
            let mut resume_at = None;
            let mut planned_bytes = 0u64;
            let mut planned_samples = Vec::with_capacity(SMOKE_SAMPLE_BATCH_MAX_ENTRIES);
            let visit = context.metadata.visit_verified_series(
                &context.root,
                &candidates,
                |verified| -> io::Result<SegmentMetadataVisitControl> {
                    let start_chunk_index = if verified.series_ref() == batch_first_series_ref {
                        batch_first_chunk_index
                    } else {
                        0
                    };
                    let mut chunk_index = 0usize;
                    let locator_visit = verified.chunks().visit(|locator| {
                        let current_chunk_index = chunk_index;
                        chunk_index = chunk_index.saturating_add(1);
                        if current_chunk_index < start_chunk_index {
                            return Ok::<_, io::Error>(SegmentMetadataVisitControl::Continue);
                        }
                        if locator.max_time_ms() < start_ms || locator.min_time_ms() > end_ms {
                            return Ok(SegmentMetadataVisitControl::Continue);
                        }

                        let kind_index = smoke_kind_index(locator.kind());
                        let should_sample = sample_limit_per_kind != 0
                            && sample_counts[kind_index] < sample_limit_per_kind;
                        let chunk_bytes = u64::from(locator.chunk_len());
                        let batch_entry_limit_reached =
                            planned_samples.len() >= SMOKE_SAMPLE_BATCH_MAX_ENTRIES;
                        let batch_byte_limit_reached = !planned_samples.is_empty()
                            && planned_bytes.saturating_add(chunk_bytes)
                                > SMOKE_SAMPLE_BATCH_MAX_BYTES;
                        if should_sample && (batch_entry_limit_reached || batch_byte_limit_reached)
                        {
                            resume_at = Some((verified.series_ref(), current_chunk_index));
                            return Ok(SegmentMetadataVisitControl::Stop);
                        }

                        if collect_totals {
                            report.totals.chunks = report.totals.chunks.saturating_add(1);
                            report.totals.chunk_bytes =
                                report.totals.chunk_bytes.saturating_add(chunk_bytes);
                            report.totals.by_kind.add_chunk(locator.kind(), chunk_bytes);
                        }
                        if should_sample {
                            sample_counts[kind_index] = sample_counts[kind_index].saturating_add(1);
                            planned_bytes = planned_bytes.saturating_add(chunk_bytes);
                            planned_samples.push(PlannedSmokeSample {
                                series_ref: verified.series_ref(),
                                series_id: verified.series_id(),
                                labels: verified.labels().to_vec(),
                                locator: locator.to_owned_indexed_locator(),
                            });
                        }
                        Ok(SegmentMetadataVisitControl::Continue)
                    })?;
                    if locator_visit == super::metadata_facade::SegmentMetadataVisitOutcome::Stopped
                    {
                        Ok(SegmentMetadataVisitControl::Stop)
                    } else {
                        Ok(SegmentMetadataVisitControl::Continue)
                    }
                },
            );
            match visit {
                Ok(_) => {}
                Err(SegmentMetadataVisitError::Metadata(error)) => {
                    return Err(metadata_facade_io_error(error));
                }
                Err(SegmentMetadataVisitError::Visitor(error)) => return Err(error),
            }
            drop(candidates);

            let payload_requests = planned_samples
                .iter()
                .map(|planned| {
                    let entry = planned.locator.entry();
                    ChunkPayloadRead {
                        file_id: entry.file_id,
                        offset: entry.offset,
                        len: u64::from(entry.length),
                    }
                })
                .collect::<Vec<_>>();
            let payloads = context.read_chunk_payload_batch(self, &payload_requests)?;
            for planned in planned_samples {
                let entry = payloads.authenticate_indexed_locator(&planned.locator)?;
                let record = payloads.decode_indexed_chunk_record(&entry)?;
                report.sample_series.push(smoke_series_sample(
                    self.meta.segment_id.clone(),
                    planned.series_ref,
                    planned.series_id,
                    planned.labels,
                    &record,
                    entry.length,
                ));
            }

            if let Some((series_ref, chunk_index)) = resume_at {
                next_series_ref = series_ref;
                next_chunk_index = chunk_index;
            } else {
                next_series_ref = candidate_end;
                next_chunk_index = 0;
            }
        }
        Ok(())
    }

    pub(super) fn query_normalized(
        &self,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        label_demand: &QueryLabelDemand,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let mut context = self.standalone_facade_context()?;
        let mut label_cache = SeriesLabelCache::default();
        let mut label_interner = QueryLabelInterner::default();
        let mut projected_label_cache = ProjectedLabelCache::default();
        self.query_normalized_with_facade_context(
            &mut context,
            0,
            matchers,
            projection,
            label_demand,
            start_ms,
            end_ms,
            budget,
            &mut label_cache,
            &mut label_interner,
            &mut projected_label_cache,
            None,
        )
    }

    pub(super) fn standalone_facade_context(&self) -> io::Result<FacadeSegmentQueryContext> {
        let chunk_reader = Arc::new(crate::storage::io::ChunkReader::new(
            crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Pread,
                queue_depth: 1,
            },
        )?);
        FacadeSegmentQueryContext::open(&self.metadata_reader, chunk_reader)
    }
}

fn segment_footer_file_len(footer: &SegmentFooter, file: SegmentFile) -> io::Result<u64> {
    footer
        .files
        .iter()
        .find_map(|entry| (entry.file == file).then_some(entry.size))
        .ok_or_else(|| invalid_segment_data("segment footer omits a tracked file"))
}

pub(super) fn metadata_facade_io_error(error: SegmentMetadataFacadeError) -> io::Error {
    let kind = metadata_facade_error_kind(&error);
    io::Error::new(kind, error)
}

fn metadata_facade_error_kind(error: &SegmentMetadataFacadeError) -> io::ErrorKind {
    use crate::storage::chunk::Schema6ChunkIndexReaderError;
    use crate::storage::file_manager::MetadataFileManagerError;
    use crate::storage::index::{Schema6IndexReaderError, Schema7IndexReaderError};
    use crate::storage::metadata_cache::{
        MetadataArtifactRegistrationError, MetadataCacheError, StructuralMetadataErrorKind,
    };
    use crate::storage::metadata_runtime::StoreMetadataRuntimeError;
    use crate::storage::series::{
        v2_runtime::Schema6SeriesReaderError, v3::Schema7MetadataReaderError,
    };
    use crate::storage::symbols::GovernedSymbolReaderError;

    fn cache_kind(error: &MetadataCacheError) -> io::ErrorKind {
        match error {
            MetadataCacheError::Budget(_) => io::ErrorKind::OutOfMemory,
            MetadataCacheError::Structural(corruption) => match corruption.kind {
                StructuralMetadataErrorKind::InvalidData => io::ErrorKind::InvalidData,
                StructuralMetadataErrorKind::UnexpectedEof => io::ErrorKind::UnexpectedEof,
            },
            MetadataCacheError::Transient { kind, .. } => *kind,
            MetadataCacheError::DeclaredBoundExceeded { .. } => io::ErrorKind::InvalidData,
            MetadataCacheError::TypeMismatch | MetadataCacheError::UnregisteredArtifact { .. } => {
                io::ErrorKind::Other
            }
            MetadataCacheError::RetiringArtifact { .. } => io::ErrorKind::WouldBlock,
        }
    }

    fn registration_kind(error: &MetadataArtifactRegistrationError) -> io::ErrorKind {
        match error {
            MetadataArtifactRegistrationError::Budget(_) => io::ErrorKind::OutOfMemory,
            MetadataArtifactRegistrationError::Retiring { .. } => io::ErrorKind::WouldBlock,
            MetadataArtifactRegistrationError::PartialInventory { .. } => io::ErrorKind::Other,
            MetadataArtifactRegistrationError::EmptySegmentIdentity
            | MetadataArtifactRegistrationError::EmptyArtifactBatch
            | MetadataArtifactRegistrationError::UnsupportedFile { .. }
            | MetadataArtifactRegistrationError::DuplicateFile { .. }
            | MetadataArtifactRegistrationError::NonCanonicalOrder { .. }
            | MetadataArtifactRegistrationError::SegmentIdentityTooLarge => {
                io::ErrorKind::InvalidInput
            }
        }
    }

    fn file_kind(error: &MetadataFileManagerError) -> io::ErrorKind {
        match error {
            MetadataFileManagerError::Open { source, .. } => source.kind(),
            MetadataFileManagerError::SegmentRetiring { .. }
            | MetadataFileManagerError::OpenFileCapacityUnavailable { .. } => {
                io::ErrorKind::WouldBlock
            }
            MetadataFileManagerError::StructuralReplacement { .. } => io::ErrorKind::InvalidData,
            MetadataFileManagerError::UnsupportedPlatformIdentity => io::ErrorKind::Unsupported,
            MetadataFileManagerError::EmptySegmentIdentity
            | MetadataFileManagerError::UntrackedSegmentFile { .. }
            | MetadataFileManagerError::ConflictingHandle { .. }
            | MetadataFileManagerError::RequestExceedsOpenFileLimit { .. } => {
                io::ErrorKind::InvalidInput
            }
        }
    }

    fn runtime_kind(error: &StoreMetadataRuntimeError) -> io::ErrorKind {
        match error {
            StoreMetadataRuntimeError::FileManager(error) => file_kind(error),
            StoreMetadataRuntimeError::Cache(error) => registration_kind(error),
            StoreMetadataRuntimeError::SegmentNotActive { .. }
            | StoreMetadataRuntimeError::SegmentRetiring { .. } => io::ErrorKind::WouldBlock,
            StoreMetadataRuntimeError::EmptySegmentIdentity
            | StoreMetadataRuntimeError::InvalidArtifactCount { .. }
            | StoreMetadataRuntimeError::NonCanonicalArtifact { .. }
            | StoreMetadataRuntimeError::ConflictingRegistration { .. } => {
                io::ErrorKind::InvalidInput
            }
            StoreMetadataRuntimeError::LifecycleFailed { .. }
            | StoreMetadataRuntimeError::GenerationExhausted => io::ErrorKind::Other,
        }
    }

    fn symbols_kind(error: &GovernedSymbolReaderError) -> io::ErrorKind {
        match error {
            GovernedSymbolReaderError::Runtime(error) => runtime_kind(error),
            GovernedSymbolReaderError::Cache(error) => cache_kind(error),
            GovernedSymbolReaderError::CacheKey(_error) => io::ErrorKind::InvalidInput,
            GovernedSymbolReaderError::Planning(error) => error.kind(),
            GovernedSymbolReaderError::ForeignSegmentGeneration => io::ErrorKind::InvalidData,
        }
    }

    fn chunk_index_kind(error: &Schema6ChunkIndexReaderError) -> io::ErrorKind {
        match error {
            Schema6ChunkIndexReaderError::Runtime(error) => runtime_kind(error),
            Schema6ChunkIndexReaderError::Cache(error) => cache_kind(error),
            Schema6ChunkIndexReaderError::CacheKey(_error) => io::ErrorKind::InvalidInput,
            Schema6ChunkIndexReaderError::ForeignSegmentGeneration => io::ErrorKind::InvalidData,
        }
    }

    fn schema6_series_kind(error: &Schema6SeriesReaderError) -> io::ErrorKind {
        match error {
            Schema6SeriesReaderError::Runtime(error) => runtime_kind(error),
            Schema6SeriesReaderError::Cache(error) => cache_kind(error),
            Schema6SeriesReaderError::CacheKey(_error) => io::ErrorKind::InvalidInput,
            Schema6SeriesReaderError::Planning(error) => error.kind(),
            Schema6SeriesReaderError::Symbols(error) => symbols_kind(error),
            Schema6SeriesReaderError::ChunkIndex(error) => chunk_index_kind(error),
            Schema6SeriesReaderError::ForeignSegmentGeneration
            | Schema6SeriesReaderError::InvalidSeriesRef { .. } => io::ErrorKind::InvalidData,
        }
    }

    fn schema7_metadata_kind(error: &Schema7MetadataReaderError) -> io::ErrorKind {
        match error {
            Schema7MetadataReaderError::Runtime(error) => runtime_kind(error),
            Schema7MetadataReaderError::Cache(error) => cache_kind(error),
            Schema7MetadataReaderError::CacheKey(_error) => io::ErrorKind::InvalidInput,
            Schema7MetadataReaderError::Planning(error) => error.kind(),
            Schema7MetadataReaderError::Symbols(error) => symbols_kind(error),
            Schema7MetadataReaderError::ForeignSegmentGeneration => io::ErrorKind::InvalidData,
        }
    }

    match error {
        SegmentMetadataFacadeError::Runtime(error) => runtime_kind(error),
        SegmentMetadataFacadeError::Symbols(error) => symbols_kind(error),
        SegmentMetadataFacadeError::Schema6Series(error) => schema6_series_kind(error),
        SegmentMetadataFacadeError::Schema6ChunkIndex(error) => chunk_index_kind(error),
        SegmentMetadataFacadeError::Schema6Index(error) => match error {
            Schema6IndexReaderError::Runtime(error) => runtime_kind(error),
            Schema6IndexReaderError::Cache(error) => cache_kind(error),
            Schema6IndexReaderError::CacheKey(_error) => io::ErrorKind::InvalidInput,
            Schema6IndexReaderError::Symbols(error) => symbols_kind(error),
            Schema6IndexReaderError::ForeignSegmentGeneration
            | Schema6IndexReaderError::ForeignRootContext
            | Schema6IndexReaderError::ForeignSeriesCountBinding { .. }
            | Schema6IndexReaderError::ForeignSymbolCountBinding { .. } => {
                io::ErrorKind::InvalidData
            }
        },
        SegmentMetadataFacadeError::Schema7Metadata(error) => schema7_metadata_kind(error),
        SegmentMetadataFacadeError::Schema7Index(error) => match error {
            Schema7IndexReaderError::Runtime(error) => runtime_kind(error),
            Schema7IndexReaderError::Cache(error) => cache_kind(error),
            Schema7IndexReaderError::CacheKey(_error) => io::ErrorKind::InvalidInput,
            Schema7IndexReaderError::Symbols(error) => symbols_kind(error),
            Schema7IndexReaderError::ForeignSegmentGeneration
            | Schema7IndexReaderError::ForeignRootContext => io::ErrorKind::InvalidData,
        },
        SegmentMetadataFacadeError::Budget(_) => io::ErrorKind::OutOfMemory,
        SegmentMetadataFacadeError::ReversedTimeRange { .. } => io::ErrorKind::InvalidInput,
        SegmentMetadataFacadeError::RefSetAllocation(error) => error.kind(),
        SegmentMetadataFacadeError::RefSetSizeOverflow => io::ErrorKind::OutOfMemory,
        SegmentMetadataFacadeError::ForeignSegmentGeneration
        | SegmentMetadataFacadeError::ForeignLayoutBackend
        | SegmentMetadataFacadeError::CompactLabelsUnsupportedForSchema6
        | SegmentMetadataFacadeError::InvalidSeriesRef { .. } => io::ErrorKind::InvalidData,
    }
}

fn smoke_kind_index(kind: ChunkKind) -> usize {
    match kind {
        ChunkKind::Float => 0,
        ChunkKind::Int64 => 1,
        ChunkKind::Histogram => 2,
        ChunkKind::ExponentialHistogram => 3,
        ChunkKind::Summary => 4,
    }
}

fn smoke_sample_limits_reached_for_counts(
    summary: &SegmentChunkSummary,
    sample_limit_per_kind: usize,
    sample_counts: &[usize; 5],
) -> bool {
    [
        ChunkKind::Float,
        ChunkKind::Int64,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ]
    .into_iter()
    .all(|kind| {
        summary.by_kind.stats(kind).chunks == 0
            || sample_counts[smoke_kind_index(kind)] >= sample_limit_per_kind
    })
}

pub(super) fn open_metadata_runtime(
    config: MetadataGovernorConfig,
) -> io::Result<StoreMetadataRuntime> {
    StoreMetadataRuntime::new(config)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
}

pub(super) fn register_segment_metadata(
    runtime: &StoreMetadataRuntime,
    dir: &Path,
    footer: &SegmentFooter,
) -> io::Result<RegisteredSegment> {
    let segment_identity = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment directory name is not valid UTF-8",
            )
        })?;
    SegmentId::parse_dir_name(segment_identity).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid segment directory id: {error}"),
        )
    })?;
    let artifacts = footer
        .files
        .iter()
        .map(|entry| {
            SegmentArtifactRegistration::new(
                entry.file,
                dir.join(entry.file.filename()),
                entry.size,
            )
        })
        .collect::<Vec<_>>();
    runtime
        .register_segment(segment_identity, &artifacts)
        .map_err(metadata_runtime_io_error)
}

fn metadata_runtime_io_error(error: StoreMetadataRuntimeError) -> io::Error {
    let kind = match &error {
        StoreMetadataRuntimeError::FileManager(error) if error.is_structural() => {
            io::ErrorKind::InvalidData
        }
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, error)
}

mod facade;
mod generic;
mod helpers;
mod native;
mod projection;
mod query;

pub(super) use helpers::delta_projection_reset_hint;
