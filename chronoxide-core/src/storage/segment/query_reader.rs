use super::range_scalar_cache::{
    RangeScalarCacheAdmission, RangeScalarCacheCall, RangeScalarCacheKey, RangeScalarCacheLookup,
};
use super::*;

pub(super) struct NativeTypedCrossSegmentPlan {
    pub(super) series: Vec<NativeTypedCrossSegmentSeries>,
    pub(super) payload_requests: Vec<ChunkPayloadRead>,
}

pub(super) struct NativeTypedCrossSegmentSeries {
    series_id: u64,
    labels: QueryLabels,
    chunks: Vec<ChunkIndexEntry>,
}

pub(super) struct GenericCrossSegmentPlan {
    projection: SegmentProjection,
    projected_label_filter: Option<Vec<CompiledLabelMatcher>>,
    series: Vec<GenericCrossSegmentSeries>,
    pub(super) payload_requests: Vec<ChunkPayloadRead>,
}

struct GenericCrossSegmentSeries {
    series_id: u64,
    labels: QueryLabels,
    chunks: Arc<Vec<ChunkIndexEntry>>,
}

impl GenericCrossSegmentPlan {
    pub(super) fn empty(projection: SegmentProjection) -> Self {
        Self {
            projection,
            projected_label_filter: None,
            series: Vec::new(),
            payload_requests: Vec::new(),
        }
    }
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
        let dir = dir.as_ref().to_path_buf();
        let meta_path = dir.join(SegmentFile::MetaJson.filename());
        let meta_bytes = fs::read(meta_path)?;
        let meta = serde_json::from_slice(&meta_bytes).map_err(io::Error::other)?;
        Ok(Self {
            dir,
            meta,
            query_cache: Arc::new(SegmentReaderQueryCache::default()),
        })
    }

    pub fn open_validated(dir: impl AsRef<Path>) -> io::Result<Self> {
        let reader = Self::open(dir)?;
        validate_segment_footer(&reader.dir)?;
        Ok(reader)
    }

    pub fn meta(&self) -> &SegmentMeta {
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
                symbols: Arc::clone(symbols),
                cache_hit: true,
                file_bytes: 0,
                open_elapsed: Duration::ZERO,
            });
        }

        let path = self.file_path(SegmentFile::Symbols);
        let file_bytes = file_len(&path)?;
        let start = Instant::now();
        let symbols = Arc::new(read_symbols_bin(File::open(path)?)?);
        let open_elapsed = start.elapsed();
        *cached = Some(Arc::clone(&symbols));
        Ok(CachedSymbols {
            symbols,
            cache_hit: false,
            file_bytes,
            open_elapsed,
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
        self.query_normalized(&matchers, &selector.projection, start_ms, end_ms, budget)
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

        let symbols = read_symbols_bin(File::open(self.file_path(SegmentFile::Symbols))?)?;
        let mut series_reader =
            SeriesReader::open(File::open(self.file_path(SegmentFile::Series))?)?;
        let mut chunk_index_reader =
            ChunkIndexReader::open(File::open(self.file_path(SegmentFile::ChunkIndex))?)?;
        let mut chunk_file = self.open_chunks()?;

        if collect_totals {
            chunk_index_reader.for_each_series_entries(|series_ref, entries| {
                Self::collect_smoke_entries_for_series(
                    &self.meta.segment_id,
                    series_ref,
                    entries,
                    start_ms,
                    end_ms,
                    sample_limit_per_kind,
                    collect_totals,
                    report,
                    &symbols,
                    &mut series_reader,
                    &mut chunk_file,
                )
            })?;
        } else {
            for series_ref in 0..chunk_index_reader.len() {
                if self.meta.chunk_summary.as_ref().is_some_and(|summary| {
                    report.sample_limits_reached_for_summary(summary, sample_limit_per_kind)
                }) {
                    break;
                }
                let series_ref = u32::try_from(series_ref).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "series_ref exceeds u32")
                })?;
                let Some(entries) = chunk_index_reader.read_entries(series_ref)? else {
                    continue;
                };
                Self::collect_smoke_entries_for_series(
                    &self.meta.segment_id,
                    series_ref,
                    &entries,
                    start_ms,
                    end_ms,
                    sample_limit_per_kind,
                    collect_totals,
                    report,
                    &symbols,
                    &mut series_reader,
                    &mut chunk_file,
                )?;
            }
        }

        Ok(())
    }

    pub(super) fn collect_smoke_entries_for_series(
        segment_id: &str,
        series_ref: u32,
        entries: &[ChunkIndexEntry],
        start_ms: u64,
        end_ms: u64,
        sample_limit_per_kind: usize,
        collect_totals: bool,
        report: &mut SegmentStoreSmokeReport,
        symbols: &SegmentSymbols,
        series_reader: &mut SeriesReader<File>,
        chunk_file: &mut File,
    ) -> io::Result<()> {
        let mut resolved_entry: Option<(SeriesEntry, Vec<(String, String)>)> = None;
        for entry in entries {
            if !chunk_overlaps_range(entry, start_ms, end_ms) {
                continue;
            }

            let chunk_bytes = u64::from(entry.length);
            if collect_totals {
                report.totals.chunks = report.totals.chunks.saturating_add(1);
                report.totals.chunk_bytes = report.totals.chunk_bytes.saturating_add(chunk_bytes);
                report.totals.by_kind.add_chunk(entry.kind, chunk_bytes);
            }

            if sample_limit_per_kind == 0
                || report.sample_count_for_kind(entry.kind) >= sample_limit_per_kind
            {
                continue;
            }

            if resolved_entry.is_none() {
                let Some(series_entry) = series_reader.read_entry(series_ref)? else {
                    continue;
                };
                let labels = Self::resolve_series_labels(symbols, &series_entry)?;
                resolved_entry = Some((series_entry, labels));
            }
            let Some((series_entry, labels)) = resolved_entry.as_ref() else {
                continue;
            };
            let record = read_chunk_record_at(chunk_file, entry.offset, entry.length)?;
            report.sample_series.push(smoke_series_sample(
                segment_id.to_string(),
                series_ref,
                series_entry.series_id,
                labels.clone(),
                &record,
                entry.length,
            ));
        }
        Ok(())
    }

    pub(super) fn query_normalized(
        &self,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let mut context = SegmentQueryContext::open(self, None)?;
        let mut label_cache = SeriesLabelCache::default();
        let mut projected_label_cache = ProjectedLabelCache::default();
        self.query_normalized_with_context(
            &mut context,
            0,
            matchers,
            projection,
            start_ms,
            end_ms,
            budget,
            &mut label_cache,
            &mut projected_label_cache,
            None,
        )
    }
}

mod generic;
mod helpers;
mod native;
mod projection;
mod query;

pub(super) use helpers::delta_projection_reset_hint;
use helpers::metric_series_range_candidates;
