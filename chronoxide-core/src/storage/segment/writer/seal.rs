use super::*;

impl SegmentWriter {
    pub fn flush(&mut self) -> io::Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        let ActiveSegment {
            id: segment_id,
            start_ms,
            end_ms,
            datapoints,
            symbols,
            series_entries,
            chunk_entries,
            chunks,
            temp_dir: tmp,
            metric_query_ordered_input,
            ..
        } = active;
        if series_entries.len() != chunk_entries.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series and chunk entry counts differ",
            ));
        }
        let storage_schema = self.config.storage_schema;

        let total_start = Instant::now();
        let series = series_entries.len() as u64;
        let chunk_summary = SegmentChunkSummary::from_chunk_entries(&chunk_entries);
        let mut profile =
            SegmentFlushProfile::new(segment_id.dir_name(), start_ms, end_ms, datapoints, series);
        let series_permutation = if metric_query_ordered_input {
            #[cfg(debug_assertions)]
            {
                let expected = metric_query_series_order(&series_entries, &symbols)?;
                debug_assert!(
                    expected.iter().copied().eq(0..series_entries.len()),
                    "metric-query ordered input flag was set for non-metric-order series"
                );
            }
            None
        } else {
            let series_order = metric_query_series_order(&series_entries, &symbols)?;
            let old_to_new_refs = old_to_new_series_refs(&series_order)?;
            Some((series_order, old_to_new_refs))
        };

        let meta = SegmentMeta {
            segment_id: segment_id.dir_name(),
            start_ms,
            end_ms,
            datapoints,
            series,
            chunk_summary: Some(chunk_summary),
        };
        time_flush_stage(&mut profile, SegmentFlushStageKind::MetaJson, || {
            let meta_bytes = serde_json::to_vec_pretty(&meta).map_err(io::Error::other)?;
            fs::write(tmp.file_path(SegmentFile::MetaJson), meta_bytes)
        })?;

        let mut chunk_entries = chunk_entries;
        let chunks_path = tmp.file_path(SegmentFile::Chunks);
        let chunk_rewrite =
            time_flush_stage(&mut profile, SegmentFlushStageKind::ChunksFlush, || {
                let mut chunks = chunks;
                chunks.flush()?;
                drop(chunks);
                match &series_permutation {
                    Some((series_order, old_to_new_refs)) => rewrite_chunks_in_series_major_order(
                        &chunks_path,
                        &mut chunk_entries,
                        series_order,
                        old_to_new_refs,
                    ),
                    None => {
                        rewrite_chunks_in_identity_series_order(&chunks_path, &mut chunk_entries)
                    }
                }
            })?;
        profile.add_chunk_rewrite(chunk_rewrite.frames, chunk_rewrite.payload_bytes);
        let (mut series_entries, chunk_entries) = match &series_permutation {
            Some((series_order, _)) => (
                reorder_vec_by_old_indices(series_entries, series_order, "series entries")?,
                reorder_vec_by_old_indices(chunk_entries, series_order, "chunk entries")?,
            ),
            None => (series_entries, chunk_entries),
        };

        time_flush_stage(&mut profile, SegmentFlushStageKind::ChunkIndex, || {
            if storage_schema != SegmentStorageSchema::Schema6 {
                return Ok(());
            }
            let mut chunk_index = File::create(tmp.file_path(SegmentFile::ChunkIndex))?;
            write_chunk_index(&mut chunk_index, &chunk_entries)?;
            chunk_index.flush()
        })?;

        let finalized_metadata =
            time_flush_stage(&mut profile, SegmentFlushStageKind::SegmentMetadata, || {
                if storage_schema == SegmentStorageSchema::Schema6 {
                    let chunk_ranges = chunk_index_ranges(&chunk_entries)?;
                    if series_entries.len() != chunk_ranges.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "series and chunk index range counts differ",
                        ));
                    }
                    for (entry, range) in
                        series_entries.iter_mut().zip(chunk_ranges.iter().copied())
                    {
                        entry.chunk_index = range;
                    }
                }
                finalize_segment_symbol_ids(symbols, series_entries, &chunk_entries)
            })?;
        let label_values =
            time_flush_stage(&mut profile, SegmentFlushStageKind::LabelValues, || {
                LabelValueFstIndex::from_series(
                    &finalized_metadata.series_entries,
                    &finalized_metadata.symbols,
                )
            })?;
        let label_value_time_ranges = time_flush_stage(
            &mut profile,
            SegmentFlushStageKind::LabelValueTimeRanges,
            || Ok(finalized_metadata.label_value_time_ranges),
        )?;
        let metric_series_ranges = time_flush_stage(
            &mut profile,
            SegmentFlushStageKind::MetricSeriesRanges,
            || {
                MetricSeriesRangeIndex::from_series(
                    &finalized_metadata.series_entries,
                    &finalized_metadata.symbols,
                    &label_value_time_ranges,
                )
            },
        )?;
        let routing_index = time_flush_stage(
            &mut profile,
            SegmentFlushStageKind::RoutingIndexBuild,
            || {
                if storage_schema == SegmentStorageSchema::Schema8 {
                    SegmentRoutingIndex::from_indexes_adaptive(
                        &finalized_metadata.symbols,
                        &finalized_metadata.postings,
                        &label_value_time_ranges,
                    )
                } else {
                    SegmentRoutingIndex::from_indexes(
                        &finalized_metadata.symbols,
                        &finalized_metadata.postings,
                        &label_value_time_ranges,
                    )
                }
            },
        )?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Indexes, || {
            let mut index_file = File::create(tmp.file_path(SegmentFile::Indexes))?;
            let num_series =
                u32::try_from(finalized_metadata.series_entries.len()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "series count exceeds u32")
                })?;
            let indexes = SegmentIndexes {
                exact_postings: finalized_metadata.postings,
                label_values,
                label_value_time_ranges,
                metric_series_ranges,
                routing_index: Some(routing_index),
            };
            match storage_schema {
                SegmentStorageSchema::Schema6 => write_segment_indexes_for_roots(
                    &mut index_file,
                    &indexes,
                    num_series,
                    &finalized_metadata.symbols,
                )?,
                SegmentStorageSchema::Schema7 => write_segment_indexes_v8_for_roots(
                    &mut index_file,
                    &indexes,
                    num_series,
                    &finalized_metadata.symbols,
                    &finalized_metadata.series_entries,
                )?,
                SegmentStorageSchema::Schema8 => write_segment_indexes_v9_for_roots(
                    &mut index_file,
                    &indexes,
                    num_series,
                    &finalized_metadata.symbols,
                    &finalized_metadata.series_entries,
                )?,
            }
            index_file.flush()
        })?;

        time_flush_stage(&mut profile, SegmentFlushStageKind::Symbols, || {
            let mut symbols_file = File::create(tmp.file_path(SegmentFile::Symbols))?;
            write_symbols_bin(&mut symbols_file, &finalized_metadata.symbols)?;
            symbols_file.flush()
        })?;

        if storage_schema != SegmentStorageSchema::Schema6 {
            time_flush_stage(&mut profile, SegmentFlushStageKind::OooChunks, || {
                File::create(tmp.file_path(SegmentFile::OooChunks)).map(|_| ())
            })?;
        }

        let mut schema7_stats: Option<Schema7SeriesAssemblyStats> = None;
        time_flush_stage(&mut profile, SegmentFlushStageKind::Series, || {
            let mut series_file = File::create(tmp.file_path(SegmentFile::Series))?;
            if storage_schema == SegmentStorageSchema::Schema6 {
                write_series_bin(&mut series_file, &finalized_metadata.series_entries)?;
                return series_file.flush();
            }

            let mut chunk_index_file = File::create(tmp.file_path(SegmentFile::ChunkIndex))?;
            let chunks_source = File::open(&chunks_path)?;
            let ooo_chunks_source = File::open(tmp.file_path(SegmentFile::OooChunks))?;
            let result = write_canonical_schema7_series_and_chunk_index(
                &mut series_file,
                &mut chunk_index_file,
                Schema7SeriesAssemblyInput {
                    series_entries: &finalized_metadata.series_entries,
                    chunk_entries: &chunk_entries,
                    segment_start_ms: start_ms,
                    segment_end_ms: end_ms,
                    chunk_file_lens: [
                        chunks_source.metadata()?.len(),
                        ooo_chunks_source.metadata()?.len(),
                    ],
                    chunk_sources: [&chunks_source, &ooo_chunks_source],
                },
            )?;
            schema7_stats = Some(result.stats);
            series_file.flush()?;
            chunk_index_file.flush()
        })?;

        if storage_schema == SegmentStorageSchema::Schema6 {
            time_flush_stage(&mut profile, SegmentFlushStageKind::OooChunks, || {
                File::create(tmp.file_path(SegmentFile::OooChunks)).map(|_| ())
            })?;
        }

        time_flush_stage(&mut profile, SegmentFlushStageKind::Footer, || {
            write_segment_footer_for_schema(tmp.path(), storage_schema.footer_version())
        })?;
        profile.set_file_sizes(collect_segment_file_sizes(tmp.path())?);
        let published_dir = time_flush_stage(&mut profile, SegmentFlushStageKind::Publish, || {
            tmp.publish()
        })?;
        append_segment_manifest_record(&self.config.segments_dir, &meta)?;
        profile.total = total_start.elapsed();
        let duration = Duration::from_millis(end_ms - start_ms);
        info!(
            segment_id = %segment_id,
            start_ms,
            end_ms,
            duration=?duration,
            datapoints,
            series,
            storage_schema_version = storage_schema.footer_version(),
            schema7_inline_series = schema7_stats.map(|stats| stats.inline_series_count).unwrap_or_default(),
            schema7_overflow_series = schema7_stats.map(|stats| stats.overflow_series_count).unwrap_or_default(),
            schema7_first_prefix_bytes = schema7_stats.map(|stats| stats.first_prefix_bytes).unwrap_or_default(),
            schema7_second_prefix_bytes = schema7_stats.map(|stats| stats.second_prefix_bytes).unwrap_or_default(),
            elapsed_ms = duration_ms_u64(profile.total),
            meta_json_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::MetaJson),
            chunks_flush_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::ChunksFlush),
            chunk_index_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::ChunkIndex),
            segment_metadata_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::SegmentMetadata),
            label_values_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::LabelValues),
            label_value_time_ranges_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::LabelValueTimeRanges),
            symbols_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Symbols),
            series_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Series),
            indexes_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Indexes),
            routing_index_build_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::RoutingIndexBuild),
            ooo_chunks_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::OooChunks),
            footer_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Footer),
            publish_ms = profile.stage_elapsed_ms(SegmentFlushStageKind::Publish),
            chunk_rewrite_frames = profile.chunk_rewrite_frames(),
            chunk_rewrite_payload_bytes = profile.chunk_rewrite_payload_bytes(),
            total_bytes = profile.total_file_bytes(),
            data_bytes = profile.data_file_bytes(),
            metadata_bytes = profile.metadata_file_bytes(),
            index_bytes = profile.index_file_bytes(),
            footer_bytes = profile.footer_file_bytes(),
            meta_json_bytes = profile.file_size_bytes(SegmentFile::MetaJson).unwrap_or_default(),
            symbols_bytes = profile.file_size_bytes(SegmentFile::Symbols).unwrap_or_default(),
            series_bytes = profile.file_size_bytes(SegmentFile::Series).unwrap_or_default(),
            chunks_bytes = profile.file_size_bytes(SegmentFile::Chunks).unwrap_or_default(),
            ooo_chunks_bytes = profile.file_size_bytes(SegmentFile::OooChunks).unwrap_or_default(),
            chunk_index_bytes = profile.file_size_bytes(SegmentFile::ChunkIndex).unwrap_or_default(),
            indexes_bytes = profile.file_size_bytes(SegmentFile::Indexes).unwrap_or_default(),
            footer_file_bytes = profile.file_size_bytes(SegmentFile::Footer).unwrap_or_default(),
            path = %published_dir.display(),
            "Segment published"
        );
        self.last_flush_profile = Some(profile);
        Ok(())
    }
}

pub(in super::super) fn time_flush_stage<T>(
    profile: &mut SegmentFlushProfile,
    kind: SegmentFlushStageKind,
    f: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let started = Instant::now();
    let result = f();
    profile.push_stage(kind, started.elapsed());
    result
}

pub(in super::super) fn collect_segment_file_sizes(
    segment_dir: &Path,
) -> io::Result<Vec<SegmentFlushFileSize>> {
    SEGMENT_FLUSH_SIZE_FILES
        .into_iter()
        .map(|file| {
            fs::metadata(segment_dir.join(file.filename())).map(|metadata| SegmentFlushFileSize {
                file,
                bytes: metadata.len(),
            })
        })
        .collect()
}
