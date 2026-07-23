use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::chunk::{ChunkIndexEntry, ChunkWriter};
use chronoxide_core::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OtlpAggregationTemporality, SummaryQuantileValue, SummaryValue, TypedSampleMetadata,
};
use chronoxide_core::storage::index::{SegmentIndexReadCount, SegmentIndexReadStats};
use chronoxide_core::storage::manifest::{
    ManifestRecord, ManifestSegment, ManifestWriter, write_current,
};
use chronoxide_core::storage::segment::{
    ChunkReadSchedulerProfile, SegmentMeta, SegmentStorageSchema, SegmentStoreOpenOptions,
    SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
};

use super::benchmark::{
    BenchmarkOutputKind, QueryBenchmarkConfig, QueryBenchmarkMetadataRuntimeReport,
    QueryBenchmarkMode, QueryBenchmarkRangeScalarCacheReport,
    QueryBenchmarkRawChunkReadSchedulerV2, QueryBenchmarkRawQueryLabelStorageV2,
    QueryBenchmarkRawRangeExecutionV1, QueryBenchmarkRawRangeScalarCacheV3,
    QueryBenchmarkRawSymbolReadsV5, QueryBenchmarkResult, QueryBenchmarkRunKind, RawQueryStatsV1,
    StagedBenchmarkOutput, add_session_profile, effective_query_end_ms,
    format_payload_read_amplification, median_duration, publish_benchmark_outputs_with_stager,
    render_index_positional_read_table, render_profile_table, render_query_label_storage,
    render_query_result_index_positional_reads, render_range_scalar_cache_runs,
    run_query_benchmark, run_query_benchmark_with_experimental_flow,
    run_query_benchmark_with_experimental_flow_and_instrumentation,
    validate_query_label_storage_stats, validate_query_stage_accounting,
};
use super::smoke::{
    ExpectedReadback, QueryReadbackDiagnostics, QueryReadbackMismatch, QuerySmokeDiagnostics,
    ReadbackIsolationCheck, SCALAR_RANGE_READBACK_STEP_MS, bounded_scalar_counter_range_readback,
    collect_expected_readbacks, exponential_histogram_expected_readbacks,
    project_exponential_histogram_bucket_samples_with_range_hints,
    project_histogram_bucket_samples_with_range_hints, project_optional_f64_counter_samples,
    project_u64_counter_samples, promql_exact_selector, promql_samples_eq,
    push_counter_range_readbacks, read_chunk_record_from_payload_files, render_markdown,
    run_query_smoke, sample_limits_reached, scalar_counter_range_increase,
    scalar_expected_readbacks, segment_dirs, verify_expected_readbacks, verify_readbacks,
};
use super::store::{open_segment_store, open_segment_store_for_layout_ab, query_projection_config};
use super::*;

#[path = "tests/benchmark_execution_config.rs"]
mod benchmark_execution_config;
#[path = "tests/cli_validation.rs"]
mod cli_validation;
#[path = "tests/fixtures_corruption.rs"]
mod fixtures_corruption;
#[path = "tests/output_publication_raw_schema.rs"]
mod output_publication_raw_schema;
#[path = "tests/reporting_profile.rs"]
mod reporting_profile;
#[path = "tests/smoke_readback.rs"]
mod smoke_readback;

fn json_object_keys(value: &serde_json::Value) -> BTreeSet<&str> {
    value
        .as_object()
        .expect("JSON value must be an object")
        .keys()
        .map(String::as_str)
        .collect()
}

fn sample_index_read_stats(multiplier: u64) -> SegmentIndexReadStats {
    let count = |value| SegmentIndexReadCount {
        calls: value * multiplier,
        bytes: value * multiplier * 10,
    };
    SegmentIndexReadStats {
        root: count(1),
        routing: count(2),
        exact_directory: count(3),
        exact_page: count(4),
        auxiliary_directory: count(5),
        payload: count(6),
    }
}

fn sample_symbol_read_stats(multiplier: u64) -> SegmentSymbolReadStats {
    SegmentSymbolReadStats {
        legacy_eager: SegmentSymbolReadCount::default(),
        logical_returned: SegmentSymbolReadCount::default(),
        root: SegmentSymbolReadCount {
            calls: multiplier,
            bytes: multiplier * 10,
        },
        page: SegmentSymbolReadCount {
            calls: multiplier * 2,
            bytes: multiplier * 20,
        },
        page_validation: SegmentSymbolReadCount {
            calls: multiplier * 2,
            bytes: multiplier * 20,
        },
        page_validation_ns: multiplier * 30,
        touched_corrupt_pages: multiplier * 6,
        page_cache_hits: multiplier * 3,
        page_cache_misses: multiplier * 4,
        page_cache_evictions: multiplier * 5,
    }
}

fn benchmark_config_for_outputs(
    segments_dir: PathBuf,
    output: PathBuf,
    raw_output: PathBuf,
) -> QueryBenchmarkConfig {
    QueryBenchmarkConfig {
        segments_dir,
        output,
        raw_output: Some(raw_output),
        start_ms: 0,
        end_ms: 10_000,
        mode: QueryBenchmarkMode::Instant,
        range_scalar_cache_max_bytes: None,
        query_label_arena_max_bytes: DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES,
        chunk_read_mode: ChunkReadModeArg::Pread,
        chunk_read_queue_depth: 128,
        chunk_payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        queries: vec!["cpu.usage".to_string()],
        benchmark_repeats: 1,
        prewarm_query_contexts: false,
        prefetch_query_data: false,
        exponential_histogram_bucket_boundaries: Vec::new(),
        limits: QueryLimits::production_default(),
        validate_segment_footers: false,
    }
}

fn assert_no_benchmark_temp_files(directory: &Path) {
    if !directory.exists() {
        return;
    }
    for entry in fs::read_dir(directory).unwrap() {
        let name = entry.unwrap().file_name();
        assert!(
            !name.to_string_lossy().contains(".chronoxide-tmp-"),
            "temporary benchmark output was not cleaned up: {name:?}"
        );
    }
}

fn delta_projection_metadata() -> [(u64, TypedSampleMetadata); 8] {
    let metadata = |start_time_ms, reset_hint| TypedSampleMetadata {
        start_time_ms,
        temporality: OtlpAggregationTemporality::Delta,
        reset_hint,
        ..TypedSampleMetadata::default()
    };
    [
        (1_000, metadata(Some(0), CounterResetHint::Unknown)),
        (
            2_000,
            metadata(Some(1_000), CounterResetHint::NotCounterReset),
        ),
        (
            3_000,
            metadata(Some(2_500), CounterResetHint::NotCounterReset),
        ),
        (
            4_000,
            metadata(Some(2_500), CounterResetHint::NotCounterReset),
        ),
        (5_000, metadata(Some(4_000), CounterResetHint::CounterReset)),
        (6_000, metadata(Some(5_000), CounterResetHint::GaugeType)),
        (
            7_000,
            TypedSampleMetadata {
                flags: chronoxide_core::storage::head::OTLP_FLAG_NO_RECORDED_VALUE,
                temporality: OtlpAggregationTemporality::Delta,
                ..TypedSampleMetadata::default()
            },
        ),
        (
            8_000,
            metadata(Some(7_000), CounterResetHint::NotCounterReset),
        ),
    ]
}

fn delta_projection_u64_intervals() -> [(u64, TypedSampleMetadata, u64); 8] {
    let values = [1, 2, 4, 8, 16, 32, 64, 128];
    delta_projection_metadata().map(|(timestamp_ms, metadata)| {
        let value = values[usize::try_from(timestamp_ms / 1_000 - 1).unwrap()];
        (timestamp_ms, metadata, value)
    })
}

fn delta_projection_u64_expected() -> [(u64, f64); 8] {
    [
        (1_000, 1.0),
        (2_000, 3.0),
        (3_000, 4.0),
        (4_000, 8.0),
        (5_000, 16.0),
        (6_000, 32.0),
        (7_000, prometheus_stale_nan()),
        (8_000, 128.0),
    ]
}

fn assert_delta_projection_sequence(actual: &[(u64, f64)], expected: &[(u64, f64)]) {
    assert!(
        promql_samples_eq(actual, expected),
        "delta projection differs: actual={actual:?}, expected={expected:?}"
    );
}

fn segment_store_with_float_and_histogram() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 1.0), (2_000, 2.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "cpu.usage");
                visit("instance", "host-a");
            },
        )
        .unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(2),
            &[(
                1_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 2, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn schema7_segment_store_with_all_inline_kinds() -> tempfile::TempDir {
    segment_store_with_all_inline_kinds_for_schema(false)
}

fn schema8_segment_store_with_all_inline_kinds() -> tempfile::TempDir {
    segment_store_with_all_inline_kinds_for_schema(true)
}

fn segment_store_with_all_inline_kinds_for_schema(schema8: bool) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let config = if schema8 {
        config.with_storage_schema(SegmentStorageSchema::Schema8)
    } else {
        config.with_storage_schema(SegmentStorageSchema::Schema7)
    };
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 1.0), (2_000, 2.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_float");
                visit("kind", "float");
            },
        )
        .unwrap();
    writer
        .record_i64_samples_ordered_with_label_visitor(
            SeriesRef::new(2),
            &[(1_000, 7), (2_000, 9)],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_int64");
                visit("kind", "int64");
            },
        )
        .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(3),
            &[(
                3_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 2, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_histogram");
                visit("kind", "histogram");
            },
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(4),
            &[(
                4_000,
                ExponentialHistogramValue {
                    count: 5,
                    sum: Some(12.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![2, 3],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_exponential_histogram");
                visit("kind", "exponential_histogram");
            },
        )
        .unwrap();
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(5),
            &[(
                5_000,
                SummaryValue {
                    count: 10,
                    sum: 50.0,
                    metadata: TypedSampleMetadata::default(),
                    quantiles: vec![SummaryQuantileValue {
                        quantile: 0.5,
                        value: 4.0,
                    }],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_summary");
                visit("kind", "summary");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    tempdir
}

fn schema7_segment_store_with_inline_float() -> tempfile::TempDir {
    segment_store_with_inline_float_for_schema(false)
}

fn schema8_segment_store_with_inline_float() -> tempfile::TempDir {
    segment_store_with_inline_float_for_schema(true)
}

fn segment_store_with_inline_float_for_schema(schema8: bool) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let config = if schema8 {
        config.with_storage_schema(SegmentStorageSchema::Schema8)
    } else {
        config.with_storage_schema(SegmentStorageSchema::Schema7)
    };
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 1.0), (2_000, 2.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "schema7_float");
                visit("kind", "float");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    tempdir
}

fn schema7_segment_store_with_float_overflow() -> tempfile::TempDir {
    segment_store_with_float_overflow_for_schema(false)
}

fn schema8_segment_store_with_float_overflow() -> tempfile::TempDir {
    segment_store_with_float_overflow_for_schema(true)
}

fn segment_store_with_float_overflow_for_schema(schema8: bool) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let config = if schema8 {
        config.with_storage_schema(SegmentStorageSchema::Schema8)
    } else {
        config.with_storage_schema(SegmentStorageSchema::Schema7)
    };
    let mut writer = SegmentWriter::new(config).unwrap();
    for samples in [
        [(1_000, 1_000.0), (1_500, 1_500.0)],
        [(2_000, 2_000.0), (2_500, 2_500.0)],
    ] {
        writer
            .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &samples, |visit| {
                visit(METRIC_NAME_LABEL, "schema7_overflow");
                visit("kind", "float");
            })
            .unwrap();
    }
    writer.flush().unwrap();
    tempdir
}

fn replace_schema7_inline_locator(
    segment_dir: &Path,
    series_ref: u32,
    replacement: &ChunkIndexEntry,
) -> u64 {
    const SERIES_HEADER_LEN: usize = 176;
    const DESCRIPTOR_LEN: usize = 16;
    const HOT_PAGE_LEN: usize = 16_384;
    const HOT_PAGE_HEADER_LEN: usize = 24;
    const HOT_RECORD_LEN: usize = 40;
    const HOT_RECORDS_PER_PAGE: u32 = 409;

    assert_eq!(replacement.file_id, 1);
    let series_path = segment_dir.join(SegmentFile::Series.filename());
    let mut series = fs::read(&series_path).unwrap();
    let hot_pages_offset = usize::try_from(test_read_u64(&series, 80)).unwrap();
    let segment_start_ms = test_read_u64(&series, 144);
    let page_index = series_ref / HOT_RECORDS_PER_PAGE;
    let ordinal = usize::try_from(series_ref % HOT_RECORDS_PER_PAGE).unwrap();
    let page_offset = hot_pages_offset + usize::try_from(page_index).unwrap() * HOT_PAGE_LEN;
    let record_offset = page_offset + HOT_PAGE_HEADER_LEN + ordinal * HOT_RECORD_LEN;
    let control = test_read_u32(&series, record_offset + 16);
    assert_eq!((control >> 9) & 0b11, 1, "expected an inline hot record");
    assert_eq!((control >> 8) & 1, 0, "expected chunks.bin routing");
    assert_eq!((control >> 5) & 0b111, replacement.kind as u32);
    let original_offset = u64::from(test_read_u32(&series, record_offset + 28));

    let min_delta = u32::try_from(replacement.min_time_ms - segment_start_ms).unwrap();
    let max_delta = u32::try_from(replacement.max_time_ms - segment_start_ms).unwrap();
    let file_offset = u32::try_from(replacement.offset).unwrap();
    let prefix_crc = schema7_indexed_prefix_crc(segment_dir, replacement);
    test_put_u32(&mut series, record_offset + 16, control | (1 << 8));
    test_put_u32(&mut series, record_offset + 20, min_delta);
    test_put_u32(&mut series, record_offset + 24, max_delta);
    test_put_u32(&mut series, record_offset + 28, file_offset);
    test_put_u32(&mut series, record_offset + 32, replacement.length);
    test_put_u32(&mut series, record_offset + 36, prefix_crc);

    let page_crc = crc32c::crc32c(&series[page_offset..page_offset + HOT_PAGE_LEN]);
    let descriptor_offset =
        SERIES_HEADER_LEN + usize::try_from(page_index).unwrap() * DESCRIPTOR_LEN;
    test_put_u32(&mut series, descriptor_offset + 8, page_crc);
    series[52..56].fill(0);
    let root_crc = crc32c::crc32c(&series[..hot_pages_offset]);
    test_put_u32(&mut series, 52, root_crc);
    fs::write(series_path, series).unwrap();
    refresh_schema7_footer_file_length(segment_dir, SegmentFile::OooChunks);
    original_offset
}

fn set_schema7_inline_chunk_flags(segment_dir: &Path, series_ref: u32, flags: u16) {
    const SERIES_HEADER_LEN: usize = 176;
    const DESCRIPTOR_LEN: usize = 16;
    const HOT_PAGE_LEN: usize = 16_384;
    const HOT_PAGE_HEADER_LEN: usize = 24;
    const HOT_RECORD_LEN: usize = 40;
    const HOT_RECORDS_PER_PAGE: u32 = 409;

    let series_path = segment_dir.join(SegmentFile::Series.filename());
    let mut series = fs::read(&series_path).unwrap();
    let hot_pages_offset = usize::try_from(test_read_u64(&series, 80)).unwrap();
    let page_index = series_ref / HOT_RECORDS_PER_PAGE;
    let ordinal = usize::try_from(series_ref % HOT_RECORDS_PER_PAGE).unwrap();
    let page_offset = hot_pages_offset + usize::try_from(page_index).unwrap() * HOT_PAGE_LEN;
    let record_offset = page_offset + HOT_PAGE_HEADER_LEN + ordinal * HOT_RECORD_LEN;
    let control = test_read_u32(&series, record_offset + 16);
    assert_eq!((control >> 9) & 0b11, 1, "expected an inline hot record");
    assert_eq!((control >> 8) & 1, 0, "expected chunks.bin routing");
    let chunk_offset = usize::try_from(test_read_u32(&series, record_offset + 28)).unwrap();
    let scalar_lane_len = control >> 11;
    let prefix_len = if scalar_lane_len == 0 { 40 } else { 56 };

    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let mut chunks = fs::read(&chunks_path).unwrap();
    chunks[chunk_offset + 2..chunk_offset + 4].copy_from_slice(&flags.to_le_bytes());
    let indexed_prefix_crc = crc32c::crc32c(&chunks[chunk_offset..chunk_offset + prefix_len]);
    fs::write(chunks_path, chunks).unwrap();

    test_put_u32(&mut series, record_offset + 36, indexed_prefix_crc);
    let page_crc = crc32c::crc32c(&series[page_offset..page_offset + HOT_PAGE_LEN]);
    let descriptor_offset =
        SERIES_HEADER_LEN + usize::try_from(page_index).unwrap() * DESCRIPTOR_LEN;
    test_put_u32(&mut series, descriptor_offset + 8, page_crc);
    series[52..56].fill(0);
    let root_crc = crc32c::crc32c(&series[..hot_pages_offset]);
    test_put_u32(&mut series, 52, root_crc);
    fs::write(series_path, series).unwrap();
}

fn replace_schema7_overflow_locator(
    segment_dir: &Path,
    ordinal: u32,
    replacement: &ChunkIndexEntry,
) -> u64 {
    const CHUNK_INDEX_ROOT_LEN: usize = 64;
    const OVERFLOW_HEADER_LEN: usize = 32;
    const OVERFLOW_ENTRY_LEN: usize = 44;

    assert_eq!(replacement.file_id, 1);
    let index_path = segment_dir.join(SegmentFile::ChunkIndex.filename());
    let mut index = fs::read(&index_path).unwrap();
    assert_eq!(test_read_u32(&index, 24), 1, "expected one overflow blob");
    let chunk_count = test_read_u32(&index, CHUNK_INDEX_ROOT_LEN + 16);
    assert!(ordinal < chunk_count);
    let first_entry = CHUNK_INDEX_ROOT_LEN + OVERFLOW_HEADER_LEN;
    let first_in_order_offset = test_read_u64(&index, first_entry + 20);
    let entry_offset = first_entry + usize::try_from(ordinal).unwrap() * OVERFLOW_ENTRY_LEN;
    assert_eq!(index[entry_offset], 0, "expected chunks.bin routing");
    assert_eq!(index[entry_offset + 1], replacement.kind as u8);

    index[entry_offset] = replacement.file_id;
    index[entry_offset + 1] = replacement.kind as u8;
    index[entry_offset + 2..entry_offset + 4].fill(0);
    test_put_u64(&mut index, entry_offset + 4, replacement.min_time_ms);
    test_put_u64(&mut index, entry_offset + 12, replacement.max_time_ms);
    test_put_u64(&mut index, entry_offset + 20, replacement.offset);
    test_put_u32(&mut index, entry_offset + 28, replacement.length);
    test_put_u32(
        &mut index,
        entry_offset + 32,
        replacement.scalar_lane_offset,
    );
    test_put_u32(&mut index, entry_offset + 36, replacement.scalar_lane_len);
    test_put_u32(
        &mut index,
        entry_offset + 40,
        schema7_indexed_prefix_crc(segment_dir, replacement),
    );

    let blob_len = OVERFLOW_HEADER_LEN + usize::try_from(chunk_count).unwrap() * OVERFLOW_ENTRY_LEN;
    index[CHUNK_INDEX_ROOT_LEN + 28..CHUNK_INDEX_ROOT_LEN + 32].fill(0);
    let blob_crc = crc32c::crc32c(&index[CHUNK_INDEX_ROOT_LEN..CHUNK_INDEX_ROOT_LEN + blob_len]);
    test_put_u32(&mut index, CHUNK_INDEX_ROOT_LEN + 28, blob_crc);
    fs::write(index_path, index).unwrap();
    refresh_schema7_footer_file_length(segment_dir, SegmentFile::OooChunks);
    first_in_order_offset
}

fn refresh_schema7_footer_file_length(segment_dir: &Path, file: SegmentFile) {
    const FOOTER_HEADER_LEN: usize = 16;
    const FOOTER_ENTRY_LEN: usize = 20;

    let file_id = match file {
        SegmentFile::MetaJson => 1,
        SegmentFile::Symbols => 2,
        SegmentFile::Series => 3,
        SegmentFile::Chunks => 4,
        SegmentFile::OooChunks => 5,
        SegmentFile::ChunkIndex => 6,
        SegmentFile::Indexes => 7,
        SegmentFile::Footer => panic!("footer cannot inventory itself"),
    };
    let footer_path = segment_dir.join(SegmentFile::Footer.filename());
    let mut footer = fs::read(&footer_path).unwrap();
    let file_count = usize::from(u16::from_le_bytes(footer[16..18].try_into().unwrap()));
    let entry_start = (0..file_count)
        .map(|ordinal| FOOTER_HEADER_LEN + 4 + ordinal * FOOTER_ENTRY_LEN)
        .find(|offset| {
            u16::from_le_bytes(footer[*offset..*offset + 2].try_into().unwrap()) == file_id
        })
        .expect("footer must inventory the replacement file");
    let file_len = fs::metadata(segment_dir.join(file.filename()))
        .unwrap()
        .len();
    test_put_u64(&mut footer, entry_start + 4, file_len);
    let trailer_offset = footer.len() - 4;
    let footer_crc = crc32c::crc32c(&footer[..trailer_offset]);
    test_put_u32(&mut footer, trailer_offset, footer_crc);
    fs::write(footer_path, footer).unwrap();
}

fn schema7_indexed_prefix_crc(segment_dir: &Path, entry: &ChunkIndexEntry) -> u32 {
    let file = match entry.file_id {
        0 => SegmentFile::Chunks,
        1 => SegmentFile::OooChunks,
        other => panic!("unexpected chunk file ID {other}"),
    };
    let bytes = fs::read(segment_dir.join(file.filename())).unwrap();
    let offset = usize::try_from(entry.offset).unwrap();
    let prefix_len = if entry.scalar_lane_len == 0 { 40 } else { 56 };
    crc32c::crc32c(&bytes[offset..offset + prefix_len])
}

fn test_read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

fn test_read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn test_put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn test_put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn segment_store_with_histogram_counter_series() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let not_reset = TypedSampleMetadata {
        reset_hint: chronoxide_core::storage::head::CounterResetHint::NotCounterReset,
        ..TypedSampleMetadata::default()
    };

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(10.0),
                        min: Some(1.0),
                        max: Some(4.0),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0, 5.0],
                        bucket_counts: vec![1, 2, 1],
                    },
                ),
                (
                    4_000,
                    HistogramValue {
                        count: 10,
                        sum: Some(28.0),
                        min: Some(1.0),
                        max: Some(6.0),
                        metadata: not_reset,
                        explicit_bounds: vec![1.0, 5.0],
                        bucket_counts: vec![3, 4, 3],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "request_duration_range");
                visit("route", "/hist-range");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_int64() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_i64_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 7), (2_000, 9)],
            |visit| {
                visit(METRIC_NAME_LABEL, "queue_depth");
                visit("instance", "host-a");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_summary() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(
                5_000,
                SummaryValue {
                    count: 10,
                    sum: 50.0,
                    metadata: TypedSampleMetadata::default(),
                    quantiles: vec![
                        SummaryQuantileValue {
                            quantile: 0.5,
                            value: 4.0,
                        },
                        SummaryQuantileValue {
                            quantile: 0.9,
                            value: 8.0,
                        },
                    ],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request_latency");
                visit("route", "/summary");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_overlapping_histogram_counter_segments() -> tempfile::TempDir {
    segment_store_with_overlapping_histogram_counter_segments_for_schema(false)
}

fn schema8_segment_store_with_overlapping_histogram_counter_segments() -> tempfile::TempDir {
    segment_store_with_overlapping_histogram_counter_segments_for_schema(true)
}

fn segment_store_with_overlapping_histogram_counter_segments_for_schema(
    schema8: bool,
) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let labels = |visit: &mut dyn FnMut(&str, &str)| {
        visit(METRIC_NAME_LABEL, "overlap_duration");
        visit("route", "/overlap");
    };

    let broad_config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_deterministic_segment_ids(1);
    let broad_config = broad_config.with_storage_schema(if schema8 {
        SegmentStorageSchema::Schema8
    } else {
        SegmentStorageSchema::Schema6
    });
    let mut broad_writer = SegmentWriter::new(broad_config).unwrap();
    broad_writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 4,
                        sum: Some(10.0),
                        min: Some(1.0),
                        max: Some(4.0),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 3],
                    },
                ),
                (
                    4_000,
                    HistogramValue {
                        count: 50,
                        sum: Some(150.0),
                        min: Some(1.0),
                        max: Some(10.0),
                        metadata: TypedSampleMetadata {
                            reset_hint:
                                chronoxide_core::storage::head::CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![5, 45],
                    },
                ),
            ],
            labels,
        )
        .unwrap();
    broad_writer.flush().unwrap();

    let overlapping_config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_deterministic_segment_ids(2);
    let overlapping_config = overlapping_config.with_storage_schema(if schema8 {
        SegmentStorageSchema::Schema8
    } else {
        SegmentStorageSchema::Schema6
    });
    let mut overlapping_writer = SegmentWriter::new(overlapping_config).unwrap();
    overlapping_writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    2_000,
                    HistogramValue {
                        count: 20,
                        sum: Some(60.0),
                        min: Some(1.0),
                        max: Some(8.0),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![2, 18],
                    },
                ),
                (
                    3_000,
                    HistogramValue {
                        count: 40,
                        sum: Some(120.0),
                        min: Some(1.0),
                        max: Some(9.0),
                        metadata: TypedSampleMetadata {
                            reset_hint:
                                chronoxide_core::storage::head::CounterResetHint::NotCounterReset,
                            ..TypedSampleMetadata::default()
                        },
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![4, 36],
                    },
                ),
            ],
            labels,
        )
        .unwrap();
    overlapping_writer.flush().unwrap();

    tempdir
}

fn segment_store_with_sparse_final_window() -> tempfile::TempDir {
    segment_store_with_sparse_final_window_for_schema(SegmentStorageSchema::Schema8)
}

fn segment_store_with_sparse_final_window_for_schema(
    storage_schema: SegmentStorageSchema,
) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(600))
        .with_storage_schema(storage_schema);
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 1.0)], |visit| {
            visit(METRIC_NAME_LABEL, "sparse.cpu");
            visit("instance", "host-a");
        })
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_two_windows() -> tempfile::TempDir {
    segment_store_with_two_windows_for_layout(false)
}

fn segment_store_with_two_windows_schema7() -> tempfile::TempDir {
    segment_store_with_two_windows_for_layout(true)
}

fn segment_store_with_two_windows_for_layout(schema7: bool) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(if schema7 {
            SegmentStorageSchema::Schema7
        } else {
            SegmentStorageSchema::Schema8
        });
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), "published".to_string()),
            ],
            &[(5_000, 1.0)],
        )
        .unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(2),
            &[
                (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
                ("pod.name".to_string(), "orphan".to_string()),
            ],
            &[(15_000, 2.0)],
        )
        .unwrap();
    writer.flush().unwrap();
    tempdir
}

fn sorted_segment_metadata(segments_dir: &Path) -> Vec<SegmentMeta> {
    let mut segments = fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| {
            serde_json::from_slice::<SegmentMeta>(
                &fs::read(entry.path().join(SegmentFile::MetaJson.filename())).unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    segments.sort_by(|left, right| {
        left.start_ms
            .cmp(&right.start_ms)
            .then_with(|| left.end_ms.cmp(&right.end_ms))
            .then_with(|| left.segment_id.cmp(&right.segment_id))
    });
    segments
}

fn publish_manifest_segments(segments_dir: &Path, segments: &[&SegmentMeta]) {
    let manifest_dir = segments_dir.join("manifest");
    let mut writer = ManifestWriter::create(&manifest_dir, 99).unwrap();
    for meta in segments {
        writer
            .append(&ManifestRecord::SegmentSealed(
                ManifestSegment::new(meta.segment_id.clone(), meta.start_ms, meta.end_ms, None)
                    .unwrap(),
            ))
            .unwrap();
    }
    writer.sync_all().unwrap();
    write_current(&manifest_dir, writer.file_name()).unwrap();
}

fn segment_store_with_delta_histogram() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
        temporality: OtlpAggregationTemporality::Delta,
        ..TypedSampleMetadata::default()
    };

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    HistogramValue {
                        count: 1,
                        sum: Some(2.0),
                        min: Some(2.0),
                        max: Some(2.0),
                        metadata: metadata(0),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![0, 1],
                    },
                ),
                (
                    2_000,
                    HistogramValue {
                        count: 1,
                        sum: Some(3.0),
                        min: Some(3.0),
                        max: Some(3.0),
                        metadata: metadata(1_000),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 0],
                    },
                ),
                (
                    3_000,
                    HistogramValue {
                        count: 1,
                        sum: Some(4.0),
                        min: Some(4.0),
                        max: Some(4.0),
                        metadata: metadata(2_000),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![0, 1],
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "delta.request.duration");
                visit("route", "/delta");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_exponential_histogram() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(
                5_000,
                ExponentialHistogramValue {
                    count: 5,
                    sum: Some(12.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![2, 3],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: Vec::new(),
                    },
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "http.request.size");
                visit("route", "/exphist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_delta_exponential_histogram() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let metadata = |start_time_ms| TypedSampleMetadata {
        start_time_ms: Some(start_time_ms),
        temporality: OtlpAggregationTemporality::Delta,
        ..TypedSampleMetadata::default()
    };

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    ExponentialHistogramValue {
                        count: 1,
                        sum: Some(2.0),
                        min: None,
                        max: None,
                        metadata: metadata(0),
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![1, 0],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
                (
                    2_000,
                    ExponentialHistogramValue {
                        count: 1,
                        sum: Some(4.0),
                        min: None,
                        max: None,
                        metadata: metadata(1_000),
                        scale: 0,
                        zero_count: 0,
                        zero_threshold: 0.0,
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: vec![0, 1],
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "delta_http_request_size");
                visit("route", "/delta-exphist");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_long_float_series(schema: SegmentStorageSchema) -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(1))
        .with_storage_schema(schema);
    let mut writer = SegmentWriter::new(config).unwrap();
    let samples = (0..5_000)
        .map(|timestamp_ms| (timestamp_ms, timestamp_ms as f64))
        .collect::<Vec<_>>();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &samples, |visit| {
            visit(METRIC_NAME_LABEL, "long.range.cpu");
            visit("instance", "host-a");
        })
        .unwrap();
    writer.flush().unwrap();

    tempdir
}

fn segment_store_with_scalar_range_counters() -> tempfile::TempDir {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(3_600))
        .with_storage_schema(SegmentStorageSchema::Schema8);
    let mut writer = SegmentWriter::new(config).unwrap();
    let timestamps = [
        0, 300_000, 600_000, 900_000, 1_200_000, 1_500_000, 1_800_000,
    ];
    let float_values = [100.0, 110.0, prometheus_stale_nan(), 5.0, 9.0, 2.0, 6.0];
    let float_samples = timestamps.into_iter().zip(float_values).collect::<Vec<_>>();
    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &float_samples, |visit| {
            visit(METRIC_NAME_LABEL, "oracle_float_counter");
            visit("kind", "float");
        })
        .unwrap();

    let int64_values = [10, 20, 30, 5, 10, 15, 20];
    let int64_samples = timestamps.into_iter().zip(int64_values).collect::<Vec<_>>();
    writer
        .record_i64_samples_ordered_with_label_visitor(SeriesRef::new(2), &int64_samples, |visit| {
            visit(METRIC_NAME_LABEL, "oracle_int64_counter");
            visit("kind", "int64");
        })
        .unwrap();
    writer.flush().unwrap();

    tempdir
}
