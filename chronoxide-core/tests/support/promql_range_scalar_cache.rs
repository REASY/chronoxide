use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chronoxide_core::labels::{METRIC_NAME_LABEL, SeriesRef};
use chronoxide_core::promql::PromqlQueryError;
use chronoxide_core::storage::chunk::{ChunkIndexEntry, read_chunk_index, write_chunk_index};
use chronoxide_core::storage::head::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue,
    OTLP_FLAG_NO_RECORDED_VALUE, OtlpAggregationTemporality, SummaryQuantileValue, SummaryValue,
    TypedSampleMetadata,
};
use chronoxide_core::storage::segment::{
    DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES, QueryExecution, QueryLimits, QueryStats, SegmentFile,
    SegmentId, SegmentStorageSchema, SegmentStoreOpenOptions, SegmentStoreReader,
    SegmentStoreSchemaPolicy, SegmentWriter, SegmentWriterConfig,
};
use serde::{Deserialize, Serialize};

pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
pub const ORDINARY_NAN_BITS: u64 = 0x7ff8_0000_0000_0042;
pub const LARGE_COUNT: u64 = (1_u64 << 53) + 1;
pub const ERROR_ORACLE_SCHEMA_V1: &str = "chronoxide.promql-range-prechange-errors/v1";

pub type ExecutionLabelSet = Vec<(String, String)>;
pub type ExecutionSampleBits = Vec<(u64, u64)>;
pub type ExecutionRow = (ExecutionLabelSet, ExecutionSampleBits);

fn open_schema6_store(path: impl AsRef<Path>) -> SegmentStoreReader {
    SegmentStoreReader::open_with_options(
        path,
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap()
}

fn schema6_writer_config(
    path: impl AsRef<Path>,
    segment_duration: Duration,
) -> SegmentWriterConfig {
    SegmentWriterConfig::new(path, segment_duration)
        .with_storage_schema(SegmentStorageSchema::Schema6)
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheBypassKind {
    NoScalarLane,
}

#[allow(dead_code)]
impl CacheBypassKind {
    fn label(self) -> &'static str {
        match self {
            Self::NoScalarLane => "no-lane",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointProvenanceV1 {
    pub checkpoint_head: String,
    pub benchmark_binary_sha256: String,
    pub rustc: String,
    pub rustc_commit: String,
    pub host: String,
    pub llvm: String,
    pub cargo: String,
    pub os_product: String,
    pub os_version: String,
    pub os_build: String,
    pub working_tree_dirty: bool,
    pub query_promql_diff_sha256: String,
}

pub fn checkpoint_provenance_v1() -> CheckpointProvenanceV1 {
    CheckpointProvenanceV1 {
        checkpoint_head: "3c09d5da18a4dbaf09cc0e623f34085bb91c933c".to_string(),
        benchmark_binary_sha256: "37c044ca644f9496d0818f8c35ebbc5941c3b604adc166526a466a12ddfbc246"
            .to_string(),
        rustc: "rustc 1.95.0 (59807616e 2026-04-14)".to_string(),
        rustc_commit: "59807616e1fa2540724bfbac14d7976d7e4a3860".to_string(),
        host: "aarch64-apple-darwin".to_string(),
        llvm: "22.1.2".to_string(),
        cargo: "cargo 1.95.0 (f2d3ce0bd 2026-03-21)".to_string(),
        os_product: "macOS".to_string(),
        os_version: "26.5.2".to_string(),
        os_build: "25F84".to_string(),
        working_tree_dirty: true,
        query_promql_diff_sha256:
            "a2b5aea77bc55f35cafdc9cd8433e6bb2b87358a596968fe67ea3fe33a0fb8cd".to_string(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorOracleDocument {
    pub schema: String,
    pub checkpoint_provenance: CheckpointProvenanceV1,
    pub rows: Vec<ErrorOracleRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorOracleRow {
    pub id: String,
    pub api: String,
    pub expression: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub step_ms: u64,
    pub chunk_order: String,
    pub variant: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy)]
enum ErrorApi {
    Direct,
    Session,
}

impl ErrorApi {
    fn name(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Session => "session",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CorruptionKind {
    ScalarLane,
    FullPayload,
}

pub struct TypedRangeFixture {
    pub tempdir: tempfile::TempDir,
    pub store: SegmentStoreReader,
}

impl TypedRangeFixture {
    pub fn path(&self) -> &std::path::Path {
        self.tempdir.path()
    }

    pub fn run_range(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> QueryExecution {
        self.store
            .query_promql_range_with_limits(
                query,
                start_ms,
                end_ms,
                step_ms,
                QueryLimits::unlimited(),
            )
            .unwrap()
    }

    pub fn run_session_range(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> QueryExecution {
        self.store
            .query_session()
            .unwrap()
            .query_promql_range_with_limits(
                query,
                start_ms,
                end_ms,
                step_ms,
                QueryLimits::unlimited(),
            )
            .unwrap()
    }

    pub fn run_instant(&self, query: &str, start_ms: u64, end_ms: u64) -> QueryExecution {
        self.store
            .query_promql_with_limits(query, start_ms, end_ms, QueryLimits::unlimited())
            .unwrap()
    }
}

pub fn delta_histogram(
    count: u64,
    sum: Option<f64>,
    flags: u32,
    reset_hint: CounterResetHint,
    start_time_ms: Option<u64>,
) -> HistogramValue {
    HistogramValue {
        count,
        sum,
        min: None,
        max: None,
        metadata: TypedSampleMetadata {
            start_time_ms,
            flags,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint,
        },
        explicit_bounds: vec![1.0],
        bucket_counts: vec![count, 0],
    }
}

pub fn write_stale_reset_delta_fixture() -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        schema6_writer_config(tempdir.path(), Duration::from_secs(60))
            .with_deterministic_segment_ids(0x0ca5_e001),
    )
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    10_000,
                    delta_histogram(2, Some(2.0), 0, CounterResetHint::NotCounterReset, Some(0)),
                ),
                (
                    20_000,
                    delta_histogram(
                        3,
                        Some(3.0),
                        0,
                        CounterResetHint::NotCounterReset,
                        Some(10_000),
                    ),
                ),
                (
                    30_000,
                    delta_histogram(
                        0,
                        None,
                        OTLP_FLAG_NO_RECORDED_VALUE,
                        CounterResetHint::Unknown,
                        Some(20_000),
                    ),
                ),
                (
                    40_000,
                    delta_histogram(
                        7,
                        Some(7.0),
                        0,
                        CounterResetHint::CounterReset,
                        Some(30_000),
                    ),
                ),
                (
                    50_000,
                    delta_histogram(
                        4,
                        Some(4.0),
                        0,
                        CounterResetHint::NotCounterReset,
                        Some(40_000),
                    ),
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "cache");
                visit("route", "/stale-reset");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    let store = open_schema6_store(tempdir.path());
    TypedRangeFixture { tempdir, store }
}

fn case_metadata(flags: u32) -> TypedSampleMetadata {
    TypedSampleMetadata {
        start_time_ms: Some(0),
        flags,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::NotCounterReset,
    }
}

pub fn write_missing_sum_nan_fixture() -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        schema6_writer_config(tempdir.path(), Duration::from_secs(60))
            .with_deterministic_segment_ids(0x0ca5_e002),
    )
    .unwrap();
    let ordinary_nan = f64::from_bits(ORDINARY_NAN_BITS);
    let cases = [
        ("recorded-missing", 3, None, 0),
        ("recorded-nan", 4, Some(ordinary_nan), 0),
        ("stale-missing", 0, None, OTLP_FLAG_NO_RECORDED_VALUE),
        (
            "stale-nan",
            0,
            Some(ordinary_nan),
            OTLP_FLAG_NO_RECORDED_VALUE,
        ),
    ];
    for (idx, (case, count, sum, flags)) in cases.iter().copied().enumerate() {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(idx as u32 + 1),
                &[(
                    10_000,
                    HistogramValue {
                        count,
                        sum,
                        min: None,
                        max: None,
                        metadata: case_metadata(flags),
                        explicit_bounds: Vec::new(),
                        bucket_counts: vec![count],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "hist_sum_cases");
                    visit("case", case);
                    visit("kind", "histogram");
                },
            )
            .unwrap();
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(idx as u32 + 101),
                &[(
                    10_000,
                    ExponentialHistogramValue {
                        count,
                        sum,
                        min: None,
                        max: None,
                        scale: 0,
                        zero_threshold: 0.0,
                        zero_count: count,
                        metadata: case_metadata(flags),
                        positive: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                        negative: ExponentialHistogramBuckets {
                            offset: 0,
                            counts: Vec::new(),
                        },
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "exphist_sum_cases");
                    visit("case", case);
                    visit("kind", "exponential_histogram");
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();
    let store = open_schema6_store(tempdir.path());
    TypedRangeFixture { tempdir, store }
}

fn histogram_with_metadata(
    count: u64,
    sum: Option<f64>,
    metadata: TypedSampleMetadata,
) -> HistogramValue {
    HistogramValue {
        count,
        sum,
        min: None,
        max: None,
        metadata,
        explicit_bounds: Vec::new(),
        bucket_counts: vec![count],
    }
}

fn temporality_metadata(
    temporality: OtlpAggregationTemporality,
    reset_hint: CounterResetHint,
) -> TypedSampleMetadata {
    TypedSampleMetadata {
        start_time_ms: Some(0),
        flags: 0,
        temporality,
        reset_hint,
    }
}

pub fn write_mixed_temporality_fixture() -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        schema6_writer_config(tempdir.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(0x0ca5_e003),
    )
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    histogram_with_metadata(
                        10,
                        Some(100.0),
                        temporality_metadata(
                            OtlpAggregationTemporality::Cumulative,
                            CounterResetHint::NotCounterReset,
                        ),
                    ),
                ),
                (
                    2_000,
                    histogram_with_metadata(
                        3,
                        Some(30.0),
                        temporality_metadata(
                            OtlpAggregationTemporality::Delta,
                            CounterResetHint::CounterReset,
                        ),
                    ),
                ),
                (
                    3_000,
                    histogram_with_metadata(
                        14,
                        Some(140.0),
                        temporality_metadata(
                            OtlpAggregationTemporality::Unspecified,
                            CounterResetHint::NotCounterReset,
                        ),
                    ),
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "mixed_within");
                visit("boundary", "within-chunk");
            },
        )
        .unwrap();
    for (timestamp_ms, count, sum, temporality, reset_hint) in [
        (
            5_000,
            5,
            50.0,
            OtlpAggregationTemporality::Cumulative,
            CounterResetHint::NotCounterReset,
        ),
        (
            15_000,
            2,
            20.0,
            OtlpAggregationTemporality::Delta,
            CounterResetHint::CounterReset,
        ),
        (
            25_000,
            9,
            90.0,
            OtlpAggregationTemporality::Unspecified,
            CounterResetHint::NotCounterReset,
        ),
    ] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(2),
                &[(
                    (timestamp_ms),
                    histogram_with_metadata(
                        count,
                        Some(sum),
                        temporality_metadata(temporality, reset_hint),
                    ),
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "mixed_across");
                    visit("boundary", "across-segments");
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();
    let store = open_schema6_store(tempdir.path());
    TypedRangeFixture { tempdir, store }
}

pub fn write_start_time_reset_fixture() -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        schema6_writer_config(tempdir.path(), Duration::from_secs(60))
            .with_deterministic_segment_ids(0x0ca5_e004),
    )
    .unwrap();
    let sample = |count, start_time_ms, reset_hint| {
        histogram_with_metadata(
            count,
            Some(count as f64 * 10.0),
            TypedSampleMetadata {
                start_time_ms: Some(start_time_ms),
                flags: 0,
                temporality: OtlpAggregationTemporality::Delta,
                reset_hint,
            },
        )
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (10_000, sample(2, 0, CounterResetHint::NotCounterReset)),
                (20_000, sample(3, 10_000, CounterResetHint::NotCounterReset)),
                (30_000, sample(7, 25_000, CounterResetHint::CounterReset)),
                (40_000, sample(4, 30_000, CounterResetHint::NotCounterReset)),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "start_time_cases");
                visit("route", "/start-reset");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    let store = open_schema6_store(tempdir.path());
    TypedRangeFixture { tempdir, store }
}

pub fn write_large_count_fixture() -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        schema6_writer_config(tempdir.path(), Duration::from_secs(60))
            .with_deterministic_segment_ids(0x0ca5_e005),
    )
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (
                    1_000,
                    delta_histogram(
                        LARGE_COUNT,
                        None,
                        0,
                        CounterResetHint::NotCounterReset,
                        Some(0),
                    ),
                ),
                (
                    2_000,
                    delta_histogram(1, None, 0, CounterResetHint::NotCounterReset, Some(1_000)),
                ),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "large_count_cases");
                visit("route", "/large");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    let store = open_schema6_store(tempdir.path());
    TypedRangeFixture { tempdir, store }
}

pub fn write_duplicate_offset_fixture() -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        schema6_writer_config(tempdir.path(), Duration::from_secs(60))
            .with_deterministic_segment_ids(0x0ca5_e006),
    )
    .unwrap();
    let value = |count| {
        histogram_with_metadata(
            count,
            Some(count as f64),
            temporality_metadata(
                OtlpAggregationTemporality::Cumulative,
                CounterResetHint::NotCounterReset,
            ),
        )
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(0, value(9)), (10_000, value(1)), (10_000, value(2))],
            |visit| {
                visit(METRIC_NAME_LABEL, "offset_cases");
                visit("route", "/offsets");
            },
        )
        .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(20_000, value(3))],
            |visit| {
                visit(METRIC_NAME_LABEL, "offset_cases");
                visit("route", "/offsets");
            },
        )
        .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(20_000, value(4))],
            |visit| {
                visit(METRIC_NAME_LABEL, "offset_cases");
                visit("route", "/offsets");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    let store = open_schema6_store(tempdir.path());
    TypedRangeFixture { tempdir, store }
}

#[allow(dead_code)]
pub fn write_summary_scalar_fixture() -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        schema6_writer_config(tempdir.path(), Duration::from_secs(60))
            .with_deterministic_segment_ids(0x0ca5_e008),
    )
    .unwrap();
    let sample = |count, sum, median| SummaryValue {
        count,
        sum,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(0),
            flags: 0,
            temporality: OtlpAggregationTemporality::Unspecified,
            reset_hint: CounterResetHint::NotCounterReset,
        },
        quantiles: vec![SummaryQuantileValue {
            quantile: 0.5,
            value: median,
        }],
    };
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (10_000, sample(2, 20.0, 10.0)),
                (20_000, sample(3, 30.0, 12.0)),
                (30_000, sample(5, 50.0, 15.0)),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "summary_cache");
                visit("route", "/summary");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    let store = open_schema6_store(tempdir.path());
    TypedRangeFixture { tempdir, store }
}

#[allow(dead_code)]
pub fn write_scalar_cache_bypass_fixture(kind: CacheBypassKind) -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        schema6_writer_config(tempdir.path(), Duration::from_secs(60))
            .with_deterministic_segment_ids(0x0ca5_e009),
    )
    .unwrap();
    let value = |count, sum| {
        histogram_with_metadata(
            count,
            Some(sum),
            temporality_metadata(
                OtlpAggregationTemporality::Cumulative,
                CounterResetHint::NotCounterReset,
            ),
        )
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (10_000, value(2, 20.0)),
                (20_000, value(3, 30.0)),
                (30_000, value(5, 50.0)),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "cache_bypass");
                visit("layout", kind.label());
            },
        )
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let segment_dirs = deterministic_segment_dirs(tempdir.path());
    assert_eq!(
        segment_dirs.len(),
        1,
        "bypass fixture must target exactly one deterministic segment"
    );
    let store = open_schema6_store(tempdir.path());
    let chunk_index_path = segment_dirs[0].join(SegmentFile::ChunkIndex.filename());
    let original_len = fs::metadata(&chunk_index_path).unwrap().len();
    let mut index_file = File::open(&chunk_index_path).unwrap();
    let mut entries = read_chunk_index(&mut index_file).unwrap();
    let populated_series = entries
        .iter()
        .enumerate()
        .filter(|(_, series_entries)| !series_entries.is_empty())
        .map(|(series_ref, _)| series_ref)
        .collect::<Vec<_>>();
    assert_eq!(
        populated_series,
        vec![0],
        "bypass fixture must target only SeriesRef(1)'s dense index entry"
    );
    assert_eq!(
        entries[0].len(),
        1,
        "bypass fixture must target exactly one chunk entry"
    );
    let entry = &mut entries[0][0];
    assert_eq!(entry.file_id, 0);
    assert!(entry.scalar_lane_offset > 0);
    assert!(entry.scalar_lane_len > 0);
    match kind {
        CacheBypassKind::NoScalarLane => {
            entry.scalar_lane_offset = 0;
            entry.scalar_lane_len = 0;
        }
    }

    let mut replacement = tempfile::NamedTempFile::new_in(&segment_dirs[0]).unwrap();
    write_chunk_index(replacement.as_file_mut(), &entries).unwrap();
    replacement.as_file_mut().sync_all().unwrap();
    assert_eq!(
        replacement.as_file().metadata().unwrap().len(),
        original_len,
        "fixed-size chunk-index mutation must preserve every series range"
    );
    replacement.persist(&chunk_index_path).unwrap();

    let mut rewritten_file = File::open(&chunk_index_path).unwrap();
    let rewritten = read_chunk_index(&mut rewritten_file).unwrap();
    assert_eq!(
        rewritten, entries,
        "rewritten bypass index must remain fully decodable"
    );
    let rewritten_entry = &rewritten[0][0];
    match kind {
        CacheBypassKind::NoScalarLane => {
            assert_eq!(
                (
                    rewritten_entry.scalar_lane_offset,
                    rewritten_entry.scalar_lane_len
                ),
                (0, 0)
            );
        }
    }

    TypedRangeFixture { tempdir, store }
}

pub fn deterministic_segment_dirs(segments_dir: &Path) -> Vec<PathBuf> {
    let mut dirs = fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter_map(|entry| {
            let name = entry.file_name();
            SegmentId::parse_dir_name(&name.to_string_lossy())
                .is_ok()
                .then_some(entry.path())
        })
        .collect::<Vec<_>>();
    dirs.sort();
    dirs
}

struct ErrorFixture {
    _tempdir: tempfile::TempDir,
    store: SegmentStoreReader,
}

fn error_fixture(corruption: Option<(usize, CorruptionKind)>) -> ErrorFixture {
    error_fixture_with_counts(corruption, [1, 2])
}

fn error_fixture_with_counts(
    corruption: Option<(usize, CorruptionKind)>,
    counts: [u64; 2],
) -> ErrorFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        schema6_writer_config(tempdir.path(), Duration::from_secs(60))
            .with_deterministic_segment_ids(0x0ca5_e007),
    )
    .unwrap();
    let value = |count| HistogramValue {
        count,
        sum: Some(count as f64),
        min: None,
        max: None,
        metadata: temporality_metadata(
            OtlpAggregationTemporality::Cumulative,
            CounterResetHint::NotCounterReset,
        ),
        explicit_bounds: vec![1.0],
        bucket_counts: vec![count, 0],
    };
    for ((timestamp_ms, _), count) in [(1_000, 1), (2_000, 2)].into_iter().zip(counts) {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(timestamp_ms, value(count))],
                |visit| {
                    visit(METRIC_NAME_LABEL, "oracle_error");
                    visit("route", "/error");
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();
    let store = open_schema6_store(tempdir.path());
    let planned = store
        .query_promql_range_with_limits(
            "oracle_error_count",
            2_000,
            2_000,
            1_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(planned.stats.chunk_reads, 2);
    assert_eq!(planned.stats.samples_decoded, 2);
    assert_eq!(planned.stats.typed_scalar_chunks_decoded, 2);
    assert_eq!(
        sample_bits(&planned),
        vec![(2_000, (counts[1] as f64).to_bits())]
    );
    if let Some((entry_index, kind)) = corruption {
        let segment_dir = deterministic_segment_dirs(tempdir.path())
            .into_iter()
            .next()
            .expect("one deterministic error segment");
        let mut index_file =
            File::open(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
        let entries = read_chunk_index(&mut index_file).unwrap();
        let entries = entries
            .into_iter()
            .flatten()
            .collect::<Vec<ChunkIndexEntry>>();
        assert_eq!(
            entries.len(),
            2,
            "error fixture must contain exactly two chunks"
        );
        let entry = &entries[entry_index];
        let byte_offset = match kind {
            CorruptionKind::ScalarLane => {
                assert!(entry.scalar_lane_offset > 0);
                assert!(entry.scalar_lane_len > 16);
                entry
                    .offset
                    .checked_add(u64::from(entry.scalar_lane_offset))
                    .and_then(|offset| offset.checked_add(16))
                    .unwrap()
            }
            CorruptionKind::FullPayload => entry
                .offset
                .checked_add(u64::from(entry.length))
                .and_then(|offset| offset.checked_sub(1))
                .unwrap(),
        };
        let chunks = segment_dir.join(SegmentFile::Chunks.filename());
        flip_byte(&chunks, byte_offset);

        let expression = match kind {
            CorruptionKind::ScalarLane => "oracle_error_count",
            CorruptionKind::FullPayload => "oracle_error_bucket{le=\"1\"}",
        };
        let control = execute_range_error(
            &store,
            ErrorApi::Direct,
            expression,
            2_000,
            2_000,
            1_000,
            QueryLimits::unlimited(),
            DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES,
        );
        assert!(
            matches!(control, PromqlQueryError::Storage(_)),
            "unopposed corruption control must reach storage decode: {control:?}"
        );
    }
    ErrorFixture {
        _tempdir: tempdir,
        store,
    }
}

fn flip_byte(path: &Path, offset: u64) {
    let before_len = fs::metadata(path).unwrap().len();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    byte[0] ^= 0x80;
    file.write_all(&byte).unwrap();
    file.flush().unwrap();
    assert_eq!(fs::metadata(path).unwrap().len(), before_len);
}

#[expect(
    clippy::too_many_arguments,
    reason = "the error-precedence oracle keeps the selected API, range, limits, and cache budget explicit"
)]
fn execute_range_error(
    store: &SegmentStoreReader,
    api: ErrorApi,
    expression: &str,
    start_ms: u64,
    end_ms: u64,
    step_ms: u64,
    limits: QueryLimits,
    session_cache_budget_bytes: u64,
) -> PromqlQueryError {
    match api {
        ErrorApi::Direct => store
            .query_promql_range_with_limits(expression, start_ms, end_ms, step_ms, limits)
            .unwrap_err(),
        ErrorApi::Session => {
            let mut session = store.query_session().unwrap();
            session
                .set_range_scalar_cache_budget_bytes(session_cache_budget_bytes)
                .unwrap();
            session
                .query_promql_range_with_limits(expression, start_ms, end_ms, step_ms, limits)
                .unwrap_err()
        }
    }
}

pub fn error_variant(error: &PromqlQueryError) -> &'static str {
    match error {
        PromqlQueryError::Invalid(_) => "invalid",
        PromqlQueryError::Unsupported(_) => "unsupported",
        PromqlQueryError::LimitExceeded { .. } => "limit_exceeded",
        PromqlQueryError::Storage(_) => "storage",
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the oracle row helper keeps all recorded inputs and the expected error variant explicit"
)]
fn push_error_row(
    rows: &mut Vec<ErrorOracleRow>,
    id: &str,
    api: ErrorApi,
    expression: &str,
    start_ms: u64,
    end_ms: u64,
    step_ms: u64,
    chunk_order: &str,
    limits: QueryLimits,
    corruption: Option<(usize, CorruptionKind)>,
    expected_variant: &str,
    session_cache_budget_bytes: u64,
) {
    let fixture = error_fixture(corruption);
    let error = execute_range_error(
        &fixture.store,
        api,
        expression,
        start_ms,
        end_ms,
        step_ms,
        limits,
        session_cache_budget_bytes,
    );
    let variant = error_variant(&error);
    assert_eq!(
        variant, expected_variant,
        "unexpected winner for {id}: {error}"
    );
    rows.push(ErrorOracleRow {
        id: id.to_string(),
        api: api.name().to_string(),
        expression: expression.to_string(),
        start_ms,
        end_ms,
        step_ms,
        chunk_order: chunk_order.to_string(),
        variant: variant.to_string(),
        message: error.to_string(),
    });
}

pub fn build_error_oracle_document() -> ErrorOracleDocument {
    build_error_oracle_document_with_session_budget(DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES)
}

pub fn build_error_oracle_document_with_session_budget(
    session_cache_budget_bytes: u64,
) -> ErrorOracleDocument {
    let mut rows = Vec::new();
    let unlimited = QueryLimits::unlimited();
    for (api, suffix) in [(ErrorApi::Direct, "direct"), (ErrorApi::Session, "session")] {
        push_error_row(
            &mut rows,
            &format!("parse_before_bounds_{suffix}"),
            api,
            "rate(",
            2_000,
            1_000,
            0,
            "not-applicable",
            unlimited,
            None,
            "invalid",
            session_cache_budget_bytes,
        );
        push_error_row(
            &mut rows,
            &format!("zero_step_before_reversed_bounds_{suffix}"),
            api,
            "time()",
            2_000,
            1_000,
            0,
            "not-applicable",
            unlimited,
            None,
            "invalid",
            session_cache_budget_bytes,
        );
        push_error_row(
            &mut rows,
            &format!("reversed_bounds_{suffix}"),
            api,
            "time()",
            2_000,
            1_000,
            1_000,
            "not-applicable",
            unlimited,
            None,
            "invalid",
            session_cache_budget_bytes,
        );
    }
    push_error_row(
        &mut rows,
        "storage_scalar_lane_direct",
        ErrorApi::Direct,
        "oracle_error_count",
        2_000,
        2_000,
        1_000,
        "corrupt-first",
        unlimited,
        Some((0, CorruptionKind::ScalarLane)),
        "storage",
        session_cache_budget_bytes,
    );
    push_error_row(
        &mut rows,
        "storage_full_payload_session",
        ErrorApi::Session,
        "oracle_error_bucket{le=\"1\"}",
        2_000,
        2_000,
        1_000,
        "corrupt-first",
        unlimited,
        Some((0, CorruptionKind::FullPayload)),
        "storage",
        session_cache_budget_bytes,
    );

    let paired_cases = [
        (
            "matched_series_before_corruption",
            QueryLimits {
                max_matched_series: Some(0),
                ..unlimited
            },
            "limit_exceeded",
            "limit_exceeded",
        ),
        (
            "chunk_reads_before_corruption",
            QueryLimits {
                max_chunk_reads: Some(0),
                ..unlimited
            },
            "limit_exceeded",
            "limit_exceeded",
        ),
        (
            "bytes_read_before_corruption",
            QueryLimits {
                max_bytes_read: Some(0),
                ..unlimited
            },
            "limit_exceeded",
            "limit_exceeded",
        ),
        (
            "sample_limit_vs_corruption",
            QueryLimits {
                max_samples_decoded: Some(0),
                ..unlimited
            },
            "storage",
            "limit_exceeded",
        ),
        (
            "corrupt_chunk_before_own_sample_charge",
            QueryLimits {
                max_samples_decoded: Some(1),
                ..unlimited
            },
            "storage",
            "storage",
        ),
        (
            "projected_series_after_corruption",
            QueryLimits {
                max_projected_series: Some(0),
                ..unlimited
            },
            "storage",
            "storage",
        ),
    ];
    for (id, limits, corrupt_first_variant, good_first_variant) in paired_cases {
        for (entry_index, order, expected_variant) in [
            (0, "corrupt-first", corrupt_first_variant),
            (1, "good-first", good_first_variant),
        ] {
            push_error_row(
                &mut rows,
                &format!("{id}_{order}"),
                ErrorApi::Session,
                "oracle_error_count",
                2_000,
                2_000,
                1_000,
                order,
                limits,
                Some((entry_index, CorruptionKind::ScalarLane)),
                expected_variant,
                session_cache_budget_bytes,
            );
        }
    }

    for (order, counts) in [("low-value-first", [1, 2]), ("high-value-first", [2, 1])] {
        let fixture = error_fixture_with_counts(None, counts);
        let expression = "oracle_error_count";
        let limits = QueryLimits {
            max_projected_series: Some(0),
            max_samples_decoded: Some(0),
            ..unlimited
        };
        let error = execute_range_error(
            &fixture.store,
            ErrorApi::Session,
            expression,
            2_000,
            2_000,
            1_000,
            limits,
            session_cache_budget_bytes,
        );
        let variant = error_variant(&error);
        assert_eq!(variant, "limit_exceeded");
        rows.push(ErrorOracleRow {
            id: format!("sample_limit_before_projected_series_{order}"),
            api: ErrorApi::Session.name().to_string(),
            expression: expression.to_string(),
            start_ms: 2_000,
            end_ms: 2_000,
            step_ms: 1_000,
            chunk_order: order.to_string(),
            variant: variant.to_string(),
            message: error.to_string(),
        });
    }

    assert_eq!(rows.len(), 22);
    ErrorOracleDocument {
        schema: ERROR_ORACLE_SCHEMA_V1.to_string(),
        checkpoint_provenance: checkpoint_provenance_v1(),
        rows,
    }
}

pub fn pretty_json_with_newline<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    bytes
}

pub fn sample_bits(execution: &QueryExecution) -> Vec<(u64, u64)> {
    execution
        .results
        .iter()
        .flat_map(|result| result.samples.iter())
        .map(|(timestamp_ms, value)| (*timestamp_ms, value.to_bits()))
        .collect()
}

pub fn ordered_labels(execution: &QueryExecution) -> Vec<Vec<(String, String)>> {
    execution
        .results
        .iter()
        .map(|result| result.labels.to_vec())
        .collect()
}

pub fn execution_rows(execution: &QueryExecution) -> Vec<ExecutionRow> {
    execution
        .results
        .iter()
        .map(|result| {
            (
                result.labels.to_vec(),
                result
                    .samples
                    .iter()
                    .map(|(timestamp_ms, value)| (*timestamp_ms, value.to_bits()))
                    .collect(),
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn exact_stats(
    segments_considered: u64,
    segments_skipped_by_time: u64,
    segments_skipped_by_missing_equality: u64,
    segments_queried: u64,
    matched_series: u64,
    projected_series: u64,
    chunk_reads: u64,
    bytes_read: u64,
    samples_decoded: u64,
    typed_scalar_chunks_decoded: u64,
) -> QueryStats {
    // These focused fixtures have one single-ref metric-name posting in each
    // queried segment. QueryStats must now expose those exact reads because
    // unauthenticated routing and metric-range shortcuts no longer suppress
    // the canonical postings path.
    QueryStats {
        segments_considered,
        segments_skipped_by_time,
        segments_skipped_by_missing_equality,
        segments_skipped_by_matcher_time_range: 0,
        segments_queried,
        matched_series,
        projected_series,
        chunk_reads,
        bytes_read,
        samples_decoded,
        typed_scalar_chunks_decoded,
        typed_full_chunks_decoded: 0,
        regex_values_examined: 0,
        index_postings_reads: segments_queried,
        index_postings_bytes_read: segments_queried.saturating_mul(8),
    }
}
