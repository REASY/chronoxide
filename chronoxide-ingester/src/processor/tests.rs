use super::*;
use crate::app_config::LabelSetStoreKind;
use crate::source::SourceMessageMetadata;
use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, LabelSetStore, METRIC_NAME_LABEL,
};
use chronoxide_core::promql::{normalize_label_name, normalize_metric_name};
use chronoxide_core::storage::chunk::{ChunkKind, ChunkReader, ChunkSamples, read_chunk_index};
use chronoxide_core::storage::head::{
    CounterResetHint, HeadConfig, IntEncoding, OtlpAggregationTemporality, SeriesSamples,
};
use chronoxide_core::storage::index::read_segment_indexes;
use chronoxide_core::storage::segment::{
    QueryProjectionConfig, SegmentFile, SegmentMeta, SegmentStorageSchema, SegmentStoreReader,
    SegmentWriterConfig,
};
use chronoxide_core::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_HISTOGRAM, SERIES_KIND_SUMMARY, read_series_bin,
    read_symbols_bin,
};
use chronoxide_core::storage::wal::{OtlpWalBatch, WalWriter};
use chronoxide_core::storage::wal_replay::replay_wal_file_into_head;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, exponential_histogram_data_point::Buckets,
    summary_data_point::ValueAtQuantile,
};
use prost::Message;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::Path;
use std::process::Command;

fn kv_any(key: &str, value: tonic::common::v1::any_value::Value) -> tonic::common::v1::KeyValue {
    tonic::common::v1::KeyValue {
        key: key.to_string(),
        value: Some(tonic::common::v1::AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

fn kv_str(key: &str, value: &str) -> tonic::common::v1::KeyValue {
    kv_any(
        key,
        tonic::common::v1::any_value::Value::StringValue(value.to_string()),
    )
}

fn kv_bool(key: &str, value: bool) -> tonic::common::v1::KeyValue {
    kv_any(key, tonic::common::v1::any_value::Value::BoolValue(value))
}

fn kv_int(key: &str, value: i64) -> tonic::common::v1::KeyValue {
    kv_any(key, tonic::common::v1::any_value::Value::IntValue(value))
}

fn kv_double(key: &str, value: f64) -> tonic::common::v1::KeyValue {
    kv_any(key, tonic::common::v1::any_value::Value::DoubleValue(value))
}

fn kv_bytes(key: &str, value: &[u8]) -> tonic::common::v1::KeyValue {
    kv_any(
        key,
        tonic::common::v1::any_value::Value::BytesValue(value.to_vec()),
    )
}

fn kv_array(key: &str) -> tonic::common::v1::KeyValue {
    kv_any(
        key,
        tonic::common::v1::any_value::Value::ArrayValue(tonic::common::v1::ArrayValue {
            values: vec![],
        }),
    )
}

fn kv_kvlist(key: &str) -> tonic::common::v1::KeyValue {
    kv_any(
        key,
        tonic::common::v1::any_value::Value::KvlistValue(tonic::common::v1::KeyValueList {
            values: vec![],
        }),
    )
}

fn number_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::NumberDataPoint {
    tonic::metrics::v1::NumberDataPoint {
        attributes: attrs,
        time_unix_nano: 2_000_000_000,
        ..Default::default()
    }
}

fn histogram_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::HistogramDataPoint {
    tonic::metrics::v1::HistogramDataPoint {
        attributes: attrs,
        time_unix_nano: 2_000_000_000,
        ..Default::default()
    }
}

fn exp_histogram_dp(
    attrs: Vec<tonic::common::v1::KeyValue>,
) -> tonic::metrics::v1::ExponentialHistogramDataPoint {
    tonic::metrics::v1::ExponentialHistogramDataPoint {
        attributes: attrs,
        time_unix_nano: 2_000_000_000,
        ..Default::default()
    }
}

fn summary_dp(attrs: Vec<tonic::common::v1::KeyValue>) -> tonic::metrics::v1::SummaryDataPoint {
    tonic::metrics::v1::SummaryDataPoint {
        attributes: attrs,
        time_unix_nano: 2_000_000_000,
        ..Default::default()
    }
}

fn metric_gauge(
    name: &str,
    dps: Vec<tonic::metrics::v1::NumberDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Gauge(
            tonic::metrics::v1::Gauge { data_points: dps },
        )),
        ..Default::default()
    }
}

fn metric_sum(
    name: &str,
    dps: Vec<tonic::metrics::v1::NumberDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Sum(
            tonic::metrics::v1::Sum {
                data_points: dps,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn metric_histogram(
    name: &str,
    dps: Vec<tonic::metrics::v1::HistogramDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Histogram(
            tonic::metrics::v1::Histogram {
                data_points: dps,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn metric_exp_histogram(
    name: &str,
    dps: Vec<tonic::metrics::v1::ExponentialHistogramDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(
            tonic::metrics::v1::ExponentialHistogram {
                data_points: dps,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn metric_summary(
    name: &str,
    dps: Vec<tonic::metrics::v1::SummaryDataPoint>,
) -> tonic::metrics::v1::Metric {
    tonic::metrics::v1::Metric {
        name: name.to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Summary(
            tonic::metrics::v1::Summary { data_points: dps },
        )),
        ..Default::default()
    }
}

fn request(
    resource_attrs: Vec<tonic::common::v1::KeyValue>,
    metrics: Vec<tonic::metrics::v1::Metric>,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![tonic::metrics::v1::ResourceMetrics {
            resource: Some(tonic::resource::v1::Resource {
                attributes: resource_attrs,
                ..Default::default()
            }),
            scope_metrics: vec![tonic::metrics::v1::ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn segment_dir_count(segments_dir: &std::path::Path) -> usize {
    fs::read_dir(segments_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .count()
}

fn snapshot_tree(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn visit(root: &Path, dir: &Path, files: &mut Vec<(String, Vec<u8>)>) {
        let mut entries = fs::read_dir(dir)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                files.push((relative, fs::read(path).unwrap()));
            }
        }
    }

    let mut files = Vec::new();
    visit(root, root, &mut files);
    files
}

fn write_partition_drain_fixture(segments_dir: &Path, reverse: bool) {
    fs::create_dir_all(segments_dir).unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(segments_dir, Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6)
            .with_deterministic_segment_ids(0x5eed),
    )
    .unwrap();
    let head = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(6));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head),
        Some(writer),
    )
    .with_shutdown_report(false);

    let partitions: Vec<i32> = if reverse {
        (0..16).rev().collect()
    } else {
        (0..16).collect()
    };
    for partition in partitions {
        // Every partition drains the same [10s, 20s) range in both lanes.
        // This makes byte determinism depend on the complete
        // (range, partition, lane) order rather than accidentally sorting by
        // distinct time ranges before partition or lane can matter.
        for (ordinal, timestamp_ms) in [15_000_u64, 12_000].into_iter().enumerate() {
            let mut point = number_dp(vec![kv_str("host", "shared")]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(
                i64::from(partition) * 2 + i64::try_from(ordinal).unwrap(),
            ));
            processor
                .process(
                    SourceMessageMetadata {
                        topic: "metrics".to_owned(),
                        partition,
                        offset: i64::from(partition) * 2 + i64::try_from(ordinal).unwrap(),
                        timestamp_ms: timestamp_ms as i64,
                        captured_at_ms: 15_000,
                    },
                    request(vec![], vec![metric_gauge("drain.order", vec![point])]),
                )
                .unwrap();
        }
    }
    processor.flush_head().unwrap();
}

#[test]
fn processor_partition_drain_is_byte_deterministic_across_fresh_processes() {
    const CHILD_DIR_ENV: &str = "CHRONOXIDE_PARTITION_DRAIN_CHILD_DIR";
    const CHILD_REVERSE_ENV: &str = "CHRONOXIDE_PARTITION_DRAIN_CHILD_REVERSE";
    if let Some(segments_dir) = std::env::var_os(CHILD_DIR_ENV) {
        write_partition_drain_fixture(
            Path::new(&segments_dir),
            std::env::var_os(CHILD_REVERSE_ENV).is_some(),
        );
        return;
    }

    let tempdir = tempfile::tempdir().unwrap();
    let mut snapshots = Vec::new();
    for ordinal in 0..4 {
        let segments_dir = tempdir.path().join(format!("run-{ordinal}"));
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .arg("processor_partition_drain_is_byte_deterministic_across_fresh_processes")
            .arg("--nocapture")
            .env(CHILD_DIR_ENV, &segments_dir);
        if ordinal % 2 == 1 {
            command.env(CHILD_REVERSE_ENV, "1");
        }
        let status = command.status().unwrap();
        assert!(status.success(), "fresh-process fixture {ordinal} failed");
        snapshots.push(snapshot_tree(&segments_dir));
    }

    for snapshot in &snapshots[1..] {
        assert_eq!(snapshot, &snapshots[0]);
    }
}

fn open_default_store(segments_dir: &std::path::Path) -> SegmentStoreReader {
    SegmentStoreReader::open(segments_dir).unwrap()
}

fn read_segment_meta(segment_dir: &std::path::Path) -> SegmentMeta {
    serde_json::from_slice(&fs::read(segment_dir.join(SegmentFile::MetaJson.filename())).unwrap())
        .unwrap()
}

fn collect_labelset(processor: &OtlpLabelSetProcessor, series: SeriesRef) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    match &processor.labelsets {
        LabelSetInterner::Naive(store) => {
            store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
        }
        LabelSetInterner::FlatInterned(store) => {
            store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
        }
        LabelSetInterner::KeySetDictEncoded(store) => {
            store.visit_labelset(series, |k, v| out.push((k.to_string(), v.to_string())))
        }
    }
    out.sort();
    out
}

fn reset_hints_for_metric(
    samples_by_labels: &BTreeMap<Vec<(String, String)>, SeriesSamples>,
    metric_name: &str,
) -> Vec<CounterResetHint> {
    let samples = samples_by_labels
        .iter()
        .find_map(|(labels, samples)| {
            labels
                .iter()
                .any(|(key, value)| key == METRIC_NAME_LABEL && value == metric_name)
                .then_some(samples)
        })
        .unwrap_or_else(|| {
            let available = samples_by_labels
                .keys()
                .flat_map(|labels| labels.iter())
                .filter_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value))
                .collect::<Vec<_>>();
            panic!("missing samples for metric {metric_name}; available={available:?}")
        });
    match samples {
        SeriesSamples::Histogram { samples } => samples
            .iter()
            .map(|(_, value)| value.metadata.reset_hint)
            .collect(),
        SeriesSamples::ExponentialHistogram { samples } => samples
            .iter()
            .map(|(_, value)| value.metadata.reset_hint)
            .collect(),
        other => panic!("expected typed histogram samples, got {other:?}"),
    }
}

#[test]
fn labelset_interner_builds_segment_metadata_for_all_store_kinds() {
    let labels = [
        KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        KeyValueRef::from(("namespace", "default")),
        KeyValueRef::from(("pod.name", "backend-1")),
    ];
    let mut expected = SegmentSeriesMetadataBuilder::new();
    for label in &labels {
        expected.push_label(label.key, label.value);
    }
    let expected = expected.finish();

    for store in [
        LabelSetStoreKind::Naive,
        LabelSetStoreKind::FlatInterned,
        LabelSetStoreKind::ExperimentalFlatInternedPaged,
        LabelSetStoreKind::ExperimentalFlatInternedCanonicalStringHash,
        LabelSetStoreKind::ExperimentalFlatInternedSipHash,
        LabelSetStoreKind::ExperimentalFlatInternedSipHashSymbols,
        LabelSetStoreKind::KeySetDictEncoded,
    ] {
        let mut stats = OtlpMetricsIngestionStats::new();
        let mut interner = LabelSetInterner::new(store);
        let series = interner.intern(&labels, &mut stats).unwrap();

        let metadata = interner.segment_metadata(series);

        assert_eq!(metadata.series_id(), expected.series_id());
        assert_eq!(metadata.labels(), expected.labels());
    }
}

#[test]
fn flat_interned_config_selects_default_and_experimental_comparators() {
    let contiguous = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let paged = LabelSetInterner::new(LabelSetStoreKind::ExperimentalFlatInternedPaged);
    let canonical_string_hash =
        LabelSetInterner::new(LabelSetStoreKind::ExperimentalFlatInternedCanonicalStringHash);
    let siphash = LabelSetInterner::new(LabelSetStoreKind::ExperimentalFlatInternedSipHash);
    let siphash_symbols =
        LabelSetInterner::new(LabelSetStoreKind::ExperimentalFlatInternedSipHashSymbols);

    assert_eq!(
        contiguous
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .key_values_storage,
        "contiguous"
    );
    assert_eq!(
        contiguous
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "interned_ids_ahash"
    );
    assert_eq!(contiguous.kind(), "FlatInterned");
    assert_eq!(
        paged
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .key_values_storage,
        "paged"
    );
    assert_eq!(paged.kind(), "ExperimentalFlatInternedPaged");
    assert_eq!(
        paged
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "interned_ids_ahash"
    );
    assert_eq!(
        canonical_string_hash
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .key_values_storage,
        "contiguous"
    );
    assert_eq!(
        canonical_string_hash
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "canonical_strings"
    );
    assert_eq!(
        canonical_string_hash.kind(),
        "ExperimentalFlatInternedCanonicalStringHash"
    );
    assert_eq!(
        siphash
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "interned_ids_siphash"
    );
    assert_eq!(siphash.kind(), "ExperimentalFlatInternedSipHash");
    assert_eq!(
        siphash_symbols
            .as_flat_interned()
            .expect("flat interner")
            .buffer_stats()
            .labelset_hash,
        "interned_ids_ahash"
    );
    assert_eq!(
        siphash_symbols
            .as_flat_interned()
            .expect("flat interner")
            .symbols()
            .symbol_hash_kind(),
        "siphash"
    );
    assert_eq!(
        siphash_symbols.kind(),
        "ExperimentalFlatInternedSipHashSymbols"
    );
}

fn assert_flat_metric_order_matches_owned_reference(
    interner: &LabelSetInterner,
    source: Vec<(SeriesRef, SeriesSamples)>,
) -> Vec<(SeriesRef, SeriesSamples)> {
    let mut indirect = source.clone();
    let mut reference = source;
    let canonical_label_counts =
        order_series_samples_for_metric_query(&mut indirect, interner).unwrap();
    order_flat_interned_series_samples_for_metric_query_owned_reference(
        &mut reference,
        interner.as_flat_interned().unwrap(),
    )
    .unwrap();
    assert_eq!(indirect, reference);
    assert_eq!(
        canonical_label_counts,
        indirect
            .iter()
            .map(|(series, _)| {
                checked_canonical_label_count(interner.segment_metadata(*series).labels().len())
                    .unwrap()
            })
            .collect::<Vec<_>>()
    );
    indirect
}

fn metric_order_test_samples(kind: usize, marker: u64) -> SeriesSamples {
    match kind % 5 {
        0 => SeriesSamples::Float {
            encoding: FloatEncoding::Gorilla,
            samples: vec![(marker, marker as f64)],
        },
        1 => SeriesSamples::Int64 {
            encoding: IntEncoding::DeltaZigZag,
            samples: vec![(marker, marker as i64)],
        },
        2 => SeriesSamples::Histogram {
            samples: Vec::new(),
        },
        3 => SeriesSamples::ExponentialHistogram {
            samples: Vec::new(),
        },
        _ => SeriesSamples::Summary {
            samples: Vec::new(),
        },
    }
}

#[test]
fn metric_query_order_returns_empty_and_singleton_label_counts() {
    for store_kind in [LabelSetStoreKind::FlatInterned, LabelSetStoreKind::Naive] {
        let mut stats = OtlpMetricsIngestionStats::new();
        let mut interner = LabelSetInterner::new(store_kind);
        let mut empty = Vec::new();

        assert_eq!(
            order_series_samples_for_metric_query(&mut empty, &interner).unwrap(),
            Vec::<u32>::new()
        );

        let normalized_name = normalize_label_name("pod.name");
        let series = interner
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "singleton.metric")),
                    KeyValueRef::from(("pod.name", "first")),
                    KeyValueRef::from((normalized_name.as_str(), "last")),
                ],
                &mut stats,
            )
            .unwrap();
        let sample = metric_order_test_samples(0, 1);
        let mut singleton = vec![(series, sample.clone())];
        let expected_count =
            u32::try_from(interner.segment_metadata(series).labels().len()).unwrap();

        assert_eq!(
            order_series_samples_for_metric_query(&mut singleton, &interner).unwrap(),
            vec![expected_count]
        );
        assert_eq!(singleton, vec![(series, sample)]);
    }
}

#[test]
fn fallback_metric_query_order_returns_label_counts_in_reordered_series_order() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::Naive);
    let a_series = interner
        .intern(
            &[KeyValueRef::from((METRIC_NAME_LABEL, "a.metric"))],
            &mut stats,
        )
        .unwrap();
    let normalized_name = normalize_label_name("pod.name");
    let middle_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "middle.metric")),
                KeyValueRef::from(("pod.name", "first")),
                KeyValueRef::from((normalized_name.as_str(), "last")),
            ],
            &mut stats,
        )
        .unwrap();
    let z_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "z.metric")),
                KeyValueRef::from(("namespace", "default")),
                KeyValueRef::from(("pod", "backend")),
            ],
            &mut stats,
        )
        .unwrap();
    let sample = metric_order_test_samples(0, 1);
    let mut source = vec![
        (z_series, sample.clone()),
        (middle_series, sample.clone()),
        (a_series, sample),
    ];

    let canonical_label_counts =
        order_series_samples_for_metric_query(&mut source, &interner).unwrap();

    assert_eq!(
        source.iter().map(|(series, _)| *series).collect::<Vec<_>>(),
        vec![a_series, middle_series, z_series]
    );
    assert_eq!(canonical_label_counts, vec![1, 2, 3]);
    assert_eq!(
        canonical_label_counts,
        source
            .iter()
            .map(|(series, _)| {
                u32::try_from(interner.segment_metadata(*series).labels().len()).unwrap()
            })
            .collect::<Vec<_>>()
    );
}

#[cfg(target_pointer_width = "64")]
#[test]
fn canonical_metric_order_label_count_rejects_u32_overflow() {
    let error = checked_canonical_label_count(u32::MAX as usize + 1).unwrap_err();

    let crate::error::ErrorKind::IoError(error) = error.kind() else {
        panic!("expected an I/O error");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "canonical metric-order label count exceeds u32"
    );
}

#[test]
fn flat_metric_query_order_matches_metadata_order_for_normalized_labels() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let z_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "z.metric")),
                KeyValueRef::from(("pod.name", "z")),
            ],
            &mut stats,
        )
        .unwrap();
    let normalized_collision_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same.metric")),
                KeyValueRef::from(("a.label", "dropped")),
                KeyValueRef::from(("a_label", "kept")),
            ],
            &mut stats,
        )
        .unwrap();
    let a_series = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "a.metric")),
                KeyValueRef::from(("pod.name", "a")),
            ],
            &mut stats,
        )
        .unwrap();

    let samples = SeriesSamples::Float {
        encoding: FloatEncoding::Gorilla,
        samples: vec![(1_000, 1.0)],
    };
    let source = vec![
        (z_series, samples.clone()),
        (normalized_collision_series, samples.clone()),
        (a_series, samples),
    ];
    let fast = assert_flat_metric_order_matches_owned_reference(&interner, source.clone());
    let mut fallback = source;

    let fallback_label_counts =
        order_series_samples_for_metric_query_with_metadata(&mut fallback, &interner).unwrap();

    let fast_refs: Vec<_> = fast.iter().map(|(series, _)| *series).collect();
    let fallback_refs: Vec<_> = fallback.iter().map(|(series, _)| *series).collect();
    assert_eq!(fast_refs, fallback_refs);
    assert_eq!(fallback_label_counts, vec![2, 3, 2]);
    assert_eq!(
        fast_refs,
        vec![a_series, normalized_collision_series, z_series]
    );
}

#[test]
fn metric_query_order_uses_source_series_ref_after_normalized_label_collision() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let normalized_name = normalize_label_name("a.label");
    let first = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same.metric")),
                KeyValueRef::from(("a.label", "same")),
            ],
            &mut stats,
        )
        .unwrap();
    let second = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same.metric")),
                KeyValueRef::from((normalized_name.as_str(), "same")),
            ],
            &mut stats,
        )
        .unwrap();
    assert_ne!(first, second);
    let first_metadata = interner.segment_metadata(first);
    let second_metadata = interner.segment_metadata(second);
    assert_eq!(
        (first_metadata.series_id(), first_metadata.labels()),
        (second_metadata.series_id(), second_metadata.labels()),
        "test setup must reach the exact canonical ordering tie"
    );

    let samples = SeriesSamples::Float {
        encoding: FloatEncoding::Gorilla,
        samples: vec![(1_000, 1.0)],
    };
    let expected = vec![first.min(second), first.max(second)];

    for input in [vec![first, second], vec![second, first]] {
        let source = input
            .into_iter()
            .map(|series| (series, samples.clone()))
            .collect::<Vec<_>>();
        let fast = assert_flat_metric_order_matches_owned_reference(&interner, source.clone());
        let mut fallback = source;

        order_series_samples_for_metric_query_with_metadata(&mut fallback, &interner).unwrap();

        assert_eq!(
            fast.iter().map(|(series, _)| *series).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(fast, fallback);
    }
}

#[test]
fn flat_metric_query_order_handles_metric_name_edges() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let raw_metric = "raw.metric";
    let normalized_metric = normalize_metric_name(raw_metric);
    let cases = [
        (Vec::new(), String::new()),
        (vec![KeyValueRef::from(("tag", "same"))], String::new()),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, "")),
                KeyValueRef::from(("tag", "same")),
            ],
            normalize_metric_name(""),
        ),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, "9invalid.metric")),
                KeyValueRef::from(("tag", "same")),
            ],
            normalize_metric_name("9invalid.metric"),
        ),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, raw_metric)),
                KeyValueRef::from(("tag", "same")),
            ],
            normalized_metric.clone(),
        ),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, normalized_metric.as_str())),
                KeyValueRef::from(("tag", "same")),
            ],
            normalized_metric.clone(),
        ),
        (
            vec![
                KeyValueRef::from((METRIC_NAME_LABEL, "z.first")),
                KeyValueRef::from(("tag", "same")),
            ],
            normalize_metric_name("z.first"),
        ),
    ];

    let mut expected = Vec::new();
    for (labels, projected_metric) in cases {
        let series = interner.intern(&labels, &mut stats).unwrap();
        expected.push((projected_metric, series));
    }
    expected.sort();

    let source = expected
        .iter()
        .rev()
        .enumerate()
        .map(|(index, (_, series))| (*series, metric_order_test_samples(0, index as u64 + 1)))
        .collect::<Vec<_>>();
    let ordered = assert_flat_metric_order_matches_owned_reference(&interner, source);

    assert_eq!(
        ordered
            .iter()
            .map(|(series, _)| *series)
            .collect::<Vec<_>>(),
        expected
            .into_iter()
            .map(|(_, series)| series)
            .collect::<Vec<_>>()
    );
}

#[test]
fn flat_metric_query_order_keeps_last_normalized_label_collision() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let raw_name = "a.label";
    let normalized_name = normalize_label_name(raw_name);
    let collision = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same")),
                KeyValueRef::from((raw_name, "z-first")),
                KeyValueRef::from((normalized_name.as_str(), "a-last")),
            ],
            &mut stats,
        )
        .unwrap();
    let middle = interner
        .intern(
            &[
                KeyValueRef::from((METRIC_NAME_LABEL, "same")),
                KeyValueRef::from((normalized_name.as_str(), "m-middle")),
            ],
            &mut stats,
        )
        .unwrap();
    let samples = metric_order_test_samples(0, 1);

    for refs in [[collision, middle], [middle, collision]] {
        let ordered = assert_flat_metric_order_matches_owned_reference(
            &interner,
            refs.into_iter()
                .map(|series| (series, samples.clone()))
                .collect(),
        );
        assert_eq!(
            ordered
                .iter()
                .map(|(series, _)| *series)
                .collect::<Vec<_>>(),
            [collision, middle]
        );
    }
}

#[test]
fn flat_metric_query_order_matches_reference_for_all_sample_kinds() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let series = interner
        .intern(
            &[KeyValueRef::from((METRIC_NAME_LABEL, "same"))],
            &mut stats,
        )
        .unwrap();
    let source = vec![
        (series, metric_order_test_samples(4, 10)),
        (series, metric_order_test_samples(1, 20)),
        (series, metric_order_test_samples(3, 30)),
        (series, metric_order_test_samples(0, 40)),
        (series, metric_order_test_samples(2, 50)),
    ];
    let ordered = assert_flat_metric_order_matches_owned_reference(&interner, source);

    assert!(matches!(ordered[0].1, SeriesSamples::Int64 { .. }));
    assert!(matches!(ordered[1].1, SeriesSamples::Float { .. }));
    assert!(matches!(ordered[2].1, SeriesSamples::Histogram { .. }));
    assert!(matches!(
        ordered[3].1,
        SeriesSamples::ExponentialHistogram { .. }
    ));
    assert!(matches!(ordered[4].1, SeriesSamples::Summary { .. }));
}

#[test]
fn flat_metric_query_order_uses_original_index_as_the_final_tie_break() {
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let series = interner
        .intern(
            &[KeyValueRef::from((METRIC_NAME_LABEL, "same"))],
            &mut stats,
        )
        .unwrap();
    let source = vec![
        (series, metric_order_test_samples(0, 30)),
        (series, metric_order_test_samples(0, 10)),
        (series, metric_order_test_samples(0, 20)),
    ];
    let ordered = assert_flat_metric_order_matches_owned_reference(&interner, source.clone());

    assert_eq!(ordered, source);
}

#[test]
fn flat_metric_query_order_matches_owned_reference_for_generated_inputs() {
    for store_kind in [
        LabelSetStoreKind::FlatInterned,
        LabelSetStoreKind::ExperimentalFlatInternedPaged,
    ] {
        let mut stats = OtlpMetricsIngestionStats::new();
        let mut interner = LabelSetInterner::new(store_kind);
        let raw_label = "generated.label";
        let normalized_label = normalize_label_name(raw_label);
        let raw_metric = "generated.metric";
        let normalized_metric = normalize_metric_name(raw_metric);
        let mut source = Vec::new();
        let mut state = 0x9e37_79b9_u32;

        for index in 0..512_usize {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let metric_case = (state as usize + index) % 6;
            let mut owned_labels = Vec::<(String, String)>::new();
            match metric_case {
                0 => {}
                1 => owned_labels.push((METRIC_NAME_LABEL.to_string(), String::new())),
                2 => owned_labels.push((METRIC_NAME_LABEL.to_string(), raw_metric.to_string())),
                3 => {
                    owned_labels.push((METRIC_NAME_LABEL.to_string(), normalized_metric.clone()));
                }
                4 => owned_labels.push((
                    METRIC_NAME_LABEL.to_string(),
                    format!("9invalid.metric.{}", index % 17),
                )),
                _ => owned_labels.push((
                    METRIC_NAME_LABEL.to_string(),
                    format!("first.metric.{}", index % 13),
                )),
            }
            owned_labels.push((
                format!("zone.{}", index % 7),
                format!("value-{}", state % 23),
            ));
            if index % 3 == 0 {
                owned_labels.push((raw_label.to_string(), format!("first-{}", index % 5)));
                owned_labels.push((
                    normalized_label.clone(),
                    format!("last-{}", (index + 1) % 5),
                ));
            }
            if index % 5 == 0 {
                owned_labels.push(("__reserved".to_string(), format!("r-{}", index % 19)));
            }
            owned_labels.sort_by(|left, right| left.0.cmp(&right.0));

            let labels = owned_labels
                .iter()
                .map(|(key, value)| KeyValueRef::from((key.as_str(), value.as_str())))
                .collect::<Vec<_>>();
            let series = interner.intern(&labels, &mut stats).unwrap();
            source.push((
                series,
                metric_order_test_samples((state as usize) % 5, index as u64 + 1),
            ));
        }

        for index in 0..source.len() {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let swap_with = state as usize % source.len();
            source.swap(index, swap_with);
        }

        assert_flat_metric_order_matches_owned_reference(&interner, source);
    }
}

#[test]
fn indirect_metric_order_is_byte_identical_to_owned_reference_order() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut stats = OtlpMetricsIngestionStats::new();
    let mut interner = LabelSetInterner::new(LabelSetStoreKind::FlatInterned);
    let normalized_label = normalize_label_name("a.label");
    let series = [
        interner
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "z.metric")),
                    KeyValueRef::from(("pod", "z")),
                ],
                &mut stats,
            )
            .unwrap(),
        interner
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "same.metric")),
                    KeyValueRef::from(("a.label", "z-first")),
                    KeyValueRef::from((normalized_label.as_str(), "a-last")),
                ],
                &mut stats,
            )
            .unwrap(),
        interner
            .intern(
                &[
                    KeyValueRef::from((METRIC_NAME_LABEL, "a.metric")),
                    KeyValueRef::from(("pod", "a")),
                ],
                &mut stats,
            )
            .unwrap(),
    ];
    let source = series
        .into_iter()
        .enumerate()
        .map(|(index, series)| (series, metric_order_test_samples(0, index as u64 + 1)))
        .collect::<Vec<_>>();

    let write = |root: &Path, indirect: bool| {
        let mut ordered = source.clone();
        if indirect {
            order_series_samples_for_metric_query(&mut ordered, &interner).unwrap();
        } else {
            order_flat_interned_series_samples_for_metric_query_owned_reference(
                &mut ordered,
                interner.as_flat_interned().unwrap(),
            )
            .unwrap();
        }
        assert_ne!(ordered, source);

        let mut writer = SegmentWriter::new(
            SegmentWriterConfig::new(root, Duration::from_secs(10))
                .with_deterministic_segment_ids(0x001d_1ec7)
                .with_storage_schema(SegmentStorageSchema::Schema8),
        )
        .unwrap();
        writer
            .reserve_metric_query_ordered_window_series(0, 10_000, ordered.len())
            .unwrap();
        for (series, samples) in ordered {
            let SeriesSamples::Float { samples, .. } = samples else {
                panic!("fixture uses float samples");
            };
            record_segment_float_samples(&interner, &mut writer, series, &samples, false).unwrap();
        }
        writer.flush().unwrap();
        assert_eq!(
            writer.last_flush_profile().unwrap().chunk_rewrite_frames(),
            0
        );
    };

    let indirect_root = tempdir.path().join("indirect");
    let reference_root = tempdir.path().join("reference");
    fs::create_dir_all(&indirect_root).unwrap();
    fs::create_dir_all(&reference_root).unwrap();
    write(&indirect_root, true);
    write(&reference_root, false);

    assert_eq!(
        snapshot_tree(&indirect_root),
        snapshot_tree(&reference_root)
    );
}

#[test]
fn processor_non_flat_metric_order_fallback_matches_flat_segment_bytes_and_readback() {
    fn write_fixture(root: &Path, store_kind: LabelSetStoreKind) -> Vec<(String, Vec<u8>)> {
        fs::create_dir_all(root).unwrap();
        let writer = SegmentWriter::new(
            SegmentWriterConfig::new(root, Duration::from_secs(10))
                .with_deterministic_segment_ids(0x00fa_11ba)
                .with_storage_schema(SegmentStorageSchema::Schema8),
        )
        .unwrap();
        let head = Some(HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        ));
        let mut processor =
            OtlpLabelSetProcessor::new(store_kind, Duration::from_secs(3600), head, Some(writer))
                .with_shutdown_report(false);

        // Intern the lexically later metric first so the head drains in the
        // opposite order from the metric-query layout. The two series also
        // have unequal canonical label counts (three versus two).
        let mut z = number_dp(vec![kv_str("namespace", "default"), kv_str("pod", "z")]);
        z.time_unix_nano = 5_000_000_000;
        z.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(30.0));
        let mut a = number_dp(vec![kv_str("pod", "a")]);
        a.time_unix_nano = 5_000_000_000;
        a.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(10.0));

        assert_eq!(
            processor
                .process(
                    SourceMessageMetadata {
                        topic: "metrics".to_owned(),
                        partition: 0,
                        offset: 0,
                        timestamp_ms: 5_000,
                        captured_at_ms: 10_000,
                    },
                    request(
                        vec![],
                        vec![
                            metric_gauge("z_metric", vec![z]),
                            metric_gauge("a_metric", vec![a]),
                        ],
                    ),
                )
                .unwrap(),
            ProcessResult::Ok
        );
        processor.flush_head().unwrap();
        let profile = processor.last_head_window_write_profile().unwrap();
        assert_eq!(profile.series, 2);
        assert_eq!(profile.datapoints, 2);
        drop(processor);

        snapshot_tree(root)
    }

    let tempdir = tempfile::tempdir().unwrap();
    let flat_root = tempdir.path().join("flat");
    let naive_root = tempdir.path().join("naive");
    let keyset_root = tempdir.path().join("keyset");
    let flat = write_fixture(&flat_root, LabelSetStoreKind::FlatInterned);
    let naive = write_fixture(&naive_root, LabelSetStoreKind::Naive);
    let keyset = write_fixture(&keyset_root, LabelSetStoreKind::KeySetDictEncoded);

    assert_eq!(
        naive, flat,
        "Naive fallback changed segment or manifest bytes"
    );
    assert_eq!(
        keyset, flat,
        "KeySetDictEncoded fallback changed segment or manifest bytes"
    );

    assert_promql_samples(
        &open_default_store(&keyset_root),
        r#"a_metric{pod="a"}"#,
        vec![(5_000, 10.0)],
    );
}

#[test]
fn format_window_ms_formats_positive_and_negative() {
    assert_eq!(format_window_ms(0), "00:00:00.000");
    assert_eq!(format_window_ms(3_661_001), "01:01:01.001");
    assert_eq!(format_window_ms(-1), "-00:00:00.001");
}

#[test]
fn processor_drops_old_and_future_datapoints_using_captured_at_ms() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ))
    .with_shutdown_report(false);

    let mut accepted = number_dp(vec![kv_str("pod.name", "accepted")]);
    accepted.time_unix_nano = 95_000_000_000;
    accepted.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    let mut too_old = number_dp(vec![kv_str("pod.name", "old")]);
    too_old.time_unix_nano = 89_999_000_000;
    too_old.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    let mut too_future = number_dp(vec![kv_str("pod.name", "future")]);
    too_future.time_unix_nano = 105_001_000_000;
    too_future.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(3.0));

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 100_000,
            },
            request(
                vec![],
                vec![metric_gauge(
                    "cpu.usage",
                    vec![accepted, too_old, too_future],
                )],
            ),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::Ok);
    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.totals.observed_datapoints, 3);
    assert_eq!(snap.totals.datapoints, 1);
    assert_eq!(snap.totals.observed_datapoint_types.gauge, 3);
    assert_eq!(snap.totals.datapoint_types.gauge, 1);
    assert_eq!(snap.totals.datapoint_policy.accepted, 1);
    assert_eq!(snap.totals.datapoint_policy.dropped_too_old, 1);
    assert_eq!(snap.totals.datapoint_policy.dropped_too_future, 1);
    assert_eq!(snap.totals.datapoint_policy.missing_timestamp, 0);
    let skew = snap.totals.event_time_skew;
    let all_skew = skew.all.unwrap();
    assert_eq!(all_skew.count, 3);
    assert_eq!(all_skew.min, -10_001);
    assert_eq!(all_skew.max, 5_001);
    assert_eq!(skew.accepted.unwrap().min, -5_000);
    assert_eq!(skew.accepted.unwrap().max, -5_000);
    assert_eq!(skew.dropped_too_old.unwrap().min, -10_001);
    assert_eq!(skew.dropped_too_future.unwrap().max, 5_001);
    assert_eq!(processor.labelsets.stats().series, 1);

    processor.flush_head().unwrap();
    let store = open_default_store(tempdir.path());
    let metric = normalize_metric_name("cpu.usage");
    let pod_label = normalize_label_name("pod.name");
    let results = store
        .query_exact(
            &[
                (METRIC_NAME_LABEL, metric.as_str()),
                (pod_label.as_str(), "accepted"),
            ],
            0,
            200_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(95_000, 1.0)]);
    assert_eq!(segment_dir_count(tempdir.path()), 1);
}

#[test]
#[should_panic(expected = "max_event_lead must be non-negative")]
fn event_time_policy_rejects_negative_event_lead() {
    EventTimePolicy::new(
        chrono::TimeDelta::seconds(60),
        chrono::TimeDelta::seconds(-1),
        true,
    );
}

#[test]
fn processor_rejects_missing_otlp_timestamp_instead_of_using_kafka_timestamp() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ));

    let mut missing = number_dp(vec![kv_str("pod.name", "missing")]);
    missing.time_unix_nano = 0;
    missing.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 95_000,
                captured_at_ms: 100_000,
            },
            request(
                vec![
                    kv_str("rejected.resource", "must-not-intern"),
                    kv_bytes("rejected.non-scalar", b"must-not-count"),
                ],
                vec![metric_gauge("cpu.usage", vec![missing])],
            ),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::DroppedOutdated);
    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.totals.observed_datapoints, 1);
    assert_eq!(snap.totals.datapoints, 0);
    assert_eq!(snap.totals.observed_datapoint_types.gauge, 1);
    assert_eq!(snap.totals.datapoint_types.gauge, 0);
    assert_eq!(snap.totals.datapoint_policy.accepted, 0);
    assert_eq!(snap.totals.datapoint_policy.missing_timestamp, 1);
    let store_stats = processor.labelsets.stats();
    assert_eq!(store_stats.series, 0);
    assert_eq!(store_stats.symbols, Some(0));
    assert_eq!(snap.totals.skipped_non_scalar_values, 0);

    processor.flush_head().unwrap();
    assert_eq!(segment_dir_count(tempdir.path()), 0);
}

#[test]
fn processor_applies_the_same_event_time_policy_to_every_otlp_metric_kind() {
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        None,
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ))
    .with_shutdown_report(false);

    let timestamps = [
        ("accepted", 95_000),
        ("old", 89_999),
        ("future", 105_001),
        ("missing", 0),
    ];
    let number_points = || {
        timestamps
            .into_iter()
            .map(|(case, timestamp_ms)| {
                let mut point = number_dp(vec![kv_str("case", case)]);
                point.time_unix_nano = timestamp_ms * 1_000_000;
                point.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
                point
            })
            .collect::<Vec<_>>()
    };
    let histograms = timestamps
        .into_iter()
        .map(|(case, timestamp_ms)| {
            let mut point = histogram_dp(vec![kv_str("case", case)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 1;
            point.bucket_counts = vec![1];
            point
        })
        .collect();
    let exponential_histograms = timestamps
        .into_iter()
        .map(|(case, timestamp_ms)| {
            let mut point = exp_histogram_dp(vec![kv_str("case", case)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 1;
            point.zero_count = 1;
            point
        })
        .collect();
    let summaries = timestamps
        .into_iter()
        .map(|(case, timestamp_ms)| {
            let mut point = summary_dp(vec![kv_str("case", case)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 1;
            point.sum = 1.0;
            point
        })
        .collect();

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "metrics".to_string(),
                partition: 0,
                offset: 1,
                // This deliberately contradictory source timestamp is diagnostic only.
                timestamp_ms: 9_999_999,
                captured_at_ms: 100_000,
            },
            request(
                vec![],
                vec![
                    metric_gauge("policy.gauge", number_points()),
                    metric_sum("policy.sum", number_points()),
                    metric_histogram("policy.histogram", histograms),
                    metric_exp_histogram("policy.exponential_histogram", exponential_histograms),
                    metric_summary("policy.summary", summaries),
                ],
            ),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::Ok);
    let snapshot = processor.labelset_stats.snapshot();
    assert_eq!(snapshot.totals.observed_datapoints, 20);
    assert_eq!(snapshot.totals.datapoints, 5);
    assert_eq!(snapshot.totals.datapoint_policy.accepted, 5);
    assert_eq!(snapshot.totals.datapoint_policy.dropped_too_old, 5);
    assert_eq!(snapshot.totals.datapoint_policy.dropped_too_future, 5);
    assert_eq!(snapshot.totals.datapoint_policy.missing_timestamp, 5);
    assert_eq!(snapshot.totals.datapoint_types.gauge, 1);
    assert_eq!(snapshot.totals.datapoint_types.sum, 1);
    assert_eq!(snapshot.totals.datapoint_types.histogram, 1);
    assert_eq!(snapshot.totals.datapoint_types.exponential_histogram, 1);
    assert_eq!(snapshot.totals.datapoint_types.summary, 1);
    assert_eq!(processor.labelsets.stats().series, 5);
    let symbols = processor
        .labelsets
        .as_flat_interned()
        .expect("test uses the flat interned label store")
        .symbols();
    for rejected_value in ["old", "future", "missing"] {
        assert_eq!(
            symbols.lookup(rejected_value),
            None,
            "rejected datapoints must not intern symbol {rejected_value}"
        );
    }

    let head = &mut processor
        .partition_heads
        .values_mut()
        .next()
        .expect("partition head exists")
        .head;
    let samples = head
        .drain()
        .expect("accepted datapoints create a head window")
        .into_series_samples()
        .unwrap();
    assert_eq!(samples.len(), 5);
    for (_, samples) in samples {
        let timestamps = match samples {
            SeriesSamples::Float { samples, .. } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
            SeriesSamples::Int64 { samples, .. } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
            SeriesSamples::Histogram { samples } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
            SeriesSamples::ExponentialHistogram { samples } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
            SeriesSamples::Summary { samples } => samples
                .into_iter()
                .map(|sample| sample.0)
                .collect::<Vec<_>>(),
        };
        assert_eq!(timestamps, vec![95_000]);
    }
}

#[test]
fn ingest_moves_only_accepted_histogram_bucket_storage() {
    let head_config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head_config.clone()),
        None,
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ))
    .with_shutdown_report(false);
    let mut head_state = PartitionHead {
        head: HeadBuffer::new(head_config).unwrap(),
        stats: HeadBufferStats::new(),
    };

    let timestamps = [95_000, 89_999, 105_001, 0];
    let histograms = timestamps
        .into_iter()
        .enumerate()
        .map(|(index, timestamp_ms)| {
            let mut point = histogram_dp(vec![kv_int("case", index as i64)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 3;
            point.explicit_bounds = vec![index as f64 + 0.5];
            point.bucket_counts = vec![index as u64, 3 - index as u64];
            point
        })
        .collect();
    let exponential_histograms = timestamps
        .into_iter()
        .enumerate()
        .map(|(index, timestamp_ms)| {
            let mut point = exp_histogram_dp(vec![kv_int("case", index as i64)]);
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = 3;
            point.positive = Some(Buckets {
                offset: index as i32,
                bucket_counts: vec![index as u64],
            });
            point.negative = Some(Buckets {
                offset: -(index as i32),
                bucket_counts: vec![3 - index as u64],
            });
            point
        })
        .collect();
    let mut req = request(
        vec![],
        vec![
            metric_histogram("move.histogram", histograms),
            metric_exp_histogram("move.exponential_histogram", exponential_histograms),
        ],
    );

    let result = processor
        .ingest_otlp_metrics(&mut req, 100_000, Some(&mut head_state), true)
        .unwrap();
    assert_eq!(
        result,
        DatapointIngestResult {
            accepted: 2,
            dropped_too_old: 2,
            dropped_too_future: 2,
            missing_timestamp: 2,
            invalid_typed: 0,
        }
    );

    let metrics = &req.resource_metrics[0].scope_metrics[0].metrics;
    let Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) = &metrics[0].data else {
        panic!("expected histogram metric");
    };
    assert!(histogram.data_points[0].explicit_bounds.is_empty());
    assert!(histogram.data_points[0].bucket_counts.is_empty());
    for (index, point) in histogram.data_points.iter().enumerate().skip(1) {
        assert_eq!(point.explicit_bounds, vec![index as f64 + 0.5]);
        assert_eq!(point.bucket_counts, vec![index as u64, 3 - index as u64]);
    }

    let Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram)) = &metrics[1].data
    else {
        panic!("expected exponential histogram metric");
    };
    assert!(histogram.data_points[0].positive.is_none());
    assert!(histogram.data_points[0].negative.is_none());
    for (index, point) in histogram.data_points.iter().enumerate().skip(1) {
        assert_eq!(
            point.positive.as_ref().unwrap().bucket_counts,
            vec![index as u64]
        );
        assert_eq!(
            point.negative.as_ref().unwrap().bucket_counts,
            vec![3 - index as u64]
        );
    }

    let samples = head_state
        .head
        .drain()
        .expect("accepted datapoints create a head window")
        .into_series_samples()
        .unwrap();
    assert_eq!(samples.len(), 2);
}

#[test]
fn live_and_wal_replay_preserve_label_allocation_and_typed_head_semantics() {
    let mut missing_value = number_dp(vec![kv_str("case", "missing-value")]);
    missing_value.time_unix_nano = 90_000_000_000;
    missing_value.value = None;
    let mut gauge = number_dp(vec![kv_str("case", "equivalence")]);
    gauge.time_unix_nano = 95_000_000_000;
    gauge.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.25));
    let mut sum = number_dp(vec![kv_str("case", "equivalence")]);
    sum.time_unix_nano = 95_000_000_000;
    sum.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(42));

    let histogram_point =
        |case: &str, start_ms: u64, timestamp_ms: u64, bucket_counts: Vec<u64>| {
            let mut point = histogram_dp(vec![kv_str("case", case)]);
            point.start_time_unix_nano = start_ms * 1_000_000;
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = bucket_counts.iter().sum();
            point.sum = Some(point.count as f64);
            point.explicit_bounds = vec![1.0];
            point.bucket_counts = bucket_counts;
            point
        };
    let mut cumulative_histogram = metric_histogram(
        "equivalence.cumulative_histogram",
        vec![
            histogram_point("cumulative", 80_000, 91_000, vec![4, 6]),
            histogram_point("cumulative", 80_000, 92_000, vec![5, 7]),
            histogram_point("cumulative", 92_000, 93_000, vec![1, 2]),
        ],
    );
    let Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) =
        cumulative_histogram.data.as_mut()
    else {
        unreachable!("histogram helper returned a different metric kind")
    };
    histogram.aggregation_temporality = AggregationTemporality::Cumulative as i32;

    let exponential_histogram_point =
        |case: &str, start_ms: u64, timestamp_ms: u64, bucket_counts: Vec<u64>| {
            let mut point = exp_histogram_dp(vec![kv_str("case", case)]);
            point.start_time_unix_nano = start_ms * 1_000_000;
            point.time_unix_nano = timestamp_ms * 1_000_000;
            point.count = bucket_counts.iter().sum();
            point.sum = Some(point.count as f64);
            point.positive = Some(Buckets {
                offset: 0,
                bucket_counts,
            });
            point
        };
    let mut cumulative_exponential_histogram = metric_exp_histogram(
        "equivalence.cumulative_exponential_histogram",
        vec![
            exponential_histogram_point("cumulative", 80_000, 94_000, vec![4, 6]),
            exponential_histogram_point("cumulative", 80_000, 95_000, vec![5, 7]),
            exponential_histogram_point("cumulative", 95_000, 96_000, vec![1, 2]),
        ],
    );
    let Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram)) =
        cumulative_exponential_histogram.data.as_mut()
    else {
        unreachable!("exponential histogram helper returned a different metric kind")
    };
    histogram.aggregation_temporality = AggregationTemporality::Cumulative as i32;

    let mut delta_histogram = metric_histogram(
        "equivalence.delta_histogram",
        vec![histogram_point("delta", 96_000, 97_000, vec![1, 2])],
    );
    let Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) =
        delta_histogram.data.as_mut()
    else {
        unreachable!("histogram helper returned a different metric kind")
    };
    histogram.aggregation_temporality = AggregationTemporality::Delta as i32;

    let mut delta_exponential_histogram = metric_exp_histogram(
        "equivalence.delta_exponential_histogram",
        vec![exponential_histogram_point(
            "delta",
            97_000,
            98_000,
            vec![1, 2],
        )],
    );
    let Some(tonic::metrics::v1::metric::Data::ExponentialHistogram(histogram)) =
        delta_exponential_histogram.data.as_mut()
    else {
        unreachable!("exponential histogram helper returned a different metric kind")
    };
    histogram.aggregation_temporality = AggregationTemporality::Delta as i32;

    let mut summary = summary_dp(vec![kv_str("case", "equivalence")]);
    summary.start_time_unix_nano = 90_000_000_000;
    summary.time_unix_nano = 95_000_000_000;
    summary.flags = 1;
    summary.count = 10;
    summary.sum = 50.0;
    summary.quantile_values = vec![ValueAtQuantile {
        quantile: 0.5,
        value: 4.0,
    }];

    let request = request(
        vec![
            kv_str("service.name", "checkout-first"),
            kv_str("service.name", "checkout"),
            kv_bytes("resource.unsupported", b"count-per-accepted-datapoint"),
        ],
        vec![
            metric_gauge("equivalence.gauge", vec![missing_value, gauge]),
            metric_sum("equivalence.sum", vec![sum]),
            cumulative_histogram,
            cumulative_exponential_histogram,
            delta_histogram,
            delta_exponential_histogram,
            metric_summary("equivalence.summary", vec![summary]),
        ],
    );
    let policy = EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    );

    let head_config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut live = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        Some(head_config.clone()),
        None,
    )
    .with_event_time_policy(policy)
    .with_shutdown_report(false);
    live.process(
        SourceMessageMetadata {
            topic: "metrics".to_string(),
            partition: 3,
            offset: 44,
            timestamp_ms: 9_999_999,
            captured_at_ms: 100_000,
        },
        request.clone(),
    )
    .unwrap();
    let live_snapshot = live.labelset_stats.snapshot();
    let live_series_count = live.labelsets.stats().series;
    let live_labelsets = (0..live_series_count)
        .map(|series| {
            collect_labelset(
                &live,
                SeriesRef::new(u32::try_from(series).expect("test series count fits u32")),
            )
        })
        .collect::<Vec<_>>();
    let live_samples = live
        .partition_heads
        .values_mut()
        .next()
        .expect("live partition head exists")
        .head
        .drain()
        .expect("live request creates a window")
        .into_series_samples()
        .unwrap();
    let mut live_by_labels = BTreeMap::new();
    for (series, samples) in live_samples {
        let mut labels = Vec::new();
        live.labelsets.visit_labelset(series, |key, value| {
            labels.push((key.to_string(), value.to_string()))
        });
        labels.sort();
        live_by_labels.insert(labels, samples);
    }

    let tempdir = tempfile::tempdir().unwrap();
    let wal_path = tempdir.path().join("wal-live-equivalence.log");
    let mut writer = WalWriter::create(&wal_path).unwrap();
    writer
        .append_otlp_batch(&OtlpWalBatch {
            topic: "metrics".to_string(),
            partition: 3,
            offset: 44,
            source_timestamp_ms: 9_999_999,
            captured_at_ms: 100_000,
            payload: request.encode_to_vec(),
        })
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut replay_head = HeadBuffer::new(head_config).unwrap();
    let mut replay_labels = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let replay_outcome =
        replay_wal_file_into_head(&wal_path, policy, &mut replay_head, &mut replay_labels).unwrap();
    assert!(replay_outcome.completed_windows.is_empty());
    let replay_partition = replay_outcome.partition.as_ref().unwrap();
    assert_eq!(replay_partition.topic, "metrics");
    assert_eq!(replay_partition.partition, 3);
    let replay_report = replay_outcome.report;
    let replay_labelsets = (0..replay_labels.len())
        .map(|series| {
            let mut labels = Vec::new();
            replay_labels.visit_labelset(
                SeriesRef::new(u32::try_from(series).expect("test series count fits u32")),
                |key, value| labels.push((key.to_string(), value.to_string())),
            );
            labels.sort();
            labels
        })
        .collect::<Vec<_>>();
    let replay_samples = replay_head
        .drain()
        .expect("WAL replay creates a window")
        .into_series_samples()
        .unwrap();
    let mut replay_by_labels = BTreeMap::new();
    for (series, samples) in replay_samples {
        let mut labels = Vec::new();
        replay_labels.visit_labelset(series, |key, value| {
            labels.push((key.to_string(), value.to_string()))
        });
        labels.sort();
        replay_by_labels.insert(labels, samples);
    }

    assert_eq!(live_snapshot.totals.datapoint_policy.accepted, 12);
    assert_eq!(live_snapshot.totals.datapoint_policy.rejected(), 0);
    assert_eq!(live_snapshot.totals.datapoint_storage.recorded_samples, 11);
    assert_eq!(
        live_snapshot.totals.datapoint_storage.missing_number_values,
        1
    );
    assert_eq!(live_series_count, 8);
    assert_eq!(live_snapshot.totals.skipped_non_scalar_values, 12);
    assert_eq!(replay_report.policy_accepted_datapoints, 12);
    assert_eq!(replay_report.dropped_too_old_datapoints, 0);
    assert_eq!(replay_report.dropped_too_future_datapoints, 0);
    assert_eq!(replay_report.missing_timestamp_datapoints, 0);
    assert_eq!(replay_report.datapoints_replayed, 11);
    assert_eq!(replay_report.skipped_non_scalar_labels, 12);
    assert_eq!(replay_labelsets, live_labelsets);
    assert_eq!(replay_by_labels, live_by_labels);
    assert_eq!(
        reset_hints_for_metric(&replay_by_labels, "equivalence.cumulative_histogram"),
        vec![
            CounterResetHint::Unknown,
            CounterResetHint::NotCounterReset,
            CounterResetHint::CounterReset,
        ]
    );
    assert_eq!(
        reset_hints_for_metric(
            &replay_by_labels,
            "equivalence.cumulative_exponential_histogram"
        ),
        vec![
            CounterResetHint::Unknown,
            CounterResetHint::NotCounterReset,
            CounterResetHint::CounterReset,
        ]
    );
    assert_eq!(
        reset_hints_for_metric(&replay_by_labels, "equivalence.delta_histogram"),
        vec![CounterResetHint::NotCounterReset]
    );
    assert_eq!(
        reset_hints_for_metric(&replay_by_labels, "equivalence.delta_exponential_histogram"),
        vec![CounterResetHint::NotCounterReset]
    );
}

#[test]
fn processor_counts_missing_number_values_separately_from_time_policy_acceptance() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(5),
        true,
    ));

    let attrs = vec![kv_str("pod.name", "same")];
    let mut valid = number_dp(attrs.clone());
    valid.time_unix_nano = 95_000_000_000;
    valid.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    let mut missing_value = number_dp(attrs);
    missing_value.time_unix_nano = 95_001_000_000;
    missing_value.value = None;

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 100_000,
            },
            request(
                vec![],
                vec![metric_gauge("cpu.usage", vec![valid, missing_value])],
            ),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::Ok);
    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.totals.datapoints, 2);
    assert_eq!(snap.totals.datapoint_policy.accepted, 2);
    assert_eq!(snap.totals.datapoint_policy.rejected(), 0);
    assert_eq!(snap.totals.datapoint_storage.recorded_samples, 1);
    assert_eq!(snap.totals.datapoint_storage.missing_number_values, 1);
    assert_eq!(snap.window.datapoint_storage, snap.totals.datapoint_storage);

    processor.flush_head().unwrap();
    let store = open_default_store(tempdir.path());
    let metric = normalize_metric_name("cpu.usage");
    let pod_label = normalize_label_name("pod.name");
    let results = store
        .query_exact(
            &[
                (METRIC_NAME_LABEL, metric.as_str()),
                (pod_label.as_str(), "same"),
            ],
            0,
            200_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(95_000, 1.0)]);
}

#[test]
fn processor_rejects_invalid_histogram_before_reset_and_head_mutation() {
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        None,
    )
    .with_shutdown_report(false);

    let make_point = |timestamp_ms: u64, pod: &str, count: u64, bucket_counts: Vec<u64>| {
        let mut point = histogram_dp(vec![kv_str("pod.name", pod)]);
        point.time_unix_nano = timestamp_ms * 1_000_000;
        point.start_time_unix_nano = 500_000_000;
        point.count = count;
        point.explicit_bounds = vec![1.0];
        point.bucket_counts = bucket_counts;
        point
    };
    let metric = tonic::metrics::v1::Metric {
        name: "request.duration".to_string(),
        data: Some(tonic::metrics::v1::metric::Data::Histogram(
            tonic::metrics::v1::Histogram {
                aggregation_temporality: tonic::metrics::v1::AggregationTemporality::Cumulative
                    as i32,
                data_points: vec![
                    make_point(1_000, "same", 10, vec![4, 6]),
                    make_point(2_000, "same", u64::MAX, vec![u64::MAX, 1]),
                    make_point(2_500, "invalid-only", u64::MAX, vec![u64::MAX, 1]),
                    make_point(3_000, "same", 12, vec![5, 7]),
                ],
            },
        )),
        ..Default::default()
    };

    let result = processor
        .process(
            SourceMessageMetadata {
                topic: "metrics".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 3_000,
                captured_at_ms: 3_000,
            },
            request(vec![], vec![metric]),
        )
        .unwrap();

    assert_eq!(result, ProcessResult::Ok);
    let snapshot = processor.labelset_stats.snapshot();
    assert_eq!(snapshot.totals.messages, 1);
    assert_eq!(snapshot.totals.datapoint_policy.accepted, 4);
    assert_eq!(snapshot.totals.datapoint_storage.recorded_samples, 2);
    assert_eq!(snapshot.totals.datapoint_storage.invalid_typed_values, 2);
    assert_eq!(processor.labelsets.stats().series, 1);

    let window = processor
        .partition_heads
        .values_mut()
        .next()
        .unwrap()
        .head
        .drain()
        .unwrap();
    let mut samples = window.into_series_samples().unwrap();
    assert_eq!(samples.len(), 1);
    let SeriesSamples::Histogram { samples } = samples.pop().unwrap().1 else {
        panic!("expected histogram samples");
    };
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].0, 1_000);
    assert_eq!(samples[0].1.metadata.reset_hint, CounterResetHint::Unknown);
    assert_eq!(samples[1].0, 3_000);
    assert_eq!(
        samples[1].1.metadata.reset_hint,
        CounterResetHint::NotCounterReset
    );
}

#[test]
fn processor_canonicalizes_labels_and_skips_non_scalar_values() {
    for store in [
        LabelSetStoreKind::FlatInterned,
        LabelSetStoreKind::KeySetDictEncoded,
    ] {
        let mut processor =
            OtlpLabelSetProcessor::new(store, Duration::from_secs(3600), None, None);

        let resource_attrs = vec![
            kv_str("cluster", "prod"),
            kv_str("resource_only", "r1"),
            kv_int("int_value", 42),
        ];
        let dp_attrs = vec![
            kv_str("cluster", "staging"), // overrides resource
            kv_str("pod", "backend-123"),
            kv_str(chronoxide_core::labels::METRIC_NAME_LABEL, "ignored"),
            kv_bool("bool_value", true),
            kv_double("double_value", 314.0 / 100.0),
            kv_bytes("bytes_value", b"abc"),
            kv_array("array_value"),
            kv_kvlist("kvlist_value"),
            kv_str("", "ignored_empty_key"),
        ];

        let req = request(
            resource_attrs,
            vec![metric_gauge("cpu_usage", vec![number_dp(dp_attrs)])],
        );

        processor
            .process(
                SourceMessageMetadata {
                    topic: "t".to_string(),
                    partition: 0,
                    offset: 0,
                    timestamp_ms: 1_000,
                    captured_at_ms: 10_000,
                },
                req,
            )
            .unwrap();

        let store_stats = processor.labelsets.stats();
        assert_eq!(store_stats.series, 1);

        let labels = collect_labelset(&processor, SeriesRef::new(0));
        let mut expected = vec![
            ("__name__".to_string(), "cpu_usage".to_string()),
            ("bool_value".to_string(), "true".to_string()),
            ("cluster".to_string(), "staging".to_string()),
            ("double_value".to_string(), "3.14".to_string()),
            ("int_value".to_string(), "42".to_string()),
            ("pod".to_string(), "backend-123".to_string()),
            ("resource_only".to_string(), "r1".to_string()),
        ];
        expected.sort();
        assert_eq!(labels, expected);

        let snap = processor.labelset_stats.snapshot();
        assert_eq!(snap.totals.skipped_non_scalar_values, 3);
    }
}

#[test]
fn processor_counts_metric_and_datapoint_types_and_dedups_series() {
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        None,
        None,
    );

    let same_attrs = vec![kv_str("pod", "same")];
    let req = request(
        vec![],
        vec![
            metric_gauge(
                "m_gauge",
                vec![number_dp(same_attrs.clone()), number_dp(same_attrs)],
            ),
            metric_sum("m_sum", vec![number_dp(vec![kv_str("pod", "sum")])]),
            metric_histogram("m_hist", vec![histogram_dp(vec![kv_str("pod", "hist")])]),
            metric_exp_histogram(
                "m_exphist",
                vec![exp_histogram_dp(vec![kv_str("pod", "exphist")])],
            ),
            metric_summary(
                "m_summary",
                vec![summary_dp(vec![kv_str("pod", "summary")])],
            ),
        ],
    );

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 1,
                offset: 123,
                timestamp_ms: 2_000,
                captured_at_ms: 10_001,
            },
            req,
        )
        .unwrap();

    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.totals.messages, 1);
    assert_eq!(snap.totals.metrics, 5);
    assert_eq!(snap.totals.unique_metrics, 5);
    assert_eq!(snap.totals.datapoints, 6);

    assert_eq!(snap.totals.metric_types.gauge, 1);
    assert_eq!(snap.totals.metric_types.sum, 1);
    assert_eq!(snap.totals.metric_types.histogram, 1);
    assert_eq!(snap.totals.metric_types.exponential_histogram, 1);
    assert_eq!(snap.totals.metric_types.summary, 1);

    assert_eq!(snap.totals.datapoint_types.gauge, 2);
    assert_eq!(snap.totals.datapoint_types.sum, 1);
    assert_eq!(snap.totals.datapoint_types.histogram, 1);
    assert_eq!(snap.totals.datapoint_types.exponential_histogram, 1);
    assert_eq!(snap.totals.datapoint_types.summary, 1);

    let store_stats = processor.labelsets.stats();
    assert_eq!(store_stats.series, 5); // gauge datapoints dedup to 1 series

    assert_eq!(snap.partition_watermarks.len(), 1);
    let ((topic, partition), wm) = &snap.partition_watermarks[0];
    assert_eq!(topic, "t");
    assert_eq!(*partition, 1);
    assert_eq!(wm.messages, 1);
    assert_eq!(wm.datapoints, 6);

    processor.maybe_report_labelset_stats(true);
    let snap = processor.labelset_stats.snapshot();
    assert_eq!(snap.window.messages, 0);
    assert_eq!(snap.window.metrics, 0);
    assert_eq!(snap.window.datapoints, 0);
    assert_eq!(snap.window.unique_metrics, 0);
}

#[test]
fn data_type_counts_markdown_reports_metric_records_and_datapoints() {
    let metric_types = OtlpDataTypeCounts {
        gauge: 1,
        sum: 2,
        histogram: 3,
        exponential_histogram: 4,
        summary: 5,
    };
    let observed_datapoint_types = OtlpDataTypeCounts {
        gauge: 10,
        sum: 20,
        histogram: 30,
        exponential_histogram: 40,
        summary: 50,
    };
    let accepted_datapoint_types = OtlpDataTypeCounts {
        gauge: 8,
        sum: 18,
        histogram: 28,
        exponential_histogram: 38,
        summary: 48,
    };

    let markdown = data_type_counts_markdown(
        &metric_types,
        &observed_datapoint_types,
        &accepted_datapoint_types,
    );

    assert!(markdown.contains("## OTLP Data Type Counts"));
    assert!(
        markdown.contains("| Type | Metric Records | Observed Datapoints | Accepted Datapoints |")
    );
    assert!(markdown.contains("| Gauge | 1 | 10 | 8 |"));
    assert!(markdown.contains("| Sum | 2 | 20 | 18 |"));
    assert!(markdown.contains("| Histogram | 3 | 30 | 28 |"));
    assert!(markdown.contains("| Exponential Histogram | 4 | 40 | 38 |"));
    assert!(markdown.contains("| Summary | 5 | 50 | 48 |"));
}

#[test]
fn datapoint_policy_counts_markdown_reports_drop_reasons() {
    let totals = DatapointPolicyCounts {
        accepted: 10,
        dropped_too_old: 2,
        dropped_too_future: 3,
        missing_timestamp: 4,
    };
    let window = DatapointPolicyCounts {
        accepted: 1,
        dropped_too_old: 0,
        dropped_too_future: 1,
        missing_timestamp: 0,
    };

    let markdown = datapoint_policy_counts_markdown(&totals, &window);

    assert!(markdown.contains("## Datapoint Policy Counts"));
    assert!(markdown.contains("| Observed | 19 | 2 |"));
    assert!(markdown.contains("| Time-Policy Accepted | 10 | 1 |"));
    assert!(markdown.contains("| Dropped Too Old | 2 | 0 |"));
    assert!(markdown.contains("| Dropped Too Future | 3 | 1 |"));
    assert!(markdown.contains("| Missing Timestamp | 4 | 0 |"));
    assert!(markdown.contains("| Rejected Total | 9 | 1 |"));
}

#[test]
fn datapoint_storage_counts_markdown_reports_recorded_and_missing_number_values() {
    let totals = DatapointStorageCounts {
        recorded_samples: 7,
        missing_number_values: 2,
        invalid_typed_values: 1,
    };
    let window = DatapointStorageCounts {
        recorded_samples: 3,
        missing_number_values: 1,
        invalid_typed_values: 0,
    };
    let policy_totals = DatapointPolicyCounts {
        accepted: 10,
        ..Default::default()
    };
    let policy_window = DatapointPolicyCounts {
        accepted: 4,
        ..Default::default()
    };

    let markdown =
        datapoint_storage_counts_markdown(&totals, &window, &policy_totals, &policy_window);

    assert!(markdown.contains("## Datapoint Storage Counts"));
    assert!(markdown.contains("| Time-Policy Accepted | 10 | 4 |"));
    assert!(markdown.contains("| Recorded Samples | 7 | 3 |"));
    assert!(markdown.contains("| Missing Number Value | 2 | 1 |"));
    assert!(markdown.contains("| Invalid Typed Value | 1 | 0 |"));
    assert!(markdown.contains("| Accepted Not Recorded | 3 | 1 |"));
}

#[test]
fn event_time_skew_markdown_reports_signed_distributions() {
    let mut stats = OtlpMetricsIngestionStats::new();
    stats.record_event_time_skew(metrics_ingestion_stats::EventTimeSkewOutcome::Accepted, -5);
    stats.record_event_time_skew(
        metrics_ingestion_stats::EventTimeSkewOutcome::DroppedTooOld,
        -10,
    );
    stats.record_event_time_skew(
        metrics_ingestion_stats::EventTimeSkewOutcome::DroppedTooFuture,
        3,
    );
    let snapshot = stats.snapshot();

    let markdown = event_time_skew_markdown(&snapshot.totals.event_time_skew);

    assert!(markdown.contains("## Event Time Skew"));
    assert!(markdown.contains("event_ms - captured_at_ms"));
    assert!(markdown.contains("| All Timestamped | 3 |"));
    assert!(markdown.contains("| Accepted | 1 |"));
    assert!(markdown.contains("| Dropped Too Old | 1 |"));
    assert!(markdown.contains("| Dropped Too Future | 1 |"));
}

#[test]
fn number_value_handles_int_and_double() {
    let mut dp = number_dp(vec![]);
    dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(5));
    assert_eq!(number_value(&dp), Some(SampleValue::Int64(5)));

    dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.5));
    assert_eq!(number_value(&dp), Some(SampleValue::Float(2.5)));

    dp.value = None;
    assert_eq!(number_value(&dp), None);
}

#[test]
fn processor_writes_segment_meta() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut dp = number_dp(vec![kv_str("pod", "backend-1")]);
    dp.time_unix_nano = 5_000_000_000;
    dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(
        314.0 / 100.0,
    ));
    let req = request(vec![], vec![metric_gauge("cpu_usage", vec![dp])]);

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_002,
            },
            req,
        )
        .unwrap();
    processor.flush_head().unwrap();
    let profile = processor.last_head_window_write_profile().unwrap();
    assert_eq!(profile.series, 1);
    assert_eq!(profile.datapoints, 1);
    assert_eq!(profile.record_subphases.chunks, 1);
    assert_eq!(profile.record_subphases.samples, 1);
    assert!(profile.series_reserve <= profile.total);
    assert!(profile.total >= profile.writer_flush);

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let meta = read_segment_meta(&seg_dir);
    assert_eq!(meta.datapoints, 1);
    assert_eq!(meta.series, 1);
    let chunk_len = fs::metadata(seg_dir.join(SegmentFile::Chunks.filename()))
        .unwrap()
        .len();
    assert!(chunk_len > 0);
}

#[test]
fn processor_writes_segment_series_metadata_and_exact_postings() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6),
    )
    .unwrap();

    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut dp1 = number_dp(vec![
        kv_str("namespace", "default"),
        kv_str("pod.name", "backend-1"),
    ]);
    dp1.time_unix_nano = 5_000_000_000;
    dp1.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));

    let mut dp2 = number_dp(vec![
        kv_str("namespace", "default"),
        kv_str("pod.name", "backend-2"),
    ]);
    dp2.time_unix_nano = 6_000_000_000;
    dp2.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));

    let req = request(vec![], vec![metric_gauge("cpu.usage", vec![dp1, dp2])]);

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_003,
            },
            req,
        )
        .unwrap();
    processor.flush_head().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let symbols = read_symbols_bin(
        File::open(seg_dir.join(SegmentFile::Symbols.filename())).expect("open symbols"),
    )
    .unwrap();
    let series = read_series_bin(
        File::open(seg_dir.join(SegmentFile::Series.filename())).expect("open series"),
    )
    .unwrap();
    let indexes = read_segment_indexes(
        File::open(seg_dir.join(SegmentFile::Indexes.filename())).expect("open indexes"),
    )
    .unwrap();
    let postings = indexes.exact_postings;

    assert_eq!(series.len(), 2);
    let metric_sym = symbols.lookup(METRIC_NAME_LABEL).unwrap();
    let metric_value = series[0]
        .labels
        .iter()
        .find_map(|(key, value)| (*key == metric_sym).then_some(*value))
        .and_then(|sym| symbols.resolve(sym))
        .unwrap();
    assert!(metric_value.starts_with("cpu_usage_x"));

    let namespace_sym = symbols.lookup("namespace").unwrap();
    let default_sym = symbols.lookup("default").unwrap();
    assert_eq!(postings.get(namespace_sym, default_sym), Some(&[0, 1][..]));

    let labels: Vec<_> = series
        .iter()
        .flat_map(|entry| {
            entry.labels.iter().map(|(key, value)| {
                (
                    symbols.resolve(*key).unwrap().to_string(),
                    symbols.resolve(*value).unwrap().to_string(),
                )
            })
        })
        .collect();
    assert!(
        labels
            .iter()
            .any(|(key, value)| { key.starts_with("pod_name_x") && value == "backend-1" })
    );
    assert!(
        labels
            .iter()
            .any(|(key, value)| { key.starts_with("pod_name_x") && value == "backend-2" })
    );
}

#[test]
fn processor_writes_head_window_chunks_in_metric_query_order_without_rewrite() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6),
    )
    .unwrap();

    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut z_dp = number_dp(vec![kv_str("pod.name", "z")]);
    z_dp.time_unix_nano = 5_000_000_000;
    z_dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(10.0));
    let mut a_dp = number_dp(vec![kv_str("pod.name", "a")]);
    a_dp.time_unix_nano = 5_000_000_000;
    a_dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(20.0));

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_003,
            },
            request(
                vec![],
                vec![
                    metric_gauge("z.metric", vec![z_dp]),
                    metric_gauge("a.metric", vec![a_dp]),
                ],
            ),
        )
        .unwrap();
    processor.flush_head().unwrap();

    let writer = processor.segment_writer.as_ref().unwrap();
    let profile = writer.last_flush_profile().unwrap();
    assert_eq!(profile.chunk_rewrite_frames(), 0);
    assert_eq!(profile.chunk_rewrite_payload_bytes(), 0);

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let symbols = read_symbols_bin(
        File::open(seg_dir.join(SegmentFile::Symbols.filename())).expect("open symbols"),
    )
    .unwrap();
    let series = read_series_bin(
        File::open(seg_dir.join(SegmentFile::Series.filename())).expect("open series"),
    )
    .unwrap();
    let mut chunk_index = File::open(seg_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    let chunk_entries = read_chunk_index(&mut chunk_index).unwrap();
    assert_eq!(series.len(), 2);
    assert_eq!(chunk_entries.len(), 2);

    let metric_sym = symbols.lookup(METRIC_NAME_LABEL).unwrap();
    let metric_names = series
        .iter()
        .map(|entry| {
            entry
                .labels
                .iter()
                .find_map(|(key, value)| (*key == metric_sym).then_some(*value))
                .and_then(|sym| symbols.resolve(sym))
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        metric_names,
        vec![
            normalize_metric_name("a.metric"),
            normalize_metric_name("z.metric")
        ]
    );

    let chunk_offsets = chunk_entries
        .iter()
        .map(|entries| {
            assert_eq!(entries.len(), 1);
            entries[0].offset
        })
        .collect::<Vec<_>>();
    assert_eq!(chunk_offsets, {
        let mut sorted = chunk_offsets.clone();
        sorted.sort_unstable();
        sorted
    });
}

#[test]
fn processor_writes_raw_head_float_samples_through_deferred_metadata() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6),
    )
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );
    let mut datapoint = number_dp(vec![kv_str("pod.name", "raw")]);
    datapoint.time_unix_nano = 5_000_000_000;
    datapoint.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(-0.0));

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_004,
            },
            request(vec![], vec![metric_gauge("raw.metric", vec![datapoint])]),
        )
        .unwrap();
    processor.flush_head().unwrap();

    let segment = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let mut chunks = ChunkReader::new(
        File::open(segment.join(SegmentFile::Chunks.filename())).expect("open chunks"),
    );
    let record = chunks.read_next().unwrap().unwrap();
    let ChunkSamples::Float(samples) = record.samples else {
        panic!("expected raw float chunk");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].0, 5_000);
    assert_eq!(samples[0].1.to_bits(), (-0.0_f64).to_bits());
    assert!(chunks.read_next().unwrap().is_none());
}

#[test]
fn processor_writes_integer_number_datapoints_as_promql_float_samples() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut dp = number_dp(vec![kv_str("pod.name", "backend-1")]);
    dp.time_unix_nano = 5_000_000_000;
    dp.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(42));
    let req = request(vec![], vec![metric_sum("requests.total", vec![dp])]);

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_004,
            },
            req,
        )
        .unwrap();
    processor.flush_head().unwrap();

    let metric = normalize_metric_name("requests.total");
    let pod_label = normalize_label_name("pod.name");
    let results = open_default_store(tempdir.path())
        .query_exact(
            &[
                (METRIC_NAME_LABEL, metric.as_str()),
                (pod_label.as_str(), "backend-1"),
            ],
            0,
            10_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 42.0)]);
}

#[test]
fn processor_writes_typed_otlp_datapoints_to_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema6),
    )
    .unwrap();

    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut hist = histogram_dp(vec![kv_str("pod.name", "hist")]);
    hist.time_unix_nano = 5_000_000_000;
    hist.count = 4;
    hist.sum = Some(10.0);
    hist.min = Some(1.0);
    hist.max = Some(4.0);
    hist.explicit_bounds = vec![1.0, 5.0];
    hist.bucket_counts = vec![1, 2, 1];

    let mut exphist = exp_histogram_dp(vec![kv_str("pod.name", "exphist")]);
    exphist.time_unix_nano = 6_000_000_000;
    exphist.count = 6;
    exphist.sum = Some(15.0);
    exphist.min = Some(1.0);
    exphist.max = Some(8.0);
    exphist.scale = 2;
    exphist.zero_count = 1;
    exphist.positive = Some(Buckets {
        offset: -1,
        bucket_counts: vec![2, 3],
    });
    exphist.negative = Some(Buckets {
        offset: 0,
        bucket_counts: vec![0],
    });

    let mut summary = summary_dp(vec![kv_str("pod.name", "summary")]);
    summary.time_unix_nano = 7_000_000_000;
    summary.count = 10;
    summary.sum = 50.0;
    summary.quantile_values = vec![
        ValueAtQuantile {
            quantile: 0.5,
            value: 4.0,
        },
        ValueAtQuantile {
            quantile: 0.9,
            value: 8.0,
        },
    ];

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_005,
            },
            request(
                vec![],
                vec![
                    metric_histogram("request.duration", vec![hist]),
                    metric_exp_histogram("request.size", vec![exphist]),
                    metric_summary("request.latency", vec![summary]),
                ],
            ),
        )
        .unwrap();
    processor.flush_head().unwrap();

    let profile = processor.last_head_window_write_profile().unwrap();
    assert_eq!(profile.datapoints, 3);
    assert_eq!(profile.series, 3);
    assert_eq!(profile.record_subphases.chunks, 3);
    assert_eq!(profile.record_subphases.samples, 3);
    assert!(profile.series_reserve <= profile.total);
    assert_eq!(profile.dropped_histogram_series, 0);
    assert_eq!(profile.dropped_exponential_histogram_series, 0);
    assert_eq!(profile.dropped_summary_series, 0);

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let meta = read_segment_meta(&seg_dir);
    assert_eq!(meta.datapoints, 3);
    assert_eq!(meta.series, 3);

    let series = read_series_bin(
        File::open(seg_dir.join(SegmentFile::Series.filename())).expect("open series"),
    )
    .unwrap();
    let kind_masks: Vec<u8> = series.iter().map(|entry| entry.kind_mask).collect();
    assert!(
        kind_masks
            .iter()
            .any(|mask| mask & SERIES_KIND_HISTOGRAM == SERIES_KIND_HISTOGRAM)
    );
    assert!(kind_masks.iter().any(|mask| {
        mask & SERIES_KIND_EXPONENTIAL_HISTOGRAM == SERIES_KIND_EXPONENTIAL_HISTOGRAM
    }));
    assert!(
        kind_masks
            .iter()
            .any(|mask| mask & SERIES_KIND_SUMMARY == SERIES_KIND_SUMMARY)
    );

    let mut chunk_reader = ChunkReader::new(
        File::open(seg_dir.join(SegmentFile::Chunks.filename())).expect("open chunks"),
    );
    let mut chunk_kinds = Vec::new();
    while let Some(record) = chunk_reader.read_next().unwrap() {
        chunk_kinds.push(record.kind);
        match record.samples {
            ChunkSamples::Histogram(samples) => assert_eq!(samples.len(), 1),
            ChunkSamples::ExponentialHistogram(samples) => assert_eq!(samples.len(), 1),
            ChunkSamples::Summary(samples) => assert_eq!(samples.len(), 1),
            ChunkSamples::Float(_) | ChunkSamples::Int64(_) => {
                panic!("unexpected scalar chunk in typed ingest test")
            }
        }
    }
    assert!(chunk_kinds.contains(&ChunkKind::Histogram));
    assert!(chunk_kinds.contains(&ChunkKind::ExponentialHistogram));
    assert!(chunk_kinds.contains(&ChunkKind::Summary));
}

#[test]
fn e2e_roundtrips_controlled_otlp_metrics_through_segments_and_promql() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    )
    .with_event_time_policy(EventTimePolicy::new(
        chrono::TimeDelta::seconds(10),
        chrono::TimeDelta::seconds(0),
        true,
    ));

    let mut gauge = number_dp(vec![kv_str("test.case", "roundtrip")]);
    gauge.time_unix_nano = 5_000_000_000;
    gauge.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.25));

    let mut sum = number_dp(vec![kv_str("test.case", "roundtrip")]);
    sum.time_unix_nano = 5_000_000_000;
    sum.value = Some(tonic::metrics::v1::number_data_point::Value::AsInt(42));

    let mut hist = histogram_dp(vec![kv_str("test.case", "roundtrip")]);
    hist.time_unix_nano = 5_000_000_000;
    hist.count = 4;
    hist.sum = Some(10.0);
    hist.min = Some(1.0);
    hist.max = Some(4.0);
    hist.explicit_bounds = vec![1.0, 5.0];
    hist.bucket_counts = vec![1, 2, 1];

    let mut exphist = exp_histogram_dp(vec![kv_str("test.case", "roundtrip")]);
    exphist.time_unix_nano = 5_000_000_000;
    exphist.count = 5;
    exphist.sum = Some(12.0);
    exphist.min = Some(1.0);
    exphist.max = Some(3.0);
    exphist.scale = 0;
    exphist.zero_count = 0;
    exphist.positive = Some(Buckets {
        offset: 0,
        bucket_counts: vec![2, 3],
    });
    exphist.negative = Some(Buckets {
        offset: 0,
        bucket_counts: vec![],
    });

    let mut summary = summary_dp(vec![kv_str("test.case", "roundtrip")]);
    summary.time_unix_nano = 5_000_000_000;
    summary.count = 10;
    summary.sum = 50.0;
    summary.quantile_values = vec![
        ValueAtQuantile {
            quantile: 0.5,
            value: 4.0,
        },
        ValueAtQuantile {
            quantile: 0.9,
            value: 8.0,
        },
    ];

    processor
        .process(
            SourceMessageMetadata {
                topic: "controlled".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 5_000,
                captured_at_ms: 5_000,
            },
            request(
                vec![kv_str("service.name", "roundtrip-suite")],
                vec![
                    metric_gauge("controlled.gauge", vec![gauge]),
                    metric_sum("controlled.sum", vec![sum]),
                    metric_histogram("controlled.histogram", vec![hist]),
                    metric_exp_histogram("controlled.exphist", vec![exphist]),
                    metric_summary("controlled.summary", vec![summary]),
                ],
            ),
        )
        .unwrap();
    processor.flush_head().unwrap();

    assert_eq!(segment_dir_count(tempdir.path()), 1);
    let store = open_default_store(tempdir.path()).with_query_projection_config(
        QueryProjectionConfig::default()
            .with_exponential_histogram_bucket_boundaries(vec![2.0, 4.0]),
    );

    assert_promql_samples(
        &store,
        r#"controlled.gauge{test.case="roundtrip",service.name="roundtrip-suite"}"#,
        vec![(5_000, 1.25)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.sum{test.case="roundtrip",service.name="roundtrip-suite"}"#,
        vec![(5_000, 42.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_count{test.case="roundtrip"}"#,
        vec![(5_000, 4.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_sum{test.case="roundtrip"}"#,
        vec![(5_000, 10.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_bucket{test.case="roundtrip",le="1"}"#,
        vec![(5_000, 1.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_bucket{test.case="roundtrip",le="5"}"#,
        vec![(5_000, 3.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.histogram_bucket{test.case="roundtrip",le="+Inf"}"#,
        vec![(5_000, 4.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.exphist_count{test.case="roundtrip"}"#,
        vec![(5_000, 5.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.exphist_sum{test.case="roundtrip"}"#,
        vec![(5_000, 12.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.exphist_bucket{test.case="roundtrip",le="2"}"#,
        vec![(5_000, 2.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.exphist_bucket{test.case="roundtrip",le="+Inf"}"#,
        vec![(5_000, 5.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.summary_count{test.case="roundtrip"}"#,
        vec![(5_000, 10.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.summary_sum{test.case="roundtrip"}"#,
        vec![(5_000, 50.0)],
    );
    assert_promql_samples(
        &store,
        r#"controlled.summary{test.case="roundtrip",quantile="0.9"}"#,
        vec![(5_000, 8.0)],
    );
}

fn assert_promql_samples(store: &SegmentStoreReader, query: &str, expected: Vec<(u64, f64)>) {
    let results = store.query_promql(query, 0, 10_000).unwrap();
    assert_eq!(results.len(), 1, "query {query}");
    assert_eq!(results[0].samples, expected, "query {query}");
}

#[test]
fn processor_stamps_cumulative_histogram_reset_hints() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    let head = Some(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ));
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut first = histogram_dp(vec![kv_str("pod.name", "hist")]);
    first.start_time_unix_nano = 1_000_000_000;
    first.time_unix_nano = 5_000_000_000;
    first.count = 10;
    first.sum = Some(20.0);
    first.explicit_bounds = vec![1.0];
    first.bucket_counts = vec![4, 6];

    let mut reset = histogram_dp(vec![kv_str("pod.name", "hist")]);
    reset.start_time_unix_nano = 4_000_000_000;
    reset.time_unix_nano = 6_000_000_000;
    reset.count = 3;
    reset.sum = Some(7.0);
    reset.explicit_bounds = vec![1.0];
    reset.bucket_counts = vec![1, 2];

    let mut metric = metric_histogram("request.duration", vec![first, reset]);
    if let Some(tonic::metrics::v1::metric::Data::Histogram(histogram)) = &mut metric.data {
        histogram.aggregation_temporality = AggregationTemporality::Cumulative as i32;
    }

    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_005,
            },
            request(vec![], vec![metric]),
        )
        .unwrap();
    processor.flush_head().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let mut chunk_reader = ChunkReader::new(
        File::open(seg_dir.join(SegmentFile::Chunks.filename())).expect("open chunks"),
    );
    let record = chunk_reader.read_next().unwrap().unwrap();
    let ChunkSamples::Histogram(samples) = record.samples else {
        panic!("expected histogram chunk");
    };

    assert_eq!(samples.len(), 2);
    assert_eq!(
        samples[0].1.metadata.temporality,
        OtlpAggregationTemporality::Cumulative
    );
    assert_eq!(samples[0].1.metadata.reset_hint, CounterResetHint::Unknown);
    assert_eq!(
        samples[1].1.metadata.reset_hint,
        CounterResetHint::CounterReset
    );
}

#[test]
fn processor_flushes_bounded_late_sample_as_overlapping_segment() {
    let tempdir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();

    let head = Some(
        HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_out_of_order_time_window(Duration::from_secs(6)),
    );
    let mut processor = OtlpLabelSetProcessor::new(
        LabelSetStoreKind::FlatInterned,
        Duration::from_secs(3600),
        head,
        Some(writer),
    );

    let mut first = number_dp(vec![kv_str("pod.name", "backend-1")]);
    first.time_unix_nano = 15_000_000_000;
    first.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(1.0));
    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 0,
                timestamp_ms: 1_000,
                captured_at_ms: 10_005,
            },
            request(vec![], vec![metric_gauge("cpu.usage", vec![first])]),
        )
        .unwrap();
    assert_eq!(segment_dir_count(tempdir.path()), 0);

    let mut late = number_dp(vec![kv_str("pod.name", "backend-1")]);
    late.time_unix_nano = 9_500_000_000;
    late.value = Some(tonic::metrics::v1::number_data_point::Value::AsDouble(2.0));
    processor
        .process(
            SourceMessageMetadata {
                topic: "t".to_string(),
                partition: 0,
                offset: 1,
                timestamp_ms: 2_000,
                captured_at_ms: 10_006,
            },
            request(vec![], vec![metric_gauge("cpu.usage", vec![late])]),
        )
        .unwrap();
    assert_eq!(segment_dir_count(tempdir.path()), 0);

    processor.flush_head().unwrap();
    assert_eq!(segment_dir_count(tempdir.path()), 2);

    let store = open_default_store(tempdir.path());
    let metric = normalize_metric_name("cpu.usage");
    let pod_label = normalize_label_name("pod.name");
    let results = store
        .query_exact(
            &[
                (METRIC_NAME_LABEL, metric.as_str()),
                (pod_label.as_str(), "backend-1"),
            ],
            0,
            20_000,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(9_500, 2.0), (15_000, 1.0)]);
}
